package pgwire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/modelpack"
	"github.com/akkio-inc/igloo/internal/snowflake"
	"github.com/akkio-inc/igloo/internal/tsforecast"
)

// Forecasting with foundation models, as other warehouses write it
// (docs/ml-functions.md): BigQuery's AI.FORECAST and AI.DETECT_ANOMALIES,
// and Databricks' ai_forecast and ai_detect_anomalies. Each becomes a query
// that gathers each series' points with list() and forecasts them with
// tsforecast's functions, which the server and every worker define: the
// distributed planner may give each worker the series of a partition. The
// input is read as the user reads it, policies applied, as in any query.

// points is the SQL of a series' points, as the functions take them.
func points(ts, y string) string {
	return "list({'t': CAST(" + ts + " AS TIMESTAMP), 'y': CAST(" + y + " AS DOUBLE)})"
}

const noPoints = "CAST([] AS STRUCT(t TIMESTAMP, y DOUBLE)[])"

// forecastArgs reads a forecasting call's arguments: positional ones by
// the names given, the rest by name, among names.
func forecastArgs(c *snowflake.ModelCall, positional []string, names ...string) error {
	for i := range c.Args {
		a := &c.Args[i]
		if a.Name == "" {
			if i >= len(positional) {
				return modelSyntax("name %s's arguments after the first %d, as in %s => ...", c.Method, len(positional), strings.ToLower(names[0]))
			}
			a.Name = positional[i]
		}
		if !slices.Contains(names, a.Name) && !slices.Contains(positional, a.Name) {
			return modelSyntax("%s takes %s, not %s", c.Method, strings.ToLower(strings.Join(append(slices.Clone(positional), names...), ", ")),
				strings.ToLower(a.Name))
		}
	}
	return nil
}

// columnsArg reads an argument naming one column or an array of them.
func columnsArg(c *snowflake.ModelCall, name string) ([]string, error) {
	v := c.Arg(name)
	if v == nil || v.Kind == snowflake.ValueNull {
		return nil, nil
	}
	switch v.Kind {
	case snowflake.ValueString:
		return []string{v.Str}, nil
	case snowflake.ValueArray:
		var out []string
		for _, e := range v.Elems {
			if e.Kind != snowflake.ValueString {
				return nil, modelSyntax("%s's columns are named in strings", strings.ToLower(name))
			}
			out = append(out, e.Str)
		}
		return out, nil
	}
	return nil, modelSyntax("%s takes a column's name or an array of them", strings.ToLower(name))
}

func numberArg(c *snowflake.ModelCall, name string) (*float64, error) {
	v := c.Arg(name)
	if v == nil || v.Kind == snowflake.ValueNull {
		return nil, nil
	}
	if v.Kind != snowflake.ValueNumber {
		return nil, modelSyntax("%s takes a number", strings.ToLower(name))
	}
	n := v.Num
	return &n, nil
}

func intArg(c *snowflake.ModelCall, name string, lo, hi int) (int, error) {
	n, err := numberArg(c, name)
	if err != nil || n == nil {
		return 0, err
	}
	if *n != math.Trunc(*n) || *n < float64(lo) || *n > float64(hi) {
		return 0, pgerror.WithCode(fmt.Errorf("%s is a whole number from %d to %d", strings.ToLower(name), lo, hi), codes.InvalidParameterValue)
	}
	return int(*n), nil
}

func boolArg(c *snowflake.ModelCall, name string) (bool, error) {
	v := c.Arg(name)
	if v == nil || v.Kind == snowflake.ValueNull {
		return false, nil
	}
	if v.Kind != snowflake.ValueBool {
		return false, modelSyntax("%s takes TRUE or FALSE", strings.ToLower(name))
	}
	return v.Bool, nil
}

// timeArg reads a timestamp given as a string, in microseconds.
func timeArg(c *snowflake.ModelCall, name string) (*int64, error) {
	v := c.Arg(name)
	if v == nil || v.Kind == snowflake.ValueNull {
		return nil, nil
	}
	if v.Kind != snowflake.ValueString {
		return nil, modelSyntax("%s takes a timestamp, such as '2026-12-31' or TIMESTAMP '2026-12-31 00:00:00'", strings.ToLower(name))
	}
	for _, layout := range []string{time.DateOnly, time.DateTime, "2006-01-02T15:04:05", time.RFC3339, "2006-01-02 15:04:05.999999",
		"2006-01-02T15:04:05.999999", "2006-01-02 15:04:05Z07:00"} {
		if t, err := time.Parse(layout, strings.TrimSpace(v.Str)); err == nil {
			m := t.UTC().UnixMicro()
			return &m, nil
		}
	}
	return nil, pgerror.WithCode(fmt.Errorf("%s: %q is not a timestamp", strings.ToLower(name), v.Str), codes.InvalidParameterValue)
}

// forecastModel resolves the model a call names, or its default.
func (s *Server) forecastModel(c *snowflake.ModelCall, def string) (string, error) {
	name := def
	if v := c.Arg("MODEL"); v != nil && v.Kind != snowflake.ValueNull {
		if v.Kind != snowflake.ValueString {
			return "", modelSyntax("model takes a model's name")
		}
		name = v.Str
	}
	model, ok := s.forecasting.Model(name)
	if !ok {
		return "", pgerror.WithHint(pgerror.WithCode(fmt.Errorf("%s is not a forecasting model Igloo has", name), codes.UndefinedObject),
			"the model pack's forecasting models are chronos-bolt-tiny, -mini, -small and -base, chronos-2, chronos-2-small and timesfm-2.5 "+
				"(docs/models.md)")
	}
	if _, err := s.forecasting.MaxContext(model); err != nil {
		return "", loadError(err)
	}
	return model, nil
}

// checkForecastMethod checks the method of a FORECAST or
// ANOMALY_DETECTION model's CONFIG_OBJECT: 'best', 'fast', or a
// forecasting model of the model pack, installed.
func (s *Server) checkForecastMethod(method string) error {
	switch strings.ToLower(strings.TrimSpace(method)) {
	case "", "best", "fast":
		return nil
	}
	model, ok := s.forecasting.Model(method)
	if !ok {
		return pgerror.WithHint(modelSyntax("method is 'best' or 'fast', or a forecasting model of the model pack, not %q", method),
			"the model pack's forecasting models are chronos-bolt-tiny, -mini, -small and -base, chronos-2, chronos-2-small and timesfm-2.5 "+
				"(docs/models.md)")
	}
	if _, err := s.forecasting.MaxContext(model); err != nil {
		return loadError(err)
	}
	return nil
}

// loadError is the error of a model the pack cannot load: one not
// installed, or one whose license the pack does not take.
func loadError(err error) error {
	if errors.Is(err, modelpack.ErrLicense) {
		return pgerror.WithCode(err, codes.FeatureNotSupported)
	}
	return pgerror.WithCode(err, codes.ObjectNotInPrerequisiteState)
}

func optionsJSON(o tsforecast.Options) string {
	b, _ := json.Marshal(o)
	return quoteLiteral(string(b))
}

func identList(names []string, prefix string) string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = prefix + quoteIdent(n)
	}
	return strings.Join(out, ", ")
}

// grouped is "SELECT ids..., exprs... FROM (input) WHERE ts IS NOT NULL
// GROUP BY ids...".
func grouped(ids []string, exprs []string, input, ts string) string {
	sel := slices.Clone(exprs)
	if len(ids) > 0 {
		sel = append([]string{identList(ids, "")}, exprs...)
	}
	q := "SELECT " + strings.Join(sel, ", ") + " FROM (" + input + ") AS __igloo_in WHERE " + ts + " IS NOT NULL"
	if len(ids) > 0 {
		q += " GROUP BY " + identList(ids, "")
	}
	return q
}

func withIDs(ids []string, prefix string, rest ...string) string {
	if len(ids) > 0 {
		return strings.Join(append([]string{identList(ids, prefix)}, rest...), ", ")
	}
	return strings.Join(rest, ", ")
}

// forecastQuery writes a forecasting call as a query.
func (s *Server) forecastQuery(ctx context.Context, sess *session, user auth.User, text string) (string, error) {
	c, err := snowflake.ParseForecastCall(text)
	if err != nil {
		return "", err
	}
	switch c.Method {
	case "AI.FORECAST":
		return s.bigqueryForecast(ctx, sess, user, c)
	case "AI.DETECT_ANOMALIES":
		return s.bigqueryDetect(ctx, sess, user, c)
	case "AI_FORECAST":
		return s.databricksForecast(ctx, sess, user, c)
	}
	return s.databricksDetect(ctx, sess, user, c)
}

// contextWindow reads BigQuery's context_window: for TimesFM 2.5, a power
// of two from 64 to 8,192, or 15,360.
func (s *Server) contextWindow(c *snowflake.ModelCall, model string) (int, error) {
	n, err := intArg(c, "CONTEXT_WINDOW", 1, 16384)
	if err != nil || n == 0 {
		return 0, err
	}
	if model == "timesfm-2.5" && n != 15360 && (n < 64 || n > 8192 || n&(n-1) != 0) {
		return 0, pgerror.WithCode(fmt.Errorf("context_window is 64, 128, ..., 8192 or 15360"), codes.InvalidParameterValue)
	}
	if max, err := s.forecasting.MaxContext(model); err == nil && n > max {
		return 0, pgerror.WithCode(fmt.Errorf("context_window %d is more than %s reads, %d", n, model, max), codes.InvalidParameterValue)
	}
	return n, nil
}

// bigqueryForecast writes AI.FORECAST(TABLE t | (query), data_col =>,
// timestamp_col => [, model, id_cols, horizon, forecast_end_timestamp,
// confidence_level, output_historical_time_series, context_window]).
func (s *Server) bigqueryForecast(ctx context.Context, sess *session, user auth.User, c *snowflake.ModelCall) (string, error) {
	if c.Arg("TARGET_COLS") != nil {
		return "", pgerror.WithCode(fmt.Errorf("AI.FORECAST's multivariate form (target_cols) is TimesFM 3.0's, which Igloo does not have"),
			codes.FeatureNotSupported)
	}
	if err := forecastArgs(c, []string{"INPUT"}, "DATA_COL", "TIMESTAMP_COL", "MODEL", "ID_COLS", "HORIZON", "FORECAST_END_TIMESTAMP",
		"CONFIDENCE_LEVEL", "OUTPUT_HISTORICAL_TIME_SERIES", "CONTEXT_WINDOW"); err != nil {
		return "", err
	}
	data, err := stringArg(c, "DATA_COL", true)
	if err != nil {
		return "", err
	}
	tsCol, err := stringArg(c, "TIMESTAMP_COL", true)
	if err != nil {
		return "", err
	}
	model, err := s.forecastModel(c, "TimesFM 2.5")
	if err != nil {
		return "", err
	}
	ids, err := columnsArg(c, "ID_COLS")
	if err != nil {
		return "", err
	}
	opts := tsforecast.Options{MinPoints: 3, Confidence: 0.95}
	if opts.Horizon, err = intArg(c, "HORIZON", 1, 10000); err != nil {
		return "", err
	}
	if opts.End, err = timeArg(c, "FORECAST_END_TIMESTAMP"); err != nil {
		return "", err
	}
	switch {
	case opts.End != nil && opts.Horizon > 0:
		return "", modelSyntax("AI.FORECAST takes horizon or forecast_end_timestamp, not both")
	case opts.End == nil && opts.Horizon == 0:
		opts.Horizon = 10
	case opts.End != nil:
		// BigQuery's end is the last forecast's timestamp; Options.End is
		// the first not forecast, as Databricks' horizon.
		*opts.End++
	}
	if v, err := numberArg(c, "CONFIDENCE_LEVEL"); err != nil {
		return "", err
	} else if v != nil {
		if *v <= 0 || *v >= 1 {
			return "", pgerror.WithCode(fmt.Errorf("confidence_level is between 0 and 1"), codes.InvalidParameterValue)
		}
		opts.Confidence = *v
	}
	if opts.History, err = boolArg(c, "OUTPUT_HISTORICAL_TIME_SERIES"); err != nil {
		return "", err
	}
	if opts.Context, err = s.contextWindow(c, model); err != nil {
		return "", err
	}
	input, err := s.inputQuery(ctx, sess, user, c.Arg("INPUT"), "AI.FORECAST's input")
	if err != nil {
		return "", err
	}
	ts := quoteIdent(tsCol)
	inner := grouped(ids, []string{tsforecast.ForecastFunction + "(" + quoteLiteral(model) + ", " + points(ts, quoteIdent(data)) + ", " +
		optionsJSON(opts) + ") AS __igloo_fs"}, input, ts)
	mid := "SELECT " + withIDs(ids, "", "unnest(__igloo_fs) AS f") + " FROM (" + inner + ") AS __igloo_g"
	conf := fmt.Sprintf("CAST(%v AS DOUBLE) AS confidence_level", opts.Confidence)
	if opts.History {
		return "(SELECT " + withIDs(ids, "", "f.kind AS time_series_type", "f.ts AS time_series_timestamp", "f.value AS time_series_data",
			conf, "f.lower AS prediction_interval_lower_bound", "f.upper AS prediction_interval_upper_bound",
			"f.status AS ai_forecast_status") + " FROM (" + mid + ") AS __igloo_u)", nil
	}
	return "(SELECT " + withIDs(ids, "", "f.ts AS forecast_timestamp", "f.value AS forecast_value", conf,
		"f.lower AS prediction_interval_lower_bound", "f.upper AS prediction_interval_upper_bound", "f.status AS ai_forecast_status") +
		" FROM (" + mid + ") AS __igloo_u)", nil
}

// bigqueryDetect writes AI.DETECT_ANOMALIES(history [, target],
// data_col =>, timestamp_col => [, target_last_n_points,
// target_start_timestamp, model, id_cols, anomaly_prob_threshold,
// context_window]).
func (s *Server) bigqueryDetect(ctx context.Context, sess *session, user auth.User, c *snowflake.ModelCall) (string, error) {
	positional := []string{"HISTORY"}
	if len(c.Args) > 1 && c.Args[1].Name == "" && (c.Args[1].Value.Kind == snowflake.ValueTable || c.Args[1].Value.Kind == snowflake.ValueQuery) {
		positional = append(positional, "TARGET")
	}
	if err := forecastArgs(c, positional, "DATA_COL", "TIMESTAMP_COL", "TARGET_LAST_N_POINTS", "TARGET_START_TIMESTAMP", "MODEL",
		"MODEL_NAME", "ID_COLS", "ANOMALY_PROB_THRESHOLD", "CONTEXT_WINDOW"); err != nil {
		return "", err
	}
	if v := c.Arg("MODEL_NAME"); v != nil && c.Arg("MODEL") == nil {
		c.Args = append(c.Args, snowflake.ModelArg{Name: "MODEL", Value: *v})
	}
	data, err := stringArg(c, "DATA_COL", true)
	if err != nil {
		return "", err
	}
	tsCol, err := stringArg(c, "TIMESTAMP_COL", true)
	if err != nil {
		return "", err
	}
	model, err := s.forecastModel(c, "TimesFM 2.5")
	if err != nil {
		return "", err
	}
	ids, err := columnsArg(c, "ID_COLS")
	if err != nil {
		return "", err
	}
	opts := tsforecast.Options{MinPoints: 3, Threshold: 0.95, MaxPoints: 1024}
	if opts.LastN, err = intArg(c, "TARGET_LAST_N_POINTS", 1, 10000); err != nil {
		return "", err
	}
	if opts.Start, err = timeArg(c, "TARGET_START_TIMESTAMP"); err != nil {
		return "", err
	}
	target := c.Arg("TARGET")
	n := 0
	for _, given := range []bool{target != nil, opts.LastN > 0, opts.Start != nil} {
		if given {
			n++
		}
	}
	if n != 1 {
		return "", modelSyntax("AI.DETECT_ANOMALIES takes one of a target table, target_last_n_points and target_start_timestamp")
	}
	if v, err := numberArg(c, "ANOMALY_PROB_THRESHOLD"); err != nil {
		return "", err
	} else if v != nil {
		if *v <= 0 || *v >= 1 {
			return "", pgerror.WithCode(fmt.Errorf("anomaly_prob_threshold is between 0 and 1"), codes.InvalidParameterValue)
		}
		opts.Threshold = *v
	}
	if opts.Context, err = s.contextWindow(c, model); err != nil {
		return "", err
	}
	history, err := s.inputQuery(ctx, sess, user, c.Arg("HISTORY"), "AI.DETECT_ANOMALIES's history")
	if err != nil {
		return "", err
	}
	ts, y := quoteIdent(tsCol), quoteIdent(data)
	call := func(h, t string) string {
		return "unnest(" + tsforecast.DetectFunction + "(" + quoteLiteral(model) + ", " + h + ", " + t + ", " + optionsJSON(opts) + ")) AS d"
	}
	var mid string
	if target != nil {
		tq, err := s.inputQuery(ctx, sess, user, target, "AI.DETECT_ANOMALIES's target")
		if err != nil {
			return "", err
		}
		h := grouped(ids, []string{points(ts, y) + " AS p"}, history, ts)
		t := grouped(ids, []string{points(ts, y) + " AS p"}, tq, ts)
		join := " CROSS JOIN (" + h + ") AS __igloo_h"
		if len(ids) > 0 {
			join = " LEFT JOIN (" + h + ") AS __igloo_h USING (" + identList(ids, "") + ")"
		}
		mid = "SELECT " + withIDs(ids, "__igloo_t.", call("coalesce(__igloo_h.p, "+noPoints+")", "__igloo_t.p")) + " FROM (" + t +
			") AS __igloo_t" + join
	} else {
		mid = "SELECT " + withIDs(ids, "", call("p", noPoints)) + " FROM (" + grouped(ids, []string{points(ts, y) + " AS p"}, history, ts) +
			") AS __igloo_g"
	}
	return "(SELECT " + withIDs(ids, "", "d.ts AS time_series_timestamp", "d.value AS time_series_data", "d.is_anomaly AS is_anomaly",
		"d.lower AS lower_bound", "d.upper AS upper_bound", "d.probability AS anomaly_probability",
		"d.status AS ai_detect_anomalies_status") + " FROM (" + mid + ") AS __igloo_u)", nil
}

// databricksForecast writes ai_forecast(observed, horizon, time_col,
// value_col [, group_col, prediction_interval_width, frequency, seed,
// parameters, positive_only, version, model]).
func (s *Server) databricksForecast(ctx context.Context, sess *session, user auth.User, c *snowflake.ModelCall) (string, error) {
	if err := forecastArgs(c, []string{"OBSERVED", "HORIZON", "TIME_COL", "VALUE_COL", "GROUP_COL"}, "PREDICTION_INTERVAL_WIDTH",
		"FREQUENCY", "SEED", "PARAMETERS", "VERSION", "COVARIATE_COL", "HOLIDAY_REGION", "POSITIVE_ONLY", "MODEL"); err != nil {
		return "", err
	}
	for _, unsupported := range []string{"COVARIATE_COL", "HOLIDAY_REGION"} {
		if v := c.Arg(unsupported); v != nil && v.Kind != snowflake.ValueNull {
			return "", pgerror.WithCode(fmt.Errorf("ai_forecast's %s is not supported: Igloo's models forecast from the values alone",
				strings.ToLower(unsupported)), codes.FeatureNotSupported)
		}
	}
	if v := c.Arg("PARAMETERS"); v != nil && v.Kind == snowflake.ValueString && strings.TrimSpace(v.Str) != "" && strings.TrimSpace(v.Str) != "{}" {
		return "", pgerror.WithCode(fmt.Errorf("ai_forecast's parameters are version 1's prophet-like model's, which Igloo does not have; "+
			"Igloo forecasts with a foundation model"), codes.FeatureNotSupported)
	}
	if v := c.Arg("VERSION"); v != nil && v.Kind == snowflake.ValueString && v.Str != "1" && v.Str != "2" {
		return "", pgerror.WithCode(fmt.Errorf("ai_forecast's version is '1' or '2'"), codes.InvalidParameterValue)
	}
	tsCol, err := stringArg(c, "TIME_COL", true)
	if err != nil {
		return "", err
	}
	values, err := columnsArg(c, "VALUE_COL")
	if err != nil {
		return "", err
	}
	if len(values) == 0 || len(values) > 100 {
		return "", modelSyntax("ai_forecast needs value_col: a column, or an array of up to 100")
	}
	groups, err := columnsArg(c, "GROUP_COL")
	if err != nil {
		return "", err
	}
	model, err := s.forecastModel(c, "chronos-2")
	if err != nil {
		return "", err
	}
	opts := tsforecast.Options{Confidence: 0.95}
	if opts.End, err = timeArg(c, "HORIZON"); err != nil {
		return "", err
	}
	if opts.End == nil {
		return "", modelSyntax("ai_forecast needs horizon, the timestamp the forecast ends before, such as '2026-12-31'")
	}
	if v, err := numberArg(c, "PREDICTION_INTERVAL_WIDTH"); err != nil {
		return "", err
	} else if v != nil {
		if *v <= 0 || *v >= 1 {
			return "", pgerror.WithCode(fmt.Errorf("prediction_interval_width is between 0 and 1"), codes.InvalidParameterValue)
		}
		opts.Confidence = *v
	}
	if f, err := stringArg(c, "FREQUENCY", false); err != nil {
		return "", err
	} else if _, err := tsforecast.ParseFrequency(f); err != nil {
		return "", pgerror.WithCode(err, codes.InvalidParameterValue)
	} else {
		opts.Frequency = f
	}
	if opts.Positive, err = boolArg(c, "POSITIVE_ONLY"); err != nil {
		return "", err
	}
	input, err := s.inputQuery(ctx, sess, user, c.Arg("OBSERVED"), "ai_forecast's observed")
	if err != nil {
		return "", err
	}
	ts := quoteIdent(tsCol)
	var calls, unnests, cols []string
	for i, v := range values {
		calls = append(calls, fmt.Sprintf("%s(%s, %s, %s) AS __igloo_fs%d", tsforecast.ForecastFunction, quoteLiteral(model),
			points(ts, quoteIdent(v)), optionsJSON(opts), i))
		unnests = append(unnests, fmt.Sprintf("unnest(__igloo_fs%d) AS f%d", i, i))
		cols = append(cols, fmt.Sprintf("f%d.value AS %s", i, quoteIdent(v+"_forecast")), fmt.Sprintf("f%d.upper AS %s", i, quoteIdent(v+"_upper")),
			fmt.Sprintf("f%d.lower AS %s", i, quoteIdent(v+"_lower")))
	}
	mid := "SELECT " + withIDs(groups, "", unnests...) + " FROM (" + grouped(groups, calls, input, ts) + ") AS __igloo_g"
	return "(SELECT " + strings.Join(append([]string{"f0.ts AS " + quoteIdent(tsCol)}, withIDs(groups, "", cols...)), ", ") +
		" FROM (" + mid + ") AS __igloo_u WHERE f0.ts IS NOT NULL)", nil
}

// databricksDetect writes ai_detect_anomalies(observed, time_col,
// value_col [, group_col, anomaly_threshold, only_last_n, smoothing,
// model]).
func (s *Server) databricksDetect(ctx context.Context, sess *session, user auth.User, c *snowflake.ModelCall) (string, error) {
	if err := forecastArgs(c, []string{"OBSERVED", "TIME_COL", "VALUE_COL", "GROUP_COL"}, "ANOMALY_THRESHOLD", "COVARIATE_COL",
		"HOLIDAY_REGION", "ONLY_LAST_N", "SMOOTHING", "MODEL"); err != nil {
		return "", err
	}
	for _, unsupported := range []string{"COVARIATE_COL", "HOLIDAY_REGION"} {
		if v := c.Arg(unsupported); v != nil && v.Kind != snowflake.ValueNull {
			return "", pgerror.WithCode(fmt.Errorf("ai_detect_anomalies's %s is not supported: Igloo's models read the values alone",
				strings.ToLower(unsupported)), codes.FeatureNotSupported)
		}
	}
	tsCol, err := stringArg(c, "TIME_COL", true)
	if err != nil {
		return "", err
	}
	value, err := stringArg(c, "VALUE_COL", true)
	if err != nil {
		return "", err
	}
	groups, err := columnsArg(c, "GROUP_COL")
	if err != nil {
		return "", err
	}
	model, err := s.forecastModel(c, "chronos-2")
	if err != nil {
		return "", err
	}
	opts := tsforecast.Options{Rolling: true, Warmup: 8, Window: 64, Threshold: 0.95, MaxPoints: 8192}
	if v, err := numberArg(c, "ANOMALY_THRESHOLD"); err != nil {
		return "", err
	} else if v != nil {
		if *v <= 0 || *v >= 1 {
			return "", pgerror.WithCode(fmt.Errorf("anomaly_threshold is between 0 and 1"), codes.InvalidParameterValue)
		}
		opts.Threshold = *v
	}
	if v := c.Arg("ONLY_LAST_N"); v != nil && v.Kind != snowflake.ValueNull {
		if v.Kind == snowflake.ValueNumber {
			if opts.LastN, err = intArg(c, "ONLY_LAST_N", 1, 8192); err != nil {
				return "", err
			}
		} else if opts.Start, err = timeArg(c, "ONLY_LAST_N"); err != nil {
			return "", err
		}
	}
	input, err := s.inputQuery(ctx, sess, user, c.Arg("OBSERVED"), "ai_detect_anomalies's observed")
	if err != nil {
		return "", err
	}
	ts := quoteIdent(tsCol)
	call := "unnest(" + tsforecast.DetectFunction + "(" + quoteLiteral(model) + ", p, " + noPoints + ", " + optionsJSON(opts) + ")) AS d"
	mid := "SELECT " + withIDs(groups, "", call) + " FROM (" + grouped(groups, []string{points(ts, quoteIdent(value)) + " AS p"}, input, ts) +
		") AS __igloo_g"
	return "(SELECT " + strings.Join([]string{"d.ts AS " + quoteIdent(tsCol), withIDs(groups, "", "d.value AS "+quoteIdent(value),
		"d.expected AS expected", "d.lower AS lower_bound", "d.upper AS upper_bound", "d.probability AS anomaly_score",
		"d.is_anomaly AS is_anomaly")}, ", ") + " FROM (" + mid + ") AS __igloo_u)", nil
}

// forecastRewrite writes the forecasting calls of a statement in DuckDB's
// dialect as queries (the Snowflake dialect's translation marks them for
// resolveModelText instead).
func (s *Server) forecastRewrite(ctx context.Context, sess *session, user auth.User, query string) (string, error) {
	low := strings.ToLower(query)
	if !strings.Contains(low, "forecast") && !strings.Contains(low, "detect_anomalies") {
		return query, nil
	}
	toks := mlTokens(query)
	type span struct{ start, end int }
	var calls []span
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if t.kind != 'w' {
			continue
		}
		j := i + 1
		switch {
		case t.text == "ai" && i+3 < len(toks) && toks[i+1].text == "." && (toks[i+2].text == "forecast" || toks[i+2].text == "detect_anomalies") &&
			toks[i+3].text == "(":
			j = i + 3
		case (t.text == "ai_forecast" || t.text == "ai_detect_anomalies") && i+1 < len(toks) && toks[i+1].text == "(":
			if i > 0 && toks[i-1].text == "." {
				continue
			}
		default:
			continue
		}
		depth, end := 0, -1
		for k := j; k < len(toks); k++ {
			switch toks[k].text {
			case "(":
				depth++
			case ")":
				depth--
			}
			if toks[k].kind == 'p' && depth == 0 {
				end = toks[k].end
				break
			}
		}
		if end < 0 {
			return query, nil
		}
		// Databricks' and BigQuery's calls name their arguments or take a
		// TABLE: a call of a user's function of the same name does not.
		text := query[t.start:end]
		lowText := strings.ToLower(text)
		if !strings.Contains(lowText, "=>") && !strings.Contains(lowText, "table") {
			continue
		}
		calls = append(calls, span{t.start, end})
		for i < len(toks) && toks[i].end <= end {
			i++
		}
		i--
	}
	if len(calls) == 0 {
		return query, nil
	}
	keeps, refuse := keepsSQL(query)
	if refuse != "" {
		return "", pgerror.WithHint(pgerror.WithCode(fmt.Errorf("a %s cannot forecast", strings.ToLower(refuse)), codes.FeatureNotSupported),
			"create a table from the forecast (CREATE TABLE ... AS), or a task that does so on a schedule")
	}
	if keeps {
		return query, nil
	}
	var b strings.Builder
	last := 0
	for _, c := range calls {
		repl, err := s.forecastQuery(ctx, sess, user, query[c.start:c.end])
		if err != nil {
			return "", modelError(err)
		}
		b.WriteString(query[last:c.start])
		b.WriteString(repl)
		last = c.end
	}
	b.WriteString(query[last:])
	return b.String(), nil
}
