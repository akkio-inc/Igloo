package tsforecast

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/models"
	"github.com/akkio-inc/igloo/internal/tsfm"
)

// The functions package pgwire writes AI.FORECAST, ai_forecast,
// AI.DETECT_ANOMALIES and ai_detect_anomalies as, over each series'
// points gathered with list({'t': ts, 'y': y}) (see docs/ml-functions.md):
//
//	igloo_ts_forecast(model, points, options)          → LIST(STRUCT(kind, ts, value, lower, upper, status))
//	igloo_ts_detect(model, history, target, options)   → LIST(STRUCT(ts, value, expected, lower, upper, probability, is_anomaly, status))
//
// They are the same on the server and on workers, which load the models
// from the same model pack, so a statement grouping a large table's series
// runs on workers, each forecasting the series of its partition. options
// is a JSON object.
const (
	ForecastFunction = "igloo_ts_forecast"
	DetectFunction   = "igloo_ts_detect"
)

// Functions are the names of the functions, which the distributed planner
// lets workers run.
var Functions = []string{ForecastFunction, DetectFunction}

// Options are the functions' options.
type Options struct {
	// Horizon is the steps to forecast; End, if set, the timestamp the
	// forecast stops before (as Databricks' horizon and BigQuery's
	// forecast_end_timestamp), in microseconds.
	Horizon int    `json:"horizon,omitempty"`
	End     *int64 `json:"end,omitempty"`
	// Confidence is the prediction interval's coverage (0.95 by default);
	// for detection, Threshold is the probability from which a point is
	// anomalous.
	Confidence float64 `json:"confidence,omitempty"`
	Threshold  float64 `json:"threshold,omitempty"`
	// Context is the most history the model reads; 0 is its default.
	Context int `json:"context,omitempty"`
	// Frequency is the series' step, as Snowflake ('1 day') or pandas
	// ('D', '15min') writes it; empty or auto infers it.
	Frequency string `json:"frequency,omitempty"`
	// History adds the series' points to the forecast, as kind 'history'.
	History bool `json:"history,omitempty"`
	// MinPoints is the fewest points a series needs (2 by default).
	MinPoints int `json:"min_points,omitempty"`
	// Positive clamps forecasts at zero.
	Positive bool `json:"positive,omitempty"`
	// Detection: Rolling scores every point (after the first Warmup) from
	// the points before it, by windows of Window steps, as Databricks'
	// ai_detect_anomalies; otherwise the target points are scored from the
	// history before them, as BigQuery's AI.DETECT_ANOMALIES. LastN and
	// Start choose the target points among the series' own; MaxPoints
	// keeps the most recent.
	Rolling   bool   `json:"rolling,omitempty"`
	Warmup    int    `json:"warmup,omitempty"`
	Window    int    `json:"window,omitempty"`
	LastN     int    `json:"last_n,omitempty"`
	Start     *int64 `json:"start,omitempty"`
	MaxPoints int    `json:"max_points,omitempty"`
}

func parseOptions(v any) (Options, error) {
	var o Options
	s, _ := v.(string)
	if strings.TrimSpace(s) != "" {
		if err := json.Unmarshal([]byte(s), &o); err != nil {
			return o, fmt.Errorf("%w: options: %v", ErrInput, err)
		}
	}
	if o.Confidence == 0 {
		o.Confidence = 0.95
	}
	if o.Threshold == 0 {
		o.Threshold = 0.95
	}
	if o.MinPoints == 0 {
		o.MinPoints = 2
	}
	if o.Window == 0 {
		o.Window = 64
	}
	if o.Confidence <= 0 || o.Confidence >= 1 || o.Threshold <= 0 || o.Threshold >= 1 {
		return o, fmt.Errorf("%w: the confidence level and threshold are between 0 and 1", ErrInput)
	}
	return o, nil
}

var pandasFreq = regexp.MustCompile(`^(\d*)\s*([A-Za-z]+)$`)

// ParseFrequency reads a frequency as Snowflake ('1 day', '15 minutes') or
// pandas ('D', 'H', '15min', 'W', 'MS') writes it; "" and "auto" are none.
func ParseFrequency(s string) (*models.Frequency, error) {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "auto") {
		return nil, nil
	}
	if f, err := models.ParseFrequency(s); err == nil {
		return &f, nil
	}
	m := pandasFreq.FindStringSubmatch(s)
	if m == nil {
		return nil, fmt.Errorf("%w: frequency %q", ErrInput, s)
	}
	n := 1
	if m[1] != "" {
		n, _ = strconv.Atoi(m[1])
	}
	unit := map[string]string{"s": "second", "S": "second", "min": "minute", "T": "minute", "h": "hour", "H": "hour", "D": "day",
		"d": "day", "W": "week", "M": "month", "MS": "month", "ME": "month", "Q": "quarter", "QS": "quarter", "QE": "quarter",
		"Y": "year", "YS": "year", "YE": "year", "A": "year", "AS": "year"}[m[2]]
	if unit == "" {
		return nil, fmt.Errorf("%w: frequency %q: use seconds, minutes, hours, days, weeks, months, quarters or years", ErrInput, s)
	}
	f, err := models.ParseFrequency(strconv.Itoa(n) + " " + unit)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInput, err)
	}
	return &f, nil
}

// signature is a function's arguments' and result's types.
type signature struct {
	args   []duckdb.TypeInfo
	result duckdb.TypeInfo
}

// types are the forecasting and detection functions' signatures.
func types() (forecast, detect signature, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("the forecasting functions' types: %v", r)
		}
	}()
	t := func(tt duckdb.Type) duckdb.TypeInfo {
		info, err := duckdb.NewTypeInfo(tt)
		if err != nil {
			panic(err)
		}
		return info
	}
	list := func(child duckdb.TypeInfo) duckdb.TypeInfo {
		info, err := duckdb.NewListInfo(child)
		if err != nil {
			panic(err)
		}
		return info
	}
	strct := func(names []string, types []duckdb.TypeInfo) duckdb.TypeInfo {
		entries := make([]duckdb.StructEntry, len(names))
		for i := range names {
			e, err := duckdb.NewStructEntry(types[i], names[i])
			if err != nil {
				panic(err)
			}
			entries[i] = e
		}
		info, err := duckdb.NewStructInfo(entries[0], entries[1:]...)
		if err != nil {
			panic(err)
		}
		return info
	}
	varchar, ts, double, boolean := t(duckdb.TYPE_VARCHAR), t(duckdb.TYPE_TIMESTAMP), t(duckdb.TYPE_DOUBLE), t(duckdb.TYPE_BOOLEAN)
	points := list(strct([]string{"t", "y"}, []duckdb.TypeInfo{ts, double}))
	forecast = signature{args: []duckdb.TypeInfo{varchar, points, varchar},
		result: list(strct([]string{"kind", "ts", "value", "lower", "upper", "status"},
			[]duckdb.TypeInfo{varchar, ts, double, double, double, varchar}))}
	detect = signature{args: []duckdb.TypeInfo{varchar, points, points, varchar},
		result: list(strct([]string{"ts", "value", "expected", "lower", "upper", "probability", "is_anomaly", "status"},
			[]duckdb.TypeInfo{ts, double, double, double, double, double, boolean, varchar}))}
	return forecast, detect, nil
}

// Register defines the functions on conn's database, with svc's models.
func Register(conn *sql.Conn, svc *Service) error {
	forecast, detect, err := types()
	if err != nil {
		return err
	}
	if err := duckdb.RegisterScalarUDF(conn, ForecastFunction, &udf{svc: svc, sig: forecast}); err != nil {
		return err
	}
	return duckdb.RegisterScalarUDF(conn, DetectFunction, &udf{svc: svc, sig: detect, detect: true})
}

type udf struct {
	svc    *Service
	detect bool
	sig    signature
}

func (u *udf) Config() duckdb.ScalarFuncConfig {
	return duckdb.ScalarFuncConfig{InputTypeInfos: u.sig.args, ResultTypeInfo: u.sig.result}
}

func (u *udf) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		ChunkContextExecutor: func(ctx context.Context, chunk *duckdb.ChunkIteratorState) error {
			return evalChunk(ctx, chunk, u.svc, u.detect)
		},
	}
}

// evalChunk forecasts, or scores, a chunk's rows with b.
func evalChunk(ctx context.Context, chunk *duckdb.ChunkIteratorState, b backend, detect bool) error {
	var rows [][]any
	for st, err := range chunk.Rows() {
		if err != nil {
			return err
		}
		r := make([]any, st.ColumnCount())
		for i := range r {
			r[i] = *st.GetValuePtr(i)
		}
		rows = append(rows, r)
	}
	if len(rows) == 0 {
		return nil
	}
	var out []any
	var err error
	if detect {
		out, err = detectRows(ctx, b, rows)
	} else {
		out, err = forecastRows(ctx, b, rows)
	}
	if err != nil {
		return err
	}
	k := 0
	for st, err := range chunk.Rows() {
		if err != nil {
			return err
		}
		if err := st.SetResult(out[k]); err != nil {
			return err
		}
		k++
	}
	return nil
}

// points reads a series' points, a list of {'t': timestamp, 'y': value}.
func points(v any) ([]int64, []float64) {
	l, _ := v.([]any)
	ts := make([]int64, 0, len(l))
	ys := make([]float64, 0, len(l))
	for _, e := range l {
		m, _ := e.(map[string]any)
		t, ok := m["t"].(time.Time)
		if !ok {
			continue
		}
		y := math.NaN()
		switch v := m["y"].(type) {
		case float64:
			y = v
		case float32:
			y = float64(v)
		}
		ts = append(ts, t.UnixMicro())
		ys = append(ys, y)
	}
	return ts, ys
}

// job is one series to forecast: its grid's last steps, and how far.
type job struct {
	row     int
	model   string
	context []float32
	horizon int
	opts    Options
	grid    Grid
	// from is the grid step the forecast starts at.
	from int
	// interval is the interval that matters most: the forecast's
	// confidence, or detection's threshold.
	interval float64
}

// backend forecasts jobs: the model pack's models (Service), or hosted ones
// through an AI integration (hosted).
type backend interface {
	// model resolves the model SQL names; ok is false when it names none.
	model(name string) (string, bool)
	// run forecasts the jobs; errors that are not ErrInput's fail the
	// statement.
	run(ctx context.Context, jobs []*job) ([]tsfm.Forecast, []error)
}

func (s *Service) model(name string) (string, bool) { return s.Model(name) }

// run forecasts the jobs, model by model, on the machine's threads.
func (s *Service) run(ctx context.Context, jobs []*job) ([]tsfm.Forecast, []error) {
	out := make([]tsfm.Forecast, len(jobs))
	errs := make([]error, len(jobs))
	type key struct {
		model            string
		horizon, context int
	}
	groups := map[key][]int{}
	var order []key
	for i, j := range jobs {
		k := key{j.model, j.horizon, j.opts.Context}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], i)
	}
	for _, k := range order {
		idx := groups[k]
		series := make([][]float32, len(idx))
		for n, i := range idx {
			series[n] = jobs[i].context
		}
		fs, err := s.Forecast(ctx, k.model, series, k.horizon, k.context)
		for n, i := range idx {
			if err != nil {
				errs[i] = err
				continue
			}
			out[i] = fs[n]
		}
	}
	return out, errs
}

// failed is a status row: why a series has no forecast.
func failed(kind bool, err error) []any {
	msg := strings.TrimPrefix(err.Error(), ErrInput.Error()+": ")
	if kind {
		return []any{map[string]any{"kind": "forecast", "ts": nil, "value": nil, "lower": nil, "upper": nil, "status": msg}}
	}
	return []any{map[string]any{"ts": nil, "value": nil, "expected": nil, "lower": nil, "upper": nil, "probability": nil,
		"is_anomaly": nil, "status": msg}}
}

// fatal reports whether err fails the statement rather than one series:
// a model that cannot be loaded, a cancelled statement.
func fatal(err error) bool {
	return err != nil && !strings.HasPrefix(err.Error(), ErrInput.Error()) && !isInputErr(err)
}

func isInputErr(err error) bool {
	return err != nil && (strings.Contains(err.Error(), tsfm.ErrInput.Error()))
}

func forecastRows(ctx context.Context, b backend, rows [][]any) ([]any, error) {
	out := make([]any, len(rows))
	var jobs []*job
	for i, r := range rows {
		model, _ := r[0].(string)
		opts, err := parseOptions(r[2])
		if err != nil {
			return nil, err
		}
		name, ok := b.model(model)
		if !ok {
			return nil, fmt.Errorf("%w: %q is not a forecasting model of the model pack (docs/models.md)", ErrInput, model)
		}
		freq, err := ParseFrequency(opts.Frequency)
		if err != nil {
			return nil, err
		}
		ts, ys := points(r[1])
		g, err := Layout(ts, ys, freq)
		if err == nil && g.Points < opts.MinPoints {
			err = fmt.Errorf("%w: the series has %d points; it needs at least %d", ErrInput, g.Points, opts.MinPoints)
		}
		if err != nil {
			out[i] = failed(true, err)
			continue
		}
		h := opts.Horizon
		if opts.End != nil {
			h = g.Freq.Index(g.Start, *opts.End-1) + 1 - len(g.Values)
		}
		if h <= 0 {
			out[i] = failed(true, fmt.Errorf("%w: the horizon ends before the series does", ErrInput))
			continue
		}
		jobs = append(jobs, &job{row: i, model: name, context: g.Values, horizon: h, opts: opts, grid: g, from: len(g.Values),
			interval: opts.Confidence})
	}
	fs, errs := b.run(ctx, jobs)
	for n, j := range jobs {
		if errs[n] != nil {
			if fatal(errs[n]) {
				return nil, errs[n]
			}
			out[j.row] = failed(true, errs[n])
			continue
		}
		f := fs[n]
		lo, hi := (1-j.opts.Confidence)/2, (1+j.opts.Confidence)/2
		var res []any
		if j.opts.History {
			for k, v := range j.grid.Values {
				if isNaN(v) {
					continue
				}
				res = append(res, map[string]any{"kind": "history", "ts": time.UnixMicro(j.grid.At(k)).UTC(), "value": float64(v),
					"lower": nil, "upper": nil, "status": ""})
			}
		}
		clamp := func(v float64) float64 {
			if j.opts.Positive && v < 0 {
				return 0
			}
			return v
		}
		for t := 0; t < j.horizon; t++ {
			res = append(res, map[string]any{"kind": "forecast", "ts": time.UnixMicro(j.grid.At(j.from + t)).UTC(),
				"value": clamp(f.Point[t]), "lower": clamp(f.QuantileAt(lo, t)), "upper": clamp(f.QuantileAt(hi, t)), "status": ""})
		}
		out[j.row] = res
	}
	return out, nil
}

func isNaN(v float32) bool { return v != v }

// Score is how anomalous y is in a forecast's distribution: 0 at its
// median, near 1 far in either tail.
func Score(f tsfm.Forecast, y float64, t int) float64 {
	return math.Abs(2*f.CDF(y, t) - 1)
}

func detectRows(ctx context.Context, b backend, rows [][]any) ([]any, error) {
	out := make([]any, len(rows))
	type target struct {
		row   int
		grid  Grid
		steps []int // grid steps to score
		opts  Options
	}
	var jobs []*job
	var owners [][]int // per job: the target steps it scores, as grid steps
	var targets []*target
	for i, r := range rows {
		model, _ := r[0].(string)
		opts, err := parseOptions(r[3])
		if err != nil {
			return nil, err
		}
		name, ok := b.model(model)
		if !ok {
			return nil, fmt.Errorf("%w: %q is not a forecasting model of the model pack (docs/models.md)", ErrInput, model)
		}
		freq, err := ParseFrequency(opts.Frequency)
		if err != nil {
			return nil, err
		}
		ts, ys := points(r[1])
		tts, tys := points(r[2])
		separate := len(tts) > 0
		all := append(append([]int64{}, ts...), tts...)
		allY := append(append([]float64{}, ys...), tys...)
		g, err := Layout(all, allY, freq)
		if err != nil {
			out[i] = failed(false, err)
			continue
		}
		// The steps to score, and where history ends.
		var steps []int
		histEnd := len(g.Values)
		switch {
		case separate:
			for _, t := range tts {
				steps = append(steps, g.Freq.Index(g.Start, t))
			}
			if len(ts) > 0 {
				last := ts[0]
				for _, t := range ts {
					last = max(last, t)
				}
				histEnd = g.Freq.Index(g.Start, last) + 1
			} else {
				histEnd = 0
			}
		case opts.LastN > 0:
			histEnd = 0
			for k := len(g.Values) - 1; k >= 0 && len(steps) < opts.LastN; k-- {
				if !isNaN(g.Values[k]) {
					steps = append(steps, k)
				}
			}
		case opts.Start != nil:
			first := g.Freq.Index(g.Start, *opts.Start-1) + 1
			for k := max(first, 0); k < len(g.Values); k++ {
				steps = append(steps, k)
			}
		default:
			for k := 0; k < len(g.Values); k++ {
				steps = append(steps, k)
			}
		}
		steps = dedupe(steps, g)
		if opts.MaxPoints > 0 && len(steps) > opts.MaxPoints {
			steps = steps[len(steps)-opts.MaxPoints:]
		}
		if len(steps) == 0 {
			out[i] = []any{}
			continue
		}
		tg := &target{row: i, grid: g, steps: steps, opts: opts}
		targets = append(targets, tg)
		// Each window of steps is scored from the history before it: one
		// window after the history, or windows of opts.Window steps when
		// every point is scored in turn.
		add := func(from int, window []int) {
			ctxVals := g.Values[:from]
			n := 0
			for _, v := range ctxVals {
				if !isNaN(v) {
					n++
				}
			}
			if n < max(opts.MinPoints, 2) {
				return
			}
			jobs = append(jobs, &job{row: len(targets) - 1, model: name, context: ctxVals, horizon: window[len(window)-1] - from + 1,
				opts: opts, grid: g, from: from, interval: opts.Threshold})
			owners = append(owners, window)
		}
		if opts.Rolling {
			// The first Warmup steps are listed, but not scored.
			a := 0
			for a < len(steps) && steps[a] < opts.Warmup {
				a++
			}
			for a < len(steps) {
				b := a
				for b < len(steps) && steps[b]-steps[a] < opts.Window {
					b++
				}
				add(steps[a], steps[a:b])
				a = b
			}
		} else {
			from := steps[0]
			if separate {
				from = min(histEnd, steps[0])
			}
			add(from, steps)
		}
	}
	fs, errs := b.run(ctx, jobs)
	scored := make([]map[int][]any, len(targets))
	for n, j := range jobs {
		tg := targets[j.row]
		if scored[j.row] == nil {
			scored[j.row] = map[int][]any{}
		}
		if errs[n] != nil {
			if fatal(errs[n]) {
				return nil, errs[n]
			}
			continue
		}
		f := fs[n]
		thr := tg.opts.Threshold
		for _, k := range owners[n] {
			t := k - j.from
			y := float64(tg.grid.Values[k])
			lo, hi := f.QuantileAt((1-thr)/2, t), f.QuantileAt((1+thr)/2, t)
			p := Score(f, y, t)
			scored[j.row][k] = []any{f.Point[t], lo, hi, p, p > thr}
		}
	}
	for n, tg := range targets {
		var res []any
		for _, k := range tg.steps {
			v := float64(tg.grid.Values[k])
			row := map[string]any{"ts": time.UnixMicro(tg.grid.At(k)).UTC(), "value": v, "expected": nil, "lower": nil, "upper": nil,
				"probability": nil, "is_anomaly": nil, "status": ""}
			if s := scored[n][k]; s != nil {
				row["expected"], row["lower"], row["upper"], row["probability"], row["is_anomaly"] = s[0], s[1], s[2], s[3], s[4]
			} else {
				row["status"] = "not enough history before this point"
			}
			res = append(res, row)
		}
		out[tg.row] = res
	}
	return out, nil
}

// dedupe sorts steps, dropping repeats and steps without a value.
func dedupe(steps []int, g Grid) []int {
	seen := map[int]bool{}
	var out []int
	for _, k := range steps {
		if k < 0 || k >= len(g.Values) || seen[k] || isNaN(g.Values[k]) {
			continue
		}
		seen[k] = true
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}
