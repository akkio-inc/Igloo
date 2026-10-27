package pgwire

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/modelpack"
	"github.com/akkio-inc/igloo/internal/tsforecast"
	"github.com/akkio-inc/igloo/internal/tsforecast/tsforecasttest"
)

// The tests forecast with tsfm's tiny Chronos-Bolt, random weights in the
// real architecture, installed as test-bolt: its forecasts are not good
// ones, so they check what each surface returns, not how well it
// forecasts (package tsfm checks the models against their reference
// implementations).

// startForecastServer starts a server whose model pack has test-bolt, with
// two campaigns' daily spend.
func startForecastServer(t *testing.T) *catalogServer {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "models")
	tsforecasttest.InstallTiny(t, dir, "test-bolt")
	cs := startCatalogServerWithConfig(t, config.Config{Models: config.Models{Dir: dir}})
	mustExec(t, cs.admin, dailySpend)
	return cs
}

// registerForecasting defines the forecasting functions on a test
// worker's DuckDB, with the models of the pack in dir, as
// engine.OpenWorker does.
func registerForecasting(c *duckdb.Connector, dir string) error {
	db := sql.OpenDB(keepConnector{c})
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	return tsforecast.Register(conn, tsforecast.New(modelpack.Open(dir)))
}

// columns returns the names of a query's columns.
func columns(t *testing.T, conn *pgx.Conn, sql string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	defer rows.Close()
	var out []string
	for _, f := range rows.FieldDescriptions() {
		out = append(out, f.Name)
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}

// Snowflake's FORECAST and ANOMALY_DETECTION take a model of the pack as
// their method.
func TestForecastWithFoundationModel(t *testing.T) {
	t.Parallel()
	cs := startForecastServer(t)
	mustExec(t, cs.admin, `CREATE TABLE lake.public.new_spend AS
		SELECT TIMESTAMP '2025-01-01' + to_days(d::INTEGER) AS day,
			100 * (1 + 0.2 * sin(2 * pi() * d / 7) + 0.001 * d) + CASE WHEN d IN (183, 187) THEN 60 ELSE 0 END AS spend
		FROM range(180, 190) t(d)`)
	conn := snowflakeConn(t, cs.admin)
	mustExec(t, conn, `CREATE SNOWFLAKE.ML.FORECAST spend_forecast(
		INPUT_DATA => TABLE(daily_spend), SERIES_COLNAME => 'campaign', TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend',
		CONFIG_OBJECT => {'method': 'Test_Bolt', 'evaluation_config': {'n_splits': 2}})`)
	if got := texts(t, conn, "CALL spend_forecast!FORECAST(FORECASTING_PERIODS => 7)"); len(got) != 14 ||
		!strings.HasPrefix(got[0], "c1|2025-06-30 00:00:00") || !strings.HasPrefix(got[13], "c2|2025-07-06 00:00:00") {
		t.Errorf("FORECAST: %q", got)
	}
	if n := count(t, conn, `SELECT count(*) FROM TABLE(spend_forecast!FORECAST(FORECASTING_PERIODS => 3, SERIES_VALUE => 'c2',
		CONFIG_OBJECT => {'prediction_interval': 0.8})) WHERE series = 'c2' AND lower_bound <= forecast AND forecast <= upper_bound`); n != 3 {
		t.Errorf("%d of c2's 3 forecasts within their interval", n)
	}
	if n := count(t, conn, `SELECT count(*) FROM TABLE(spend_forecast!SHOW_EVALUATION_METRICS())
		WHERE error_metric = 'MAE' AND metric_value >= 0`); n != 2 {
		t.Errorf("%d series' MAE", n)
	}
	if got := texts(t, conn, "CALL spend_forecast!EXPLAIN_FEATURE_IMPORTANCE()"); len(got) != 0 {
		t.Errorf("a foundation model's feature importance: %q", got)
	}
	// A saved model forecasts again, from the history it keeps.
	mustExec(t, conn, "CREATE TABLE spend_next AS SELECT * FROM TABLE(spend_forecast!FORECAST(FORECASTING_PERIODS => 2))")
	if n := count(t, conn, "SELECT count(*) FROM spend_next"); n != 4 {
		t.Errorf("%d rows saved", n)
	}

	mustExec(t, conn, `CREATE SNOWFLAKE.ML.ANOMALY_DETECTION spend_monitor(
		INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT day, spend FROM daily_spend WHERE campaign = ''c1'''),
		TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend', LABEL_COLNAME => '', CONFIG_OBJECT => {'method': 'test-bolt'})`)
	if n := count(t, conn, `SELECT count(*) FROM TABLE(spend_monitor!DETECT_ANOMALIES(INPUT_DATA => TABLE(new_spend),
		TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend')) WHERE percentile BETWEEN 0 AND 1 AND lower_bound <= upper_bound`); n != 10 {
		t.Errorf("%d of 10 points scored", n)
	}

	for sql, want := range map[string][2]string{
		`CREATE SNOWFLAKE.ML.FORECAST f2(INPUT_DATA => TABLE(daily_spend), SERIES_COLNAME => 'campaign', TIMESTAMP_COLNAME => 'day',
			TARGET_COLNAME => 'spend', CONFIG_OBJECT => {'method': 'chronos-2'})`: {"55000", "igloo models install chronos-2"},
		`CREATE SNOWFLAKE.ML.FORECAST f2(INPUT_DATA => TABLE(daily_spend), SERIES_COLNAME => 'campaign', TIMESTAMP_COLNAME => 'day',
			TARGET_COLNAME => 'spend', CONFIG_OBJECT => {'method': 'TimesFM 3.0'})`: {"0A000", "non-commercial"},
		`CREATE SNOWFLAKE.ML.FORECAST f2(INPUT_DATA => TABLE(daily_spend), SERIES_COLNAME => 'campaign', TIMESTAMP_COLNAME => 'day',
			TARGET_COLNAME => 'spend', CONFIG_OBJECT => {'method': 'prophet'})`: {"42601", "'best' or 'fast'"},
		`CREATE SNOWFLAKE.ML.FORECAST f2(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT day, spend, hash(day) % 7 AS promo FROM daily_spend WHERE campaign = ''c1'''),
			TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend', CONFIG_OBJECT => {'method': 'test-bolt'})`: {"22023", "from the target alone"},
	} {
		expectCode(t, conn, sql, want[0], want[1])
	}
}

// BigQuery's AI.FORECAST, in DuckDB's dialect and Snowflake's.
func TestAIForecast(t *testing.T) {
	t.Parallel()
	cs := startForecastServer(t)
	conn := cs.admin
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	q := `SELECT * FROM AI.FORECAST(TABLE lake.public.daily_spend, data_col => 'spend', timestamp_col => 'day',
		model => 'test-bolt', id_cols => ['campaign'], horizon => 5, confidence_level => 0.8) ORDER BY campaign, forecast_timestamp`
	if got := strings.Join(columns(t, conn, q), ","); got != "campaign,forecast_timestamp,forecast_value,confidence_level,"+
		"prediction_interval_lower_bound,prediction_interval_upper_bound,ai_forecast_status" {
		t.Errorf("columns %s", got)
	}
	got := texts(t, conn, "SELECT campaign, forecast_timestamp::VARCHAR, confidence_level, ai_forecast_status FROM ("+q+") AS f")
	if len(got) != 10 || got[0] != "c1|2025-06-30 00:00:00|0.8|" || got[9] != "c2|2025-07-04 00:00:00|0.8|" {
		t.Errorf("forecasts %q", got)
	}
	if n := count(t, conn, "SELECT count(*) FROM ("+q+`) AS f
		WHERE prediction_interval_lower_bound <= forecast_value AND forecast_value <= prediction_interval_upper_bound`); n != 10 {
		t.Errorf("%d forecasts within their interval", n)
	}
	// A query, to an end; with the history.
	if n := count(t, conn, `SELECT count(*) FROM AI.FORECAST((SELECT day, spend FROM daily_spend WHERE campaign = 'c1'),
		data_col => 'spend', timestamp_col => 'day', model => 'test-bolt', forecast_end_timestamp => '2025-07-09')`); n != 10 {
		t.Errorf("%d forecasts to the end", n)
	}
	if got := texts(t, conn, `SELECT time_series_type, count(*) FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend',
		timestamp_col => 'day', id_cols => ['campaign'], model => 'test-bolt', horizon => 3, output_historical_time_series => TRUE)
		GROUP BY ALL ORDER BY 1`); strings.Join(got, ";") != "forecast|6;history|360" {
		t.Errorf("with history: %q", got)
	}
	// A series too short to forecast says so, and the others forecast.
	if got := texts(t, conn, `SELECT campaign, ai_forecast_status FROM AI.FORECAST(
		(SELECT * FROM daily_spend WHERE campaign = 'c1' OR day < TIMESTAMP '2025-01-03'), data_col => 'spend', timestamp_col => 'day',
		id_cols => ['campaign'], model => 'test-bolt', horizon => 2) WHERE ai_forecast_status <> '' ORDER BY 1`); len(got) != 1 ||
		!strings.HasPrefix(got[0], "c2|") {
		t.Errorf("statuses %q", got)
	}
	// Snowflake's dialect has it too.
	sf := snowflakeConn(t, cs.user(t, "ana", auth.RoleReader, pgx.QueryExecModeSimpleProtocol))
	if n := count(t, sf, `SELECT count(*) FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day',
		model => 'test-bolt', id_cols => ['campaign'], horizon => 4)`); n != 8 {
		t.Errorf("%d forecasts in Snowflake's dialect", n)
	}

	for sql, want := range map[string][2]string{
		// The default, TimesFM 2.5, is not installed.
		`SELECT * FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day')`: {
			"55000", "igloo models install timesfm-2.5"},
		`SELECT * FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day', model => 'TimesFM 3.0')`: {
			"0A000", "non-commercial"},
		`SELECT * FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day', model => 'prophet')`: {
			"42704", "not a forecasting model"},
		`SELECT * FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day', model => 'test-bolt',
			horizon => 3, forecast_end_timestamp => '2025-07-09')`: {"42601", "not both"},
		`SELECT * FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day', model => 'test-bolt',
			confidence_level => 1.5)`: {"22023", "between 0 and 1"},
		`SELECT * FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day', model => 'test-bolt', season => 7)`: {
			"42601", "not season"},
		`CREATE VIEW spend_ahead AS SELECT * FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day',
			model => 'test-bolt')`: {"0A000", "cannot forecast"},
	} {
		expectCode(t, conn, sql, want[0], want[1])
	}
}

// BigQuery's AI.DETECT_ANOMALIES and Databricks' ai_forecast and
// ai_detect_anomalies.
func TestDetectAnomaliesAndDatabricksForecast(t *testing.T) {
	t.Parallel()
	cs := startForecastServer(t)
	conn := cs.admin
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	mustExec(t, conn, `CREATE TABLE lake.public.new_spend AS
		SELECT TIMESTAMP '2025-01-01' + to_days(d::INTEGER) AS day, c AS campaign,
			(CASE c WHEN 'c1' THEN 100 ELSE 1000 END) * (1 + 0.2 * sin(2 * pi() * d / 7) + 0.001 * d)
				* CASE WHEN d = 185 THEN 3 ELSE 1 END AS spend
		FROM range(180, 190) t(d), (VALUES ('c1'), ('c2')) s(c)`)

	q := `SELECT * FROM AI.DETECT_ANOMALIES(TABLE daily_spend, TABLE new_spend, data_col => 'spend', timestamp_col => 'day',
		id_cols => ['campaign'], model => 'test-bolt', anomaly_prob_threshold => 0.9)`
	if got := strings.Join(columns(t, conn, q), ","); got != "campaign,time_series_timestamp,time_series_data,is_anomaly,lower_bound,"+
		"upper_bound,anomaly_probability,ai_detect_anomalies_status" {
		t.Errorf("columns %s", got)
	}
	if n := count(t, conn, "SELECT count(*) FROM ("+q+`) AS d WHERE anomaly_probability BETWEEN 0 AND 1
		AND is_anomaly = (anomaly_probability > 0.9) AND lower_bound <= upper_bound`); n != 20 {
		t.Errorf("%d of 20 target points scored", n)
	}
	// The tripled day is the most anomalous of each campaign's.
	if got := texts(t, conn, "SELECT DISTINCT campaign, (arg_max(time_series_timestamp, anomaly_probability) OVER (PARTITION BY campaign))::VARCHAR FROM ("+
		q+") AS d ORDER BY 1"); strings.Join(got, ";") != "c1|2025-07-05 00:00:00;c2|2025-07-05 00:00:00" {
		t.Errorf("most anomalous %q", got)
	}
	if n := count(t, conn, `SELECT count(*) FROM AI.DETECT_ANOMALIES(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day',
		id_cols => ['campaign'], model => 'test-bolt', target_last_n_points => 10) WHERE anomaly_probability IS NOT NULL`); n != 20 {
		t.Errorf("%d of the last 20 points scored", n)
	}
	expectCode(t, conn, `SELECT * FROM AI.DETECT_ANOMALIES(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day',
		model => 'test-bolt')`, "42601", "one of a target table")

	// Databricks: horizon is the first time not forecast.
	f := `SELECT * FROM ai_forecast(observed => TABLE(daily_spend), horizon => '2025-07-05', time_col => 'day', value_col => 'spend',
		group_col => 'campaign', prediction_interval_width => 0.9, model => 'test-bolt', version => '2')`
	if got := strings.Join(columns(t, conn, f), ","); got != "day,campaign,spend_forecast,spend_upper,spend_lower" {
		t.Errorf("columns %s", got)
	}
	if got := texts(t, conn, "SELECT campaign, count(*), max(day)::VARCHAR FROM ("+f+`) AS f
		WHERE spend_lower <= spend_forecast AND spend_forecast <= spend_upper GROUP BY 1 ORDER BY 1`); strings.Join(got, ";") !=
		"c1|5|2025-07-04 00:00:00;c2|5|2025-07-04 00:00:00" {
		t.Errorf("ai_forecast %q", got)
	}
	// A query, and two value columns.
	if got := texts(t, conn, `SELECT count(*), count(spend_forecast), count(twice_lower) FROM ai_forecast(
		observed => TABLE(SELECT day, spend, 2 * spend AS twice FROM daily_spend WHERE campaign = 'c1'), horizon => '2025-07-02',
		time_col => 'day', value_col => ['spend', 'twice'], model => 'test-bolt')`); strings.Join(got, ";") != "2|2|2" {
		t.Errorf("two values: %q", got)
	}
	d := `SELECT * FROM ai_detect_anomalies(observed => TABLE(new_spend), time_col => 'day', value_col => 'spend',
		group_col => 'campaign', model => 'test-bolt')`
	if got := strings.Join(columns(t, conn, d), ","); got != "day,campaign,spend,expected,lower_bound,upper_bound,anomaly_score,is_anomaly" {
		t.Errorf("columns %s", got)
	}
	// The first 8 points of each campaign are its warmup, unscored.
	if got := texts(t, conn, "SELECT campaign, count(*), count(anomaly_score) FROM ("+d+") AS d GROUP BY 1 ORDER BY 1"); strings.Join(got, ";") !=
		"c1|10|2;c2|10|2" {
		t.Errorf("ai_detect_anomalies %q", got)
	}
	for sql, want := range map[string][2]string{
		`SELECT * FROM ai_forecast(observed => TABLE(daily_spend), horizon => '2025-07-05', time_col => 'day', value_col => 'spend',
			model => 'test-bolt', parameters => '{"global_floor": 0}')`: {"0A000", "parameters"},
		`SELECT * FROM ai_forecast(observed => TABLE(daily_spend), horizon => '2025-07-05', time_col => 'day', value_col => 'spend')`: {
			"55000", "igloo models install chronos-2"},
		`SELECT * FROM ai_forecast(observed => TABLE(daily_spend), horizon => '2025-07-05', time_col => 'day', value_col => 'spend',
			model => 'test-bolt', version => '3')`: {"22023", "'1' or '2'"},
		`SELECT * FROM ai_detect_anomalies(observed => TABLE(daily_spend), time_col => 'day', value_col => 'spend',
			covariate_col => 'campaign', model => 'test-bolt')`: {"0A000", "covariate_col"},
	} {
		expectCode(t, conn, sql, want[0], want[1])
	}
}

// Forecasts read the input as the user would: grants and row access
// policies apply.
func TestForecastReadsAsTheUser(t *testing.T) {
	t.Parallel()
	cs := startForecastServer(t)
	for _, sql := range policyFixture {
		mustExec(t, cs.admin, sql)
	}
	mustExec(t, cs.admin, `CREATE TABLE lake.crm.client_spend AS
		SELECT client, TIMESTAMP '2025-01-01' + to_days(d::INTEGER) AS day, (d % 7) + CASE client WHEN 'acme' THEN 10 ELSE 50 END AS spend
		FROM range(0, 60) t(d), (VALUES ('acme'), ('globex')) s(client)`)
	mustExec(t, cs.admin, "ALTER TABLE lake.crm.client_spend ADD ROW ACCESS POLICY client_rows ON (client)")
	ann := cs.user(t, "ann", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO ann")
	q := `SELECT DISTINCT client FROM AI.FORECAST(TABLE lake.crm.client_spend, data_col => 'spend', timestamp_col => 'day',
		id_cols => ['client'], model => 'test-bolt', horizon => 2) ORDER BY 1`
	if got := texts(t, cs.admin, q); strings.Join(got, ",") != "acme,globex" {
		t.Errorf("admin forecasts %q", got)
	}
	if got := texts(t, ann, q); strings.Join(got, ",") != "acme" {
		t.Errorf("ann forecasts %q", got)
	}
	expectDenied(t, nina, q, "client_spend")
	expectDenied(t, nina, `SELECT * FROM ai_forecast(observed => TABLE(lake.crm.client_spend), horizon => '2025-03-05', time_col => 'day',
		value_col => 'spend', model => 'test-bolt')`, "client_spend")
}

// Workers forecast the series of their partitions.
func TestDistributedForecast(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tsforecasttest.InstallTiny(t, filepath.Join(dir, "model-pack"), "test-bolt")
	ds := startDistServerIn(t, dir, 2)
	mustExec(t, ds.admin, dailySpend)
	conn := ds.admin
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	q := `SELECT campaign, forecast_timestamp, round(forecast_value, 3) FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend',
		timestamp_col => 'day', id_cols => ['campaign'], model => 'test-bolt', horizon => 4) ORDER BY 1, 2`
	mustExec(t, conn, "SET distributed_execution = off")
	local := texts(t, conn, q)
	if q := newestRun(t, ds.history, "igloo_ts_forecast("); q.Workers != 0 {
		t.Errorf("with distributed_execution off, %d workers forecast", q.Workers)
	}
	mustExec(t, conn, "SET distributed_execution = on")
	if got := texts(t, conn, q); strings.Join(got, ";") != strings.Join(local, ";") || len(got) != 8 {
		t.Errorf("on workers: %q; here: %q", got, local)
	}
	run := newestRun(t, ds.history, "igloo_ts_forecast(")
	if run.Workers == 0 {
		t.Errorf("no worker forecast: %q", texts(t, conn, "EXPLAIN DISTRIBUTED "+run.Text))
	}
	// The forecasts are made by a stage that holds whole series, not by
	// this replica once every series reached it.
	plan := texts(t, conn, "EXPLAIN DISTRIBUTED "+run.Text)
	stages := 0
	for _, row := range plan {
		if strings.HasPrefix(row, "stage ") && strings.Contains(row, "igloo_ts_forecast(") {
			stages++
		}
	}
	if stages != 1 {
		t.Errorf("%d stages forecast: %q", stages, plan)
	}
}
