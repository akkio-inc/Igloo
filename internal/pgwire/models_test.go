package pgwire

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/models"
)

// leadsTable is a synthetic table of leads, of whom those in the south or
// with spend above 60 converted: a separable classification set.
const leadsTable = `CREATE TABLE lake.public.leads AS
	SELECT i AS person_id,
		['north', 'south', 'east', 'west'][1 + i % 4] AS region,
		((i * 7919) % 1000) / 10.0 AS spend,
		(i * 31) % 17 AS visits,
		((i * 7919) % 1000) / 10.0 > 60 OR i % 4 = 1 AS converted
	FROM range(1, 1201) t(i)`

// snowflakeConn returns conn in Snowflake's dialect, reading the stand-in
// catalog's public schema.
func snowflakeConn(t *testing.T, conn *pgx.Conn) *pgx.Conn {
	t.Helper()
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	mustExec(t, conn, "SET sql_dialect = 'snowflake'")
	return conn
}

func TestClassificationModel(t *testing.T) {
	t.Parallel()
	store := models.NewMemoryStore()
	cs := startCatalogServer(t, func(o *Options) { o.Models = store })
	mustExec(t, cs.admin, leadsTable)
	conn := snowflakeConn(t, cs.admin)
	create := `CREATE SNOWFLAKE.ML.CLASSIFICATION lead_model(
		INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT region, spend, visits, converted FROM leads'),
		TARGET_COLNAME => 'converted') COMMENT = 'who converts'`
	mustExec(t, conn, create)
	m, err := store.Get(context.Background(), "public", "lead_model")
	if err != nil || m.Class != "CLASSIFICATION" || m.Owner != "igloo" || m.Comment != "who converts" || len(m.Blob) != 64 {
		t.Fatalf("stored %+v, %v", m, err)
	}

	// PREDICT scores rows of the table, with every column given.
	if n := count(t, conn, `SELECT count(*) FROM leads
		WHERE (lead_model!PREDICT(INPUT_DATA => {*}):class::STRING = 'True') = converted`); n < 1180 {
		t.Errorf("%d of 1200 rows predicted right", n)
	}
	rows := texts(t, conn, `SELECT TO_JSON(lead_model!PREDICT(INPUT_DATA => {'region': 'south', 'spend': 5, 'visits': 1})) AS p`)
	if len(rows) != 1 || !strings.HasPrefix(rows[0], `{"class":"True","logs":null,"probability":{"False":`) {
		t.Errorf("PREDICT returned %q", rows)
	}
	// A probability of each class, as an object.
	if n := count(t, conn, `SELECT count(*) FROM leads
		WHERE ABS(lead_model!PREDICT(INPUT_DATA => {*}):probability:True::FLOAT +
		          lead_model!PREDICT(INPUT_DATA => {*}):probability:False::FLOAT - 1) < 1e-9`); n != 1200 {
		t.Errorf("probabilities add up to 1 in %d rows", n)
	}

	// The SHOW methods, by CALL and as tables.
	if got := texts(t, conn, "CALL lead_model!SHOW_FEATURE_IMPORTANCE()"); len(got) != 3 ||
		!strings.HasPrefix(got[0], "1|spend|") && !strings.HasPrefix(got[0], "1|region|") || !strings.HasSuffix(got[0], "|user_provided") {
		t.Errorf("SHOW_FEATURE_IMPORTANCE: %q", got)
	}
	if got := texts(t, conn, "SELECT feature FROM TABLE(lead_model!SHOW_FEATURE_IMPORTANCE()) WHERE rank = 3"); len(got) != 1 || got[0] != "visits" {
		t.Errorf("the least important feature: %q", got)
	}
	metrics := texts(t, conn, "CALL lead_model!SHOW_EVALUATION_METRICS()")
	if len(metrics) != 8 || !strings.HasPrefix(metrics[0], "EVAL|False|precision|") {
		t.Errorf("SHOW_EVALUATION_METRICS: %q", metrics)
	}
	if n := count(t, conn, `SELECT count(*) FROM TABLE(lead_model!SHOW_GLOBAL_EVALUATION_METRICS())
		WHERE error_metric = 'auc' AND metric_value > 0.98`); n != 2 {
		t.Errorf("%d AUCs above 0.98, want macro and weighted", n)
	}
	if got := texts(t, conn, `SELECT average_type, error_metric FROM TABLE(lead_model!SHOW_GLOBAL_EVALUATION_METRICS())
		WHERE average_type IS NULL`); len(got) != 1 || got[0] != "NULL|log_loss" {
		t.Errorf("log loss row: %q", got)
	}
	if n := count(t, conn, "SELECT SUM(count) FROM TABLE(lead_model!SHOW_CONFUSION_MATRIX())"); n < 100 || n > 260 {
		t.Errorf("the confusion matrix counts %d held-out rows", n)
	}
	if n := count(t, conn, "SELECT count(*) FROM TABLE(lead_model!SHOW_THRESHOLD_METRICS())"); n != 202 {
		t.Errorf("%d threshold rows, want 101 per class", n)
	}
	if got := texts(t, conn, "CALL lead_model!SHOW_TRAINING_LOGS()"); len(got) != 1 || got[0] != "NULL" {
		t.Errorf("training logs %q", got)
	}
	// Snowflake's dialect names the columns in upper case.
	r, err := conn.Query(context.Background(), "CALL lead_model!SHOW_CONFUSION_MATRIX()")
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, f := range r.FieldDescriptions() {
		names = append(names, f.Name)
	}
	r.Close()
	if strings.Join(names, ",") != "DATASET_TYPE,ACTUAL_CLASS,PREDICTED_CLASS,COUNT,LOGS" {
		t.Errorf("columns %v", names)
	}

	// SHOW lists it; the same data trains the same model.
	if got := texts(t, conn, "SHOW SNOWFLAKE.ML.CLASSIFICATION LIKE 'lead%'"); len(got) != 1 || !strings.Contains(got[0], "|lead_model|lake|public|1|who converts|igloo") {
		t.Errorf("SHOW: %q", got)
	}
	expectCode(t, conn, create, "42710", "already exists")
	mustExec(t, conn, strings.Replace(create, "CLASSIFICATION lead_model", "CLASSIFICATION IF NOT EXISTS lead_model", 1))
	mustExec(t, conn, strings.Replace(create, "CREATE", "CREATE OR REPLACE", 1))
	if again, _ := store.Get(context.Background(), "public", "lead_model"); again.Blob != m.Blob {
		t.Errorf("retraining on the same rows made blob %s, then %s", m.Blob, again.Blob)
	}
	// Rows in another order make the same model.
	mustExec(t, cs.admin, "SET sql_dialect = 'duckdb'")
	mustExec(t, cs.admin, "CREATE TABLE lake.public.leads_shuffled AS SELECT * FROM lake.public.leads ORDER BY hash(person_id)")
	mustExec(t, conn, "SET sql_dialect = 'snowflake'")
	mustExec(t, conn, `CREATE SNOWFLAKE.ML.CLASSIFICATION shuffled_model(
		INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT region, spend, visits, converted FROM leads_shuffled'),
		TARGET_COLNAME => 'converted')`)
	if s, _ := store.Get(context.Background(), "public", "shuffled_model"); s.Blob != m.Blob {
		t.Errorf("the same rows in another order made blob %s, not %s", s.Blob, m.Blob)
	}
	// A table of scores, as dbt or a task makes one.
	mustExec(t, conn, `CREATE TABLE lead_scores AS
		SELECT person_id, lead_model!PREDICT(INPUT_DATA => {*}):probability:True::FLOAT AS p FROM leads`)
	if n := count(t, conn, "SELECT count(*) FROM lead_scores WHERE p BETWEEN 0 AND 1"); n != 1200 {
		t.Errorf("%d scores", n)
	}

	// Dropped, it is gone, and so is its blob once no model uses it.
	mustExec(t, conn, "DROP SNOWFLAKE.ML.CLASSIFICATION lead_model")
	blobs := cs.srv.modelRuntime.Blobs()
	if _, err := blobs.Get(context.Background(), m.Blob); err != nil {
		t.Errorf("the blob shuffled_model uses is gone: %v", err)
	}
	mustExec(t, conn, "DROP SNOWFLAKE.ML.CLASSIFICATION shuffled_model")
	if _, err := blobs.Get(context.Background(), m.Blob); err == nil {
		t.Error("a blob no model uses is kept")
	}
	expectCode(t, conn, "SELECT lead_model!PREDICT(INPUT_DATA => {*}) FROM leads", "42704", "does not exist")
	mustExec(t, conn, "DROP SNOWFLAKE.ML.CLASSIFICATION IF EXISTS lead_model")
	if got := texts(t, conn, "SHOW SNOWFLAKE.ML.CLASSIFICATION"); len(got) != 0 {
		t.Errorf("SHOW after DROP: %q", got)
	}
}

// dailySpend is two campaigns' daily spend over 180 days: a weekly cycle,
// a slight trend and a little noise, around 100 and 1000.
const dailySpend = `CREATE TABLE lake.public.daily_spend AS
	SELECT TIMESTAMP '2025-01-01' + to_days(d::INTEGER) AS day, c AS campaign,
		(CASE c WHEN 'c1' THEN 100 ELSE 1000 END) * (1 + 0.2 * sin(2 * pi() * d / 7) + 0.001 * d)
			+ ((hash(d, c) % 100) / 100.0 - 0.5) AS spend
	FROM range(0, 180) t(d), (VALUES ('c1'), ('c2')) s(c)`

func TestForecastModels(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	mustExec(t, cs.admin, dailySpend)
	// Ten new days of c1, two of them far off.
	mustExec(t, cs.admin, `CREATE TABLE lake.public.new_spend AS
		SELECT TIMESTAMP '2025-01-01' + to_days(d::INTEGER) AS day,
			100 * (1 + 0.2 * sin(2 * pi() * d / 7) + 0.001 * d) + CASE WHEN d IN (183, 187) THEN 60 ELSE 0 END AS spend
		FROM range(180, 190) t(d)`)
	conn := snowflakeConn(t, cs.admin)
	mustExec(t, conn, `CREATE SNOWFLAKE.ML.FORECAST spend_forecast(
		INPUT_DATA => TABLE(daily_spend), SERIES_COLNAME => 'campaign', TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend',
		CONFIG_OBJECT => {'method': 'fast', 'evaluation_config': {'n_splits': 2}})`)
	if got := texts(t, conn, "CALL spend_forecast!FORECAST(FORECASTING_PERIODS => 7)"); len(got) != 14 ||
		!strings.HasPrefix(got[0], "c1|2025-06-30 00:00:00") {
		t.Errorf("FORECAST: %q", got)
	}
	// As a table, one series, saved.
	if n := count(t, conn, `SELECT count(*) FROM TABLE(spend_forecast!FORECAST(FORECASTING_PERIODS => 3, SERIES_VALUE => 'c2'))
		WHERE series = 'c2' AND forecast BETWEEN 0.75 * 1180 AND 1.25 * 1180 AND lower_bound < forecast AND upper_bound > forecast`); n != 3 {
		t.Errorf("%d of c2's 3 forecasts near 1180", n)
	}
	mustExec(t, conn, "CREATE TABLE spend_next_week AS SELECT * FROM TABLE(spend_forecast!FORECAST(FORECASTING_PERIODS => 7))")
	if n := count(t, conn, "SELECT count(*) FROM spend_next_week WHERE ts > '2025-06-29'"); n != 14 {
		t.Errorf("%d rows saved", n)
	}
	// A task forecasts with the model as it is when it runs. It runs in a
	// session of its owner's, in the default schema.
	mustExec(t, conn, `CREATE TASK forecast_spend SCHEDULE '1 day' AS
		CREATE OR REPLACE TABLE spend_forecasts AS SELECT * FROM TABLE(public.spend_forecast!FORECAST(FORECASTING_PERIODS => 2))`)
	mustExec(t, conn, "SET sql_dialect = 'duckdb'")
	mustExec(t, conn, "EXECUTE TASK forecast_spend")
	mustExec(t, conn, "SET sql_dialect = 'snowflake'")
	if n := count(t, conn, "SELECT count(*) FROM spend_forecasts"); n != 4 {
		t.Errorf("the task saved %d forecasts", n)
	}
	if got := texts(t, conn, "SELECT DISTINCT error_metric FROM TABLE(spend_forecast!SHOW_EVALUATION_METRICS()) ORDER BY 1"); strings.Join(got, ",") !=
		"COVERAGE_INTERVAL=0.95,MAE,MAPE,MDA,MSE,SMAPE,WINKLER_ALPHA=0.05" {
		t.Errorf("metrics %q", got)
	}
	if n := count(t, conn, `SELECT count(*) FROM TABLE(spend_forecast!SHOW_EVALUATION_METRICS())
		WHERE error_metric = 'MAPE' AND metric_value < 0.1 AND standard_deviation IS NOT NULL`); n != 2 {
		t.Errorf("%d series with MAPE under 10%%", n)
	}
	if got := texts(t, conn, "SELECT DISTINCT feature_type FROM TABLE(spend_forecast!EXPLAIN_FEATURE_IMPORTANCE()) ORDER BY 1"); strings.Join(got, ",") !=
		"derived_from_endogenous,derived_from_timestamp" {
		t.Errorf("feature types %q", got)
	}
	// Anomalies of one series, against new days.
	mustExec(t, conn, `CREATE SNOWFLAKE.ML.ANOMALY_DETECTION spend_monitor(
		INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT day, spend FROM daily_spend WHERE campaign = ''c1'''),
		TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend', LABEL_COLNAME => '')`)
	got := texts(t, conn, `SELECT ts::DATE::STRING, is_anomaly FROM TABLE(spend_monitor!DETECT_ANOMALIES(
		INPUT_DATA => TABLE(new_spend), TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend')) WHERE is_anomaly ORDER BY ts`)
	if strings.Join(got, ";") != "2025-07-03|true;2025-07-07|true" {
		t.Errorf("anomalies %q", got)
	}
	if got := texts(t, conn, `CALL spend_monitor!DETECT_ANOMALIES(INPUT_DATA => TABLE(new_spend), TIMESTAMP_COLNAME => 'day',
		TARGET_COLNAME => 'spend', CONFIG_OBJECT => {'prediction_interval': 0.5})`); len(got) != 10 {
		t.Errorf("DETECT_ANOMALIES: %q", got)
	}
	for sql, want := range map[string][2]string{
		`CALL spend_forecast!FORECAST(FORECASTING_PERIODS => 7, SERIES_VALUE => 'c9')`: {"22023", "no series c9"},
		`CALL spend_forecast!DETECT_ANOMALIES(INPUT_DATA => TABLE(new_spend), TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend')`: {
			"42883", "has no method DETECT_ANOMALIES"},
		`CALL spend_monitor!DETECT_ANOMALIES(INPUT_DATA => TABLE(daily_spend), TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend',
			SERIES_COLNAME => 'campaign')`: {"22023", "trained on one series"},
		`CALL spend_monitor!DETECT_ANOMALIES(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT day, spend FROM daily_spend WHERE campaign = ''c1'''),
			TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend')`: {"22023", "after the training data"},
		`CREATE SNOWFLAKE.ML.FORECAST f2(INPUT_DATA => TABLE(daily_spend), TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'campaign')`: {
			"22023", "must be a number"},
		`CREATE SNOWFLAKE.ML.FORECAST f2(INPUT_DATA => TABLE(daily_spend), TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend',
			CONFIG_OBJECT => {'method': 'slow'})`: {"42601", "'best' or 'fast'"},
		`CREATE SNOWFLAKE.ML.FORECAST f2(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT day, spend FROM daily_spend LIMIT 1'),
			TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend')`: {"22023", "two unique timestamps"},
	} {
		expectCode(t, conn, sql, want[0], want[1])
	}
}

func TestTopInsightsAndTrainAndScore(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	// Spend by segment in two periods: US mobile tripled.
	mustExec(t, cs.admin, `CREATE TABLE lake.public.spend_by_segment AS
		SELECT ['us', 'uk', 'de'][1 + i % 3] AS country, ['mobile', 'desktop'][1 + (i // 3) % 2] AS device, p AS is_test,
			(10 + i % 7) * CASE WHEN p AND i % 3 = 0 AND (i // 3) % 2 = 0 THEN 3 ELSE 1 END AS spend
		FROM range(0, 600) t(i), (VALUES (false), (true)) v(p)`)
	// Leads, a tenth of them not yet known to convert or not.
	mustExec(t, cs.admin, leadsTable)
	mustExec(t, cs.admin, `CREATE TABLE lake.public.open_leads AS
		SELECT person_id, region, spend, CASE WHEN person_id % 10 = 0 THEN NULL ELSE converted END AS converted, converted AS truth,
			CASE WHEN person_id % 10 = 0 THEN NULL ELSE 2 * spend + CASE region WHEN 'south' THEN 50 ELSE 0 END END AS value,
			2 * spend + CASE region WHEN 'south' THEN 50 ELSE 0 END AS true_value
		FROM lake.public.leads`)
	conn := snowflakeConn(t, cs.admin)
	mustExec(t, conn, "CREATE SNOWFLAKE.ML.TOP_INSIGHTS spend_drivers()")
	drivers := `spend_drivers!GET_DRIVERS(INPUT_DATA => TABLE(spend_by_segment), LABEL_COLNAME => 'is_test', METRIC_COLNAME => 'spend')`
	got := texts(t, conn, "SELECT contributor, contribution, relative_contribution FROM TABLE("+drivers+")")
	if len(got) < 3 || !strings.HasPrefix(got[0], `["Overall"]|`) || !strings.HasSuffix(got[0], "|1") {
		t.Fatalf("drivers %q", got)
	}
	found := false
	for _, row := range got {
		if strings.HasPrefix(row, `["COUNTRY = us","DEVICE = mobile"]|`) && strings.HasSuffix(row, "|1") {
			found = true
		}
	}
	if !found {
		t.Errorf("no driver US mobile with all the change: %q", got)
	}
	if rows := texts(t, conn, "CALL "+drivers); len(rows) != len(got) {
		t.Errorf("CALL GET_DRIVERS: %d rows, the table %d", len(rows), len(got))
	}

	// Train and score in one call, in either dialect; no model is kept.
	scored := `SELECT count(*) FROM ai_predict_class(input => TABLE(open_leads), target_col => 'converted',
		feature_cols => %s, seed => 7) WHERE converted_prediction = truth AND converted_confidence > 0.5`
	if n := count(t, conn, strings.Replace(scored, "%s", "ARRAY_CONSTRUCT('region', 'spend')", 1)); n < 115 {
		t.Errorf("Snowflake's dialect: %d of 120 open leads predicted right", n)
	}
	mustExec(t, conn, "SET sql_dialect = 'duckdb'")
	if n := count(t, conn, strings.Replace(scored, "%s", "['region', 'spend']", 1)); n < 115 {
		t.Errorf("DuckDB's dialect: %d of 120 open leads predicted right", n)
	}
	if n := count(t, conn, `SELECT count(*) FROM ai_predict_value(input => TABLE(open_leads), target_col => 'value',
		feature_cols => ['region', 'spend']) WHERE abs(value_prediction - true_value) < 10`); n < 110 {
		t.Errorf("%d of 120 values predicted within 10", n)
	}
	if got := texts(t, conn, "SHOW SNOWFLAKE.ML.CLASSIFICATION"); len(got) != 0 {
		t.Errorf("a model was kept: %q", got)
	}
	for sql, want := range map[string][2]string{
		`SELECT * FROM ai_predict_class(input => TABLE(open_leads), target_col => 'converted', feature_cols => [])`: {"42601",
			"non-empty array"},
		`SELECT * FROM ai_predict_value(input => TABLE(open_leads), target_col => 'region', feature_cols => ['spend'])`: {"22023",
			"predicts numbers"},
		`SELECT * FROM igloo_ml_score('x', 1)`: {"42501", "Igloo's own"},
	} {
		expectCode(t, conn, sql, want[0], want[1])
	}
}

func TestModelRefusals(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	mustExec(t, cs.admin, leadsTable)
	conn := snowflakeConn(t, cs.admin)
	mustExec(t, conn, `CREATE SNOWFLAKE.ML.CLASSIFICATION m(INPUT_DATA => TABLE(leads), TARGET_COLNAME => 'converted',
		CONFIG_OBJECT => {'evaluate': FALSE})`)
	for sql, want := range map[string][2]string{
		`CALL m!SHOW_EVALUATION_METRICS()`:                                     {"22023", "evaluate"},
		`CALL m!FORECAST(FORECASTING_PERIODS => 3)`:                            {"42883", "has no method FORECAST"},
		`CALL m!PREDICT(INPUT_DATA => {'region': 'south'})`:                    {"42809", "not a procedure"},
		`CALL m!PREDICT(INPUT_DATA => {*})`:                                    {"42601", "for PREDICT in a query"},
		`SELECT m!SHOW_FEATURE_IMPORTANCE()`:                                   {"0A000", "SHOW_FEATURE_IMPORTANCE in an expression"},
		`SELECT m!PREDICT(INPUT_DATA => region) FROM leads`:                    {"0A000", "INPUT_DATA other than an object"},
		`CREATE VIEW v AS SELECT m!PREDICT(INPUT_DATA => {*}) AS p FROM leads`: {"0A000", "a view cannot call"},
		`ALTER SNOWFLAKE.ML.CLASSIFICATION m SET COMMENT = 'x'`:                {"0A000", "ALTER SNOWFLAKE.ML.CLASSIFICATION"},
		`CREATE SNOWFLAKE.ML.CUSTOM_CLASSIFIER c()`:                            {"0A000", "SNOWFLAKE.ML.CUSTOM_CLASSIFIER"},
		`GRANT CREATE SNOWFLAKE.ML.CLASSIFICATION ON SCHEMA public TO ROLE r`:  {"0A000", "GRANT CREATE SNOWFLAKE.ML"},
		`CREATE SNOWFLAKE.ML.CLASSIFICATION c(TABLE(leads), 'converted')`:      {"42601", "name"},
		`CREATE SNOWFLAKE.ML.CLASSIFICATION c(INPUT_DATA => TABLE(leads))`:     {"42601", "TARGET_COLNAME"},
		`CREATE SNOWFLAKE.ML.CLASSIFICATION c(INPUT_DATA => TABLE(leads), TARGET_COLNAME => 'nope')`: {"22023",
			"not a column"},
		`CREATE SNOWFLAKE.ML.CLASSIFICATION c(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT spend, 1 AS y FROM leads'), TARGET_COLNAME => 'y')`: {
			"22023", "at least two classes"},
		`CREATE SNOWFLAKE.ML.CLASSIFICATION c(INPUT_DATA => SYSTEM$QUERY_REFERENCE('DELETE FROM leads'), TARGET_COLNAME => 'y')`: {
			"22023", "must be a query"},
		`CREATE SNOWFLAKE.ML.CLASSIFICATION c(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT spend, converted FROM leads; DROP TABLE leads'),
			TARGET_COLNAME => 'converted')`: {"22023", "one query"},
		`DROP SNOWFLAKE.ML.FORECAST m`:          {"42809", "is a SNOWFLAKE.ML.CLASSIFICATION"},
		`CALL nope!SHOW_TRAINING_LOGS()`:        {"42704", "does not exist"},
		`CALL my_procedure()`:                   {"0A000", "stored procedures"},
		`SELECT * FROM TABLE(m!SHOW_NOTHING())`: {"42883", "has no method"},
	} {
		expectCode(t, conn, sql, want[0], want[1])
	}
	// Igloo's own functions are not for users, in either dialect.
	mustExec(t, conn, "SET sql_dialect = 'duckdb'")
	expectCode(t, conn, "SELECT igloo_ml_predict('00', 1)", "42501", "Igloo's own")
	expectCode(t, conn, `SELECT * FROM "IGLOO_ML_RESULT"('x')`, "42501", "Igloo's own")
	expectCode(t, conn, "CREATE MACRO p(x) AS igloo_ml_predict('00', x)", "42501", "Igloo's own")
	// Prepared without the rewrite (as a clean room's template is), a call
	// names only what the session resolved.
	sess := &session{}
	call := "SELECT igloo_ml_predict('" + strings.Repeat("a", 64) + "', 1) FROM t"
	if err := sess.checkModelCalls(call); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("an unresolved blob: %v", err)
	}
	sess.allowModel(strings.Repeat("a", 64))
	if err := sess.checkModelCalls(call); err != nil {
		t.Errorf("a resolved blob: %v", err)
	}
	if err := sess.checkModelCalls("SELECT igloo_ml_result(x) FROM t"); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("a computed token: %v", err)
	}
	// The DuckDB dialect reads statements on models too.
	if got := texts(t, conn, "CALL m!SHOW_TRAINING_LOGS()"); len(got) != 1 {
		t.Errorf("CALL in DuckDB's dialect: %q", got)
	}
}

func TestModelGrants(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	mustExec(t, cs.admin, leadsTable)
	ana := snowflakeConn(t, cs.user(t, "ana", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol))
	bob := snowflakeConn(t, cs.user(t, "bob", auth.RoleReader, pgx.QueryExecModeSimpleProtocol))
	nina := snowflakeConn(t, cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol))
	mustExec(t, cs.admin, "CREATE ROLE analysts")
	mustExec(t, cs.admin, "GRANT analysts TO bob")

	create := `CREATE SNOWFLAKE.ML.CLASSIFICATION %s(INPUT_DATA => TABLE(lake.public.leads), TARGET_COLNAME => 'converted')`
	// Creating a model takes what creating a table does.
	expectDenied(t, bob, strings.Replace(create, "%s", "bobs", 1), "writer role or CREATE")
	mustExec(t, ana, strings.Replace(create, "%s", "anas", 1))

	predict := "SELECT count(*) FROM leads WHERE anas!PREDICT(INPUT_DATA => {*}):class = 'True'"
	metrics := "CALL anas!SHOW_EVALUATION_METRICS()"
	// Others see nothing of it.
	expectCode(t, bob, predict, "42704", "does not exist or not authorized")
	expectCode(t, bob, metrics, "42704", "does not exist or not authorized")
	if got := texts(t, bob, "SHOW SNOWFLAKE.ML.CLASSIFICATION"); len(got) != 0 {
		t.Errorf("bob sees %q", got)
	}
	expectCode(t, bob, "GRANT USAGE ON SNOWFLAKE.ML.CLASSIFICATION anas TO ROLE analysts", "42704", "does not exist")

	// MLCONSUMER predicts; only USAGE (MLADMIN) shows the rest.
	mustExec(t, ana, "GRANT SNOWFLAKE.ML.CLASSIFICATION ROLE anas!MLCONSUMER TO ROLE analysts")
	if n := count(t, bob, predict); n < 300 {
		t.Errorf("bob predicts %d conversions", n)
	}
	expectDenied(t, bob, metrics, "may only predict")
	expectDenied(t, bob, "DROP SNOWFLAKE.ML.CLASSIFICATION anas", "does not own")
	expectDenied(t, bob, "GRANT USAGE ON SNOWFLAKE.ML.CLASSIFICATION anas TO USER nina", "only the owner")
	mustExec(t, ana, "GRANT USAGE ON SNOWFLAKE.ML.CLASSIFICATION anas TO ROLE analysts")
	if got := texts(t, bob, metrics); len(got) != 8 {
		t.Errorf("bob's metrics: %q", got)
	}
	if got := texts(t, bob, "SHOW SNOWFLAKE.ML.CLASSIFICATION"); len(got) != 1 || !strings.Contains(got[0], "|anas|") {
		t.Errorf("bob sees %q", got)
	}
	// Nina has no role that reads leads: she may use a model granted to
	// her, but reads nothing through it she could not read already.
	mustExec(t, ana, "GRANT USAGE ON SNOWFLAKE.ML.CLASSIFICATION anas TO USER nina")
	if got := texts(t, nina, "SELECT anas!PREDICT(INPUT_DATA => {'region': 'south'}):class::STRING"); len(got) != 1 || got[0] != "True" {
		t.Errorf("nina's prediction: %q", got)
	}
	expectDenied(t, nina, predict, "SELECT on table public.leads")
	mustExec(t, ana, "REVOKE USAGE ON SNOWFLAKE.ML.CLASSIFICATION anas FROM ROLE analysts")
	mustExec(t, ana, "REVOKE SNOWFLAKE.ML.CLASSIFICATION ROLE anas!MLCONSUMER FROM ROLE analysts")
	expectCode(t, bob, predict, "42704", "not authorized")

	// A grant of CREATE on a schema lets a reader create models there.
	mustExec(t, cs.admin, "GRANT CREATE ON SCHEMA lake.public TO bob")
	mustExec(t, bob, strings.Replace(create, "%s", "bobs", 1))
	expectDenied(t, bob, "CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION anas(INPUT_DATA => TABLE(leads), TARGET_COLNAME => 'converted')",
		"does not own")
	mustExec(t, ana, "DROP SNOWFLAKE.ML.CLASSIFICATION anas")
}

// localBlobs keeps model blobs in a local directory, the tests' warehouse.
func localBlobs(dir string) models.Blobs {
	return models.NewWarehouse(dir, func(context.Context) (icebergio.IO, error) { return icebergio.LocalFS{}, nil })
}

// keepConnector hides a connector's Close from database/sql.
type keepConnector struct{ driver.Connector }

// registerModelScoring defines the model scoring function on a test
// worker's DuckDB, reading blobs from dir, as engine.OpenWorker does.
func registerModelScoring(c *duckdb.Connector, dir string) error {
	db := sql.OpenDB(keepConnector{c})
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	r := models.NewRuntime()
	r.SetBlobs(localBlobs(dir))
	return r.RegisterScoring(conn)
}

// newestRun returns the newest statement in the history whose text has
// part in it.
func newestRun(t *testing.T, h *audit.MemoryStore, part string) audit.Query {
	t.Helper()
	qs, err := h.Queries(context.Background(), audit.Viewer{All: true}, audit.Page{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range qs {
		if strings.Contains(q.Text, part) {
			return q
		}
	}
	t.Fatalf("no history of a statement with %q", part)
	return audit.Query{}
}

// Scoring runs on workers: they read the model's blob from the warehouse.
func TestDistributedModelScoring(t *testing.T) {
	t.Parallel()
	ds := startDistServer(t, 2)
	mustExec(t, ds.admin, leadsTable)
	conn := snowflakeConn(t, ds.admin)
	mustExec(t, conn, `CREATE SNOWFLAKE.ML.CLASSIFICATION lead_model(
		INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT region, spend, visits, converted FROM leads'), TARGET_COLNAME => 'converted')`)
	query := `SELECT region, count(*) AS n FROM leads
		WHERE lead_model!PREDICT(INPUT_DATA => {*}):class::STRING = 'True' GROUP BY region ORDER BY region`
	mustExec(t, conn, "SET distributed_execution = off")
	local := texts(t, conn, query)
	if q := newestRun(t, ds.history, "igloo_ml_predict("); q.Workers != 0 {
		t.Errorf("with distributed_execution off, %d workers scored", q.Workers)
	}
	mustExec(t, conn, "SET distributed_execution = on")
	if got := texts(t, conn, query); strings.Join(got, ";") != strings.Join(local, ";") || len(got) != 4 {
		t.Errorf("on workers: %q; here: %q", got, local)
	}
	if q := newestRun(t, ds.history, "igloo_ml_predict("); q.Workers != 2 {
		t.Errorf("%d workers scored, want 2 (%s)", q.Workers, q.Text)
	}
}

// A model trains on what its creator reads, masking policies applied.
func TestModelTrainsOnMaskedValues(t *testing.T) {
	t.Parallel()
	cs := startPolicyServer(t)
	mustExec(t, cs.admin, `CREATE TABLE lake.crm.signups AS
		SELECT i AS id, CASE WHEN i % 3 = 0 THEN 'x' || i || '@acme.com' ELSE 'x' || i || '@globex.com' END AS email,
			i % 3 = 0 AS acme, (i * 13) % 7 AS noise FROM range(1, 601) t(i)`)
	mustExec(t, cs.admin, "ALTER TABLE lake.crm.signups ALTER COLUMN email SET MASKING POLICY mask_email")
	ann := snowflakeConn(t, cs.user(t, "ann", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol))
	pia := snowflakeConn(t, cs.user(t, "pia", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol))
	mustExec(t, cs.admin, "GRANT pii_reader TO pia")
	// Emails are unique; the model learns from their domains.
	train := `CREATE SNOWFLAKE.ML.CLASSIFICATION crm.%s(
		INPUT_DATA => SYSTEM$QUERY_REFERENCE($$SELECT split_part(email, '@', 2) AS domain, noise, acme FROM lake.crm.signups$$),
		TARGET_COLNAME => 'acme')`
	mustExec(t, ann, strings.Replace(train, "%s", "ann_model", 1))
	mustExec(t, pia, strings.Replace(train, "%s", "pia_model", 1))
	auc := `SELECT metric_value FROM TABLE(crm.%s!SHOW_GLOBAL_EVALUATION_METRICS()) WHERE average_type = 'macro' AND error_metric = 'auc'`
	var a, p float64
	if err := ann.QueryRow(context.Background(), strings.Replace(auc, "%s", "ann_model", 1)).Scan(&a); err != nil {
		t.Fatal(err)
	}
	if err := pia.QueryRow(context.Background(), strings.Replace(auc, "%s", "pia_model", 1)).Scan(&p); err != nil {
		t.Fatal(err)
	}
	if a > 0.75 || p < 0.99 {
		t.Errorf("AUC %.3f from masked emails, %.3f from the emails themselves", a, p)
	}
	// Snowflake's dialect names the query's column DOMAIN, as Snowflake would.
	if got := texts(t, ann, "SELECT feature, score FROM TABLE(crm.ann_model!SHOW_FEATURE_IMPORTANCE()) WHERE feature = 'DOMAIN'"); len(got) != 1 ||
		got[0] != "DOMAIN|0" {
		t.Errorf("a masked feature's importance: %q", got)
	}
	expectCode(t, ann, "CALL crm.pia_model!SHOW_FEATURE_IMPORTANCE()", "42704", "not authorized")
}
