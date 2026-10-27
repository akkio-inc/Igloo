package snowflake

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestParseModelStatement(t *testing.T) {
	st, ok, err := ParseModelStatement(`CREATE OR REPLACE SNOWFLAKE.ML.FORECAST IF NOT EXISTS demo.spend_model(
		INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'demo."Daily Spend"'),
		SERIES_COLNAME => 'campaign',
		TIMESTAMP_COLNAME => 'day',
		TARGET_COLNAME => 'spend',
		CONFIG_OBJECT => {'method': 'fast', 'evaluation_config': {'n_splits': 2, 'gap': 0}, 'lower_bound': -1.5,
		                  'on_error': 'skip', 'evaluate': TRUE}
	) WITH TAG (team = 'growth') COMMENT = 'daily spend, it\'s fine';`)
	if err != nil || !ok {
		t.Fatalf("ok %v, err %v", ok, err)
	}
	if st.Verb != "CREATE" || st.Class != "FORECAST" || !st.Replace || !st.IfNotExists || !slices.Equal(st.Name, []string{"demo", "spend_model"}) {
		t.Errorf("parsed %+v", st)
	}
	if st.Comment == nil || *st.Comment != "daily spend, it's fine" || !st.Tags {
		t.Errorf("comment %v, tags %v", st.Comment, st.Tags)
	}
	in := st.Call.Arg("INPUT_DATA")
	if in == nil || in.Kind != ValueTable || !slices.Equal(in.Table, []string{"demo", "Daily Spend"}) {
		t.Errorf("INPUT_DATA %+v", in)
	}
	cfg := st.Call.Arg("CONFIG_OBJECT")
	if cfg.Field("method").Str != "fast" || cfg.Field("evaluation_config").Field("n_splits").Num != 2 ||
		cfg.Field("lower_bound").Num != -1.5 || !cfg.Field("evaluate").Bool {
		t.Errorf("CONFIG_OBJECT %+v", cfg)
	}

	for in, want := range map[string]ModelStatement{
		`DROP SNOWFLAKE.ML.CLASSIFICATION IF EXISTS m`: {Verb: "DROP", Class: "CLASSIFICATION", IfExists: true, Name: []string{"m"}},
		`SHOW SNOWFLAKE.ML.FORECAST INSTANCES LIKE 'sp%' IN SCHEMA demo`: {Verb: "SHOW", Class: "FORECAST", In: "SCHEMA",
			InName: []string{"demo"}},
		`GRANT USAGE ON SNOWFLAKE.ML.FORECAST demo.m TO ROLE analysts`: {Verb: "GRANT", Class: "FORECAST", Privilege: "USAGE",
			Name: []string{"demo", "m"}, Grantee: "analysts"},
		`REVOKE SNOWFLAKE.ML.CLASSIFICATION ROLE m!MLCONSUMER FROM USER ana`: {Verb: "REVOKE", Class: "CLASSIFICATION",
			Privilege: "MLCONSUMER", Name: []string{"m"}, Grantee: "ana", GranteeUser: true},
		`GRANT CREATE SNOWFLAKE.ML.FORECAST ON SCHEMA demo TO ROLE r`: {Verb: "GRANT", Class: "FORECAST", Privilege: "CREATE",
			Name: []string{"demo"}, Grantee: "r"},
	} {
		st, ok, err := ParseModelStatement(in)
		if err != nil || !ok {
			t.Errorf("%s: ok %v, err %v", in, ok, err)
			continue
		}
		st.Like, st.Call = nil, nil
		if st.Verb != want.Verb || st.Class != want.Class || st.IfExists != want.IfExists || !slices.Equal(st.Name, want.Name) ||
			st.Privilege != want.Privilege || st.Grantee != want.Grantee || st.GranteeUser != want.GranteeUser ||
			st.In != want.In || !slices.Equal(st.InName, want.InName) {
			t.Errorf("%s: parsed %+v", in, st)
		}
	}

	st, ok, err = ParseModelStatement(`CALL demo.m!FORECAST(FORECASTING_PERIODS => 7, SERIES_VALUE => 'c1'::VARIANT,
		CONFIG_OBJECT => {'prediction_interval': 0.8})`)
	if err != nil || !ok || st.Verb != "CALL" || st.Call.Method != "FORECAST" || st.Call.Arg("FORECASTING_PERIODS").Num != 7 ||
		st.Call.Arg("SERIES_VALUE").Str != "c1" || st.Call.Arg("CONFIG_OBJECT").Field("prediction_interval").Num != 0.8 {
		t.Errorf("CALL: %+v, %v", st, err)
	}

	for _, in := range []string{`SELECT 1`, `CALL my_procedure()`, `CREATE TABLE ml (a INT)`, `SHOW TABLES`, `CREATE SNOWFLAKE_ML x`} {
		if _, ok, _ := ParseModelStatement(in); ok {
			t.Errorf("%s: read as a model statement", in)
		}
	}
	for in, says := range map[string]string{
		`CREATE SNOWFLAKE.ML.FORECAST m(INPUT_DATA => a + b)`:   "INPUT_DATA",
		`CREATE SNOWFLAKE.ML.FORECAST m`:                        "arguments",
		`CALL m!FORECAST(7) extra`:                              "unexpected text",
		`GRANT SNOWFLAKE.ML.CLASSIFICATION ROLE m TO ROLE r`:    "instance role",
		`CREATE SNOWFLAKE.ML.FORECAST m(INPUT_DATA => TABLE())`: "TABLE()",
	} {
		_, ok, err := ParseModelStatement(in)
		if !ok || err == nil || !strings.Contains(err.Error(), says) {
			t.Errorf("%s: ok %v, err %v, want one mentioning %q", in, ok, err, says)
		}
	}
}

func TestParseModelCall(t *testing.T) {
	c, err := ParseModelCall(`ai_predict_class(input => TABLE(SELECT * FROM demo.people WHERE x > 1), target_col => 'churned',
		feature_cols => ARRAY('tenure', 'plan'), seed => 42)`)
	if err != nil {
		t.Fatal(err)
	}
	if c.Method != "AI_PREDICT_CLASS" || c.Name != nil || c.Arg("INPUT").Kind != ValueQuery ||
		c.Arg("INPUT").Str != "SELECT * FROM demo.people WHERE x > 1" || len(c.Arg("FEATURE_COLS").Elems) != 2 ||
		c.Arg("SEED").Num != 42 {
		t.Errorf("parsed %+v", c)
	}
	c, err = ParseModelCall(`"My".m!DETECT_ANOMALIES(INPUT_DATA => TABLE(demo.new_rows), TIMESTAMP_COLNAME => 'ts')`)
	if err != nil || !slices.Equal(c.Name, []string{"My", "m"}) || c.Arg("INPUT_DATA").Kind != ValueTable {
		t.Errorf("parsed %+v, %v", c, err)
	}
}

func TestModelTranslation(t *testing.T) {
	for in, want := range map[string]string{
		`SELECT * FROM TABLE(m!FORECAST(FORECASTING_PERIODS => 3))`: `SELECT * FROM igloo_ml_method('m!FORECAST(FORECASTING_PERIODS => 3)')`,
		`CREATE TABLE f AS SELECT ts, forecast FROM TABLE(demo.m!FORECAST(FORECASTING_PERIODS => 3, CONFIG_OBJECT => {'prediction_interval': 0.9})) WHERE forecast > 0`: `CREATE TABLE f AS SELECT ts, forecast FROM igloo_ml_method('demo.m!FORECAST(FORECASTING_PERIODS => 3, CONFIG_OBJECT => {''prediction_interval'': 0.9})') WHERE forecast > 0`,
		`SELECT id, m!PREDICT(INPUT_DATA => {*}) AS p FROM t`:                                         `SELECT id, CAST(igloo_ml_predict_ref('m', '*') AS JSON) AS "P" FROM t`,
		`SELECT m!PREDICT(INPUT_DATA => {* EXCLUDE (id, label)}) FROM t`:                              `SELECT CAST(igloo_ml_predict_ref('m', '*', 'id', 'label') AS JSON) AS "M!PREDICT(INPUT_DATA => {* EXCLUDE (ID, LABEL)})" FROM t`,
		`SELECT s.m!PREDICT(INPUT_DATA => {t.*}):class::STRING AS c FROM t`:                           `SELECT json_extract_string(CAST(igloo_ml_predict_ref('s.m', '*:t') AS JSON), '$.class') AS "C" FROM t`,
		`SELECT m!PREDICT({'plan': UPPER(p), 'tenure': IFF(a, 1, 2)}) AS p FROM t`:                    `SELECT CAST(igloo_ml_predict_ref('m', '=', 'plan', UPPER(p), 'tenure', CASE WHEN a THEN 1 ELSE 2 END) AS JSON) AS "P" FROM t`,
		`SELECT * FROM ai_predict_value(input => TABLE(t), target_col => 'y', feature_cols => ['a'])`: `SELECT * FROM igloo_ml_method('ai_predict_value(input => TABLE(t), target_col => ''y'', feature_cols => [''a''])')`,
	} {
		res, err := Translate(in, Options{})
		if err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		if res.SQL != want {
			t.Errorf("%s:\n got %s\nwant %s", in, res.SQL, want)
		}
	}
	// Statements on the classes pass as written.
	for _, in := range []string{
		`CREATE SNOWFLAKE.ML.CLASSIFICATION m(INPUT_DATA => SYSTEM$REFERENCE('TABLE', 't'), TARGET_COLNAME => 'y')`,
		`CALL m!SHOW_EVALUATION_METRICS()`,
		`SHOW SNOWFLAKE.ML.FORECAST`,
		`GRANT USAGE ON SNOWFLAKE.ML.FORECAST m TO ROLE r`,
	} {
		res, err := Translate(in, Options{})
		if err != nil || res.SQL != in || !res.Untranslated {
			t.Errorf("%s: %+v, %v", in, res, err)
		}
	}
	for in, says := range map[string]string{
		`SELECT m!FORECAST(FORECASTING_PERIODS => 3)`:           "CALL model!FORECAST",
		`SELECT m!PREDICT(INPUT_DATA => x) FROM t`:              "INPUT_DATA other than an object",
		`SELECT m!PREDICT(INPUT_DATA => {* ILIKE 'a%'}) FROM t`: "{* ILIKE",
		`SELECT m!PREDICT() FROM t`:                             "needs INPUT_DATA",
		`CALL my_procedure()`:                                   "stored procedures",
	} {
		_, err := Translate(in, Options{})
		var e *Error
		if !errors.As(err, &e) || !strings.Contains(e.Message+" "+e.Hint, says) {
			t.Errorf("%s: %v, want an error mentioning %q", in, err, says)
		}
	}
}
