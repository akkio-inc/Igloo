package pgwire

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/models"
)

// campaignsTable is a synthetic table of campaigns whose value is planted:
// 2·spend − 3·visits, a region's effect, and a little noise; converted is
// whether that is above 4.
const campaignsTable = `CREATE TABLE lake.public.campaigns AS
	SELECT i AS campaign_id, region, spend, visits,
		2 * spend - 3 * visits + CASE region WHEN 'south' THEN 5 WHEN 'east' THEN -2 WHEN 'west' THEN 1 ELSE 0 END
			+ ((i * 37) % 11 - 5) / 10.0 AS value,
		2 * spend - 3 * visits + CASE region WHEN 'south' THEN 5 WHEN 'east' THEN -2 WHEN 'west' THEN 1 ELSE 0 END > 4 AS converted
	FROM (SELECT i, ['north', 'south', 'east', 'west'][1 + i % 4] AS region, ((i * 7919) % 1000) / 100.0 AS spend,
		((i * 104729) % 500) / 100.0 AS visits FROM range(1, 1501) t(i))`

// segmentsTable is a synthetic table of people in three planted segments
// by spend and visits, and ten outliers far from them (person_id 901 to
// 910).
const segmentsTable = `CREATE TABLE lake.public.segments AS
	SELECT i AS person_id,
		CASE WHEN i > 900 THEN 300 + (i % 7) * 10 ELSE [10, 50, 90][1 + i % 3] + ((i * 7919) % 600) / 100.0 - 3 END AS spend,
		CASE WHEN i > 900 THEN 25 ELSE [1, 8, 2][1 + i % 3] + ((i * 104729) % 100) / 100.0 - 0.5 END AS visits
	FROM range(1, 911) t(i)`

func TestIglooKMeans(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	mustExec(t, cs.admin, segmentsTable)
	conn := snowflakeConn(t, cs.admin)
	mustExec(t, conn, `CREATE IGLOO.ML.KMEANS segs(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT spend, visits FROM segments WHERE person_id <= 900'))`)
	if got := texts(t, conn, "SELECT count(DISTINCT centroid_id) FROM TABLE(segs!CENTROIDS())"); len(got) != 1 || got[0] != "3" {
		t.Errorf("CENTROIDS: %q", got)
	}
	// Each planted segment is one cluster.
	if got := texts(t, conn, `SELECT count(DISTINCT centroid_id) FROM TABLE(segs!PREDICT(INPUT_DATA => TABLE(segments)))
		WHERE person_id <= 900 GROUP BY person_id % 3`); len(got) != 3 || got[0] != "1" || got[1] != "1" || got[2] != "1" {
		t.Errorf("segments' clusters: %q", got)
	}
	if n := count(t, conn, `SELECT count(*) FROM segments WHERE segs!PREDICT(INPUT_DATA => {*}):nearest_centroids_distance[2]:distance::FLOAT
		>= segs!PREDICT(INPUT_DATA => {*}):nearest_centroids_distance[0]:distance::FLOAT`); n != 910 {
		t.Errorf("distances sorted in %d of 910 rows", n)
	}
	// The outliers are anomalies, and a few of the training rows (half a
	// percent of them, by the contamination).
	if got := texts(t, conn, `SELECT count(*) FILTER (WHERE person_id > 900), count(*) <= 16 FROM TABLE(segs!DETECT_ANOMALIES(
		INPUT_DATA => TABLE(segments), CONFIG_OBJECT => {'contamination': 0.005})) WHERE is_anomaly AND normalized_distance > 1`); len(got) != 1 ||
		got[0] != "10|true" {
		t.Errorf("anomalies: %q", got)
	}
	if n := count(t, conn, `SELECT count(*) FROM segments
		WHERE segs!DETECT_ANOMALIES(INPUT_DATA => {*}, CONFIG_OBJECT => {'contamination': 0.005}):is_anomaly::BOOLEAN AND person_id > 900`); n != 10 {
		t.Errorf("in a query, %d of 10 outliers flagged", n)
	}
	if got := texts(t, conn, "SELECT davies_bouldin_index < 0.5 FROM TABLE(segs!EVALUATE())"); len(got) != 1 || got[0] != "true" {
		t.Errorf("EVALUATE: %q", got)
	}
	expectCode(t, conn, "SELECT * FROM TABLE(segs!DETECT_ANOMALIES(INPUT_DATA => TABLE(segments)))", "22023", "contamination")
	expectCode(t, conn, "CALL segs!WEIGHTS()", "42883", "has no method WEIGHTS")
}

func TestIglooRegression(t *testing.T) {
	t.Parallel()
	store := models.NewMemoryStore()
	cs := startCatalogServer(t, func(o *Options) { o.Models = store })
	mustExec(t, cs.admin, campaignsTable)
	conn := snowflakeConn(t, cs.admin)
	create := `CREATE OR REPLACE IGLOO.ML.REGRESSION value_model(
		INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT region, spend, visits, value FROM campaigns'),
		TARGET_COLNAME => 'value', CONFIG_OBJECT => {'algorithm': 'linear'})`
	mustExec(t, conn, create)
	m, err := store.Get(context.Background(), "public", "value_model")
	if err != nil || m.Class != models.ClassRegression || !strings.Contains(m.Metrics, `"r2_score"`) {
		t.Fatalf("stored %+v, %v", m, err)
	}
	// The same rows give the same model.
	mustExec(t, conn, create)
	if again, _ := store.Get(context.Background(), "public", "value_model"); again.Blob != m.Blob {
		t.Error("training again changed the model")
	}

	// The planted coefficients, with their standard errors, as BigQuery's
	// ML.WEIGHTS and ML.ADVANCED_WEIGHTS give them.
	weights := texts(t, conn, "SELECT processed_input, weight, standard_error, p_value FROM TABLE(value_model!WEIGHTS()) "+
		"WHERE processed_input IN ('spend', 'visits') ORDER BY 1")
	if len(weights) != 2 {
		t.Errorf("WEIGHTS: %q", weights)
	}
	for _, row := range weights {
		parts := strings.Split(row, "|")
		w, _ := strconv.ParseFloat(parts[1], 64)
		want := 2.0
		if parts[0] == "visits" {
			want = -3
		}
		if d := w - want; d > 0.02 || d < -0.02 || parts[2] == "" || parts[3] != "0" {
			t.Errorf("weight %q, want %v", row, want)
		}
	}
	if got := texts(t, conn, "SELECT r2_score > 0.99 FROM TABLE(value_model!EVALUATE())"); len(got) != 1 || got[0] != "true" {
		t.Errorf("EVALUATE: %q", got)
	}
	if got := texts(t, conn, `SELECT r2_score > 0.99 FROM TABLE(value_model!EVALUATE(INPUT_DATA => TABLE(campaigns)))`); len(got) != 1 || got[0] != "true" {
		t.Errorf("EVALUATE on given rows: %q", got)
	}

	// PREDICT in a query, and as a table of the input's rows.
	if n := count(t, conn, "SELECT count(*) FROM campaigns WHERE abs(value_model!PREDICT(INPUT_DATA => {*}):value::FLOAT - value) < 0.6"); n != 1500 {
		t.Errorf("%d of 1500 predictions are close", n)
	}
	if n := count(t, conn, `SELECT count(*) FROM TABLE(value_model!PREDICT(INPUT_DATA => TABLE(campaigns)))
		WHERE abs(predicted_value - value) < 0.6 AND campaign_id > 0`); n != 1500 {
		t.Errorf("as a table, %d of 1500 predictions are close", n)
	}
	// EXPLAIN_PREDICT: the top features' attributions, adding up with the
	// baseline to the prediction.
	rows := texts(t, conn, `SELECT TO_JSON(top_feature_attributions), baseline_prediction_value, prediction_value, predicted_value
		FROM TABLE(value_model!EXPLAIN_PREDICT(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT * FROM campaigns ORDER BY campaign_id LIMIT 3'),
			CONFIG_OBJECT => {'top_k_features': 3}))`)
	if len(rows) != 3 {
		t.Fatalf("EXPLAIN_PREDICT: %q", rows)
	}
	for _, r := range rows {
		parts := strings.Split(r, "|")
		var attrs []struct {
			Feature     string  `json:"feature"`
			Attribution float64 `json:"attribution"`
		}
		if err := json.Unmarshal([]byte(parts[0]), &attrs); err != nil || len(attrs) != 3 {
			t.Fatalf("attributions %q: %v", parts[0], err)
		}
		sum, _ := strconv.ParseFloat(parts[1], 64)
		for _, a := range attrs {
			sum += a.Attribution
		}
		if v, _ := strconv.ParseFloat(parts[2], 64); sum-v > 1e-6 || v-sum > 1e-6 || parts[2] != parts[3] {
			t.Errorf("%q does not add up", r)
		}
	}
	if got := texts(t, conn, `SELECT value_model!EXPLAIN_PREDICT(INPUT_DATA => {*}, CONFIG_OBJECT => {'top_k_features': 1})
		:top_feature_attributions[0]:feature::STRING FROM campaigns WHERE campaign_id = 7`); len(got) != 1 ||
		got[0] != "spend" && got[0] != "visits" && got[0] != "region" {
		t.Errorf("the top feature in a query: %q", got)
	}

	// Boosted trees, the default, explain themselves by their trees too.
	mustExec(t, conn, `CREATE OR REPLACE IGLOO.ML.REGRESSION value_model(
		INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT region, spend, visits, value FROM campaigns'), TARGET_COLNAME => 'value',
		CONFIG_OBJECT => {'max_iterations': 300, 'learn_rate': 0.2})`)
	if got := texts(t, conn, "SELECT feature FROM TABLE(value_model!FEATURE_IMPORTANCE()) LIMIT 1"); len(got) != 1 || got[0] != "spend" {
		t.Errorf("FEATURE_IMPORTANCE: %q", got)
	}
	if got := texts(t, conn, "SELECT count(*) > 1 FROM TABLE(value_model!TRAINING_INFO()) WHERE loss > 0"); len(got) != 1 || got[0] != "true" {
		t.Errorf("TRAINING_INFO: %q", got)
	}
	expectCode(t, conn, "CALL value_model!WEIGHTS()", "22023", "linear model")
	if got := texts(t, conn, "SHOW IGLOO.ML.REGRESSION LIKE 'value%'"); len(got) != 1 || !strings.Contains(got[0], "|value_model|") {
		t.Errorf("SHOW: %q", got)
	}
	mustExec(t, conn, "DROP IGLOO.ML.REGRESSION value_model")
	expectCode(t, conn, "CALL value_model!EVALUATE()", "42704", "does not exist")
}

func TestIglooClassification(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	mustExec(t, cs.admin, campaignsTable)
	conn := snowflakeConn(t, cs.admin)
	for _, alg := range []string{"random_forest", "logistic", "boosted_trees"} {
		mustExec(t, conn, `CREATE OR REPLACE IGLOO.ML.CLASSIFICATION converts(
			INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT region, spend, visits, converted FROM campaigns'), TARGET_COLNAME => 'converted',
			CONFIG_OBJECT => {'algorithm': '`+alg+`', 'data_split_method': 'random', 'data_split_eval_fraction': 0.25})`)
		if got := texts(t, conn, "SELECT accuracy > 0.9, roc_auc > 0.95 FROM TABLE(converts!EVALUATE())"); len(got) != 1 || got[0] != "true|true" {
			t.Errorf("%s: EVALUATE %q", alg, got)
		}
		// The prediction as the target's type, its probabilities as JSON.
		if n := count(t, conn, `SELECT count(*) FROM TABLE(converts!PREDICT(INPUT_DATA => TABLE(campaigns)))
			WHERE predicted_converted = converted AND predicted_converted_probs IS NOT NULL`); n < 1400 {
			t.Errorf("%s: %d of 1500 predicted right", alg, n)
		}
		if n := count(t, conn, `SELECT count(*) FROM campaigns
			WHERE converts!PREDICT(INPUT_DATA => {*}):class::STRING = CASE WHEN converted THEN 'True' ELSE 'False' END`); n < 1400 {
			t.Errorf("%s: in a query, %d of 1500 predicted right", alg, n)
		}
	}
	if got := texts(t, conn, "SELECT CAST(sum(count) AS INT) FROM TABLE(converts!CONFUSION_MATRIX())"); len(got) != 1 || got[0] != "375" {
		t.Errorf("CONFUSION_MATRIX counts %q of 375 held-out rows", got)
	}
	if got := texts(t, conn, "SELECT count(*), max(recall) FROM TABLE(converts!ROC_CURVE())"); len(got) != 1 || got[0] != "101|1" {
		t.Errorf("ROC_CURVE: %q", got)
	}
	if got := texts(t, conn, "SELECT feature FROM TABLE(converts!GLOBAL_EXPLAIN()) LIMIT 1"); len(got) != 1 || got[0] != "spend" && got[0] != "visits" {
		t.Errorf("GLOBAL_EXPLAIN: %q", got)
	}
	for sql, want := range map[string][2]string{
		`CREATE IGLOO.ML.CLASSIFICATION c(INPUT_DATA => TABLE(campaigns), TARGET_COLNAME => 'converted', CONFIG_OBJECT => {'algorithm': 'linear'})`: {
			"22023", "algorithm is one of"},
		`CREATE IGLOO.ML.REGRESSION c(INPUT_DATA => TABLE(campaigns), TARGET_COLNAME => 'value', CONFIG_OBJECT => {'learn_rte': 0.1})`: {
			"22023", "unknown CONFIG_OBJECT key learn_rte"},
		`CREATE IGLOO.ML.REGRESSION c(INPUT_DATA => TABLE(campaigns), TARGET_COLNAME => 'region')`: {"22023", "predicts numbers"},
		`CREATE IGLOO.ML.REGRESSION c(INPUT_DATA => TABLE(campaigns))`:                             {"42601", "TARGET_COLNAME"},
		`CREATE IGLOO.ML.NEURAL_FOREST c(INPUT_DATA => TABLE(campaigns))`:                          {"0A000", "IGLOO.ML.NEURAL_FOREST is not supported"},
		`CALL converts!PREDICT(INPUT_DATA => TABLE(campaigns))`:                                    {"42809", "scores rows"},
		`SELECT * FROM TABLE(converts!PREDICT())`:                                                  {"42601", "needs INPUT_DATA"},
		`SELECT * FROM TABLE(converts!DETECT_ANOMALIES(INPUT_DATA => TABLE(campaigns)))`:           {"42883", "has no method DETECT_ANOMALIES"},
		`SELECT converts!RECOMMEND(INPUT_DATA => {*}) FROM campaigns`:                              {"42883", "no method RECOMMEND"},
		`SELECT * FROM TABLE(converts!EXPLAIN_PREDICT(INPUT_DATA => TABLE(campaigns), CONFIG_OBJECT => {'top_k': 2}))`: {
			"22023", "unknown CONFIG_OBJECT key top_k"},
		`DROP IGLOO.ML.REGRESSION converts`: {"42809", "is a IGLOO.ML.CLASSIFICATION, not a IGLOO.ML.REGRESSION"},
	} {
		expectCode(t, conn, sql, want[0], want[1])
	}
}

// Igloo's classes train on a sample of large inputs, the same sample for
// the same rows.
func TestIglooTrainsOnASample(t *testing.T) {
	t.Parallel()
	store := models.NewMemoryStore()
	cs := startCatalogServer(t, func(o *Options) { o.Models, o.trainRows = store, 500 })
	mustExec(t, cs.admin, campaignsTable)
	conn := snowflakeConn(t, cs.admin)
	create := `CREATE OR REPLACE IGLOO.ML.REGRESSION sampled(INPUT_DATA => TABLE(campaigns), TARGET_COLNAME => 'value',
		CONFIG_OBJECT => {'algorithm': 'linear'})`
	mustExec(t, conn, create)
	m, err := store.Get(context.Background(), "public", "sampled")
	if err != nil {
		t.Fatal(err)
	}
	var summary struct {
		Rows    int `json:"rows"`
		Sampled int `json:"sampled_rows"`
		Input   int `json:"input_rows"`
	}
	if err := json.Unmarshal([]byte(m.Metrics), &summary); err != nil || summary.Input != 1500 || summary.Sampled < 400 || summary.Sampled > 600 {
		t.Errorf("summary %s: %v", m.Metrics, err)
	}
	mustExec(t, conn, create)
	if again, _ := store.Get(context.Background(), "public", "sampled"); again.Blob != m.Blob {
		t.Error("a second sample differs")
	}
}

func TestIglooModelGrants(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	mustExec(t, cs.admin, campaignsTable)
	ana := snowflakeConn(t, cs.user(t, "ana", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol))
	bob := snowflakeConn(t, cs.user(t, "bob", auth.RoleReader, pgx.QueryExecModeSimpleProtocol))
	mustExec(t, cs.admin, "CREATE ROLE analysts")
	mustExec(t, cs.admin, "GRANT analysts TO bob")
	mustExec(t, ana, `CREATE IGLOO.ML.REGRESSION anas(INPUT_DATA => TABLE(lake.public.campaigns), TARGET_COLNAME => 'value')`)
	predict := "SELECT count(*) FROM TABLE(anas!PREDICT(INPUT_DATA => TABLE(campaigns)))"
	explain := "SELECT count(*) FROM campaigns WHERE anas!EXPLAIN_PREDICT(INPUT_DATA => {*}):value IS NOT NULL"
	expectCode(t, bob, predict, "42704", "does not exist or not authorized")
	expectDenied(t, bob, "CREATE IGLOO.ML.REGRESSION bobs(INPUT_DATA => TABLE(campaigns), TARGET_COLNAME => 'value')", "writer role or CREATE")
	mustExec(t, ana, "GRANT IGLOO.ML.REGRESSION ROLE anas!MLCONSUMER TO ROLE analysts")
	if n := count(t, bob, predict); n != 1500 {
		t.Errorf("bob predicts %d rows", n)
	}
	if n := count(t, bob, explain); n != 1500 {
		t.Errorf("bob explains %d rows", n)
	}
	expectDenied(t, bob, "SELECT * FROM TABLE(anas!EVALUATE())", "may only predict")
	mustExec(t, ana, "GRANT USAGE ON IGLOO.ML.REGRESSION anas TO ROLE analysts")
	if got := texts(t, bob, "SELECT count(*) FROM TABLE(anas!EVALUATE())"); len(got) != 1 || got[0] != "1" {
		t.Errorf("bob's EVALUATE: %q", got)
	}
	if got := texts(t, bob, "SHOW IGLOO.ML.REGRESSION"); len(got) != 1 {
		t.Errorf("bob sees %q", got)
	}
	expectDenied(t, bob, "DROP IGLOO.ML.REGRESSION anas", "does not own")
}

// Scoring an Igloo model's rows runs on workers, as a table and in a
// query.
func TestDistributedIglooScoring(t *testing.T) {
	t.Parallel()
	ds := startDistServer(t, 2)
	mustExec(t, ds.admin, campaignsTable)
	conn := snowflakeConn(t, ds.admin)
	mustExec(t, conn, `CREATE IGLOO.ML.CLASSIFICATION converts(INPUT_DATA => SYSTEM$QUERY_REFERENCE(
		'SELECT region, spend, visits, converted FROM campaigns'), TARGET_COLNAME => 'converted', CONFIG_OBJECT => {'algorithm': 'random_forest'})`)
	mustExec(t, conn, `CREATE IGLOO.ML.KMEANS segs(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT spend, visits FROM campaigns'),
		CONFIG_OBJECT => {'num_clusters': 4})`)
	for _, query := range []string{
		`SELECT region, count(*) FROM TABLE(segs!DETECT_ANOMALIES(INPUT_DATA => TABLE(campaigns), CONFIG_OBJECT => {'contamination': 0.05}))
			WHERE NOT is_anomaly GROUP BY region ORDER BY region`,
		`SELECT region, count(*) FROM TABLE(converts!PREDICT(INPUT_DATA => TABLE(campaigns))) WHERE predicted_converted GROUP BY region ORDER BY region`,
		`SELECT region, count(*) FROM campaigns WHERE converts!EXPLAIN_PREDICT(INPUT_DATA => {*}):class::STRING = 'True' GROUP BY region ORDER BY region`,
	} {
		mustExec(t, conn, "SET distributed_execution = off")
		local := texts(t, conn, query)
		mustExec(t, conn, "SET distributed_execution = on")
		if got := texts(t, conn, query); strings.Join(got, ";") != strings.Join(local, ";") || len(got) != 4 {
			t.Errorf("on workers: %q; here: %q", got, local)
		}
		if q := newestRun(t, ds.history, "igloo_ml_predict("); q.Workers != 2 {
			t.Errorf("%d workers scored, want 2 (%s)", q.Workers, q.Text)
		}
	}
}

// An Igloo model, too, trains on what its creator reads, masking policies
// applied.
func TestIglooModelTrainsOnMaskedValues(t *testing.T) {
	t.Parallel()
	cs := startPolicyServer(t)
	mustExec(t, cs.admin, `CREATE TABLE lake.crm.signups AS
		SELECT i AS id, CASE WHEN i % 3 = 0 THEN 'x' || i || '@acme.com' ELSE 'x' || i || '@globex.com' END AS email,
			i % 3 = 0 AS acme, (i * 13) % 7 AS noise FROM range(1, 601) t(i)`)
	mustExec(t, cs.admin, "ALTER TABLE lake.crm.signups ALTER COLUMN email SET MASKING POLICY mask_email")
	ann := snowflakeConn(t, cs.user(t, "ann", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol))
	pia := snowflakeConn(t, cs.user(t, "pia", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol))
	mustExec(t, cs.admin, "GRANT pii_reader TO pia")
	train := `CREATE IGLOO.ML.CLASSIFICATION crm.%s(
		INPUT_DATA => SYSTEM$QUERY_REFERENCE($$SELECT split_part(email, '@', 2) AS domain, noise, acme FROM lake.crm.signups$$),
		TARGET_COLNAME => 'acme', CONFIG_OBJECT => {'algorithm': 'logistic'})`
	mustExec(t, ann, strings.Replace(train, "%s", "ann_model", 1))
	mustExec(t, pia, strings.Replace(train, "%s", "pia_model", 1))
	var a, p float64
	if err := ann.QueryRow(context.Background(), "SELECT roc_auc FROM TABLE(crm.ann_model!EVALUATE())").Scan(&a); err != nil {
		t.Fatal(err)
	}
	if err := pia.QueryRow(context.Background(), "SELECT roc_auc FROM TABLE(crm.pia_model!EVALUATE())").Scan(&p); err != nil {
		t.Fatal(err)
	}
	if a > 0.75 || p < 0.99 {
		t.Errorf("AUC %.3f from masked emails, %.3f from the emails themselves", a, p)
	}
	expectCode(t, ann, "SELECT * FROM TABLE(crm.pia_model!WEIGHTS())", "42704", "not authorized")
}
