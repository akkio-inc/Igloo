package pgwire

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/models"
	"github.com/akkio-inc/igloo/internal/tsforecast/tsforecasttest"
)

// blobText is a model blob's JSON, or "" once object storage has none.
func blobText(t *testing.T, s *Server, sha string) string {
	t.Helper()
	b, err := s.modelRuntime.Blobs().Get(context.Background(), sha)
	if errors.Is(err, models.ErrNoBlob) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestErasureRetrainsModels: an erasure request on tables a recommender,
// a classifier with the person's email among its categories, and
// forecasters of a series per email, one of them a foundation model's that
// keeps each series' last steps, were trained from completes only once
// it has trained them again, as their owner, and deleted their old blobs:
// RECOMMEND no longer knows the person, no blob holds their email, and a
// statement prepared before may not use what the replica held of the old
// models. A model trained from other tables stays as it is. A model whose
// owner is disabled blocks a request until they are enabled again.
func TestErasureRetrainsModels(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	// A model pack with a foundation model, whose FORECAST models keep each
	// series' last steps.
	vc.cfg.Models.Dir = filepath.Join(t.TempDir(), "models")
	tsforecasttest.InstallTiny(t, vc.cfg.Models.Dir, "test-bolt")
	r := vc.replica(t)
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		// Two halves of the people, each clicking its own half of the
		// campaigns.
		`CREATE TABLE lake.public.clicks AS SELECT 'u' || u || '@example.com' AS email, 'c' || i AS campaign, 1 + (u * i) % 3 AS clicks
			FROM range(0, 40) a(u), range(0, 20) b(i) WHERE (u % 2 = 0) = (i < 10) AND hash(u * 1000 + i) % 5 < 3`,
		// Eight visits each.
		`CREATE TABLE lake.public.visits AS SELECT 'u' || u || '@example.com' AS email,
			['north', 'south', 'west'][1 + u % 3] AS region, (u * 7 + v * 13) % 50 AS spend, (u * 7 + v * 13) % 50 > 25 AS converted
			FROM range(0, 40) a(u), range(0, 8) b(v)`,
		// A month of each one's daily spend.
		`CREATE TABLE lake.public.daily AS SELECT 'u' || u || '@example.com' AS email, TIMESTAMP '2026-01-01' + to_days(d::INTEGER) AS day,
			10 + u + 2 * sin(d) AS spend FROM range(0, 40) a(u), range(0, 30) b(d)`,
		`CREATE TABLE lake.public.campaigns AS SELECT 'c' || i AS campaign, i % 4 AS segment, i * 10.0 AS budget, i % 2 = 0 AS active
			FROM range(0, 200) r(i)`,
		"CREATE TAG pii",
		"ALTER TABLE lake.public.clicks ALTER COLUMN email SET TAG pii = 'email'",
		"ALTER TABLE lake.public.visits ALTER COLUMN email SET TAG pii = 'email'",
		"ALTER TABLE lake.public.daily ALTER COLUMN email SET TAG pii = 'email'",
		"CREATE USER otto WITH PASSWORD 'otto-password' ROLE writer",
		"CREATE VIEW lake.public.big_spenders AS SELECT email, spend, converted FROM lake.public.visits WHERE spend > 10",
	} {
		mustExec(t, admin, q)
	}
	walt := snowflakeConn(t, r.dbt(t))
	for _, q := range []string{
		`CREATE IGLOO.ML.MATRIX_FACTORIZATION tastes(INPUT_DATA => TABLE(clicks), USER_COLNAME => 'email', ITEM_COLNAME => 'campaign',
			RATING_COLNAME => 'clicks', CONFIG_OBJECT => {'feedback_type': 'implicit', 'num_factors': 4})`,
		`CREATE SNOWFLAKE.ML.CLASSIFICATION converts(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT email, region, spend, converted FROM visits'),
			TARGET_COLNAME => 'converted')`,
		`CREATE SNOWFLAKE.ML.FORECAST spend_forecast(INPUT_DATA => TABLE(daily), SERIES_COLNAME => 'email', TIMESTAMP_COLNAME => 'day',
			TARGET_COLNAME => 'spend', CONFIG_OBJECT => {'method': 'fast', 'evaluate': FALSE})`,
		`CREATE SNOWFLAKE.ML.FORECAST pack_forecast(INPUT_DATA => TABLE(daily), SERIES_COLNAME => 'email', TIMESTAMP_COLNAME => 'day',
			TARGET_COLNAME => 'spend', CONFIG_OBJECT => {'method': 'test-bolt', 'evaluate': FALSE})`,
		`CREATE SNOWFLAKE.ML.CLASSIFICATION budgets(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT segment, budget, active FROM campaigns'),
			TARGET_COLNAME => 'active')`,
		"GRANT USAGE ON SNOWFLAKE.ML.CLASSIFICATION converts TO USER otto",
		// Through a view, which records the table the view reads.
		`CREATE SNOWFLAKE.ML.CLASSIFICATION big(INPUT_DATA => TABLE(big_spenders), TARGET_COLNAME => 'converted')`,
	} {
		mustExec(t, walt, q)
	}
	const person = "u3@example.com"
	model := func(name string) models.Model {
		t.Helper()
		m, err := r.srv.models.Get(ctx, "public", name)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	before := map[string]models.Model{}
	for name, tables := range map[string]string{"tastes": "public.clicks", "converts": "public.visits", "spend_forecast": "public.daily",
		"pack_forecast": "public.daily",
		"budgets":       "public.campaigns", "big": "public.visits"} {
		m := model(name)
		if strings.Join(m.Tables, ",") != tables || m.Epoch < 1 || m.Dialect != dialectSnowflake || !strings.Contains(m.SearchPath, "public") {
			t.Errorf("%s records tables %q, epoch %d, dialect %q, search path %q", name, m.Tables, m.Epoch, m.Dialect, m.SearchPath)
		}
		before[name] = m
	}
	for _, name := range []string{"tastes", "converts", "spend_forecast", "pack_forecast", "big"} {
		if !strings.Contains(blobText(t, r.srv, before[name].Blob), person) {
			t.Errorf("%s's blob does not hold %s: the test shows nothing", name, person)
		}
	}
	recommend := `SELECT count(*) FROM TABLE(tastes!RECOMMEND(INPUT_DATA => SYSTEM$QUERY_REFERENCE($$SELECT 'u3@example.com' AS email$$),
		CONFIG_OBJECT => {'top_k': 3}))`
	known := `SELECT count(*) FROM TABLE(tastes!RECOMMEND(CONFIG_OBJECT => {'top_k': 1})) WHERE email = 'u3@example.com'`
	if n := count(t, walt, recommend); n != 3 {
		t.Fatalf("RECOMMEND for %s: %d rows", person, n)
	}
	if n := count(t, walt, known); n != 1 {
		t.Fatalf("RECOMMEND to everyone the model knows: %d rows for %s", n, person)
	}
	// A statement prepared now names the models the replica holds now.
	prepared := snowflakeConn(t, connect(t, r.url("walt", "walt-password"), pgx.QueryExecModeCacheStatement))
	if _, err := prepared.Prepare(ctx, "score", `SELECT converts!PREDICT(INPUT_DATA => {*}):class::STRING FROM visits LIMIT 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Exec(ctx, "score"); err != nil {
		t.Fatal(err)
	}

	cat, err := catalog.Open(ctx, vc.cfg)
	if err != nil {
		t.Fatal(err)
	}
	store := erasure.NewMemoryStore()
	retrainer := &ModelRetrainer{}
	retrainer.Bind(r.srv)
	var mu sync.Mutex
	var alerts []string
	svc := erasure.New(erasure.Options{Store: store, Catalog: cat, Policies: r.srv.gov.store.Load, Engine: r.eng,
		Key: []byte("an erasure key of thirty-two bytes"), CatalogName: "lake", Warehouse: vc.cfg.Storage.Location + "/",
		Models: retrainer, Purger: erasure.NewPurger(store, r.eng, erasure.MemoryCache+"replica", "replica", slog.New(slog.DiscardHandler)),
		Blocked: func(id, step, err string) {
			mu.Lock()
			defer mu.Unlock()
			alerts = append(alerts, step+": "+err)
		}})
	run := func(id string, until ...string) *erasure.Request {
		t.Helper()
		var got *erasure.Request
		for range 8 {
			_ = svc.Run(ctx, id)
			if got, err = store.Get(ctx, id); err != nil {
				t.Fatal(err)
			}
			if slices.Contains(until, got.Status) {
				return got
			}
		}
		t.Fatalf("request %s: status %s at %s: %s", id, got.Status, got.Step, got.Error)
		return nil
	}
	submit := func(email string) string {
		t.Helper()
		req, err := svc.Submit(ctx, "privacy", []erasure.Wanted{{Tag: "pii", Value: "email", Identifiers: []string{email}}})
		if err != nil {
			t.Fatal(err)
		}
		return req.ID
	}

	got := run(submit(person), erasure.StatusCompleted)
	if got.ModelsRetrained != 5 || got.Verification == nil || got.Verification.Models != 0 {
		t.Errorf("trained again %d models, verification %+v", got.ModelsRetrained, got.Verification)
	}
	for _, name := range []string{"tastes", "converts", "spend_forecast", "pack_forecast", "big"} {
		m := model(name)
		if m.Blob == before[name].Blob || m.Epoch <= before[name].Epoch || m.Owner != "walt" || m.TrainingSQL != before[name].TrainingSQL {
			t.Errorf("%s after the request: %+v", name, m)
		}
		if text := blobText(t, r.srv, before[name].Blob); text != "" {
			t.Errorf("%s's old blob is still in object storage", name)
		}
		if strings.Contains(blobText(t, r.srv, m.Blob), person) {
			t.Errorf("%s's new blob holds %s", name, person)
		}
	}
	if m := model("budgets"); m.Blob != before["budgets"].Blob || blobText(t, r.srv, m.Blob) == "" {
		t.Errorf("a model trained from another table was trained again: %+v", m)
	}
	// Grants stay.
	if p, err := r.srv.modelPrivilege(ctx, otto(t, r), model("converts")); err != nil || p != models.PrivilegeUsage {
		t.Errorf("otto's privilege on converts: %q, %v", p, err)
	}
	if n := count(t, walt, recommend); n != 0 {
		t.Errorf("RECOMMEND for %s after the request: %d rows", person, n)
	}
	if n := count(t, walt, known); n != 0 {
		t.Errorf("the model still knows %s", person)
	}
	for _, name := range []string{"spend_forecast", "pack_forecast"} {
		expectCode(t, walt, `SELECT * FROM TABLE(`+name+`!FORECAST(FORECASTING_PERIODS => 2, SERIES_VALUE => 'u3@example.com'))`, "22023",
			"no series")
	}
	if n := count(t, walt, `SELECT count(*) FROM TABLE(pack_forecast!FORECAST(FORECASTING_PERIODS => 2, SERIES_VALUE => 'u4@example.com'))`); n != 2 {
		t.Errorf("the foundation model forecast %d steps of another series", n)
	}
	// The statement prepared before may not use what the replica held.
	if _, err := prepared.Exec(ctx, "score"); err == nil || !strings.Contains(err.Error(), "prepare it again") {
		t.Errorf("a statement prepared before the request: %v", err)
	}

	// otto's model, trained from the same rows, cannot be trained again
	// while otto may not create models: the next request is blocked, and
	// alerts, until otto may again.
	ottoConn := snowflakeConn(t, connect(t, r.url("otto", "otto-password"), pgx.QueryExecModeSimpleProtocol))
	mustExec(t, ottoConn, `CREATE SNOWFLAKE.ML.CLASSIFICATION regions(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT email, spend, region FROM visits'),
		TARGET_COLNAME => 'region')`)
	regions := model("regions")
	mustExec(t, admin, "ALTER USER otto ROLE reader")
	id := submit("u5@example.com")
	got = run(id, erasure.StatusBlocked)
	if got.Step != erasure.StepModels || !strings.Contains(got.Error, "public.regions: permission denied: otto may not create models in schema public") ||
		len(alerts) == 0 || !strings.HasPrefix(alerts[len(alerts)-1], "models: ") {
		t.Errorf("blocked at %s: %q; alerts %q", got.Step, got.Error, alerts)
	}
	if m := model("regions"); m.Blob != regions.Blob || blobText(t, r.srv, m.Blob) == "" {
		t.Errorf("the model not trained again changed: %+v", m)
	}
	mustExec(t, admin, "ALTER USER otto ROLE writer")
	got = run(id, erasure.StatusCompleted)
	if m := model("regions"); m.Blob == regions.Blob || m.Owner != "otto" || blobText(t, r.srv, regions.Blob) != "" ||
		strings.Contains(blobText(t, r.srv, m.Blob), "u5@example.com") {
		t.Errorf("after otto may create models again: %+v", m)
	}
	if got.Verification == nil || got.Verification.Models != 0 {
		t.Errorf("verification %+v", got.Verification)
	}
}

// otto is the user otto, as the server has them.
func otto(t *testing.T, r *viewReplica) auth.User {
	t.Helper()
	u, err := r.srv.users.Get(context.Background(), "otto")
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// bumpingStore is a model store in which an erasure request moves the
// epoch on just after each CREATE reads it, before CREATE reads its
// training data.
type bumpingStore struct{ *models.MemoryStore }

func (s bumpingStore) Epoch(ctx context.Context) (int64, error) {
	e, err := s.MemoryStore.Epoch(ctx)
	if err == nil {
		_, err = s.MemoryStore.BumpEpoch(ctx)
	}
	return e, err
}

// TestModelTrainedDuringErasure: a model whose training read its rows
// before an erasure request moved the epoch on is not stored, and its blob
// goes; CREATE asks for it to be made again.
func TestModelTrainedDuringErasure(t *testing.T) {
	t.Parallel()
	store := bumpingStore{models.NewMemoryStore()}
	cs := startCatalogServer(t, func(o *Options) { o.Models = store })
	mustExec(t, cs.admin, leadsTable)
	conn := snowflakeConn(t, cs.admin)
	expectCode(t, conn, `CREATE SNOWFLAKE.ML.CLASSIFICATION lead_model(
		INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT region, spend, visits, converted FROM leads'), TARGET_COLNAME => 'converted')`,
		"40001", "create it again")
	if _, err := store.Get(context.Background(), "public", "lead_model"); !errors.Is(err, models.ErrNotFound) {
		t.Errorf("the model was stored: %v", err)
	}
	if b, ok := cs.srv.modelRuntime.Blobs().(*models.Memory); ok && b.Len() != 0 {
		t.Errorf("%d blobs kept", b.Len())
	}
}

// TestModelTablesRecorded: a model records the tables its training read,
// and those the models its input used had read.
func TestModelTablesRecorded(t *testing.T) {
	t.Parallel()
	store := models.NewMemoryStore()
	cs := startCatalogServer(t, func(o *Options) { o.Models = store })
	mustExec(t, cs.admin, leadsTable)
	for _, q := range []string{
		"CREATE TABLE lake.public.regions AS SELECT * FROM (VALUES ('north', 1), ('south', 2), ('east', 3), ('west', 4)) v(region, zone)",
		"CREATE TABLE lake.public.prospects AS SELECT spend, visits, zone FROM lake.public.leads JOIN lake.public.regions USING (region)",
	} {
		mustExec(t, cs.admin, q)
	}
	conn := snowflakeConn(t, cs.admin)
	mustExec(t, conn, `CREATE SNOWFLAKE.ML.CLASSIFICATION lead_model(INPUT_DATA => SYSTEM$QUERY_REFERENCE(
		'SELECT l.spend, l.visits, r.zone, l.converted FROM leads l JOIN regions r USING (region)'), TARGET_COLNAME => 'converted')`)
	// Trained on another model's predictions of prospects.
	mustExec(t, conn, `CREATE SNOWFLAKE.ML.CLASSIFICATION scored(INPUT_DATA => SYSTEM$QUERY_REFERENCE(
		$$SELECT zone, lead_model!PREDICT(INPUT_DATA => {*}):class::STRING AS predicted FROM prospects$$), TARGET_COLNAME => 'predicted')`)
	mustExec(t, conn, "CREATE SNOWFLAKE.ML.TOP_INSIGHTS insights()")
	for name, want := range map[string]string{"lead_model": "public.leads,public.regions", "scored": "public.leads,public.prospects,public.regions",
		"insights": ""} {
		m, err := store.Get(context.Background(), "public", name)
		if err != nil || m.Tables == nil || strings.Join(m.Tables, ",") != want {
			t.Errorf("%s records %q, %v", name, m.Tables, err)
		}
	}
}
