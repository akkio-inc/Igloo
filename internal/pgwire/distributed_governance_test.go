package pgwire

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/federation"
	"github.com/akkio-inc/igloo/internal/workload"
)

// Distributed execution against everything else that decides what a
// statement may read and how it runs: policies, grants, connections,
// external tables and workload limits.

// TestDistributedPolicies checks that statements that masking or row
// access policies apply to run on one machine, directly or through a view:
// workers read table files and could not apply the policies, so no raw
// value and no hidden row's contribution may come back, however the
// statement aggregates them.
func TestDistributedPolicies(t *testing.T) {
	ds := startDistServer(t, 3)
	for _, sql := range policyFixture {
		mustExec(t, ds.admin, sql)
	}
	// Enough rows that every worker would get some.
	for _, sql := range []string{
		`INSERT INTO lake.crm.contacts SELECT i, CASE WHEN i % 2 = 0 THEN 'acme' ELSE 'globex' END,
			'user' || i || CASE WHEN i % 2 = 0 THEN '@acme.com' ELSE '@globex.com' END, 'dev-' || i, i FROM range(5, 3000) t(i)`,
		"CREATE VIEW memory.main.contact_view AS SELECT id, client, email, spend FROM lake.crm.contacts",
	} {
		mustExec(t, ds.admin, sql)
	}
	rita := ds.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, ds.admin, "GRANT acme_team TO rita")
	mustExec(t, rita, "SET distributed_execution = on")
	mustExec(t, ds.admin, "SET distributed_execution = on")

	// acme's rows alone: ids 1 and 2, and the even ids from 6 to 2998.
	acme := `SELECT count(*), sum(spend)::BIGINT FROM lake.crm.contacts WHERE client = 'acme'`
	mustExec(t, ds.admin, "SET distributed_execution = off")
	want := texts(t, ds.admin, acme)
	mustExec(t, ds.admin, "SET distributed_execution = on")
	if len(want) != 1 || want[0] != "1499|2248524" {
		t.Fatalf("acme's rows: %q", want)
	}
	for _, c := range []struct {
		conn  *pgx.Conn
		user  string
		query string
		want  []string
	}{
		// A masked column, aggregated every way that could show it.
		{rita, "rita", "SELECT count(DISTINCT email), min(email), max(email) FROM lake.crm.contacts", []string{"1|***|***"}},
		{rita, "rita", "SELECT client, count(DISTINCT email), max(email) FROM lake.crm.contacts GROUP BY client ORDER BY client", []string{"acme|1|***"}},
		// Rows the row access policy hides count nowhere.
		{rita, "rita", "SELECT count(*), sum(spend)::BIGINT FROM lake.crm.contacts", want},
		{rita, "rita", "SELECT client, count(*) FROM lake.crm.contacts GROUP BY ALL ORDER BY ALL", []string{"acme|1499"}},
		// Through a view.
		{rita, "rita", "SELECT count(DISTINCT email), sum(spend)::BIGINT FROM memory.main.contact_view", []string{"1|2248524"}},
		// Joined to a table without policies.
		{rita, "rita", "SELECT count(*) FROM lake.crm.contacts c JOIN lake.public.campaigns p ON c.client = p.client", []string{"1499"}},
		// Admins see every row, and masked values too.
		{ds.admin, "igloo", "SELECT count(DISTINCT email), min(email), count(*) FROM lake.crm.contacts", []string{"1|***|2999"}},
	} {
		got := texts(t, c.conn, c.query)
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("%s: got %q, want %q", c.query, got, c.want)
		}
		for _, row := range got {
			if strings.Contains(row, "@") || strings.Contains(row, "globex") {
				t.Errorf("%s returned a raw value: %s", c.query, row)
			}
		}
		if q := lastQuery(t, ds.catalogServer, c.user, c.query); q.Workers != 0 || len(q.Policies) == 0 {
			t.Errorf("%s: %d workers, policies %q", c.query, q.Workers, q.Policies)
		}
	}

	// Tables without policies still run on workers, for everyone.
	orders := "SELECT amount, count(*) FROM lake.public.orders GROUP BY amount"
	for _, c := range []struct {
		conn *pgx.Conn
		user string
	}{{rita, "rita"}, {ds.admin, "igloo"}} {
		expectAllowed(t, c.conn, orders)
		if q := lastQuery(t, ds.catalogServer, c.user, orders); q.Workers != 3 {
			t.Errorf("%s's statement over a table without policies: %d workers", c.user, q.Workers)
		}
	}

	// Should a statement reach the planner without the rewriter having
	// seen it, the planner still refuses a protected table.
	ctx := context.Background()
	user, err := ds.store.Get(ctx, "igloo")
	if err != nil {
		t.Fatal(err)
	}
	es, err := ds.srv.OpenEmbedded(ctx, user, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	env := &sessionEnv{s: ds.srv, sess: es.sess, user: user, st: &statement{}}
	_, err = ds.srv.dist.Plan(ctx, "SELECT count(*), max(email) FROM lake.crm.contacts", env, distributed.ModeOn)
	if !distributed.IsUnsupported(err) || !strings.Contains(err.Error(), "policies") {
		t.Errorf("planning a protected table: %v", err)
	}
}

// TestDistributedCustomGrants checks users with the none role who read
// through grants of their own and of custom roles: what they may read runs
// on workers, and nothing else does, or runs at all.
func TestDistributedCustomGrants(t *testing.T) {
	ds := startDistServer(t, 3)
	mustExec(t, ds.admin, "INSERT INTO lake.sales.leads SELECT i FROM range(4, 3000) t(i)")
	mustExec(t, ds.admin, "CREATE ROLE analyst")
	nina := ds.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	nils := ds.user(t, "nils", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, ds.admin, "GRANT analyst TO nina")
	mustExec(t, ds.admin, "GRANT SELECT ON TABLE lake.public.orders TO analyst")
	mustExec(t, ds.admin, "GRANT SELECT ON SCHEMA lake.sales TO nils")
	for _, conn := range []*pgx.Conn{nina, nils} {
		mustExec(t, conn, "SET distributed_execution = on")
	}

	orders := "SELECT amount, count(*) FROM orders GROUP BY amount"
	leads := "SELECT count(*), sum(id)::BIGINT FROM lake.sales.leads"
	expectAllowed(t, nina, orders)
	expectAllowed(t, nils, leads)
	for user, query := range map[string]string{"nina": orders, "nils": leads} {
		if q := lastQuery(t, ds.catalogServer, user, query); q.Workers != 3 {
			t.Errorf("%s's granted table: %d workers", user, q.Workers)
		}
	}
	if got := texts(t, nils, leads); len(got) != 1 || got[0] != "2999|4498500" {
		t.Errorf("%s: %q", leads, got)
	}
	// Each reads only their own.
	expectDenied(t, nina, leads, "SELECT on table sales.leads")
	expectDenied(t, nils, orders, "SELECT on table public.orders")
	expectDenied(t, nina, "SELECT o.amount, count(*) FROM orders o JOIN secrets s ON o.id = s.id GROUP BY 1", "SELECT on table public.secrets")
	expectDenied(t, nina, "SELECT amount, count(*) FROM orders WHERE id IN (SELECT id FROM secrets) GROUP BY 1", "SELECT on table public.secrets")
	// A grant taken back holds from the next statement on.
	mustExec(t, ds.admin, "REVOKE SELECT ON TABLE lake.public.orders FROM analyst")
	expectDenied(t, nina, orders, "SELECT on table public.orders")
}

// TestDistributedConnections checks that connections' tables, in
// PostgreSQL here, are never read by workers: a statement that reads one in
// a part it would distribute runs on one machine, and one that reads one
// only in the part the coordinator runs itself reads it there.
func TestDistributedConnections(t *testing.T) {
	needExtensions(t)
	pg := remoteFromEnv(t, "IGLOO_TEST_DATABASE_URL")
	schema := pgFixture(t, pg)
	store := federation.NewMemoryStore()
	c := federation.New("crm", federation.Postgres)
	c.Host, c.Port, c.User, c.Password, c.Database = pg.host, pg.port, pg.user, pg.password, pg.database
	c.Schemas = []string{schema}
	if err := store.Create(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	dir, spill := t.TempDir(), filepath.Join(t.TempDir(), "spill")
	source := &lakeSource{dir: dir, files: map[string][]distributed.File{}}
	var addrs []string
	for i := range 2 {
		addr, _ := startTestWorker(t, dir, i, nil)
		addrs = append(addrs, addr)
	}
	co, err := distributed.NewCoordinator(distributed.Options{
		Source: source, Secret: testWorkerSecret, Version: "test", TempDir: spill, Threads: 1,
		Pool: distributed.PoolOptions{Addresses: addrs},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { co.Close() })
	cfg := config.Config{DuckDB: config.DuckDB{TempDir: spill}, Storage: config.Storage{AllowedLocations: []string{dir + "/"}}}
	fs := startFedServerWithConfig(t, store, cfg, func(s *Server) { s.dist = co })
	conn, err := fs.srv.engine.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	source.conn = conn
	mustExec(t, fs.admin, "INSERT INTO lake.public.audiences SELECT i % 10 + 1, CASE WHEN i % 3 = 0 THEN 'new' ELSE 'loyal' END FROM range(3000) t(i)")
	mustExec(t, fs.admin, "SET distributed_execution = on")
	customers := "crm." + schema + ".customers"

	for query, want := range map[string]int{
		"SELECT segment, count(*) FROM lake.public.audiences GROUP BY 1 ORDER BY 1":                                                      2,
		"SELECT region, count(*) FROM " + customers + " GROUP BY 1 ORDER BY 1":                                                           0,
		"SELECT a.segment, count(*) FROM lake.public.audiences a JOIN " + customers + " c ON c.id = a.customer_id GROUP BY 1 ORDER BY 1": 0,
		"SELECT count(*) FROM lake.public.audiences WHERE customer_id IN (SELECT id FROM " + customers + " WHERE region = 'east')":       0,
		// The coordinator reads the connection's table; workers read the
		// catalog's.
		"SELECT x.segment, x.n, (SELECT count(*) FROM " + customers + ") AS customers FROM (SELECT segment, count(*) AS n FROM lake.public.audiences GROUP BY 1) AS x ORDER BY 1": 2,
	} {
		mustExec(t, fs.admin, "SET distributed_execution = off")
		local := queryAll(t, fs.admin, query)
		mustExec(t, fs.admin, "SET distributed_execution = on")
		if got := queryAll(t, fs.admin, query); got != local {
			t.Errorf("%s: got %s, want %s", query, got, local)
		}
		if n := lastWorkers(t, fs.history, query); n != want {
			t.Errorf("%s: %d workers, want %d", query, n, want)
		}
	}
}

// TestDistributedExternalTables checks that Delta Sharing's external
// tables, views over files that only Igloo's proxy serves, run on one
// machine, alone and joined to catalog tables.
func TestDistributedExternalTables(t *testing.T) {
	dir := t.TempDir()
	proxy := dir + "/proxy/"
	if err := os.MkdirAll(proxy, 0o755); err != nil {
		t.Fatal(err)
	}
	ds := startDistServerIn(t, dir, 3, func(o *Options) {
		o.external, o.externalFiles = fakeExternal{"public.viewing": true}, proxy
	})
	viewing := proxy + "f1.parquet"
	for _, sql := range []string{
		"COPY (SELECT i % 7 AS amount, i AS minutes FROM range(5000) t(i)) TO '" + viewing + "' (FORMAT parquet)",
		"CREATE SCHEMA memory.public",
		"CREATE VIEW memory.public.viewing AS SELECT * FROM read_parquet(['" + viewing + "'])",
		"SET distributed_execution = on",
	} {
		mustExec(t, ds.admin, sql)
	}
	for _, query := range []string{
		"SELECT amount, sum(minutes) FROM memory.public.viewing GROUP BY 1 ORDER BY 1",
		"SELECT o.amount, count(*) FROM lake.public.orders o JOIN memory.public.viewing v USING (amount) GROUP BY 1 ORDER BY 1",
		"SELECT x.amount, x.n, (SELECT count(*) FROM memory.public.viewing) FROM (SELECT amount, count(*) AS n FROM lake.public.orders GROUP BY 1) x",
	} {
		expectAllowed(t, ds.admin, query)
		if q := lastQuery(t, ds.catalogServer, "igloo", query); q.Workers != 0 || q.Outcome != audit.OutcomeOK {
			t.Errorf("%s: %d workers, %s", query, q.Workers, q.Outcome)
		}
	}
}

// TestDistributedWorkload checks that a distributed statement counts
// against its user's role limits while it runs, statement slots and
// memory alike, that statement_timeout ends its tasks on the workers, and
// that the history records its pool and query tag.
func TestDistributedWorkload(t *testing.T) {
	ctl := workload.NewController(1 << 30)
	ds := startDistServer(t, 2, func(o *Options) { o.Workload = ctl; o.Pool = "reporting" })
	ctx := context.Background()
	mustExec(t, ds.admin, "CREATE ROLE batch")
	bea := ds.user(t, "bea", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	bo := ds.user(t, "bo", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	cy := ds.user(t, "cy", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	for _, sql := range []string{
		"GRANT batch TO bea", "GRANT batch TO bo",
		"ALTER ROLE batch SET max_concurrent_statements = 1",
		"ALTER USER bea SET memory_limit = '600MiB'",
		"ALTER USER cy SET memory_limit = '600MiB'",
	} {
		mustExec(t, ds.admin, sql)
	}
	mustExec(t, bea, "SET distributed_execution = on")
	mustExec(t, bea, "SET query_tag = 'client:acme'")
	query := "SELECT amount, count(*) AS n FROM orders GROUP BY amount"

	// The workers hold bea's tasks; meanwhile her statement holds the
	// role's one slot and her memory reservation.
	release := ds.gate.hold()
	var wg sync.WaitGroup
	var beaErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, beaErr = bea.Exec(ctx, query)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for waiting, _ := ds.gate.counts(); waiting < 2 && time.Now().Before(deadline); waiting, _ = ds.gate.counts() {
		time.Sleep(10 * time.Millisecond)
	}
	if n := ctl.Running("role batch"); n != 1 {
		t.Errorf("%d statements of role batch run while the workers hold bea's", n)
	}
	mustExec(t, bo, "SET statement_timeout = '300ms'")
	if _, err := bo.Exec(ctx, "SELECT 1"); pgCode(err) != "57014" || !strings.Contains(err.Error(), "the limit of 1 statement at once for role batch") {
		t.Errorf("bo's statement while bea's runs on workers: %v", err)
	}
	mustExec(t, cy, "SET statement_timeout = '300ms'")
	if _, err := cy.Exec(ctx, "SELECT 2"); pgCode(err) != "57014" || !strings.Contains(err.Error(), "memory") {
		t.Errorf("cy's reservation while bea's runs on workers: %v", err)
	}
	release()
	wg.Wait()
	if beaErr != nil {
		t.Fatal(beaErr)
	}
	q := lastQuery(t, ds.catalogServer, "bea", query)
	if q.Workers != 2 || q.Pool != "reporting" || q.QueryTag != "client:acme" || q.Outcome != audit.OutcomeOK {
		t.Errorf("bea's distributed statement: workers %d, pool %q, tag %q, %s", q.Workers, q.Pool, q.QueryTag, q.Outcome)
	}

	// statement_timeout ends the statement and its tasks.
	release = ds.gate.hold()
	defer release()
	mustExec(t, bea, "SET statement_timeout = '500ms'")
	start := time.Now()
	if _, err := bea.Exec(ctx, query); pgCode(err) != "57014" || time.Since(start) > 10*time.Second {
		t.Errorf("a distributed statement past its timeout: %v after %s", err, time.Since(start))
	}
	deadline = time.Now().Add(10 * time.Second)
	for _, canceled := ds.gate.counts(); canceled < 2 && time.Now().Before(deadline); _, canceled = ds.gate.counts() {
		time.Sleep(10 * time.Millisecond)
	}
	if waiting, canceled := ds.gate.counts(); waiting != 0 || canceled < 2 {
		t.Errorf("after the timeout, %d tasks still wait and %d were canceled", waiting, canceled)
	}
	if n := ctl.Running("role batch"); n != 0 {
		t.Errorf("%d statements of role batch still run", n)
	}
}

// arrowRecorder is an ArrowWriter, as Flight SQL's, that keeps the schema
// and row count of what it receives.
type arrowRecorder struct {
	schema string
	rows   int64
	text   int
}

func (a *arrowRecorder) Columns([]Column) error { return nil }
func (a *arrowRecorder) Row([]any) error        { a.text++; return nil }
func (a *arrowRecorder) Complete(string) error  { return nil }
func (a *arrowRecorder) Records(rdr array.RecordReader) (int64, error) {
	a.schema = rdr.Schema().String()
	for rdr.Next() {
		a.rows += rdr.RecordBatch().NumRows()
	}
	return a.rows, rdr.Err()
}

// TestDistributedArrow checks a distributed statement read as Arrow
// record batches, as Flight SQL reads statements: the same schema and rows
// as on one machine.
func TestDistributedArrow(t *testing.T) {
	ds := startDistServer(t, 2)
	ctx := context.Background()
	user, err := ds.store.Get(ctx, "igloo")
	if err != nil {
		t.Fatal(err)
	}
	es, err := ds.srv.OpenEmbedded(ctx, user, "flight")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	query := "SELECT amount, count(*) AS n, sum(id) AS s, avg(id) AS a FROM lake.public.orders GROUP BY amount"
	run := func(mode string) *arrowRecorder {
		if err := es.Run(ctx, "SET distributed_execution = "+mode, &arrowRecorder{}); err != nil {
			t.Fatal(err)
		}
		rec := &arrowRecorder{}
		if err := es.Run(ctx, query, rec); err != nil {
			t.Fatal(err)
		}
		return rec
	}
	local, dist := run("off"), run("on")
	if dist.schema != local.schema || dist.rows != local.rows || dist.rows != 9 || dist.text != 0 {
		t.Errorf("distributed: %d rows (%d as text) of %s\nlocal: %d rows of %s", dist.rows, dist.text, dist.schema, local.rows, local.schema)
	}
	if n := lastWorkers(t, ds.history, query); n != 2 {
		t.Errorf("%d workers", n)
	}
}
