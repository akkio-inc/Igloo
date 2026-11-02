package pgwire

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	icebergio "github.com/apache/iceberg-go/io"
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

// TestDistributedPolicies checks statements that masking and row access
// policies apply to on workers, for every kind of user: each gives what it
// gives on one machine, and no stage of its plan writes a raw value of a
// masked column, or anything of a row the policy hides, to an exchange or
// to the coordinator, however the statement aggregates, joins, sorts or
// windows them (see distributed's governed.go).
func TestDistributedPolicies(t *testing.T) {
	t.Parallel()
	ds := startDistServer(t, 3)
	for _, sql := range policyFixture {
		mustExec(t, ds.admin, sql)
	}
	// Enough rows that every worker gets some: acme's even ids, globex's
	// odd ones, whose spend is 10^9 and more, so that a hidden row's
	// contribution to a sum shows.
	for _, sql := range []string{
		`INSERT INTO lake.crm.contacts SELECT i, CASE WHEN i % 2 = 0 THEN 'acme' ELSE 'globex' END,
			'user' || i || CASE WHEN i % 2 = 0 THEN '@acme.com' ELSE '@globex.com' END, 'dev-' || i,
			CASE WHEN i % 2 = 0 THEN i ELSE 1000000000 + i END FROM range(5, 3000) t(i)`,
		"CREATE TABLE lake.public.visits_by (id INTEGER, contact_id INTEGER, amount INTEGER)",
		"INSERT INTO lake.public.visits_by SELECT i, i % 3000, i % 13 FROM range(9000) t(i)",
		"CREATE VIEW memory.main.contact_view AS SELECT id, client, email, spend FROM lake.crm.contacts",
	} {
		mustExec(t, ds.admin, sql)
	}
	conns := map[string]*pgx.Conn{"igloo": ds.admin}
	for _, u := range []struct{ name, roles string }{
		{"rita", "acme_team"}, {"pia", "acme_team, pii_reader"}, {"gus", "globex_team"}, {"nemo", ""},
	} {
		conns[u.name] = ds.user(t, u.name, auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
		for _, role := range strings.Split(u.roles, ", ") {
			if role != "" {
				mustExec(t, ds.admin, "GRANT "+role+" TO "+u.name)
			}
		}
	}
	// What no stage may write for each user: masked values, and the rows
	// the row access policy hides, by their client and their spend.
	hidden := map[string][]string{
		"igloo": {"@"},
		"rita":  {"@", "globex", "spend>=1e9"},
		"pia":   {"globex", "spend>=1e9"},
		"gus":   {"@", "acme"},
		"nemo":  {"@", "acme", "globex", "spend>=1e9"},
	}
	// The same statements over a copy of the table without policies show
	// what workers would do with them: wherever they run one there, they
	// run it through the policies too, for everyone.
	for _, sql := range []string{
		"CREATE TABLE lake.public.contacts_plain (id INTEGER, client VARCHAR, email VARCHAR, device_id VARCHAR, spend INTEGER)",
		`INSERT INTO lake.public.contacts_plain SELECT i, CASE WHEN i % 2 = 0 THEN 'acme' ELSE 'globex' END,
			'user' || i || CASE WHEN i % 2 = 0 THEN '@acme.com' ELSE '@globex.com' END, 'dev-' || i,
			CASE WHEN i % 2 = 0 THEN i ELSE 1000000000 + i END FROM range(1, 3000) t(i)`,
		"CREATE VIEW memory.main.contact_plain_view AS SELECT id, client, email, spend FROM lake.public.contacts_plain",
	} {
		mustExec(t, ds.admin, sql)
	}
	plain := strings.NewReplacer("lake.crm.contacts", "lake.public.contacts_plain", "memory.main.contact_view", "memory.main.contact_plain_view")
	queries := []string{
		// A masked column, aggregated every way that could show it.
		"SELECT count(DISTINCT email), min(email), max(email), count(*) FROM lake.crm.contacts",
		"SELECT client, count(DISTINCT email), max(email), sum(spend)::BIGINT FROM lake.crm.contacts GROUP BY client",
		"SELECT email, count(*) FROM lake.crm.contacts GROUP BY email ORDER BY 2 DESC, 1 LIMIT 3",
		"SELECT count(*) FROM lake.crm.contacts WHERE email = '***'",
		"SELECT string_agg(email, ',' ORDER BY id) FROM lake.crm.contacts WHERE id < 40",
		// Rows the row access policy hides count nowhere.
		"SELECT count(*), sum(spend)::BIGINT FROM lake.crm.contacts WHERE client = 'globex'",
		"SELECT DISTINCT client FROM lake.crm.contacts",
		"SELECT id, email, spend FROM lake.crm.contacts ORDER BY spend DESC, id LIMIT 5",
		// Through a view and a common table expression.
		"SELECT count(DISTINCT email), sum(spend)::BIGINT FROM memory.main.contact_view",
		"WITH big AS (SELECT client, email, spend FROM lake.crm.contacts WHERE spend > 100) SELECT client, count(*), max(email) FROM big GROUP BY 1",
		// Joined to a table without policies, and to itself by a masked
		// column; in, and not in, a subquery.
		"SELECT c.client, count(*), sum(v.amount)::BIGINT FROM lake.crm.contacts c JOIN lake.public.visits_by v ON v.contact_id = c.id GROUP BY 1",
		"SELECT c.email, count(*) FROM lake.public.visits_by v JOIN lake.crm.contacts c ON v.contact_id = c.id GROUP BY 1",
		"SELECT count(*) FROM lake.crm.contacts a JOIN lake.crm.contacts b ON a.email = b.email AND a.id = b.id",
		"SELECT count(*) FROM lake.public.visits_by WHERE contact_id IN (SELECT id FROM lake.crm.contacts WHERE email LIKE '%@acme.com')",
		"SELECT count(*), sum(v.amount)::BIGINT FROM lake.public.visits_by v WHERE NOT EXISTS (SELECT 1 FROM lake.crm.contacts c WHERE c.id = v.contact_id)",
		// Windows, and a union.
		"SELECT client, id, email FROM lake.crm.contacts QUALIFY row_number() OVER (PARTITION BY client ORDER BY spend DESC) <= 2",
		"SELECT email FROM lake.crm.contacts WHERE id < 20 UNION ALL SELECT email FROM lake.crm.contacts WHERE id > 2980",
	}
	ctx := context.Background()
	// Once as the table sizes have it, and once with every join partitioned
	// by its key, as joins of large tables are: what each table's rows go
	// through then is an exchange.
	raw := 0
	for _, broadcast := range []int64{0, 4096} {
		ds.srv.dist.SetMaxBroadcast(broadcast)
		onWorkers := map[string]int{}
		for _, query := range queries {
			mustExec(t, ds.admin, "SET distributed_execution = on")
			sortedTexts(t, ds.admin, plain.Replace(query))
			plainOn := lastQuery(t, ds.catalogServer, "igloo", "SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ("+plain.Replace(query)).Workers > 0
			if values, _ := ds.stageValues(t, ctx, "igloo", plain.Replace(query)); slices.ContainsFunc(values, func(v string) bool { return hiddenIn(v, hidden["igloo"]) != "" }) {
				raw++
			}
			for _, name := range slices.Sorted(maps.Keys(conns)) {
				conn := conns[name]
				mustExec(t, conn, "SET distributed_execution = off")
				want := sortedTexts(t, conn, query)
				mustExec(t, conn, "SET distributed_execution = on")
				got := sortedTexts(t, conn, query)
				if !slices.Equal(got, want) {
					t.Errorf("%s, %s: on workers %q, on one machine %q", name, query, got, want)
				}
				run := lastQuery(t, ds.catalogServer, name, "SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ("+query)
				values, local := ds.stageValues(t, ctx, name, query)
				if run.Workers > 0 {
					onWorkers[query]++
				} else if plainOn {
					t.Errorf("%s, %s: on one machine, though it runs on workers without policies: %v", name, query, local)
				}
				if len(run.Policies) == 0 {
					t.Errorf("%s, %s: no policies applied", name, query)
				}
				for _, v := range values {
					if leak := hiddenIn(v, hidden[name]); leak != "" {
						t.Errorf("%s, %s: a stage writes %q (%s)", name, query, v, leak)
						break
					}
				}
			}
		}
		t.Logf("broadcast %d: on workers, of %d users: %v", broadcast, len(conns), onWorkers)
	}
	ds.srv.dist.SetMaxBroadcast(0)
	if raw == 0 {
		t.Error("no stage of the statements over the table without policies wrote a raw value: the check of the stages sees nothing")
	}
	rita := conns["rita"]

	// Tables without policies still run on workers, for everyone: forced by
	// an admin, on every worker; by anyone else, on as many as the table's
	// size calls for (one here).
	orders := "SELECT amount, count(*) FROM lake.public.orders GROUP BY amount"
	for _, c := range []struct {
		conn    *pgx.Conn
		user    string
		workers int
	}{{rita, "rita", 1}, {ds.admin, "igloo", 3}} {
		expectAllowed(t, c.conn, orders)
		if q := lastQuery(t, ds.catalogServer, c.user, orders); q.Workers != c.workers {
			t.Errorf("%s's statement over a table without policies: %d workers, want %d", c.user, q.Workers, c.workers)
		}
	}

	// Statements that only spell a protected table's name, in a column, a
	// string or the text of a view they read, which the rewriter analyzes
	// and finds read none, run on workers as written, through the views it
	// expanded too; one that reads the protected table besides runs there
	// through its policies.
	for _, sql := range []string{
		"CREATE TABLE lake.public.visits (id INTEGER, contacts VARCHAR, spend INTEGER)",
		"INSERT INTO lake.public.visits SELECT i, 'c' || (i % 7), i FROM range(3000) t(i)",
		"CREATE VIEW memory.main.visit_view AS SELECT id, contacts AS who, spend FROM lake.public.visits",
	} {
		mustExec(t, ds.admin, sql)
	}
	for _, c := range []struct {
		query   string
		workers bool
	}{
		{"SELECT contacts, count(*), sum(spend)::BIGINT FROM lake.public.visits GROUP BY 1 ORDER BY 1", true},
		{"SELECT who, count(*), max(spend) FROM memory.main.visit_view GROUP BY 1 ORDER BY 1", true},
		{"SELECT count(*), sum(spend)::BIGINT FROM lake.public.visits WHERE contacts <> 'contacts'", true},
		{"SELECT v.contacts, count(*) FROM lake.public.visits v JOIN lake.crm.contacts c ON c.id = v.id GROUP BY 1 ORDER BY 1", true},
	} {
		mustExec(t, rita, "SET distributed_execution = off")
		want := texts(t, rita, c.query)
		mustExec(t, rita, "SET distributed_execution = on")
		if got := texts(t, rita, c.query); strings.Join(got, ",") != strings.Join(want, ",") || len(want) == 0 {
			t.Errorf("%s: got %q, want %q", c.query, got, want)
		}
		q := lastQuery(t, ds.catalogServer, "rita", c.query)
		if (q.Workers > 0) != c.workers || strings.Contains(c.query, "crm.contacts") != (len(q.Policies) > 0) {
			t.Errorf("%s: %d workers, policies %q; want on workers %v", c.query, q.Workers, q.Policies, c.workers)
		}
		explain := strings.Join(texts(t, rita, "EXPLAIN DISTRIBUTED "+c.query), "\n")
		if strings.Contains(explain, "workers") != c.workers {
			t.Errorf("EXPLAIN DISTRIBUTED %s:\n%s", c.query, explain)
		}
	}

	// Should a statement reach the planner without the rewriter having
	// seen it, the planner still refuses a protected table.
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
	t.Parallel()
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
	for user, query := range map[string][2]string{"nina": {orders, "lake.public.orders"}, "nils": {leads, "lake.sales.leads"}} {
		if q := lastQuery(t, ds.catalogServer, user, query[0]); q.Workers != 1 || strings.Join(q.TablesRead, ",") != query[1] {
			t.Errorf("%s's granted table: %d workers, read %q", user, q.Workers, q.TablesRead)
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
	t.Parallel()
	needExtensions(t)
	pg := remoteFromEnv(t, "IGLOO_TEST_DATABASE_URL")
	schema := pgFixture(t, pg)
	store := federation.NewMemoryStore()
	c := federation.New("crm", federation.Postgres)
	c.Host, c.Port, c.User, c.Password, c.Database = pg.host, pg.port, pg.user, pg.password, pg.database
	c.TLS = federation.TLSDisable // the test databases have no TLS
	c.Schemas = []string{schema}
	if err := store.Create(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	dir, spill := t.TempDir(), filepath.Join(t.TempDir(), "spill")
	source := &lakeSource{dir: dir, files: map[string][]distributed.File{}}
	var addrs []string
	for i := range 2 {
		addr, _, _ := startTestWorker(t, dir, i, nil)
		addrs = append(addrs, addr)
	}
	co, err := distributed.NewCoordinator(distributed.Options{
		Source: source, Secret: testWorkerSecret, Version: "test", TempDir: spill, Cores: 1, Storage: distributed.NewStorage(icebergio.LocalFS{}),
		// Workers complete the groups of the table's columns through an
		// exchange there.
		Warehouse: dir + "/",
		Pool:      distributed.PoolOptions{Addresses: addrs},
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
	t.Parallel()
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
	t.Parallel()
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

	// The workers hold bea's task (one: she is no admin, and the table is
	// small); meanwhile her statement holds the role's one slot and her
	// memory reservation.
	release := ds.gate.hold()
	var wg sync.WaitGroup
	var beaErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, beaErr = bea.Exec(ctx, query)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for waiting, _ := ds.gate.counts(); waiting < 1 && time.Now().Before(deadline); waiting, _ = ds.gate.counts() {
		time.Sleep(10 * time.Millisecond)
	}
	if n := ctl.Running("role batch"); n != 1 {
		t.Errorf("%d statements of role batch run while the workers hold bea's", n)
	}
	// What runs on workers takes none of this replica's cores from others.
	if n := ds.srv.othersRunning(nil); n != 0 {
		t.Errorf("%d statements run here while the workers hold bea's", n)
	}
	mustExec(t, bo, "SET statement_queued_timeout = '300ms'")
	if _, err := bo.Exec(ctx, "SELECT 1"); pgCode(err) != "57014" || !strings.Contains(err.Error(), "the limit of 1 statement at once for role batch") {
		t.Errorf("bo's statement while bea's runs on workers: %v", err)
	}
	mustExec(t, cy, "SET statement_queued_timeout = '300ms'")
	if _, err := cy.Exec(ctx, "SELECT 2"); pgCode(err) != "57014" || !strings.Contains(err.Error(), "memory") {
		t.Errorf("cy's reservation while bea's runs on workers: %v", err)
	}
	release()
	wg.Wait()
	if beaErr != nil {
		t.Fatal(beaErr)
	}
	q := lastQuery(t, ds.catalogServer, "bea", query)
	if q.Workers != 1 || q.Pool != "reporting" || q.QueryTag != "client:acme" || q.Outcome != audit.OutcomeOK {
		t.Errorf("bea's distributed statement: workers %d, pool %q, tag %q, %s", q.Workers, q.Pool, q.QueryTag, q.Outcome)
	}
	// What it read is worked out from the statement she sent, as for any
	// statement, in the background.
	deadline = time.Now().Add(20 * time.Second)
	for len(q.TablesRead) == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		q = lastQuery(t, ds.catalogServer, "bea", query)
	}
	if strings.Join(q.TablesRead, ",") != "lake.public.orders" || len(q.TablesWritten) != 0 {
		t.Errorf("bea's distributed statement read %q and wrote %q", q.TablesRead, q.TablesWritten)
	}

	// statement_timeout ends the statement and its tasks. On a busy machine
	// it can run out before the workers have the task, which leaves none to
	// end: then the statement runs again with twice the timeout.
	release = ds.gate.hold()
	defer release()
	for timeout := 500 * time.Millisecond; ; timeout *= 2 {
		mustExec(t, bea, fmt.Sprintf("SET statement_timeout = '%dms'", timeout.Milliseconds()))
		start := time.Now()
		if _, err := bea.Exec(ctx, query); pgCode(err) != "57014" || time.Since(start) > timeout+10*time.Second {
			t.Errorf("a distributed statement past its timeout of %s: %v after %s", timeout, err, time.Since(start))
		}
		deadline = time.Now().Add(10 * time.Second)
		for waiting, canceled := ds.gate.counts(); (waiting != 0 || canceled < 1) && time.Now().Before(deadline); waiting, canceled = ds.gate.counts() {
			time.Sleep(10 * time.Millisecond)
		}
		waiting, canceled := ds.gate.counts()
		if waiting == 0 && canceled == 0 && timeout < 8*time.Second {
			continue
		}
		if waiting != 0 || canceled < 1 {
			t.Errorf("after the timeout, %d tasks still wait and %d were canceled", waiting, canceled)
		}
		break
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
	t.Parallel()
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

// stageValues plans query as user does, on every worker, and runs each of
// its stages as one task over the whole of its inputs, in order, on a
// connection of the replica's that no policy applies to: their tables'
// files are the tables, their exchanges what the stages before them wrote.
// It returns every value the stages write, as text, or why the statement
// does not run on workers.
func (ds *distServer) stageValues(t *testing.T, ctx context.Context, name, query string) ([]string, error) {
	t.Helper()
	user, err := ds.store.Get(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	es, err := ds.srv.OpenEmbedded(ctx, user, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	if _, err := es.sess.conn.ExecContext(ctx, "SET search_path = 'lake.public,memory.main'", nil); err != nil {
		t.Fatal(err)
	}
	st, err := ds.srv.prepareChecked(ctx, es.sess, user, query, nil)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer st.close() //nolint:errcheck
	plan, err := ds.srv.dist.Plan(ctx, st.query, &sessionEnv{s: ds.srv, sess: es.sess, user: user, st: st}, distributed.ModeOn)
	if err != nil {
		if !distributed.IsUnsupported(err) {
			t.Errorf("planning %s: %v", query, err)
		}
		return nil, err
	}
	conn, err := ds.srv.engine.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	exec := func(q string) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, q, nil); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	var out []string
	made := map[string]bool{}
	for _, s := range plan.Stages {
		for _, in := range s.Inputs {
			var from string
			switch {
			case in.Table != nil:
				from = "lake." + quoteIdent(in.Table.Schema) + "." + quoteIdent(in.Table.Name)
			case in.Exchange != nil:
				from = fmt.Sprintf("__sim_x%d", in.Exchange.ID)
			default:
				t.Fatalf("stage %d of %s reads %s, neither a table nor an exchange", s.ID, query, in.View)
			}
			exec("CREATE OR REPLACE TEMP VIEW " + quoteIdent(in.View) + " AS SELECT * FROM " + from)
		}
		q := "SELECT * FROM (" + s.SQL + ") AS s"
		if len(s.Columns) > 0 {
			cols := make([]string, len(s.Columns))
			for i, c := range s.Columns {
				cols[i] = quoteIdent(c)
			}
			q += "(" + strings.Join(cols, ", ") + ")"
		}
		target := fmt.Sprintf("__sim_s%d", s.ID)
		if s.Out != nil {
			target = fmt.Sprintf("__sim_x%d", s.Out.ID)
		}
		if made[target] {
			// Another branch of a union writes it too.
			exec("INSERT INTO " + target + " " + q)
		} else {
			exec("CREATE TEMP TABLE " + target + " AS " + q)
			made[target] = true
		}
		rows, err := conn.QueryContext(ctx, "SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ("+q+") AS q", nil)
		if err != nil {
			t.Fatalf("stage %d of %s: %v\n%s", s.ID, query, err, s.SQL)
		}
		cols := rows.Columns()
		dest := make([]driver.Value, len(cols))
		for rows.Next(dest) == nil {
			for _, v := range dest {
				if v != nil {
					out = append(out, fmt.Sprint(v))
				}
			}
		}
		_ = rows.Close()
	}
	return out, nil
}

// hiddenIn returns which of hidden, substrings or "spend>=1e9", value
// shows, or "".
func hiddenIn(value string, hidden []string) string {
	for _, h := range hidden {
		if h == "spend>=1e9" {
			if f, err := strconv.ParseFloat(value, 64); err == nil && f >= 1e9 && f < 1e15 {
				return h
			}
			continue
		}
		if strings.Contains(value, h) {
			return h
		}
	}
	return ""
}
