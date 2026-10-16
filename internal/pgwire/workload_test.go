package pgwire

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/workload"
)

// lastQuery is the newest history record of user whose text has prefix.
func lastQuery(t *testing.T, cs *catalogServer, user, prefix string) audit.Query {
	t.Helper()
	qs, err := cs.history.Queries(context.Background(), audit.Viewer{All: true}, audit.Page{})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range qs {
		if q.User == user && strings.HasPrefix(q.Text, prefix) {
			return q
		}
	}
	t.Fatalf("no statement of %s starting %q in the history", user, prefix)
	return audit.Query{}
}

func TestQueryTags(t *testing.T) {
	cs := startCatalogServer(t, func(o *Options) { o.Pool = "acme" })
	ctx := context.Background()
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := cs.testServer.admin(t, mode)
			var tag string
			if err := conn.QueryRow(ctx, "SHOW query_tag").Scan(&tag); err != nil || tag != "" {
				t.Fatalf("no tag yet: %q %v", tag, err)
			}
			mustExec(t, conn, "SET query_tag = 'client:acme;campaign:q4'")
			if err := conn.QueryRow(ctx, "SHOW query_tag").Scan(&tag); err != nil || tag != "client:acme;campaign:q4" {
				t.Fatalf("after SET: %q %v", tag, err)
			}
			mustExec(t, conn, "SELECT 42 AS tagged_"+name)
			mustExec(t, conn, "ALTER SESSION SET QUERY_TAG = 'client:it''s'")
			if err := conn.QueryRow(ctx, "SHOW query_tag").Scan(&tag); err != nil || tag != "client:it's" {
				t.Fatalf("ALTER SESSION SET: %q %v", tag, err)
			}
			mustExec(t, conn, "RESET query_tag")
			mustExec(t, conn, "SELECT 43 AS untagged_"+name)
			if _, err := conn.Exec(ctx, "SET query_tag = 'a'; SELECT 1"); pgCode(err) != "42601" {
				t.Errorf("a tag followed by more SQL: %v", err)
			}
		})
	}
	q := lastQuery(t, cs, "igloo", "SELECT 42 AS tagged_simple")
	if q.QueryTag != "client:acme;campaign:q4" || q.Pool != "acme" || q.Resources == nil || q.Resources.CPUTime <= 0 {
		t.Errorf("tagged statement: tag %q, pool %q, resources %+v", q.QueryTag, q.Pool, q.Resources)
	}
	if q := lastQuery(t, cs, "igloo", "SELECT 43"); q.QueryTag != "" {
		t.Errorf("after RESET: tag %q", q.QueryTag)
	}
}

func TestRoleSettings(t *testing.T) {
	cs := startCatalogServer(t)
	ctx := context.Background()
	mustExec(t, cs.admin, "CREATE ROLE acme")
	ana := cs.user(t, "ana", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme TO ana")

	for _, sql := range []string{
		"ALTER ROLE acme SET query_tag = 'client:acme'",
		"ALTER ROLE reader SET statement_timeout TO '1h'",
		"ALTER ROLE acme SET max_concurrent_statements = 4",
		"ALTER USER ana SET memory_limit = '2GB'",
	} {
		mustExec(t, cs.admin, sql)
	}
	expectDenied(t, ana, "ALTER ROLE acme SET query_tag = 'client:other'", "admin")
	for sql, code := range map[string]string{
		"ALTER ROLE acme SET work_mem = '4MB'":                "42704",
		"ALTER ROLE acme SET max_concurrent_statements = 0":   "22023",
		"ALTER ROLE acme SET memory_limit = 'plenty'":         "22023",
		"ALTER ROLE acme SET statement_timeout = 'soon'":      "22023",
		"ALTER ROLE nobody SET query_tag = 'x'":               "42704",
		"ALTER USER acme SET query_tag = 'x'":                 "42704",
		"ALTER ROLE acme SET query_tag 'x'":                   "42601",
		"ALTER ROLE acme SET max_concurrent_statements = 1.5": "42601",
		"ALTER ROLE acme RESET query_tag, statement_timeout":  "42601",
	} {
		if _, err := cs.admin.Exec(ctx, sql); pgCode(err) != code {
			t.Errorf("%s: want %s, got %v", sql, code, err)
		}
	}

	// ana's sessions take their default tag from their role.
	conn := connect(t, cs.url("ana", "ana-password"), pgx.QueryExecModeSimpleProtocol)
	var tag, timeout string
	if err := conn.QueryRow(ctx, "SHOW query_tag").Scan(&tag); err != nil || tag != "client:acme" {
		t.Errorf("default tag: %q %v", tag, err)
	}
	if err := conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&timeout); err != nil || timeout != "1h" {
		t.Errorf("timeout: %q %v", timeout, err)
	}
	// A session can lower the limit but not lift it.
	mustExec(t, conn, "SET statement_timeout = '5min'")
	_ = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&timeout)
	mustExec(t, conn, "SET statement_timeout = 0")
	var lifted string
	_ = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&lifted)
	if timeout != "5min" || lifted != "1h" {
		t.Errorf("session timeouts: %q, then %q", timeout, lifted)
	}

	rows, err := conn.Query(ctx, "SHOW ROLE SETTINGS")
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	for rows.Next() {
		var kind, target, setting, value, by string
		var at time.Time
		if err := rows.Scan(&kind, &target, &setting, &value, &by, &at); err != nil {
			t.Fatal(err)
		}
		seen = append(seen, kind+" "+target+" "+setting+"="+value)
	}
	rows.Close()
	want := "built-in role reader statement_timeout=1h,role acme max_concurrent_statements=4,role acme query_tag=client:acme,user ana memory_limit=2GB"
	if strings.Join(seen, ",") != want {
		t.Errorf("SHOW ROLE SETTINGS:\n%s\nwant\n%s", strings.Join(seen, ","), want)
	}

	// Changes reach open sessions; RESET ALL clears a target.
	mustExec(t, cs.admin, "ALTER ROLE acme RESET query_tag")
	if err := conn.QueryRow(ctx, "SHOW query_tag").Scan(&tag); err != nil || tag != "" {
		t.Errorf("after RESET: %q %v", tag, err)
	}
	mustExec(t, cs.admin, "ALTER ROLE acme SET query_tag = DEFAULT")
	mustExec(t, cs.admin, "ALTER ROLE acme RESET ALL")
	mustExec(t, cs.admin, "DROP USER ana")
	settings, _ := cs.grants.Settings(ctx)
	if len(settings) != 1 || settings[0].Target != "reader" {
		t.Errorf("settings left: %+v", settings)
	}
}

// TestProfilingStaysOutOfTheWay checks that the profiler sessions run with
// changes nothing clients see, and measures only the statement it should.
func TestProfilingStaysOutOfTheWay(t *testing.T) {
	cs := startCatalogServer(t)
	ctx := context.Background()
	var kind, plan string
	if err := cs.admin.QueryRow(ctx, "EXPLAIN ANALYZE SELECT count(*) FROM range(1000)").Scan(&kind, &plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "RANGE") {
		t.Errorf("EXPLAIN ANALYZE: %s %s", kind, plan)
	}
	if _, err := cs.admin.Exec(ctx, "SELECT 1/0 AS failed, error('boom')"); err == nil {
		t.Fatal("expected an error")
	}
	mustExec(t, cs.admin, "SELECT 42 AS measured")
	if q := lastQuery(t, cs, "igloo", "SELECT 1/0"); q.Resources != nil {
		t.Errorf("a failed statement has resources: %+v", q.Resources)
	}
	if q := lastQuery(t, cs, "igloo", "SELECT 42"); q.Resources == nil {
		t.Error("a statement after a failed one was not measured")
	}
	// Igloo's own commands are not measured.
	mustExec(t, cs.admin, "SET query_tag = 'x'")
	if q := lastQuery(t, cs, "igloo", "SET query_tag"); q.Resources != nil {
		t.Errorf("SET query_tag has resources: %+v", q.Resources)
	}
}

func TestRoleStatementTimeout(t *testing.T) {
	cs := startCatalogServer(t)
	ctx := context.Background()
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "ALTER ROLE reader SET statement_timeout = '200ms'")
	start := time.Now()
	_, err := rita.Exec(ctx, slowQuery)
	if pgCode(err) != "57014" || time.Since(start) > 10*time.Second {
		t.Errorf("a reader's slow query: %v after %s", err, time.Since(start))
	}
	// Admins are not readers.
	mustExec(t, cs.admin, "SELECT count(*) FROM range(1000)")
}

func TestRoleConcurrencyLimit(t *testing.T) {
	ctl := workload.NewController(0)
	cs := startCatalogServer(t, func(o *Options) { o.Workload = ctl })
	ctx := context.Background()
	mustExec(t, cs.admin, "CREATE ROLE batch")
	var conns []*pgx.Conn
	for _, name := range []string{"bea", "bo"} {
		conns = append(conns, cs.user(t, name, auth.RoleReader, pgx.QueryExecModeSimpleProtocol))
		mustExec(t, cs.admin, "GRANT batch TO "+name)
	}
	mustExec(t, cs.admin, "ALTER ROLE batch SET max_concurrent_statements = 1")

	// bea's slow query holds the role's one slot; bo's waits behind it
	// until its timeout, with a message that says so.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mustExec(t, conns[0], "SET statement_timeout = '3s'")
		_, _ = conns[0].Exec(ctx, slowQuery)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for ctl.Running("role batch") == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	mustExec(t, conns[1], "SET statement_timeout = '300ms'")
	_, err := conns[1].Exec(ctx, "SELECT 1")
	if pgCode(err) != "57014" || !strings.Contains(err.Error(), "the limit of 1 statement at once for role batch") {
		t.Errorf("queued past its timeout: %v", err)
	}
	// Once the slot is free, statements run, and the history says how
	// long they waited.
	mustExec(t, conns[1], "SET statement_timeout = '30s'")
	if _, err := conns[1].Exec(ctx, "SELECT 2 AS after_wait"); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	q := lastQuery(t, cs, "bo", "SELECT 2 AS after_wait")
	if q.Outcome != audit.OutcomeOK || q.Queued <= 0 {
		t.Errorf("waited statement: %+v", q)
	}
	// Other users are not held up.
	mustExec(t, cs.admin, "SELECT 3")
}

func TestMemoryReservations(t *testing.T) {
	ctl := workload.NewController(1 << 30)
	cs := startCatalogServer(t, func(o *Options) { o.Workload = ctl })
	ctx := context.Background()
	ana := cs.user(t, "ana", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "ALTER USER ana SET memory_limit = '600MiB'")

	// Another statement holds most of the replica's query memory.
	release, _, err := ctl.Admit(ctx, workload.Limits{Memory: 600 << 20})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, ana, "SET statement_timeout = '200ms'")
	if _, err := ana.Exec(ctx, "SELECT 1"); pgCode(err) != "57014" || !strings.Contains(err.Error(), "memory") {
		t.Errorf("a reservation that does not fit: %v", err)
	}
	// It starts once the memory is free.
	mustExec(t, ana, "SET statement_timeout = 0")
	go func() {
		time.Sleep(100 * time.Millisecond)
		release()
	}()
	mustExec(t, ana, "SELECT 1 AS reserved")
	if q := lastQuery(t, cs, "ana", "SELECT 1 AS reserved"); q.Queued < 50*time.Millisecond {
		t.Errorf("queued %s", q.Queued)
	}
	// Users without a reservation never wait for memory.
	release, _, _ = ctl.Admit(ctx, workload.Limits{Memory: 1 << 40})
	defer release()
	mustExec(t, cs.admin, "SELECT 2")
}
