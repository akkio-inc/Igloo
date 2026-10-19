package pgwire

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// A session listing the attached catalog's tables and views
// (information_schema, pg_catalog, duckdb_tables/views/columns, SHOW,
// DESCRIBE, or a missing-table suggestion) while another renames and drops
// tables on the same replica crashes the embedded DuckDB: in DuckDB 1.5.6
// with DuckDB-Iceberg the listing walks the catalog's one in-memory set of
// tables while a rename or drop frees from it without the lock the listing
// holds, and the process segfaults, taking down every session on the
// replica. Plain reads of the tables do the same harm another way: DuckDB
// binds them again on every run, replacing their entries in the set, and a
// rename or drop removing an entry at that moment corrupts it, so that a
// later listing segfaults or never returns. A BI tool browsing schemas, or a
// dashboard refreshing, while dbt swaps tables hits this. The engine
// serializes them (see engine.Engine.CatalogLock); these tests reproduce the
// crash and check the guard holds.

// catalogCrashQueries are listings a reader may send over the wire that
// crash the process when a table is renamed or dropped at the same time,
// each confirmed to segfault DuckDB 1.5.6 + DuckDB-Iceberg without the guard.
var catalogCrashQueries = []string{
	`SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics'`,
	`SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'analytics'`,
	`SELECT table_schema, table_name FROM information_schema.tables`,
	`SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'analytics'`,
	`SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid WHERE c.relname LIKE 'mart%'`,
	`SELECT table_name FROM duckdb_tables() WHERE schema_name = 'analytics'`,
	`SELECT view_name FROM duckdb_views()`,
	`SELECT table_name FROM duckdb_columns() WHERE schema_name = 'analytics'`,
	`SHOW ALL TABLES`,
}

// catalogCrashChildEnv, when set, makes TestCatalogListingSurvivesConcurrentDDL
// run the stress itself rather than spawn a subprocess to.
const catalogCrashChildEnv = "IGLOO_CATALOG_CRASH_CHILD"

// TestCatalogListingSurvivesConcurrentDDL runs the stress in a subprocess, so
// that a crash fails this test rather than taking the test binary down with
// it, and checks that it exits cleanly. Without the guard the subprocess
// segfaults, or wedges in a listing until its timeout, and exits non-zero;
// with it the subprocess completes every build and returns. Remove the guard
// (make Server.holdCatalog return a no-op), or only the bind hold
// (openSession's SetBindHold), to see this fail.
func TestCatalogListingSurvivesConcurrentDDL(t *testing.T) {
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	if os.Getenv(catalogCrashChildEnv) != "" {
		catalogCrashStress(t)
		return
	}
	// It takes seconds; a wedged listing runs into the timeout.
	cmd := exec.Command(os.Args[0], "-test.run=^TestCatalogListingSurvivesConcurrentDDL$", "-test.v", "-test.timeout=3m")
	cmd.Env = append(os.Environ(), catalogCrashChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("a reader listing the catalog while a writer swapped tables did not exit cleanly "+
			"(a crash here means the guard is gone): %v\n%s", err, out)
	}
}

// plainQueries are data reads. Their run holds no lock (see catalogRunLock),
// but DuckDB binds each again as it runs it, which looks the tables up in
// the catalog's in-memory set and replaces their entries there: without the
// lock around that bind (see the connection's bind hold in openSession) it
// races a rename's or drop's removal from the set, corrupting it, and a
// later listing spins forever or segfaults.
var plainQueries = []string{
	`SELECT run FROM "lake"."analytics"."mart"`,
	`SELECT count(*) FROM "lake"."analytics"."other_0"`,
	`SELECT count(*) FROM "lake"."analytics"."other_3"`,
	`SELECT count(*) FROM "lake"."analytics"."other_5"`,
	`SELECT count(*) FROM "lake"."analytics"."other_7"`,
}

// readerQueries are what the listing readers rotate through: the catalog
// listings that crash without the guard, and a few plain reads, so that
// both are exercised against the swaps.
var readerQueries = append(append([]string{}, catalogCrashQueries...), plainQueries[:3]...)

// catalogCrashStress is the body that runs in the subprocess: four readers
// list the catalog and read tables over and over as the reader role, and
// four more only read tables, while a writer swaps the mart table dbt-style
// (CTAS into __dbt_tmp, rename the old table to __dbt_backup, rename tmp
// into place, drop backup), appends to an incremental table, and the
// background view sync polls. Every build must complete, and once it has, a
// listing must show the mart table and never a dropped __dbt_tmp or
// __dbt_backup. A corrupted set shows as a crash or as a listing that never
// returns, which the subprocess's timeout turns into a failure.
func catalogCrashStress(t *testing.T) {
	const builds = 100
	const readers = 4
	const plainReaders = 4
	vc := newViewCatalog(t)
	r := vc.replica(t)
	mustExec(t, r.admin(t, pgx.QueryExecModeSimpleProtocol), "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	writer := r.dbt(t)
	mustExec(t, writer, `create schema if not exists "lake"."analytics"`)
	// A few other tables and a view, so listings have more than mart to walk.
	for i := range 8 {
		mustExec(t, writer, fmt.Sprintf(`create table "lake"."analytics"."other_%d" as select %d as run`, i, i))
	}
	mustExec(t, writer, `create view "lake"."analytics"."v" as select 1 as run`)
	// The background view sync, as it runs in production: through the engine
	// lock (see ddl.Manager.Run), not raw.
	syncing(t, func(ctx context.Context) (bool, error) {
		defer r.eng.CatalogLock()()
		return r.objects.PollViews(ctx)
	})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range readers + plainReaders {
		queries := readerQueries
		if i >= readers {
			queries = plainQueries
		}
		conn := connect(t, r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
		wg.Go(func() {
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				q := queries[n%len(queries)]
				rows, err := conn.Query(context.Background(), q)
				if err == nil {
					for rows.Next() {
					}
					rows.Close()
					err = rows.Err()
				}
				// A listing may fail transiently while a swap is in flight;
				// it must never crash the process. Errors are tolerated here,
				// so the only failures are a subprocess crash or the stale
				// listing the main goroutine checks for below.
				_ = err
			}
		})
	}

	mustExec(t, writer, `create table "lake"."analytics"."log" (run INTEGER)`)
	check := connect(t, r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	for i := 1; i <= builds; i++ {
		if err := dbtRun(writer, "mart", dbtTable("mart", fmt.Sprintf("select %d as run", i), i > 1, false)); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("build %d: %v", i, err)
		}
		// An INSERT commits a snapshot; its run holds no catalog lock, so this
		// exercises that path against the swaps and the readers.
		if _, err := writer.Exec(context.Background(), fmt.Sprintf(`INSERT INTO "lake"."analytics"."log" VALUES (%d)`, i)); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("build %d: insert: %v", i, err)
		}
		// The build is done; no swap is in flight on this connection, so the
		// listing must show mart and nothing left over from the swap.
		mart := martTables(t, check)
		if !slices.Equal(mart, []string{"mart"}) {
			close(stop)
			wg.Wait()
			t.Fatalf("build %d: a listing shows %v, want [mart]; a dropped table is still listed", i, mart)
		}
	}
	close(stop)
	wg.Wait()
}

// martTables lists the analytics tables whose name starts with mart, as
// information_schema has them.
func martTables(t *testing.T, conn *pgx.Conn) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'analytics' AND table_name LIKE 'mart%' ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestCatalogLockIsReentrantPerSession checks that holding the catalog lock
// for one step of handling a statement and then another, as preparing,
// binding, rechecking and running do, does not deadlock, and that the engine
// lock is held once, in the strongest mode the session's holds need: a step
// that needs it exclusively inside one that holds it shared has the session
// let go of it and take it exclusively, and back once that step is done. It
// also checks that withoutCatalog lets go of the lock while it waits.
func TestCatalogLockIsReentrantPerSession(t *testing.T) {
	cases := []struct {
		name  string
		steps func(s *Server, sess *session)
		want  []string
	}{
		{"exclusive in exclusive", func(s *Server, sess *session) {
			outer := s.holdCatalog(sess)
			s.holdCatalog(sess)()
			outer()
		}, []string{"lock", "unlock"}},
		{"shared in shared", func(s *Server, sess *session) {
			outer := s.shareCatalog(sess)
			s.shareCatalog(sess)()
			outer()
		}, []string{"rlock", "runlock"}},
		{"shared in exclusive", func(s *Server, sess *session) {
			outer := s.holdCatalog(sess)
			s.shareCatalog(sess)()
			outer()
		}, []string{"lock", "unlock"}},
		{"exclusive in shared", func(s *Server, sess *session) {
			outer := s.shareCatalog(sess)
			s.holdCatalog(sess)()
			outer()
		}, []string{"rlock", "runlock", "lock", "unlock", "rlock", "runlock"}},
		{"let go while waiting", func(s *Server, sess *session) {
			outer := s.holdCatalog(sess)
			s.withoutCatalog(sess, func() {
				e := s.engine.(*recordingEngine)
				if e.mu.TryLock() {
					e.events = append(e.events, "free")
					e.mu.Unlock()
				}
			})
			outer()
		}, []string{"lock", "unlock", "free", "lock", "unlock"}},
		{"released out of order", func(s *Server, sess *session) {
			first := s.holdCatalog(sess)
			second := s.shareCatalog(sess)
			first()
			second()
			second() // twice: a no-op
		}, []string{"lock", "unlock", "rlock", "runlock"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := &recordingEngine{}
			s := &Server{engine: e, catalog: "lake", log: slog.New(slog.DiscardHandler)}
			sess := &session{}
			done := make(chan struct{})
			go func() {
				defer close(done)
				c.steps(s, sess)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("nested catalog holds deadlocked")
			}
			if !slices.Equal(e.events, c.want) {
				t.Errorf("engine lock events %v, want %v", e.events, c.want)
			}
			if len(sess.catHolds) != 0 || sess.catHeld != catalogFree {
				t.Errorf("after releasing every hold the session still has %d, held %v", len(sess.catHolds), sess.catHeld)
			}
		})
	}
}

// recordingEngine is an Engine whose catalog lock is a real read/write lock
// that records how it is taken and released, for
// TestCatalogLockIsReentrantPerSession.
type recordingEngine struct {
	Engine
	mu     sync.RWMutex
	events []string
}

func (e *recordingEngine) CatalogLock() func() {
	e.mu.Lock()
	e.events = append(e.events, "lock")
	return func() { e.events = append(e.events, "unlock"); e.mu.Unlock() }
}

func (e *recordingEngine) CatalogShare() func() {
	e.mu.RLock()
	e.events = append(e.events, "rlock")
	return func() { e.events = append(e.events, "runlock"); e.mu.RUnlock() }
}

// (helpers newViewCatalog, viewReplica, syncing, dbtRun, dbtTable, connect,
// mustExec live in viewsync_test.go.)
