package iceberg

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// The iceberg extension's catalog of tables is shared by every connection to
// a DuckDB, and Igloo's build patches it to be safe under concurrency (see
// patches/). TestCatalogStress checks that against the extension alone: a
// plain DuckDB, without Igloo or its catalog lock, binds, lists, renames,
// drops and swaps tables on many connections at once, against a catalog that
// answers each request a round trip late, and checks that binds load tables
// from it side by side rather than one at a time. DuckDB 1.5.6's own
// iceberg extension segfaults or wedges in a listing within seconds of this;
// run the test with IGLOO_TEST_ICEBERG_EXTENSION naming the official file
// (the pack's v1.5.6/linux_amd64/iceberg.duckdb_extension) to see it.

// stressChildEnv, when set, makes TestCatalogStress run the stress itself
// rather than spawn a subprocess to, so that a crash fails the test.
const stressChildEnv = "IGLOO_ICEBERG_STRESS_CHILD"

// stressTimeEnv sets how long the stress runs (a Go duration; default 20s).
const stressTimeEnv = "IGLOO_ICEBERG_STRESS_TIME"

// stall is how long a connection may go without finishing a statement before
// the stress counts the DuckDB as wedged.
const stall = time.Minute

// roundTrip is how late the catalog answers each request.
const roundTrip = 25 * time.Millisecond

func TestCatalogStress(t *testing.T) {
	ext := os.Getenv("IGLOO_TEST_ICEBERG_EXTENSION")
	if ext == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("IGLOO_TEST_ICEBERG_EXTENSION is not set: CI builds the iceberg extension (extensions/iceberg/build.sh)")
		}
		t.Skip("IGLOO_TEST_ICEBERG_EXTENSION is not set; build the extension with extensions/iceberg/build.sh")
	}
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR is not set (the avro extension the iceberg one reads manifests with)")
	}
	run := 20 * time.Second
	if v := os.Getenv(stressTimeEnv); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("%s: %v", stressTimeEnv, err)
		}
		run = d
	}
	if os.Getenv(stressChildEnv) != "" {
		catalogStress(t, ext, run)
		return
	}
	timeout := run + 3*time.Minute
	cmd := exec.Command(os.Args[0], "-test.run=^TestCatalogStress$", "-test.v", "-test.timeout="+timeout.String())
	cmd.Env = append(os.Environ(), stressChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatalf("the stress did not exit cleanly (a crash or a wedged connection): %v", err)
	}
}

// listings are what the listing connections rotate through: each walks the
// catalog's set of tables, as a BI tool browsing schemas does.
var listings = []string{
	`SELECT table_name FROM information_schema.tables WHERE table_catalog = 'lake'`,
	`SELECT table_name, column_name FROM information_schema.columns WHERE table_catalog = 'lake'`,
	`SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake'`,
	`SELECT table_name, column_name FROM duckdb_columns() WHERE database_name = 'lake'`,
	`SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 's'`,
	`SHOW ALL TABLES`,
	// Scans of both schemas, which DuckDB may start on several threads at once.
	`SELECT table_name FROM duckdb_tables() UNION ALL SELECT table_name FROM duckdb_columns() UNION ALL SELECT table_name FROM information_schema.tables`,
}

// tables is how many tables the binds and listings find in schema s, t0 to
// t15. The renaming connections each move their own share of them away and
// back. Schema s2 has tables of its own, u0 to u3, which only binds
// and listings read.
const tables = 16

// counts are the statements of each kind that succeeded and failed, and the
// first error.
type counts struct {
	ok, failed atomic.Int64
	first      atomic.Pointer[string]
}

func catalogStress(t *testing.T, ext string, run time.Duration) {
	// DuckDB's iceberg writer creates a relative directory named data.
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	cat := icebergtest.NewCatalog(filepath.Join(t.TempDir(), "warehouse"))
	defer cat.Close()
	// The catalog, a round trip away; loads counts the table loads it serves
	// at once, mostLoads the most it did.
	var loads, mostLoads atomic.Int64
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/namespaces/s/tables/") {
			n := loads.Add(1)
			defer loads.Add(-1)
			for m := mostLoads.Load(); n > m && !mostLoads.CompareAndSwap(m, n); m = mostLoads.Load() {
			}
		}
		time.Sleep(roundTrip)
		cat.ServeHTTP(w, r)
	}))
	defer slow.Close()
	endpoint := slow.URL + "/catalog"

	connector, err := duckdb.NewConnector("?allow_unsigned_extensions=true", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close() //nolint:errcheck
	ctx := context.Background()
	// A DuckDB built with the extension in it, as for a run under
	// AddressSanitizer, has it loaded already.
	var linked bool
	if err := db.QueryRowContext(ctx, "SELECT count(*) > 0 FROM duckdb_extensions() WHERE extension_name = 'iceberg' AND loaded").Scan(&linked); err != nil {
		t.Fatal(err)
	}
	load := "LOAD '" + ext + "'"
	if linked {
		load = "SELECT 1"
	}
	for _, s := range []string{
		"SET GLOBAL extension_directory = '" + os.Getenv("IGLOO_TEST_EXTENSION_DIR") + "'",
		"SET GLOBAL autoinstall_known_extensions = false",
		load,
		"ATTACH 'wh' AS lake (TYPE iceberg, ENDPOINT '" + endpoint + "', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none', PURGE_REQUESTED false, REMOVE_FILES_ON_DELETE false)",
		"CREATE SCHEMA lake.s",
		"CREATE SCHEMA lake.s2",
	} {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	for i := range 4 {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE lake.s2.u%d AS SELECT range AS id FROM range(%d)", i, i+1)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range tables {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE lake.s.t%d AS SELECT range AS id, 'row ' || range AS label FROM range(%d)", i, 10+i)); err != nil {
			t.Fatal(err)
		}
	}

	type worker struct {
		kind string
		n    int
		step func(conn *sql.Conn, r *rand.Rand, i int) error
	}
	query := func(conn *sql.Conn, q string) error {
		rows, err := conn.QueryContext(ctx, q)
		if err != nil {
			return err
		}
		for rows.Next() {
		}
		rows.Close() //nolint:errcheck
		return rows.Err()
	}
	do := func(conn *sql.Conn, stmts ...string) error {
		for _, s := range stmts {
			if _, err := conn.ExecContext(ctx, s); err != nil {
				return err
			}
		}
		return nil
	}
	const renamers = 4
	workers := []worker{
		// Binds: DuckDB looks each table up again on every run, replacing its
		// entry in the set.
		{"bind", 8, func(conn *sql.Conn, r *rand.Rand, _ int) error {
			if r.IntN(4) == 0 {
				return query(conn, fmt.Sprintf("SELECT count(*) FROM lake.s2.u%d", r.IntN(4)))
			}
			return query(conn, fmt.Sprintf("SELECT count(*), max(label) FROM lake.s.t%d", r.IntN(tables)))
		}},
		{"describe", 2, func(conn *sql.Conn, r *rand.Rand, _ int) error {
			return query(conn, fmt.Sprintf("DESCRIBE lake.s.t%d", r.IntN(tables)))
		}},
		{"list", 6, func(conn *sql.Conn, r *rand.Rand, _ int) error {
			return query(conn, listings[r.IntN(len(listings))])
		}},
		// Renames: each connection moves its own tables away and back.
		{"rename", renamers, func(conn *sql.Conn, r *rand.Rand, w int) error {
			i := w + renamers*r.IntN(tables/renamers)
			err := do(conn, fmt.Sprintf("ALTER TABLE lake.s.t%d RENAME TO moved%d", i, i))
			// Put it back, if it moved, trying again on a conflicting commit.
			back := fmt.Sprintf("ALTER TABLE IF EXISTS lake.s.moved%d RENAME TO t%d", i, i)
			for range 10 {
				if err2 := do(conn, back); err2 == nil {
					break
				} else if err == nil {
					err = err2
				}
			}
			return err
		}},
		// Creates and drops of tables of their own.
		{"create+drop", 2, func(conn *sql.Conn, r *rand.Rand, w int) error {
			name := fmt.Sprintf("lake.s.scratch%d_%d", w, r.IntN(1000))
			return do(conn, "CREATE TABLE "+name+" AS SELECT range AS id FROM range(5)", "DROP TABLE "+name)
		}},
		// dbt's table materialization: build beside, swap in by renames, drop the old.
		{"swap", 2, func(conn *sql.Conn, r *rand.Rand, w int) error {
			m := fmt.Sprintf("mart%d", w)
			return do(conn,
				"CREATE TABLE lake.s."+m+"__tmp AS SELECT range AS id FROM range(3)",
				"ALTER TABLE IF EXISTS lake.s."+m+" RENAME TO "+m+"__backup",
				"ALTER TABLE lake.s."+m+"__tmp RENAME TO "+m,
				"DROP TABLE IF EXISTS lake.s."+m+"__backup")
		}},
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	byKind := map[string]*counts{}
	type progress struct {
		name string
		last atomic.Int64
	}
	var all []*progress
	for _, w := range workers {
		c := &counts{}
		byKind[w.kind] = c
		for i := range w.n {
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			p := &progress{name: fmt.Sprintf("%s %d", w.kind, i)}
			p.last.Store(time.Now().UnixNano())
			all = append(all, p)
			r := rand.New(rand.NewPCG(uint64(len(all)), 1))
			wg.Go(func() {
				defer conn.Close() //nolint:errcheck
				for {
					select {
					case <-stop:
						return
					default:
					}
					// A statement may fail on a table another connection
					// renamed or dropped a moment before, or on a conflicting
					// commit; the stress counts those and goes on. It fails
					// only on a crash or a wedged connection.
					if err := w.step(conn, r, i); err != nil {
						c.failed.Add(1)
						msg := err.Error()
						c.first.CompareAndSwap(nil, &msg)
					} else {
						c.ok.Add(1)
					}
					p.last.Store(time.Now().UnixNano())
				}
			})
		}
	}

	deadline := time.Now().Add(run)
	for time.Now().Before(deadline) {
		time.Sleep(time.Second)
		for _, p := range all {
			if idle := time.Since(time.Unix(0, p.last.Load())); idle > stall {
				// Closing the DuckDB would wait for the wedged connection.
				_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
				fmt.Fprintf(os.Stderr, "%s has not finished a statement in %v: DuckDB is wedged\n", p.name, idle.Round(time.Second))
				os.Exit(3)
			}
		}
	}
	close(stop)
	wg.Wait()

	var summary []string
	for _, w := range workers {
		c := byKind[w.kind]
		line := fmt.Sprintf("%s %d ok %d failed", w.kind, c.ok.Load(), c.failed.Load())
		if first := c.first.Load(); first != nil {
			line += fmt.Sprintf(" (first: %.200s)", *first)
		}
		summary = append(summary, line)
		if c.ok.Load() == 0 {
			t.Errorf("no %s statement succeeded: the stress did not exercise it", w.kind)
		}
	}
	t.Logf("%d connections for %v:\n%s", len(all), run, strings.Join(summary, "\n"))

	// The set is whole: a listing returns, and every table it lists binds.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	rows, err := conn.QueryContext(ctx, "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 's' ORDER BY 1")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if err := query(conn, "SELECT count(*) FROM lake.s."+n); err != nil {
			t.Errorf("%s is listed but does not bind: %v", n, err)
		}
	}
	if want := cat.TableNames("s"); !slices.Equal(names, want) {
		t.Errorf("after the stress DuckDB lists %v, and the catalog has %v", names, want)
	}
	t.Logf("after the stress DuckDB lists the catalog's %d tables", len(names))
	// Binds of one schema's tables load them from the catalog side by side:
	// one at a time, they would wait for each other's round trips.
	t.Logf("the catalog served up to %d table loads at once", mostLoads.Load())
	if mostLoads.Load() < 2 {
		t.Errorf("the catalog never served two table loads at once: binds load tables one at a time")
	}
}
