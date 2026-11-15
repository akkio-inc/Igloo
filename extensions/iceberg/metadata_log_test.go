package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// A transaction reads an Iceberg table as the table was when the
// transaction started. DuckDB-Iceberg finds that state in the table's
// metadata log, the list of its earlier metadata files and when each was
// written. When the table's latest metadata is newer than the transaction
// and no entry of the log is as old, the table did not exist yet when the
// transaction started (or changed more times since than its log keeps).
// DuckDB 1.5.6's extension threw an InternalException there. DuckDB fails
// only the statement when that happens as it prepares one, and invalidates
// its whole database when it happens as one runs, as in a listing of the
// catalog: every connection then fails with "database has been invalidated
// because of a previous fatal error" until the process restarts.
//
// TestTableNewerThanTransaction takes each way to that throw, against the
// extension alone in a DuckDB of its own, and checks that the statement
// fails with an error that ends only its transaction (patches/0006), which
// names the table and the time the transaction started, or, for a listing,
// shows the table at its current schema (patches/0007); that the DuckDB
// goes on serving the other connection and a new one; and that the same
// statement succeeds once the transaction starts again.
// internal/pgwire's test of the same name takes the way a pilot deployment
// met it, through Igloo.
func TestTableNewerThanTransaction(t *testing.T) {
	ext := testExtension(t)
	// DuckDB's iceberg writer creates a relative directory named data.
	t.Chdir(t.TempDir())

	// The other session makes t after the transaction began, and writes to
	// it once: t's metadata log then holds only its creation, which is
	// newer than the transaction.
	createAndInsert := []string{"CREATE TABLE lake.s.t (id INTEGER)", "INSERT INTO lake.s.t VALUES (1)"}
	cases := []struct {
		name string
		// begin is what the transaction runs after BEGIN and before the
		// other session changes t; nil runs stmt outside a transaction.
		begin []string
		// other is what another connection runs then: before stmt, or, for
		// a statement outside a transaction, while stmt loads t from the
		// catalog.
		other []string
		stmt  string
		// listed is what stmt returns if it does not fail.
		listed []string
		// rows is how many rows t has once stmt has run in a transaction
		// that starts after other.
		rows int
	}{
		// As a pilot deployment's transaction block ran: a MERGE into
		// another table, then the insert.
		{"insert after a merge", []string{"MERGE INTO lake.s.m USING (SELECT 2 AS id) src ON m.id = src.id WHEN NOT MATCHED THEN INSERT VALUES (src.id)"},
			createAndInsert, "INSERT INTO lake.s.t VALUES (2)", nil, 2},
		// The transaction starts at BEGIN.
		{"insert after BEGIN alone", []string{}, createAndInsert, "INSERT INTO lake.s.t VALUES (2)", nil, 2},
		{"read", []string{"SELECT count(*) FROM lake.s.m"}, createAndInsert, "SELECT count(*) FROM lake.s.t", nil, 1},
		// Without a snapshot t binds at its current schema; the insert then
		// copies t into the transaction, as of its start.
		{"insert after an ALTER", []string{"SELECT count(*) FROM lake.s.m"},
			[]string{"CREATE TABLE lake.s.t (id INTEGER)", "ALTER TABLE lake.s.t ADD COLUMN label VARCHAR"},
			"INSERT INTO lake.s.t VALUES (2, 'two')", nil, 1},
		// Outside a transaction a statement starts its own, before it loads
		// t: a table made and written in between is newer than it.
		{"read outside a transaction", nil, createAndInsert, "SELECT count(*) FROM lake.s.t", nil, 1},
		// The throws above come as DuckDB prepares the statement; this one
		// came as it ran, and invalidated the database. A listing shows each
		// table's entry in the set every connection shares, as of the
		// transaction's start, and the other session's read put t's latest
		// metadata there. It shows t, which has no schema that old, at its
		// current one.
		{"listing", []string{"SELECT count(*) FROM lake.s.m"}, append(createAndInsert, "SELECT count(*) FROM lake.s.t"),
			"SELECT column_name FROM duckdb_columns() WHERE database_name = 'lake' AND table_name = 't'", []string{"id"}, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			// Cleanups run last first: the connections, the DuckDB, then
			// the catalog.
			cat := icebergtest.NewCatalog(filepath.Join(t.TempDir(), "warehouse"))
			t.Cleanup(cat.Close)
			// onLoad, when set, runs once, as the catalog is asked for t.
			var onLoad atomic.Pointer[func()]
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/namespaces/s/tables/t") {
					if f := onLoad.Swap(nil); f != nil {
						(*f)()
					}
				}
				cat.ServeHTTP(w, r)
			}))
			t.Cleanup(srv.Close)
			db := openLake(t, ext, srv.URL+"/catalog")
			t.Cleanup(func() { _ = db.Close() })
			for _, s := range []string{"CREATE SCHEMA lake.s", "CREATE TABLE lake.s.m AS SELECT 1 AS id"} {
				if _, err := db.ExecContext(ctx, s); err != nil {
					t.Fatalf("%s: %v", s, err)
				}
			}
			tx, other := conn(t, db), conn(t, db)
			runOther := func() {
				// t's creation is stamped in milliseconds: a later one.
				time.Sleep(5 * time.Millisecond)
				for _, s := range c.other {
					if _, err := other.ExecContext(ctx, s); err != nil {
						t.Errorf("the other session: %s: %v", s, err)
					}
				}
			}

			began := time.Now()
			if c.begin != nil {
				for _, s := range append([]string{"BEGIN"}, c.begin...) {
					if _, err := tx.ExecContext(ctx, s); err != nil {
						t.Fatalf("%s: %v", s, err)
					}
				}
				runOther()
			} else {
				onLoad.Store(&runOther)
			}
			got, err := strs(ctx, tx, c.stmt)
			switch {
			case c.listed != nil && err != nil:
				t.Errorf("%s: %v", c.stmt, err)
			case c.listed != nil:
				if !slices.Equal(got, c.listed) {
					t.Errorf("%s returned %v, want %v", c.stmt, got, c.listed)
				}
			case err == nil:
				t.Errorf("%s succeeded in a transaction older than t", c.stmt)
			default:
				checkNewerTable(t, err, began)
			}

			// The DuckDB serves on: the other connection, a new one, and the
			// transaction's, once it starts again.
			for who, c := range map[string]*sql.Conn{"the other connection": other, "a new connection": conn(t, db)} {
				if err := query(ctx, c, "SELECT count(*) FROM lake.s.m"); err != nil {
					t.Fatalf("%s, after the statement: %v", who, err)
				}
			}
			if c.begin != nil {
				if _, err := tx.ExecContext(ctx, "ROLLBACK"); err != nil {
					t.Fatalf("ROLLBACK: %v", err)
				}
			}
			for _, s := range []string{"BEGIN", c.stmt, "COMMIT"} {
				if err := query(ctx, tx, s); err != nil {
					t.Fatalf("in a new transaction: %s: %v", s, err)
				}
			}
			var rows int
			if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM lake.s.t").Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if rows != c.rows {
				t.Errorf("t has %d rows, want %d", rows, c.rows)
			}
		})
	}
}

// startedAt is how the error gives the transaction's start.
var startedAt = regexp.MustCompile(`started at (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)?) UTC`)

// checkNewerTable checks that err is the error that a transaction which
// began about began is older than table s.t: one that fails only the
// transaction, and names the table and the transaction's start.
func checkNewerTable(t *testing.T, err error, began time.Time) {
	t.Helper()
	var de *duckdb.Error
	if !errors.As(err, &de) || de.Type != duckdb.ErrorTypeTransaction {
		t.Errorf("not a transaction error (%d): %v", duckdb.ErrorTypeTransaction, err)
		return
	}
	if !strings.Contains(de.Msg, "Table s.t ") {
		t.Errorf("the error does not name the table s.t: %v", err)
	}
	m := startedAt.FindStringSubmatch(de.Msg)
	if m == nil {
		t.Errorf("the error does not say when the transaction started (%q): %v", startedAt, err)
		return
	}
	at, perr := time.Parse("2006-01-02 15:04:05.999999", m[1])
	if perr != nil {
		t.Errorf("the error's start time %q: %v", m[1], perr)
		return
	}
	if d := at.Sub(began); d < -time.Second || d > time.Second {
		t.Errorf("the error says the transaction started at %s UTC, %v from when it began (%s UTC)",
			m[1], d, began.UTC().Format(time.DateTime))
	}
}

// conn is a connection of db's own, closed when the test ends.
func conn(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// query runs q on c and reads its rows.
func query(ctx context.Context, c *sql.Conn, q string) error {
	rows, err := c.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	rows.Close() //nolint:errcheck
	return rows.Err()
}

// strs runs q, a query, on c and returns its first column as text.
func strs(ctx context.Context, c *sql.Conn, q string) ([]string, error) {
	rows, err := c.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	cols, err := rows.Columns()
	if err != nil || len(cols) == 0 {
		return nil, err
	}
	dest := make([]any, len(cols))
	var first sql.NullString
	dest[0] = &first
	for i := 1; i < len(dest); i++ {
		dest[i] = new(any)
	}
	var out []string
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		out = append(out, first.String)
	}
	return out, rows.Err()
}
