package federation

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// TestMySQLScanTimes measures how long statements that need only part of a
// large MySQL relation take, against reading all of it: a LIMIT above a
// filter DuckDB applies, an EXISTS, a statement that fails on its first row
// and one a timeout ends after 200 ms, and the first two again as the first
// statement of a transaction block, with the block's next statement. It
// needs IGLOO_TEST_MYSQL_SCAN_RELATIONS, the schema-qualified names,
// separated by commas, of relations of IGLOO_TEST_MYSQL_URL's server with a
// text column called name, most of whose values hold an "a" and none of
// which is a number, such as
//
//	CREATE TABLE big.events (id BIGINT PRIMARY KEY, name CHAR(32) NOT NULL);
//	INSERT INTO big.events WITH RECURSIVE seq (n) AS
//	  (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 20000000) SELECT n, MD5(n) FROM seq;
//	CREATE VIEW big.slow AS SELECT id, name FROM big.small WHERE id <= 5000 OR SLEEP(0.0005) = 0;
//
// With IGLOO_TEST_MYSQL_SCAN_DIRECT set, DuckDB's extension is attached to
// the server itself, without Igloo's proxy, for comparison.
func TestMySQLScanTimes(t *testing.T) {
	relations := os.Getenv("IGLOO_TEST_MYSQL_SCAN_RELATIONS")
	if relations == "" {
		t.Skip("IGLOO_TEST_MYSQL_SCAN_RELATIONS not set")
	}
	installExtensions(t)
	my := parseRemote(t, "IGLOO_TEST_MYSQL_URL")
	connect := mysqlScanSessions(t, my, strings.Split(relations, ","))
	outcome := func(got string, err error) string {
		if err != nil {
			return "error: " + strings.SplitN(err.Error(), "\n", 2)[0]
		}
		if len(got) > 24 {
			got = got[:24] + "..."
		}
		return strings.ReplaceAll(got, "\n", " ")
	}
	for _, relation := range strings.Split(relations, ",") {
		whole := "SELECT count(name) FROM my." + relation
		for _, st := range []struct {
			name, sql string
			timeout   time.Duration
			// block says the statement is also timed in a transaction block.
			block bool
		}{
			{"the whole relation", whole, 0, false},
			{"LIMIT 10 above a filter", "SELECT name FROM my." + relation + " WHERE name LIKE '%a%' LIMIT 10", 0, true},
			{"EXISTS", "SELECT 'found' WHERE EXISTS (SELECT 1 FROM my." + relation + " WHERE name LIKE '%a%')", 0, true},
			{"fails on its first row", "SELECT name::INT FROM my." + relation, 0, false},
			{"a timeout of 200 ms", whole, 200 * time.Millisecond, false},
		} {
			for _, block := range []bool{false, true} {
				if block && !st.block {
					continue
				}
				for range 3 {
					conn := connect()
					if block {
						mustQuery(t, conn, "BEGIN")
					}
					ctx, cancel := context.Background(), context.CancelFunc(func() {})
					if st.timeout > 0 {
						ctx, cancel = context.WithTimeout(ctx, st.timeout)
					}
					at := time.Now()
					got, err := queryOn(ctx, conn, st.sql)
					took := time.Since(at)
					cancel()
					if !block {
						t.Logf("%s: %-24s %7.2f s  %s", relation, st.name, took.Seconds(), outcome(got, err))
						_ = conn.Close()
						continue
					}
					// The block's next statement reads a little, on the
					// connection the scan was on.
					at = time.Now()
					next, err := queryOn(context.Background(), conn, "SELECT count(name) FROM (SELECT name FROM my."+relation+" LIMIT 1000)")
					nextTook := time.Since(at)
					t.Logf("%s: %-24s %7.2f s  %s; the block's next statement %.2f s  %s", relation, st.name+" in a block",
						took.Seconds(), outcome(got, nil), nextTook.Seconds(), outcome(next, err))
					mustQuery(t, conn, "ROLLBACK")
					_ = conn.Close()
				}
			}
		}
	}
}

// mysqlScanSessions returns how to open a DuckDB connection that reads the
// relations as my.<schema>.<name>: through Igloo's proxy, or with
// IGLOO_TEST_MYSQL_SCAN_DIRECT straight from the server.
func mysqlScanSessions(t *testing.T, my remote, relations []string) func() *duckdb.Conn {
	t.Helper()
	ctx := context.Background()
	if os.Getenv("IGLOO_TEST_MYSQL_SCAN_DIRECT") != "" {
		connector, err := duckdb.NewConnector("", nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = connector.Close() })
		open := func() *duckdb.Conn {
			c, err := connector.Connect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			return c.(*duckdb.Conn)
		}
		first := open()
		dsn := "host=" + my.host + " port=" + strconv.Itoa(my.port) + " user=" + my.user + " password=" + my.password + " database=" + my.database
		for _, sql := range append(useExtensionDir(), "LOAD mysql", "ATTACH '"+dsn+"' AS my (TYPE mysql, READ_ONLY)") {
			if _, err := first.ExecContext(ctx, sql, nil); err != nil {
				t.Fatal(err)
			}
		}
		_ = first.Close()
		return open
	}
	store := NewMemoryStore()
	c := my.connection("my", MySQL)
	c.QueryTimeout = 10 * time.Minute
	for _, relation := range relations {
		schema, _, _ := strings.Cut(relation, ".")
		c.Schemas = append(c.Schemas, schema)
	}
	if err := store.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	_, eng := startEngine(t, store)
	return func() *duckdb.Conn { return connect(t, eng) }
}
