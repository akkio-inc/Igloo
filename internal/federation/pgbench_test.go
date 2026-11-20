package federation

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestPostgresScanTimes measures how long statements that need only part
// of a large PostgreSQL relation take, against reading all of it: a LIMIT
// above a filter DuckDB applies, an EXISTS, a statement that fails on its
// first row and one a timeout ends after 200 ms, and the first two again
// as the first statement of a transaction block, with the block's next
// statement. It needs IGLOO_TEST_POSTGRES_SCAN_RELATIONS, the
// schema-qualified names, separated by commas, of relations of
// IGLOO_TEST_DATABASE_URL with a text column called name, most of whose
// values hold an "a" and none of which is a number, such as
//
//	CREATE TABLE big.events AS SELECT g::bigint AS id, md5(g::text) AS name
//	FROM generate_series(1, 30000000) g;
//	CREATE VIEW big.events_view AS SELECT * FROM big.events;
//
// The extension reads a table that has been analyzed 1,000 pages a task,
// and a view, a partitioned table and a table never analyzed in one COPY.
// IGLOO_TEST_POSTGRES_SCAN_MBPS, if set, is how many megabytes a second the
// database's answers arrive at, all connections together, as over a link
// of that speed.
func TestPostgresScanTimes(t *testing.T) {
	relations := os.Getenv("IGLOO_TEST_POSTGRES_SCAN_RELATIONS")
	if relations == "" {
		t.Skip("IGLOO_TEST_POSTGRES_SCAN_RELATIONS not set")
	}
	installExtensions(t)
	pg := parseRemote(t, "IGLOO_TEST_DATABASE_URL")
	store := NewMemoryStore()
	c := pg.connection("pg", Postgres)
	c.QueryTimeout = 10 * time.Minute
	if mbps, _ := strconv.ParseFloat(os.Getenv("IGLOO_TEST_POSTGRES_SCAN_MBPS"), 64); mbps > 0 {
		link := newSlowLink(t, net.JoinHostPort(pg.host, strconv.Itoa(pg.port)), mbps*1e6)
		c.Host, c.Port = "127.0.0.1", link.port()
	}
	for _, relation := range strings.Split(relations, ",") {
		schema, _, _ := strings.Cut(relation, ".")
		c.Schemas = append(c.Schemas, schema)
	}
	if err := store.Create(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	_, eng := startEngine(t, store)
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
		whole := "SELECT count(name) FROM pg." + relation
		for _, st := range []struct {
			name, sql string
			timeout   time.Duration
			// block says the statement is also timed in a transaction block.
			block bool
		}{
			{"the whole relation", whole, 0, false},
			{"LIMIT 10 above a filter", "SELECT name FROM pg." + relation + " WHERE name LIKE '%a%' LIMIT 10", 0, true},
			{"EXISTS", "SELECT 'found' WHERE EXISTS (SELECT 1 FROM pg." + relation + " WHERE name LIKE '%a%')", 0, true},
			{"fails on its first row", "SELECT name::INT FROM pg." + relation, 0, false},
			{"a timeout of 200 ms", whole, 200 * time.Millisecond, false},
		} {
			for _, block := range []bool{false, true} {
				if block && !st.block {
					continue
				}
				for range 3 {
					conn := connect(t, eng)
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
						continue
					}
					// The block's next statement reads a little, on the
					// connection the scan was on.
					at = time.Now()
					next, err := queryOn(context.Background(), conn, "SELECT count(name) FROM (SELECT name FROM pg."+relation+" LIMIT 1000)")
					nextTook := time.Since(at)
					t.Logf("%s: %-24s %7.2f s  %s; the block's next statement %.2f s  %s", relation, st.name+" in a block",
						took.Seconds(), outcome(got, nil), nextTook.Seconds(), outcome(next, err))
					mustQuery(t, conn, "ROLLBACK")
				}
			}
		}
	}
}

// slowLink relays TCP connections to target and passes on what target
// sends at no more than rate bytes a second, all connections together.
type slowLink struct {
	ln     net.Listener
	target string
	rate   float64

	mu sync.Mutex
	// free is when the link has passed on all it has taken.
	free time.Time
}

func newSlowLink(t *testing.T, target string, rate float64) *slowLink {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &slowLink{ln: ln, target: target, rate: rate}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", l.target)
			if err != nil {
				_ = c.Close()
				continue
			}
			go func() {
				buf := make([]byte, 32<<10)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						_, _ = up.Write(buf[:n])
					}
					if err != nil {
						_ = up.Close()
						return
					}
				}
			}()
			go func() {
				buf := make([]byte, 32<<10)
				for {
					n, err := up.Read(buf)
					if n > 0 {
						l.take(n)
						_, _ = c.Write(buf[:n])
					}
					if err != nil {
						_ = c.Close()
						return
					}
				}
			}()
		}
	}()
	return l
}

func (l *slowLink) port() int { return l.ln.Addr().(*net.TCPAddr).Port }

// take waits for the link to have room for n more bytes.
func (l *slowLink) take(n int) {
	l.mu.Lock()
	now := time.Now()
	if l.free.Before(now) {
		l.free = now
	}
	l.free = l.free.Add(time.Duration(float64(n) / l.rate * float64(time.Second)))
	wait := l.free.Sub(now)
	l.mu.Unlock()
	time.Sleep(wait)
}
