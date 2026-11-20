package federation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
)

// The tests of scans a statement stops needing run against a real MySQL,
// with DuckDB's own mysql extension. That extension asks for the end of a
// scan it drops (KILL QUERY, on another connection) and looks for a cancel
// only between the vectors a scan hands over, so the tests read two views:
// one whose scan waits, past its first rows, for a lock the test holds, for
// statements that have what they need, and one whose rows keep coming
// slowly, for a statement that is canceled.

// myRows is how many rows the tests' table has. The slow view takes some
// six seconds to read: a statement that is canceled, and whose scan is not,
// is told from one whose scan ended by the seconds it goes on for.
const myRows = 60000

// mysqlClient is a session with a MySQL server, or with one of Igloo's
// proxies, signed in with the proxy's own code.
type mysqlClient struct {
	t  *testing.T
	up *packetConn
}

// newMySQLClient signs in to the server at host and port.
func newMySQLClient(t *testing.T, host string, port int, user, password, database string) *mysqlClient {
	t.Helper()
	c := New("test", MySQL)
	c.Host, c.Port, c.User, c.Password, c.Database = host, port, user, password, database
	c.ConnectTimeout, c.TLS = 5*time.Second, TLSDisable
	p := &proxy{name: "test", log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	up, g, err := p.dialMySQL(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = up.conn.Close() })
	hs := &handshakeResponse{maxPacket: 1 << 24, charset: 45}
	flags := g.caps & (capProtocol41 | capSecureConnection | capPluginAuth | capPluginAuthLenenc)
	if err := p.authenticateMySQL(up, g, c, flags, hs); err != nil {
		t.Fatal(err)
	}
	_ = up.conn.SetDeadline(time.Time{})
	return &mysqlClient{t: t, up: up}
}

// query runs sql and returns its rows, as text.
func (c *mysqlClient) query(sql string) ([][]string, error) {
	_ = c.up.conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer c.up.conn.SetDeadline(time.Time{}) //nolint:errcheck
	if err := c.up.writePacket(0, append([]byte{comQuery}, sql...)); err != nil {
		return nil, err
	}
	pkt, _, err := c.up.readPacket()
	switch {
	case err != nil:
		return nil, err
	case len(pkt) == 0:
		return nil, errors.New("empty answer")
	case pkt[0] == 0xff:
		return nil, parseErrPacket(pkt)
	case pkt[0] == 0x00:
		return nil, nil
	}
	columns, _ := readLenenc(pkt)
	// The columns and the packet that ends them.
	for range columns + 1 {
		if _, _, err := c.up.readPacket(); err != nil {
			return nil, err
		}
	}
	var rows [][]string
	for {
		pkt, _, err := c.up.readPacket()
		switch {
		case err != nil:
			return nil, err
		case len(pkt) > 0 && pkt[0] == 0xff:
			return nil, parseErrPacket(pkt)
		case len(pkt) > 0 && pkt[0] == 0xfe && len(pkt) < 9:
			return rows, nil
		}
		var row []string
		for len(pkt) > 0 {
			if pkt[0] == 0xfb {
				row, pkt = append(row, "NULL"), pkt[1:]
				continue
			}
			n, size := lenenc(pkt)
			if size == 0 || uint64(len(pkt)-size) < n {
				return nil, errors.New("malformed row")
			}
			row, pkt = append(row, string(pkt[size:size+int(n)])), pkt[size+int(n):]
		}
		rows = append(rows, row)
	}
}

// must runs statements that have to succeed.
func (c *mysqlClient) must(stmts ...string) {
	c.t.Helper()
	for _, sql := range stmts {
		if _, err := c.query(sql); err != nil {
			c.t.Fatalf("%s: %v", sql, err)
		}
	}
}

// value runs sql, which returns one value.
func (c *mysqlClient) value(sql string) string {
	c.t.Helper()
	rows, err := c.query(sql)
	if err != nil || len(rows) != 1 || len(rows[0]) != 1 {
		c.t.Fatalf("%s: got %v, %v; want one value", sql, rows, err)
	}
	return rows[0][0]
}

// myGated is a MySQL database with views whose scans wait or come slowly.
type myGated struct {
	t     *testing.T
	admin *mysqlClient
	eng   *engine.Engine
	m     *Manager
	// schema holds orders, a table; orders_gated, a view of it whose scan
	// waits past its first rows while the test holds the lock; and
	// orders_slow, a view of it whose rows come slowly.
	schema, user, lock string
	open               bool
}

// startMyGated creates the relations, with the gated view's scans held up,
// and an engine with the database attached as my.
func startMyGated(t *testing.T) *myGated {
	t.Helper()
	installExtensions(t)
	my := parseRemote(t, "IGLOO_TEST_MYSQL_URL")
	ctx := context.Background()
	lock := randomName("gate_")
	g := &myGated{t: t, schema: randomName("scan_"), user: randomName("scan_"), lock: lock + "0"}
	g.admin = newMySQLClient(t, my.host, my.port, my.user, my.password, my.database)
	t.Cleanup(func() {
		_, _ = g.admin.query("DROP DATABASE " + g.schema)
		_, _ = g.admin.query("DROP USER " + g.user)
	})
	// The gated view's scan reads 5,999 rows, of which the server sends
	// those that fill its buffers, more than two of DuckDB's vectors, and
	// then waits at every thousandth row for the lock. The lock's name is
	// made from the row: MySQL takes a lock with a constant name once,
	// before it reads the table, and sends nothing while it waits. The slow
	// view's scan sleeps at every thirty-second row: its rows reach DuckDB
	// a buffer at a time, a vector every tenth of a second or so.
	g.admin.must(
		"SELECT GET_LOCK('"+g.lock+"', 10)",
		"CREATE DATABASE "+g.schema,
		"CREATE TABLE "+g.schema+".orders (id BIGINT PRIMARY KEY, name VARCHAR(20) NOT NULL)",
		"CREATE TABLE "+g.schema+".digits (d INT PRIMARY KEY)",
		"INSERT INTO "+g.schema+".digits VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)",
		fmt.Sprintf("INSERT INTO %[1]s.orders SELECT n, CONCAT('n', n) FROM (SELECT a.d + 10 * b.d + 100 * c.d + 1000 * d.d + 10000 * e.d + 1 AS n "+
			"FROM %[1]s.digits a, %[1]s.digits b, %[1]s.digits c, %[1]s.digits d, %[1]s.digits e) numbers WHERE n <= %[2]d", g.schema, myRows),
		fmt.Sprintf("CREATE VIEW %s.orders_gated AS SELECT id, name FROM %s.orders WHERE id < 6000 OR id %% 1000 <> 0 "+
			"OR (GET_LOCK(CONCAT('%s', id %% 1), 3600) = 1 AND RELEASE_LOCK(CONCAT('%s', id %% 1)) = 1)",
			g.schema, g.schema, lock, lock),
		fmt.Sprintf("CREATE VIEW %s.orders_slow AS SELECT id, name FROM %s.orders WHERE id %% 32 <> 0 OR SLEEP(0.002) = 0",
			g.schema, g.schema),
		"CREATE USER "+g.user+" IDENTIFIED BY 'scan-password'",
		"GRANT SELECT ON "+g.schema+".* TO "+g.user,
	)
	store := NewMemoryStore()
	c := my.connection("my", MySQL)
	c.User, c.Password, c.Database, c.Schemas = g.user, "scan-password", g.schema, []string{g.schema}
	// A scan the test fails to end is not waited for long.
	c.QueryTimeout = 30 * time.Second
	if err := store.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	g.m = NewManager(store, Options{}, log)
	// A scan that waits holds a thread, and the tests hold up to three.
	cfg := config.Config{DuckDB: config.DuckDB{ExtensionDir: os.Getenv("IGLOO_TEST_EXTENSION_DIR"), Threads: 8}}
	var err error
	if g.eng, err = engine.Open(ctx, cfg, log, engine.BeforeLockdown(g.m.Attach)); err != nil {
		t.Fatal(err)
	}
	g.m.SetConnect(g.eng.Connect)
	t.Cleanup(func() {
		g.m.closeAll()
		_ = g.eng.Close()
	})
	// First of all, so that no scan a failed test left keeps the engine
	// from closing.
	t.Cleanup(func() {
		g.t = t
		g.release()
	})
	if g.m.Catalogs()["my"] != MySQL {
		list, _ := g.m.List(ctx)
		t.Fatalf("not attached: %+v", list)
	}
	return g
}

// proxy is the proxy the database is attached through.
func (g *myGated) proxy() *proxy {
	g.m.mu.Lock()
	defer g.m.mu.Unlock()
	return g.m.attached["my"].ep.(*proxy)
}

// release lets the gated view's scans read on, and hold holds them up
// again.
func (g *myGated) release() {
	if g.open {
		return
	}
	g.open = true
	if _, err := g.admin.query("SELECT RELEASE_LOCK('" + g.lock + "')"); err != nil {
		g.t.Error(err)
	}
}

func (g *myGated) hold() {
	if !g.open {
		return
	}
	g.open = false
	if got := g.admin.value("SELECT GET_LOCK('" + g.lock + "', 10)"); got != "1" {
		g.t.Fatalf("the test could not take the lock back: %s", got)
	}
}

// scans is how many statements the user's sessions are running at the
// server.
func (g *myGated) scans() int {
	g.t.Helper()
	n, err := strconv.Atoi(g.admin.value(
		"SELECT COUNT(*) FROM information_schema.processlist WHERE user = '" + g.user + "' AND command = 'Execute'"))
	if err != nil {
		g.t.Fatal(err)
	}
	return n
}

// rises waits until the server runs n of the user's scans.
func (g *myGated) rises(n int) {
	g.t.Helper()
	got := -1
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if got = g.scans(); got == n {
			return
		}
	}
	g.t.Fatalf("the server runs %d scans, want %d", got, n)
}

// falls waits until the server runs only n of the user's scans.
func (g *myGated) falls(n int) {
	g.t.Helper()
	got := -1
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if got = g.scans(); got == n {
			return
		}
	}
	g.t.Errorf("the server still runs %d scans, want the other session's %d", got, n)
}

// TestMySQLScansEndWhenTheirStatementNeedsNoMore checks that a statement
// that has what it needs of a scan returns then, and that the scan ends at
// the MySQL server: one with the rows its LIMIT asks for, above a filter
// DuckDB applies, one whose EXISTS has found a row, and one that failed on
// a row. DuckDB's mysql extension sends a KILL QUERY for the session of
// such a scan, by the ID of the proxy's greeting, and the proxy has the
// server kill the statement that session is at. Until it did, the request
// went to the server as written, with the ID of another session: the scan
// ran to its end before the statement returned, and the other session,
// here one that reads the same view, lost its statement. MySQL fails only
// the statement it kills, so the next statement of a transaction block
// reads on, and from the block's snapshot, without a row committed since
// the block's first statement.
func TestMySQLScansEndWhenTheirStatementNeedsNoMore(t *testing.T) {
	g := startMyGated(t)
	p := g.proxy()
	ten := "1\n2\n3\n4\n5\n6\n7\n8\n9\n10"
	whole := "SELECT count(id) FROM my." + g.schema + ".orders_gated"
	rows := myRows
	for _, tc := range []struct {
		name, sql, want string
		fails           bool
	}{
		{name: "a LIMIT above a filter", sql: "SELECT id FROM my.%s.orders_gated WHERE name LIKE '%%n%%' LIMIT 10", want: ten},
		{name: "EXISTS", sql: "SELECT 'found' WHERE EXISTS (SELECT 1 FROM my.%s.orders_gated WHERE name LIKE '%%n%%')", want: "found"},
		{name: "a failure", sql: "SELECT name::INT FROM my.%s.orders_gated", fails: true},
	} {
		for _, block := range []bool{false, true} {
			name := tc.name
			if block {
				name += " in a transaction block"
			}
			t.Run(name, func(t *testing.T) {
				g.t = t
				g.hold()
				// Another session reads the same view meanwhile.
				other := runStatement(t, g.eng, context.Background(), whole)
				// A scan that waits for the lock ends only once it has it,
				// canceled or not: before anything waits for a statement.
				t.Cleanup(g.release)
				g.rises(1)

				conn := connect(t, g.eng)
				if block {
					mustQuery(t, conn, "BEGIN")
				}
				at := time.Now()
				got, err := queryOn(context.Background(), conn, fmt.Sprintf(tc.sql, g.schema))
				took := time.Since(at)
				switch {
				case tc.fails && err == nil:
					t.Errorf("got %q, want a failure", got)
				case !tc.fails && (err != nil || got != tc.want):
					t.Errorf("got %q, %v; want %q", got, err, tc.want)
				}
				if took > 3*time.Second {
					t.Errorf("the statement returned after %s, with the server still at its scan", took)
				}
				// Its scan ended at the server, and only its.
				g.falls(1)
				if block && tc.fails {
					// DuckDB's transaction failed with its statement.
					mustQuery(t, conn, "ROLLBACK")
					block = false
				}

				// A row committed now is not in the block's snapshot, which
				// the block's session keeps through a change of the
				// connection's settings: that moves only sessions with no
				// transaction open to a new session with the server, and
				// the killed statement was the transaction's first.
				before := rows
				rows++
				g.admin.must(fmt.Sprintf("INSERT INTO %s.orders VALUES (%d, 'n')", g.schema, rows))
				p.update(p.settings())
				g.release()
				expectResult(t, other, strconv.Itoa(before))
				want := rows
				if block {
					want = before
				}
				if got, err := queryOn(context.Background(), conn, whole); err != nil || got != strconv.Itoa(want) {
					t.Errorf("the connection's next statement: got %q, %v; want %d", got, err, want)
				}
				if block {
					mustQuery(t, conn, "COMMIT")
					if got, err := queryOn(context.Background(), conn, whole); err != nil || got != strconv.Itoa(rows) {
						t.Errorf("after the block: got %q, %v; want %d", got, err, rows)
					}
				}
			})
		}
	}
}

// TestMySQLScansEndWithTheirStatement checks that the scan of a canceled
// statement ends at the MySQL server when the statement does, and no other
// session's: the mysql extension drops the scan's result once it has the
// vector it was filling, and its KILL QUERY reaches the scan's session
// through the proxy. Until the proxy saw to that, the statement returned
// only when the server had sent the whole view. Inside a transaction block
// the statement leaves DuckDB's transaction failed, as any canceled
// statement does, and the connection reads again after its ROLLBACK.
func TestMySQLScansEndWithTheirStatement(t *testing.T) {
	g := startMyGated(t)
	whole := "SELECT count(id) FROM my." + g.schema + ".orders_slow"
	for _, block := range []bool{false, true} {
		name := "a view"
		if block {
			name += " in a transaction block"
		}
		t.Run(name, func(t *testing.T) {
			g.t = t
			// Another session reads the same view meanwhile.
			other := runStatement(t, g.eng, context.Background(), whole)
			g.rises(1)

			conn := connect(t, g.eng)
			if block {
				mustQuery(t, conn, "BEGIN")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			canceled := startOn(t, ctx, conn, whole)
			g.rises(2)
			cancel()
			at := time.Now()
			select {
			case r := <-canceled:
				if r.err == nil {
					t.Errorf("the canceled statement returned %q", r.got)
				}
				if d := time.Since(at); d > 2*time.Second {
					t.Errorf("the canceled statement returned %s after its cancel", d)
				}
			case <-time.After(20 * time.Second):
				t.Error("the canceled statement had not returned 20 s after its cancel")
			}
			// Its scan ended at the server, and only its.
			g.falls(1)

			expectResult(t, other, strconv.Itoa(myRows))
			if block {
				mustQuery(t, conn, "ROLLBACK")
			}
			if got, err := queryOn(context.Background(), conn, "SELECT count(id) FROM my."+g.schema+".orders"); err != nil || got != strconv.Itoa(myRows) {
				t.Errorf("the connection's next statement: got %q, %v; want %d", got, err, myRows)
			}
		})
	}
}

// TestMySQLKillQueryNamesItsSession checks that a KILL QUERY sent through
// the proxy ends the statement of the proxy's session it names and nothing
// else: not that of the server's session with that ID, which the statement
// would reach if the proxy passed it on as written.
func TestMySQLKillQueryNamesItsSession(t *testing.T) {
	g := startMyGated(t)
	whole := "SELECT count(id) FROM my." + g.schema + ".orders_gated"
	reading := runStatement(t, g.eng, context.Background(), whole)
	t.Cleanup(g.release)
	g.rises(1)
	p := g.proxy()
	var session, thread uint32
	sessions := map[uint32]bool{}
	p.mu.Lock()
	for id, s := range p.my {
		sessions[id] = true
		s.r.mu.Lock()
		if s.r.pending > 0 {
			session, thread = id, s.thread.id
		}
		s.r.mu.Unlock()
	}
	p.mu.Unlock()
	if session == 0 {
		t.Fatal("no session of the proxy's is at a statement")
	}
	// As DuckDB's client does: a session of its own for each request.
	kill := func(id uint32) {
		t.Helper()
		c := newMySQLClient(t, "127.0.0.1", p.port(), "igloo", p.token, g.schema)
		if _, err := c.query("KILL QUERY " + strconv.FormatUint(uint64(id), 10)); err != nil {
			t.Errorf("KILL QUERY %d: %v", id, err)
		}
		_ = c.up.conn.Close()
	}
	unknown := session + 1000
	for sessions[unknown] || unknown == thread {
		unknown++
	}
	kill(unknown)
	if !sessions[thread] {
		// The scan's ID at the server names no session of the proxy's.
		kill(thread)
	}
	time.Sleep(300 * time.Millisecond)
	if n := g.scans(); n != 1 {
		t.Fatalf("the server runs %d scans after requests that name no session of the proxy's, want 1", n)
	}
	// The session's own ID ends its scan.
	kill(session)
	g.falls(0)
	select {
	case r := <-reading:
		// MySQL's message says the statement was interrupted; MariaDB's
		// reaches DuckDB empty.
		if r.err == nil || !strings.Contains(r.err.Error(), "orders_gated") {
			t.Errorf("the statement whose scan was killed: got %q, %v", r.got, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Error("the statement whose scan was killed did not return")
	}
}
