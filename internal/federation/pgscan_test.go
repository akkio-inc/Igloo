package federation

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
)

// The tests of scans a statement stops needing run against a real
// PostgreSQL, where a scan can be held up at will: past its first rows,
// every row of the test's relations waits for an advisory lock the test
// holds. A scan delivers enough rows for DuckDB to hand some on, more than
// two of its vectors, and then stays in flight until the test lets go,
// however fast the server is.

// tableRows makes the table span more than three of the extension's tasks
// (1,000 pages each), which it reads on three connections at once: the one
// of DuckDB's transaction and two from its pool.
const tableRows = 200000

// gated is a PostgreSQL database with a table and a view whose scans wait.
type gated struct {
	t     *testing.T
	admin *pgx.Conn
	eng   *engine.Engine
	m     *Manager
	// schema holds orders, a table, and orders_view, a view of it, which
	// a scan of one COPY reads on the connection of DuckDB's transaction.
	schema, role string
	key          int64
	open         bool
}

// startGated creates the relations, with their scans held up, and an engine
// with the database attached as pg, whose DuckDB runs on threads threads.
// A scan that waits holds one of them, and the tests hold up to six scans
// at once: 16 threads start them all on a machine of any size, and use no
// core while they wait.
func startGated(t *testing.T, threads int) *gated {
	t.Helper()
	installExtensions(t)
	ownPostgres(t)
	pg := parseRemote(t, "IGLOO_TEST_DATABASE_URL")
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, os.Getenv("IGLOO_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	g := &gated{t: t, admin: admin, schema: randomName("scan_"), role: randomName("scanrole_"), key: time.Now().UnixNano()}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+g.schema+" CASCADE")
		_, _ = admin.Exec(ctx, "DROP OWNED BY "+g.role)
		_, _ = admin.Exec(ctx, "DROP ROLE "+g.role)
		_ = admin.Close(ctx)
	})
	// A scan is held up past the first 80 of each 1,000 pages, which hold
	// some 4,600 rows: a statement that needs no more of a scan still waits
	// for each of the scan's tasks to hand over the vector it is filling,
	// and a task is 1,000 pages. The rows are told by their page, not
	// their id: a row a test adds goes wherever a page has room. The
	// role's scans of the table are held up by a policy. The table's owner
	// reads it whole, so the view, which is read in one task, holds up its
	// own scans.
	wait := fmt.Sprintf("((ctid::text::point)[0]::int %% 1000 < 80 OR pg_advisory_xact_lock_shared(%d) IS NOT NULL)", g.key)
	for _, sql := range []string{
		fmt.Sprintf("SELECT pg_advisory_lock(%d)", g.key),
		"CREATE SCHEMA " + g.schema,
		"CREATE TABLE " + g.schema + ".orders (id bigint, name text, pad text)",
		fmt.Sprintf("INSERT INTO %s.orders SELECT i, 'n' || i, repeat('x', 90) FROM generate_series(1, %d) i", g.schema, tableRows),
		"ANALYZE " + g.schema + ".orders",
		"CREATE VIEW " + g.schema + ".orders_view AS SELECT id, name FROM " + g.schema + ".orders WHERE " + wait,
		"ALTER TABLE " + g.schema + ".orders ENABLE ROW LEVEL SECURITY",
		"CREATE ROLE " + g.role + " LOGIN PASSWORD 'scan-password'",
		// Each scan starts at the first page, also where the server's
		// memory is small enough for it to start a scan where another is.
		"ALTER ROLE " + g.role + " SET synchronize_seqscans = off",
		"CREATE POLICY gate ON " + g.schema + ".orders FOR SELECT TO " + g.role + " USING " + wait,
		"GRANT USAGE ON SCHEMA " + g.schema + " TO " + g.role,
		"GRANT SELECT ON ALL TABLES IN SCHEMA " + g.schema + " TO " + g.role,
	} {
		if _, err := admin.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	store := NewMemoryStore()
	c := pg.connection("pg", Postgres)
	c.User, c.Password, c.Schemas = g.role, "scan-password", []string{g.schema}
	// A scan the test fails to end is not waited for long.
	c.QueryTimeout = 20 * time.Second
	if err := store.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	g.m = NewManager(store, Options{}, log)
	cfg := config.Config{DuckDB: config.DuckDB{ExtensionDir: os.Getenv("IGLOO_TEST_EXTENSION_DIR"), Threads: threads}}
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
	return g
}

// release lets every scan read on, and hold holds them up again.
func (g *gated) release() {
	if g.open {
		return
	}
	g.open = true
	if _, err := g.admin.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", g.key); err != nil {
		g.t.Error(err)
	}
}

func (g *gated) hold() {
	if !g.open {
		return
	}
	g.open = false
	if _, err := g.admin.Exec(context.Background(), "SELECT pg_advisory_lock($1)", g.key); err != nil {
		g.t.Fatal(err)
	}
}

// scans is how many of the role's COPYs the server is running.
func (g *gated) scans() int {
	g.t.Helper()
	var n int
	err := g.admin.QueryRow(context.Background(),
		"SELECT count(*) FROM pg_stat_activity WHERE usename = $1 AND state = 'active' AND query LIKE 'COPY %'", g.role).Scan(&n)
	if err != nil {
		g.t.Fatal(err)
	}
	return n
}

// settled waits until the server runs more than above COPYs of the role's
// and their number has stood still for a while, and returns it.
func (g *gated) settled(above int) int {
	g.t.Helper()
	last, since := -1, time.Now()
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		n := g.scans()
		if n != last {
			last, since = n, time.Now()
		}
		if n > above && time.Since(since) > 300*time.Millisecond {
			return n
		}
	}
	g.t.Fatalf("the server runs %d scans, want more than %d", last, above)
	return 0
}

// falls waits until the server runs only n COPYs of the role's.
func (g *gated) falls(n int) {
	g.t.Helper()
	got := -1
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if got = g.scans(); got == n {
			return
		}
	}
	g.t.Errorf("the server still runs %d scans, want the other session's %d", got, n)
}

// startOn runs sql under ctx on conn, which the test opened, and returns
// where its answer arrives. Like runStatement, it has the test end the
// statement and wait for it before the connection closes.
func startOn(t *testing.T, ctx context.Context, conn *duckdb.Conn, sql string) chan answer {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan answer, 1)
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		got, err := queryOn(ctx, conn, sql)
		done <- answer{got, err}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-returned:
		case <-time.After(30 * time.Second):
			t.Error("a statement was still running 30 s after the test stopped it")
		}
	})
	return done
}

// expectResult waits for another session's statement to return want.
func expectResult(t *testing.T, done chan answer, want string) {
	t.Helper()
	select {
	case r := <-done:
		if r.err != nil || r.got != want {
			t.Errorf("another session's statement: got %q, %v; want %q", r.got, r.err, want)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("another session's statement did not return")
	}
}

func mustQuery(t *testing.T, conn *duckdb.Conn, sql string) {
	t.Helper()
	if _, err := queryOn(context.Background(), conn, sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// TestPostgresScansEndWithTheirStatement checks that the scans of a canceled
// statement end at the PostgreSQL server when the statement does, and no
// other session's. Igloo's build of the postgres extension sends a cancel
// request for each connection a scan of the statement is in flight on, and
// the proxy passes it on to the server for that session. Until it did, the
// extension read such a scan to its end before the statement returned: a
// statement over a view, which is one COPY, went on for as long as the
// server took to produce the view. Inside a transaction block the statement
// leaves DuckDB's transaction failed, as any canceled statement does, and
// the connection reads again after its ROLLBACK.
func TestPostgresScansEndWithTheirStatement(t *testing.T) {
	g := startGated(t, 16)
	for _, tc := range []struct{ name, relation, sql string }{
		// Three tasks, one on the connection of DuckDB's transaction.
		{"a table", "orders", "SELECT count(id) FROM pg.%s.orders"},
		// One COPY, on that connection.
		{"a view", "orders_view", "SELECT count(id) FROM pg.%s.orders_view"},
		// A LIMIT of the scan's own makes a table one COPY too.
		{"a LIMIT scan", "orders", "SELECT count(id) FROM (SELECT id FROM pg.%s.orders LIMIT 100000)"},
	} {
		for _, block := range []bool{false, true} {
			name := tc.name
			if block {
				name += " in a transaction block"
			}
			t.Run(name, func(t *testing.T) {
				g.t = t
				sql := fmt.Sprintf(tc.sql, g.schema)
				whole := "SELECT count(id) FROM pg." + g.schema + "." + tc.relation
				g.hold()
				// Another session reads the same relation meanwhile.
				other := runStatement(t, g.eng, context.Background(), whole)
				others := g.settled(0)

				conn := connect(t, g.eng)
				if block {
					mustQuery(t, conn, "BEGIN")
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				canceled := startOn(t, ctx, conn, sql)
				g.settled(others)
				cancel()
				at := time.Now()
				select {
				case r := <-canceled:
					if r.err == nil {
						t.Errorf("the canceled statement returned %q", r.got)
					}
					if d := time.Since(at); d > 3*time.Second {
						t.Errorf("the canceled statement returned %s after its cancel", d)
					}
				case <-time.After(20 * time.Second):
					t.Error("the canceled statement had not returned 20 s after its cancel")
				}
				// Its scans ended at the server, and only its.
				g.falls(others)

				g.release()
				expectResult(t, other, strconv.Itoa(tableRows))
				if block {
					mustQuery(t, conn, "ROLLBACK")
				}
				if got, err := queryOn(context.Background(), conn, whole); err != nil || got != strconv.Itoa(tableRows) {
					t.Errorf("the connection's next statement: got %q, %v; want %d", got, err, tableRows)
				}
			})
		}
	}
}

// TestPostgresScansEndWhenTheirStatementNeedsNoMore checks that a statement
// that has what it needs of a scan returns then, and that the scan ends at
// the PostgreSQL server: one with the rows its LIMIT asks for, above a
// filter DuckDB applies, one whose EXISTS has found a row, and one that
// failed on a row. The proxy passes the extension's cancel request for the
// scan on to the server. PostgreSQL fails the transaction of a COPY it
// cancels, and the scan of such a statement is on the connection of DuckDB's
// transaction, so the proxy runs it in a savepoint: the next statement of a
// transaction block reads on, and from the block's snapshot, without a row
// committed since the block's first statement.
func TestPostgresScansEndWhenTheirStatementNeedsNoMore(t *testing.T) {
	g := startGated(t, 16)
	ten := "1\n2\n3\n4\n5\n6\n7\n8\n9\n10"
	rows := tableRows
	for _, tc := range []struct {
		name, sql, want string
		fails           bool
	}{
		{name: "a LIMIT above a filter", sql: "SELECT id FROM pg.%s.%s WHERE name LIKE '%%n%%' LIMIT 10", want: ten},
		{name: "EXISTS", sql: "SELECT 'found' WHERE EXISTS (SELECT 1 FROM pg.%s.%s WHERE name LIKE '%%n%%')", want: "found"},
		{name: "a failure", sql: "SELECT name::INT FROM pg.%s.%s", fails: true},
	} {
		for _, relation := range []string{"orders", "orders_view"} {
			for _, block := range []bool{false, true} {
				name := tc.name + " of " + relation
				if block {
					name += " in a transaction block"
				}
				t.Run(name, func(t *testing.T) {
					g.t = t
					sql := fmt.Sprintf(tc.sql, g.schema, relation)
					whole := "SELECT count(id) FROM pg." + g.schema + "." + relation
					g.hold()
					other := runStatement(t, g.eng, context.Background(), whole)
					others := g.settled(0)

					conn := connect(t, g.eng)
					if block {
						mustQuery(t, conn, "BEGIN")
					}
					at := time.Now()
					got, err := queryOn(context.Background(), conn, sql)
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
					g.falls(others)

					// A row committed now is not in the block's snapshot.
					before := rows
					rows++
					if _, err := g.admin.Exec(context.Background(),
						"INSERT INTO "+g.schema+".orders VALUES ($1, 'n', 'x')", rows); err != nil {
						t.Fatal(err)
					}
					g.release()
					expectResult(t, other, strconv.Itoa(before))
					if block && tc.fails {
						// DuckDB's transaction failed with its statement.
						mustQuery(t, conn, "ROLLBACK")
						block = false
					}
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
}

// TestPostgresCancelRequestNamesItsSession checks that the proxy passes a
// cancel request on only for the session it names: a request with another
// key ends no scan.
func TestPostgresCancelRequestNamesItsSession(t *testing.T) {
	g := startGated(t, 16)
	whole := "SELECT count(id) FROM pg." + g.schema + ".orders_view"
	reading := runStatement(t, g.eng, context.Background(), whole)
	g.settled(0)
	g.m.mu.Lock()
	p := g.m.attached["pg"].ep.(*proxy)
	g.m.mu.Unlock()
	p.mu.Lock()
	var pid uint32
	var key []byte
	for s := range p.pg {
		s.r.mu.Lock()
		if s.r.pending > 0 {
			pid, key = s.clientPID, s.clientKey
		}
		s.r.mu.Unlock()
	}
	p.mu.Unlock()
	if key == nil {
		t.Fatal("no session of the proxy's is at a statement")
	}
	request := func(pid uint32, key []byte) {
		t.Helper()
		conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(p.port())))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close() //nolint:errcheck
		req, _ := (&pgproto3.CancelRequest{ProcessID: pid, SecretKey: key}).Encode(nil)
		if _, err := conn.Write(req); err != nil {
			t.Fatal(err)
		}
		// The proxy closes the connection once the request is dealt with.
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, _ = conn.Read(make([]byte, 1))
	}
	wrong := append([]byte(nil), key...)
	wrong[0] ^= 1
	request(pid, wrong)
	request(pid+1, key)
	time.Sleep(200 * time.Millisecond)
	if n := g.scans(); n != 1 {
		t.Fatalf("the server runs %d scans after cancel requests that name no session, want 1", n)
	}
	// The session's own name ends its scan.
	request(pid, key)
	g.falls(0)
	select {
	case r := <-reading:
		if r.err == nil || !strings.Contains(r.err.Error(), "canceling statement") {
			t.Errorf("the statement whose scan was canceled: got %q, %v", r.got, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Error("the statement whose scan was canceled did not return")
	}
}

// TestPostgresRowsArriveWhileTheServerPauses checks that the proxy hands
// DuckDB every row the server has sent whole before it waits for the
// server. PostgreSQL sends a scan's rows a buffer at a time, and a buffer
// seldom ends where a row does, so a scan that is slow to find its next
// rows pauses in the middle of one. The proxy kept up to 64 kB of the rows
// before it until more arrived: a statement that needed only those, as one
// with a LIMIT does, waited with them.
func TestPostgresRowsArriveWhileTheServerPauses(t *testing.T) {
	row, _ := (&pgproto3.CopyData{Data: []byte("one row")}).Encode(nil)
	// The server pauses inside a row's header, and inside its body.
	for _, part := range []int{3, 7} {
		client, duckdb := net.Pipe()
		proxy, server := net.Pipe()
		s := &pgSession{client: client, out: bufio.NewWriterSize(client, 64<<10), r: newRelay(client, 0)}
		go func() { _ = s.fromServer(bufio.NewReaderSize(proxy, 64<<10)) }()
		go func() { _, _ = server.Write(append(bytes.Repeat(row, 3), row[:part]...)) }()
		got := make([]byte, 3*len(row))
		_ = duckdb.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(duckdb, got); err != nil || !bytes.Equal(got, bytes.Repeat(row, 3)) {
			t.Errorf("with %d bytes of the next row sent, DuckDB got %q, %v; want the three rows before it", part, got, err)
		}
		for _, c := range []net.Conn{client, duckdb, proxy, server} {
			_ = c.Close()
		}
	}
}

// TestPostgresScanWaitsThroughSignals checks that a scan whose thread is
// signaled while it waits for the server goes on waiting. The runtime of a
// Go program signals its threads, the one that runs a query for a session
// among them, and a signal ends the reader's wait for the server early: the
// reader took that for a failure, and a statement failed now and then with
// "Interrupted system call".
func TestPostgresScanWaitsThroughSignals(t *testing.T) {
	// One thread: the scan waits on the thread that runs the statement.
	g := startGated(t, 1)
	whole := "SELECT count(id) FROM pg." + g.schema + ".orders_view"
	conn := connect(t, g.eng)
	ctx, cancel := context.WithCancel(context.Background())
	reading := make(chan answer, 1)
	thread := make(chan int, 1)
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		thread <- threadID()
		got, err := queryOn(ctx, conn, whole)
		reading <- answer{got, err}
	}()
	// The statement has returned before its connection closes.
	t.Cleanup(func() {
		cancel()
		<-returned
	})
	tid := <-thread
	g.settled(0)
	stop := time.After(time.Second)
signals:
	for {
		select {
		case <-stop:
			break signals
		case r := <-reading:
			t.Fatalf("the statement returned while its scan waited: %q, %v", r.got, r.err)
		default:
			for range 100 {
				signalThread(tid)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
	g.release()
	expectResult(t, reading, strconv.Itoa(tableRows))
}
