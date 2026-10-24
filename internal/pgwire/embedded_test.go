package pgwire

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
)

// slowQuery runs until it is canceled.
const slowQuery = "SELECT count(*) FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range"

// newEmbeddedServer is a server on an in-memory DuckDB, without a listener,
// with the "igloo" admin and a reader "rita" (password rita-password).
func newEmbeddedServer(t *testing.T, log io.Writer, configure ...func(*Options)) (*Server, *testStore) {
	t.Helper()
	ctx := context.Background()
	if log == nil {
		log = io.Discard
	}
	logger := slog.New(slog.NewTextHandler(log, nil))
	eng, err := engine.Open(ctx, config.Config{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	store := &testStore{users: map[string]auth.User{}}
	for name, pw := range map[string]string{"igloo": adminPassword, "rita": "rita-password"} {
		v, err := auth.NewVerifier(pw)
		if err != nil {
			t.Fatal(err)
		}
		role := auth.RoleReader
		if name == "igloo" {
			role = auth.RoleAdmin
		}
		store.users[name] = auth.User{Name: name, Role: role, Verifier: v}
	}
	opts := Options{Users: store, RoleRefresh: time.Hour}
	for _, f := range configure {
		f(&opts)
	}
	srv, err := New(eng, opts, logger)
	if err != nil {
		t.Fatal(err)
	}
	return srv, store
}

// syncBuffer is a log destination tests can read while the server writes.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// collected is a ResultWriter that keeps everything, stopping after limit
// rows when limit is positive.
type collected struct {
	columns []Column
	rows    [][]any
	tags    []string
	limit   int
}

func (c *collected) Columns(cols []Column) error { c.columns = cols; return nil }

func (c *collected) Row(values []any) error {
	if c.limit > 0 && len(c.rows) == c.limit {
		return ErrStopRows
	}
	c.rows = append(c.rows, append([]any(nil), values...))
	return nil
}

func (c *collected) Complete(tag string) error { c.tags = append(c.tags, tag); return nil }

func openAs(t *testing.T, srv *Server, store *testStore, name string) *EmbeddedSession {
	t.Helper()
	user, err := store.Get(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	es, err := srv.OpenEmbedded(context.Background(), user, "console 192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = es.Close() })
	return es
}

func codeOf(err error) string {
	if err == nil {
		return ""
	}
	return string(pgerror.Flatten(err).Code)
}

func TestAuthenticate(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	srv, _ := newEmbeddedServer(t, &logs)
	ctx := context.Background()
	addr := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 4242}

	user, err := srv.Authenticate(ctx, "igloo", adminPassword, addr)
	if err != nil || user.Role != auth.RoleAdmin {
		t.Fatalf("good password: %+v, %v", user, err)
	}
	if !strings.Contains(logs.String(), `msg=login user=igloo role=admin addr=192.0.2.1:4242 client=console`) {
		t.Errorf("login not logged as SQL logins are:\n%s", logs.String())
	}

	// Wrong passwords and unknown users fail alike, after the same delay
	// as a SQL login.
	for _, c := range []struct{ user, password string }{{"igloo", "wrong-password"}, {"nobody", "whatever-it-is"}} {
		start := time.Now()
		_, err := srv.Authenticate(ctx, c.user, c.password, addr)
		if !errors.Is(err, ErrLoginFailed) {
			t.Errorf("%s/%s: %v", c.user, c.password, err)
		}
		if took := time.Since(start); took < failedLoginDelay {
			t.Errorf("%s: failed after %s, want at least %s", c.user, took, failedLoginDelay)
		}
		if !strings.Contains(logs.String(), `msg="login failed" user=`+c.user) {
			t.Errorf("failed login of %s not logged", c.user)
		}
	}
}

func TestEmbeddedRun(t *testing.T) {
	t.Parallel()
	srv, store := newEmbeddedServer(t, nil)
	es := openAs(t, srv, store, "igloo")
	ctx := context.Background()

	var out collected
	if err := es.Run(ctx, "SELECT 42 AS n, 'it''s' AS s, NULL::INTEGER AS z, 1.50::DECIMAL(4,2) AS d, DATE '2026-10-16' AS day", &out); err != nil {
		t.Fatal(err)
	}
	want := []Column{{"n", "integer"}, {"s", "text"}, {"z", "integer"}, {"d", "numeric"}, {"day", "date"}}
	if len(out.columns) != len(want) {
		t.Fatalf("columns %+v", out.columns)
	}
	for i := range want {
		if out.columns[i] != want[i] {
			t.Errorf("column %d = %+v, want %+v", i, out.columns[i], want[i])
		}
	}
	// Values come as psql shows them.
	if len(out.rows) != 1 || out.rows[0][0] != "42" || out.rows[0][1] != "it's" || out.rows[0][2] != nil ||
		out.rows[0][3] != "1.50" || out.rows[0][4] != "2026-10-16" {
		t.Errorf("rows %v", out.rows)
	}
	if len(out.tags) != 1 || out.tags[0] != "SELECT 1" {
		t.Errorf("tags %v", out.tags)
	}

	out = collected{}
	for _, q := range []string{"CREATE TABLE t (i INTEGER)", "INSERT INTO t VALUES (1), (2)", "SET statement_timeout = '5s'", "SHOW USERS"} {
		if err := es.Run(ctx, q, &out); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if strings.Join(out.tags, ",") != "CREATE TABLE,INSERT 0 2,SET,SELECT 2" {
		t.Errorf("tags %v", out.tags)
	}
}

func TestEmbeddedAuthorization(t *testing.T) {
	t.Parallel()
	srv, store := newEmbeddedServer(t, nil)
	admin := openAs(t, srv, store, "igloo")
	ctx := context.Background()
	if err := admin.Run(ctx, "CREATE TABLE t AS SELECT 1 AS i", &collected{}); err != nil {
		t.Fatal(err)
	}

	// The reader is refused exactly as over the wire.
	reader := openAs(t, srv, store, "rita")
	err := reader.Run(ctx, "INSERT INTO t VALUES (2)", &collected{})
	if codeOf(err) != "42501" || !strings.Contains(err.Error(), "requires the writer role") {
		t.Errorf("reader insert: %v", err)
	}
	if err := reader.Run(ctx, "CREATE USER eve PASSWORD 'eve-password'", &collected{}); codeOf(err) != "42501" {
		t.Errorf("reader CREATE USER: %v", err)
	}
	var out collected
	if err := reader.Run(ctx, "SELECT count(*) FROM t", &out); err != nil || out.rows[0][0] != "1" {
		t.Errorf("reader select: %v %v", out.rows, err)
	}
	if err := reader.Run(ctx, "SELECT current_user", &out); err != nil || out.rows[1][0] != "rita" {
		t.Errorf("current_user: %v %v", out.rows, err)
	}
}

func TestEmbeddedStopRows(t *testing.T) {
	t.Parallel()
	srv, store := newEmbeddedServer(t, nil)
	es := openAs(t, srv, store, "igloo")
	ctx := context.Background()

	// A query stops after the rows the writer wants.
	out := collected{limit: 10}
	if err := es.Run(ctx, "SELECT * FROM range(1000000)", &out); err != nil {
		t.Fatal(err)
	}
	if len(out.rows) != 10 || out.tags[0] != "SELECT 10" {
		t.Errorf("got %d rows, tag %v", len(out.rows), out.tags)
	}

	// A statement that writes is never cut short: it runs to the end.
	if err := es.Run(ctx, "CREATE TABLE t (i BIGINT)", &collected{}); err != nil {
		t.Fatal(err)
	}
	out = collected{limit: 10}
	if err := es.Run(ctx, "INSERT INTO t SELECT * FROM range(5000) RETURNING i", &out); err != nil {
		t.Fatal(err)
	}
	if len(out.rows) != 10 || out.tags[0] != "INSERT 0 5000" {
		t.Errorf("got %d rows, tag %v", len(out.rows), out.tags)
	}
	count := collected{}
	if err := es.Run(ctx, "SELECT count(*) FROM t", &count); err != nil || count.rows[0][0] != "5000" {
		t.Errorf("count after INSERT ... RETURNING: %v %v", count.rows, err)
	}

	// Only reads may run again for a download.
	if err := es.RunQuery(ctx, "INSERT INTO t VALUES (1)", &collected{}); codeOf(err) != "0A000" {
		t.Errorf("RunQuery INSERT: %v", err)
	}
	if err := es.RunQuery(ctx, "SHOW USERS", &collected{}); codeOf(err) != "0A000" {
		t.Errorf("RunQuery SHOW USERS: %v", err)
	}
	if err := es.RunQuery(ctx, "SELECT count(*) FROM t", &count); err != nil || count.rows[1][0] != "5000" {
		t.Errorf("RunQuery SELECT: %v %v", count.rows, err)
	}
}

func TestEmbeddedCancel(t *testing.T) {
	t.Parallel()
	srv, store := newEmbeddedServer(t, nil)
	ctx := context.Background()

	// Canceling the context cancels the statement, as for a client going away.
	es := openAs(t, srv, store, "rita")
	runCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := es.Run(runCtx, slowQuery, &collected{}); codeOf(err) != "57014" {
		t.Errorf("canceled run: %v", err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("cancel took %s", took)
	}

	// The compute page lists the running statement and can cancel it.
	other := openAs(t, srv, store, "rita")
	done := make(chan error, 1)
	go func() { done <- other.Run(ctx, slowQuery, &collected{}) }()
	var running SessionInfo
	deadline := time.Now().Add(10 * time.Second)
	for running.PID == 0 {
		for _, s := range srv.Sessions() {
			if s.Statement == slowQuery {
				running = s
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the running statement is not listed: %+v", srv.Sessions())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if running.User != "rita" || running.Client != "console 192.0.2.1" || running.Started.IsZero() {
		t.Errorf("listed as %+v", running)
	}
	if !srv.CancelStatement(running.PID, "igloo") {
		t.Error("CancelStatement found nothing running")
	}
	if err := <-done; codeOf(err) != "57014" {
		t.Errorf("canceled statement: %v", err)
	}
	if srv.CancelStatement(running.PID, "igloo") {
		t.Error("CancelStatement canceled an idle session")
	}
}

func TestEmbeddedSessionsCount(t *testing.T) {
	t.Parallel()
	srv, store := newEmbeddedServer(t, nil, func(o *Options) { o.MaxConnections = 1 })
	user, _ := store.Get(context.Background(), "igloo")
	es, err := srv.OpenEmbedded(context.Background(), user, "console")
	if err != nil {
		t.Fatal(err)
	}
	if len(srv.Sessions()) != 1 {
		t.Errorf("sessions %+v", srv.Sessions())
	}
	if _, err := srv.OpenEmbedded(context.Background(), user, "console"); codeOf(err) != "53300" {
		t.Errorf("second session over the limit: %v", err)
	}
	if err := es.Close(); err != nil {
		t.Fatal(err)
	}
	if len(srv.Sessions()) != 0 {
		t.Errorf("closed session still listed: %+v", srv.Sessions())
	}
	es, err = srv.OpenEmbedded(context.Background(), user, "console")
	if err != nil {
		t.Fatalf("closing did not free the slot: %v", err)
	}
	_ = es.Close()
}
