package pgwire

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
)

const adminPassword = "admin-secret"

// testStore is a writable in-memory user store.
type testStore struct {
	mu    sync.Mutex
	users map[string]auth.User
}

func (s *testStore) Get(_ context.Context, name string) (auth.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[name]
	if !ok {
		return auth.User{}, auth.ErrNotFound
	}
	return u, nil
}

func (s *testStore) List(context.Context) ([]auth.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []auth.User
	for _, u := range s.users {
		out = append(out, u)
	}
	return out, nil
}

func (s *testStore) Create(_ context.Context, u auth.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[u.Name]; ok {
		return auth.ErrExists
	}
	u.CreatedAt = time.Now()
	s.users[u.Name] = u
	return nil
}

func (s *testStore) update(name string, f func(*auth.User)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[name]
	if !ok {
		return auth.ErrNotFound
	}
	f(&u)
	s.users[name] = u
	return nil
}

func (s *testStore) SetVerifier(_ context.Context, name, v string) error {
	return s.update(name, func(u *auth.User) { u.Verifier = v })
}

func (s *testStore) SetRole(_ context.Context, name string, r auth.Role) error {
	return s.update(name, func(u *auth.User) { u.Role = r })
}

func (s *testStore) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[name]; !ok {
		return auth.ErrNotFound
	}
	delete(s.users, name)
	return nil
}

func (s *testStore) EnsureAdmin(_ context.Context, name, v string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[name]; ok {
		return false, nil
	}
	s.users[name] = auth.User{Name: name, Role: auth.RoleAdmin, Verifier: v}
	return true, nil
}

func (s *testStore) MockSecret(context.Context) ([]byte, error) {
	return []byte("test mock secret"), nil
}

func (s *testStore) Ping(context.Context) error { return nil }
func (s *testStore) Close()                     {}

type testServer struct {
	addr  string
	store *testStore
	srv   *Server
}

// startServer runs Igloo on an in-memory DuckDB with an "igloo" admin.
// configure, if given, adjusts the server options.
func startServer(t testing.TB, configure ...func(*Options)) *testServer {
	t.Helper()
	return startServerWithConfig(t, config.Config{}, configure...)
}

// startServerWithConfig is startServer with an engine configured by cfg.
func startServerWithConfig(t testing.TB, cfg config.Config, configure ...func(*Options)) *testServer {
	t.Helper()
	return startServerWithEngine(t, cfg, nil, configure...)
}

// startServerWithEngine is startServerWithConfig with engine options.
func startServerWithEngine(t testing.TB, cfg config.Config, engineOpts []engine.Option, configure ...func(*Options)) *testServer {
	t.Helper()
	eng, err := engine.Open(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), engineOpts...)
	if err != nil {
		t.Fatal(err)
	}
	return startServerOn(t, eng, configure...)
}

// startServerOn runs Igloo on eng, which it closes when the test ends.
func startServerOn(t testing.TB, eng *engine.Engine, configure ...func(*Options)) *testServer {
	t.Helper()
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	store := &testStore{users: map[string]auth.User{}}
	v, err := auth.NewVerifier(adminPassword)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsureAdmin(ctx, "igloo", v); err != nil {
		t.Fatal(err)
	}

	opts := Options{Users: store, RoleRefresh: time.Hour}
	for _, f := range configure {
		f(&opts)
	}
	if opts.Audit != nil {
		eng.SetHistory(opts.Audit, HistoryViewer)
	}
	if opts.Tasks != nil {
		eng.SetJobHistory(opts.Tasks.Store(), HistoryViewer)
	}
	if opts.Notify != nil {
		eng.SetAlertHistory(opts.Notify.Store(), HistoryViewer)
	}
	srv, err := New(eng, opts, log)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Erasure != nil {
		eng.SetErasure(opts.Erasure.List, srv.ErasureViewer)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln) //nolint:errcheck

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = eng.Close()
	})

	return &testServer{addr: ln.Addr().String(), store: store, srv: srv}
}

func (ts *testServer) url(user, password string) string {
	u := url.URL{Scheme: "postgres", User: url.UserPassword(user, password), Host: ts.addr, Path: "/igloo", RawQuery: "sslmode=disable"}
	return u.String()
}

func (ts *testServer) admin(t *testing.T, mode pgx.QueryExecMode) *pgx.Conn {
	return connect(t, ts.url("igloo", adminPassword), mode)
}

func connect(t *testing.T, connURL string, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	conn, err := tryConnect(connURL, mode)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func tryConnect(connURL string, mode pgx.QueryExecMode) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(connURL)
	if err != nil {
		return nil, err
	}
	cfg.DefaultQueryExecMode = mode
	return pgx.ConnectConfig(context.Background(), cfg)
}

// modes covers the simple protocol (text results) and the extended protocol
// with both named and unnamed statements (binary results).
var modes = map[string]pgx.QueryExecMode{
	"simple":   pgx.QueryExecModeSimpleProtocol,
	"cached":   pgx.QueryExecModeCacheStatement,
	"describe": pgx.QueryExecModeDescribeExec,
}

func numeric(t *testing.T, s string) pgtype.Numeric {
	t.Helper()
	var n pgtype.Numeric
	if err := n.Scan(s); err != nil {
		t.Fatal(err)
	}
	return n
}

func pow10(n int32) *big.Int {
	if n < 0 {
		return big.NewInt(1)
	}
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// numericEqual compares two numerics that may use different exponents.
func numericEqual(a, b pgtype.Numeric) bool {
	if a.Exp > b.Exp {
		a, b = b, a
	}
	return a.Int.Cmp(new(big.Int).Mul(b.Int, pow10(b.Exp-a.Exp))) == 0
}

func mustExec(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func TestTypes(t *testing.T) {
	ts := startServer(t)
	utc := func(y int, mo time.Month, d, h, mi, s int) time.Time {
		return time.Date(y, mo, d, h, mi, s, 0, time.UTC)
	}

	cases := []struct {
		expr string
		want any
	}{
		{"true", true},
		{"1::TINYINT", int16(1)},
		{"2::SMALLINT", int16(2)},
		{"3::UTINYINT", int16(3)},
		{"4::INTEGER", int32(4)},
		{"5::USMALLINT", int32(5)},
		{"6::BIGINT", int64(6)},
		{"7::UINTEGER", int64(7)},
		{"1.5::FLOAT", float32(1.5)},
		{"2.5::DOUBLE", 2.5},
		{"'hi'", "hi"},
		{"'\\xAA\\xBB'::BLOB", []byte{0xAA, 0xBB}},
		{"'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{"DATE '2026-10-14'", utc(2026, 10, 14, 0, 0, 0)},
		{"TIME '12:34:56.5'", pgtype.Time{Microseconds: 45296500000, Valid: true}},
		{"TIMESTAMP '2026-10-14 12:00:00'", utc(2026, 10, 14, 12, 0, 0)},
		{"INTERVAL 1 DAY", pgtype.Interval{Days: 1, Valid: true}},
		{"[1, 2, 3]", "[1,2,3]"},
		{"{'a': 1}", `{"a":1}`},
		{"MAP {'k': 'v'}", `{"k":"v"}`},
		{`'{"x": 1}'::JSON`, `{"x":1}`},
	}

	numerics := []struct{ expr, want string }{
		{"18446744073709551615::UBIGINT", "18446744073709551615"},
		{"170141183460469231731687303715884105727::HUGEINT", "170141183460469231731687303715884105727"},
		{"12.34::DECIMAL(10,2)", "12.34"},
		{"-0.001::DECIMAL(18,3)", "-0.001"},
	}

	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := ts.admin(t, mode)
			ctx := context.Background()

			for _, c := range cases {
				dest := reflect.New(reflect.TypeOf(c.want))
				if err := conn.QueryRow(ctx, "SELECT "+c.expr).Scan(dest.Interface()); err != nil {
					t.Errorf("%s: %v", c.expr, err)
					continue
				}
				got := dest.Elem().Interface()
				if gt, ok := got.(time.Time); ok {
					if !gt.Equal(c.want.(time.Time)) {
						t.Errorf("%s: got %v, want %v", c.expr, got, c.want)
					}
					continue
				}
				if !reflect.DeepEqual(got, c.want) {
					t.Errorf("%s: got %#v, want %#v", c.expr, got, c.want)
				}
			}

			for _, c := range numerics {
				var got pgtype.Numeric
				if err := conn.QueryRow(ctx, "SELECT "+c.expr).Scan(&got); err != nil {
					t.Errorf("%s: %v", c.expr, err)
					continue
				}
				if !numericEqual(got, numeric(t, c.want)) {
					t.Errorf("%s: got %v, want %s", c.expr, got, c.want)
				}
			}

			var ts time.Time
			if err := conn.QueryRow(ctx, "SELECT TIMESTAMPTZ '2026-10-14 12:00:00+00'").Scan(&ts); err != nil {
				t.Fatal(err)
			}
			if !ts.Equal(utc(2026, 10, 14, 12, 0, 0)) {
				t.Errorf("timestamptz: got %v", ts)
			}

			var null *int32
			if err := conn.QueryRow(ctx, "SELECT NULL::INTEGER").Scan(&null); err != nil || null != nil {
				t.Errorf("NULL: got %v, %v", null, err)
			}
		})
	}
}

func TestParameters(t *testing.T) {
	ts := startServer(t)
	for _, name := range []string{"cached", "describe"} {
		t.Run(name, func(t *testing.T) {
			conn := ts.admin(t, modes[name])
			ctx := context.Background()

			var n int32
			if err := conn.QueryRow(ctx, "SELECT $1::INTEGER + 1", 41).Scan(&n); err != nil || n != 42 {
				t.Errorf("integer param: got %d, %v", n, err)
			}

			var s string
			if err := conn.QueryRow(ctx, "SELECT $1::VARCHAR || '!'", "hi").Scan(&s); err != nil || s != "hi!" {
				t.Errorf("text param: got %q, %v", s, err)
			}

			var isNull bool
			if err := conn.QueryRow(ctx, "SELECT $1::INTEGER IS NULL", nil).Scan(&isNull); err != nil || !isNull {
				t.Errorf("null param: got %v, %v", isNull, err)
			}

			var d time.Time
			day := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
			if err := conn.QueryRow(ctx, "SELECT $1::DATE + 1", day).Scan(&d); err != nil || !d.Equal(day.AddDate(0, 0, 1)) {
				t.Errorf("date param: got %v, %v", d, err)
			}

			var total pgtype.Numeric
			if err := conn.QueryRow(ctx, "SELECT $1::DECIMAL(10,2) * 2", 1.25).Scan(&total); err != nil || !numericEqual(total, numeric(t, "2.5")) {
				t.Errorf("decimal param: got %v, %v", total, err)
			}
		})
	}
}

func TestCommandTags(t *testing.T) {
	ts := startServer(t)
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := ts.admin(t, mode)
			ctx := context.Background()
			table := "tags_" + name

			steps := []struct{ sql, tag string }{
				{"CREATE TABLE " + table + " (i INTEGER)", "CREATE TABLE"},
				{"INSERT INTO " + table + " VALUES (1), (2), (3)", "INSERT 0 3"},
				{"UPDATE " + table + " SET i = i + 10 WHERE i > 1", "UPDATE 2"},
				{"DELETE FROM " + table + " WHERE i = 1", "DELETE 1"},
				{"SET threads = 2", "SET"},
				{"DROP TABLE " + table, "DROP TABLE"},
			}
			for _, step := range steps {
				tag, err := conn.Exec(ctx, step.sql)
				if err != nil {
					t.Fatalf("%s: %v", step.sql, err)
				}
				if tag.String() != step.tag {
					t.Errorf("%s: tag %q, want %q", step.sql, tag, step.tag)
				}
			}
		})
	}
}

func TestReturning(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeCacheStatement)
	ctx := context.Background()

	mustExec(t, conn, "CREATE TABLE ret (i INTEGER)")
	rows, err := conn.Query(ctx, "INSERT INTO ret VALUES (1), (2) RETURNING i * 10")
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int32{10, 20}) {
		t.Errorf("got %v", got)
	}
	if rows.CommandTag().String() != "INSERT 0 2" {
		t.Errorf("tag %q", rows.CommandTag())
	}
}

func TestTransactions(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeCacheStatement)
	ctx := context.Background()
	status := func() byte { return conn.PgConn().TxStatus() }

	mustExec(t, conn, "CREATE TABLE tx (i INTEGER)")

	mustExec(t, conn, "BEGIN")
	if status() != 'T' {
		t.Errorf("after BEGIN: status %c", status())
	}
	mustExec(t, conn, "INSERT INTO tx VALUES (1)")
	if _, err := conn.Exec(ctx, "SELECT * FROM missing_table"); err == nil {
		t.Fatal("expected error")
	}
	if status() != 'E' {
		t.Errorf("after error: status %c", status())
	}
	mustExec(t, conn, "ROLLBACK")
	if status() != 'I' {
		t.Errorf("after ROLLBACK: status %c", status())
	}

	var n int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM tx").Scan(&n); err != nil || n != 0 {
		t.Errorf("rolled back rows visible: %d, %v", n, err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO tx VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM tx").Scan(&n); err != nil || n != 1 {
		t.Errorf("committed rows: %d, %v", n, err)
	}
}

func TestErrorCodes(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeCacheStatement)

	cases := map[string]string{
		"SELECT * FROM no_such_table": "42P01",
		"SELEC 1":                     "42601",
		"SELECT 'abc'::INTEGER":       "22P02",
		"SELECT no_such_column":       "42703",
	}
	for sql, want := range cases {
		_, err := conn.Exec(context.Background(), sql)
		if got := pgCode(err); got != want {
			t.Errorf("%s: code %q, want %s (%v)", sql, got, want, err)
		}
	}
	mustExec(t, conn, "SELECT 1")
}

func TestPostgresOnlySettingsAreAccepted(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, sql := range []string{
		"SET application_name = 'dbeaver'",
		"SET extra_float_digits TO 3",
		"SET SESSION statement_timeout = 0",
		"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
	} {
		mustExec(t, conn, sql)
	}

	var enc string
	if err := conn.QueryRow(context.Background(), "SHOW password_encryption").Scan(&enc); err != nil || enc != "scram-sha-256" {
		t.Errorf("SHOW password_encryption: %q, %v", enc, err)
	}
}

func TestMultipleStatementsInOneQuery(t *testing.T) {
	ts := startServer(t)
	ctx := context.Background()
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE USER walt WITH PASSWORD 'walt-password' ROLE writer")
	mustExec(t, admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	walt := connect(t, ts.url("walt", "walt-password"), pgx.QueryExecModeSimpleProtocol)
	rita := connect(t, ts.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	rows := func(conn *pgx.Conn) int64 { return count(t, conn, "SELECT count(*) FROM multi") }

	// Not only admins: each statement is checked and run on its own, can
	// use what the one before created, and sends its own result.
	results, err := walt.PgConn().Exec(ctx,
		"CREATE TABLE multi (i INTEGER); INSERT INTO multi VALUES (1), (2); SELECT count(*) FROM multi").ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var tags []string
	for _, r := range results {
		tags = append(tags, r.CommandTag.String())
	}
	if !slices.Equal(tags, []string{"CREATE TABLE", "INSERT 0 2", "SELECT 1"}) || string(results[2].Rows[0][0]) != "2" {
		t.Errorf("results: %q, last %q", tags, results[2].Rows)
	}

	// They form one transaction: a failure undoes the statements before it
	// and skips the ones after.
	_, err = walt.PgConn().Exec(ctx, "INSERT INTO multi VALUES (3); INSERT INTO missing VALUES (4); INSERT INTO multi VALUES (5)").ReadAll()
	if pgCode(err) != "42P01" || rows(walt) != 2 || walt.PgConn().TxStatus() != 'I' {
		t.Errorf("failed statement: %v, %d rows, status %c", err, rows(walt), walt.PgConn().TxStatus())
	}
	// Unless they manage transactions themselves.
	_, err = walt.PgConn().Exec(ctx, "BEGIN; INSERT INTO multi VALUES (3); COMMIT; INSERT INTO missing VALUES (4)").ReadAll()
	if pgCode(err) != "42P01" || rows(walt) != 3 {
		t.Errorf("own transaction: %v, %d rows", err, rows(walt))
	}

	// A reader's DELETE is refused, wherever it comes in the query.
	_, err = rita.PgConn().Exec(ctx, "SELECT 1; DELETE FROM multi").ReadAll()
	if pgCode(err) != "42501" || rows(rita) != 3 {
		t.Errorf("reader's DELETE: %v, %d rows", err, rows(rita))
	}

	// Statements Igloo carries out outside DuckDB's transactions must come
	// alone, and nothing of the query runs.
	_, err = walt.PgConn().Exec(ctx, "INSERT INTO multi VALUES (6); ALTER TABLE multi OWNER TO rita").ReadAll()
	if pgCode(err) != "0A000" || rows(walt) != 3 {
		t.Errorf("an owner change among other statements: %v, %d rows", err, rows(walt))
	}
}

func TestLogin(t *testing.T) {
	ts := startServer(t)

	for _, c := range []struct{ user, password string }{
		{"igloo", "wrong-password"},
		{"nobody", adminPassword},
	} {
		_, err := tryConnect(ts.url(c.user, c.password), pgx.QueryExecModeSimpleProtocol)
		if pgCode(err) != "28P01" {
			t.Errorf("%s/%s: expected invalid_password, got %v", c.user, c.password, err)
		}
	}

	// A verifier created by PostgreSQL itself works too.
	const pgVerifier = "SCRAM-SHA-256$4096:pgwMjYmXL0hco19rzQ3u2A==$IPoyWaURz8j4p0emZuNjRNDcEujy217e/WY2AausmUU=:1lgzgbspoHbYwWMpUZ8Wj1eRW4ahJy2jGCKplEoNrZY="
	if err := ts.store.Create(context.Background(), auth.User{Name: "migrated", Role: auth.RoleReader, Verifier: pgVerifier}); err != nil {
		t.Fatal(err)
	}
	connect(t, ts.url("migrated", "correct horse battery staple"), pgx.QueryExecModeSimpleProtocol)
}

func TestUserManagement(t *testing.T) {
	ts := startServer(t)
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()

	tag, err := admin.Exec(ctx, "CREATE USER ana WITH PASSWORD 'ana-password' ROLE writer")
	if err != nil || tag.String() != "CREATE ROLE" {
		t.Fatalf("CREATE USER: %q %v", tag, err)
	}
	if _, err := admin.Exec(ctx, "CREATE USER ana PASSWORD 'ana-password'"); pgCode(err) != "42710" {
		t.Errorf("duplicate user: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE USER bob PASSWORD 'short'"); pgCode(err) != "22023" {
		t.Errorf("short password: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE USER \"Bad Name\" PASSWORD 'long-enough'"); pgCode(err) != "42601" {
		t.Errorf("bad name: %v", err)
	}

	type row struct {
		Name     string
		Role     string
		Created  time.Time
		Type     string
		Owner    *string
		CanLogin bool
		Password bool
		Email    *string
		SSO      bool
	}
	rows, _ := admin.Query(ctx, "SHOW USERS")
	users, err := pgx.CollectRows(rows, pgx.RowToStructByPos[row])
	if err != nil || len(users) != 2 || users[0].Type != "person" || !users[0].CanLogin || !users[0].Password {
		t.Fatalf("SHOW USERS: %v %v", users, err)
	}

	ana := connect(t, ts.url("ana", "ana-password"), pgx.QueryExecModeCacheStatement)
	if _, err := ana.Exec(ctx, "CREATE USER eve PASSWORD 'eve-password'"); pgCode(err) != "42501" {
		t.Errorf("writer created a user: %v", err)
	}
	if _, err := ana.Exec(ctx, "ALTER USER ana ROLE admin"); pgCode(err) != "42501" {
		t.Errorf("writer promoted themselves: %v", err)
	}
	// Users may change their own password.
	if _, err := ana.Exec(ctx, "ALTER USER CURRENT_USER PASSWORD 'new-ana-password'"); err != nil {
		t.Fatal(err)
	}
	connect(t, ts.url("ana", "new-ana-password"), pgx.QueryExecModeSimpleProtocol)

	var seen []string
	rows, _ = ana.Query(ctx, "SHOW USERS")
	mine, _ := pgx.CollectRows(rows, pgx.RowToStructByPos[row])
	for _, r := range mine {
		seen = append(seen, r.Name)
	}
	if !reflect.DeepEqual(seen, []string{"ana"}) {
		t.Errorf("non-admin SHOW USERS saw %v", seen)
	}

	mustExec(t, admin, "DROP USER ana")
	if _, err := admin.Exec(ctx, "DROP USER ana"); pgCode(err) != "42704" {
		t.Errorf("dropping a missing user: %v", err)
	}
	mustExec(t, admin, "DROP USER IF EXISTS ana")
}

func TestRoles(t *testing.T) {
	ts := startServer(t)
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()

	mustExec(t, admin, "CREATE TABLE facts (i INTEGER)")
	mustExec(t, admin, "INSERT INTO facts VALUES (1)")
	mustExec(t, admin, "CREATE USER rita PASSWORD 'rita-password' ROLE reader")
	mustExec(t, admin, "CREATE USER walt PASSWORD 'walt-password' ROLE writer")

	reader := connect(t, ts.url("rita", "rita-password"), pgx.QueryExecModeCacheStatement)
	writer := connect(t, ts.url("walt", "walt-password"), pgx.QueryExecModeCacheStatement)

	allowed := []struct {
		conn *pgx.Conn
		sql  string
	}{
		{reader, "SELECT count(*) FROM facts"},
		{reader, "EXPLAIN SELECT * FROM facts"},
		{reader, "SET search_path = 'memory.main'"},
		{reader, "SET VARIABLE x = 1"},
		{reader, "PRAGMA table_info('facts')"},
		{writer, "INSERT INTO facts VALUES (2)"},
		{writer, "CREATE TABLE scratch AS SELECT 1 AS x"},
		{writer, "DROP TABLE scratch"},
	}
	for _, c := range allowed {
		if _, err := c.conn.Exec(ctx, c.sql); err != nil {
			t.Errorf("%s should be allowed: %v", c.sql, err)
		}
	}

	denied := []struct {
		conn *pgx.Conn
		sql  string
	}{
		{reader, "INSERT INTO facts VALUES (3)"},
		{reader, "UPDATE facts SET i = 0"},
		{reader, "DELETE FROM facts"},
		{reader, "DROP TABLE facts"},
		{reader, "CREATE TABLE nope (i INTEGER)"},
		{reader, "EXPLAIN ANALYZE INSERT INTO facts VALUES (3)"},
		{reader, "SET threads = 1"},
		{reader, "PRAGMA threads = 1"},
		{writer, "SET GLOBAL search_path = 'x'"},
		{writer, "ATTACH ':memory:' AS other"},
		{writer, "DROP SECRET IF EXISTS s"},
		{writer, "SET memory_limit = '1GB'"},
		{writer, "CALL range(3)"},
		{writer, "INSTALL spatial"},
		// Comments must not hide what a statement does.
		{reader, "EXPLAIN /*x*/ ANALYZE INSERT INTO facts VALUES (3)"},
		{writer, "DROP /*x*/ SECRET IF EXISTS s"},
		{writer, "EXPLAIN ANALYZE SET memory_limit = '1GB'"},
		{reader, "SELECT * FROM duckdb_secrets()"},
	}
	for _, c := range denied {
		if _, err := c.conn.Exec(ctx, c.sql); pgCode(err) != "42501" {
			t.Errorf("%s should be denied, got %v", c.sql, err)
		}
	}

	// psql's catalog queries use PostgreSQL-only syntax that Igloo rewrites.
	var tables int
	err := reader.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_class c
		WHERE c.relname OPERATOR(pg_catalog.~) '^(facts)$' COLLATE pg_catalog.default`).Scan(&tables)
	if err != nil || tables != 1 {
		t.Errorf("reader catalog query: %d, %v", tables, err)
	}

	// Each statement of a query is checked on its own.
	simpleReader := connect(t, ts.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	if _, err := simpleReader.Exec(ctx, "SELECT 1; DELETE FROM facts"); pgCode(err) != "42501" {
		t.Errorf("multi-statement query from a reader: %v", err)
	}
	var n int64
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM facts").Scan(&n); err != nil || n != 2 {
		t.Errorf("facts has %d rows, want 2 (%v)", n, err)
	}
}

// TestSQLFromStrings checks that no one, admins included, may run SQL that
// a statement builds from strings, which Igloo's checks cannot see into, or
// move the read cache.
func TestSQLFromStrings(t *testing.T) {
	ts := startServer(t)
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	mustExec(t, admin, "CREATE TABLE facts (i INTEGER)")
	mustExec(t, admin, "CREATE USER rita PASSWORD 'rita-password' ROLE reader")
	mustExec(t, admin, "CREATE USER walt PASSWORD 'walt-password' ROLE writer")
	reader := connect(t, ts.url("rita", "rita-password"), pgx.QueryExecModeCacheStatement)
	writer := connect(t, ts.url("walt", "walt-password"), pgx.QueryExecModeSimpleProtocol)

	for _, c := range []struct {
		conn    *pgx.Conn
		sql     string
		mention string
	}{
		{reader, "SELECT * FROM query('SELECT * FROM duckdb_secrets()')", "query()"},
		{reader, `SELECT * FROM "query"('SELECT * FROM duckdb_secrets()')`, "query()"},
		{reader, "SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT * FROM duckdb_secrets()'))", "json_execute_serialized_sql"},
		{reader, "SELECT * FROM query_table('facts')", "admin"},
		{writer, "CREATE VIEW leak AS SELECT * FROM query('SELECT * FROM duckdb_secrets()')", "query()"},
		{admin, "SELECT * FROM query('SELECT 1')", "query()"},
		{admin, "SET cache_httpfs_cache_directory = '/tmp/elsewhere'", "cache_httpfs_cache_directory"},
		{admin, "SELECT cache_httpfs_add_exclusion_regex('.*')", "cache_httpfs_add_exclusion_regex"},
		// Readers calling the cache's other functions is in the e2e test:
		// this engine does not load the extension.
	} {
		_, err := c.conn.Exec(ctx, c.sql)
		if pgCode(err) != "42501" || !strings.Contains(err.Error(), c.mention) {
			t.Errorf("%s: want 42501 mentioning %q, got %v", c.sql, c.mention, err)
		}
	}

	// The statements of a query form one transaction, so an admin's
	// statements before a refused one are undone.
	if _, err := admin.Exec(ctx, "CREATE TABLE ran (i INTEGER); SELECT * FROM query('SELECT 1')"); pgCode(err) != "42501" {
		t.Errorf("multi-statement query with query(): %v", err)
	}
	if _, err := admin.Exec(ctx, "SELECT * FROM ran"); err == nil {
		t.Error("the statement before query() ran")
	}

	// Tables and columns may still be called query.
	mustExec(t, writer, "CREATE TABLE query (query VARCHAR)")
	mustExec(t, writer, "INSERT INTO query (query) VALUES ('q')")
	var q string
	if err := reader.QueryRow(ctx, "SELECT query FROM query").Scan(&q); err != nil || q != "q" {
		t.Errorf("reading a table named query: %q, %v", q, err)
	}
}

func TestRoleChangesReachOpenSessions(t *testing.T) {
	ts := startServer(t)
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()

	mustExec(t, admin, "CREATE TABLE t (i INTEGER)")
	mustExec(t, admin, "CREATE USER walt PASSWORD 'walt-password' ROLE writer")
	walt := connect(t, ts.url("walt", "walt-password"), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, walt, "INSERT INTO t VALUES (1)")

	mustExec(t, admin, "ALTER USER walt ROLE reader")
	if _, err := walt.Exec(ctx, "INSERT INTO t VALUES (2)"); pgCode(err) != "42501" {
		t.Errorf("demoted user could still write: %v", err)
	}

	mustExec(t, admin, "DROP USER walt")
	if _, err := walt.Exec(ctx, "SELECT 1"); err == nil {
		t.Error("dropped user could still run queries")
	}
}

func TestCancel(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)

	// pgx sends a cancel request when the context ends mid-query.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := conn.Exec(ctx, "SELECT count(*) FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range")
	if err == nil {
		t.Fatal("query was not canceled")
	}
	if time.Since(start) > 10*time.Second {
		t.Errorf("cancel took %s", time.Since(start))
	}
}

func TestStatementCacheReleasesReplacedStatements(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close() //nolint:errcheck

	conn, err := eng.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sess := &session{conn: conn, open: make(map[*wire.PreparedStatement]*statement)}
	ctx = context.WithValue(ctx, sessionKey{}, sess)
	cache := newStatementCache()

	for i := 0; i < 50; i++ {
		st, err := prepare(ctx, conn, "SELECT 1", nil)
		if err != nil {
			t.Fatal(err)
		}
		ps := wire.NewStatement(nil)
		sess.open[ps] = st
		// Clients reuse the unnamed statement for every query.
		if err := cache.Set(ctx, "", ps); err != nil {
			t.Fatal(err)
		}
	}
	if len(sess.open) != 1 {
		t.Errorf("%d statements still open, want 1", len(sess.open))
	}

	if err := cache.Delete(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if len(sess.open) != 0 {
		t.Errorf("%d statements still open after Close, want 0", len(sess.open))
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStatementTimeout(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	slow := "SELECT count(*) FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range"

	show := func() string {
		var v string
		if err := conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if got := show(); got != "0" {
		t.Errorf("default statement_timeout %q, want 0", got)
	}

	mustExec(t, conn, "SET statement_timeout = '200ms'")
	if got := show(); got != "200ms" {
		t.Errorf("statement_timeout %q, want 200ms", got)
	}
	start := time.Now()
	_, err := conn.Exec(ctx, slow)
	if pgCode(err) != "57014" {
		t.Fatalf("slow query: %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Errorf("timeout took %s", time.Since(start))
	}
	mustExec(t, conn, "SELECT 1") // the session is still usable

	mustExec(t, conn, "SET statement_timeout TO 30000")
	if got := show(); got != "30s" {
		t.Errorf("statement_timeout %q, want 30s", got)
	}
	mustExec(t, conn, "RESET statement_timeout")
	if got := show(); got != "0" {
		t.Errorf("after RESET %q, want 0", got)
	}
	if _, err := conn.Exec(ctx, "SET statement_timeout = 'soon'"); pgCode(err) != "22023" {
		t.Errorf("bad value: %v", err)
	}
}

func TestServerStatementTimeout(t *testing.T) {
	ts := startServer(t, func(o *Options) { o.StatementTimeout = 150 * time.Millisecond })
	conn := ts.admin(t, pgx.QueryExecModeCacheStatement)
	_, err := conn.Exec(context.Background(), "SELECT count(*) FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range")
	if pgCode(err) != "57014" {
		t.Fatalf("expected the server default timeout to cancel the query, got %v", err)
	}
}

func TestMaxConnections(t *testing.T) {
	ts := startServer(t, func(o *Options) { o.MaxConnections = 2 })
	first := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ts.admin(t, pgx.QueryExecModeSimpleProtocol)

	_, err := tryConnect(ts.url("igloo", adminPassword), pgx.QueryExecModeSimpleProtocol)
	if pgCode(err) != "53300" {
		t.Fatalf("third connection: %v", err)
	}

	// Closing a session frees its slot.
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := tryConnect(ts.url("igloo", adminPassword), pgx.QueryExecModeSimpleProtocol)
		if err == nil {
			_ = conn.Close(context.Background())
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot not released: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestSessionIdentity(t *testing.T) {
	ts := startServer(t, func(o *Options) { o.Version = "9.9.9" })
	conn := ts.admin(t, pgx.QueryExecModeCacheStatement)
	var user, version string
	if err := conn.QueryRow(context.Background(), "SELECT current_user, version()").Scan(&user, &version); err != nil {
		t.Fatal(err)
	}
	if user != "igloo" {
		t.Errorf("current_user = %q", user)
	}
	if !strings.HasPrefix(version, "PostgreSQL 15.0 (Igloo 9.9.9, DuckDB v") {
		t.Errorf("version() = %q", version)
	}
}

// selfSignedTLS returns a server TLS config for "localhost"/127.0.0.1.
func selfSignedTLS(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

func TestRequireTLS(t *testing.T) {
	ts := startServer(t, func(o *Options) {
		o.TLS = selfSignedTLS(t)
		o.RequireTLS = true
	})
	plain := ts.url("igloo", adminPassword)
	encrypted := strings.Replace(plain, "sslmode=disable", "sslmode=require", 1)

	_, err := tryConnect(plain, pgx.QueryExecModeSimpleProtocol)
	if pgCode(err) != "28000" {
		t.Fatalf("plaintext session: %v", err)
	}

	conn := connect(t, encrypted, pgx.QueryExecModeCacheStatement)
	if !conn.PgConn().Conn().(interface{ ConnectionState() tls.ConnectionState }).ConnectionState().HandshakeComplete {
		t.Fatal("connection is not encrypted")
	}
	mustExec(t, conn, "SELECT 1")

	// Cancel requests still work.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := conn.Exec(ctx, "SELECT count(*) FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range"); err == nil {
		t.Fatal("query was not canceled")
	}
}

func TestReaderSettingsStayInTheSession(t *testing.T) {
	ts := startServer(t)
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE USER rita PASSWORD 'rita-password' ROLE reader")
	reader := connect(t, ts.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()

	// default_order is a global setting in DuckDB; a reader's SET must not
	// change it for everyone.
	mustExec(t, reader, "SET default_order = 'desc'")
	var got string
	if err := admin.QueryRow(ctx, "SELECT current_setting('default_order')").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "ASCENDING" && got != "ASC" {
		t.Errorf("a reader's SET leaked to another session: default_order = %q", got)
	}
}

func TestMockCredentials(t *testing.T) {
	s := &Server{mockSecret: []byte("secret")}
	a1, a2, b := s.mockCredentials("ann"), s.mockCredentials("ann"), s.mockCredentials("bob")
	if a1.Salt != a2.Salt {
		t.Error("an unknown user must see the same salt every time")
	}
	if a1.Salt == b.Salt {
		t.Error("different unknown users must see different salts")
	}
}

// rawStartup opens a TCP connection and sends a startup message for user.
func rawStartup(t *testing.T, addr, user string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte{0, 3, 0, 0}
	body = append(body, "user\x00"+user+"\x00database\x00igloo\x00\x00"...)
	msg := binary.BigEndian.AppendUint32(nil, uint32(4+len(body)))
	if _, err := conn.Write(append(msg, body...)); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestMalformedSASLLengthDoesNotCrash(t *testing.T) {
	ts := startServer(t)
	conn := rawStartup(t, ts.addr, "igloo")
	defer conn.Close() //nolint:errcheck
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 512)
	if _, err := conn.Read(buf); err != nil { // AuthenticationSASL
		t.Fatal(err)
	}

	// SASLInitialResponse with a client-first length of -2.
	body := append([]byte("SCRAM-SHA-256\x00"), 0xFF, 0xFF, 0xFF, 0xFE)
	msg := append([]byte{'p'}, binary.BigEndian.AppendUint32(nil, uint32(4+len(body)))...)
	_, _ = conn.Write(append(msg, body...))
	_, _ = conn.Read(buf)

	// The server is still up.
	c := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, c, "SELECT 1")
}

func TestMalformedBindLengthDoesNotCrash(t *testing.T) {
	ts := startServer(t)
	victim := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	raw := victim.PgConn().Conn()

	// Parse "SELECT $1::INTEGER", then Bind it with a parameter length of -2.
	parse := []byte("\x00SELECT $1::INTEGER\x00\x00\x00")
	bind := []byte{0, 0, 0, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0xFE, 0, 0}
	var out []byte
	for _, m := range []struct {
		t    byte
		body []byte
	}{{'P', parse}, {'B', bind}, {'S', nil}} {
		out = append(out, m.t)
		out = binary.BigEndian.AppendUint32(out, uint32(4+len(m.body)))
		out = append(out, m.body...)
	}
	_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = raw.Write(out)
	_, _ = raw.Read(make([]byte, 512))

	c := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, c, "SELECT 1")
}

// TestIbisCatalogQueries covers what Ibis's Postgres backend needs: array
// parameters, temporary views it can find in the catalog, and DEALLOCATE
// ALL, which psycopg sends after DDL.
func TestIbisCatalogQueries(t *testing.T) {
	ts := startServer(t)
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeExec, pgx.QueryExecModeCacheStatement} {
		conn := connect(t, ts.url("igloo", adminPassword), mode)
		ctx := context.Background()

		var found bool
		if err := conn.QueryRow(ctx, "SELECT 'public' = ANY($1)", []string{"x", "public"}).Scan(&found); err != nil || !found {
			t.Errorf("%v: array parameter: %v %v", mode, found, err)
		}

		mustExec(t, conn, "CREATE TEMPORARY VIEW ibis_meta AS SELECT 1 AS a, 'x' AS b")
		var cols int
		err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
			JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = ANY($1) AND c.relname = $2 AND a.attnum > 0`,
			[]string{"main", "pg_temp"}, "ibis_meta").Scan(&cols)
		if err != nil || cols != 2 {
			t.Errorf("%v: temporary view in the catalog: %d columns, %v", mode, cols, err)
		}
		var temp string
		if err := conn.QueryRow(ctx, "SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema()").Scan(&temp); err != nil || temp != "pg_temp" {
			t.Errorf("%v: pg_my_temp_schema: %q %v", mode, temp, err)
		}
		mustExec(t, conn, "DROP VIEW ibis_meta")
		if _, err := conn.Exec(ctx, "DEALLOCATE ALL"); err != nil {
			t.Errorf("%v: DEALLOCATE ALL: %v", mode, err)
		}
		// A client that reuses its statements afterwards gets PostgreSQL's
		// error, once, and keeps its connection.
		err = conn.QueryRow(ctx, "SELECT 'public' = ANY($1)", []string{"public"}).Scan(&found)
		if err != nil && strings.Contains(err.Error(), "26000") {
			err = conn.QueryRow(ctx, "SELECT 'public' = ANY($1)", []string{"public"}).Scan(&found)
		}
		if err != nil || !found {
			t.Errorf("%v: query after DEALLOCATE ALL: %v %v", mode, found, err)
		}
		conn.Close(ctx) //nolint:errcheck
	}
}

func TestTypeArrayParams(t *testing.T) {
	got, oids := typeArrayParams("SELECT $1 = ANY($2), '$2', $10 FROM t WHERE x = $2", []uint32{25, 1009})
	want := "SELECT $1 = ANY(($2::VARCHAR[])), '$2', $10 FROM t WHERE x = ($2::VARCHAR[])"
	if got != want || !reflect.DeepEqual(oids, []uint32{25, 1009}) {
		t.Errorf("declared:\n got  %s %v\nwant %s", got, oids, want)
	}
	// Undeclared, as with pgx's cached statements: inferred from ANY.
	got, oids = typeArrayParams("SELECT x FROM t WHERE x = ANY ( $2 ) AND y = $1", nil)
	want = "SELECT x FROM t WHERE x = ANY ( ($2::VARCHAR[]) ) AND y = $1"
	if got != want || !reflect.DeepEqual(oids, []uint32{0, 1009}) {
		t.Errorf("inferred:\n got  %s %v\nwant %s", got, oids, want)
	}
	if q, oids := typeArrayParams("SELECT $1", []uint32{25}); q != "SELECT $1" || !reflect.DeepEqual(oids, []uint32{25}) {
		t.Errorf("query without array parameters changed: %s %v", q, oids)
	}
}

// TestTemporaryViewsStayLocal checks that statements on a session's
// temporary views go to DuckDB even when a catalog is configured. Without a
// catalog, as here, they always do; the e2e test covers the catalog case.
func TestTemporaryViewsStayLocal(t *testing.T) {
	ts := startServer(t)
	conn := connect(t, ts.url("igloo", adminPassword), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE TEMPORARY VIEW tv AS SELECT 1 AS a")
	mustExec(t, conn, `DROP VIEW IF EXISTS "tv"`)
}

func TestCursors(t *testing.T) {
	ts := startServer(t)
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec} {
		conn := connect(t, ts.url("igloo", adminPassword), mode)
		ctx := context.Background()
		count := func(sql string) int {
			rows, err := conn.Query(ctx, sql)
			if err != nil {
				t.Fatalf("%v: %s: %v", mode, sql, err)
			}
			n := 0
			for rows.Next() {
				n++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("%v: %s: %v", mode, sql, err)
			}
			return n
		}
		mustExec(t, conn, "BEGIN")
		mustExec(t, conn, `DECLARE "C1" NO SCROLL CURSOR WITHOUT HOLD FOR SELECT range AS n FROM range(10)`)
		if n := count(`FETCH 3 FROM "C1"`); n != 3 {
			t.Errorf("%v: FETCH 3 returned %d rows", mode, n)
		}
		if n := count(`FETCH FORWARD 10 FROM "C1"`); n != 7 {
			t.Errorf("%v: FETCH FORWARD 10 returned %d rows", mode, n)
		}
		if n := count(`FETCH ALL FROM "C1"`); n != 0 {
			t.Errorf("%v: FETCH ALL at the end returned %d rows", mode, n)
		}
		mustExec(t, conn, `CLOSE "C1"`)
		mustExec(t, conn, "DECLARE c2 CURSOR FOR SELECT 1")
		mustExec(t, conn, "COMMIT")
		// The transaction's end closed c2.
		if _, err := conn.Exec(ctx, "FETCH NEXT FROM c2"); err == nil || !strings.Contains(err.Error(), "34000") {
			t.Errorf("%v: FETCH from a closed cursor: %v", mode, err)
		}
		conn.Close(ctx) //nolint:errcheck
	}
}
