package flightsql

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pgcodes "github.com/jeroenrinzema/psql-wire/codes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/pgwire"
)

// store is a writable in-memory user store.
type store struct {
	mu    sync.Mutex
	users map[string]auth.User
}

func (s *store) Get(_ context.Context, name string) (auth.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[name]
	if !ok {
		return auth.User{}, auth.ErrNotFound
	}
	return u, nil
}

func (s *store) List(context.Context) ([]auth.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []auth.User
	for _, u := range s.users {
		out = append(out, u)
	}
	return out, nil
}

func (s *store) Create(_ context.Context, u auth.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[u.Name]; ok {
		return auth.ErrExists
	}
	s.users[u.Name] = u
	return nil
}

func (s *store) update(name string, f func(*auth.User)) error {
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

func (s *store) SetVerifier(_ context.Context, name, v string) error {
	return s.update(name, func(u *auth.User) { u.Verifier = v })
}

func (s *store) SetRole(_ context.Context, name string, r auth.Role) error {
	return s.update(name, func(u *auth.User) { u.Role = r })
}

func (s *store) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.users, name)
	return nil
}

func (s *store) EnsureAdmin(context.Context, string, string) (bool, error) { return false, nil }
func (s *store) MockSecret(context.Context) ([]byte, error)                { return []byte("mock"), nil }
func (s *store) Ping(context.Context) error                                { return nil }
func (s *store) Close()                                                    {}

func (s *store) add(t *testing.T, name, password string, role auth.Role) {
	t.Helper()
	v, err := auth.NewVerifier(password)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[name] = auth.User{Name: name, Role: role, Verifier: v, CreatedAt: time.Now()}
}

type testServer struct {
	addr    string
	users   *store
	history *audit.MemoryStore
	sql     *pgwire.Server
}

// startServer serves Flight SQL on a loopback port, without TLS unless
// opts sets it, for users igloo (admin), walt (writer) and rita (reader),
// each with the password "<name>-password".
func startServer(t *testing.T, opts Options) *testServer {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.Open(context.Background(), config.Config{}, log)
	if err != nil {
		t.Fatal(err)
	}
	users := &store{users: map[string]auth.User{}}
	users.add(t, "igloo", "igloo-password", auth.RoleAdmin)
	users.add(t, "walt", "walt-password", auth.RoleWriter)
	users.add(t, "rita", "rita-password", auth.RoleReader)
	history := audit.NewMemoryStore(1000, 1000)
	hlog := audit.New(history, audit.Options{}, log)
	eng.SetHistory(hlog, pgwire.HistoryViewer)
	sql, err := pgwire.New(eng, pgwire.Options{Users: users, Audit: hlog}, log)
	if err != nil {
		t.Fatal(err)
	}
	opts.SQL, opts.Version, opts.Log = sql, "test", log
	srv, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln) //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
		_ = eng.Close()
	})
	return &testServer{addr: ln.Addr().String(), users: users, history: history, sql: sql}
}

// client connects without TLS unless opts say otherwise.
func (ts *testServer) client(t *testing.T, opts ...grpc.DialOption) *fsql.Client {
	t.Helper()
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	cl, err := fsql.NewClient(ts.addr, nil, nil, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// signIn connects as user and returns the client and a context carrying
// its bearer token.
func (ts *testServer) signIn(t *testing.T, user string) (*fsql.Client, context.Context) {
	t.Helper()
	cl := ts.client(t)
	ctx, err := cl.Client.AuthenticateBasicToken(context.Background(), user, user+"-password")
	if err != nil {
		t.Fatalf("sign in as %s: %v", user, err)
	}
	return cl, ctx
}

// fetch reads a FlightInfo's result: its schema and its rows, each value
// as text ("(null)" for NULL).
func fetch(ctx context.Context, cl *fsql.Client, info *flight.FlightInfo) (*arrow.Schema, [][]string, error) {
	if len(info.Endpoint) == 0 {
		schema, err := flight.DeserializeSchema(info.Schema, memory.DefaultAllocator)
		return schema, nil, err
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, nil, err
	}
	defer rdr.Release()
	var rows [][]string
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make([]string, rec.NumCols())
			for j, col := range rec.Columns() {
				row[j] = col.ValueStr(i)
			}
			rows = append(rows, row)
		}
	}
	return rdr.Schema(), rows, rdr.Err()
}

func query(t *testing.T, ctx context.Context, cl *fsql.Client, sql string) (*arrow.Schema, [][]string) {
	t.Helper()
	info, err := cl.Execute(ctx, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	schema, rows, err := fetch(ctx, cl, info)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return schema, rows
}

func update(t *testing.T, ctx context.Context, cl *fsql.Client, sql string) int64 {
	t.Helper()
	n, err := cl.ExecuteUpdate(ctx, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func wantCode(t *testing.T, what string, err error, code codes.Code, mention string) {
	t.Helper()
	if status.Code(err) != code || !strings.Contains(status.Convert(err).Message(), mention) {
		t.Errorf("%s: want %s mentioning %q, got %v", what, code, mention, err)
	}
}

func TestQueryReturnsDuckDBArrowTypes(t *testing.T) {
	ts := startServer(t, Options{})
	cl, ctx := ts.signIn(t, "rita")

	schema, rows := query(t, ctx, cl, `SELECT 42::INTEGER AS i, 'snow' AS s, 1.5::DOUBLE AS d, DATE '2026-10-16' AS day,
		TIMESTAMP '2026-10-16 12:34:56.789' AS at, 12.34::DECIMAL(10,2) AS amount, NULL::BIGINT AS missing,
		[1, 2, 3] AS list, {'a': 1} AS rec, true AS yes`)
	want := []arrow.DataType{
		arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Float64, arrow.FixedWidthTypes.Date32,
		&arrow.TimestampType{Unit: arrow.Microsecond}, &arrow.Decimal128Type{Precision: 10, Scale: 2},
		arrow.PrimitiveTypes.Int64, arrow.ListOfField(arrow.Field{Name: "l", Type: arrow.PrimitiveTypes.Int32, Nullable: true}),
		arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}), arrow.FixedWidthTypes.Boolean,
	}
	for i, f := range schema.Fields() {
		if f.Type.String() != want[i].String() {
			t.Errorf("column %s is %s, want %s", f.Name, f.Type, want[i])
		}
	}
	got := strings.Join(rows[0], "|")
	if wantRow := "42|snow|1.5|2026-10-16|2026-10-16T12:34:56.789Z|12.34|(null)|[1,2,3]|{\"a\":1}|true"; len(rows) != 1 || got != wantRow {
		t.Errorf("rows = %v, want [%s]", rows, wantRow)
	}
}

func TestLargeResultStreamsInBatches(t *testing.T) {
	ts := startServer(t, Options{})
	cl, ctx := ts.signIn(t, "rita")
	info, err := cl.Execute(ctx, "SELECT range AS id, md5(range::VARCHAR) AS h FROM range(1000000)")
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	var rows, sum int64
	batches := 0
	for rdr.Next() {
		rec := rdr.RecordBatch()
		ids := rec.Column(0).(*array.Int64)
		for i := range ids.Len() {
			sum += ids.Value(i)
		}
		rows += rec.NumRows()
		batches++
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != 1_000_000 || sum != 1_000_000*999_999/2 || batches < 100 {
		t.Errorf("got %d rows summing to %d in %d batches", rows, sum, batches)
	}
}

func TestAuthentication(t *testing.T) {
	ts := startServer(t, Options{})
	ctx := context.Background()

	cl := ts.client(t)
	_, err := cl.Client.AuthenticateBasicToken(ctx, "rita", "wrong-password")
	wantCode(t, "wrong password", err, codes.Unauthenticated, `password authentication failed for user "rita"`)
	_, err = cl.Client.AuthenticateBasicToken(ctx, "nobody", "whatever-password")
	wantCode(t, "unknown user", err, codes.Unauthenticated, `password authentication failed for user "nobody"`)

	_, err = cl.Execute(ctx, "SELECT 1")
	wantCode(t, "no sign-in", err, codes.Unauthenticated, "sign in first")
	bogus := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer not-a-token")
	_, err = cl.Execute(bogus, "SELECT 1")
	wantCode(t, "unknown token", err, codes.Unauthenticated, "no session")

	// A token works on the connection it was issued on, and no other.
	// Names are case-insensitive, as in the console.
	rita := ts.client(t)
	ritaCtx, err := rita.Client.AuthenticateBasicToken(ctx, " Rita", "rita-password")
	if err != nil {
		t.Fatal(err)
	}
	query(t, ritaCtx, rita, "SELECT 1")
	other := ts.client(t)
	_, err = other.Execute(ritaCtx, "SELECT 1")
	wantCode(t, "token on another connection", err, codes.Unauthenticated, "no session")

	// CloseSession ends it.
	if _, err := rita.CloseSession(ritaCtx, &flight.CloseSessionRequest{}); err != nil {
		t.Fatal(err)
	}
	_, err = rita.Execute(ritaCtx, "SELECT 1")
	wantCode(t, "closed session", err, codes.Unauthenticated, "no session")

	logins, err := ts.history.Logins(ctx, audit.Viewer{All: true}, audit.Page{})
	if err != nil {
		t.Fatal(err)
	}
	var ok, failed int
	for _, l := range logins {
		if l.ApplicationName != "flight sql" {
			t.Errorf("login recorded for application %q", l.ApplicationName)
		}
		if l.Result == audit.LoginSuccess {
			ok++
		} else {
			failed++
		}
	}
	if ok != 1 || failed != 2 {
		t.Errorf("login history has %d successes and %d failures, want 1 and 2", ok, failed)
	}
}

func TestStatementsAreAuthorizedAsSQL(t *testing.T) {
	ts := startServer(t, Options{})
	admin, adminCtx := ts.signIn(t, "igloo")
	update(t, adminCtx, admin, "CREATE TABLE facts (i INTEGER)")
	update(t, adminCtx, admin, "INSERT INTO facts VALUES (1), (2)")

	rita, ctx := ts.signIn(t, "rita")
	if _, rows := query(t, ctx, rita, "SELECT count(*) FROM facts"); rows[0][0] != "2" {
		t.Errorf("reader counted %v", rows)
	}
	_, err := rita.ExecuteUpdate(ctx, "INSERT INTO facts VALUES (3)")
	wantCode(t, "reader inserts", err, codes.PermissionDenied, "(SQLSTATE 42501)")
	_, err = rita.Execute(ctx, "DELETE FROM facts RETURNING i")
	wantCode(t, "reader deletes as a query", err, codes.PermissionDenied, "(SQLSTATE 42501)")
	_, err = rita.Prepare(ctx, "DROP TABLE facts")
	wantCode(t, "reader prepares DROP TABLE", err, codes.PermissionDenied, "(SQLSTATE 42501)")
	// Igloo's own commands are checked when they run, as for SQL clients.
	createUser, err := rita.Prepare(ctx, "CREATE USER eve PASSWORD 'eve-password'")
	if err != nil {
		t.Fatal(err)
	}
	_, err = createUser.ExecuteUpdate(ctx)
	wantCode(t, "reader creates a user", err, codes.PermissionDenied, "(SQLSTATE 42501)")
	_, err = admin.Execute(adminCtx, "SELECT * FROM query('SELECT 1')")
	wantCode(t, "SQL from strings", err, codes.PermissionDenied, "(SQLSTATE 42501)")
	_, err = rita.Execute(ctx, "SELECT * FROM no_such_table")
	wantCode(t, "missing table", err, codes.NotFound, "(SQLSTATE 42P01)")

	// The history has what ran and what was refused, as for SQL clients.
	queries, err := ts.history.Queries(context.Background(), audit.Viewer{All: true}, audit.Page{})
	if err != nil {
		t.Fatal(err)
	}
	var counted, denied bool
	for _, q := range queries {
		if q.User != "rita" {
			continue
		}
		if !strings.HasPrefix(q.ClientAddress, "flight sql 127.0.0.1") {
			t.Errorf("client address %q", q.ClientAddress)
		}
		counted = counted || q.Text == "SELECT count(*) FROM facts" && q.Outcome == "ok" && q.Rows == 1
		denied = denied || strings.HasPrefix(q.Text, "INSERT") && q.Outcome == "denied" && q.SQLState == "42501"
	}
	if !counted || !denied {
		t.Errorf("history is missing the reader's query (%v) or refused insert (%v): %+v", counted, denied, queries)
	}
}

func TestPreparedStatements(t *testing.T) {
	ts := startServer(t, Options{})
	cl, ctx := ts.signIn(t, "igloo")
	update(t, ctx, cl, "CREATE TABLE notes (id INTEGER, body VARCHAR, noted TIMESTAMP)")

	ins, err := cl.Prepare(ctx, "INSERT INTO notes VALUES ($1, $2, $3)")
	if err != nil {
		t.Fatal(err)
	}
	defer ins.Close(ctx) //nolint:errcheck
	if got := ins.ParameterSchema().String(); !strings.Contains(got, "$1: type=int32") || !strings.Contains(got, "$3: type=timestamp[us]") {
		t.Errorf("parameter schema: %s", got)
	}
	if n := ins.DatasetSchema().NumFields(); n != 0 {
		t.Errorf("an INSERT returns %d columns", n)
	}
	// Clients bind whatever types they have; three rows run three times.
	params := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "body", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
	}, nil))
	defer params.Release()
	params.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	params.Field(1).(*array.StringBuilder).AppendValues([]string{"it's", "", ""}, []bool{true, true, false})
	at := time.Date(2026, 10, 16, 9, 30, 0, 0, time.UTC)
	for range 3 {
		params.Field(2).(*array.TimestampBuilder).AppendTime(at)
	}
	rec := params.NewRecordBatch()
	defer rec.Release()
	ins.SetParameters(rec)
	if n, err := ins.ExecuteUpdate(ctx); err != nil || n != 3 {
		t.Fatalf("prepared insert: %d rows, %v", n, err)
	}

	sel, err := cl.Prepare(ctx, "SELECT id, body, noted FROM notes WHERE id >= $1 ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer sel.Close(ctx) //nolint:errcheck
	for _, min := range []int64{2, 1} {
		b := array.NewInt64Builder(memory.DefaultAllocator)
		b.Append(min)
		arr := b.NewArray()
		p := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "min", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{arr}, 1)
		sel.SetParameters(p)
		info, err := sel.Execute(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, rows, err := fetch(ctx, cl, info)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != int(4-min) || rows[0][0] != "2" && min == 2 {
			t.Errorf("id >= %d: %v", min, rows)
		}
		if min == 1 && strings.Join(rows[0], "|") != "1|it's|2026-10-16T09:30:00Z" || min == 1 && rows[1][1] != "" || min == 1 && rows[2][1] != "(null)" {
			t.Errorf("rows: %v", rows)
		}
		p.Release()
		arr.Release()
		b.Release()
	}

	// Without parameters the statement says how many it needs.
	sel2, err := cl.Prepare(ctx, "SELECT $1::INTEGER + 1")
	if err != nil {
		t.Fatal(err)
	}
	info, err := sel2.Execute(ctx)
	if err == nil {
		_, _, err = fetch(ctx, cl, info)
	}
	wantCode(t, "missing parameters", err, codes.InvalidArgument, "1 parameters, but 0 values")
}

// ADBC and pyarrow users run DDL and DML as queries; they run when the
// client executes them, not when (or whether) it fetches the empty result.
func TestStatementsWithoutRowsRunWhenExecuted(t *testing.T) {
	ts := startServer(t, Options{})
	cl, ctx := ts.signIn(t, "igloo")
	info, err := cl.Execute(ctx, "CREATE TABLE made AS SELECT range AS n FROM range(5)")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Endpoint) != 0 || info.TotalRecords != -1 {
		t.Errorf("CREATE TABLE AS: %d endpoints, %d records", len(info.Endpoint), info.TotalRecords)
	}
	if info, err = cl.Execute(ctx, "INSERT INTO made VALUES (5), (6)"); err != nil || info.TotalRecords != 2 || len(info.Endpoint) != 0 {
		t.Fatalf("INSERT: %v, %v", info, err)
	}

	ins, err := cl.Prepare(ctx, "INSERT INTO made VALUES ($1)")
	if err != nil {
		t.Fatal(err)
	}
	defer ins.Close(ctx) //nolint:errcheck
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues([]int64{7, 8}, nil)
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{arr}, 2)
	defer rec.Release()
	ins.SetParameters(rec)
	if info, err = ins.Execute(ctx); err != nil || info.TotalRecords != 2 {
		t.Fatalf("prepared INSERT as a query: %v, %v", info, err)
	}
	if _, rows := query(t, ctx, cl, "SELECT count(*), sum(n) FROM made"); strings.Join(rows[0], " ") != "9 36" {
		t.Errorf("rows: %v", rows)
	}
}

func TestSessionsKeepState(t *testing.T) {
	ts := startServer(t, Options{})
	cl, ctx := ts.signIn(t, "igloo")
	update(t, ctx, cl, "CREATE TEMP TABLE scratch AS SELECT 7 AS n")
	update(t, ctx, cl, "SET statement_timeout = '5min'")
	if _, rows := query(t, ctx, cl, "SELECT n FROM scratch"); rows[0][0] != "7" {
		t.Errorf("temp table: %v", rows)
	}
	// SHOW statement_timeout is answered by Igloo, not DuckDB: its result
	// arrives as text.
	schema, rows := query(t, ctx, cl, "SHOW statement_timeout")
	if schema.Field(0).Type.ID() != arrow.STRING || rows[0][0] != "5min" {
		t.Errorf("SHOW statement_timeout: %s %v", schema, rows)
	}
}

const slow = "SELECT count(*) FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range"

func TestCancel(t *testing.T) {
	ts := startServer(t, Options{})
	cl, ctx := ts.signIn(t, "rita")

	// A client canceling its call cancels the statement.
	info, err := cl.Execute(ctx, slow)
	if err != nil {
		t.Fatal(err)
	}
	callCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	start := time.Now()
	_, _, err = fetch(callCtx, cl, info)
	cancel()
	if status.Code(err) != codes.DeadlineExceeded && status.Code(err) != codes.Canceled {
		t.Errorf("canceled fetch: %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Errorf("cancel took %s", time.Since(start))
	}
	// The session is free again.
	if _, rows := query(t, ctx, cl, "SELECT 1"); rows[0][0] != "1" {
		t.Errorf("after cancel: %v", rows)
	}

	// CancelFlightInfo cancels a query being fetched.
	info, err = cl.Execute(ctx, slow)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := fetch(ctx, cl, info)
		done <- err
	}()
	// Once it runs; the console lists Flight sessions with the others.
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if s := ts.sql.Sessions(); len(s) == 1 && s[0].Statement == slow && strings.HasPrefix(s[0].Client, "flight sql") {
			break
		}
	}
	res, err := cl.CancelFlightInfo(ctx, &flight.CancelFlightInfoRequest{Info: info})
	if err != nil || res.Status != flight.CancelStatusCancelled {
		t.Fatalf("CancelFlightInfo: %v %v", res, err)
	}
	wantCode(t, "canceled query", <-done, codes.Canceled, "(SQLSTATE 57014)")

	// The statement timeout applies as for SQL clients.
	update(t, ctx, cl, "SET statement_timeout = '200ms'")
	info, err = cl.Execute(ctx, slow)
	if err == nil {
		_, _, err = fetch(ctx, cl, info)
	}
	wantCode(t, "statement timeout", err, codes.DeadlineExceeded, "statement timeout (SQLSTATE 57014)")
}

func TestMetadata(t *testing.T) {
	ts := startServer(t, Options{})
	cl, ctx := ts.signIn(t, "igloo")
	update(t, ctx, cl, "CREATE TABLE orders (id BIGINT NOT NULL, total DECIMAL(15,2), tags VARCHAR[], placed TIMESTAMPTZ)")
	update(t, ctx, cl, "CREATE VIEW big_orders AS SELECT * FROM orders WHERE total > 100")

	read := func(info *flight.FlightInfo, err error) [][]string {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		_, rows, err := fetch(ctx, cl, info)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	if rows := read(cl.GetCatalogs(ctx)); !strings.Contains(flat(rows), "memory") {
		t.Errorf("catalogs: %v", rows)
	}
	main := "main"
	if rows := read(cl.GetDBSchemas(ctx, &fsql.GetDBSchemasOpts{DbSchemaFilterPattern: &main})); len(rows) == 0 || rows[0][1] != "main" {
		t.Errorf("schemas: %v", rows)
	}
	pattern := `%orders`
	rows := read(cl.GetTables(ctx, &fsql.GetTablesOpts{TableNameFilterPattern: &pattern}))
	if got := flat(rows); got != "memory main big_orders VIEW; memory main orders TABLE" {
		t.Errorf("tables: %s", got)
	}
	exact := `orders`
	rows = read(cl.GetTables(ctx, &fsql.GetTablesOpts{TableNameFilterPattern: &exact, TableTypes: []string{"TABLE"}, IncludeSchema: true}))
	if len(rows) != 1 {
		t.Fatalf("tables with schema: %v", rows)
	}
	info, err := cl.GetTables(ctx, &fsql.GetTablesOpts{TableNameFilterPattern: &exact, IncludeSchema: true})
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	if !rdr.Next() {
		t.Fatal(rdr.Err())
	}
	schema, err := flight.DeserializeSchema(rdr.RecordBatch().Column(4).(*array.Binary).Value(0), memory.DefaultAllocator)
	rdr.Release()
	if err != nil {
		t.Fatal(err)
	}
	want := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "total", Type: &arrow.Decimal128Type{Precision: 15, Scale: 2}, Nullable: true},
		{Name: "tags", Type: arrow.ListOfField(arrow.Field{Name: "l", Type: arrow.BinaryTypes.String, Nullable: true}), Nullable: true},
		{Name: "placed", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
	}
	for i, f := range schema.Fields() {
		if ts, ok := f.Type.(*arrow.TimestampType); ok && ts.TimeZone != "" {
			// TIMESTAMPTZ comes in the session's time zone.
			f.Type = &arrow.TimestampType{Unit: ts.Unit}
		}
		if f.Name != want[i].Name || f.Type.String() != want[i].Type.String() || f.Nullable != want[i].Nullable {
			t.Errorf("column %d: %s, want %s", i, f, want[i])
		}
	}
	if tn, _ := schema.Field(1).Metadata.GetValue("ARROW:FLIGHT:SQL:TYPE_NAME"); tn != "DECIMAL(15,2)" {
		t.Errorf("type name metadata %q", tn)
	}
	if rows := read(cl.GetTableTypes(ctx)); flat(rows) != "LOCAL TEMPORARY; TABLE; VIEW" {
		t.Errorf("table types: %v", rows)
	}
	rows = read(cl.GetSqlInfo(ctx, []fsql.SqlInfo{fsql.SqlInfoFlightSqlServerName, fsql.SqlInfoFlightSqlServerVersion}))
	if flat(rows) != `0 [0,"Igloo"]; 1 [0,"test"]` {
		t.Errorf("sql info: %v", rows)
	}
	if rows := read(cl.GetPrimaryKeys(ctx, fsql.TableRef{Table: "orders"})); len(rows) != 0 {
		t.Errorf("primary keys: %v", rows)
	}
}

// flat joins rows with "; " and their values with spaces, for comparing
// small results.
func flat(rows [][]string) string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = strings.Join(r, " ")
	}
	return strings.Join(out, "; ")
}

func TestUnencryptedOnlyAtLocalhost(t *testing.T) {
	ts := startServer(t, Options{})
	// The client names the server as igloo.example.com: without TLS it
	// would send the password across a network.
	cl := ts.client(t, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithAuthority("igloo.example.com:31337"))
	_, err := cl.Client.AuthenticateBasicToken(context.Background(), "rita", "rita-password")
	wantCode(t, "remote handshake without TLS", err, codes.PermissionDenied, "only at localhost")

	for _, host := range []string{"localhost:31337", "127.0.0.1", "[::1]:1", "x.localhost"} {
		if !loopback(host) {
			t.Errorf("%s is not loopback", host)
		}
	}
	for _, host := range []string{"igloo.example.com", "10.0.0.1:31337", "", "localhost.example.com"} {
		if loopback(host) {
			t.Errorf("%s is loopback", host)
		}
	}
}

func TestTLS(t *testing.T) {
	cert := selfSigned(t)
	ts := startServer(t, Options{TLS: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}})
	pool := x509.NewCertPool()
	pool.AddCert(cert.Leaf)
	creds := credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "igloo.example.com", MinVersion: tls.VersionTLS12})
	cl := ts.client(t, grpc.WithTransportCredentials(creds), grpc.WithAuthority("igloo.example.com"))
	ctx, err := cl.Client.AuthenticateBasicToken(context.Background(), "rita", "rita-password")
	if err != nil {
		t.Fatal(err)
	}
	if _, rows := query(t, ctx, cl, "SELECT 'secret' AS s"); rows[0][0] != "secret" {
		t.Errorf("over TLS: %v", rows)
	}

	// Without TLS the connection fails.
	plain := ts.client(t)
	if _, err := plain.Client.AuthenticateBasicToken(context.Background(), "rita", "rita-password"); err == nil {
		t.Error("a TLS server accepted a connection without TLS")
	}
}

func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "igloo.example.com"},
		DNSNames:     []string{"igloo.example.com"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// tokens accepts one personal access token, for rita.
type tokens struct{ users *store }

func (tk tokens) AuthenticateToken(ctx context.Context, token string, _ net.Addr) (auth.User, error) {
	if token != "igloo_pat_rita" {
		return auth.User{}, errors.New("unknown token")
	}
	return tk.users.Get(ctx, "rita")
}

func TestTokensWithoutHandshake(t *testing.T) {
	users := &store{users: map[string]auth.User{}}
	ts := startServer(t, Options{Tokens: tokens{users}})
	users.users = ts.users.users

	cl := ts.client(t)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer igloo_pat_rita")
	update(t, ctx, cl, "SET statement_timeout = '1min'")
	// The token's session lasts as long as the connection.
	if _, rows := query(t, ctx, cl, "SHOW statement_timeout"); rows[0][0] != "1min" {
		t.Errorf("session with a token: %v", rows)
	}
	_, err := cl.ExecuteUpdate(ctx, "CREATE TABLE t (i INTEGER)")
	wantCode(t, "token user's role", err, codes.PermissionDenied, "42501")

	bad := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer igloo_pat_eve")
	_, err = cl.Execute(bad, "SELECT 1")
	wantCode(t, "unknown token", err, codes.Unauthenticated, "no session")
}

func TestStatusCodes(t *testing.T) {
	for state, want := range map[string]codes.Code{
		"42501": codes.PermissionDenied, "57014": codes.Canceled, "42P01": codes.NotFound, "42601": codes.InvalidArgument,
		"0A000": codes.Unimplemented, "40001": codes.Aborted, "53300": codes.ResourceExhausted, "28P01": codes.Unauthenticated,
		"23505": codes.AlreadyExists, "25P02": codes.FailedPrecondition, "XX000": codes.Internal,
	} {
		if got := grpcCode(pgcodes.Code(state)); got != want {
			t.Errorf("%s: %s, want %s", state, got, want)
		}
	}
}
