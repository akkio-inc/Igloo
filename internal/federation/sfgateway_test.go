package federation

import (
	"bufio"
	"context"
	"errors"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// fakeSnowflake stands in for Snowflake behind a gateway: it serves a
// catalog, records the queries it is sent and answers them with Arrow
// records in Snowflake's own forms.
type fakeSnowflake struct {
	cat *sfCatalog
	// down fails signing in, as an unreachable Snowflake would.
	down error
	// slow lists the tables until it is stopped, as a Snowflake slower
	// than Igloo's startup would.
	slow bool
	// answer returns a query's result batches.
	answer func(ctx context.Context, q string) ([][]arrow.Record, error)

	mu      sync.Mutex
	queries []string
}

func (f *fakeSnowflake) check(context.Context) error { return f.down }

func (f *fakeSnowflake) loadCatalog(ctx context.Context, _ func(string, ...any)) (*sfCatalog, error) {
	if f.slow {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.cat, nil
}

func (f *fakeSnowflake) query(ctx context.Context, q string, timeout time.Duration) (*sfResult, error) {
	f.mu.Lock()
	f.queries = append(f.queries, q)
	f.mu.Unlock()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	batches, err := f.answer(ctx, q)
	if err != nil {
		return nil, err
	}
	res := &sfResult{queryID: "fake"}
	for _, b := range batches {
		res.batches = append(res.batches, fakeBatch(b))
	}
	return res, nil
}

func (f *fakeSnowflake) close() {}

func (f *fakeSnowflake) sent() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.queries...)
}

type fakeBatch []arrow.Record

// flakyBatch fails its first fails downloads, as one cut off part way would,
// after stalling its first stalls until it is stopped.
type flakyBatch struct {
	fails, stalls, calls int
}

func (b *flakyBatch) fetch(ctx context.Context) ([]arrow.Record, error) {
	b.calls++
	if b.calls <= b.stalls {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if b.calls <= b.fails {
		return nil, errors.New("decoding Arrow batch: arrow/ipc: could not read message body: unexpected EOF")
	}
	return nil, nil
}

func TestFetchBatchRetries(t *testing.T) {
	b := &flakyBatch{fails: fetchAttempts - 1}
	if _, err := fetchBatch(context.Background(), b, 0); err != nil || b.calls != fetchAttempts {
		t.Errorf("err %v after %d downloads", err, b.calls)
	}
	b = &flakyBatch{fails: fetchAttempts}
	if _, err := fetchBatch(context.Background(), b, 0); err == nil || !strings.Contains(err.Error(), "unexpected EOF") || b.calls != fetchAttempts {
		t.Errorf("err %v after %d downloads", err, b.calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b = &flakyBatch{fails: fetchAttempts}
	if _, err := fetchBatch(ctx, b, 0); err == nil || b.calls != 1 {
		t.Errorf("err %v after %d downloads of a canceled query", err, b.calls)
	}

	// A stalled download is tried again after fetchAttemptTimeout, and the
	// last attempt waits for as long as the query may.
	attempt := fetchAttemptTimeout
	fetchAttemptTimeout = 50 * time.Millisecond
	t.Cleanup(func() { fetchAttemptTimeout = attempt })
	b = &flakyBatch{stalls: fetchAttempts - 1, fails: fetchAttempts - 1}
	if _, err := fetchBatch(context.Background(), b, 0); err != nil || b.calls != fetchAttempts {
		t.Errorf("err %v after %d downloads", err, b.calls)
	}
	b = &flakyBatch{stalls: fetchAttempts, fails: fetchAttempts}
	start := time.Now()
	_, err := fetchBatch(context.Background(), b, time.Second)
	if err == nil || !strings.Contains(err.Error(), "within query_timeout") || time.Since(start) < time.Second {
		t.Errorf("err %v after %s", err, time.Since(start))
	}
}

func (b fakeBatch) fetch(context.Context) ([]arrow.Record, error) {
	for _, r := range b {
		r.Retain()
	}
	return b, nil
}

// t1Columns are the test table's columns in Snowflake's Arrow forms, two
// rows each, the second all NULL.
func t1Columns(t *testing.T) map[string]arrow.Array {
	t.Helper()
	pool := memory.NewGoAllocator()
	at := time.Date(2020, 1, 2, 3, 4, 5, 123456789, time.UTC)
	out := map[string]arrow.Array{}

	i8 := array.NewInt8Builder(pool)
	i8.Append(1)
	i8.AppendNull()
	out["ID"] = i8.NewArray()
	i16 := array.NewInt16Builder(pool)
	i16.Append(-1050)
	i16.AppendNull()
	out["AMT"] = i16.NewArray()
	for _, name := range []string{"NAME", "Mixed Case", "V", "G"} {
		s := array.NewStringBuilder(pool)
		s.Append(map[string]string{"NAME": `it's a\b`, "Mixed Case": "x", "V": `{"a":[1,2]}`, "G": "POINT(1 2)"}[name])
		s.AppendNull()
		out[name] = s.NewArray()
	}
	f := array.NewFloat64Builder(pool)
	f.Append(1.5)
	f.AppendNull()
	out["F"] = f.NewArray()
	b := array.NewBooleanBuilder(pool)
	b.Append(true)
	b.AppendNull()
	out["FLAG"] = b.NewArray()
	d := array.NewDate32Builder(pool)
	d.Append(arrow.Date32FromTime(at))
	d.AppendNull()
	out["D"] = d.NewArray()
	tm := array.NewTime64Builder(pool, &arrow.Time64Type{Unit: arrow.Nanosecond})
	tm.Append(arrow.Time64((3*3600+4*60+5)*1e9 + 123456789))
	tm.AppendNull()
	out["TM"] = tm.NewArray()
	ts := arrow.StructOf(arrow.Field{Name: "epoch", Type: arrow.PrimitiveTypes.Int64}, arrow.Field{Name: "fraction", Type: arrow.PrimitiveTypes.Int32})
	for _, name := range []string{"TS", "LTZ"} {
		sb := array.NewStructBuilder(pool, ts)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.Int64Builder).Append(at.Unix())
		sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(at.Nanosecond()))
		sb.AppendNull()
		sb.FieldBuilder(0).(*array.Int64Builder).Append(0)
		sb.FieldBuilder(1).(*array.Int32Builder).Append(0)
		out[name] = sb.NewArray()
	}
	ts3 := array.NewInt64Builder(pool)
	ts3.Append(at.UnixMilli())
	ts3.AppendNull()
	out["TS3"] = ts3.NewArray()
	tz := arrow.StructOf(arrow.Field{Name: "epoch", Type: arrow.PrimitiveTypes.Int64}, arrow.Field{Name: "fraction", Type: arrow.PrimitiveTypes.Int32},
		arrow.Field{Name: "timezone", Type: arrow.PrimitiveTypes.Int32})
	tzb := array.NewStructBuilder(pool, tz)
	tzb.Append(true)
	tzb.FieldBuilder(0).(*array.Int64Builder).Append(at.Unix())
	tzb.FieldBuilder(1).(*array.Int32Builder).Append(int32(at.Nanosecond()))
	tzb.FieldBuilder(2).(*array.Int32Builder).Append(1440 + 120)
	tzb.AppendNull()
	for i := range 3 {
		tzb.FieldBuilder(i).(interface{ AppendNull() }).AppendNull()
	}
	out["TZ"] = tzb.NewArray()
	bin := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	bin.Append([]byte{0x0a, 0xff})
	bin.AppendNull()
	out["B"] = bin.NewArray()
	return out
}

// selected matches the columns of the gateway's SELECTs.
var selected = regexp.MustCompile(`"([^"]+)"`)

// recordFor answers a SELECT of the test table with the columns it names.
func recordFor(t *testing.T, cols map[string]arrow.Array, q string, rows int) arrow.Record {
	t.Helper()
	list := q[len("SELECT "):strings.Index(q, " FROM ")]
	var fields []arrow.Field
	var arrays []arrow.Array
	for _, expr := range strings.Split(list, ", ") {
		name := selected.FindStringSubmatch(expr)[1]
		a := array.NewSlice(cols[name], 0, int64(rows))
		fields = append(fields, arrow.Field{Name: name, Type: a.DataType(), Nullable: true})
		arrays = append(arrays, a)
	}
	return array.NewRecord(arrow.NewSchema(fields, nil), arrays, int64(rows))
}

func countRecord(n int64) arrow.Record {
	b := array.NewInt64Builder(memory.NewGoAllocator())
	b.Append(n)
	a := b.NewArray()
	return array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "COUNT(*)", Type: a.DataType()}}, nil), []arrow.Array{a}, 1)
}

// TestSnowflakeGateway runs DuckDB's postgres extension against the
// gateway, with a fake Snowflake.
func TestSnowflakeGateway(t *testing.T) {
	installExtensions(t)
	cols := t1Columns(t)
	fake := &fakeSnowflake{cat: testCatalog(t)}
	var blocked sync.WaitGroup
	fake.answer = func(ctx context.Context, q string) ([][]arrow.Record, error) {
		switch {
		case strings.Contains(q, `COLLATE("NAME", 'utf8') = 'boom'`):
			return nil, errors.New("SQL compilation error: something Snowflake said")
		case strings.Contains(q, `COLLATE("NAME", 'utf8') = 'slow'`):
			// Waits for the query_timeout, as a long query would.
			blocked.Add(1)
			defer blocked.Done()
			<-ctx.Done()
			return nil, errors.New("the query ran longer than query_timeout (1s) on Snowflake and was canceled there")
		case strings.HasPrefix(q, "SELECT COUNT(*)"):
			return [][]arrow.Record{{countRecord(4)}}, nil
		case strings.Contains(q, "LIMIT 1"):
			return [][]arrow.Record{{recordFor(t, cols, q, 1)}}, nil
		case strings.Contains(q, `"ID" = 1`):
			// Three batches of rows, the second empty.
			r := recordFor(t, cols, q, 1)
			return [][]arrow.Record{{r}, {}, {r, r}}, nil
		}
		return [][]arrow.Record{{recordFor(t, cols, q, 2)}}, nil
	}
	open := openSnowflake
	openSnowflake = func(Connection, string) sfBackend { return fake }
	t.Cleanup(func() { openSnowflake = open })

	store := NewMemoryStore()
	c := New("sf", Snowflake)
	c.Account, c.User, c.Database, c.Password = "myorg-acct", "u", "db", "pw"
	c.QueryTimeout = time.Second
	if err := store.Create(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	m, eng := startEngine(t, store)
	if got := m.Catalogs(); got["sf"] != Snowflake {
		t.Fatalf("not attached: %v", got)
	}

	// Snowflake's names, as DuckDB sees them, and their types.
	expect(t, eng, "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'sf'", "tpch|t1")
	expect(t, eng, "SELECT string_agg(column_name || ' ' || data_type, ', ' ORDER BY column_index) FROM duckdb_columns() WHERE database_name = 'sf'",
		"id DECIMAL(38,0), amt DECIMAL(12,2), name VARCHAR, f DOUBLE, flag BOOLEAN, d DATE, tm TIME, ts TIMESTAMP, ts3 TIMESTAMP, "+
			"ltz TIMESTAMP WITH TIME ZONE, tz TIMESTAMP WITH TIME ZONE, v VARCHAR, b BLOB, g VARCHAR, Mixed Case VARCHAR")
	expect(t, eng, "SELECT count(*) FROM information_schema.tables WHERE table_catalog = 'sf'", "1")

	// Every value as Snowflake sent it.
	expect(t, eng, `SELECT id::VARCHAR, amt::VARCHAR, name, f, flag, d::VARCHAR, tm::VARCHAR, ts::VARCHAR, ts3::VARCHAR,
		timezone('UTC', ltz)::VARCHAR, timezone('UTC', tz)::VARCHAR, v::VARCHAR, hex(b), g, "Mixed Case" FROM sf.tpch.t1 ORDER BY id`,
		`1|-10.50|it's a\b|1.5|true|2020-01-02|03:04:05.123456|2020-01-02 03:04:05.123456|2020-01-02 03:04:05.123|`+
			`2020-01-02 03:04:05.123456|2020-01-02 03:04:05.123456|{"a":[1,2]}|0AFF|POINT(1 2)|x`+"\n"+
			"NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL")
	last := func() string {
		q := fake.sent()
		return q[len(q)-1]
	}
	if q := last(); q != `SELECT "ID", "AMT", "NAME", "F", "FLAG", "D", CAST("TM" AS TIME(9)), "TS", "TS3", "LTZ", "TZ", `+
		`TO_JSON(CAST("V" AS VARIANT)), "B", ST_ASWKT("G"), "Mixed Case" FROM "DB"."TPCH"."T1"` {
		t.Errorf("Snowflake was sent %s", q)
	}

	// Filters, columns and limits go to Snowflake; count(*) is counted
	// there.
	expect(t, eng, "SELECT id FROM sf.tpch.t1 WHERE amt < 0 AND name = 'it''s a\\b' LIMIT 1", "1")
	if q := last(); q != `SELECT "AMT", "NAME", "ID" FROM "DB"."TPCH"."T1" WHERE "AMT" < 0.00 AND COLLATE("NAME", 'utf8') = 'it''s a\\b' LIMIT 1` {
		t.Errorf("Snowflake was sent %s", q)
	}
	expect(t, eng, "SELECT count(*) FROM sf.tpch.t1", "4")
	if q := last(); q != `SELECT COUNT(*) FROM "DB"."TPCH"."T1"` {
		t.Errorf("Snowflake was sent %s", q)
	}
	expect(t, eng, "SELECT count(*), sum(id)::VARCHAR FROM sf.tpch.t1 WHERE id = 1", "3|3")

	// DuckDB does not check the rows of a scan against the filters it
	// pushed into it, a row access policy's among them (this is the shape
	// of Igloo's policy rewrite), and counts every row Snowflake returns:
	// Snowflake must compare strings as DuckDB does, byte by byte, even in
	// a column whose collation ignores case, or the count includes rows the
	// policy hides (which other queries return with every column NULL).
	expect(t, eng, `SELECT count(*) FROM (SELECT CASE WHEN t.ok THEN t.id END AS id
		FROM (SELECT t.id, (t.name = 'EU') AS ok FROM sf.tpch.t1 AS t) AS t WHERE t.ok) AS p`, "2")
	if q := last(); q != `SELECT "NAME" FROM "DB"."TPCH"."T1" WHERE COLLATE("NAME", 'utf8') = 'EU'` {
		t.Errorf("Snowflake was sent %s", q)
	}
	expect(t, eng, "SELECT count(*) FROM sf.tpch.t1 WHERE name LIKE 'EU%'", "2")
	if q := last(); q != `SELECT "NAME" FROM "DB"."TPCH"."T1" WHERE COLLATE("NAME", 'utf8') >= 'EU' AND COLLATE("NAME", 'utf8') < 'EV'` {
		t.Errorf("Snowflake was sent %s", q)
	}

	// Joined with DuckDB's own tables.
	expect(t, eng, "SELECT t.name, x.label FROM sf.tpch.t1 t JOIN (VALUES (1, 'one')) x(id, label) ON t.id = x.id", "it's a\\b|one")

	// Snowflake's errors, and queries past query_timeout, fail the
	// statement, not the session.
	expectError(t, eng, "SELECT * FROM sf.tpch.t1 WHERE name = 'boom'", "something Snowflake said")
	started := time.Now()
	expectError(t, eng, "SELECT * FROM sf.tpch.t1 WHERE name = 'slow'", "query_timeout")
	if d := time.Since(started); d > 10*time.Second {
		t.Errorf("query_timeout took %s", d)
	}
	blocked.Wait()
	expect(t, eng, "SELECT count(*) FROM sf.tpch.t1", "4")

	// The gateway answers nothing but the extension's own statements.
	expectError(t, eng, "SELECT * FROM postgres_query('sf', 'SELECT 1')", "not supported")
	expectError(t, eng, "CREATE TABLE sf.tpch.x (i int)", "read-only")
	// A name in a user's statement reaches the extension's catalog query
	// as a string literal; it does not change which query the gateway
	// takes it for (a version check, here, which made DuckDB fail with an
	// internal error).
	for _, name := range []string{"VERSION() LIMIT 0", "PG_EXPORT_SNAPSHOT()", "PG_CLASS.XMIN", "x' OR relname = 't1"} {
		expectError(t, eng, `SELECT * FROM sf.tpch."`+name+`"`, "does not exist")
	}

	// Listing tables never asks Snowflake.
	n := len(fake.sent())
	if err := m.Refresh(context.Background(), "sf"); err != nil {
		t.Fatal(err)
	}
	expect(t, eng, "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'sf'", "1")
	if len(fake.sent()) != n {
		t.Errorf("listing sent %q", fake.sent()[n:])
	}
}

// countingBatch is a batch of one record that counts its downloads.
type countingBatch struct {
	rec arrow.Record
	n   *atomic.Int32
}

func (b countingBatch) fetch(context.Context) ([]arrow.Record, error) {
	b.n.Add(1)
	b.rec.Retain()
	return []arrow.Record{b.rec}, nil
}

// TestSnowflakeSharedPrefetch checks that scans together download no more
// than sharedPrefetch batches ahead of the ones they send: a batch can
// take 160 MB, and every scan downloading eight ahead of the one it sends
// made memory grow with nine times the number of scans.
func TestSnowflakeSharedPrefetch(t *testing.T) {
	// Rows larger than a session's buffer, so that a scan whose client
	// reads nothing waits sending its first.
	sb := array.NewStringBuilder(memory.NewGoAllocator())
	sb.Append(strings.Repeat("x", 1<<20))
	col := sb.NewArray()
	rec := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "C", Type: col.DataType()}}, nil), []arrow.Array{col}, 1)
	typ, _, err := parseSnowflakeType(`{"type":"TEXT","length":16777216}`)
	if err != nil {
		t.Fatal(err)
	}
	scan := &sfScan{columns: []*sfColumn{{name: "C", pgName: "c", typ: typ}}}
	var fetches atomic.Int32
	res := &sfResult{}
	for range 40 {
		res.batches = append(res.batches, countingBatch{rec, &fetches})
	}

	const scans = 3
	var wg sync.WaitGroup
	var clients []net.Conn
	for range scans {
		server, client := net.Pipe()
		clients = append(clients, client)
		s := &sfSession{conn: server, out: bufio.NewWriterSize(server, 256<<10)}
		wg.Go(func() {
			_, _ = s.stream(context.Background(), res, scan, 0)
			_ = server.Close()
		})
	}
	want := int32(scans + sharedPrefetch)
	for deadline := time.Now().Add(5 * time.Second); fetches.Load() < want && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if n := fetches.Load(); n != want {
		t.Errorf("%d scans waiting to send downloaded %d batches, want %d", scans, n, want)
	}
	for _, c := range clients {
		_ = c.Close()
	}
	wg.Wait()
	if n := len(prefetchTokens); n != 0 {
		t.Errorf("%d prefetch tokens not given back", n)
	}
}

// TestSnowflakeDown checks that a Snowflake that cannot be reached when
// Igloo starts leaves its connection out, with the reason, and Igloo
// starts.
func TestSnowflakeDown(t *testing.T) {
	installExtensions(t)
	fake := &fakeSnowflake{cat: testCatalog(t), down: errors.New("dial tcp: no route to host")}
	open := openSnowflake
	openSnowflake = func(Connection, string) sfBackend { return fake }
	t.Cleanup(func() { openSnowflake = open })
	store := NewMemoryStore()
	c := New("sf", Snowflake)
	c.Account, c.User, c.Database, c.Password = "myorg-acct", "u", "db", "pw"
	if err := store.Create(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	// A connection of a type this version does not know, as a newer Igloo
	// sharing the metadata database could make now that the table no
	// longer limits the types, is left out too, without signing in to its
	// host as if it were a PostgreSQL database (as replicas of the version
	// before Snowflake leave out a Snowflake connection).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	dialed := make(chan struct{}, 1)
	go func() {
		if c, err := ln.Accept(); err == nil {
			dialed <- struct{}{}
			_ = c.Close()
		}
	}()
	future := New("future", Type("bigquery"))
	future.Host, future.Port, future.Database, future.User, future.Password = "127.0.0.1", ln.Addr().(*net.TCPAddr).Port, "db", "u", "pw"
	future.TLS = TLSDisable
	if err := store.Create(context.Background(), future); err != nil {
		t.Fatal(err)
	}

	m, eng := startEngine(t, store)
	if _, ok := m.Catalogs()["sf"]; ok {
		t.Fatal("attached")
	}
	if _, ok := m.Catalogs()["future"]; ok {
		t.Fatal("a connection of an unknown type was attached")
	}
	select {
	case <-dialed:
		t.Error("Igloo connected to the host of a connection of an unknown type")
	case <-time.After(200 * time.Millisecond):
	}
	st, err := m.List(context.Background())
	if err != nil || len(st) != 2 || st[0].State != "failed" || !strings.Contains(st[0].Detail, `unsupported type "bigquery"`) ||
		st[1].State != "failed" || !strings.Contains(st[1].Detail, "no route to host") {
		t.Errorf("%+v %v", st, err)
	}
	expect(t, eng, "SELECT 42", "42")
}

// TestSnowflakeSlow checks that a Snowflake that has not listed its tables
// by shortly before the startup's deadline is left out, so that Igloo still
// starts.
func TestSnowflakeSlow(t *testing.T) {
	installExtensions(t)
	fake := &fakeSnowflake{cat: testCatalog(t), slow: true}
	open := openSnowflake
	openSnowflake = func(Connection, string) sfBackend { return fake }
	t.Cleanup(func() { openSnowflake = open })
	store := NewMemoryStore()
	c := New("sf", Snowflake)
	c.Account, c.User, c.Database, c.Password = "myorg-acct", "u", "db", "pw"
	if err := store.Create(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), attachMargin+2*time.Second)
	defer cancel()
	m, eng := startEngineWithin(t, ctx, store)
	if _, ok := m.Catalogs()["sf"]; ok {
		t.Fatal("attached")
	}
	st, err := m.List(context.Background())
	if err != nil || len(st) != 1 || st[0].State != "failed" || !strings.Contains(st[0].Detail, "no answer") {
		t.Errorf("%+v %v", st, err)
	}
	expect(t, eng, "SELECT 42", "42")
}
