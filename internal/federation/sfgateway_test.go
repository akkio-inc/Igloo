package federation

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
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
		case strings.Contains(q, `"NAME" = 'boom'`):
			return nil, errors.New("SQL compilation error: something Snowflake said")
		case strings.Contains(q, `"NAME" = 'slow'`):
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
	if q := last(); q != `SELECT "AMT", "NAME", "ID" FROM "DB"."TPCH"."T1" WHERE "AMT" < 0.00 AND "NAME" = 'it''s a\\b' LIMIT 1` {
		t.Errorf("Snowflake was sent %s", q)
	}
	expect(t, eng, "SELECT count(*) FROM sf.tpch.t1", "4")
	if q := last(); q != `SELECT COUNT(*) FROM "DB"."TPCH"."T1"` {
		t.Errorf("Snowflake was sent %s", q)
	}
	expect(t, eng, "SELECT count(*), sum(id)::VARCHAR FROM sf.tpch.t1 WHERE id = 1", "3|3")

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
	m, eng := startEngine(t, store)
	if _, ok := m.Catalogs()["sf"]; ok {
		t.Fatal("attached")
	}
	st, err := m.List(context.Background())
	if err != nil || len(st) != 1 || st[0].State != "failed" || !strings.Contains(st[0].Detail, "no route to host") {
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
