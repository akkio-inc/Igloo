package sharing

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
)

func field(id int, name string, t iceberg.Type, required bool) iceberg.NestedField {
	return iceberg.NestedField{ID: id, Name: name, Type: t, Required: required}
}

func TestDeltaSchema(t *testing.T) {
	s := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: "Row ID"},
		field(2, "name", iceberg.PrimitiveTypes.String, false),
		field(3, "price", iceberg.DecimalTypeOf(12, 2), false),
		field(4, "day", iceberg.PrimitiveTypes.Date, false),
		field(5, "at", iceberg.PrimitiveTypes.Timestamp, false),
		field(6, "at_tz", iceberg.PrimitiveTypes.TimestampTz, false),
		field(7, "ok", iceberg.PrimitiveTypes.Bool, false),
		field(8, "f", iceberg.PrimitiveTypes.Float32, false),
		field(9, "d", iceberg.PrimitiveTypes.Float64, false),
		field(10, "n", iceberg.PrimitiveTypes.Int32, false),
		field(11, "raw", iceberg.PrimitiveTypes.Binary, false),
		field(12, "u", iceberg.PrimitiveTypes.UUID, false),
		field(13, "tags", &iceberg.ListType{ElementID: 14, Element: iceberg.PrimitiveTypes.String, ElementRequired: true}, false),
		field(15, "attrs", &iceberg.MapType{KeyID: 16, KeyType: iceberg.PrimitiveTypes.String, ValueID: 17,
			ValueType: iceberg.PrimitiveTypes.Int64}, false),
		field(18, "info", &iceberg.StructType{FieldList: []iceberg.NestedField{field(19, "a", iceberg.PrimitiveTypes.Int32, true)}}, false),
	)
	got, err := deltaSchema(s)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"type":"struct","fields":[` +
		`{"name":"id","type":"long","nullable":false,"metadata":{"comment":"Row ID"}},` +
		`{"name":"name","type":"string","nullable":true,"metadata":{}},` +
		`{"name":"price","type":"decimal(12,2)","nullable":true,"metadata":{}},` +
		`{"name":"day","type":"date","nullable":true,"metadata":{}},` +
		`{"name":"at","type":"timestamp","nullable":true,"metadata":{}},` +
		`{"name":"at_tz","type":"timestamp","nullable":true,"metadata":{}},` +
		`{"name":"ok","type":"boolean","nullable":true,"metadata":{}},` +
		`{"name":"f","type":"float","nullable":true,"metadata":{}},` +
		`{"name":"d","type":"double","nullable":true,"metadata":{}},` +
		`{"name":"n","type":"integer","nullable":true,"metadata":{}},` +
		`{"name":"raw","type":"binary","nullable":true,"metadata":{}},` +
		`{"name":"u","type":"binary","nullable":true,"metadata":{}},` +
		`{"name":"tags","type":{"type":"array","elementType":"string","containsNull":false},"nullable":true,"metadata":{}},` +
		`{"name":"attrs","type":{"type":"map","keyType":"string","valueType":"long","valueContainsNull":true},"nullable":true,"metadata":{}},` +
		`{"name":"info","type":{"type":"struct","fields":[{"name":"a","type":"integer","nullable":false,"metadata":{}}]},"nullable":true,"metadata":{}}]}`
	if got != want {
		t.Errorf("schema\n got %s\nwant %s", got, want)
	}

	// The schema converts back to DuckDB types for the consumer.
	rs, err := parseRemoteSchema(got)
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for _, f := range rs.Fields {
		dt, err := duckType(f.Type)
		if err != nil {
			t.Fatalf("%s: %v", f.Name, err)
		}
		types = append(types, dt)
	}
	wantTypes := "BIGINT VARCHAR DECIMAL(12,2) DATE TIMESTAMPTZ TIMESTAMPTZ BOOLEAN FLOAT DOUBLE INTEGER BLOB BLOB VARCHAR[] " +
		`MAP(VARCHAR, BIGINT) STRUCT("a" INTEGER)`
	if got := strings.Join(types, " "); got != wantTypes {
		t.Errorf("DuckDB types\n got %s\nwant %s", got, wantTypes)
	}

	for _, typ := range []iceberg.Type{iceberg.PrimitiveTypes.Time, iceberg.PrimitiveTypes.TimestampNs, iceberg.PrimitiveTypes.Unknown,
		&iceberg.ListType{ElementID: 2, Element: iceberg.PrimitiveTypes.TimestampTzNs}} {
		_, err := deltaSchema(iceberg.NewSchema(0, field(1, "c", typ, false)))
		if !isNotShareable(err) || !strings.Contains(err.Error(), "column c") {
			t.Errorf("%s: err = %v, want a not shareable error naming the column", typ, err)
		}
	}
}

func TestSchemaHistory(t *testing.T) {
	current := iceberg.NewSchema(2,
		field(1, "id", iceberg.PrimitiveTypes.Int64, false),
		field(2, "amount", iceberg.PrimitiveTypes.Float64, false),
		field(3, "info", &iceberg.StructType{FieldList: []iceberg.NestedField{field(4, "a", iceberg.PrimitiveTypes.Int32, false)}}, false),
	)
	added := iceberg.NewSchema(1,
		field(1, "id", iceberg.PrimitiveTypes.Int64, false),
		field(3, "info", &iceberg.StructType{FieldList: []iceberg.NestedField{field(4, "a", iceberg.PrimitiveTypes.Int32, false)}}, false),
	)
	if err := checkSchemaHistory(current, []*iceberg.Schema{added, current}, "s.t"); err != nil {
		t.Errorf("an added column: %v", err)
	}

	renamed := iceberg.NewSchema(1, field(1, "id", iceberg.PrimitiveTypes.Int64, false), field(2, "total", iceberg.PrimitiveTypes.Float64, false))
	err := checkSchemaHistory(current, []*iceberg.Schema{renamed, current}, "s.t")
	if !isNotShareable(err) || !strings.Contains(err.Error(), "total of s.t was renamed to amount") {
		t.Errorf("a renamed column: %v", err)
	}

	nested := iceberg.NewSchema(1, field(1, "id", iceberg.PrimitiveTypes.Int64, false),
		field(3, "info", &iceberg.StructType{FieldList: []iceberg.NestedField{field(4, "b", iceberg.PrimitiveTypes.Int32, false)}}, false))
	if err := checkSchemaHistory(current, []*iceberg.Schema{nested}, "s.t"); !isNotShareable(err) || !strings.Contains(err.Error(), "info.b") {
		t.Errorf("a renamed nested field: %v", err)
	}

	// A column dropped and added again under its old name has a new ID;
	// files written before still hold the old one's values.
	dropped := iceberg.NewSchema(1, field(1, "id", iceberg.PrimitiveTypes.Int64, false), field(9, "amount", iceberg.PrimitiveTypes.Float64, false))
	err = checkSchemaHistory(current, []*iceberg.Schema{dropped, current}, "s.t")
	if !isNotShareable(err) || !strings.Contains(err.Error(), "amount of s.t was dropped") || !strings.Contains(err.Error(), "CREATE OR REPLACE TABLE s.t") {
		t.Errorf("a dropped column: %v", err)
	}
}

func bound(t *testing.T, l iceberg.Literal) []byte {
	t.Helper()
	b, err := l.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestFileStats(t *testing.T) {
	s := iceberg.NewSchema(0,
		field(1, "id", iceberg.PrimitiveTypes.Int64, false),
		field(2, "name", iceberg.PrimitiveTypes.String, false),
		field(3, "price", iceberg.DecimalTypeOf(12, 2), false),
		field(4, "day", iceberg.PrimitiveTypes.Date, false),
		field(5, "at", iceberg.PrimitiveTypes.Timestamp, false),
		field(6, "score", iceberg.PrimitiveTypes.Float64, false),
		field(7, "ratio", iceberg.PrimitiveTypes.Float64, false),
		field(8, "tags", &iceberg.ListType{ElementID: 9, Element: iceberg.PrimitiveTypes.String}, false),
		field(10, "before", iceberg.PrimitiveTypes.Timestamp, false),
	)
	day := iceberg.Date(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400)
	at := time.Date(2026, 10, 1, 10, 0, 0, 123456000, time.UTC).UnixMicro()
	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, "s3://b/w/t/data/f.parquet",
		iceberg.ParquetFile, nil, nil, nil, 3, 100)
	if err != nil {
		t.Fatal(err)
	}
	df := b.NullValueCounts(map[int]int64{1: 0, 2: 1, 8: 2}).
		NaNValueCounts(map[int]int64{6: 0, 7: 1}).
		LowerBoundValues(map[int][]byte{
			1: bound(t, iceberg.Int64Literal(-5)), 2: bound(t, iceberg.StringLiteral("aa")),
			3: bound(t, iceberg.DecimalLiteral{Val: decimal128.FromI64(-1050), Scale: 2}),
			4: bound(t, iceberg.DateLiteral(day)), 5: bound(t, iceberg.TimestampLiteral(at)),
			6: bound(t, iceberg.Float64Literal(0.5)), 7: bound(t, iceberg.Float64Literal(0.1)),
			10: bound(t, iceberg.TimestampLiteral(-1500)),
		}).
		UpperBoundValues(map[int][]byte{
			1: bound(t, iceberg.Int64Literal(7)), 2: bound(t, iceberg.StringLiteral("zz")),
			3: bound(t, iceberg.DecimalLiteral{Val: decimal128.FromI64(20025), Scale: 2}),
			4: bound(t, iceberg.DateLiteral(day+1)), 5: bound(t, iceberg.TimestampLiteral(at)),
			6: bound(t, iceberg.Float64Literal(math.Inf(1))), 7: bound(t, iceberg.Float64Literal(0.9)),
			10: bound(t, iceberg.TimestampLiteral(-1500)),
		}).Build()

	var got map[string]any
	if err := json.Unmarshal([]byte(fileStats(df, s)), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"numRecords": 3.0,
		// No strings (writers may truncate them), no floating point
		// columns that may hold NaN or whose bounds are infinite, and
		// timestamps widened to the millisecond.
		"minValues": map[string]any{"id": -5.0, "price": -10.5, "day": "2026-10-01", "at": "2026-10-01T10:00:00.123Z",
			"before": "1969-12-31T23:59:59.998Z"},
		"maxValues": map[string]any{"id": 7.0, "price": 200.25, "day": "2026-10-02", "at": "2026-10-01T10:00:00.124Z",
			"before": "1969-12-31T23:59:59.999Z"},
		"nullCount": map[string]any{"id": 0.0, "name": 1.0},
	}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Errorf("stats\n got %s\nwant %s", gotJSON, wantJSON)
	}
}

func TestViewSQL(t *testing.T) {
	schema := `{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},` +
		`{"name":"day","type":"date","nullable":true,"metadata":{}},{"name":"at","type":"timestamp","nullable":true,"metadata":{}}]}`

	sql, scans, err := viewSQL(&remoteSnapshot{schema: schema}, nil)
	if err != nil || scans != 0 {
		t.Fatal(err, scans)
	}
	if want := `SELECT CAST(NULL AS BIGINT) AS "id", CAST(NULL AS DATE) AS "day", CAST(NULL AS TIMESTAMPTZ) AS "at" LIMIT 0`; sql != want {
		t.Errorf("empty table:\n got %s\nwant %s", sql, want)
	}

	urls := []string{"http://127.0.0.1:1/s/t/a.parquet", "http://127.0.0.1:1/s/t/it's.parquet"}
	sql, scans, err = viewSQL(&remoteSnapshot{schema: schema}, urls)
	if err != nil || scans != 1 {
		t.Fatal(err, scans)
	}
	if want := `SELECT * FROM read_parquet(['http://127.0.0.1:1/s/t/a.parquet', 'http://127.0.0.1:1/s/t/it''s.parquet'], ` +
		`union_by_name = true, hive_partitioning = false)`; sql != want {
		t.Errorf("files:\n got %s\nwant %s", sql, want)
	}

	sql, _, err = viewSQL(&remoteSnapshot{schema: schema, partitionColumns: []string{"day", "at"}}, urls[:1])
	if err != nil {
		t.Fatal(err)
	}
	if want := `hive_partitioning = true, hive_types = {'day': DATE, 'at': TIMESTAMP})`; !strings.HasSuffix(sql, want) {
		t.Errorf("partitioned:\n got %s\nwant it to end in %s", sql, want)
	}

	if _, _, err := viewSQL(&remoteSnapshot{schema: `{"type":"struct","fields":[]}`}, nil); err == nil {
		t.Error("a table without columns made a view")
	}
}
