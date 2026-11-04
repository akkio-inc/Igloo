package distributed

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// restTable creates table s.t, of format version and with fields, in a
// catalog of icebergtest's, and returns a client of the catalog.
func restTable(t *testing.T, version string, fields ...iceberg.NestedField) *catalog.Client {
	t.Helper()
	dir := t.TempDir()
	fake := icebergtest.NewCatalog(dir)
	t.Cleanup(fake.Close)
	cat, err := catalog.Open(t.Context(), config.Config{Catalog: config.Catalog{URI: fake.URL, Warehouse: "test"}, Storage: config.Storage{Location: dir}})
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(t.Context(), table.Identifier{"s"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateTable(t.Context(), table.Identifier{"s", "t"}, iceberg.NewSchema(0, fields...),
		icebergcatalog.WithProperties(iceberg.Properties{"format-version": version})); err != nil {
		t.Fatal(err)
	}
	return cat
}

// alterTable commits a change to the schema of table s.t, as another
// engine would.
func alterTable(t *testing.T, cat *catalog.Client, change func(*table.UpdateSchema) *table.UpdateSchema) {
	t.Helper()
	tbl, err := cat.LoadTable(t.Context(), table.Identifier{"s", "t"})
	if err != nil {
		t.Fatal(err)
	}
	tx := tbl.NewTransaction()
	if err := change(tx.UpdateSchema(true, false)).Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// TestRefreshRefusesChangedTypes checks that a table whose column types
// changed between planning and running, by a promotion another engine
// committed, is not refreshed: workers would read the new snapshot's
// files with the old types, FLOAT for DOUBLE values, and the statement
// runs on one machine instead. Nested fields count, and nested types
// printed as DuckDB prints them (see Coordinator.Table) do not.
func TestRefreshRefusesChangedTypes(t *testing.T) {
	fields := []iceberg.NestedField{
		{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Float32},
		{ID: 2, Name: "s", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 3, Name: "f", Type: iceberg.PrimitiveTypes.Int32}}}},
		{ID: 4, Name: "l", Type: &iceberg.ListType{ElementID: 5, Element: iceberg.PrimitiveTypes.Int32}},
	}
	for name, promote := range map[string]struct {
		path []string
		to   iceberg.Type
	}{
		"float to double": {[]string{"x"}, iceberg.PrimitiveTypes.Float64},
		"struct field":    {[]string{"s", "f"}, iceberg.PrimitiveTypes.Int64},
		"list element":    {[]string{"l", "element"}, iceberg.PrimitiveTypes.Int64},
	} {
		t.Run(name, func(t *testing.T) {
			cat := restTable(t, "2", fields...)
			src := NewIceberg(cat)
			tab, err := src.Table(t.Context(), "s", "t")
			if err != nil {
				t.Fatal(err)
			}
			tab.Columns[1].Type = "STRUCT(f INTEGER)"
			if err := src.Refresh(t.Context(), tab); err != nil {
				t.Fatalf("refresh of an unchanged table: %v", err)
			}
			alterTable(t, cat, func(u *table.UpdateSchema) *table.UpdateSchema {
				return u.UpdateColumn(promote.path, table.ColumnUpdate{FieldType: iceberg.Optional[iceberg.Type]{Val: promote.to, Valid: true}})
			})
			if err := src.Refresh(t.Context(), tab); !IsUnsupported(err) {
				t.Errorf("refresh after the promotion: %v, want the statement kept on one machine", err)
			}
		})
	}
}

// setSchema makes fields the schema of table s.t, as another engine would.
// (iceberg-go's UpdateSchema leaves out the initial defaults of the
// columns it adds.)
func setSchema(t *testing.T, cat *catalog.Client, fields ...iceberg.NestedField) {
	t.Helper()
	ident := table.Identifier{"s", "t"}
	tbl, err := cat.LoadTable(t.Context(), ident)
	if err != nil {
		t.Fatal(err)
	}
	current := tbl.Metadata().CurrentSchema().ID
	if _, _, err := cat.CommitTable(t.Context(), ident, []table.Requirement{table.AssertCurrentSchemaID(current)},
		[]table.Update{table.NewAddSchemaUpdate(iceberg.NewSchema(current+1, fields...)), table.NewSetCurrentSchemaUpdate(-1)}); err != nil {
		t.Fatal(err)
	}
}

// TestInitialDefaultsAreUnsupported checks that workers do not read a
// table of format version 3 with a column, or a field nested in one, that
// has an initial default: they would read it as NULL from the files
// written before it was added, where it is its default. The statement
// runs on one machine instead.
func TestInitialDefaultsAreUnsupported(t *testing.T) {
	id := iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64}
	f := iceberg.NestedField{ID: 3, Name: "f", Type: iceberg.PrimitiveTypes.Int32}
	n := iceberg.NestedField{ID: 4, Name: "n", Type: iceberg.PrimitiveTypes.Int32, InitialDefault: 7}
	s := func(fields ...iceberg.NestedField) iceberg.NestedField {
		return iceberg.NestedField{ID: 2, Name: "s", Type: &iceberg.StructType{FieldList: fields}}
	}
	for name, c := range map[string]struct {
		fields []iceberg.NestedField
		column string
	}{
		"column":       {[]iceberg.NestedField{id, s(f), n}, "n"},
		"nested field": {[]iceberg.NestedField{id, s(f, n)}, "s.n"},
	} {
		t.Run(name, func(t *testing.T) {
			cat := restTable(t, "3", id, s(f))
			if tab, err := NewIceberg(cat).Table(t.Context(), "s", "t"); err != nil || tab.Unsupported != "" {
				t.Fatalf("without initial defaults: %v, %v", tab, err)
			}
			setSchema(t, cat, c.fields...)
			tab, err := NewIceberg(cat).Table(t.Context(), "s", "t")
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(tab.Unsupported, "column "+c.column+" has an initial default") {
				t.Errorf("unsupported %q, want it to name %s's initial default", tab.Unsupported, c.column)
			}
		})
	}
}

// TestDistinctCountsLoaded checks that a table's distinct counts, as
// ANALYZE keeps them in its property, come with it, by lower-case name.
func TestDistinctCountsLoaded(t *testing.T) {
	cat := restTable(t, "2", iceberg.NestedField{ID: 1, Name: "Kind", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	if err := cat.SetDistinctCounts(t.Context(), "s", "t", map[string]int64{"Kind": 7, "id": 0}); err != nil {
		t.Fatal(err)
	}
	tab, err := NewIceberg(cat).Table(t.Context(), "s", "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(tab.Distinct) != 1 || tab.Distinct["kind"] != 7 {
		t.Errorf("distinct counts %v, want kind's 7", tab.Distinct)
	}
	if got := distinctCounts("not json"); got != nil {
		t.Errorf("unreadable counts: %v", got)
	}
}

// TestTableLoadsShared checks that lookups of a table not loaded share one load
// of it, that a lookup of one whose decision lapsed decides by it at once while
// it loads again, that one past tableStale waits for that load, until its
// context ends, that one at a newer version of the table waits for a load at
// that version, and that, watched for changes, a table loads again at its new
// version before a lookup asks for it.
func TestTableLoadsShared(t *testing.T) {
	src := NewIceberg(restTable(t, "2", iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64}))
	var loads atomic.Int32
	gate := make(chan struct{})
	load := src.loader
	src.loader = func(ctx context.Context, schema, name string) (*Table, error) {
		loads.Add(1)
		<-gate
		return load(ctx, schema, name)
	}
	lookup := func(ctx context.Context) error {
		tab, err := src.Table(ctx, "s", "t")
		if err == nil && (len(tab.Columns) != 1 || tab.Columns[0].Name != "id") {
			err = fmt.Errorf("columns %v", tab.Columns)
		}
		return err
	}
	loaded := func(n int32) {
		t.Helper()
		for start := time.Now(); loads.Load() < n; time.Sleep(time.Millisecond) {
			if time.Since(start) > 10*time.Second {
				t.Fatalf("%d loads, want %d", loads.Load(), n)
			}
		}
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := lookup(t.Context()); err != nil {
				t.Error(err)
			}
		})
	}
	loaded(1)
	gate <- struct{}{}
	wg.Wait()
	if n := loads.Load(); n != 1 {
		t.Fatalf("8 lookups loaded the table %d times", n)
	}
	age := func(d time.Duration) {
		src.mu.Lock()
		defer src.mu.Unlock()
		lt := src.tables["s\x00t"]
		lt.at = time.Now().Add(-d)
		src.tables["s\x00t"] = lt
	}
	// The load this lookup starts waits at the gate meanwhile.
	age(tableTTL + time.Second)
	if err := lookup(t.Context()); err != nil {
		t.Fatalf("a lookup of a table loaded %s ago: %v", tableTTL+time.Second, err)
	}
	loaded(2)
	age(tableStale + time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := lookup(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a lookup that waited for a load past its context: %v", err)
	}
	gate <- struct{}{}
	if err := lookup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := loads.Load(); n != 2 {
		t.Errorf("%d loads in all, want 2", n)
	}
	// Once the table's version went up, as a change raises it, a lookup of it
	// loaded just now waits for a load at the new version, which later lookups
	// decide by.
	var version atomic.Uint64
	src.SetVersions(func(string, string) uint64 { return version.Load() })
	if err := lookup(t.Context()); err != nil {
		t.Fatal(err)
	}
	version.Store(1)
	done := make(chan error, 1)
	go func() { done <- lookup(t.Context()) }()
	loaded(3)
	select {
	case err := <-done:
		t.Fatalf("a lookup at a newer version decided by the load at the older one: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	gate <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := lookup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := loads.Load(); n != 3 {
		t.Errorf("%d loads in all, want 3", n)
	}

	// Watched, a change loads the table again before any lookup, and the next
	// lookup decides by that load without waiting.
	var changed atomic.Uint64
	watch, stop := context.WithCancel(t.Context())
	defer stop()
	go src.WatchChanges(watch, changed.Load)
	version.Store(2)
	// Changed again until the watch, which starts from what it first sees,
	// has seen a change.
	for start := time.Now(); loads.Load() < 4; time.Sleep(5 * time.Millisecond) {
		changed.Add(1)
		if time.Since(start) > 10*time.Second {
			t.Fatalf("%d loads, want 4", loads.Load())
		}
	}
	gate <- struct{}{}
	for start := time.Now(); ; time.Sleep(time.Millisecond) {
		src.mu.Lock()
		at := src.tables["s\x00t"].version
		src.mu.Unlock()
		if at == 2 {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatal("the change's load did not end")
		}
	}
	ctx, cancel = context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := lookup(ctx); err != nil {
		t.Fatalf("a lookup after the change was loaded: %v", err)
	}
	if n := loads.Load(); n != 4 {
		t.Errorf("%d loads in all, want 4", n)
	}
}

// TestBounderTopRows checks the rows of the files that hold a column's most
// value: a lookup's versions written to files of their own give the latest
// version's rows; a file without bounds of the column leaves it out.
func TestBounderTopRows(t *testing.T) {
	b := newBounder([]Column{{ID: 1, Name: "Ver", Type: "INTEGER"}, {ID: 2, Name: "day", Type: "DATE"}})
	le := func(v int32) []byte { return binary.LittleEndian.AppendUint32(nil, uint32(v)) }
	b.add(map[int][]byte{1: le(0), 2: le(5)}, map[int][]byte{1: le(1), 2: le(9)}, 1_000_000)
	b.add(map[int][]byte{1: le(2), 2: le(3)}, map[int][]byte{1: le(2), 2: le(9)}, 150_000)
	b.add(map[int][]byte{1: le(2)}, map[int][]byte{1: le(2)}, 39_400)
	if got := b.topRows(3); len(got) != 1 || got["ver"] != 189_400 {
		t.Errorf("top rows %v, want ver's 189,400 alone", got)
	}
	if got := b.bounds(3); got["ver"] != [2]float64{0, 2} {
		t.Errorf("bounds %v", got)
	}
}

// TestFooterTopRows checks the rows of a column's most value counted from
// the row groups of the files that hold it, by their Parquet footers: a
// version column as a Snowflake NUMBER arrives, DECIMAL(38,0), whose
// Iceberg bounds are its unscaled values in big-endian bytes, and a BIGINT.
func TestFooterTopRows(t *testing.T) {
	be := func(v int64) []byte { return big.NewInt(v).FillBytes(make([]byte, 1)) }
	le := func(v int64) []byte { return binary.LittleEndian.AppendUint64(nil, uint64(v)) }
	b := newBounder([]Column{{ID: 3, Name: "VER", Type: "DECIMAL(38,0)"}, {ID: 4, Name: "seq", Type: "BIGINT"}})
	// Three files: of version 0; of versions 0 to 2 in row groups of 4 rows,
	// (0, 0), (0, 2) and (2, 2); and of versions 0 to 1.
	vers := [][]int64{{0, 0, 0, 0, 0}, {0, 0, 0, 0, 0, 1, 2, 2, 2, 2}, {0, 1, 1, 1}}
	var files []File
	data := map[string][]byte{}
	for i, v := range vers {
		seq := make([]int64, len(v))
		for j := range seq {
			seq[j] = int64(10*i + j)
		}
		path := fmt.Sprintf("s3://b/t/data/%d.parquet", i)
		data[path] = parquetOf(t, v, seq, 4)
		files = append(files, File{Path: path, Size: int64(len(data[path])), Rows: int64(len(v))})
		b.add(map[int][]byte{3: be(slices.Min(v)), 4: le(seq[0])}, map[int][]byte{3: be(slices.Max(v)), 4: le(seq[len(seq)-1])}, int64(len(v)))
	}
	if got := b.bounds(3); got["ver"] != [2]float64{0, 2} || got["seq"] != [2]float64{0, 23} {
		t.Errorf("bounds %v", got)
	}
	top := b.topRows(3)
	if top["ver"] != 10 {
		t.Errorf("the files' top rows of ver: %d, want the 10 of the file that holds version 2", top["ver"])
	}
	opened := 0
	fsys := memFS{files: data, opened: &opened}
	latest := b.latestRows(files, func() (icebergio.IO, context.CancelFunc, error) { return fsys, func() {}, nil })
	// ver: the row groups (0, 2) and (2, 2); seq: its last file's one row
	// group, which holds 23. Each column's files are read once.
	for range 2 {
		if n, ok := latest("ver"); !ok || n != 6 {
			t.Errorf("ver by the footers: %d rows (%v), want 6", n, ok)
		}
	}
	if n, ok := latest("seq"); !ok || n != 4 || opened != 2 {
		t.Errorf("seq by the footers: %d rows (%v), want 4; %d files opened, want 2", n, ok, opened)
	}
	// A footer that cannot be read counts nothing.
	unreadable := b.latestRows(files, func() (icebergio.IO, context.CancelFunc, error) { return memFS{opened: &opened}, func() {}, nil })
	if _, ok := unreadable("ver"); ok {
		t.Error("ver counted without its footers")
	}
}

// memFS serves files in memory, by path, counting those opened.
type memFS struct {
	icebergio.IO
	files  map[string][]byte
	opened *int
}

func (m memFS) Open(path string) (icebergio.File, error) {
	d, ok := m.files[path]
	if !ok {
		return nil, errors.New("no such file")
	}
	*m.opened++
	return memFile{bytes.NewReader(d)}, nil
}

// parquetOf writes a Parquet file of a DECIMAL(38,0) column of field ID 3,
// ver, and a BIGINT of field ID 4, seq, in row groups of rowGroup rows.
func parquetOf(t *testing.T, ver, seq []int64, rowGroup int64) []byte {
	t.Helper()
	field := func(name string, typ arrow.DataType, id string) arrow.Field {
		return arrow.Field{Name: name, Type: typ, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{id})}
	}
	schema := arrow.NewSchema([]arrow.Field{field("VER", &arrow.Decimal128Type{Precision: 38, Scale: 0}, "3"), field("seq", arrow.PrimitiveTypes.Int64, "4")}, nil)
	vb := array.NewDecimal128Builder(memory.DefaultAllocator, &arrow.Decimal128Type{Precision: 38, Scale: 0})
	sb := array.NewInt64Builder(memory.DefaultAllocator)
	for i := range ver {
		vb.Append(decimal128.FromI64(ver[i]))
		sb.Append(seq[i])
	}
	rec := array.NewRecord(schema, []arrow.Array{vb.NewArray(), sb.NewArray()}, int64(len(ver)))
	defer rec.Release()
	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(schema, &buf, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(rowGroup)), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// memFile is a file in memory, for footers to be read from.
type memFile struct{ *bytes.Reader }

func (memFile) Close() error               { return nil }
func (memFile) Stat() (fs.FileInfo, error) { return nil, errors.New("no file information") }
