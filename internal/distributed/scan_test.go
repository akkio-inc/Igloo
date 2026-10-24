package distributed

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/config"
)

// scanTables are the catalog tables of scanEnv, in schema s.
var scanTables = map[string][]Column{
	"t": {{Name: "a", ID: 1}, {Name: "b", ID: 2}, {Name: "c", ID: 3},
		{Name: "s", ID: 4, Nested: &Nesting{Kind: "struct", Fields: []Field{{Name: "f", ID: 5}, {Name: "g", ID: 6}}}}},
	"u": {{Name: "k", ID: 1}, {Name: "l", ID: 2}},
}

// scanEnv resolves names to scanTables and to views over them.
type scanEnv struct{ views map[string]string }

func (scanEnv) Resolve(_ context.Context, parts []string) (string, string, bool, error) {
	name := strings.ToLower(parts[len(parts)-1])
	if len(parts) > 1 && !strings.EqualFold(parts[len(parts)-2], "s") {
		return "", "", false, nil
	}
	if _, ok := scanTables[name]; !ok {
		return "", "", false, nil
	}
	return "s", name, true, nil
}

func (e scanEnv) ViewDefinition(_ context.Context, parts []string) (*ViewDefinition, bool, error) {
	q, ok := e.views[strings.ToLower(parts[len(parts)-1])]
	if !ok {
		return nil, false, nil
	}
	return &ViewDefinition{Query: q, Resolve: func(_ context.Context, parts []string) ([]string, bool, error) {
		name := strings.ToLower(parts[len(parts)-1])
		if _, ok := scanTables[name]; ok || e.views[name] != "" {
			return []string{"db", "s", name}, true, nil
		}
		return nil, false, nil
	}}, true, nil
}

// The walk finds every catalog table a statement reads, through common
// table expressions, subqueries and views, and the columns it may read of
// each: those it names, or all of them through a star whose columns the
// result needs.
func TestScanColumns(t *testing.T) {
	p, err := newParser()
	if err != nil {
		t.Fatal(err)
	}
	defer p.close() //nolint:errcheck
	env := scanEnv{views: map[string]string{
		"v":  "SELECT * FROM t WHERE c = 1",
		"vv": "SELECT a AS x FROM v",
		"vc": "WITH u AS (SELECT * FROM t) SELECT b FROM u",
	}}
	// Each table's columns, comma separated, then "*" if filtered.
	for _, tc := range []struct {
		query string
		want  map[string]string
	}{
		{"SELECT a FROM s.t WHERE b > 1", map[string]string{"t": "a,b*"}},
		{"SELECT * FROM t", map[string]string{"t": "a,b,c,s"}},
		{"SELECT t.* FROM t JOIN u ON t.a = u.k", map[string]string{"t": "a,b,c,s", "u": "k"}},
		{"SELECT count(*) FROM t", map[string]string{"t": ""}},
		{"SELECT a FROM (SELECT * FROM t) AS q", map[string]string{"t": "a"}},
		{"SELECT * FROM (SELECT * FROM t) AS q", map[string]string{"t": "a,b,c,s"}},
		{"SELECT q.* FROM (SELECT * FROM t) AS q, u", map[string]string{"t": "a,b,c,s", "u": ""}},
		{"WITH c AS (SELECT * FROM t) SELECT c.* FROM c", map[string]string{"t": "a,b,c,s"}},
		{"WITH c AS (SELECT * FROM t) SELECT a FROM c", map[string]string{"t": "a"}},
		{"WITH c AS (SELECT * FROM t), d AS (SELECT * FROM c) SELECT * FROM d", map[string]string{"t": "a,b,c,s"}},
		// A common table expression named as a table is not the table.
		{"WITH t AS (SELECT 1 AS a) SELECT * FROM t", map[string]string{}},
		{"SELECT a FROM t AT (VERSION => 1)", map[string]string{}},
		{"SELECT * FROM read_parquet('x.parquet')", map[string]string{}},
		{"SELECT a FROM v", map[string]string{"t": "a,c*"}},
		{"SELECT * FROM v", map[string]string{"t": "a,b,c,s*"}},
		{"SELECT x FROM vv", map[string]string{"t": "a,c*"}},
		{"SELECT * FROM vc", map[string]string{"t": "b"}},
		{"SELECT a2 FROM t AS x(a2)", map[string]string{"t": "a"}},
		{"SELECT a FROM t WHERE EXISTS (SELECT * FROM u)", map[string]string{"t": "a", "u": ""}},
		{"SELECT a FROM t WHERE a IN (SELECT * FROM u)", map[string]string{"t": "a*", "u": "k,l"}},
		{"SELECT a FROM t UNION ALL SELECT * FROM u", map[string]string{"t": "a", "u": "k,l"}},
		{"SELECT s.f FROM t", map[string]string{"t": "s"}},
		// A star in a function's arguments reads every column.
		{"SELECT m FROM (SELECT max(COLUMNS(*)) AS m FROM t) AS q", map[string]string{"t": "a,b,c,s"}},
		{"SELECT a FROM t NATURAL JOIN u", map[string]string{"t": "a,b,c,s", "u": "k,l"}},
		{"SELECT k FROM t JOIN u USING (a)", map[string]string{"t": "a", "u": "k"}},
	} {
		w, err := walkScans(context.Background(), p, tc.query, env)
		if err != nil {
			t.Errorf("%s: %v", tc.query, err)
			continue
		}
		got := map[string]string{}
		for _, key := range w.order {
			u := w.uses[key]
			var sc Scan
			w.read(&sc, scanTables[u.name], u)
			s := strings.Join(sc.Columns, ",")
			if sc.Filtered {
				s += "*"
			}
			got[u.name] = s
		}
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("%s: read %v, want %v", tc.query, got, tc.want)
		}
	}
}

// A nested column's field IDs are its fields' too, which Parquet's column
// chunks carry.
func TestScanFieldIDs(t *testing.T) {
	p, err := newParser()
	if err != nil {
		t.Fatal(err)
	}
	defer p.close() //nolint:errcheck
	w, err := walkScans(context.Background(), p, "SELECT a, s FROM t", scanEnv{})
	if err != nil {
		t.Fatal(err)
	}
	var sc Scan
	w.read(&sc, scanTables["t"], w.uses[w.order[0]])
	if !slices.Equal(sc.FieldIDs, []int{1, 4, 5, 6}) {
		t.Errorf("field IDs %v, want [1 4 5 6]", sc.FieldIDs)
	}
}

// scanSource serves scanTables, with files, failing to list u's.
type scanSource struct {
	mu    sync.Mutex
	files map[string][]File
	loads int
}

func (s *scanSource) Table(_ context.Context, schema, name string) (*Table, error) {
	s.mu.Lock()
	s.loads++
	s.mu.Unlock()
	return &Table{Schema: schema, Name: name, Columns: scanTables[name], Snapshot: 7}, nil
}

func (s *scanSource) Refresh(context.Context, *Table) error { return nil }

func (s *scanSource) Files(_ context.Context, t *Table) ([]File, error) {
	if t.Name == "u" {
		return nil, errors.New("cannot list u")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.files[t.Name], nil
}

// Scans lists each table's files at its snapshot, one Scan per table in
// the order the statement names them, and leaves out tables it cannot
// list. Changing the Scans leaves the Source's lists alone.
func TestScans(t *testing.T) {
	src := &scanSource{files: map[string][]File{"t": {{Path: "s3://w/t/1.parquet", Size: 10, Rows: 2}, {Path: "s3://w/t/2.parquet", Size: 20, Rows: 3}}}}
	s, err := NewScanner(src)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close() //nolint:errcheck
	scans, err := s.Scans(context.Background(), "SELECT b FROM t AS x JOIN u ON x.a = u.k JOIN t AS y ON y.c = x.c WHERE y.a > 0", scanEnv{})
	if err != nil {
		t.Fatal(err)
	}
	if len(scans) != 1 {
		t.Fatalf("scans %+v, want t's alone", scans)
	}
	sc := scans[0]
	if sc.Schema != "s" || sc.Table != "t" || sc.Snapshot != 7 || !sc.Filtered || strings.Join(sc.Columns, ",") != "a,b,c" ||
		!slices.Equal(sc.FieldIDs, []int{1, 2, 3}) || fmt.Sprint(sc.Files) != fmt.Sprint(src.files["t"]) {
		t.Errorf("scan %+v", sc)
	}
	sc.Files[0].Path = "changed"
	if src.files["t"][0].Path != "s3://w/t/1.parquet" {
		t.Error("changing a Scan's files changed the Source's")
	}
	if _, err := s.Scans(context.Background(), "CREATE TABLE x AS SELECT 1", scanEnv{}); err == nil {
		t.Error("a statement other than SELECT was scanned")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Scans(ctx, "SELECT * FROM t", scanEnv{}); !errors.Is(err, context.Canceled) {
		t.Errorf("scanning with the context done: %v", err)
	}

	// Resolving loads nothing, and listing the files calls no ScanEnv.
	env := &onceEnv{}
	src.loads = 0
	plan, err := s.Resolve(context.Background(), "SELECT a FROM v", env)
	if err != nil {
		t.Fatal(err)
	}
	if src.loads != 0 {
		t.Errorf("resolving loaded %d tables", src.loads)
	}
	env.done = true
	scans, err = plan.Files(context.Background())
	if err != nil || len(scans) != 1 || scans[0].Table != "t" || strings.Join(scans[0].Columns, ",") != "a,c" || !scans[0].Filtered {
		t.Errorf("files of a resolved plan: %+v %v", scans, err)
	}
	if env.late > 0 {
		t.Errorf("listing files called the ScanEnv %d times", env.late)
	}
}

// onceEnv is scanEnv with a view, counting calls once done.
type onceEnv struct {
	done bool
	late int
}

func (e *onceEnv) Resolve(ctx context.Context, parts []string) (string, string, bool, error) {
	if e.done {
		e.late++
	}
	return scanEnv{}.Resolve(ctx, parts)
}

func (e *onceEnv) ViewDefinition(ctx context.Context, parts []string) (*ViewDefinition, bool, error) {
	if e.done {
		e.late++
	}
	return scanEnv{views: map[string]string{"v": "SELECT * FROM t WHERE c = 1"}}.ViewDefinition(ctx, parts)
}

// memCatalog commits one table's metadata in memory.
type memCatalog struct{ tbl *table.Table }

func (c *memCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return c.tbl, nil
}

func (c *memCatalog) CommitTable(_ context.Context, id table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	for _, r := range reqs {
		if err := r.Validate(c.tbl.Metadata()); err != nil {
			return nil, "", fmt.Errorf("%w: %v", table.ErrCommitFailed, err)
		}
	}
	b, err := table.MetadataBuilderFromBase(c.tbl.Metadata(), "")
	if err != nil {
		return nil, "", err
	}
	for _, u := range updates {
		if err := u.Apply(b); err != nil {
			return nil, "", err
		}
	}
	meta, err := b.Build()
	if err != nil {
		return nil, "", err
	}
	c.tbl = table.New(id, meta, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c)
	return meta, "", nil
}

// An Iceberg table's files are listed in the manifest list's order, then
// each manifest's, as DuckDB's Iceberg scan reads them, every time: the
// prefetcher reads them ahead of DuckDB in this order.
func TestFilesInManifestOrder(t *testing.T) {
	ctx := context.Background()
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	cat := &memCatalog{}
	cat.tbl = table.New(table.Identifier{"s", "t"}, meta, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 12 {
		b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		rec := b.NewRecordBatch()
		tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
		if _, err := cat.tbl.AppendTable(ctx, tbl, 100, nil); err != nil {
			t.Fatal(err)
		}
		tbl.Release()
		rec.Release()
		b.Release()
	}
	snap := cat.tbl.CurrentSnapshot()
	manifests, err := snap.Manifests(iceio.LocalFS{})
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) < 2 {
		t.Fatalf("%d manifests, want several", len(manifests))
	}
	var want []string
	for _, mf := range manifests {
		entries, err := mf.FetchEntries(iceio.LocalFS{}, true)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			want = append(want, e.DataFile().FilePath())
		}
	}
	for range 20 {
		tab := &Table{Schema: "s", Name: "t", source: &icebergHandle{tbl: cat.tbl, snap: snap}}
		files, err := NewIceberg(nil).Files(ctx, tab)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(paths(files), want) {
			t.Fatalf("files %v, want %v", paths(files), want)
		}
	}
}

// Every task's view of a catalog table carries its files' sizes, the
// columns its query reads and whether it filters them, and a worker with
// a prefetcher hands them to it as the task starts, whatever the
// coordinator's own setting. Views of exchanges carry nothing: DuckDB
// reads their files around the read cache.
func TestPrefetchTasks(t *testing.T) {
	h := newHarness(t, 0.01, 4, 2)
	var mu sync.Mutex
	var got []Scan
	for _, w := range h.workers {
		w.opts.Prefetch = func(_ context.Context, scans []Scan) {
			mu.Lock()
			got = append(got, scans...)
			mu.Unlock()
		}
	}
	run := func(query string) {
		t.Helper()
		if _, _, _, _, err := h.run(context.Background(), query, ModeOn); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	ids := func(t *Table, names ...string) []int {
		var out []int
		for _, c := range t.Columns {
			if slices.Contains(names, c.Name) {
				out = append(out, c.ID)
			}
		}
		return out
	}

	query := "SELECT l_returnflag, sum(l_quantity) FROM lineitem GROUP BY 1"
	run(query)
	lineitem := h.tables["lineitem"]
	var files []File
	for _, sc := range got {
		if !slices.Equal(sc.FieldIDs, ids(lineitem, "l_quantity", "l_returnflag")) ||
			strings.Join(sc.Columns, ",") != "l_quantity,l_returnflag" || sc.Filtered {
			t.Errorf("scan %+v", sc)
		}
		files = append(files, sc.Files...)
	}
	sizes := map[string]int64{}
	for _, f := range h.files[lineitem.key()] {
		sizes[f.Path] = f.Size
	}
	if len(files) != len(sizes) {
		t.Errorf("prefetched %d files, want lineitem's %d", len(files), len(sizes))
	}
	for _, f := range files {
		if sizes[f.Path] != f.Size || f.Size == 0 {
			t.Errorf("file %s of %d bytes, want %d", f.Path, f.Size, sizes[f.Path])
		}
	}

	// A partitioned join: its later stages read exchanges, which are left
	// out.
	got = nil
	h.co.maxBroadcast = 1
	run("SELECT o_orderpriority, count(*) FROM orders JOIN lineitem ON l_orderkey = o_orderkey WHERE l_shipmode = 'AIR' GROUP BY 1")
	tables := map[string]bool{}
	for _, sc := range got {
		for _, f := range sc.Files {
			if strings.Contains(f.Path, "/"+config.ShuffleDir+"/") {
				t.Errorf("prefetched an exchange's file %s", f.Path)
				continue
			}
			// Only lineitem's is filtered, which DuckDB may read less of.
			if strings.Contains(f.Path, "/orders/") {
				tables["orders"] = true
				if !slices.Equal(sc.FieldIDs, ids(h.tables["orders"], "o_orderkey", "o_orderpriority")) || sc.Filtered {
					t.Errorf("orders scan %+v", sc)
				}
			} else if strings.Contains(f.Path, "/lineitem/") {
				tables["lineitem"] = true
				if !slices.Equal(sc.FieldIDs, ids(lineitem, "l_orderkey", "l_shipmode")) || !sc.Filtered {
					t.Errorf("lineitem scan %+v", sc)
				}
			} else {
				t.Errorf("prefetched %s, which is no table's", f.Path)
			}
		}
	}
	if !tables["orders"] || !tables["lineitem"] {
		t.Errorf("prefetched %v, want orders and lineitem", tables)
	}
	if plan := h.planOf("SELECT o_orderpriority, count(*) FROM orders JOIN lineitem ON l_orderkey = o_orderkey WHERE l_shipmode = 'AIR' GROUP BY 1"); plan.exchanges("join") == 0 {
		t.Error("the join exchanged nothing, so the test shows nothing of exchanges")
	}
}
