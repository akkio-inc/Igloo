package distributed

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/big"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// Source finds the data files of catalog tables.
type Source interface {
	// Table returns schema.name, with its columns named and typed as DuckDB
	// reads them. Workers cannot read it if Unsupported is set. Its snapshot
	// may be up to a minute old: it decides only whether to distribute.
	Table(ctx context.Context, schema, name string) (*Table, error)
	// Refresh brings t to the table's current snapshot. It fails if the
	// table's columns changed since, their types included.
	Refresh(ctx context.Context, t *Table) error
	// Files lists the data files of t's snapshot.
	Files(ctx context.Context, t *Table) ([]File, error)
}

// File is a data file.
type File struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
	// Rows is the file's record count.
	Rows int64 `json:"rows"`
	// Deletes, if set, are the delete files that apply to it.
	Deletes *Deletes `json:"deletes,omitempty"`
	// Buckets are its bucket of each of its table's Buckets, -1 for the
	// rows whose column is NULL.
	Buckets []int `json:"-"`
}

// Deletes are the delete files that apply to a data file, as the scan's
// planning matched them to it: by partition and sequence number, and
// deletion vectors by the data file they name.
type Deletes struct {
	// Positions are position delete files: Parquet files whose file_path
	// and pos columns list the deleted rows of this and other data files.
	Positions []string `json:"positions,omitempty"`
	// Vectors are deletion vectors (Iceberg format version 3): blobs of
	// Puffin files that mark the positions of the file's deleted rows.
	Vectors []Vector `json:"vectors,omitempty"`
	// Equality are equality delete files: a row is deleted if one of
	// their rows has its values in the columns they compare.
	Equality []EqualityDelete `json:"equality,omitempty"`
}

// Vector is a deletion vector: Size bytes at Offset of a Puffin file,
// which delete Rows rows.
type Vector struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
	Rows   int64  `json:"rows"`
}

// EqualityDelete is an equality delete file and the field IDs of the
// columns it compares, columns of the table's.
type EqualityDelete struct {
	Path string `json:"path"`
	IDs  []int  `json:"ids"`
}

// Iceberg is the Source for Igloo's Iceberg catalog. Every statement loads
// each table's metadata afresh, so a statement sees every commit made
// before it, as on one machine; the file lists of a snapshot are kept,
// since a snapshot never changes.
type Iceberg struct {
	cat *catalog.Client

	mu    sync.Mutex
	files map[string][]File
	// stats are what each snapshot's files' statistics give its columns
	// (see fileStats), by the same keys as files.
	stats map[string]fileStats
	order []string
	// cached counts the files of the snapshots kept.
	cached int
	// tables are recently loaded tables, for deciding, and loading the loads
	// under way, by the same keys.
	tables  map[string]loadedTable
	loading map[string]*tableLoad
	// loader loads a table's metadata: load, which tests replace. versions,
	// if set, says a table's version, which goes up whenever the table may
	// have changed (see SetVersions).
	loader   func(ctx context.Context, schema, name string) (*Table, error)
	versions func(schema, name string) uint64
}

type loadedTable struct {
	t       *Table
	at      time.Time
	version uint64
}

// tableLoad is a load of a table's metadata at version, which every lookup of
// the table at that version meanwhile waits for: t and err once done is closed.
type tableLoad struct {
	done    chan struct{}
	version uint64
	t       *Table
	err     error
}

// A loaded table decides whether a statement is distributed for tableTTL; past
// that, for up to tableStale, the lookup that finds it starts loading it again
// and decides by it meanwhile, as later lookups do until the load is done; as
// long as the table's version is the one it was loaded at, which a change to
// the table raises. Lookups of a table not loaded, loaded longer ago or at an
// older version, share one load. Each lookup of a table whose decision had
// lapsed loaded it again itself: under a mixed load, lookups that loaded the
// same tables at once took 0.3 to 0.7 s each, past the half second auto mode
// gives the planning of a cheap statement (see limitPlanning). A statement
// that runs on workers loads its tables afresh before it runs (Refresh), and
// runs on one machine if their columns changed.
const (
	tableTTL     = 10 * time.Second
	tableStale   = time.Minute
	tableLoadMax = 30 * time.Second
)

// maxCachedSnapshots and maxCachedFiles bound the snapshots whose file
// lists are kept, and their files: a table of 100,000 files committed to
// every minute would otherwise keep 256 lists of it, gigabytes, once a
// Scanner lists the tables of every statement.
const (
	maxCachedSnapshots = 256
	maxCachedFiles     = 1 << 20
)

// NewIceberg reads tables through cat.
func NewIceberg(cat *catalog.Client) *Iceberg {
	s := &Iceberg{cat: cat, files: map[string][]File{}, tables: map[string]loadedTable{}, loading: map[string]*tableLoad{}}
	s.loader = s.load
	return s
}

type icebergHandle struct {
	tbl  *table.Table
	snap *table.Snapshot
	// columns are the table's columns as loaded, which Refresh compares:
	// Coordinator.Table prints nested types afresh in the Table's own.
	columns []Column
}

// SetVersions has s decide by a loaded table only at the version it was loaded
// at (see tableTTL): versions says a table's version, which goes up whenever
// the table may have changed. The catalog proxy's (catalogproxy.TableVersion)
// goes up with every change DuckDB makes to it, and when DuckDB, preparing a
// statement, loads a table another replica or engine changed; so a statement
// is planned with its tables as it was prepared with them, whatever their age.
// It is set before s is used.
func (s *Iceberg) SetVersions(versions func(schema, name string) uint64) { s.versions = versions }

// WatchChanges loads again, in the background, the tables loaded lately whose
// version went up, until ctx ends: whenever changed, which goes up with any
// table's version (catalogproxy.Proxy.DataVersion), has gone up since it last
// looked, every tableWatch. A lookup of a table at a version not loaded waited
// for its load, 0.3 to 1.2 s under a mixed load: in one window, 37 of the 38
// lookups that waited so began within 7 s of a commit of the ELT, and so did
// 17 of the 19 statements that gave up planning at the half second auto mode
// gives a cheap one (see limitPlanning). Loaded as the commit is made, the
// table is there by the time they look it up, or nearly.
func (s *Iceberg) WatchChanges(ctx context.Context, changed func() uint64) {
	if s.versions == nil {
		return
	}
	seen := changed()
	tick := time.NewTicker(tableWatch)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if v := changed(); v != seen {
			seen = v
			s.loadChanged()
		}
	}
}

// tableWatch is how often WatchChanges looks for changed tables.
const tableWatch = 100 * time.Millisecond

// loadChanged starts loading the tables loaded within tableStale whose
// version is no longer the one they were loaded at.
func (s *Iceberg) loadChanged() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, lt := range s.tables {
		schema, name, _ := strings.Cut(key, "\x00")
		if v := s.versions(schema, name); v != lt.version && time.Since(lt.at) < tableStale {
			s.startLoad(key, schema, name, v)
		}
	}
}

// Table returns a table's metadata, loaded at most tableTTL ago, or at most
// tableStale ago while it loads again, at its current version (see tableTTL).
func (s *Iceberg) Table(ctx context.Context, schema, name string) (*Table, error) {
	key := schema + "\x00" + name
	var version uint64
	if s.versions != nil {
		version = s.versions(schema, name)
	}
	s.mu.Lock()
	lt, ok := s.tables[key]
	if age := time.Since(lt.at); ok && lt.version == version && age < tableStale {
		if age >= tableTTL {
			s.startLoad(key, schema, name, version)
		}
		s.mu.Unlock()
		return copyTable(lt.t), nil
	}
	ld := s.startLoad(key, schema, name, version)
	s.mu.Unlock()
	select {
	case <-ld.done:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
	if ld.err != nil {
		return nil, ld.err
	}
	return copyTable(ld.t), nil
}

// startLoad returns the load of the table at version under way, or starts one,
// which a load at an older version then makes way for. It runs apart from the
// context of the lookup that started it, which may end while others wait for
// it, for at most tableLoadMax. s.mu is held.
func (s *Iceberg) startLoad(key, schema, name string, version uint64) *tableLoad {
	if ld := s.loading[key]; ld != nil && ld.version == version {
		return ld
	}
	ld := &tableLoad{done: make(chan struct{}), version: version}
	s.loading[key] = ld
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), tableLoadMax)
		defer cancel()
		t, err := s.loader(ctx, schema, name)
		s.mu.Lock()
		ld.t, ld.err = t, err
		// One that made way for a load at a newer version may have read the
		// table before what raised its version.
		if s.loading[key] == ld {
			if err == nil {
				if len(s.tables) > 4096 {
					clear(s.tables)
				}
				s.tables[key] = loadedTable{t: t, at: time.Now(), version: version}
			}
			delete(s.loading, key)
		}
		s.mu.Unlock()
		close(ld.done)
	}()
	return ld
}

// copyTable is t with columns of its own, for a caller to change.
func copyTable(t *Table) *Table {
	c := *t
	c.Columns = slices.Clone(t.Columns)
	return &c
}

// Refresh loads the table's current metadata into t. The statement was
// planned with t's columns, and workers read the new snapshot's files with
// their types: a type promoted since, such as FLOAT to DOUBLE, would read
// the new files' values as the old type, so any change to the columns,
// nested fields included, keeps the statement on one machine.
func (s *Iceberg) Refresh(ctx context.Context, t *Table) error {
	h, ok := t.source.(*icebergHandle)
	if !ok {
		return fmt.Errorf("table %s was not loaded from the catalog", t)
	}
	fresh, err := s.load(ctx, t.Schema, t.Name)
	if err != nil {
		return err
	}
	if !slices.EqualFunc(fresh.Columns, h.columns, sameColumn) {
		return unsupported("the columns of %s changed", t)
	}
	if fresh.Unsupported != "" {
		return unsupported("table %s: %s", t, fresh.Unsupported)
	}
	t.Bytes, t.Snapshot, t.Versioned, t.source = fresh.Bytes, fresh.Snapshot, fresh.Versioned, fresh.source
	return nil
}

// sameColumn reports whether workers read a and b, columns as loaded, the
// same way: by the same field IDs and as the same type, nested fields
// included. Names may differ in case alone, as DuckDB resolves them.
func sameColumn(a, b Column) bool {
	return a.ID == b.ID && strings.EqualFold(a.Name, b.Name) && a.Type == b.Type && reflect.DeepEqual(a.Nested, b.Nested)
}

// load loads a table's current metadata from the catalog.
func (s *Iceberg) load(ctx context.Context, schema, name string) (*Table, error) {
	tbl, err := s.cat.LoadTable(ctx, catalog.Ident(schema, name))
	if err != nil {
		return nil, fmt.Errorf("load %s.%s: %w", schema, name, err)
	}
	t := &Table{Schema: schema, Name: name, Versioned: true}
	for _, f := range tbl.Schema().Fields() {
		c, ok := icebergColumn(f.Name, f.ID, f.Type)
		if !ok && t.Unsupported == "" {
			t.Unsupported = fmt.Sprintf("column %s has type %s", f.Name, f.Type)
		}
		t.Columns = append(t.Columns, c)
	}
	// Workers read a field as NULL from the data files written before it
	// was added, which lack it; format version 3 gives it its initial
	// default there, as DuckDB's own Iceberg scan does.
	if name, ok := initialDefault(tbl.Schema().Fields()); ok && t.Unsupported == "" {
		t.Unsupported = fmt.Sprintf("column %s has an initial default, which workers do not apply", name)
	}
	// A name mapping (schema.name-mapping.default) applies only to files
	// without field IDs, which workers refuse; iceberg-go adds one to every
	// table it compacts, whose files all have field IDs.
	t.Distinct = distinctCounts(tbl.Properties()[catalog.DistinctCountsProperty])
	snap := tbl.CurrentSnapshot()
	t.Buckets = icebergBuckets(ctx, tbl, snap)
	// Iceberg's own statistics: the distinct counts other engines' theta
	// sketches give the columns, where ANALYZE gave none.
	t.Distinct = withSketchedDistinct(t.Distinct, tbl.Metadata().Statistics(), tbl.Schema(), snap)
	t.source = &icebergHandle{tbl: tbl, snap: snap, columns: t.Columns}
	if snap == nil {
		return t, nil
	}
	t.Snapshot = snap.SnapshotID
	var props iceberg.Properties
	if snap.Summary != nil {
		props = snap.Summary.Properties
	}
	if records, err := strconv.ParseInt(props["total-records"], 10, 64); err == nil {
		t.Rows = records
	}
	if size, err := strconv.ParseInt(props["total-files-size"], 10, 64); err == nil {
		t.Bytes = size
		return t, nil
	}
	// Without a summary, add up the files.
	files, err := s.Files(ctx, t)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		t.Bytes += f.Size
	}
	return t, nil
}

// Files plans a scan of the table's snapshot.
func (s *Iceberg) Files(ctx context.Context, t *Table) ([]File, error) {
	h, ok := t.source.(*icebergHandle)
	if !ok {
		return nil, fmt.Errorf("table %s was not loaded from the catalog", t)
	}
	if h.snap == nil {
		return nil, nil
	}
	key := h.tbl.MetadataLocation() + "#" + strconv.FormatInt(h.snap.SnapshotID, 10)
	s.mu.Lock()
	files, ok := s.files[key]
	st := s.stats[key]
	s.mu.Unlock()
	if ok {
		st.set(t)
		return files, nil
	}

	tasks, err := h.tbl.Scan(table.WithSnapshotID(h.snap.SnapshotID)).PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("plan scan of %s: %w", t, err)
	}
	files = make([]File, 0, len(tasks))
	bucketed := len(t.Buckets) > 0
	bounding := newBounder(t.Columns)
	for _, task := range tasks {
		bounding.add(task.File.LowerBoundValues(), task.File.UpperBoundValues(), task.File.Count())
		bounding.size(task.File.ColumnSizes())
		if f := strings.ToUpper(string(task.File.FileFormat())); f != "PARQUET" {
			return nil, unsupported("%s has %s files", t, f)
		}
		f := File{Path: task.File.FilePath(), Size: task.File.FileSizeBytes(), Rows: task.File.Count()}
		if bucketed {
			// A file whose bucket cannot be told leaves every file without
			// one: a stage reading the table by bucket then fails, and the
			// statement runs on one machine.
			if f.Buckets, bucketed = fileBuckets(h.tbl.Metadata(), task.File, t.Buckets); !bucketed {
				for i := range files {
					files[i].Buckets = nil
				}
			}
		}
		if n := len(task.DeleteFiles) + len(task.DeletionVectorFiles) + len(task.EqualityDeleteFiles); n > 0 {
			f.Deletes = &Deletes{}
		}
		for _, d := range task.DeleteFiles {
			if format := strings.ToUpper(string(d.FileFormat())); format != "PARQUET" {
				return nil, unsupported("%s has %s delete files", t, format)
			}
			f.Deletes.Positions = append(f.Deletes.Positions, d.FilePath())
		}
		for _, d := range task.DeletionVectorFiles {
			off, size := d.ContentOffset(), d.ContentSizeInBytes()
			if format := strings.ToUpper(string(d.FileFormat())); format != "PUFFIN" || off == nil || size == nil || *off < 0 || *size < 8 {
				return nil, unsupported("%s has a deletion vector Igloo cannot read (%s)", t, d.FilePath())
			}
			f.Deletes.Vectors = append(f.Deletes.Vectors, Vector{Path: d.FilePath(), Offset: *off, Size: *size, Rows: d.Count()})
		}
		for _, d := range task.EqualityDeleteFiles {
			if format := strings.ToUpper(string(d.FileFormat())); format != "PARQUET" {
				return nil, unsupported("%s has %s delete files", t, format)
			}
			ids := d.EqualityFieldIDs()
			if len(ids) == 0 {
				return nil, unsupported("%s has an equality delete file without columns", t)
			}
			for _, id := range ids {
				// Fields nested in structs may be compared too; workers compare
				// columns of the table only.
				if !slices.ContainsFunc(t.Columns, func(c Column) bool { return c.ID == id && c.Nested == nil }) {
					return nil, unsupported("%s has equality deletes by field %d, which is not one of its columns", t, id)
				}
			}
			f.Deletes.Equality = append(f.Deletes.Equality, EqualityDelete{Path: d.FilePath(), IDs: slices.Clone(ids)})
		}
		files = append(files, f)
	}

	st = fileStats{bounds: bounding.bounds(len(tasks)), sizes: bounding.columnBytes(len(tasks)), top: bounding.topRows(len(tasks))}
	if len(st.top) > 0 {
		// Read when a statement asks, with a context of its own: the
		// snapshot's stats outlive this call.
		tbl := h.tbl
		st.latest = bounding.latestRows(files, func() (icebergio.IO, context.CancelFunc, error) {
			ctx, cancel := context.WithTimeout(context.Background(), footerReadLimit)
			fsys, err := tbl.FS(ctx)
			return fsys, cancel, err
		})
	}
	st.set(t)
	s.mu.Lock()
	if _, ok := s.files[key]; !ok {
		if s.stats == nil {
			s.stats = map[string]fileStats{}
		}
		s.files[key], s.stats[key] = files, st
		s.order = append(s.order, key)
		s.cached += len(files)
		for len(s.order) > maxCachedSnapshots || s.cached > maxCachedFiles && len(s.order) > 1 {
			s.cached -= len(s.files[s.order[0]])
			delete(s.files, s.order[0])
			delete(s.stats, s.order[0])
			s.order = s.order[1:]
		}
	}
	s.mu.Unlock()
	return files, nil
}

// fileStats are what a snapshot's data files' statistics give its columns:
// their bounds (Table.Bounds), sizes (Table.ColumnBytes) and the rows of
// the files that hold their most values (Table.TopRows).
type fileStats struct {
	bounds map[string][2]float64
	sizes  map[string]int64
	top    map[string]int64
	latest func(col string) (int64, bool)
}

func (f fileStats) set(t *Table) {
	t.Bounds, t.ColumnBytes, t.TopRows, t.latestRows = f.bounds, f.sizes, f.top, f.latest
}

// bounder gathers the bounds of a table's columns of integer types, dates
// and timestamps over its data files: the least of their lower bounds and
// the most of their upper ones (see Table.Bounds), and the rows of the
// files whose upper bound is that most (Table.TopRows).
type bounder struct {
	// kinds are the columns bounded, by field ID: their names, and how their
	// values are written (Iceberg's single-value serialization).
	kinds map[int]boundKind
	lo    map[int]float64
	hi    map[int]float64
	top   map[int]int64
	// topFiles are the files, in the order added, whose upper bound of each
	// field is its most; files counts those added.
	topFiles map[int][]int
	files    int
	// seen counts the files with both bounds of each field.
	seen map[int]int
	// names are the top-level columns' names by field ID, and sizes their
	// compressed bytes over the files, which sized counts.
	names map[int]string
	sizes map[int]int64
	sized map[int]int
}

type boundKind struct {
	name string
	size int
}

func newBounder(cols []Column) *bounder {
	b := &bounder{kinds: map[int]boundKind{}, lo: map[int]float64{}, hi: map[int]float64{}, top: map[int]int64{}, topFiles: map[int][]int{}, seen: map[int]int{},
		names: map[int]string{}, sizes: map[int]int64{}, sized: map[int]int{}}
	for _, c := range cols {
		if c.ID != 0 {
			b.names[c.ID] = strings.ToLower(c.Name)
		}
		if c.ID == 0 || c.Nested != nil {
			continue
		}
		switch t := strings.ToUpper(c.Type); {
		case t == "INTEGER", t == "DATE", t == "SMALLINT", t == "TINYINT":
			b.kinds[c.ID] = boundKind{strings.ToLower(c.Name), 4}
		case t == "BIGINT", t == "TIMESTAMP", t == "TIMESTAMP WITH TIME ZONE":
			b.kinds[c.ID] = boundKind{strings.ToLower(c.Name), 8}
		case wholeDecimal.MatchString(t):
			b.kinds[c.ID] = boundKind{strings.ToLower(c.Name), decimalBound}
		}
	}
	return b
}

// wholeDecimal matches a decimal type of no fractional digits, as a
// Snowflake NUMBER of integers arrives: its values are integers as an
// integer type's are.
var wholeDecimal = regexp.MustCompile(`^DECIMAL\(\d+,\s*0\)$`)

// decimalBound is the size of a decimal's bound, which Iceberg writes as its
// unscaled value in big-endian two's complement, in as few bytes as it
// takes.
const decimalBound = -1

// value decodes a bound of a field of size bytes: a little-endian integer,
// or a decimal's unscaled value (decimalBound).
func (b *bounder) value(raw []byte, size int) (float64, bool) {
	switch {
	case size == 4 && len(raw) == 4:
		return float64(int32(binary.LittleEndian.Uint32(raw))), true
	case size == 8 && len(raw) == 8:
		return float64(int64(binary.LittleEndian.Uint64(raw))), true
	case size == decimalBound:
		return bigEndianInt(raw)
	}
	return 0, false
}

// bigEndianInt decodes a two's complement integer of 1 to 16 big-endian
// bytes, as Iceberg's bounds and Parquet's statistics write a decimal's
// unscaled value.
func bigEndianInt(raw []byte) (float64, bool) {
	if len(raw) == 0 || len(raw) > 16 {
		return 0, false
	}
	v := new(big.Int).SetBytes(raw)
	if raw[0]&0x80 != 0 {
		v.Sub(v, new(big.Int).Lsh(big.NewInt(1), uint(8*len(raw))))
	}
	f, _ := v.Float64()
	return f, true
}

// size takes in a data file's columns' sizes.
func (b *bounder) size(sizes map[int]int64) {
	for id := range b.names {
		if n, ok := sizes[id]; ok {
			b.sizes[id] += n
			b.sized[id]++
		}
	}
}

// columnBytes are the sizes of the top-level columns every one of files
// data files had a size of, by lower-case name; nil if none. A nested
// column's size is its first field's, which counts it in part.
func (b *bounder) columnBytes(files int) map[string]int64 {
	var out map[string]int64
	for id, name := range b.names {
		if files == 0 || b.sized[id] != files {
			continue
		}
		if out == nil {
			out = map[string]int64{}
		}
		out[name] = b.sizes[id]
	}
	return out
}

// add takes in the next data file's lower and upper bounds, and its rows.
func (b *bounder) add(lower, upper map[int][]byte, rows int64) {
	for id, k := range b.kinds {
		lo, okLo := b.value(lower[id], k.size)
		hi, okHi := b.value(upper[id], k.size)
		if !okLo || !okHi {
			continue
		}
		switch {
		case b.seen[id] == 0 || hi > b.hi[id]:
			b.top[id], b.topFiles[id] = rows, []int{b.files}
		case hi == b.hi[id]:
			b.top[id], b.topFiles[id] = b.top[id]+rows, append(b.topFiles[id], b.files)
		}
		if b.seen[id] == 0 {
			b.lo[id], b.hi[id] = lo, hi
		} else {
			b.lo[id], b.hi[id] = min(b.lo[id], lo), max(b.hi[id], hi)
		}
		b.seen[id]++
	}
	b.files++
}

// topRows are the rows of the files whose upper bound of a column is its
// most, of the columns every one of files data files had bounds of, by
// lower-case name; nil if none.
func (b *bounder) topRows(files int) map[string]int64 {
	var out map[string]int64
	for id, k := range b.kinds {
		if files == 0 || b.seen[id] != files {
			continue
		}
		if out == nil {
			out = map[string]int64{}
		}
		out[k.name] = b.top[id]
	}
	return out
}

// footerFilesMost is how many files' footers a column's latest rows are
// counted from (see latestRows), and footerReadLimit how long reading them
// may take; with more, or longer, the files' rows stand. The lookup of
// 14.1 billion rows at a hundred times the data has its latest version in 8
// of its 1,483 files, 76.3 million rows, and in row groups of 2.05 million.
const (
	footerFilesMost = 16
	footerReadLimit = 10 * time.Second
)

// latestRows returns what Table.LatestRows does for a table of files, which
// b took in, read through fs when first asked for a column: the rows of the
// row groups of the files that hold the column's most value whose most
// value it is, by the files' Parquet footers. A lookup's versions written
// in turn sit in row groups of their own where its files hold several: a
// lookup of 1.41 billion rows over versions 0 to 2, whose 2 files that hold
// version 2 have 19.7 million rows, has 384,204 in their row groups that
// hold it, and 189,400 of version 2. Where more than footerFilesMost files
// hold the most value, or a footer cannot be read, ok is false.
func (b *bounder) latestRows(files []File, fs func() (icebergio.IO, context.CancelFunc, error)) func(col string) (int64, bool) {
	type count struct {
		rows int64
		ok   bool
	}
	var mu sync.Mutex
	counted := map[string]count{}
	ids := map[string]int{}
	for id, k := range b.kinds {
		ids[k.name] = id
	}
	return func(col string) (int64, bool) {
		mu.Lock()
		defer mu.Unlock()
		if c, done := counted[col]; done {
			return c.rows, c.ok
		}
		id, known := ids[col]
		held := b.topFiles[id]
		c := count{}
		if known && b.seen[id] == len(files) && len(held) > 0 && len(held) <= footerFilesMost {
			fsys, release, err := fs()
			if err == nil {
				c.ok = true
				for _, i := range held {
					n, ok := rowsAtMost(fsys.Open, files[i], id, b.hi[id])
					c.rows, c.ok = c.rows+n, c.ok && ok
				}
			}
			if release != nil {
				release()
			}
		}
		counted[col] = c
		return c.rows, c.ok
	}
}

// rowsAtMost is the rows of the row groups of f whose most value of field
// id, by their statistics, is most or more; ok is false where its footer
// cannot be read or a row group's statistics of the field cannot be
// decoded.
func rowsAtMost(open func(path string) (icebergio.File, error), f File, id int, most float64) (rows int64, ok bool) {
	r, err := open(f.Path)
	if err != nil {
		return 0, false
	}
	defer r.Close()
	meta, err := parquetFooter(r, f.Size)
	if err != nil {
		return 0, false
	}
	col := -1
	for i := 0; i < meta.Schema.NumColumns(); i++ {
		if meta.Schema.Column(i).SchemaNode().FieldID() == int32(id) {
			col = i
		}
	}
	if col < 0 {
		return 0, false
	}
	for g := 0; g < meta.NumRowGroups(); g++ {
		rg := meta.RowGroup(g)
		cc, err := rg.ColumnChunk(col)
		if err != nil {
			return 0, false
		}
		hi, known := statMost(cc)
		if !known {
			return 0, false
		}
		if hi >= most {
			rows += rg.NumRows()
		}
	}
	return rows, true
}

// parquetFooter reads the Parquet footer of r, of size bytes.
func parquetFooter(r io.ReaderAt, size int64) (*metadata.FileMetaData, error) {
	if size < 12 {
		return nil, fmt.Errorf("a Parquet file of %d bytes", size)
	}
	tail := make([]byte, 8)
	if _, err := r.ReadAt(tail, size-8); err != nil {
		return nil, err
	}
	if string(tail[4:]) != "PAR1" {
		return nil, errors.New("not a Parquet file")
	}
	n := int64(binary.LittleEndian.Uint32(tail))
	if n+8 > size {
		return nil, errors.New("a footer longer than its file")
	}
	buf := make([]byte, n)
	if _, err := r.ReadAt(buf, size-8-n); err != nil {
		return nil, err
	}
	return metadata.NewFileMetaData(buf, nil)
}

// statMost is the most value of a column chunk by its statistics: an
// integer's, or a decimal's unscaled value.
func statMost(cc *metadata.ColumnChunkMetaData) (float64, bool) {
	if set, err := cc.StatsSet(); err != nil || !set {
		return 0, false
	}
	st, err := cc.Statistics()
	if err != nil || st == nil || !st.HasMinMax() {
		return 0, false
	}
	switch s := st.(type) {
	case *metadata.Int32Statistics:
		return float64(s.Max()), true
	case *metadata.Int64Statistics:
		return float64(s.Max()), true
	case *metadata.FixedLenByteArrayStatistics:
		return bigEndianInt(s.Max())
	case *metadata.ByteArrayStatistics:
		return bigEndianInt(s.Max())
	}
	return 0, false
}

// bounds are the bounds of the columns every one of files data files had
// bounds of, by lower-case name; nil if none.
func (b *bounder) bounds(files int) map[string][2]float64 {
	var out map[string][2]float64
	for id, k := range b.kinds {
		if files == 0 || b.seen[id] != files {
			continue
		}
		if out == nil {
			out = map[string][2]float64{}
		}
		out[k.name] = [2]float64{b.lo[id], b.hi[id]}
	}
	return out
}

// distinctCounts reads the distinct counts ANALYZE keeps in a table's
// property (catalog.DistinctCountsProperty), by lower-case column name;
// nil if there are none or they cannot be read.
func distinctCounts(prop string) map[string]int64 {
	if prop == "" {
		return nil
	}
	var counts map[string]int64
	if err := json.Unmarshal([]byte(prop), &counts); err != nil {
		return nil
	}
	out := make(map[string]int64, len(counts))
	for name, n := range counts {
		if n > 0 {
			out[strings.ToLower(name)] = n
		}
	}
	return out
}

// withSketchedDistinct adds to counts, distinct counts by lower-case
// column name, those the table's statistics files give: the ndv property
// of an apache-datasketches-theta-v1 blob of one field, of the snapshot
// snap, or else of the latest snapshot that has statistics. A column
// counts already keeps its count.
func withSketchedDistinct(counts map[string]int64, files iter.Seq[table.StatisticsFile], schema *iceberg.Schema, snap *table.Snapshot) map[string]int64 {
	var chosen *table.StatisticsFile
	for f := range files {
		switch {
		case snap != nil && f.SnapshotID == snap.SnapshotID:
			chosen = &f
		case chosen == nil || (snap == nil || chosen.SnapshotID != snap.SnapshotID) && f.SnapshotID > chosen.SnapshotID:
			chosen = &f
		}
	}
	if chosen == nil || schema == nil {
		return counts
	}
	for _, b := range chosen.BlobMetadata {
		if b.Type != table.BlobTypeApacheDatasketchesThetaV1 || len(b.Fields) != 1 {
			continue
		}
		ndv, err := strconv.ParseInt(b.Properties["ndv"], 10, 64)
		field, ok := schema.FindFieldByID(int(b.Fields[0]))
		if err != nil || ndv <= 0 || !ok {
			continue
		}
		name := strings.ToLower(field.Name)
		if counts == nil {
			counts = map[string]int64{}
		}
		if _, have := counts[name]; !have {
			counts[name] = ndv
		}
	}
	return counts
}

// icebergColumn returns a column of Iceberg type t as DuckDB reads it: a
// list as T[], a map as MAP(K, V), a struct as STRUCT(name T, ...), with
// the nested fields' names and IDs. Struct fields' names are all quoted
// here; the coordinator prints the type as DuckDB does (see
// Coordinator.Table).
func icebergColumn(name string, id int, t iceberg.Type) (Column, bool) {
	c := Column{Name: name, ID: id}
	field := func(c Column) Field { return Field{Name: c.Name, ID: c.ID, Nested: c.Nested} }
	switch t := t.(type) {
	case *iceberg.ListType:
		e, ok := icebergColumn("element", t.ElementID, t.Element)
		if !ok {
			return c, false
		}
		c.Type = e.Type + "[]"
		c.Nested = &Nesting{Kind: "list", Fields: []Field{field(e)}}
	case *iceberg.MapType:
		k, ok := icebergColumn("key", t.KeyID, t.KeyType)
		v, vok := icebergColumn("value", t.ValueID, t.ValueType)
		if !ok || !vok {
			return c, false
		}
		c.Type = "MAP(" + k.Type + ", " + v.Type + ")"
		c.Nested = &Nesting{Kind: "map", Fields: []Field{field(k), field(v)}}
	case *iceberg.StructType:
		if len(t.FieldList) == 0 {
			return c, false
		}
		c.Nested = &Nesting{Kind: "struct"}
		parts := make([]string, len(t.FieldList))
		for i, f := range t.FieldList {
			fc, ok := icebergColumn(f.Name, f.ID, f.Type)
			if !ok {
				return c, false
			}
			parts[i] = quoteIdent(f.Name) + " " + fc.Type
			c.Nested.Fields = append(c.Nested.Fields, field(fc))
		}
		c.Type = "STRUCT(" + strings.Join(parts, ", ") + ")"
	default:
		typ, ok := duckType(t)
		if !ok {
			return c, false
		}
		c.Type = typ
	}
	return c, true
}

// initialDefault returns the dotted path of one of fields, or of a field
// nested in them, that has an initial default.
func initialDefault(fields []iceberg.NestedField) (string, bool) {
	for _, f := range fields {
		if f.InitialDefault != nil {
			return f.Name, true
		}
		if nested, ok := f.Type.(iceberg.NestedType); ok {
			if name, ok := initialDefault(nested.Fields()); ok {
				return f.Name + "." + name, true
			}
		}
	}
	return "", false
}

// duckType returns the DuckDB type DuckDB reads an Iceberg primitive type
// as.
func duckType(t iceberg.Type) (string, bool) {
	switch t := t.(type) {
	case iceberg.BooleanType:
		return "BOOLEAN", true
	case iceberg.Int32Type:
		return "INTEGER", true
	case iceberg.Int64Type:
		return "BIGINT", true
	case iceberg.Float32Type:
		return "FLOAT", true
	case iceberg.Float64Type:
		return "DOUBLE", true
	case iceberg.DecimalType:
		return fmt.Sprintf("DECIMAL(%d,%d)", t.Precision(), t.Scale()), true
	case iceberg.DateType:
		return "DATE", true
	case iceberg.TimeType:
		return "TIME", true
	case iceberg.TimestampType:
		return "TIMESTAMP", true
	case iceberg.TimestampTzType:
		return "TIMESTAMP WITH TIME ZONE", true
	case iceberg.StringType:
		return "VARCHAR", true
	case iceberg.UUIDType:
		return "UUID", true
	case iceberg.BinaryType, iceberg.FixedType:
		return "BLOB", true
	}
	return "", false
}
