package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// Source finds the data files of catalog tables.
type Source interface {
	// Table returns schema.name, with its columns named and typed as DuckDB
	// reads them. Workers cannot read it if Unsupported is set. Its snapshot
	// may be a few seconds old: it decides only whether to distribute.
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
	order []string
	// cached counts the files of the snapshots kept.
	cached int
	// tables are recently loaded tables, for deciding.
	tables map[string]loadedTable
}

type loadedTable struct {
	t  *Table
	at time.Time
}

// tableTTL is how long a loaded table's metadata decides whether a
// statement is distributed.
const tableTTL = 10 * time.Second

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
	return &Iceberg{cat: cat, files: map[string][]File{}, tables: map[string]loadedTable{}}
}

type icebergHandle struct {
	tbl  *table.Table
	snap *table.Snapshot
	// columns are the table's columns as loaded, which Refresh compares:
	// Coordinator.Table prints nested types afresh in the Table's own.
	columns []Column
}

// Table returns a table's metadata, loaded at most tableTTL ago.
func (s *Iceberg) Table(ctx context.Context, schema, name string) (*Table, error) {
	key := schema + "\x00" + name
	s.mu.Lock()
	lt, ok := s.tables[key]
	s.mu.Unlock()
	if ok && time.Since(lt.at) < tableTTL {
		c := *lt.t
		c.Columns = slices.Clone(lt.t.Columns)
		return &c, nil
	}
	t, err := s.load(ctx, schema, name)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if len(s.tables) > 4096 {
		clear(s.tables)
	}
	s.tables[key] = loadedTable{t: t, at: time.Now()}
	s.mu.Unlock()
	c := *t
	c.Columns = slices.Clone(t.Columns)
	return &c, nil
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
	s.mu.Unlock()
	if ok {
		return files, nil
	}

	tasks, err := h.tbl.Scan(table.WithSnapshotID(h.snap.SnapshotID)).PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("plan scan of %s: %w", t, err)
	}
	files = make([]File, 0, len(tasks))
	for _, task := range tasks {
		if f := strings.ToUpper(string(task.File.FileFormat())); f != "PARQUET" {
			return nil, unsupported("%s has %s files", t, f)
		}
		f := File{Path: task.File.FilePath(), Size: task.File.FileSizeBytes(), Rows: task.File.Count()}
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

	s.mu.Lock()
	if _, ok := s.files[key]; !ok {
		s.files[key] = files
		s.order = append(s.order, key)
		s.cached += len(files)
		for len(s.order) > maxCachedSnapshots || s.cached > maxCachedFiles && len(s.order) > 1 {
			s.cached -= len(s.files[s.order[0]])
			delete(s.files, s.order[0])
			s.order = s.order[1:]
		}
	}
	s.mu.Unlock()
	return files, nil
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
