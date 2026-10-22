package distributed

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
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
	// table's columns changed since.
	Refresh(ctx context.Context, t *Table) error
	// Files lists the data files of t's snapshot.
	Files(ctx context.Context, t *Table) ([]File, error)
	// RemoveAll deletes the files under dir, with the storage access t is
	// read with.
	RemoveAll(ctx context.Context, t *Table, dir string) error
}

// File is a data file.
type File struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
	// Rows is the file's record count.
	Rows int64 `json:"rows"`
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

// maxCachedSnapshots bounds the snapshots whose file lists are kept.
const maxCachedSnapshots = 256

// NewIceberg reads tables through cat.
func NewIceberg(cat *catalog.Client) *Iceberg {
	return &Iceberg{cat: cat, files: map[string][]File{}, tables: map[string]loadedTable{}}
}

type icebergHandle struct {
	tbl  *table.Table
	snap *table.Snapshot
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

// Refresh loads the table's current metadata into t.
func (s *Iceberg) Refresh(ctx context.Context, t *Table) error {
	fresh, err := s.load(ctx, t.Schema, t.Name)
	if err != nil {
		return err
	}
	if len(fresh.Columns) != len(t.Columns) {
		return unsupported("the columns of %s changed", t)
	}
	for i, c := range fresh.Columns {
		if c.ID != t.Columns[i].ID || !strings.EqualFold(c.Name, t.Columns[i].Name) {
			return unsupported("the columns of %s changed", t)
		}
	}
	if fresh.Unsupported != "" {
		return unsupported("table %s: %s", t, fresh.Unsupported)
	}
	t.Bytes, t.Snapshot, t.Versioned, t.source = fresh.Bytes, fresh.Snapshot, fresh.Versioned, fresh.source
	return nil
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
	// A name mapping (schema.name-mapping.default) applies only to files
	// without field IDs, which workers refuse; iceberg-go adds one to every
	// table it compacts, whose files all have field IDs.
	snap := tbl.CurrentSnapshot()
	t.source = &icebergHandle{tbl: tbl, snap: snap}
	if snap == nil {
		return t, nil
	}
	t.Snapshot = snap.SnapshotID
	var props iceberg.Properties
	if snap.Summary != nil {
		props = snap.Summary.Properties
	}
	for _, k := range []string{"total-delete-files", "total-position-deletes", "total-equality-deletes"} {
		if n, _ := strconv.ParseInt(props[k], 10, 64); n > 0 && t.Unsupported == "" {
			t.Unsupported = "it has delete files; table maintenance merges them"
		}
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
		if len(task.DeleteFiles)+len(task.EqualityDeleteFiles)+len(task.DeletionVectorFiles) > 0 {
			return nil, unsupported("%s has delete files", t)
		}
		if f := strings.ToUpper(string(task.File.FileFormat())); f != "PARQUET" {
			return nil, unsupported("%s has %s files", t, f)
		}
		files = append(files, File{Path: task.File.FilePath(), Size: task.File.FileSizeBytes(), Rows: task.File.Count()})
	}

	s.mu.Lock()
	if _, ok := s.files[key]; !ok {
		s.files[key] = files
		s.order = append(s.order, key)
		if len(s.order) > maxCachedSnapshots {
			delete(s.files, s.order[0])
			s.order = s.order[1:]
		}
	}
	s.mu.Unlock()
	return files, nil
}

// RemoveAll deletes the files under dir.
func (s *Iceberg) RemoveAll(ctx context.Context, t *Table, dir string) error {
	h, ok := t.source.(*icebergHandle)
	if !ok {
		return fmt.Errorf("table %s was not loaded from the catalog", t)
	}
	fs, err := h.tbl.FS(ctx)
	if err != nil {
		return err
	}
	lister, ok := fs.(icebergio.ListableIO)
	if !ok {
		return errors.New("the warehouse's storage cannot list files")
	}
	var files []string
	err = lister.WalkDir(strings.TrimSuffix(dir, "/")+"/", func(path string, d iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil && !errors.Is(err, iofs.ErrNotExist) {
		return err
	}
	if bulk, ok := fs.(icebergio.BulkRemovableIO); ok && len(files) > 0 {
		_, err := bulk.DeleteFiles(ctx, files)
		return err
	}
	var errs []error
	for _, f := range files {
		errs = append(errs, fs.Remove(f))
	}
	return errors.Join(errs...)
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
