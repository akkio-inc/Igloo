package distributed

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// Source finds the data files of catalog tables.
type Source interface {
	// Table returns schema.name at its current snapshot, with its columns
	// named and typed as DuckDB reads them. Workers cannot read it if
	// Unsupported is set.
	Table(ctx context.Context, schema, name string) (*Table, error)
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
}

// maxCachedSnapshots bounds the snapshots whose file lists are kept.
const maxCachedSnapshots = 256

// NewIceberg reads tables through cat.
func NewIceberg(cat *catalog.Client) *Iceberg {
	return &Iceberg{cat: cat, files: map[string][]File{}}
}

type icebergHandle struct {
	tbl  *table.Table
	snap *table.Snapshot
}

// Table loads a table's current metadata.
func (s *Iceberg) Table(ctx context.Context, schema, name string) (*Table, error) {
	tbl, err := s.cat.LoadTable(ctx, catalog.Ident(schema, name))
	if err != nil {
		return nil, fmt.Errorf("load %s.%s: %w", schema, name, err)
	}
	t := &Table{Schema: schema, Name: name}
	for _, f := range tbl.Schema().Fields() {
		typ, ok := duckType(f.Type)
		if !ok && t.Unsupported == "" {
			t.Unsupported = fmt.Sprintf("column %s has type %s", f.Name, f.Type)
		}
		t.Columns = append(t.Columns, Column{Name: f.Name, Type: typ, ID: f.ID})
	}
	if _, ok := tbl.Properties()["schema.name-mapping.default"]; ok && t.Unsupported == "" {
		// Files without field IDs, read by name: workers read by ID.
		t.Unsupported = "it maps columns by name"
	}
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
		files = append(files, File{Path: task.File.FilePath(), Size: task.File.FileSizeBytes()})
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

// duckType returns the DuckDB type DuckDB reads an Iceberg type as.
// Nested types are left to one machine for now.
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
