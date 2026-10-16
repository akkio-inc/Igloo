// Package clonetest is an Iceberg catalog in memory, with tables on the
// local disk, and helpers to fill and read them, for testing clones and
// table maintenance without a catalog server.
package clonetest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// Catalog keeps tables in memory and their files under Dir. It implements
// clone.Catalog, maintenance.Catalog and table.CatalogIO.
type Catalog struct {
	// Dir is the warehouse: every table's location is a directory in it.
	Dir string

	mu     sync.Mutex
	tables map[string]entry
}

type entry struct {
	meta    table.Metadata
	metaLoc string
}

// New makes an empty catalog in dir.
func New(dir string) *Catalog {
	return &Catalog{Dir: dir, tables: map[string]entry{}}
}

func key(ident table.Identifier) string { return strings.Join(ident, ".") }

func fsys(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }

// Create makes an empty format version 2 table called ident.
func (c *Catalog) Create(ident table.Identifier, schema *iceberg.Schema, props iceberg.Properties) (*table.Table, error) {
	loc := filepath.Join(c.Dir, uuid.NewString())
	if err := os.MkdirAll(loc, 0o755); err != nil {
		return nil, err
	}
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, loc, props)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tables[key(ident)]; ok {
		return nil, icecat.ErrTableAlreadyExists
	}
	metaLoc := loc + "/metadata/00000.metadata.json"
	c.tables[key(ident)] = entry{meta, metaLoc}
	return table.New(ident, meta, metaLoc, fsys, c), nil
}

// Drop removes a table from the catalog and leaves its files, as dropping
// a table without purging it does.
func (c *Catalog) Drop(ident table.Identifier) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.tables, key(ident))
}

// Rename renames a table.
func (c *Catalog) Rename(from, to table.Identifier) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables[key(to)] = c.tables[key(from)]
	delete(c.tables, key(from))
}

func (c *Catalog) Tables(context.Context) ([]table.Identifier, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []table.Identifier
	for k := range c.tables {
		out = append(out, strings.SplitN(k, ".", 2))
	}
	slices.SortFunc(out, func(a, b table.Identifier) int { return strings.Compare(key(a), key(b)) })
	return out, nil
}

func (c *Catalog) LoadTable(_ context.Context, ident table.Identifier) (*table.Table, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.tables[key(ident)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", icecat.ErrNoSuchTable, key(ident))
	}
	return table.New(ident, e.meta, e.metaLoc, fsys, c), nil
}

func (c *Catalog) CommitTable(_ context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.tables[key(ident)]
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", icecat.ErrNoSuchTable, key(ident))
	}
	for _, r := range reqs {
		if err := r.Validate(e.meta); err != nil {
			return nil, "", fmt.Errorf("%w: %v", table.ErrCommitFailed, err)
		}
	}
	meta, err := table.UpdateTableMetadata(e.meta, updates, e.metaLoc)
	if err != nil {
		return nil, "", err
	}
	metaLoc := meta.Location() + "/metadata/" + uuid.NewString() + ".metadata.json"
	c.tables[key(ident)] = entry{meta, metaLoc}
	return meta, metaLoc, nil
}

func (c *Catalog) Register(_ context.Context, ident table.Identifier, metadataLoc string, replace bool) (*table.Table, error) {
	f, err := os.Open(metadataLoc)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck
	meta, err := table.ParseMetadata(f)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tables[key(ident)]; ok && !replace {
		return nil, icecat.ErrTableAlreadyExists
	}
	c.tables[key(ident)] = entry{meta, metadataLoc}
	return table.New(ident, meta, metadataLoc, fsys, c), nil
}

func (c *Catalog) NamespaceLocation(context.Context, string) (string, error) { return c.Dir, nil }

// Load loads schema.name.
func (c *Catalog) Load(t testing.TB, schema, name string) *table.Table {
	t.Helper()
	tbl, err := c.LoadTable(context.Background(), table.Identifier{schema, name})
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

// Schema is a table of ids and emails.
var Schema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 2, Name: "email", Type: iceberg.PrimitiveTypes.String},
)

// Append adds one data file with rows of the given ids to a table of
// Schema.
func Append(t testing.TB, tbl *table.Table, ids ...int64) *table.Table {
	t.Helper()
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	for _, id := range ids {
		b.Field(0).(*array.Int64Builder).Append(id)
		b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("user%d@example.com", id))
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	at := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
	defer at.Release()
	out, err := tbl.AppendTable(context.Background(), at, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// IDs reads the ids of a table of Schema, sorted.
func IDs(t testing.TB, tbl *table.Table, opts ...table.ScanOption) []int64 {
	t.Helper()
	res, err := tbl.Scan(opts...).ToArrowTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer res.Release()
	var out []int64
	for _, chunk := range res.Column(0).Data().Chunks() {
		out = append(out, chunk.(*array.Int64).Int64Values()...)
	}
	slices.Sort(out)
	return out
}

// Files lists the data and delete files the table's current snapshot
// reads.
func Files(t testing.TB, tbl *table.Table) []string {
	t.Helper()
	var out []string
	if tbl.CurrentSnapshot() == nil {
		return out
	}
	fs, err := tbl.FS(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	manifests, err := tbl.CurrentSnapshot().Manifests(fs)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range manifests {
		for e, err := range m.Entries(fs, true) {
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, e.DataFile().FilePath())
		}
	}
	slices.Sort(out)
	return out
}
