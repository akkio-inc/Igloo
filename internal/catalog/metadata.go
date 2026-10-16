package catalog

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"golang.org/x/sync/errgroup"
)

// Metadata is what the catalog records about its schemas, tables and
// views: their comments and columns, and for tables how big they are and
// when their data last changed. It is read from the Iceberg metadata, so
// it covers tables that other engines wrote too.
type Metadata struct {
	Schemas []SchemaMeta
	// Tables are the tables and views.
	Tables []TableMeta
}

// SchemaMeta is a schema (namespace).
type SchemaMeta struct {
	Name    string
	Comment string
}

// TableMeta is a table or view.
type TableMeta struct {
	Schema, Name string
	View         bool
	Comment      string
	Columns      []ColumnMeta
	// LastModified is when the data last changed, the current snapshot's
	// time, or for a view when it was last replaced. Zero for a table
	// without data.
	LastModified time.Time
	// Snapshots counts a table's snapshots, which time travel can read.
	Snapshots int
	// Rows, Bytes and Files describe a table's current snapshot, from its
	// summary; -1 when it does not say.
	Rows, Bytes, Files int64
}

// ColumnMeta is a column of a table or view.
type ColumnMeta struct {
	Name string
	// Type is the DuckDB type.
	Type    string
	Comment string
}

// Comments lists the comments of every schema, table, view and column.
func (m *Metadata) Comments() []Comment {
	if m == nil {
		return nil
	}
	var out []Comment
	for _, s := range m.Schemas {
		if s.Comment != "" {
			out = append(out, Comment{Schema: s.Name, Text: s.Comment})
		}
	}
	for _, t := range m.Tables {
		if t.Comment != "" {
			out = append(out, Comment{Schema: t.Schema, Name: t.Name, Text: t.Comment, View: t.View})
		}
		for _, c := range t.Columns {
			if c.Comment != "" {
				out = append(out, Comment{Schema: t.Schema, Name: t.Name, Column: c.Name, Text: c.Comment, View: t.View})
			}
		}
	}
	return out
}

// Table returns the table or view schema.name, matching names as DuckDB
// does, without regard to case.
func (m *Metadata) Table(schema, name string) (TableMeta, bool) {
	if m == nil {
		return TableMeta{}, false
	}
	for _, t := range m.Tables {
		if strings.EqualFold(t.Schema, schema) && strings.EqualFold(t.Name, name) {
			return t, true
		}
	}
	return TableMeta{}, false
}

// WithTable returns a copy of m with the table or view schema.name
// replaced by t, or removed when t is nil.
func (m *Metadata) WithTable(schema, name string, t *TableMeta) *Metadata {
	out := &Metadata{}
	if m != nil {
		out.Schemas = m.Schemas
		for _, old := range m.Tables {
			if !strings.EqualFold(old.Schema, schema) || !strings.EqualFold(old.Name, name) {
				out.Tables = append(out.Tables, old)
			}
		}
	}
	if t != nil {
		out.Tables = append(out.Tables, *t)
	}
	return out
}

// WithSchema returns a copy of m with s in place of the schema of its
// name, or with the schema called name removed when s is nil.
func (m *Metadata) WithSchema(name string, s *SchemaMeta) *Metadata {
	out := &Metadata{}
	if m != nil {
		out.Tables = m.Tables
		for _, old := range m.Schemas {
			if !strings.EqualFold(old.Name, name) {
				out.Schemas = append(out.Schemas, old)
			}
		}
	}
	if s != nil {
		out.Schemas = append(out.Schemas, *s)
	}
	return out
}

// Metadata reads every schema, table and view of the catalog, loading each
// one's metadata: a request per object.
func (c *Client) Metadata(ctx context.Context) (*Metadata, error) {
	namespaces, err := c.Namespaces(ctx)
	if err != nil {
		return nil, err
	}
	type object struct {
		ident table.Identifier
		view  bool
	}
	var objects []object
	for _, ns := range namespaces {
		for ident, err := range c.ListTables(ctx, ns) {
			if err != nil {
				return nil, err
			}
			objects = append(objects, object{ident: ident})
		}
		for ident, err := range c.ListViews(ctx, ns) {
			if err != nil {
				return nil, err
			}
			objects = append(objects, object{ident: ident, view: true})
		}
	}

	var (
		mu  sync.Mutex
		out = &Metadata{}
	)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, ns := range namespaces {
		g.Go(func() error {
			s, err := c.SchemaMetadata(ctx, strings.Join(ns, "."))
			if errors.Is(err, icecat.ErrNoSuchNamespace) {
				return nil
			}
			if err != nil {
				return err
			}
			mu.Lock()
			out.Schemas = append(out.Schemas, s)
			mu.Unlock()
			return nil
		})
	}
	for _, o := range objects {
		g.Go(func() error {
			schema, name := splitIdent(o.ident)
			t, err := c.TableMetadata(ctx, schema, name, o.view)
			if err != nil {
				return ignoreDropped(err)
			}
			mu.Lock()
			out.Tables = append(out.Tables, t)
			mu.Unlock()
			return nil
		})
	}
	return out, g.Wait()
}

// SchemaMetadata reads one schema's metadata.
func (c *Client) SchemaMetadata(ctx context.Context, schema string) (SchemaMeta, error) {
	props, err := c.LoadNamespaceProperties(ctx, strings.Split(schema, "."))
	if err != nil {
		return SchemaMeta{}, err
	}
	return SchemaMeta{Name: schema, Comment: props[commentProperty]}, nil
}

// TableMetadata reads the metadata of the table, or with isView the view,
// schema.name.
func (c *Client) TableMetadata(ctx context.Context, schema, name string, isView bool) (TableMeta, error) {
	t := TableMeta{Schema: schema, Name: name, View: isView, Rows: -1, Bytes: -1, Files: -1}
	if isView {
		v, err := c.LoadView(ctx, Ident(schema, name))
		if err != nil {
			return t, err
		}
		describeView(&t, v)
		return t, nil
	}
	tbl, err := c.LoadTable(ctx, Ident(schema, name))
	if err != nil {
		return t, err
	}
	t.Comment = tbl.Properties()[commentProperty]
	for _, f := range tbl.Schema().Fields() {
		t.Columns = append(t.Columns, ColumnMeta{Name: f.Name, Type: duckDBType(f.Type), Comment: f.Doc})
	}
	t.Snapshots = len(tbl.Metadata().Snapshots())
	if snap := tbl.CurrentSnapshot(); snap != nil {
		t.LastModified = time.UnixMilli(snap.TimestampMs)
		if snap.Summary != nil {
			t.Rows = summaryCount(snap.Summary.Properties["total-records"])
			t.Bytes = summaryCount(snap.Summary.Properties["total-files-size"])
			t.Files = summaryCount(snap.Summary.Properties["total-data-files"])
		}
		if t.Bytes < 0 {
			t.Bytes = c.dataBytes(ctx, tbl, snap)
		}
	}
	return t, nil
}

// maxSizes bounds the snapshots whose sizes a Client keeps.
const maxSizes = 100_000

// sizeCache keeps what dataBytes worked out.
type sizeCache struct {
	mu    sync.Mutex
	bytes map[string]int64
}

// dataBytes adds up the sizes of the data files of a table's snapshot
// from its manifests, for snapshots whose summary does not say, as
// DuckDB's do not; -1 when they cannot be read. The result is kept by
// the snapshot's manifest list, which no other snapshot shares, so each
// snapshot's manifests are read once.
func (c *Client) dataBytes(ctx context.Context, tbl *table.Table, snap *table.Snapshot) int64 {
	c.sizes.mu.Lock()
	n, ok := c.sizes.bytes[snap.ManifestList]
	c.sizes.mu.Unlock()
	if ok {
		return n
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return -1
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return -1
	}
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		entries, err := m.FetchEntries(fs, true)
		if err != nil {
			return -1
		}
		for _, e := range entries {
			n += e.DataFile().FileSizeBytes()
		}
	}
	c.sizes.mu.Lock()
	defer c.sizes.mu.Unlock()
	if c.sizes.bytes == nil || len(c.sizes.bytes) >= maxSizes {
		c.sizes.bytes = map[string]int64{}
	}
	c.sizes.bytes[snap.ManifestList] = n
	return n
}

func describeView(t *TableMeta, v *view.View) {
	t.Comment = v.Metadata().Properties()[commentProperty]
	if version := v.CurrentVersion(); version != nil {
		t.LastModified = time.UnixMilli(version.TimestampMS)
	}
	if sc := v.CurrentSchema(); sc != nil {
		for _, f := range sc.Fields() {
			t.Columns = append(t.Columns, ColumnMeta{Name: f.Name, Type: duckDBType(f.Type), Comment: f.Doc})
		}
	}
}

// summaryCount reads a count from a snapshot summary, or -1.
func summaryCount(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}
