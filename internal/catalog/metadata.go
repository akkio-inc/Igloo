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
	// Catalog is the connection (package federation) that a table of
	// another database is in, and Provider the Delta Sharing provider of an
	// external table; both are empty for the catalog's own tables and
	// views. Igloo records no comments, sizes or freshness for either.
	Catalog  string
	Provider string
	Comment  string
	Columns  []ColumnMeta
	// LastModified is when the data last changed, the current snapshot's
	// time, or for a view when it was last replaced. Zero for a table
	// without data.
	LastModified time.Time
	// Snapshots counts a table's snapshots, which time travel can read.
	Snapshots int
	// Rows, Bytes and Files describe a table's current snapshot, from its
	// summary or else its manifests; -1 when they do not say. Rows are the
	// rows a query reads: deleted rows are not counted (see liveRows).
	Rows, Bytes, Files int64
	// ClusteringKey is the table's clustering key as Snowflake shows one,
	// or "" (see ClusteringKey).
	ClusteringKey string
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
	t.ClusteringKey = ClusteringKey(tbl.Metadata())
	for _, f := range tbl.Schema().Fields() {
		t.Columns = append(t.Columns, ColumnMeta{Name: f.Name, Type: duckDBType(f.Type), Comment: f.Doc})
	}
	t.Snapshots = len(tbl.Metadata().Snapshots())
	if snap := tbl.CurrentSnapshot(); snap != nil {
		t.LastModified = time.UnixMilli(snap.TimestampMs)
		var summary iceberg.Properties
		if snap.Summary != nil {
			summary = snap.Summary.Properties
		}
		t.Rows = liveRows(summary)
		t.Bytes = summaryCount(summary["total-files-size"])
		t.Files = summaryCount(summary["total-data-files"])
		if t.Bytes < 0 || t.Rows < 0 && summaryCount(summary["total-equality-deletes"]) <= 0 {
			totals := c.manifestTotals(ctx, tbl, snap)
			if t.Bytes < 0 {
				t.Bytes = totals.bytes
			}
			if t.Rows < 0 {
				t.Rows = totals.rows
			}
		}
	}
	return t, nil
}

// liveRows is how many rows a snapshot's summary says its table has: the
// records of its data files less the rows its position deletes delete.
// UPDATE, DELETE and MERGE that do not rewrite files (merge-on-read, which
// DuckDB always does) add position deletes and leave the deleted rows in
// total-records: a materialized view refresh, which deletes every row and
// inserts the new ones, doubles it. -1 when the summary cannot say: when
// it has delete files but does not count both kinds of deletes, as
// DuckDB's do not count equality deletes (manifestTotals counts those
// tables' rows), or when the table has equality deletes, whose rows only
// a scan can count.
//
// Position deletes of a data file that a rewrite removed, which some
// engines leave behind, are still subtracted until maintenance removes
// them.
func liveRows(summary iceberg.Properties) int64 {
	records := summaryCount(summary["total-records"])
	if records < 0 {
		return -1
	}
	if summaryCount(summary["total-delete-files"]) == 0 {
		return records
	}
	positions := summaryCount(summary["total-position-deletes"])
	if positions < 0 || summaryCount(summary["total-equality-deletes"]) != 0 {
		return -1
	}
	return max(records-positions, 0)
}

// maxTotals bounds the snapshots whose totals a Client keeps.
const maxTotals = 100_000

// totalsCache keeps what manifestTotals worked out.
type totalsCache struct {
	mu     sync.Mutex
	totals map[string]snapshotTotals
}

// snapshotTotals are the bytes of a snapshot's data files, and the rows
// they hold less those its position deletes delete; -1 for what is not
// known.
type snapshotTotals struct {
	bytes, rows int64
}

// manifestTotals works out a table's snapshot's totals from its
// manifests, for snapshots whose summary does not say: DuckDB's give no
// sizes, and other engines' may not count deletes. Rows are -1 when the
// snapshot has equality deletes (see liveRows), and both are -1 when the
// manifests cannot be read. The result is kept by the snapshot's manifest
// list, which no other snapshot shares, so each snapshot's manifests are
// read once.
func (c *Client) manifestTotals(ctx context.Context, tbl *table.Table, snap *table.Snapshot) snapshotTotals {
	c.totals.mu.Lock()
	out, ok := c.totals.totals[snap.ManifestList]
	c.totals.mu.Unlock()
	if ok {
		return out
	}
	unknown := snapshotTotals{bytes: -1, rows: -1}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return unknown
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return unknown
	}
	equalityDeletes := false
	for _, m := range manifests {
		entries, err := m.FetchEntries(fs, true)
		if err != nil {
			return unknown
		}
		for _, e := range entries {
			f := e.DataFile()
			switch f.ContentType() {
			case iceberg.EntryContentData:
				out.bytes += f.FileSizeBytes()
				out.rows += f.Count()
			case iceberg.EntryContentPosDeletes:
				out.rows -= f.Count()
			case iceberg.EntryContentEqDeletes:
				equalityDeletes = true
			}
		}
	}
	out.rows = max(out.rows, 0)
	if equalityDeletes {
		out.rows = -1
	}
	c.totals.mu.Lock()
	defer c.totals.mu.Unlock()
	if c.totals.totals == nil || len(c.totals.totals) >= maxTotals {
		c.totals.totals = map[string]snapshotTotals{}
	}
	c.totals.totals[snap.ManifestList] = out
	return out
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
