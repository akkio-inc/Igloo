package engine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
)

// The catalog functions list what the catalog records about its schemas,
// tables and views (comments, columns, owners, sizes and freshness), its
// lineage and how much each table is used. DuckDB reads none of it from
// Iceberg metadata, so Igloo serves it itself, and each function shows
// only what the user running it may see, as its CatalogSource decides.

// CatalogSource is what one statement may show of the catalog's metadata,
// limited already to what its user may see.
type CatalogSource interface {
	// Catalog is the catalog's name in SQL.
	Catalog() string
	// Metadata is the schemas, tables and views, with their comments,
	// columns and sizes, and the external tables and connections' tables.
	Metadata() *catalog.Metadata
	// Owners are the owners recorded.
	Owners() []auth.Ownership
	// Lineage is the lineage's edges. An end the user may not see has an
	// empty name, and the statement that made an edge is left out unless
	// the user may see it in the query history.
	Lineage(ctx context.Context) ([]audit.Edge, error)
	// TableUsage is how often tables were read and written lately.
	TableUsage(ctx context.Context) ([]audit.TableUsage, error)
	// SeesTable reports whether the user may see the table or view
	// schema.name of the catalog, and SeesConnectionTable whether they may
	// see the table schema.name of the connection catalog.
	SeesTable(schema, name string) bool
	SeesConnectionTable(catalog, schema, name string) bool
}

// CatalogViewer works out the CatalogSource of a statement prepared with
// ctx. The function it returns is called when the statement runs, and
// reports false when there is nobody to show anything to, as for
// statements from outside a session.
type CatalogViewer func(ctx context.Context) func() (CatalogSource, bool)

// SetCatalogViewer makes the catalog functions list what viewer works out
// for each statement. Until it is set, they are empty.
func (e *Engine) SetCatalogViewer(viewer CatalogViewer) {
	e.catalogViewer.Store(&viewer)
}

// catalogSource works out the source of a statement prepared with ctx.
func (e *Engine) catalogSource(ctx context.Context) func() (CatalogSource, bool) {
	if v := e.catalogViewer.Load(); v != nil && *v != nil {
		if f := (*v)(ctx); f != nil {
			return f
		}
	}
	return func() (CatalogSource, bool) { return nil, false }
}

// catalogViews are the views over the catalog functions, in the local
// database's schema HistorySchema, so they read as igloo.tables and so on.
var catalogViews = map[string]string{
	"schemata":    "igloo_schemata",
	"tables":      "igloo_tables",
	"columns":     "igloo_columns",
	"lineage":     "igloo_lineage",
	"table_usage": "igloo_table_usage",
}

// InformationSchemaView names the view that stands in for
// information_schema's view, schemata, tables, functions or procedures, in
// HistorySchema.
func InformationSchemaView(name string) string { return "information_schema_" + name }

// CatalogViews names the views registerCatalog defines in HistorySchema.
func CatalogViews() []string {
	names := make([]string, 0, len(catalogViews))
	for v := range catalogViews {
		names = append(names, v)
	}
	return names
}

// registerCatalog defines igloo_comments(), on which pg_description and the
// obj_description and col_description functions are built, and the
// catalog functions with their views:
//
//   - igloo.schemata: each schema's owner and comment
//   - igloo.tables: each table's and view's owner, comment, size, when
//     its data last changed and its clustering key (its default sort
//     order); also external tables, with their provider, and connections'
//     tables
//   - igloo.columns: each column with its DuckDB type and comment
//   - igloo.lineage: where tables' data came from, from the query history
//   - igloo.table_usage: how often tables were read and written in the last
//     30 days, from the query history
//
// The functions take no arguments: duckdb-go aborts the process when a
// table function is passed NULL.
func (e *Engine) registerCatalog(ctx context.Context) error {
	conn, release, err := e.sqlConn(ctx)
	if err != nil {
		return err
	}
	defer release()

	t, err := newHistoryTypes()
	if err != nil {
		return err
	}
	integer, err := duckdb.NewTypeInfo(duckdb.TYPE_INTEGER)
	if err != nil {
		return err
	}
	col := func(name string, typ duckdb.TypeInfo) duckdb.ColumnInfo { return duckdb.ColumnInfo{Name: name, T: typ} }
	varchars := func(names ...string) []duckdb.ColumnInfo {
		out := make([]duckdb.ColumnInfo, len(names))
		for i, n := range names {
			out[i] = col(n, t.varchar)
		}
		return out
	}
	functions := map[string]struct {
		columns []duckdb.ColumnInfo
		rows    func(context.Context, CatalogSource) ([][]any, error)
	}{
		"igloo_comments": {
			columns: varchars("table_catalog", "table_schema", "table_name", "column_name", "comment"),
			rows:    e.commentRows,
		},
		"igloo_schemata": {
			columns: varchars("catalog_name", "schema_name", "schema_owner", "owner_type", "comment"),
			rows:    e.schemaRows,
		},
		"igloo_tables": {
			columns: append(varchars("table_catalog", "table_schema", "table_name", "table_type", "table_owner", "owner_type", "comment",
				"provider"),
				col("row_count", t.bigint), col("bytes", t.bigint), col("file_count", t.bigint), col("snapshot_count", t.bigint),
				col("last_altered", t.timestamp), col("clustering_key", t.varchar)),
			rows: e.tableRows,
		},
		"igloo_columns": {
			columns: append(varchars("table_catalog", "table_schema", "table_name", "column_name"),
				col("ordinal_position", integer), col("data_type", t.varchar), col("comment", t.varchar)),
			rows: e.columnRows,
		},
		"igloo_lineage": {
			columns: append(varchars("upstream_type", "upstream_database", "upstream_schema", "upstream_name",
				"downstream_database", "downstream_schema", "downstream_name"),
				col("first_seen", t.timestamp), col("last_seen", t.timestamp), col("statements", t.bigint),
				col("last_query_id", t.varchar), col("last_user", t.varchar), col("last_statement_type", t.varchar),
				col("last_job", t.varchar)),
			rows: e.lineageRows,
		},
		"igloo_table_usage": {
			columns: append(varchars("table_catalog", "table_schema", "table_name"),
				col("reads_30d", t.bigint), col("writes_30d", t.bigint), col("users_30d", t.bigint),
				col("last_read", t.timestamp), col("last_written", t.timestamp)),
			rows: e.usageRows,
		},
	}
	for name, f := range functions {
		err := duckdb.RegisterTableUDF(conn, name, duckdb.RowTableFunction{
			BindArgumentsContext: func(ctx context.Context, _ map[string]any, _ ...any) (duckdb.RowTableSource, error) {
				source := e.catalogSource(ctx)
				return &lazySource{columns: f.columns, fill: func() ([][]any, error) {
					src, ok := source()
					if !ok {
						return nil, nil
					}
					return f.rows(context.Background(), src)
				}}, nil
			},
		})
		if err != nil {
			return err
		}
	}

	schema := ident(e.local) + "." + ident(HistorySchema)
	stmts := []string{"CREATE SCHEMA IF NOT EXISTS " + schema}
	for view, fn := range catalogViews {
		stmts = append(stmts, "CREATE OR REPLACE VIEW "+schema+"."+ident(view)+" AS SELECT * FROM system.main."+fn+"()")
	}
	// information_schema.schemata and information_schema.tables with the
	// owners, comments and sizes DuckDB does not know of, and the columns
	// Snowflake has for them; queries of those views read these instead.
	// The catalog's views are listed under the local database. The catalog's
	// comment comes first: a comment DuckDB holds can be one replaced since.
	stmts = append(stmts,
		"CREATE OR REPLACE VIEW "+schema+"."+ident(InformationSchemaView("schemata"))+` AS
			SELECT s.* REPLACE (coalesce(o.schema_owner, s.schema_owner) AS schema_owner), o.comment
			FROM system.information_schema.schemata s
			LEFT JOIN system.main.igloo_schemata() o ON o.catalog_name = s.catalog_name AND o.schema_name = s.schema_name`,
		"CREATE OR REPLACE VIEW "+schema+"."+ident(InformationSchemaView("tables"))+` AS
			SELECT t.* REPLACE (coalesce(o.comment, t.TABLE_COMMENT) AS TABLE_COMMENT),
				o.table_owner, o.comment, o.row_count, o.bytes, o.last_altered, o.clustering_key,
				'NO' AS is_transient, CASE WHEN t.table_type = 'LOCAL TEMPORARY' THEN 'YES' ELSE 'NO' END AS is_temporary
			FROM system.information_schema.tables t
			LEFT JOIN system.main.igloo_tables() o ON o.table_schema = t.table_schema AND o.table_name = t.table_name
				AND (o.table_catalog = t.table_catalog OR t.table_type = 'VIEW' AND t.table_catalog = `+literal(e.local)+`)`,
		// Snowflake's information_schema.functions, of the macros admins
		// create (not the session's own, such as the Snowflake dialect's),
		// and information_schema.procedures, which Igloo has none of.
		"CREATE OR REPLACE VIEW "+schema+"."+ident(InformationSchemaView("functions"))+` AS
			SELECT database_name AS function_catalog, schema_name AS function_schema, function_name,
				NULL::VARCHAR AS function_owner, '(' || array_to_string(parameters, ', ') || ')' AS argument_signature,
				CASE WHEN function_type = 'table_macro' THEN 'TABLE' ELSE return_type END AS data_type,
				'SQL' AS function_language, macro_definition AS function_definition, 'VOLATILE' AS volatility,
				'NO' AS is_secure, 'NO' AS is_external, NULL::TIMESTAMPTZ AS created, NULL::TIMESTAMPTZ AS last_altered,
				coalesce(comment, description) AS comment
			FROM system.main.duckdb_functions()
			WHERE function_type IN ('macro', 'table_macro') AND NOT internal AND database_name <> 'temp'`,
		"CREATE OR REPLACE VIEW "+schema+"."+ident(InformationSchemaView("procedures"))+` AS
			SELECT NULL::VARCHAR AS procedure_catalog, NULL::VARCHAR AS procedure_schema, NULL::VARCHAR AS procedure_name,
				NULL::VARCHAR AS procedure_owner, NULL::VARCHAR AS argument_signature, NULL::VARCHAR AS data_type,
				NULL::VARCHAR AS procedure_language, NULL::VARCHAR AS procedure_definition, NULL::TIMESTAMPTZ AS created,
				NULL::TIMESTAMPTZ AS last_altered, NULL::VARCHAR AS comment
			WHERE false`)
	for _, stmt := range stmts {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

// ownerOf returns o's owner and its type, NULL for none.
func ownerOf(owners []auth.Ownership, o auth.Object) (owner, kind any) {
	if w, ok := auth.OwnerOf(owners, o); ok {
		return w.Owner, w.OwnerType
	}
	return nil, nil
}

func (e *Engine) commentRows(_ context.Context, src CatalogSource) ([][]any, error) {
	var rows [][]any
	for _, c := range src.Metadata().Comments() {
		// Views live in the local database, next to the tables they read.
		db := src.Catalog()
		if c.View {
			db = e.local
		}
		rows = append(rows, []any{db, c.Schema, nullString(c.Name), nullString(c.Column), c.Text})
	}
	return rows, nil
}

func (e *Engine) schemaRows(_ context.Context, src CatalogSource) ([][]any, error) {
	md, owners := src.Metadata(), src.Owners()
	if md == nil {
		return nil, nil
	}
	rows := make([][]any, 0, len(md.Schemas))
	for _, s := range md.Schemas {
		owner, kind := ownerOf(owners, auth.Object{Schema: s.Name})
		rows = append(rows, []any{src.Catalog(), s.Name, owner, kind, nullString(s.Comment)})
	}
	return rows, nil
}

func (e *Engine) tableRows(_ context.Context, src CatalogSource) ([][]any, error) {
	md, owners := src.Metadata(), src.Owners()
	if md == nil {
		return nil, nil
	}
	rows := make([][]any, 0, len(md.Tables))
	for _, t := range md.Tables {
		db, owner, kind := src.Catalog(), any(nil), any(nil)
		if t.Catalog != "" {
			// Connections' tables have no owner in Igloo.
			db = t.Catalog
		} else {
			owner, kind = ownerOf(owners, auth.Object{Schema: t.Schema, Table: t.Name})
		}
		typ := "BASE TABLE"
		switch {
		case t.View:
			typ = "VIEW"
		case t.Provider != "":
			typ = "EXTERNAL TABLE"
		case t.Catalog != "":
			// As PostgreSQL's information_schema calls them.
			typ = "FOREIGN"
		}
		row := []any{db, t.Schema, t.Name, typ, owner, kind, nullString(t.Comment), nullString(t.Provider),
			nullCount(t.Rows), nullCount(t.Bytes), nullCount(t.Files), nullCount(int64(t.Snapshots)), nullTime(t.LastModified),
			nullString(t.ClusteringKey)}
		if t.View || t.Catalog != "" || t.Provider != "" {
			// Only the catalog's own tables have files and snapshots.
			row[8], row[9], row[10], row[11] = nil, nil, nil, nil
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *Engine) columnRows(_ context.Context, src CatalogSource) ([][]any, error) {
	md := src.Metadata()
	if md == nil {
		return nil, nil
	}
	var rows [][]any
	for _, t := range md.Tables {
		db := src.Catalog()
		if t.Catalog != "" {
			db = t.Catalog
		}
		for i, c := range t.Columns {
			rows = append(rows, []any{db, t.Schema, t.Name, c.Name, int32(i + 1), c.Type, nullString(c.Comment)})
		}
	}
	return rows, nil
}

func (e *Engine) lineageRows(ctx context.Context, src CatalogSource) ([][]any, error) {
	edges, err := src.Lineage(ctx)
	if err != nil {
		return nil, fmt.Errorf("read lineage: %w", err)
	}
	rows := make([][]any, 0, len(edges))
	for _, x := range edges {
		kind := "TABLE"
		from := []any{nil, nil, nullString(x.From)}
		if x.FromLocation {
			kind = "LOCATION"
		} else if x.From != "" {
			from = splitTableName(x.From)
		}
		to := []any{nil, nil, nil}
		if x.To != "" {
			to = splitTableName(x.To)
		}
		hidden := x.From == "" || x.To == ""
		row := append(append(append([]any{kind}, from...), to...), nullTime(x.FirstSeen), nullTime(x.LastSeen), x.Statements,
			nullString(x.LastQueryID), nullString(x.LastUser), nullString(x.LastStatementType), nullString(x.LastJob))
		if hidden {
			// Only that there is such an edge.
			for i := 7; i < len(row); i++ {
				row[i] = nil
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *Engine) usageRows(ctx context.Context, src CatalogSource) ([][]any, error) {
	usage, err := src.TableUsage(ctx)
	if err != nil {
		return nil, fmt.Errorf("read table usage: %w", err)
	}
	rows := make([][]any, 0, len(usage))
	for _, u := range usage {
		rows = append(rows, append(splitTableName(u.Table), u.Reads, u.Writes, u.Users, nullTime(u.LastRead), nullTime(u.LastWrite)))
	}
	return rows, nil
}

// splitTableName splits catalog.schema.table, as the history names tables,
// into its parts.
func splitTableName(name string) []any {
	parts := strings.Split(name, ".")
	if len(parts) < 3 {
		return []any{nil, nil, name}
	}
	return []any{parts[0], strings.Join(parts[1:len(parts)-1], "."), parts[len(parts)-1]}
}

func nullCount(n int64) any {
	if n < 0 {
		return nil
	}
	return n
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// lazySource is a table function's rows, worked out when the statement
// runs, each time it runs.
type lazySource struct {
	columns []duckdb.ColumnInfo
	fill    func() ([][]any, error)
	rows    [][]any
	loaded  bool
	next    int
}

func (s *lazySource) ColumnInfos() []duckdb.ColumnInfo     { return s.columns }
func (s *lazySource) Cardinality() *duckdb.CardinalityInfo { return nil }

func (s *lazySource) Init() {
	s.rows, s.loaded, s.next = nil, false, 0
}

func (s *lazySource) FillRow(row duckdb.Row) (bool, error) {
	if !s.loaded {
		rows, err := s.fill()
		if err != nil {
			return false, err
		}
		s.rows, s.loaded = rows, true
	}
	if s.next >= len(s.rows) {
		return false, nil
	}
	r := s.rows[s.next]
	s.next++
	for i, v := range r {
		if err := row.SetRowValue(i, v); err != nil {
			return false, err
		}
	}
	return true, nil
}
