package flightsql

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/akkio-inc/igloo/internal/pgwire"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Metadata commands answer from the information schema, with SQL run in
// the caller's session, so they show what the user's own queries would:
// table and column names are visible to every user, as in the PostgreSQL
// catalogs. Table types use JDBC's names (TABLE rather than BASE TABLE),
// which BI tools filter by.

func (h *handler) registerSqlInfo(version string) error {
	infos := map[fsql.SqlInfo]any{
		fsql.SqlInfoFlightSqlServerName:                        "Igloo",
		fsql.SqlInfoFlightSqlServerVersion:                     version,
		fsql.SqlInfoFlightSqlServerArrowVersion:                arrow.PkgVersion,
		fsql.SqlInfoFlightSqlServerReadOnly:                    false,
		fsql.SqlInfoFlightSqlServerSql:                         true,
		fsql.SqlInfoFlightSqlServerSubstrait:                   false,
		fsql.SqlInfoFlightSqlServerTransaction:                 int32(fsql.SqlTransactionNone),
		fsql.SqlInfoFlightSqlServerCancel:                      true,
		fsql.SqlInfoFlightSqlServerBulkIngestion:               false,
		fsql.SqlInfoFlightSqlServerIngestTransactionsSupported: false,
		fsql.SqlInfoDDLCatalog:                                 false,
		fsql.SqlInfoDDLSchema:                                  true,
		fsql.SqlInfoDDLTable:                                   true,
		fsql.SqlInfoIdentifierCase:                             int64(fsql.SqlCaseSensitivityCaseInsensitive),
		fsql.SqlInfoIdentifierQuoteChar:                        `"`,
		fsql.SqlInfoQuotedIdentifierCase:                       int64(fsql.SqlCaseSensitivityCaseInsensitive),
		fsql.SqlInfoSearchStringEscape:                         `\`,
		fsql.SqlInfoNullOrdering:                               int64(fsql.SqlNullOrderingSortAtEnd),
		// Grants decide which tables a user may read.
		fsql.SqlInfoAllTablesAreASelectable: false,
		fsql.SqlInfoTransactionsSupported:   false,
	}
	for id, v := range infos {
		if err := h.RegisterSqlInfo(id, v); err != nil {
			return err
		}
	}
	return nil
}

// metadataInfo describes a metadata command's result; the command is its
// own ticket.
func metadataInfo(schema *arrow.Schema, desc *flight.FlightDescriptor) *flight.FlightInfo {
	return flightInfo(schema, desc, desc.GetCmd())
}

func (h *handler) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return metadataInfo(schema_ref.Catalogs, desc), nil
}

func (h *handler) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	rows, err := sessionFrom(ctx).query(ctx,
		"SELECT DISTINCT catalog_name FROM information_schema.schemata ORDER BY catalog_name")
	if err != nil {
		return nil, nil, err
	}
	return result(schema_ref.Catalogs, rows)
}

func (h *handler) GetFlightInfoSchemas(_ context.Context, _ fsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return metadataInfo(schema_ref.DBSchemas, desc), nil
}

func (h *handler) DoGetDBSchemas(ctx context.Context, cmd fsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var f filter
	f.equal("catalog_name", cmd.GetCatalog())
	f.like("schema_name", cmd.GetDBSchemaFilterPattern())
	rows, err := sessionFrom(ctx).query(ctx,
		"SELECT catalog_name, schema_name FROM information_schema.schemata"+f.where()+" ORDER BY 1, 2", f.args...)
	if err != nil {
		return nil, nil, err
	}
	return result(schema_ref.DBSchemas, rows)
}

func (h *handler) GetFlightInfoTables(_ context.Context, cmd fsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if cmd.GetIncludeSchema() {
		return metadataInfo(schema_ref.TablesWithIncludedSchema, desc), nil
	}
	return metadataInfo(schema_ref.Tables, desc), nil
}

// tableType renames the information schema's BASE TABLE to TABLE.
const tableType = "CASE table_type WHEN 'BASE TABLE' THEN 'TABLE' ELSE table_type END"

func (h *handler) DoGetTables(ctx context.Context, cmd fsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	sess := sessionFrom(ctx)
	var f filter
	f.equal("table_catalog", cmd.GetCatalog())
	f.like("table_schema", cmd.GetDBSchemaFilterPattern())
	f.like("table_name", cmd.GetTableNameFilterPattern())
	tablesOnly := f
	f.in(tableType, cmd.GetTableTypes())
	rows, err := sess.query(ctx, "SELECT table_catalog, table_schema, table_name, "+tableType+
		" FROM information_schema.tables"+f.where()+" ORDER BY 1, 2, 3, 4", f.args...)
	if err != nil {
		return nil, nil, err
	}
	if !cmd.GetIncludeSchema() {
		return result(schema_ref.Tables, rows)
	}

	cols, err := sess.query(ctx, "SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable"+
		" FROM information_schema.columns"+tablesOnly.where()+" ORDER BY 1, 2, 3, ordinal_position", tablesOnly.args...)
	if err != nil {
		return nil, nil, err
	}
	schemas, err := sess.tableSchemas(ctx, cols)
	if err != nil {
		return nil, nil, err
	}
	for i, row := range rows {
		key := [3]any{row[0], row[1], row[2]}
		rows[i] = append(row, flight.SerializeSchema(arrow.NewSchema(schemas[key], nil), memory.DefaultAllocator))
	}
	return result(schema_ref.TablesWithIncludedSchema, rows)
}

// tableSchemas builds each table's Arrow schema from its columns, rows of
// information_schema.columns. DuckDB itself picks each type's Arrow type:
// it describes a query of a typed NULL per column, which reads no table
// and so needs no grant. A column whose type that query cannot name is a
// string.
func (sess *session) tableSchemas(ctx context.Context, cols [][]any) (map[[3]any][]arrow.Field, error) {
	types := make([]string, len(cols))
	for i, c := range cols {
		types[i], _ = c[4].(string)
	}
	arrowTypes, err := sess.arrowTypes(ctx, types)
	if err != nil {
		return nil, err
	}
	fields := make(map[[3]any][]arrow.Field)
	for i, c := range cols {
		key := [3]any{c[0], c[1], c[2]}
		md := fsql.NewColumnMetadataBuilder().TypeName(types[i])
		for j, set := range []func(string) *fsql.ColumnMetadataBuilder{md.CatalogName, md.SchemaName, md.TableName} {
			if s, ok := c[j].(string); ok {
				set(s)
			}
		}
		name, _ := c[3].(string)
		fields[key] = append(fields[key], arrow.Field{
			Name: name, Type: arrowTypes[i], Nullable: c[5] != "NO", Metadata: md.Metadata(),
		})
	}
	return fields, nil
}

// arrowTypes returns the Arrow types DuckDB gives values of the SQL types
// named in types.
func (sess *session) arrowTypes(ctx context.Context, types []string) ([]arrow.DataType, error) {
	ctx = pgwire.WithIglooSQL(ctx) // DuckDB's type names, whatever the session's dialect
	out := make([]arrow.DataType, len(types))
	for i := range out {
		out[i] = arrow.BinaryTypes.String
	}
	if len(types) == 0 {
		return out, nil
	}
	probe := func(types []string) (*arrow.Schema, error) {
		exprs := make([]string, len(types))
		for i, t := range types {
			exprs[i] = fmt.Sprintf("NULL::%s AS c%d", t, i)
		}
		query := "SELECT " + strings.Join(exprs, ", ")
		// The types come from the catalog; still, never send what could
		// be more than one statement, which preparing would run.
		if len(sqllex.Split(query)) != 1 {
			return nil, fmt.Errorf("not one statement")
		}
		var schema *arrow.Schema
		err := sess.metadata(ctx, func(es *pgwire.EmbeddedSession) error {
			ps, err := es.Prepare(ctx, query)
			if err != nil {
				return err
			}
			defer ps.Close() //nolint:errcheck
			schema, err = ps.ArrowSchema()
			return err
		})
		return schema, err
	}
	if schema, err := probe(types); err == nil {
		for i, f := range schema.Fields() {
			out[i] = f.Type
		}
		return out, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, statusOf(err)
	}
	for i, t := range types {
		if schema, err := probe([]string{t}); err == nil {
			out[i] = schema.Field(0).Type
		}
	}
	return out, nil
}

func (h *handler) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return metadataInfo(schema_ref.TableTypes, desc), nil
}

func (h *handler) DoGetTableTypes(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return result(schema_ref.TableTypes, [][]any{{"LOCAL TEMPORARY"}, {"TABLE"}, {"VIEW"}})
}

// Iceberg tables have no enforced keys, so key listings are empty; BI
// tools ask for them while browsing and fail on an unimplemented command.

func (h *handler) GetFlightInfoPrimaryKeys(_ context.Context, _ fsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return metadataInfo(schema_ref.PrimaryKeys, desc), nil
}

func (h *handler) DoGetPrimaryKeys(context.Context, fsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return result(schema_ref.PrimaryKeys, nil)
}

func (h *handler) GetFlightInfoImportedKeys(_ context.Context, _ fsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return metadataInfo(schema_ref.ImportedKeys, desc), nil
}

func (h *handler) DoGetImportedKeys(context.Context, fsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return result(schema_ref.ImportedKeys, nil)
}

func (h *handler) GetFlightInfoExportedKeys(_ context.Context, _ fsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return metadataInfo(schema_ref.ExportedKeys, desc), nil
}

func (h *handler) DoGetExportedKeys(context.Context, fsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return result(schema_ref.ExportedKeys, nil)
}

func (h *handler) GetFlightInfoCrossReference(_ context.Context, _ fsql.CrossTableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return metadataInfo(schema_ref.CrossReference, desc), nil
}

func (h *handler) DoGetCrossReference(context.Context, fsql.CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return result(schema_ref.CrossReference, nil)
}

// filter builds a WHERE clause with parameters.
type filter struct {
	conds []string
	args  []any
}

func (f *filter) arg(v any) string {
	f.args = append(f.args, v)
	return fmt.Sprintf("$%d", len(f.args))
}

// equal matches column to value, when there is one.
func (f *filter) equal(column string, value *string) {
	if value != nil {
		f.conds = append(f.conds, column+" = "+f.arg(*value))
	}
}

// like matches column to a Flight SQL search pattern, with % and _ as in
// LIKE, escaped with a backslash (see SqlInfoSearchStringEscape).
func (f *filter) like(column string, pattern *string) {
	if pattern != nil {
		f.conds = append(f.conds, column+" LIKE "+f.arg(*pattern)+` ESCAPE '\'`)
	}
}

// in matches expr to one of values, when any are given.
func (f *filter) in(expr string, values []string) {
	if len(values) == 0 {
		return
	}
	params := make([]string, len(values))
	for i, v := range values {
		params[i] = f.arg(v)
	}
	f.conds = append(f.conds, expr+" IN ("+strings.Join(params, ", ")+")")
}

func (f *filter) where() string {
	if len(f.conds) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(f.conds, " AND ")
}

// query runs a metadata query in the session and returns its rows, each
// value a string in PostgreSQL's text format or nil.
func (sess *session) query(ctx context.Context, sql string, args ...any) ([][]any, error) {
	// Igloo's own SQL, read as DuckDB's whatever the session's sql_dialect.
	ctx = pgwire.WithIglooSQL(ctx)
	var out collect
	err := sess.metadata(ctx, func(es *pgwire.EmbeddedSession) error {
		ps, err := es.Prepare(ctx, sql)
		if err != nil {
			return err
		}
		defer ps.Close() //nolint:errcheck
		return ps.Run(ctx, args, &out)
	})
	return out.rows, statusOf(err)
}

// metadata runs fn on the caller's session, or, while that is busy
// running a statement, on a session of its own for the same user, so a
// client can browse the catalog while it reads a result.
func (sess *session) metadata(ctx context.Context, fn func(*pgwire.EmbeddedSession) error) error {
	if sess.tryAcquire() {
		defer sess.release()
		return fn(sess.es)
	}
	es, err := sess.s.sql.OpenEmbedded(ctx, sess.es.User(), sess.client)
	if err != nil {
		return err
	}
	defer es.Close() //nolint:errcheck
	return fn(es)
}

// collect gathers a small result's rows as text.
type collect struct{ rows [][]any }

func (c *collect) Columns([]pgwire.Column) error { return nil }
func (c *collect) Complete(string) error         { return nil }

func (c *collect) Row(values []any) error {
	c.rows = append(c.rows, append([]any(nil), values...))
	return nil
}

// result returns rows, strings or (for a binary column) byte slices, as a
// stream of one record batch with schema.
func result(schema *arrow.Schema, rows [][]any) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, row := range rows {
		for i, v := range row {
			switch fb := b.Field(i).(type) {
			case *array.StringBuilder:
				if s, ok := v.(string); ok {
					fb.Append(s)
				} else {
					fb.AppendNull()
				}
			case *array.BinaryBuilder:
				fb.Append(v.([]byte))
			}
		}
	}
	chunks := make(chan flight.StreamChunk, 1)
	chunks <- flight.StreamChunk{Data: b.NewRecordBatch()}
	close(chunks)
	return schema, chunks, nil
}
