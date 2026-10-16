package engine

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/policy"
)

// SetPolicies makes igloo_policy_references() and igloo_tag_references()
// list what f returns. Until it is set, they are empty.
func (e *Engine) SetPolicies(f func() *policy.Snapshot) {
	e.policies.Store(&f)
}

func (e *Engine) policySnapshot() *policy.Snapshot {
	if f := e.policies.Load(); f != nil {
		return (*f)()
	}
	return nil
}

// registerPolicies defines igloo_policy_references() and
// igloo_tag_references(), and the views igloo.policy_references and
// igloo.tag_references over them, after Snowflake's views of the same
// names: which masking and row access policies apply to which tables and
// columns, and which columns have which tags. Each user sees those of the
// tables they may see (see CatalogSource).
func (e *Engine) registerPolicies(ctx context.Context) error {
	// Not closed: closing a DB closes its connector.
	db := sql.OpenDB(e.connector)
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck

	varchar, err := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	if err != nil {
		return err
	}
	list, err := duckdb.NewListInfo(varchar)
	if err != nil {
		return err
	}
	column := func(name string, t duckdb.TypeInfo) duckdb.ColumnInfo { return duckdb.ColumnInfo{Name: name, T: t} }
	// visible keeps the rows, whose database, schema and table are at 2, 3
	// and 4, of the tables the statement's user may see: the catalog's, or
	// connections'.
	visible := func(ctx context.Context, rows func(catalog string) [][]any) func() ([][]any, error) {
		source := e.catalogSource(ctx)
		return func() ([][]any, error) {
			src, ok := source()
			if !ok {
				return nil, nil
			}
			all := rows(src.Catalog())
			out := all[:0]
			for _, r := range all {
				db, _ := r[2].(string)
				schema, _ := r[3].(string)
				name, _ := r[4].(string)
				if db == src.Catalog() && src.SeesTable(schema, name) || db != src.Catalog() && src.SeesConnectionTable(db, schema, name) {
					out = append(out, r)
				}
			}
			return out, nil
		}
	}
	err = duckdb.RegisterTableUDF(conn, "igloo_policy_references", duckdb.RowTableFunction{
		BindArgumentsContext: func(ctx context.Context, _ map[string]any, _ ...any) (duckdb.RowTableSource, error) {
			return &lazySource{
				columns: []duckdb.ColumnInfo{
					column("policy_name", varchar), column("policy_kind", varchar), column("ref_database_name", varchar),
					column("ref_schema_name", varchar), column("ref_entity_name", varchar), column("ref_entity_domain", varchar),
					column("ref_column_name", varchar), column("ref_arg_column_names", list), column("tag_name", varchar),
				},
				fill: visible(ctx, func(catalog string) [][]any { return policyReferences(catalog, e.policySnapshot()) }),
			}, nil
		},
	})
	if err != nil {
		return err
	}
	err = duckdb.RegisterTableUDF(conn, "igloo_tag_references", duckdb.RowTableFunction{
		BindArgumentsContext: func(ctx context.Context, _ map[string]any, _ ...any) (duckdb.RowTableSource, error) {
			return &lazySource{
				columns: []duckdb.ColumnInfo{
					column("tag_name", varchar), column("tag_value", varchar), column("object_database", varchar),
					column("object_schema", varchar), column("object_name", varchar), column("domain", varchar),
					column("column_name", varchar),
				},
				fill: visible(ctx, func(catalog string) [][]any { return tagReferences(catalog, e.policySnapshot()) }),
			}, nil
		},
	})
	if err != nil {
		return err
	}

	schema := ident(e.local) + "." + ident(HistorySchema)
	for _, stmt := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + schema,
		"CREATE OR REPLACE VIEW " + schema + ".policy_references AS SELECT * FROM system.main.igloo_policy_references()",
		"CREATE OR REPLACE VIEW " + schema + ".tag_references AS SELECT * FROM system.main.igloo_tag_references()",
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

// policyReferences lists where each policy applies: one row per masked
// column, per column masked through a tag, and per table with a row
// access policy.
func policyReferences(catalog string, snap *policy.Snapshot) [][]any {
	if snap == nil {
		return nil
	}
	var rows [][]any
	for _, t := range snap.Tables() {
		db := catalog
		if t.Catalog != "" {
			// A table of a connection to another database.
			db = t.Catalog
		}
		if t.Filter != nil {
			cols := make([]string, len(t.Filter.Columns))
			copy(cols, t.Filter.Columns)
			rows = append(rows, []any{t.Filter.Policy, string(policy.RowAccess), db, t.Schema, t.Table, "TABLE", nil, cols, nil})
		}
		names := make([]string, 0, len(t.Columns))
		for name := range t.Columns {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			c := t.Columns[name]
			if c.Mask != nil {
				rows = append(rows, []any{c.Mask.Name, string(policy.Masking), db, t.Schema, t.Table, "TABLE", c.Column, nil, nil})
			}
			for _, tm := range c.TagMasks {
				rows = append(rows, []any{tm.Policy.Name, string(policy.Masking), db, t.Schema, t.Table, "TABLE", c.Column, nil, tm.Tag})
			}
		}
	}
	return rows
}

// tagReferences lists every tag set on a column.
func tagReferences(catalog string, snap *policy.Snapshot) [][]any {
	if snap == nil {
		return nil
	}
	tags := slices.Clone(snap.Catalog.ColumnTags)
	slices.SortFunc(tags, func(a, b policy.ColumnTag) int {
		return strings.Compare(strings.ToLower(a.Schema+"."+a.Table+"."+a.Column+"."+a.Tag),
			strings.ToLower(b.Schema+"."+b.Table+"."+b.Column+"."+b.Tag))
	})
	rows := make([][]any, len(tags))
	for i, ct := range tags {
		db := catalog
		if ct.Catalog != "" {
			db = ct.Catalog
		}
		rows[i] = []any{ct.Tag, ct.Value, db, ct.Schema, ct.Table, "COLUMN", ct.Column}
	}
	return rows
}
