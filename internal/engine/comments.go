package engine

import (
	"context"
	"database/sql"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// SetComments makes igloo_comments() list what f returns. Until it is set,
// igloo_comments() is empty.
func (e *Engine) SetComments(f func() []catalog.Comment) {
	e.comments.Store(&f)
}

// registerComments defines igloo_comments(), which lists the comments on the
// catalog's tables, views and columns: table_catalog, table_schema,
// table_name, column_name (NULL for the table or view itself) and comment.
// DuckDB does not read comments from Iceberg metadata, so Igloo serves them
// itself; pg_description and the obj_description and col_description
// functions are built on this.
func (e *Engine) registerComments(ctx context.Context) error {
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
	return duckdb.RegisterTableUDF(conn, "igloo_comments", duckdb.RowTableFunction{
		BindArguments: func(map[string]any, ...any) (duckdb.RowTableSource, error) {
			var rows []catalog.Comment
			if f := e.comments.Load(); f != nil {
				rows = (*f)()
			}
			return &commentSource{rows: rows, varchar: varchar, catalog: e.catalog, local: e.local}, nil
		},
	})
}

type commentSource struct {
	rows           []catalog.Comment
	next           int
	varchar        duckdb.TypeInfo
	catalog, local string
}

func (s *commentSource) ColumnInfos() []duckdb.ColumnInfo {
	cols := []string{"table_catalog", "table_schema", "table_name", "column_name", "comment"}
	infos := make([]duckdb.ColumnInfo, len(cols))
	for i, name := range cols {
		infos[i] = duckdb.ColumnInfo{Name: name, T: s.varchar}
	}
	return infos
}

func (s *commentSource) Cardinality() *duckdb.CardinalityInfo {
	return &duckdb.CardinalityInfo{Cardinality: uint(len(s.rows)), Exact: true}
}

func (s *commentSource) Init() {}

func (s *commentSource) FillRow(row duckdb.Row) (bool, error) {
	if s.next >= len(s.rows) {
		return false, nil
	}
	c := s.rows[s.next]
	s.next++
	// Views live in the local database, next to the tables they read.
	db := s.catalog
	if c.View {
		db = s.local
	}
	var column any
	if c.Column != "" {
		column = c.Column
	}
	for i, v := range []any{db, c.Schema, c.Name, column, c.Text} {
		if err := row.SetRowValue(i, v); err != nil {
			return false, err
		}
	}
	return true, nil
}
