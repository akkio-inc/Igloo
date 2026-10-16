//go:build duckdb_arrow

package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/duckdb/duckdb-go/v2/arrowmapping"
	"github.com/duckdb/duckdb-go/v2/mapping"
)

// Igloo patch: Apache Arrow results from a prepared statement, so a server
// can check a statement once, describe its result in Arrow before running
// it, and stream that result without preparing the statement again.

// ArrowSchema returns the Arrow schema of the statement's result: the
// schema of the record batches QueryArrowContext returns, under the
// connection's current settings (such as its time zone).
func (s *Stmt) ArrowSchema() (*arrow.Schema, error) {
	if err := s.checkState(); err != nil {
		return nil, err
	}
	n := mapping.PreparedStatementColumnCount(*s.preparedStmt)
	names := make([]string, n)
	types := make([]mapping.LogicalType, n)
	for i := range n {
		names[i] = mapping.PreparedStatementColumnName(*s.preparedStmt, i)
		types[i] = mapping.PreparedStatementColumnLogicalType(*s.preparedStmt, i)
	}
	return s.arrowSchema(types, names)
}

// ParamArrowSchema returns the Arrow types of the statement's parameters,
// named $1, $2 and so on. A parameter whose type DuckDB cannot infer is a
// string.
func (s *Stmt) ParamArrowSchema() (*arrow.Schema, error) {
	if err := s.checkState(); err != nil {
		return nil, err
	}
	n := int(mapping.NParams(*s.preparedStmt))
	names := make([]string, n)
	types := make([]mapping.LogicalType, n)
	for i := range n {
		names[i] = fmt.Sprintf("$%d", i+1)
		types[i] = mapping.ParamLogicalType(*s.preparedStmt, mapping.IdxT(i+1))
		if types[i].Ptr != nil {
			switch mapping.GetTypeId(types[i]) {
			case mapping.TypeInvalid, mapping.TypeAny, mapping.TypeSQLNull:
				mapping.DestroyLogicalType(&types[i])
			default:
				continue
			}
		}
		types[i] = mapping.CreateLogicalType(mapping.TypeVarchar)
	}
	return s.arrowSchema(types, names)
}

// arrowSchema converts types, which it destroys, to an Arrow schema.
func (s *Stmt) arrowSchema(types []mapping.LogicalType, names []string) (*arrow.Schema, error) {
	defer func() {
		for i := range types {
			mapping.DestroyLogicalType(&types[i])
		}
	}()
	var opts arrowmapping.ArrowOptions
	arrowmapping.ConnectionGetArrowOptions(s.conn.conn, &opts)
	defer arrowmapping.DestroyArrowOptions(&opts)

	schema, ed := arrowmapping.NewArrowSchema(opts, types, names)
	if err := errorDataError(ed); err != nil {
		return nil, fmt.Errorf("failed to create arrow schema: %w", err)
	}
	return schema, nil
}

// QueryArrowContext runs the statement with args and returns its result as
// Arrow record batches. Releasing the reader closes the result; the
// statement stays prepared and can run again.
func (s *Stmt) QueryArrowContext(ctx context.Context, args []driver.NamedValue) (array.RecordReader, error) {
	rows, err := s.QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}
	rdr, err := recordReaderFromRows(ctx, rows)
	if err != nil {
		return nil, errors.Join(err, rows.Close())
	}
	return rdr, nil
}
