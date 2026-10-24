package duckdb

import "github.com/duckdb/duckdb-go/v2/mapping"

// Igloo patch: what a server needs to deal with a query that will not stop,
// outside a call that a context cancels (see runWithCtxInterrupt): asking
// DuckDB to stop it again, reading how far it got, and, for tests, a scalar
// function on a connection that is not one of database/sql's.

// Interrupt asks DuckDB to stop the query running on the connection, as the
// cancellation of its context does.
func (conn *Conn) Interrupt() {
	mapping.Interrupt(conn.conn)
}

// Progress returns how far the query running on the connection is: DuckDB's
// estimate of the percentage done (-1 when it has none), and the rows it has
// processed of those it expects to.
func (conn *Conn) Progress() (percentage float64, rows, total uint64) {
	p := mapping.QueryProgress(conn.conn)
	return mapping.QueryProgressTypeMembers(&p)
}

// RegisterScalarUDFOn is RegisterScalarUDF for a connection held directly.
func RegisterScalarUDFOn(conn *Conn, name string, f ScalarFunc) error {
	if conn.closed {
		return errClosedCon
	}
	function, err := createScalarFuncIn(conn.ctxStore, name, f)
	if err != nil {
		return getError(errAPI, err)
	}
	defer mapping.DestroyScalarFunction(&function)
	if mapping.RegisterScalarFunction(conn.conn, function) == mapping.StateError {
		return getError(errAPI, errScalarUDFCreate)
	}
	return nil
}
