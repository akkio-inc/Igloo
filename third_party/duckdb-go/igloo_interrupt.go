package duckdb

import (
	"sync"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// Igloo patch: what a server needs to deal with a query that will not stop,
// outside a call that a context cancels (see runWithCtxInterrupt): asking
// DuckDB to stop it again, stopping what DuckDB's interrupt does not reach,
// reading how far it got, and, for tests, a scalar function on a connection
// that is not one of database/sql's.

// Interrupt asks DuckDB to stop the query running on the connection, as the
// cancellation of its context does.
func (conn *Conn) Interrupt() {
	interrupt(conn.conn)
}

// interruptHooks holds each connection's interrupt hook, by its
// mapping.Connection (see SetInterruptHook).
var interruptHooks sync.Map

// SetInterruptHook has f called each time the connection's query is asked
// to stop: when its context ends, again every interruptInterval until the
// query returns, and by Interrupt. f stops what the query waits for outside
// DuckDB, where no interrupt is looked for: a thread of an extension that
// waits for rows from another database stops only when those rows end. A
// context's end runs f only while the call it interrupts is in flight, as
// it interrupts DuckDB. f must not block.
func (conn *Conn) SetInterruptHook(f func()) {
	interruptHooks.Store(conn.conn, f)
}

// interrupt asks DuckDB to stop the query running on conn and runs the
// connection's interrupt hook.
func interrupt(conn mapping.Connection) {
	mapping.Interrupt(conn)
	if f, ok := interruptHooks.Load(conn); ok {
		f.(func())()
	}
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
