package duckdb

// Igloo patch: a hook around each call in which DuckDB binds, runs or drops
// a statement's plan on the calling thread, so a server can tell, should C++
// end the call there, whose statement it was.

// SetCallGuard makes the connection run through guard each step in which
// DuckDB binds, runs or drops a statement's plan on the calling goroutine's
// thread: preparing a statement, creating the pending result that runs it
// (which binds it again), carrying out its tasks and fetching its result,
// reading a result's chunks, and destroying a prepared statement or a
// result. guard must call call once, on its own goroutine, before it
// returns. Each step is one call or a few into DuckDB and Go code of this
// package's, which switches no coroutine, so guard may lock the goroutine to
// its thread for it. A nil guard removes the hook. Set it before the
// connection is used.
func (conn *Conn) SetCallGuard(guard func(call func())) {
	conn.callGuard = guard
}

// guard runs call through the connection's call guard, if any.
func (conn *Conn) guard(call func()) {
	if conn.callGuard == nil {
		call()
		return
	}
	conn.callGuard(call)
}

// guard runs call through the call guard of the rows' connection, if any.
func (r *rows) guard(call func()) {
	if r.stmt == nil || r.stmt.conn == nil {
		call()
		return
	}
	r.stmt.conn.guard(call)
}
