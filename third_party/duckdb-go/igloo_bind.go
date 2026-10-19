package duckdb

// Igloo patch: a hook around the steps in which DuckDB binds a statement,
// so a server can keep those steps from running while something else on the
// same database changes what they read.

// SetBindHold makes the connection call hold before each step in which
// DuckDB binds a statement on it, and the function hold returns once the
// step is done: preparing a statement, and creating the pending result that
// runs a prepared one. DuckDB binds a prepared statement again for each run
// when it reads a catalog that has no versions to check the bound plan
// against, such as an Iceberg REST catalog, so the hook covers every run,
// not only the prepare. Running the statement after that is not held. A nil
// hold removes the hook. Set it before the connection is used.
func (conn *Conn) SetBindHold(hold func() (release func())) {
	conn.bindHold = hold
}

// holdBind calls the connection's bind hold, if any, and returns the
// function that releases it.
func (conn *Conn) holdBind() func() {
	if conn.bindHold == nil {
		return func() {}
	}
	return conn.bindHold()
}
