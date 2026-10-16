package wire

import "context"

// Igloo patch: lets the server run statements its own parse function
// prepared without a client connection, for its in-process sessions.

// Columns returns the columns of the rows the statement returns, or nil
// when it returns none.
func (stmt *PreparedStatement) Columns() Columns {
	return stmt.columns
}

// Run executes the statement, writing its result to writer.
func (stmt *PreparedStatement) Run(ctx context.Context, writer DataWriter, parameters []Parameter) error {
	return stmt.fn(ctx, writer, parameters)
}
