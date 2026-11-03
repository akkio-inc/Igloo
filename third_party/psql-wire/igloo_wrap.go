package wire

import "context"

// CommandWrapper has every command a client sends run by wrap, which must call
// run, once, on its own goroutine, and return what run does: Igloo runs each on
// a thread of its own, from which a C++ exception that escapes DuckDB ends the
// session rather than the process.
func CommandWrapper(wrap func(ctx context.Context, run func() error) error) OptionFn {
	return func(srv *Server) error {
		srv.commandWrap = wrap
		return nil
	}
}

// WrapFn replaces the statement's function by wrap applied to it, in
// place, so the statement keeps its identity: a parse function can add
// something every run of what it prepared does, such as sending a notice,
// without knowing how the statement was made.
func (stmt *PreparedStatement) WrapFn(wrap func(PreparedStatementFn) PreparedStatementFn) {
	stmt.fn = wrap(stmt.fn)
}
