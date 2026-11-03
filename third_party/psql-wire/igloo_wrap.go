package wire

// WrapFn replaces the statement's function by wrap applied to it, in
// place, so the statement keeps its identity: a parse function can add
// something every run of what it prepared does, such as sending a notice,
// without knowing how the statement was made.
func (stmt *PreparedStatement) WrapFn(wrap func(PreparedStatementFn) PreparedStatementFn) {
	stmt.fn = wrap(stmt.fn)
}
