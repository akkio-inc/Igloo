package snowflake

import "fmt"

// SQLSTATE codes of the errors Translate returns.
const (
	// CodeSyntax is syntax_error: the statement is not valid SQL.
	CodeSyntax = "42601"
	// CodeUnsupported is feature_not_supported: the statement uses a
	// Snowflake construct Igloo cannot translate faithfully.
	CodeUnsupported = "0A000"
	// CodeInvalidValue is invalid_parameter_value.
	CodeInvalidValue = "22023"
)

// Error is a statement Translate refuses.
type Error struct {
	// Code is the SQLSTATE.
	Code string
	// Message says what is wrong; Hint, if set, what to do instead.
	Message, Hint string
}

func (e *Error) Error() string { return e.Message }

// syntaxError refuses a statement that cannot be read.
func syntaxError(pos int, msg string) *Error {
	return &Error{Code: CodeSyntax, Message: fmt.Sprintf("%s (at byte %d)", msg, pos+1)}
}

// unsupported refuses a construct Igloo does not translate.
func unsupported(construct, hint string) *Error {
	return &Error{Code: CodeUnsupported, Message: construct + " is not supported in Igloo's Snowflake dialect", Hint: hint}
}
