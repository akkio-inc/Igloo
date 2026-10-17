package flightsql

import (
	"context"
	"errors"
	"fmt"

	pgcodes "github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/akkio-inc/igloo/internal/pgwire"
)

// statusOf turns an error from a SQL session into the gRPC status a Flight
// client sees. The status code follows the error's SQLSTATE (42501, a
// refused statement, is PermissionDenied), and the message ends with the
// SQLSTATE itself, so a client can react exactly as a PostgreSQL client
// would.
func statusOf(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	e := pgerror.Flatten(err)
	if e.Code == pgcodes.Uncategorized && errors.Is(err, context.Canceled) {
		e.Code = pgcodes.QueryCanceled
	}
	code := grpcCode(e.Code)
	switch {
	case errors.Is(err, pgwire.ErrStatementTimeout):
		code = codes.DeadlineExceeded
	case e.Code == pgcodes.QueryCanceled:
		// Rather than DuckDB's "INTERRUPT Error: Interrupted!".
		e.Message = "canceling statement due to user request"
	case e.Code == pgcodes.Uncategorized:
		e.Code = pgcodes.Internal
	}
	msg := fmt.Sprintf("%s (SQLSTATE %s)", e.Message, e.Code)
	if e.Hint != "" {
		msg += "; hint: " + e.Hint
	}
	return status.Error(code, msg)
}

// grpcCode maps a SQLSTATE to the gRPC status code that means the same.
func grpcCode(state pgcodes.Code) codes.Code {
	switch state {
	case pgcodes.InsufficientPrivilege:
		return codes.PermissionDenied
	case pgcodes.QueryCanceled:
		return codes.Canceled
	case pgcodes.ProtocolViolation:
		// The wrong number of parameters, not a broken connection.
		return codes.InvalidArgument
	case pgcodes.UndefinedTable, pgcodes.UndefinedColumn, pgcodes.UndefinedFunction, pgcodes.UndefinedObject,
		pgcodes.InvalidSQLStatementName, pgcodes.InvalidCursorName:
		return codes.NotFound
	case pgcodes.DuplicateObject, pgcodes.DuplicateSchema, pgcodes.DuplicateColumn, pgcodes.UniqueViolation, "42P07":
		return codes.AlreadyExists
	case pgcodes.StatementCompletionUnknown:
		// Not Aborted, which clients retry: the commit may have landed.
		return codes.Unknown
	}
	if len(state) < 2 {
		return codes.Unknown
	}
	switch state[:2] {
	case "0A":
		return codes.Unimplemented
	case "08", "57":
		return codes.Unavailable
	case "22", "26", "42":
		return codes.InvalidArgument
	case "23", "25":
		return codes.FailedPrecondition
	case "28":
		return codes.Unauthenticated
	case "40":
		return codes.Aborted
	case "53", "54":
		return codes.ResourceExhausted
	}
	return codes.Internal
}
