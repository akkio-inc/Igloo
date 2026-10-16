package pgwire

import (
	"context"
	"errors"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
)

// pgError attaches a Postgres SQLSTATE to err so clients can react to the kind
// of failure (retry a conflict, report a missing table) rather than only
// showing the message.
func pgError(err error) error {
	if err == nil || pgerror.GetCode(err) != codes.Uncategorized {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return pgerror.WithCode(err, codes.QueryCanceled)
	}

	var de *duckdb.Error
	if !errors.As(err, &de) {
		return pgerror.WithCode(err, codes.Internal)
	}
	return pgerror.WithCode(err, sqlState(de))
}

func sqlState(err *duckdb.Error) codes.Code {
	msg := strings.ToLower(err.Msg)

	switch err.Type {
	case duckdb.ErrorTypeParser, duckdb.ErrorTypeSyntax:
		return codes.Syntax
	case duckdb.ErrorTypeCatalog:
		switch {
		case strings.Contains(msg, "already exists"):
			return codes.DuplicateObject
		case strings.Contains(msg, "does not exist"):
			return codes.UndefinedTable
		}
		return codes.UndefinedObject
	case duckdb.ErrorTypeBinder:
		switch {
		case strings.Contains(msg, "column"):
			return codes.UndefinedColumn
		case strings.Contains(msg, "function"):
			return codes.UndefinedFunction
		}
		return codes.SyntaxErrorOrAccessRuleViolation
	case duckdb.ErrorTypeConversion, duckdb.ErrorTypeInvalidInput:
		return codes.InvalidTextRepresentation
	case duckdb.ErrorTypeOutOfRange:
		return codes.NumericValueOutOfRange
	case duckdb.ErrorTypeDivideByZero:
		return codes.DivisionByZero
	case duckdb.ErrorTypeMismatchType, duckdb.ErrorTypeInvalidType:
		return codes.DatatypeMismatch
	case duckdb.ErrorTypeConstraint:
		switch {
		case strings.Contains(msg, "not null"):
			return codes.NotNullViolation
		case strings.Contains(msg, "duplicate key"):
			return codes.UniqueViolation
		}
		return codes.IntegrityConstraintViolation
	case duckdb.ErrorTypeTransaction:
		if strings.Contains(msg, "aborted") {
			return codes.InFailedSQLTransaction
		}
		return codes.SerializationFailure
	case duckdb.ErrorTypeNotImplemented:
		return codes.FeatureNotSupported
	case duckdb.ErrorTypeInterrupt:
		return codes.QueryCanceled
	case duckdb.ErrorTypeOutOfMemory:
		return codes.OutOfMemory
	case duckdb.ErrorTypePermission:
		return codes.InsufficientPrivilege
	case duckdb.ErrorTypeParameterNotResolved:
		return codes.IndeterminateDatatype
	}
	return codes.Internal
}
