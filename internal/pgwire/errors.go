package pgwire

import (
	"context"
	"errors"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/catalogproxy"
	"github.com/akkio-inc/igloo/internal/federation"
)

// pgError attaches a Postgres SQLSTATE to err so clients can react to the kind
// of failure (retry a conflict, report a missing table) rather than only
// showing the message. DuckDB's postgres and mysql extensions may quote
// the token they sign in to connections' proxies with; it is replaced. Its
// iceberg extension quotes the catalog proxy's URL, which becomes the
// catalog's.
func pgError(err error) error {
	err = codedError(err)
	if err != nil {
		msg := err.Error()
		if scrubbed := catalogproxy.Scrub(federation.Scrub(msg)); scrubbed != msg {
			return pgerror.WithCode(errors.New(scrubbed), pgerror.GetCode(err))
		}
	}
	return err
}

func codedError(err error) error {
	if err == nil || pgerror.GetCode(err) != codes.Uncategorized {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return pgerror.WithCode(err, codes.QueryCanceled)
	}
	if refused := catalogRefusal(err); refused != err {
		return refused
	}

	var de *duckdb.Error
	if !errors.As(err, &de) {
		return pgerror.WithCode(err, codes.Internal)
	}
	if de.Type == duckdb.ErrorTypeInternal && strings.Contains(de.Msg, rowidFilterFailure) {
		return pgerror.WithCode(errors.New(rowidFilterMessage), codes.FeatureNotSupported)
	}
	return pgerror.WithCode(err, sqlState(de))
}

// rowidFilterFailure is how DuckDB 1.5.6 fails to plan some filters on rowid
// when the spatial extension is loaded, as it always is in Igloo. Spatial's
// R-tree optimizer passes the column of every filter DuckDB moved into a
// table scan as an optional_idx, and rowid's column id is the value that
// means "no index", so it throws. DuckDB moves a filter into the scan as an
// expression when it reads rowid alone and is not a comparison with
// constants: rowid % 3 = 0, hash(rowid) % 3 = 0, rowid::VARCHAR = '5'.
// Iceberg tables have no rowid; DuckDB's own tables (temporary ones, and
// all of a server without a catalog) do, and views over them fail alike.
//
// Nothing can be turned off instead. DuckDB turns off extensions'
// optimizers only all at once and for every session (disabled_optimizers
// is global), and the Iceberg extension's one makes scans read the columns
// its equality deletes are checked on. Moving the filter out of the scan
// would mean rewriting the statement, and the subquery that does it for
// rowid % 3 = 0 does not for rowid::VARCHAR = '5'. So the client is told
// what to write instead, rather than shown DuckDB's assertion and stack
// trace. docs/upstream/duckdb-spatial-rowid-filter.md is the report for
// DuckDB; drop this once Igloo's DuckDB has the fix.
const rowidFilterFailure = "optional_idx cannot be initialized with an invalid index"

const rowidFilterMessage = "DuckDB could not plan this statement (optional_idx cannot be initialized with an invalid index): " +
	"with the spatial extension loaded, DuckDB 1.5.6 fails this way on filters on rowid such as rowid % 3 = 0 or hash(rowid) % 3 = 0, " +
	"in views too. Compare rowid only with constants, as in rowid < 1000 or rowid IN (1, 2), " +
	"or filter on the table's own columns, as in hash(id) % 3 = 0"

func sqlState(err *duckdb.Error) codes.Code {
	msg := strings.ToLower(err.Msg)

	// The catalog did not confirm whether a commit was applied (see
	// package catalogproxy). It must not look like a conflict, which
	// clients retry: the commit may have landed.
	if strings.Contains(msg, "commitstateunknownexception") {
		return codes.StatementCompletionUnknown
	}

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
