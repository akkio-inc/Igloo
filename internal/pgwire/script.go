package pgwire

import (
	"context"
	"errors"
	"fmt"
	"slices"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// A simple query may hold several statements, as psql -c and psycopg2
// (and so dbt) send them. Igloo runs them one at a time: each is parsed,
// checked and run as if the client had sent it alone, once the one before
// has finished, so a statement can use what an earlier one created. DuckDB
// would instead prepare them together, running all but the last while
// preparing, before Igloo could check them.
//
// As in PostgreSQL, the first statement that fails ends the query, and the
// statements form one transaction unless they hold transaction statements
// of their own or run inside a transaction block.

// parseScript returns the statements of a simple query that holds several.
func (s *Server) parseScript(sess *session, parts []sqllex.Statement) wire.PreparedStatements {
	implicit := !sess.inTx && !slices.ContainsFunc(parts, func(p sqllex.Statement) bool {
		return transactionChange(p.Text) != 0
	})
	stmts := make(wire.PreparedStatements, len(parts))
	for i, part := range parts {
		stmts[i] = wire.NewStatement(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if implicit && i == 0 {
				if err := sess.exec(ctx, "BEGIN"); err != nil {
					return pgError(err)
				}
				sess.inTx, sess.txFailed, sess.savepoints, sess.changes = true, false, nil, 0
			}
			err := s.runScriptStatement(ctx, part.Text, w)
			if !implicit || err == nil && i < len(parts)-1 {
				return err
			}
			end := "COMMIT"
			if err != nil || sess.txFailed {
				end = "ROLLBACK"
			}
			if eerr := sess.exec(context.WithoutCancel(ctx), end); eerr != nil && err == nil {
				err = pgError(eerr)
			}
			sess.inTx, sess.txFailed, sess.savepoints, sess.changes = false, false, nil, 0
			sess.closeCursors(ctx, false)
			return err
		})
	}
	return stmts
}

// aloneError returns an error for text, one statement of several in a
// query, if it is one that must be sent on its own: Igloo carries these out
// itself, outside DuckDB and its transactions, so they cannot take part in
// the transaction the statements form, or roll back with it.
func aloneError(text string) error {
	what := ""
	if isLoadingCommand(text) {
		return errNotAlone
	} else if _, ok, _ := parseIntegrationCommand(text); ok {
		what = "statements on notification integrations"
	} else if _, ok, _ := parseOwnerCommand(text); ok {
		what = "ALTER ... OWNER TO and SHOW OBJECTS"
	} else if _, ok, _ := parseShowObjects(text); ok {
		what = "ALTER ... OWNER TO and SHOW OBJECTS"
	} else if _, ok, _ := parseConnectionCommand(text); ok {
		what = "statements on connections"
	} else if _, ok, _ := parseCloneCommand(text); ok {
		what = "CLONE"
	} else if _, ok, _ := parseRefCommand(text); ok {
		what = "statements on branches and tags"
	} else if _, ok, _ := parseSortCommand(text); ok {
		what = "ALTER TABLE ... SORTED BY"
	} else if _, ok, _ := parseDepthCall(text); ok {
		what = "calls of SYSTEM$CLUSTERING_DEPTH"
	} else {
		return nil
	}
	return pgerror.WithCode(fmt.Errorf("send %s on their own", what), codes.FeatureNotSupported)
}

// runScriptStatement parses, checks and runs one statement of a simple
// query, writing its result to w.
func (s *Server) runScriptStatement(ctx context.Context, text string, w wire.DataWriter) error {
	stmts, err := s.parse(ctx, wire.Query{Query: text, SimpleQuery: true})
	if err != nil {
		return err
	}
	if len(stmts) != 1 {
		// sqllex.Split cut at every semicolon, so this cannot happen.
		return pgerror.WithCode(errors.New("cannot run this statement within several others; send it alone"), codes.FeatureNotSupported)
	}
	ps := stmts[0]
	if cols := ps.Columns(); len(cols) > 0 {
		if err := wire.DefineColumns(w, cols); err != nil {
			return err
		}
	}
	return ps.Run(ctx, w, nil)
}
