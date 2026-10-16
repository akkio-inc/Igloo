package pgwire

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
)

// Transaction blocks behave as in PostgreSQL where DuckDB differs, which
// tools such as dbt (through psycopg2) and JDBC rely on:
//
//   - BEGIN inside a block, and COMMIT or ROLLBACK outside one, only warn.
//     DuckDB refuses them.
//   - Once a statement in a block fails, every statement but COMMIT and
//     ROLLBACK fails with SQLSTATE 25P02 until the block ends, and COMMIT
//     rolls the block back. DuckDB aborts a transaction on some errors only,
//     and would commit the rest of the block.
//
// And as in Snowflake, a statement that creates, alters, renames, drops or
// comments on a schema, table or view in the Iceberg catalog commits the
// block first and runs on its own, after which a new transaction carries the
// block on. DuckDB cannot run several such changes in one transaction: it
// cannot rename a table created in the same transaction, and it applies
// catalog changes even when their commit fails. Views and comments are not
// DuckDB's to begin with. Temporary tables and views stay in the block.

// Savepoints are only marks: DuckDB cannot undo part of a transaction.
// SAVEPOINT and RELEASE always work, which is what psycopg needs for its
// nested transaction() blocks and SQLAlchemy when it connects. ROLLBACK TO
// SAVEPOINT works when nothing has changed since the savepoint, so it can
// recover from a failed query; after a change it fails, and the client must
// roll the whole transaction back.

// savepointStatement matches SAVEPOINT, RELEASE [SAVEPOINT] and ROLLBACK
// [WORK | TRANSACTION] TO [SAVEPOINT], and the savepoint's name.
var savepointStatement = regexp.MustCompile(`(?is)^\s*(SAVEPOINT|RELEASE(?:\s+SAVEPOINT)?|ROLLBACK\s+(?:(?:WORK|TRANSACTION)\s+)?TO(?:\s+SAVEPOINT)?)\s+` +
	cursorName + `\s*;?\s*$`)

// savepoint is a mark in a transaction block, with how many statements had
// changed something in the block when it was set.
type savepoint struct {
	name    string
	changes int
}

// savepointCommand answers SAVEPOINT, RELEASE and ROLLBACK TO SAVEPOINT.
func (s *Server) savepointCommand(sess *session, query string) (wire.PreparedStatements, bool) {
	m := savepointStatement.FindStringSubmatch(query)
	if m == nil {
		return nil, false
	}
	verb, name := strings.ToUpper(strings.Fields(m[1])[0]), unquoteName(m[2])
	return wire.Prepared(wire.NewStatement(s.audited(sess, query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		if !sess.inTx {
			return pgerror.WithCode(fmt.Errorf("%s can only be used in transaction blocks", m[1]), codes.NoActiveSQLTransaction)
		}
		if verb == "SAVEPOINT" {
			sess.savepoints = append(sess.savepoints, savepoint{name: name, changes: sess.changes})
			return w.Complete("SAVEPOINT")
		}
		i := len(sess.savepoints) - 1
		for i >= 0 && sess.savepoints[i].name != name {
			i--
		}
		if i < 0 {
			return pgerror.WithCode(fmt.Errorf("savepoint %q does not exist", name), codes.InvalidSavepointSpecification)
		}
		if verb == "RELEASE" {
			sess.savepoints = sess.savepoints[:i]
			return w.Complete("RELEASE")
		}
		sess.savepoints = sess.savepoints[:i+1]
		if err := sess.rollbackTo(ctx, sess.savepoints[i]); err != nil {
			sess.txFailed = true
			return err
		}
		return w.Complete("ROLLBACK")
	}))), true
}

// rollbackTo returns the block to sp, which it can only do when nothing has
// changed since: then it clears the block's failure, if any.
func (sess *session) rollbackTo(ctx context.Context, sp savepoint) error {
	notSupported := pgerror.WithCode(errors.New("cannot roll back to a savepoint after the transaction changed data; roll back the transaction"),
		codes.FeatureNotSupported)
	if sess.changes > sp.changes {
		return notSupported
	}
	if sess.txFailed && sess.exec(ctx, "SELECT 1") != nil {
		// DuckDB aborted its transaction, which can be started again only
		// if it changed nothing at all.
		if sess.changes > 0 {
			return notSupported
		}
		if err := errors.Join(sess.exec(ctx, "ROLLBACK"), sess.exec(ctx, "BEGIN")); err != nil {
			return err
		}
	}
	sess.txFailed = false
	return nil
}

// errAborted is PostgreSQL's error for a statement in a failed block.
var errAborted = pgerror.WithCode(errors.New("current transaction is aborted, commands ignored until end of transaction block"),
	codes.InFailedSQLTransaction)

// checkAborted refuses query while the session's block has failed, unless
// it ends the block.
func (sess *session) checkAborted(query string) error {
	if sess.inTx && sess.txFailed && !isTransactionEnd(query) {
		return errAborted
	}
	return nil
}

// redundantTransaction recognizes BEGIN inside a block and COMMIT or
// ROLLBACK outside one, which PostgreSQL answers with a warning and the
// usual command tag. ok is false for every other statement.
func (sess *session) redundantTransaction(query string) (tag string, code codes.Code, warning string, ok bool) {
	switch change := transactionChange(query); {
	case change > 0 && sess.inTx:
		code, warning = codes.ActiveSQLTransaction, "there is already a transaction in progress"
	case change < 0 && !sess.inTx:
		code, warning = codes.NoActiveSQLTransaction, "there is no transaction in progress"
	default:
		return "", "", "", false
	}
	return transactionTag(query), code, warning, true
}

// runTransaction runs a transaction statement st, writing its tag to w.
// It handles the cases above and leaves the rest to run.
func (sess *session) runTransaction(ctx context.Context, st *statement, w wire.DataWriter, run func() error) error {
	if tag, code, warning, ok := sess.redundantTransaction(st.query); ok {
		if n, ok := w.(wire.Noticer); ok {
			if err := n.Notice(string(code), warning); err != nil {
				return err
			}
		}
		return w.Complete(tag)
	}
	if sess.txFailed && transactionTag(st.query) == "COMMIT" {
		if err := sess.exec(ctx, "ROLLBACK"); err != nil && !noTransaction(err) {
			return err
		}
		return w.Complete("ROLLBACK")
	}
	return run()
}

// outsideTransaction runs fn, which changes the Iceberg catalog, outside
// the session's transaction block: it commits the block, runs fn on its
// own and opens a new transaction to carry the block on. Outside a block it
// just runs fn.
func (sess *session) outsideTransaction(ctx context.Context, fn func() error) error {
	if !sess.inTx {
		return fn()
	}
	err := sess.exec(ctx, "COMMIT")
	if err == nil {
		sess.inTx = false
		err = fn()
		sess.inTx = true
	}
	sess.savepoints, sess.changes = nil, 0
	// After a failed COMMIT, DuckDB has rolled back; either way the client's
	// block carries on, and fails if err is set.
	if berr := sess.exec(context.WithoutCancel(ctx), "BEGIN"); berr != nil {
		return errors.Join(err, berr)
	}
	return err
}

// changesIceberg reports whether a statement of type t creates, alters or
// drops a schema, table or view in the Iceberg catalog, rather than a
// temporary or local one. Views and comments in the catalog go through
// catalogDDL instead.
func (s *Server) changesIceberg(ctx context.Context, sess *session, t duckdb.StmtType, query string) bool {
	catalogName := s.catalogName()
	if catalogName == "" {
		return false
	}
	sc := &scanner{s: query}
	var name []string
	switch t {
	case duckdb.STATEMENT_TYPE_CREATE:
		if c, ok := parseCreateTable(query); ok {
			if c.temporary {
				return false
			}
			_, err := s.newTableTarget(ctx, sess, c.name)
			return err == nil
		}
		if !sc.keywords("CREATE", "SCHEMA") {
			return false
		}
		sc.keywords("IF", "NOT", "EXISTS")
		return s.namesCatalogSchema(ctx, sess, sc)
	case duckdb.STATEMENT_TYPE_DROP:
		switch {
		case sc.keywords("DROP", "SCHEMA"):
			sc.keywords("IF", "EXISTS")
			return s.namesCatalogSchema(ctx, sess, sc)
		case sc.keywords("DROP", "TABLE"), sc.keywords("DROP", "VIEW"):
			sc.keywords("IF", "EXISTS")
			name, _ = sc.name()
		}
	case duckdb.STATEMENT_TYPE_ALTER:
		if sc.keywords("ALTER", "TABLE") || sc.keywords("ALTER", "VIEW") {
			sc.keywords("IF", "EXISTS")
			name, _ = sc.name()
		}
	}
	if name == nil {
		return false
	}
	objects, err := s.catalogCandidates(ctx, sess, name, false)
	return err == nil && len(objects) > 0
}

// withoutIcebergCascade drops CASCADE from DROP TABLE of an Iceberg table,
// which DuckDB refuses and dbt sends whenever it replaces a table. The
// catalog records nothing that depends on a table for CASCADE to drop:
// views that read it fail when next queried, as after a plain DROP.
func (s *Server) withoutIcebergCascade(ctx context.Context, sess *session, query string) string {
	sc := &scanner{s: query}
	if !sc.keywords("DROP", "TABLE") {
		return query
	}
	sc.keywords("IF", "EXISTS")
	name, ok := sc.name()
	if !ok {
		return query
	}
	end := sc.i
	if !sc.keywords("CASCADE") || !sc.done() {
		return query
	}
	if objects, err := s.catalogCandidates(ctx, sess, name, false); err != nil || len(objects) == 0 {
		return query
	}
	return query[:end]
}

// namesCatalogSchema reports whether the schema name sc is at names one in
// the Iceberg catalog: catalog.schema, or schema while the session's
// current database is the catalog.
func (s *Server) namesCatalogSchema(ctx context.Context, sess *session, sc *scanner) bool {
	name, ok := sc.name()
	switch {
	case !ok || len(name) > 2:
		return false
	case len(name) == 2:
		return strings.EqualFold(name[0], s.catalogName())
	}
	db, _, err := currentSchema(ctx, sess.conn)
	return err == nil && strings.EqualFold(db, s.catalogName())
}

// exec runs a statement of Igloo's own on the session's connection.
func (sess *session) exec(ctx context.Context, query string) error {
	_, err := sess.conn.ExecContext(ctx, query, nil)
	return err
}

// noTransaction reports whether err is DuckDB finding no transaction to
// end, which it has already ended itself after some errors.
func noTransaction(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no transaction is active")
}
