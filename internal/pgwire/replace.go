package pgwire

import (
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/policy"
)

// icebergNoReplace is the error DuckDB gives for CREATE OR REPLACE TABLE on
// an Iceberg table. Igloo carries those statements out itself.
const icebergNoReplace = "CREATE OR REPLACE not supported in DuckDB-Iceberg"

// tableReplace is a CREATE OR REPLACE TABLE statement, carried out as: create
// the new table under a temporary name, drop the old one, rename the new one
// into place. The slow part, writing the new data, happens before the old
// table is touched, so a failure leaves the old table as it was. The swap is
// two quick catalog operations rather than one atomic one: a query that
// starts between them finds no table.
//
// DuckDB cannot run these steps in one transaction: it applies Iceberg
// catalog changes from a transaction even when its commit then reports
// failure.
type tableReplace struct {
	target  []string // the table's name as written, parts unquoted
	staging []string // the temporary name for the new table
	rest    string   // the statement after the name: column list or AS query
}

// parseTableReplace recognizes CREATE OR REPLACE TABLE name ... (not TEMP).
func parseTableReplace(query string) (tableReplace, bool) {
	sc := &scanner{s: query}
	if !sc.keywords("CREATE", "OR", "REPLACE", "TABLE") {
		return tableReplace{}, false
	}
	name, ok := sc.name()
	if !ok {
		return tableReplace{}, false
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	staging := append(append([]string{}, name[:len(name)-1]...),
		name[len(name)-1]+policy.StagingMark+hex.EncodeToString(b[:]))
	return tableReplace{target: name, staging: staging, rest: sc.rest()}, true
}

// statements returns the create, drop and rename statements.
func (r tableReplace) statements() (create, drop, rename string) {
	create = "CREATE TABLE " + quoteName(r.staging) + r.rest
	drop = "DROP TABLE IF EXISTS " + quoteName(r.target)
	rename = "ALTER TABLE " + quoteName(r.staging) + " RENAME TO " + quoteIdent(r.target[len(r.target)-1])
	return create, drop, rename
}

func (s *Server) replaceTable(ctx context.Context, sess *session, r tableReplace) error {
	create, drop, rename := r.statements()
	exec := func(ctx context.Context, q string) error {
		_, err := sess.conn.ExecContext(ctx, q, nil)
		return err
	}
	if err := exec(ctx, create); err != nil {
		return err
	}
	// The new table, as made, keeps the columns with policies or tags,
	// which the statement was checked for too.
	have, err := queryStrings(ctx, sess, "SELECT column_name FROM (DESCRIBE "+quoteName(r.staging)+")")
	if err == nil {
		err = s.checkReplaceKeeps(ctx, sess, r, have)
	}
	if err != nil {
		_ = exec(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+quoteName(r.staging))
		return err
	}

	// From here on, finish even if the client cancels: stopping between the
	// drop and the rename would leave the table missing.
	ctx = context.WithoutCancel(ctx)
	if err := exec(ctx, drop); err != nil {
		_ = exec(ctx, "DROP TABLE IF EXISTS "+quoteName(r.staging))
		return err
	}
	for attempt := 0; attempt < 3; attempt++ {
		if err = exec(ctx, rename); err == nil {
			return nil
		}
		// Another session may have replaced the table in between; the last
		// replace wins.
		_ = exec(ctx, drop)
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}
	return fmt.Errorf("the new table is complete as %s, but renaming it to %s failed: %w",
		quoteName(r.staging), quoteName(r.target), err)
}

// runStatement runs st, retrying lost Iceberg commits (see
// runWithCommitRetry), and carries out CREATE OR REPLACE TABLE on an
// Iceberg table, which DuckDB refuses.
func (s *Server) runStatement(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w wire.DataWriter) error {
	err := runWithCommitRetry(ctx, sess, st, func() error {
		return sess.profiled(ctx, st, func() error { return st.run(ctx, args, w) })
	})
	if err != nil && st.changesIceberg && s.doneAnyway(ctx, st, err) {
		return w.Complete(commandTag(st.stmtType, st.query, 0))
	}
	if !isIcebergNoReplace(err) {
		return err
	}
	if sess.inTx {
		// Statements that change the catalog run outside the block (see
		// outsideTransaction); this one was not recognized as one.
		return pgerror.WithCode(errors.New("CREATE OR REPLACE TABLE on an Iceberg table cannot run inside a transaction block"),
			codes.ActiveSQLTransaction)
	}
	r, ok := s.replaceable(sess, st)
	if !ok {
		return err
	}
	if err := s.replaceTable(ctx, sess, r); err != nil {
		return err
	}
	return w.Complete("CREATE TABLE")
}

// doneAnyway reports whether a rename or drop of an Iceberg table that
// DuckDB reported failed did in fact happen, as the catalog shows. DuckDB
// keeps one list of each schema's tables for all sessions, and a session
// that lists the schema while another renames or drops one of its tables
// can make DuckDB fail the change after the catalog has made it: a rename
// fails with "was already created by a different transaction" once
// committed, and DROP TABLE IF EXISTS of a table already gone with
// NoSuchTableException. dbt, with several threads, renames and drops
// tables in one schema at once.
func (s *Server) doneAnyway(ctx context.Context, st *statement, err error) bool {
	msg := err.Error()
	e := st.effect
	if s.ddl == nil || e == nil || len(e.objects) == 0 {
		return false
	}
	exists := func(schema, name string) bool {
		ok, err := s.ddl.TableExists(ctx, schema, name)
		return err != nil || ok // when in doubt, the error stands
	}
	switch {
	case e.renameTo != "" && strings.Contains(msg, "was already created by a different transaction"):
		for _, o := range e.objects {
			if exists(o.Schema, o.Table) || !exists(o.Schema, e.renameTo) {
				return false
			}
		}
		return true
	case st.stmtType == duckdb.STATEMENT_TYPE_DROP && strings.Contains(msg, "NoSuchTableException"):
		sc := &scanner{s: st.query}
		if !sc.keywords("DROP", "TABLE", "IF", "EXISTS") {
			return false
		}
		for _, o := range e.objects {
			if exists(o.Schema, o.Table) {
				return false
			}
		}
		return true
	}
	return false
}

// replaceable returns how to carry out st, which DuckDB refused as CREATE
// OR REPLACE TABLE on an Iceberg table. Only a single statement qualifies.
// The caller must make sure no transaction is open: DuckDB runs Iceberg
// catalog changes in a transaction unreliably (see tableReplace).
func (s *Server) replaceable(sess *session, st *statement) (tableReplace, bool) {
	if st.stmtType != duckdb.STATEMENT_TYPE_CREATE {
		return tableReplace{}, false
	}
	if n, err := s.engine.CountStatements(st.query); err != nil || n != 1 {
		return tableReplace{}, false
	}
	return parseTableReplace(st.query)
}

// isIcebergNoReplace reports whether err is DuckDB refusing CREATE OR
// REPLACE TABLE on an Iceberg table.
func isIcebergNoReplace(err error) bool {
	return err != nil && strings.Contains(err.Error(), icebergNoReplace)
}
