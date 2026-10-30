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

	"github.com/akkio-inc/igloo/internal/auth"
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

// replaceTable carries out r and returns the rows the new table was
// made with (0 without AS).
func (s *Server) replaceTable(ctx context.Context, sess *session, r tableReplace) (int64, error) {
	create, _, _ := r.statements()
	exec := func(ctx context.Context, q string) error {
		_, err := sess.conn.ExecContext(ctx, q, nil)
		return err
	}
	res, err := sess.conn.QueryContext(ctx, create, nil)
	if err != nil {
		return 0, err
	}
	rows, err := countOf(res)
	if err != nil {
		_ = exec(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+quoteName(r.staging))
		return 0, err
	}
	// The new table, as made, keeps the columns with policies or tags,
	// which the statement was checked for too.
	have, err := describeTable(ctx, sess, quoteName(r.staging))
	if err == nil {
		err = s.checkReplaceKeeps(ctx, sess, r, have)
	}
	if err != nil {
		_ = exec(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+quoteName(r.staging))
		return 0, err
	}

	// From here on, finish even if the client cancels: stopping between the
	// drop and the rename would leave the table missing.
	if err := s.swapIn(context.WithoutCancel(ctx), sess, r); err != nil {
		return 0, err
	}
	return rows, nil
}

// swapIn drops the table r replaces and renames the new one into place.
//
// DuckDB can report a drop or rename failed that the catalog made all the
// same (see doneAnyway), and the catalog proxy answers that a change's
// outcome is unknown when the catalog does not confirm it. So after a step
// fails, the catalog itself is asked what happened before anything more is
// dropped: once the old table may be gone, the new one may be the only copy
// of either. When the catalog cannot tell, swapIn stops, keeps both names'
// tables, and says where the new one is.
func (s *Server) swapIn(ctx context.Context, sess *session, r tableReplace) error {
	_, drop, rename := r.statements()
	exec := func(ctx context.Context, q string) error {
		_, err := sess.conn.ExecContext(ctx, q, nil)
		return err
	}
	target, terr := s.newTableTarget(ctx, sess, r.target)
	staging := r.staging[len(r.staging)-1]
	// exists asks the catalog, not DuckDB's copy of it, whether the table
	// name is in the target's schema; sure is false when it cannot tell.
	exists := func(name string) (there, sure bool) {
		if s.ddl == nil || terr != nil {
			return false, false
		}
		there, err := s.ddl.TableExists(ctx, target.Schema, name)
		return there, err == nil
	}
	// dropped reports whether the target is gone after a drop that failed
	// with err; when it is not, the drop was refused if refused is set,
	// and may yet happen otherwise.
	dropped := func(err error) (gone, refused bool) {
		there, sure := exists(target.Table)
		return sure && !there, sure && there && !outcomeUnknown(err)
	}
	// unsure is the error when the catalog does not say whether step
	// happened: nothing more is dropped.
	unsure := func(step string, err error, where string) error {
		return fmt.Errorf("%s failed, and the catalog does not say whether it happened: %w; nothing more was dropped, "+
			"and the new table is complete as %s", step, err, where)
	}

	if err := exec(ctx, drop); err != nil {
		switch gone, refused := dropped(err); {
		case refused:
			// The old table is as it was, so the new one goes, as after
			// any failure before the swap.
			_ = exec(ctx, "DROP TABLE IF EXISTS "+quoteName(r.staging))
			return err
		case !gone:
			return unsure("dropping the old table "+quoteName(r.target), err, fmt.Sprintf(
				"%s: once %s is gone, ALTER TABLE %s RENAME TO %s puts it in place",
				quoteName(r.staging), quoteName(r.target), quoteName(r.staging), quoteIdent(r.target[len(r.target)-1])))
		}
		// Dropped all the same.
	}
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = exec(ctx, rename); err == nil {
			return nil
		}
		staged, sure := exists(staging)
		placed, sureToo := exists(target.Table)
		switch {
		case !sure || !sureToo || staged && outcomeUnknown(err):
			// The rename may yet happen.
			return unsure("after dropping the old table, renaming the new one", err,
				quoteName(r.staging)+" or, if the rename went through, as "+quoteName(r.target))
		case !staged && placed:
			// Renamed all the same.
			s.forgetStaging(ctx, sess, r, target)
			return nil
		case !staged:
			return fmt.Errorf("renaming the new table %s to %s failed: %w; neither is in the catalog any more, so another session dropped the new table",
				quoteName(r.staging), quoteName(r.target), err)
		case placed:
			// Another session made a table of the name in between: the
			// last replace wins.
			if derr := exec(ctx, drop); derr != nil {
				if gone, _ := dropped(derr); !gone {
					return renameFailed(r, derr)
				}
			}
		}
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}
	return renameFailed(r, err)
}

// forgetStaging has DuckDB forget the new table's temporary name after a
// rename into place that DuckDB heard fail but the catalog made: DuckDB
// would list the table under it, to every session of the replica, until
// it stops. Its drop of the name drops nothing in the catalog (see
// Engine.ForgetTable). The caller holds the catalog lock exclusively.
func (s *Server) forgetStaging(ctx context.Context, sess *session, r tableReplace, target auth.Object) {
	defer s.engine.ForgetTable(target.Schema, r.staging[len(r.staging)-1], target.Table)()
	if _, err := sess.conn.ExecContext(ctx, "DROP TABLE "+quoteName(r.staging), nil); err != nil {
		s.log.Warn("DuckDB still lists the new table of a CREATE OR REPLACE TABLE under its temporary name, which the catalog renamed",
			"table", target.String(), "name", quoteName(r.staging), "err", err)
	}
}

// renameFailed is swapIn's error when the new table stays under its
// temporary name.
func renameFailed(r tableReplace, err error) error {
	return fmt.Errorf("the old table was dropped and the new table is complete as %s, but renaming it to %s failed: %w",
		quoteName(r.staging), quoteName(r.target), err)
}

// stagedCreate is CREATE [OR REPLACE] TABLE name AS query of an Iceberg
// table, carried out in steps: create the new table, empty, under a
// temporary name; fill it with INSERT INTO it query; rename it into place,
// after dropping the table it replaces. DuckDB runs CREATE TABLE ... AS as
// one statement that holds the catalog lock exclusively (see
// catalogRunLock) for as long as it writes, so a long one, such as a copy
// of a table of a connection's database, stopped every other session on
// the replica from preparing any statement, a SELECT 42 included, or from
// signing in. In steps, only creating, dropping and renaming hold it
// exclusively; filling runs like any INSERT. As with a replace, the table
// appears under its name only once it is complete.
type stagedCreate struct {
	tableReplace
	replace bool
	// target is the table, to check that it does not exist yet.
	target auth.Object
	// query is the query after AS.
	query string
}

// stagedCreateOf returns how to carry out query in steps, or nil when it is
// not CREATE [OR REPLACE] TABLE [IF NOT EXISTS] name AS query of an Iceberg
// table (with no column list, not TEMP and not WITH NO DATA) on its own.
func (s *Server) stagedCreateOf(ctx context.Context, sess *session, query string) *stagedCreate {
	if !createsTableAs(query) {
		return nil
	}
	if n, err := s.engine.CountStatements(query); err != nil || n != 1 {
		return nil
	}
	c, ok := parseCreateTable(query)
	if !ok || c.temporary || c.query == "" || topLevelKeyword(query, "WITH", "NO", "DATA") >= 0 {
		return nil
	}
	target, err := s.newTableTarget(ctx, sess, c.name)
	if err != nil {
		return nil
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	staging := append(append([]string{}, c.name[:len(c.name)-1]...),
		c.name[len(c.name)-1]+policy.StagingMark+hex.EncodeToString(b[:]))
	return &stagedCreate{
		tableReplace: tableReplace{target: c.name, staging: staging, rest: " AS " + c.query},
		replace:      c.replace, target: target, query: c.query,
	}
}

// runStagedCreate carries out c, holding the catalog lock exclusively only
// while it changes the catalog. done is false when c is CREATE TABLE of a
// table or view that exists: the statement then runs as written, under the
// lock, for DuckDB's own error or, with IF NOT EXISTS, its notice.
func (s *Server) runStagedCreate(ctx context.Context, sess *session, c *stagedCreate) (rows int64, done bool, err error) {
	exec := func(ctx context.Context, q string) error {
		_, err := sess.conn.ExecContext(ctx, q, nil)
		return err
	}
	dropStaging := func() {
		release := s.holdCatalog(sess)
		defer release()
		_ = exec(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+quoteName(c.staging))
	}

	release := s.holdCatalog(sess)
	if !c.replace {
		if exists, err := s.relationExists(ctx, sess, c.target); err != nil || exists {
			release()
			return 0, false, nil
		}
	}
	// On a line of its own, after a query that may end in a comment.
	err = exec(ctx, "CREATE TABLE "+quoteName(c.staging)+" AS "+c.query+"\nWITH NO DATA")
	if err == nil && c.replace {
		// The new table keeps the columns with policies or tags, which the
		// statement was checked for too.
		var have []columnType
		if have, err = describeTable(ctx, sess, quoteName(c.staging)); err == nil {
			err = s.checkReplaceKeeps(ctx, sess, c.tableReplace, have)
		}
		if err != nil {
			_ = exec(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+quoteName(c.staging))
		}
	}
	// A table of a member table's name is one too: the new one gets the
	// columns for ids, and is filled with them (see idassign.go).
	var (
		ids     *idWrite
		visible []string
	)
	if err == nil {
		if ids, visible, err = s.stagedIDs(ctx, sess, c); err != nil {
			_ = exec(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+quoteName(c.staging))
		}
	}
	release()
	if err != nil {
		return 0, true, err
	}

	fill := func(q string) (int64, error) {
		res, err := sess.conn.QueryContext(ctx, q, nil)
		if err != nil {
			return 0, err
		}
		return countOf(res)
	}
	plain := "INSERT INTO " + quoteName(c.staging) + "\n" + c.query
	if text, ok := s.joinedInsert(parsedInsert{target: quoteName(c.staging), columns: visible,
		source: strings.TrimRight(c.query, "; \t\r\n")}, idMembersOf(ids)); ok {
		rows, err = fill(text)
		if err != nil && joinFailed(err) {
			s.log.Debug("a new member table is filled without its ids", "table", c.target.String(), "err", err)
			rows, err = fill(plain)
		}
	} else if sw := s.sortedWriteOf(ctx, sess, "INSERT INTO "+quoteName(c.staging), c.query); sw != nil {
		// Too many rows in order to sort at once: a range at a time.
		var done bool
		if rows, done, err = s.runSortedWrite(ctx, sess, sw); !done && err == nil {
			rows, err = fill(plain)
		}
	} else if ap := s.aggPassesOf(ctx, sess, c.query, 0); ap != nil {
		// An aggregate whose states would not fit in memory: its groups a
		// share at a time (see aggpasses.go).
		rows, err = s.fillInPasses(ctx, fill, c.staging, ap)
	} else if rows, err = fill(plain); isOutOfMemory(err) && ctx.Err() == nil {
		// DuckDB's estimate was short: in passes, if the query allows,
		// into the table the failed fill left empty.
		if ap := s.aggPassesOf(ctx, sess, c.query, 16); ap != nil {
			rows, err = s.fillInPasses(ctx, fill, c.staging, ap)
		}
	}
	if err != nil {
		dropStaging()
		return 0, true, err
	}
	if ids != nil && s.idSyncMinRows > 0 && rows < s.idSyncMinRows && s.idDomainsOn {
		// A small table: mapped shortly after, under its own name once it
		// has it.
		defer s.deferIDs(c.target.Schema, c.target.Table)
	} else {
		s.fillIDs(ctx, sess, ids, -1)
	}

	// From here on, finish even if the client cancels: stopping between the
	// drop and the rename would leave the table missing.
	ctx = context.WithoutCancel(ctx)
	release = s.holdCatalog(sess)
	defer release()
	if c.replace {
		return rows, true, s.swapIn(ctx, sess, c.tableReplace)
	}
	_, _, rename := c.statements()
	if err := exec(ctx, rename); err != nil {
		// A table of that name was created in the meantime.
		_ = exec(ctx, "DROP TABLE IF EXISTS "+quoteName(c.staging))
		return 0, true, err
	}
	return rows, true, nil
}

// relationExists reports whether the catalog has a table or view named o,
// matching names as DuckDB does, whatever their case. The caller holds the
// catalog lock exclusively: it lists the catalog's set.
func (s *Server) relationExists(ctx context.Context, sess *session, o auth.Object) (bool, error) {
	if s.ddl != nil && s.ddl.IsView(o.Schema, o.Table) {
		return true, nil
	}
	found, err := queryStrings(ctx, sess, `SELECT table_name FROM duckdb_tables()
		WHERE database_name = $1 AND lower(schema_name) = lower($2) AND lower(table_name) = lower($3)
		UNION ALL SELECT view_name FROM duckdb_views()
		WHERE database_name = $1 AND lower(schema_name) = lower($2) AND lower(view_name) = lower($3)`,
		s.catalogName(), o.Schema, o.Table)
	return len(found) > 0, err
}

// runStatement runs st, retrying lost Iceberg commits (see
// runWithCommitRetry), and carries out CREATE OR REPLACE TABLE on an
// Iceberg table, which DuckDB refuses, CREATE TABLE ... AS of one in
// steps (see stagedCreate), and writes of too many rows in order to sort at
// once a range of the order at a time (see sortedwrite.go).
func (s *Server) runStatement(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w wire.DataWriter) error {
	if st.staged != nil && !sess.inTx {
		rows, done, err := s.runStagedCreate(ctx, sess, st.staged)
		if done {
			if err != nil {
				return err
			}
			reportRows(w, rows)
			return w.Complete("CREATE TABLE")
		}
		// It exists: run it as written, under the lock it would have held.
		defer s.holdCatalog(sess)()
	}
	// An INSERT of too many rows in order to sort at once sorts a range of
	// the order at a time (see sortedwrite.go).
	if done, err := s.runSortedInsert(ctx, sess, st, args, w); done {
		return err
	}
	written := int64(-1)
	err := runWithCommitRetry(ctx, sess, st, func() error {
		if err := st.freshForRun(ctx, sess.conn); err != nil {
			return err
		}
		return sess.profiled(ctx, st, func() error {
			// An INSERT into a member table writes the ids it can.
			done, n, err := s.runWithIDs(ctx, sess, st, args, w)
			if done {
				written = n
				return err
			}
			return st.run(ctx, args, w)
		})
	})
	if err == nil && st.ids != nil {
		// And then those of the values that had none.
		s.fillIDs(ctx, sess, st.ids, written)
	}
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
	rows, err := s.replaceTable(ctx, sess, r)
	if err != nil {
		return err
	}
	if createsTableAs(st.query) {
		reportRows(w, rows)
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

// outcomeUnknown reports whether err says the catalog did not confirm
// whether a change was applied (see package catalogproxy), which it may
// still apply.
func outcomeUnknown(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "commitstateunknownexception")
}
