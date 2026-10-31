package pgwire

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"slices"
	"strings"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Long statements and the catalog lock. A statement whose run holds the
// catalog lock exclusively (see catalogRunLock) stops every other session
// from preparing a statement, binding one, listing the catalog or signing
// in until it ends. So the statements that hold it to change the catalog,
// but run a query as long as any read besides, do so in steps that hold
// the lock only for the change: CREATE TABLE ... AS of an Iceberg table
// (stagedCreate), and of a temporary table, as dbt makes one of an
// incremental model's new rows (tempCreate). EXECUTE of a statement
// prepared in SQL holds it only when the statement would (executeHolds).

// tempCreate is CREATE [OR REPLACE] TEMP TABLE name AS query, carried out
// in steps: create the table, empty, holding the catalog lock; fill it with
// INSERT, which holds it only while DuckDB binds it. A replace fills a new
// table under another name and renames it into place, so a query that fails
// leaves the table it replaces.
type tempCreate struct {
	name    string
	replace bool
	query   string
}

// tempCreateOf returns how to carry out query in steps, or nil when it is
// not CREATE [OR REPLACE] TEMP TABLE name AS query, one statement, of an
// unqualified name, without IF NOT EXISTS or WITH NO DATA.
func (s *Server) tempCreateOf(query string) *tempCreate {
	if !createsTableAs(query) {
		return nil
	}
	c, ok := parseCreateTable(query)
	if !ok || !c.temporary || c.ifNotExists || c.query == "" || len(c.name) != 1 || c.partitionedBy != "" ||
		topLevelKeyword(query, "WITH", "NO", "DATA") >= 0 {
		return nil
	}
	if n, err := s.engine.CountStatements(query); err != nil || n != 1 {
		return nil
	}
	return &tempCreate{name: c.name[0], replace: c.replace, query: c.query}
}

// runTempCreate carries out c and returns the rows it wrote.
func (s *Server) runTempCreate(ctx context.Context, sess *session, c *tempCreate) (int64, error) {
	exec := func(ctx context.Context, q string) error {
		_, err := sess.conn.ExecContext(ctx, q, nil)
		return err
	}
	held := func(ctx context.Context, stmts ...string) error {
		defer s.holdCatalog(sess)()
		for _, q := range stmts {
			if err := exec(ctx, q); err != nil {
				return err
			}
		}
		return nil
	}
	name := c.name
	if c.replace {
		var b [4]byte
		_, _ = rand.Read(b[:])
		name += "__igloo_fill_" + hex.EncodeToString(b[:])
	}
	into := "temp.main." + quoteIdent(name)
	// On a line of its own, after a query that may end in a comment.
	if err := held(ctx, "CREATE TEMP TABLE "+quoteIdent(name)+" AS "+c.query+"\nWITH NO DATA"); err != nil {
		return 0, err
	}
	res, err := sess.conn.QueryContext(ctx, "INSERT INTO "+into+"\n"+c.query, nil)
	var rows int64
	if err == nil {
		rows, err = countOf(res)
	}
	if err != nil {
		_ = held(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+into)
		return 0, err
	}
	if c.replace {
		if err := held(ctx, "DROP TABLE IF EXISTS temp.main."+quoteIdent(c.name), "ALTER TABLE "+into+" RENAME TO "+quoteIdent(c.name)); err != nil {
			_ = held(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+into)
			return 0, err
		}
	}
	return rows, nil
}

// deallocates matches DEALLOCATE [PREPARE] name | ALL.
var deallocates = regexp.MustCompile(`(?is)^\s*DEALLOCATE\b`)

// executeHolds reports whether EXECUTE query, of a statement prepared in
// SQL, holds the catalog lock for its run: unless the session prepared
// that statement and it is a query or a change of rows, which bind as they
// run, as any other does (see catalogRunLock).
func (sess *session) executeHolds(query string) bool {
	sc := &scanner{s: query}
	if !sc.keywords("EXECUTE") {
		return true
	}
	name, ok := sc.name()
	if !ok || len(name) != 1 {
		return true
	}
	holds, known := sess.sqlPrepared[strings.ToLower(name[0])]
	return !known || holds
}

// notePrepared records what EXECUTE of a statement the session prepared
// in SQL holds (see executeHolds), once query has run: PREPARE name AS
// statement, or DEALLOCATE [PREPARE] name | ALL.
func (sess *session) notePrepared(query string) {
	sc := &scanner{s: query}
	switch {
	case sc.keywords("PREPARE"):
		name, ok := sc.name()
		if !ok || len(name) != 1 {
			return
		}
		sc.skip()
		if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
			end := matchParen(sc.s, sc.i)
			if end < 0 {
				return
			}
			sc.i = end + 1
		}
		if !sc.keywords("AS") {
			return
		}
		if sess.sqlPrepared == nil {
			sess.sqlPrepared = map[string]bool{}
		}
		sess.sqlPrepared[strings.ToLower(name[0])] = preparedHolds(sc.rest())
	case sc.keywords("DEALLOCATE"):
		sc.keywords("PREPARE")
		if sc.keywords("ALL") {
			sess.sqlPrepared = nil
			return
		}
		if name, ok := sc.name(); ok && len(name) == 1 {
			delete(sess.sqlPrepared, strings.ToLower(name[0]))
		}
	}
}

// preparedHolds reports whether a run of body, a statement prepared in
// SQL, holds the catalog lock: unless it is a query or a change of rows
// that lists nothing of the catalog.
func preparedHolds(body string) bool {
	words := sqllex.CodeWords(body)
	if len(words) == 0 {
		return true
	}
	rows := startsQuery(body)
	switch words[0] {
	case "WITH", "VALUES", "TABLE", "INSERT", "UPDATE", "DELETE", "MERGE":
		rows = true
	}
	if !rows || listsCatalog.MatchString(body) {
		return true
	}
	return slices.ContainsFunc(words, func(w string) bool {
		switch w {
		case "CREATE", "DROP", "ALTER", "ATTACH", "DETACH", "COPY", "PREPARE", "EXECUTE":
			return true
		}
		return false
	})
}
