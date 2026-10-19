package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
)

// SQL cursors: DECLARE name CURSOR FOR query, FETCH and CLOSE. psycopg's
// server-side cursors use them, and through those Ibis, SQLAlchemy's
// stream_results and pandas' chunked reads. DuckDB has no cursors, so Igloo
// keeps each one's result on the session.

// cursorName is a bare or "quoted" name.
const cursorName = `("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)`

var (
	declareCursor = regexp.MustCompile(`(?is)^\s*DECLARE\s+` + cursorName +
		`\s+(?:BINARY\s+)?(?:(?:ASENSITIVE|INSENSITIVE)\s+)?(?:NO\s+)?(?:SCROLL\s+)?CURSOR\s+(?:(WITH|WITHOUT)\s+HOLD\s+)?FOR\s+(.+?)\s*;?\s*$`)
	fetchCursor = regexp.MustCompile(`(?is)^\s*FETCH\s+(?:(FORWARD|NEXT)\s+)?(ALL|\d+)?\s*(?:(?:FROM|IN)\s+)?` + cursorName + `\s*;?\s*$`)
	closeCursor = regexp.MustCompile(`(?is)^\s*CLOSE\s+` + cursorName + `\s*;?\s*$`)
)

// cursor is a declared cursor and its remaining rows.
type cursor struct {
	st   *statement
	rows driver.Rows
	// hold keeps the cursor open after the transaction that declared it.
	hold bool
	done bool
}

func (c *cursor) close() {
	if c.rows != nil {
		_ = c.rows.Close()
	}
	_ = c.st.close()
}

func unquoteName(s string) string {
	if strings.HasPrefix(s, `"`) {
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	}
	return strings.ToLower(s)
}

// cursorCommand handles DECLARE, FETCH and CLOSE.
func (s *Server) cursorCommand(ctx context.Context, sess *session, user auth.User, q wire.Query) (wire.PreparedStatements, bool, error) {
	if m := declareCursor.FindStringSubmatch(q.Query); m != nil {
		ps, err := s.declareCursor(ctx, sess, user, unquoteName(m[1]), strings.EqualFold(m[2], "WITH"), m[3], q.ParameterOIDs, q.Query)
		return ps, true, err
	}
	if m := fetchCursor.FindStringSubmatch(q.Query); m != nil && !strings.EqualFold(m[3], "ALL") {
		name := unquoteName(m[3])
		c, ok := sess.cursors[name]
		if !ok {
			return nil, true, pgerror.WithCode(fmt.Errorf("cursor %q does not exist", name), codes.InvalidCursorName)
		}
		limit := -1
		switch {
		case strings.EqualFold(m[2], "ALL"):
		case m[2] != "":
			limit, _ = strconv.Atoi(m[2])
		default:
			limit = 1
		}
		return wire.Prepared(wire.NewStatement(s.audited(sess, q.Query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			n, err := c.fetch(w, limit)
			if err != nil {
				return pgError(err)
			}
			return w.Complete(fmt.Sprintf("FETCH %d", n))
		}), wire.WithColumns(c.st.columns))), true, nil
	}
	if m := closeCursor.FindStringSubmatch(q.Query); m != nil {
		all := strings.EqualFold(m[1], "ALL")
		name := unquoteName(m[1])
		return wire.Prepared(wire.NewStatement(s.audited(sess, q.Query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if all {
				sess.closeCursors(ctx, true)
				return w.Complete("CLOSE CURSOR ALL")
			}
			if _, ok := sess.cursors[name]; !ok {
				return pgerror.WithCode(fmt.Errorf("cursor %q does not exist", name), codes.InvalidCursorName)
			}
			sess.closeCursor(ctx, name)
			return w.Complete("CLOSE CURSOR")
		}))), true, nil
	}
	return nil, false, nil
}

// text is the whole DECLARE statement, for the history.
func (s *Server) declareCursor(ctx context.Context, sess *session, user auth.User, name string, hold bool, query string, oids []uint32, text string) (wire.PreparedStatements, error) {
	st, err := s.prepareStatement(ctx, sess, user, query, oids)
	if err != nil {
		return nil, err
	}
	if st.kind != returnsRows {
		_ = st.close()
		return nil, pgerror.WithCode(errors.New("a cursor needs a query that returns rows"), codes.InvalidCursorDefinition)
	}

	var ps *wire.PreparedStatement
	ps = wire.NewStatement(s.audited(sess, text, func(ctx context.Context, w wire.DataWriter, params []wire.Parameter) error {
		if _, ok := sess.cursors[name]; ok {
			return pgerror.WithCode(fmt.Errorf("cursor %q already exists", name), codes.DuplicateCursor)
		}
		args, err := decodeParams(params, st.params)
		if err != nil {
			return pgerror.WithCode(err, codes.InvalidTextRepresentation)
		}
		// Rechecking and preparing the cursor's own statement both read and
		// fill in the catalog's set (see holdCatalog).
		relist := s.holdCatalog(sess)
		err = s.recheck(ctx, sess, st)
		var own *statement
		if err == nil {
			// The cursor gets a statement of its own, which lives as long as
			// it does; the one prepared here is released with the DECLARE.
			own, err = prepare(ctx, sess.conn, st.query, st.params)
		}
		relist()
		if err != nil {
			return pgError(err)
		}
		start := time.Now()
		var rows driver.Rows
		releaseCatalog := s.catalogRunLock(sess, st)
		err = sess.runCancelable(ctx, query, func(ctx context.Context) error {
			var err error
			rows, err = own.stmt.QueryContext(ctx, args)
			return err
		})
		releaseCatalog()
		s.metrics.query(start, err)
		if err != nil {
			_ = own.close()
			return pgError(err)
		}
		if sess.cursors == nil {
			sess.cursors = map[string]*cursor{}
		}
		sess.cursors[name] = &cursor{st: own, rows: rows, hold: hold}
		// psycopg describes the cursor as a protocol-level portal of the
		// same name, as PostgreSQL allows.
		sess.describePortal(ctx, name, own.columns)
		return w.Complete("DECLARE CURSOR")
	}), wire.WithParameters(st.params))
	sess.open[ps] = st
	return wire.Prepared(ps), nil
}

// fetch writes up to limit rows (all if limit < 0) and returns how many.
func (c *cursor) fetch(w wire.DataWriter, limit int) (int, error) {
	if c.done {
		return 0, nil
	}
	b := newRowBatcher(w, c.st.columns)
	dest := make([]driver.Value, len(c.st.columns))
	n := 0
	for limit < 0 || n < limit {
		err := c.rows.Next(dest)
		if errors.Is(err, io.EOF) {
			c.done = true
			break
		}
		if err != nil {
			return n, err
		}
		if err := b.add(dest, c.st.encoders); err != nil {
			return n, err
		}
		n++
	}
	return n, b.flush()
}

// closeCursor closes one cursor.
func (sess *session) closeCursor(ctx context.Context, name string) {
	if c, ok := sess.cursors[name]; ok {
		c.close()
		delete(sess.cursors, name)
		if sess.portals != nil {
			_ = sess.portals.Delete(ctx, name)
		}
	}
}

// closeCursors closes the session's cursors: all of them, or only those
// without WITH HOLD, which a transaction's end closes.
func (sess *session) closeCursors(ctx context.Context, all bool) {
	for name, c := range sess.cursors {
		if all || !c.hold {
			sess.closeCursor(ctx, name)
		}
	}
}

// describePortal registers a portal named name with columns, so a client's
// Describe of the cursor answers. It cannot be executed; FETCH reads the
// cursor.
func (sess *session) describePortal(ctx context.Context, name string, columns wire.Columns) {
	if sess.portals == nil {
		return
	}
	cache := wire.DefaultStatementCacheFn()
	err := cache.Set(ctx, "", wire.NewStatement(func(context.Context, wire.DataWriter, []wire.Parameter) error {
		return pgerror.WithCode(fmt.Errorf("use FETCH to read cursor %q", name), codes.InvalidCursorState)
	}, wire.WithColumns(columns)))
	if err != nil {
		return
	}
	if stmt, err := cache.Get(ctx, ""); err == nil && stmt != nil {
		_ = sess.portals.Bind(ctx, name, stmt, nil, nil)
	}
}

// portalCache records itself on the session, so cursors can register
// portals.
type portalCache struct {
	wire.PortalCache
}

func newPortalCache() wire.PortalCache {
	return &portalCache{PortalCache: wire.DefaultPortalCacheFn()}
}

func (c *portalCache) Bind(ctx context.Context, name string, stmt *wire.Statement, params []wire.Parameter, formats []wire.FormatCode) error {
	if sess := sessionFrom(ctx); sess != nil && sess.portals == nil {
		sess.portals = c
	}
	return c.PortalCache.Bind(ctx, name, stmt, params, formats)
}
