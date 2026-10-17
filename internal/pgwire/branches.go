package pgwire

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Branches and tags of tables (Iceberg snapshot references):
//
//	ALTER TABLE t CREATE [OR REPLACE] BRANCH [IF NOT EXISTS] name [AT (...)]
//	ALTER TABLE t CREATE [OR REPLACE] TAG [IF NOT EXISTS] name [AT (...)]
//	ALTER TABLE t DROP BRANCH [IF EXISTS] name
//	ALTER TABLE t DROP TAG [IF EXISTS] name
//	ALTER TABLE t FAST FORWARD branch TO other
//	SHOW BRANCHES ON t
//	SELECT ... FROM t AT (BRANCH => 'name') / AT (TAG => 'name')
//
// DuckDB writes only to main, so Igloo's branches are read-only: other
// engines can write to them, and FAST FORWARD then moves main ahead to what
// they wrote. Reading a branch or tag is reading the snapshot it names
// (DuckDB's AT (VERSION => id)), so grants and policies apply as to the
// table.

// refCommand is a statement on a table's branches and tags.
type refCommand struct {
	// verb is CREATE, DROP, FAST FORWARD or SHOW.
	verb                           string
	table                          []string
	kind                           table.RefType
	name                           string
	replace, ifNotExists, ifExists bool
	at                             *atClause
	// to is the branch FAST FORWARD moves name to.
	to string
}

var refColumns = wire.Columns{
	{Name: "name", Oid: pgtype.TextOID, Width: -1},
	{Name: "kind", Oid: pgtype.TextOID, Width: -1},
	{Name: "snapshot_id", Oid: pgtype.Int8OID, Width: 8},
	{Name: "committed_at", Oid: pgtype.TimestamptzOID, Width: 8},
}

// parseRefCommand recognizes the statements above but the SELECT. ok is
// false for any other statement.
func parseRefCommand(query string) (c refCommand, ok bool, err error) {
	sc := &scanner{s: query}
	refName := func() (string, error) {
		n, ok := sc.name()
		if !ok || len(n) != 1 {
			return "", cloneSyntax("expected the name of a branch or tag")
		}
		return n[0], nil
	}
	if sc.keywords("SHOW", "BRANCHES", "ON") {
		sc.keywords("TABLE")
		c.verb = "SHOW"
		if c.table, ok = sc.name(); !ok || !sc.done() {
			return c, true, cloneSyntax("expected SHOW BRANCHES ON table")
		}
		return c, true, nil
	}
	if !sc.keywords("ALTER", "TABLE") {
		return c, false, nil
	}
	if c.table, ok = sc.alterTarget(); !ok {
		return c, false, nil
	}
	switch {
	case sc.keywords("CREATE"):
		c.verb = "CREATE"
		c.replace = sc.keywords("OR", "REPLACE")
		switch {
		case sc.keywords("BRANCH"):
			c.kind = table.BranchRef
		case sc.keywords("TAG"):
			c.kind = table.TagRef
		default:
			return c, false, nil
		}
		c.ifNotExists = sc.keywords("IF", "NOT", "EXISTS")
		if c.replace && c.ifNotExists {
			return c, true, cloneSyntax("OR REPLACE and IF NOT EXISTS cannot both be used")
		}
		if c.name, err = refName(); err != nil {
			return c, true, err
		}
		if sc.keywords("AT") {
			at, err := parseAt(sc)
			if err != nil {
				return c, true, err
			}
			c.at = &at
		}
	case sc.keywords("DROP", "BRANCH"):
		c.verb, c.kind = "DROP", table.BranchRef
	case sc.keywords("DROP", "TAG"):
		c.verb, c.kind = "DROP", table.TagRef
	case sc.keywords("FAST", "FORWARD"):
		c.verb, c.kind = "FAST FORWARD", table.BranchRef
		if c.name, err = refName(); err != nil {
			return c, true, err
		}
		if !sc.keywords("TO") {
			return c, true, cloneSyntax("expected FAST FORWARD branch TO other_branch")
		}
		if c.to, err = refName(); err != nil {
			return c, true, err
		}
	default:
		return c, false, nil
	}
	if c.verb == "DROP" {
		c.ifExists = sc.keywords("IF", "EXISTS")
		if c.name, err = refName(); err != nil {
			return c, true, err
		}
	}
	if !sc.done() {
		return c, true, cloneSyntax("unexpected %q in ALTER TABLE ... %s", strings.TrimSpace(sc.rest()), c.verb)
	}
	return c, true, nil
}

// refStatement carries out a statement on branches and tags. ok is false
// for any other statement.
func (s *Server) refStatement(sess *session, q wire.Query) (wire.PreparedStatements, bool, error) {
	c, ok, err := parseRefCommand(q.Query)
	if !ok {
		return nil, false, nil
	}
	if err == nil && s.clones == nil {
		err = pgerror.WithCode(errors.New("branches and tags need the Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
	}
	if err != nil {
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	var opts []wire.PreparedOptionFn
	if c.verb == "SHOW" {
		opts = append(opts, wire.WithColumns(refColumns))
	}
	return wire.Prepared(wire.NewStatement(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		var acc *access
		err := sess.runCancelable(ctx, q.Query, func(ctx context.Context) error {
			var err error
			acc, err = s.runRefCommand(ctx, sess, c, w)
			return err
		})
		s.metrics.query(start, err)
		s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: -1, acc: acc})
		return pgError(err)
	}, opts...)), true, nil
}

func (s *Server) runRefCommand(ctx context.Context, sess *session, c refCommand, w wire.DataWriter) (*access, error) {
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return nil, err
	}
	o, err := s.cloneSource(ctx, sess, c.table)
	if err != nil {
		return nil, err
	}
	ident := identOf(o)
	if c.verb == "SHOW" {
		acc := &access{read: []auth.Object{o}}
		if err := s.authorizeReads(ctx, sess, user, acc.read); err != nil {
			return acc, err
		}
		refs, err := s.clones.Refs(ctx, ident)
		if err != nil {
			return acc, cloneError(err)
		}
		for _, r := range refs {
			var at any
			if !r.Time.IsZero() {
				at = r.Time
			}
			if err := w.Row([]any{r.Name, string(r.Kind), r.SnapshotID, at}); err != nil {
				return acc, err
			}
		}
		return acc, w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
	}

	acc := &access{written: []auth.Object{o}}
	if err := authorize(user.Role, duckdb.STATEMENT_TYPE_ALTER, "ALTER TABLE"); err != nil {
		if err = s.authorizeSchemaCreate(ctx, sess, user, o.Schema, err); err != nil {
			s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
			return acc, err
		}
	}
	if sess.inTx {
		return acc, pgerror.WithCode(fmt.Errorf("ALTER TABLE ... %s cannot run inside a transaction block", c.verb), codes.ActiveSQLTransaction)
	}
	switch c.verb {
	case "CREATE":
		at, err := s.cloneAt(ctx, sess, user, c.at)
		if err == nil {
			err = s.clones.CreateRef(ctx, ident, c.name, c.kind, at, c.replace, c.ifNotExists)
		}
		if err != nil {
			return acc, cloneError(err)
		}
	case "DROP":
		if err := s.clones.DropRef(ctx, ident, c.name, c.kind, c.ifExists); err != nil {
			return acc, cloneError(err)
		}
	case "FAST FORWARD":
		err := s.clones.FastForward(ctx, ident, c.name, c.to)
		s.ddlGen.Add(1)
		if err != nil {
			return acc, cloneError(err)
		}
	}
	s.log.Info("table changed", "statement", "ALTER TABLE ... "+c.verb+" "+strings.ToUpper(string(c.kind)),
		"table", o.String(), "name", c.name, "by", user.Name)
	return acc, w.Complete("ALTER TABLE")
}

// authorizeReads decides whether user may read every table of reads.
func (s *Server) authorizeReads(ctx context.Context, sess *session, user auth.User, reads []auth.Object) error {
	if rank[user.Role] >= rank[auth.RoleReader] || len(reads) == 0 {
		return nil
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		return denied("could not load grants: %v", err)
	}
	for _, o := range reads {
		if n := (need{auth.PrivilegeSelect, o}); !allows(user, privs, n) {
			s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", needError(user, n))
			return needError(user, n)
		}
	}
	return nil
}

// --- reading branches and tags ----------------------------------------------

// readsRef finds statements that may read a branch or tag, before they are
// tokenized.
var readsRef = regexp.MustCompile(`(?i)\b(BRANCH|TAG)\s*=>`)

// posToken is a token of a query and where it is: a word (upper-cased),
// a quoted identifier, a string literal (its value) or a punctuation
// character.
type posToken struct {
	kind       byte // 'w', 'i', 's' or 'p'
	text       string
	start, end int
}

func positionedTokens(query string) []posToken {
	var out []posToken
	at := 0
	for _, seg := range sqllex.Segments(query) {
		base := at
		at += len(seg.Text)
		switch seg.Kind {
		case sqllex.String:
			t := posToken{kind: 's', start: base, end: at}
			if v := seg.Text; len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
				t.text = strings.ReplaceAll(v[1:len(v)-1], "''", "'")
			} else {
				t.kind = 'p' // E'...' or $$...$$: not a name
			}
			out = append(out, t)
		case sqllex.Ident:
			out = append(out, posToken{kind: 'i', text: strings.ReplaceAll(strings.Trim(seg.Text, `"`), `""`, `"`), start: base, end: at})
		case sqllex.Code:
			s := seg.Text
			for i := 0; i < len(s); {
				switch c := s[i]; {
				case sqllex.IdentByte(c):
					j := i
					for j < len(s) && sqllex.IdentByte(s[j]) {
						j++
					}
					out = append(out, posToken{kind: 'w', text: strings.ToUpper(s[i:j]), start: base + i, end: base + j})
					i = j
				case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
					i++
				default:
					out = append(out, posToken{kind: 'p', text: string(c), start: base + i, end: base + i + 1})
					i++
				}
			}
		}
	}
	return out
}

// refRead is an AT (BRANCH => 'b') or AT (TAG => 't') in a query.
type refRead struct {
	// table is the name the clause follows, as scanner.name reads it.
	table      []string
	at         clone.At
	start, end int
}

// tableStarts are the tokens a table's name follows in FROM and JOIN.
var tableStarts = map[string]bool{"FROM": true, "JOIN": true, ",": true, "(": true, "LATERAL": true}

// findRefReads finds the AT (BRANCH => ...) and AT (TAG => ...) clauses
// of query and the tables they apply to: FROM name [[AS] alias] AT (...).
func findRefReads(query string) ([]refRead, error) {
	toks := positionedTokens(query)
	is := func(i int, kind byte, text string) bool {
		return i >= 0 && i < len(toks) && toks[i].kind == kind && (text == "" || toks[i].text == text)
	}
	name := func(i int) bool { return is(i, 'w', "") || is(i, 'i', "") }
	// chain returns the start of the dotted name that ends at token end.
	chain := func(end int) int {
		start := end
		for is(start-1, 'p', ".") && name(start-2) {
			start -= 2
		}
		return start
	}
	var out []refRead
	for i := range toks {
		if !(is(i, 'w', "AT") && is(i+1, 'p', "(") && (is(i+2, 'w', "BRANCH") || is(i+2, 'w', "TAG")) &&
			is(i+3, 'p', "=") && is(i+4, 'p', ">") && is(i+5, 's', "") && is(i+6, 'p', ")")) {
			continue
		}
		at := clone.At{Unit: clone.Branch, Ref: toks[i+5].text}
		if toks[i+2].text == "TAG" {
			at.Unit = clone.Tag
		}
		end := i - 1
		if !name(end) {
			return nil, refPlacement()
		}
		start := chain(end)
		before := ""
		if start > 0 {
			before = toks[start-1].text
		}
		if !tableStarts[before] {
			// An alias, after AS or not: the name is the chain before it.
			if start != end {
				return nil, refPlacement()
			}
			end = start - 1
			if is(end, 'w', "AS") {
				end--
			}
			if !name(end) {
				return nil, refPlacement()
			}
			start = chain(end)
			if start == 0 || !tableStarts[toks[start-1].text] {
				return nil, refPlacement()
			}
		}
		var parts []string
		for j := start; j <= end; j += 2 {
			if toks[j].kind == 'w' {
				parts = append(parts, strings.ToLower(toks[j].text))
			} else {
				parts = append(parts, toks[j].text)
			}
		}
		out = append(out, refRead{table: parts, at: at, start: toks[i].start, end: toks[i+6].end})
	}
	return out, nil
}

func refPlacement() error {
	return cloneSyntax("AT (BRANCH => ...) and AT (TAG => ...) follow a table's name in FROM or JOIN, as in FROM t AT (BRANCH => 'dev')")
}

// resolveRefReads replaces each AT (BRANCH => 'b') and AT (TAG => 't') in
// query with AT (VERSION => id), the snapshot the branch or tag names when
// the statement is prepared, which DuckDB can read.
func (s *Server) resolveRefReads(ctx context.Context, sess *session, query string) (string, error) {
	if s.clones == nil || !readsRef.MatchString(query) {
		return query, nil
	}
	reads, err := findRefReads(query)
	if err != nil || len(reads) == 0 {
		return query, err
	}
	var b strings.Builder
	last := 0
	for _, r := range reads {
		o, err := s.cloneSource(ctx, sess, r.table)
		if err != nil {
			return "", err
		}
		id, err := s.clones.Resolve(ctx, identOf(o), r.at)
		if err != nil {
			return "", cloneError(err)
		}
		b.WriteString(query[last:r.start])
		b.WriteString("AT (VERSION => " + strconv.FormatInt(id, 10) + ")")
		last = r.end
	}
	b.WriteString(query[last:])
	return b.String(), nil
}
