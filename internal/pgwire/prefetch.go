package pgwire

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/prefetch"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Reading ahead for statements (package prefetch). With IGLOO_PREFETCH on,
// a statement that reads catalog tables (a query, or the query of CREATE
// TABLE ... AS or INSERT ... SELECT) has its scans resolved just before it
// runs, on the session's goroutine (distributed.Scanner.Resolve, which asks
// the session's resolver), and their files listed and read ahead off it,
// until the statement ends. A session turns it off and on for itself:
//
//	SET igloo_prefetch = off | on   -- RESET igloo_prefetch, SHOW igloo_prefetch

var (
	setPrefetch   = regexp.MustCompile(`(?is)^\s*(?:SET\s+(?:SESSION\s+)?|ALTER\s+SESSION\s+SET\s+)"?igloo_prefetch"?\s*(?:=|\bTO\b)\s*'?([a-z]+)'?\s*;?\s*$`)
	resetPrefetch = regexp.MustCompile(`(?is)^\s*(?:RESET|ALTER\s+SESSION\s+UNSET)\s+"?igloo_prefetch"?\s*;?\s*$`)
	showPrefetch  = regexp.MustCompile(`(?is)^\s*SHOW\s+"?igloo_prefetch"?\s*;?\s*$`)
)

// prefetchCommand handles SET, RESET and SHOW igloo_prefetch.
func (s *Server) prefetchCommand(sess *session, text string) (wire.PreparedStatements, bool) {
	query := stripComments(text)
	if !strings.Contains(strings.ToLower(query), "igloo_prefetch") || len(sqllex.Split(text)) > 1 {
		return nil, false
	}
	respond := func(fn wire.PreparedStatementFn, opts ...wire.PreparedOptionFn) (wire.PreparedStatements, bool) {
		return wire.Prepared(wire.NewStatement(s.audited(sess, text, fn), opts...)), true
	}
	if m := setPrefetch.FindStringSubmatch(query); m != nil {
		var off bool
		var err error
		switch strings.ToLower(m[1]) {
		case "on", "true", "default":
		case "off", "false":
			off = true
		default:
			err = pgerror.WithCode(errInvalidPrefetch(m[1]), codes.InvalidParameterValue)
		}
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err != nil {
				return err
			}
			sess.prefetchOff = off
			return w.Complete("SET")
		})
	}
	if resetPrefetch.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			sess.prefetchOff = false
			return w.Complete("RESET")
		})
	}
	if showPrefetch.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			v := "off"
			if s.prefetches(sess) {
				v = "on"
			}
			if err := w.Row([]any{v}); err != nil {
				return err
			}
			return w.Complete("SHOW")
		}, wire.WithColumns(wire.Columns{{Name: "igloo_prefetch", Oid: pgtype.TextOID, Width: -1}}))
	}
	return nil, false
}

type errInvalidPrefetch string

func (e errInvalidPrefetch) Error() string {
	return "invalid value for parameter igloo_prefetch: " + string(e) + " (on or off)"
}

// prefetches reports whether the session's statements read ahead.
func (s *Server) prefetches(sess *session) bool {
	return s.scanner != nil && s.prefetcher != nil && !sess.prefetchOff
}

// scanText is the query whose scans a statement of type t reads ahead: the
// statement itself, or the query of CREATE TABLE ... AS or INSERT ...
// SELECT; "" for anything else.
func scanText(t duckdb.StmtType, query string) string {
	switch t {
	case duckdb.STATEMENT_TYPE_SELECT:
		return query
	case duckdb.STATEMENT_TYPE_CREATE:
		if c, ok := parseCreateTable(query); ok && c.query != "" {
			return c.query
		}
	case duckdb.STATEMENT_TYPE_INSERT:
		return insertSource(query)
	}
	return ""
}

// prefetchStatement starts reading ahead for st, about to run in ctx on
// sess, and returns what stops it. Resolving its scans asks the session's
// connection, so it happens here, before the statement runs; listing their
// files, which can take seconds the first time a snapshot is seen, and
// reading ahead happen off the statement's way.
func (s *Server) prefetchStatement(ctx context.Context, sess *session, st *statement) (stop func()) {
	if !s.prefetches(sess) {
		return func() {}
	}
	text := scanText(st.stmtType, st.query)
	if text == "" {
		return func() {}
	}
	env := &scanEnv{sessionEnv: &sessionEnv{s: s, sess: sess, user: sess.currentUser(), st: st}}
	plan, err := s.scanner.Resolve(ctx, text, env)
	if err != nil {
		s.log.Debug("prefetch: cannot resolve the statement's scans", "err", err)
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	var (
		mu     sync.Mutex
		stopPf func()
		done   = make(chan struct{})
	)
	go func() {
		defer close(done)
		scans, err := plan.Files(ctx)
		if err != nil || len(scans) == 0 {
			if err != nil && ctx.Err() == nil {
				s.log.Debug("prefetch: cannot list the statement's files", "err", err)
			}
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if ctx.Err() == nil {
			stopPf = s.prefetcher.Start(ctx, prefetchScans(scans))
		}
	}()
	return func() {
		cancel()
		<-done
		mu.Lock()
		defer mu.Unlock()
		if stopPf != nil {
			stopPf()
		}
	}
}

// prefetchScans are the planner's scans as the prefetcher reads them.
func prefetchScans(scans []distributed.Scan) []prefetch.Scan {
	out := make([]prefetch.Scan, 0, len(scans))
	for _, sc := range scans {
		ps := prefetch.Scan{Filtered: sc.Filtered, Whole: sc.Whole, FieldIDs: make([]int32, len(sc.FieldIDs)), Files: make([]prefetch.File, len(sc.Files))}
		for i, id := range sc.FieldIDs {
			ps.FieldIDs[i] = int32(id)
		}
		for i, f := range sc.Files {
			ps.Files[i] = prefetch.File{Path: f.Path, Size: f.Size}
		}
		out = append(out, ps)
	}
	return out
}

// scanEnv resolves a statement's table names for the scanner as the
// session would, leaving out tables that policies or a clean room protect,
// whose reads Igloo rewrites, and, for users who read only what grants
// allow, those the statement was not checked to read.
type scanEnv struct {
	*sessionEnv
}

func (e *scanEnv) Resolve(ctx context.Context, parts []string) (schema, name string, ok bool, err error) {
	catalogName := e.s.catalogName()
	if catalogName == "" {
		return "", "", false, nil
	}
	if e.resolver == nil {
		r, err := e.s.newResolver(ctx, e.sess)
		if err != nil {
			return "", "", false, err
		}
		e.resolver = r
	}
	res, err := e.resolver.resolve(tableRef{parts: parts}, nil)
	if err != nil || len(res.all) != 1 || len(res.first) != 1 {
		return "", "", false, err
	}
	o := res.first[0]
	if o.view || !strings.EqualFold(o.db, catalogName) {
		return "", "", false, nil
	}
	if rank[e.user.Role] < rank[auth.RoleReader] {
		obj := auth.Object{Schema: o.schema, Table: o.name}
		if e.st.access == nil || !slices.ContainsFunc(e.st.access.read, obj.Equal) {
			return "", "", false, nil
		}
	}
	if e.s.Protected(ctx, o.schema, o.name) {
		return "", "", false, nil
	}
	return o.schema, o.name, true, nil
}
