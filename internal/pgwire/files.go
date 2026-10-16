package pgwire

import (
	"context"
	"strings"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/policy"
)

// Confining what files a statement may name.
//
// DuckDB's sandbox (allowed_directories, enable_external_access off) is the
// backstop, but DuckDB 1.5.6 does not canonicalise a remote path before it
// prefix-matches it against allowed_directories, so a path that climbs out
// of an allowed prefix with ".." or percent-encoded separators, or that
// redirects an s3:// read with a ?s3_endpoint= query, passes DuckDB's check
// and reaches elsewhere. Igloo therefore checks the files a non-admin names
// itself, for every statement, whether or not a table has a policy. The
// checks demand plain string literals and reject anything that could climb
// out of a prefix (see checkPath).
//
// When a table has a policy the policy rewriter already applies these checks
// (and more) as it rewrites; guardFiles then does nothing, so the work is
// not repeated. Without any policy it runs the same file checks on their
// own, letting readers and writers still read the warehouse's files.

// guardFiles refuses the files a non-admin statement names that it may not
// read or write, when no table has a policy (the policy rewriter covers the
// rest). It runs before the statement is prepared, because DuckDB opens a
// file while preparing a replacement scan or a schema-inferring read, so a
// check that ran after preparing would have let the request out already. It
// parses only when the statement's text hints at a file, so a statement that
// touches none costs one lexer pass.
func (s *Server) guardFiles(ctx context.Context, sess *session, user auth.User, query string) error {
	if user.Role == auth.RoleAdmin {
		return nil
	}
	snap := s.gov.snapshot(ctx)
	if !snap.Empty() {
		// A table has a policy, so policyRewrite already checked the files.
		return nil
	}
	t := fileStmtKind(query)
	if !mentionsFiles(t, query) {
		return nil
	}
	rw, err := s.newFileRewriter(ctx, sess, snap)
	if err != nil {
		return err
	}
	if _, err := rw.statementEdits(t, query); err != nil {
		return err
	}
	return nil
}

// fileStmtKind is the statement's kind from its leading keyword, enough for
// the file checks to route it (see statementEdits). It is worked out before
// the statement is prepared, so it cannot ask DuckDB; a leading WITH clause
// is skipped so CTEs before a DML statement do not hide it. Anything it does
// not recognise is treated as a query, which the checks read with
// json_serialize_sql.
func fileStmtKind(query string) duckdb.StmtType {
	sc := &scanner{s: query}
	if sc.keywords("WITH") {
		sc.keywords("RECURSIVE")
		for {
			if _, ok := sc.name(); !ok {
				break
			}
			sc.keywords("AS")
			sc.skip()
			if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
				end := matchParen(sc.s, sc.i)
				if end < 0 {
					break
				}
				sc.i = end + 1
			}
			sc.skip()
			if sc.i < len(sc.s) && sc.s[sc.i] == ',' {
				sc.i++
				continue
			}
			break
		}
	}
	switch strings.ToUpper(sc.word()) {
	case "COPY":
		return duckdb.STATEMENT_TYPE_COPY
	case "INSERT":
		return duckdb.STATEMENT_TYPE_INSERT
	case "UPDATE":
		return duckdb.STATEMENT_TYPE_UPDATE
	case "DELETE", "TRUNCATE":
		return duckdb.STATEMENT_TYPE_DELETE
	case "MERGE":
		return statementTypeMergeInto
	case "CREATE":
		return duckdb.STATEMENT_TYPE_CREATE
	case "EXPLAIN":
		return duckdb.STATEMENT_TYPE_EXPLAIN
	case "SET", "RESET":
		return duckdb.STATEMENT_TYPE_SET
	}
	return duckdb.STATEMENT_TYPE_SELECT
}

// newFileRewriter makes a rewriter that only checks the files a statement
// names: it makes no table edits and reads no policies, so it needs no
// sensitivity scan. allowWarehouse is true because this runs only when no
// table has a policy, when readers and writers may read the warehouse.
func (s *Server) newFileRewriter(ctx context.Context, sess *session, snap *policy.Snapshot) (*rewriter, error) {
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return nil, err
	}
	return &rewriter{
		s: s, ctx: ctx, sess: sess, r: r, snap: snap,
		sens:           &sensitivity{},
		filesOnly:      true,
		allowWarehouse: true,
		nonce:          newNonce(),
	}, nil
}

// mentionsFiles reports whether a statement might name a file: it is a COPY,
// a string or quoted name looks like a path, a name is written with Unicode
// escapes (which could spell a path), or a word names a file-reading
// function. It never parses; it only lexes.
func mentionsFiles(t duckdb.StmtType, query string) bool {
	if t == duckdb.STATEMENT_TYPE_COPY {
		return true
	}
	toks := tokensOf(query)
	if toks.paths || toks.opaque {
		return true
	}
	for _, n := range toks.names {
		if fileFunctions[n] || extraFileFunctions[n] {
			return true
		}
	}
	return false
}

// macroBody returns the body of CREATE [OR REPLACE] [TEMP] MACRO name(args)
// AS [TABLE] body, whether it is a table macro, and whether it parsed. A
// scalar macro's body is an expression; a table macro's is a query.
func macroBody(query string) (body string, table, ok bool) {
	sc := &scanner{s: query}
	if !sc.keywords("CREATE") {
		return "", false, false
	}
	sc.keywords("OR", "REPLACE")
	_ = sc.keywords("TEMP") || sc.keywords("TEMPORARY") || sc.keywords("PERSISTENT")
	if !sc.keywords("MACRO") && !sc.keywords("FUNCTION") {
		return "", false, false
	}
	if _, ok := sc.name(); !ok {
		return "", false, false
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		end := matchParen(sc.s, sc.i)
		if end < 0 {
			return "", false, false
		}
		sc.i = end + 1
	}
	if !sc.keywords("AS") {
		return "", false, false
	}
	table = sc.keywords("TABLE")
	rest := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sc.rest()), ";"))
	if rest == "" || strings.HasPrefix(rest, "(") && !table {
		// An overloaded macro (several parenthesised bodies) is left to the
		// residual check.
		return "", false, false
	}
	return rest, table, true
}

// createFileEdits checks the files named by a CREATE statement, for the
// files-only rewriter: a CREATE TABLE AS query, a view's or table macro's
// body, and, for anything else, a file named as a path-like string.
func (rw *rewriter) createFileEdits(query string) error {
	if c, ok := parseCreateSpans(query); ok {
		if c.query.empty() {
			return nil
		}
		if _, err := rw.partEdits(query, c.query, ""); err != nil {
			return err
		}
		return rw.residual(query, c.name, c.query)
	}
	if body := viewBody(query); body != "" {
		_, err := rw.queryEdits(body, nil, 0)
		return err
	}
	if body, table, ok := macroBody(query); ok {
		q := body
		if !table {
			q = "SELECT (" + body + ")"
		}
		_, err := rw.queryEdits(q, nil, 0)
		return err
	}
	return rw.residual(query)
}
