package pgwire

import (
	"context"
	"slices"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Shared prepares. Preparing a statement held the catalog lock exclusively
// (see prepareChecked), so a dashboard's burst of statements was prepared
// one at a time: a dashboard build took 23 s, sent together as one after
// another, most of it DuckDB's own prepare. The lock is there because
// walking DuckDB-Iceberg's in-memory set of the catalog's tables, or
// erasing an entry from it, alongside a bind frees memory the other is
// still reading (see engine.Engine.CatalogLock). A prepare walks the set in
// two ways: Igloo's steps that list the catalog (checks of grants, loading
// schemas, catalog functions), and DuckDB's suggestion of similar names for
// a table a statement or a view's body names that does not exist, which
// another engine may have dropped a moment before; it erases nothing.
//
// Neither happens alongside other binds: while a statement is prepared
// under the shared lock, the catalog proxy refuses the requests
// DuckDB-Iceberg makes before it walks the set or erases from it (see
// catalogproxy's shared prepares, and engine.Engine.SharedPrepare), and
// DuckDB fails the statement instead. So a prepare that holds the lock
// shared is safe whatever it does; what is left to choose is which
// prepares are likely to succeed that way, and to do the rest under the
// exclusive lock as before. (A request does not say which statement made
// it, so a listing that another statement makes meanwhile without the
// exclusive lock, as a few of Igloo's own commands do, fails too.)
// A query (one SELECT, outside a transaction) whose text calls no catalog
// function is prepared holding the lock shared, alongside other sessions'
// prepares and binds. If DuckDB fails to prepare it, or the proxy refused a
// request meanwhile (any session's: a refusal of another's may have been
// one of this prepare's checks), it is prepared again from the start under
// the exclusive lock, where a missing table gets DuckDB's suggestion and
// the steps that recover from failures (syncing views another replica made,
// for one) run as before. Users of the none role, whose every statement is
// checked against grants by listing the catalog, prepare exclusively.
//
// What DuckDB loads while a statement is prepared is recorded as before
// (see prepareChecked), now with the tables other binds load meanwhile:
// more tables, never fewer, which the result cache reloads and AI caches
// are tagged by, and both stay right.

// sharedPrepare reports whether to prepare text, for user, holding the
// catalog lock shared (see Shared prepares).
func (s *Server) sharedPrepare(sess *session, user auth.User, text string) bool {
	if s.exclusivePrepares || s.catalogName() == "" || sess.inTx || user.Role == auth.RoleNone {
		return false
	}
	if slices.ContainsFunc(sess.catHolds, func(h *catalogHold) bool { return h.mode == catalogExclusive }) {
		return false
	}
	if len(sqllex.Split(text)) != 1 || !startsQuery(text) && !cteQuery(text) {
		return false
	}
	return !listsCatalog.MatchString(text) && !listsColumns.MatchString(text) && !readsMetadata.MatchString(text) &&
		!mayCreateTemporary(text)
}

// cteQuery reports whether text, which starts with WITH, is a query: none of
// the statements a CTE may come before.
func cteQuery(text string) bool {
	words := sqllex.CodeWords(text)
	if len(words) == 0 || words[0] != "WITH" {
		return false
	}
	return !slices.ContainsFunc(words, func(w string) bool {
		switch w {
		case "INSERT", "UPDATE", "DELETE", "MERGE", "COPY", "CREATE":
			return true
		}
		return false
	})
}

// prepareChecked is prepareStatement without recording failures, for the
// statements Igloo runs for a user to carry out one of its own commands
// (COPY INTO, materialized views): they are checked exactly like the
// user's own. It holds the catalog lock shared while it prepares a query
// that may be prepared that way, and exclusively otherwise and when that
// fails (see Shared prepares).
func (s *Server) prepareChecked(ctx context.Context, sess *session, user auth.User, text string, oids []uint32) (*statement, error) {
	if !s.sharedPrepare(sess, user, text) {
		return s.prepareHeld(ctx, sess, user, text, oids, catalogExclusive)
	}
	refusals := s.engine.SharedRefusals()
	st, err := s.prepareHeld(ctx, sess, user, text, oids, catalogShared)
	if err == nil && st.stmtType == duckdb.STATEMENT_TYPE_SELECT && s.engine.SharedRefusals() == refusals {
		s.metrics.prepare("shared")
		return st, nil
	}
	if st != nil {
		_ = st.close()
	}
	s.metrics.prepare("retried")
	s.log.Debug("statement prepared again under the exclusive catalog lock", "err", err)
	return s.prepareHeld(ctx, sess, user, text, oids, catalogExclusive)
}
