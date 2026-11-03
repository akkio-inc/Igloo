package pgwire

import (
	"context"

	"github.com/akkio-inc/igloo/internal/auth"
)

// Shared prepares. Preparing a statement looks the tables it names up, in
// DuckDB's bind and in Igloo's checks, which list the catalog: it holds the
// catalog lock shared, alongside other sessions' prepares, binds and listings,
// as any step that looks tables up does, and waits only for DDL (see
// engine.Engine.CatalogLock). It held the lock exclusively, and a dashboard's
// burst of statements was prepared one at a time: a dashboard build took 23 s,
// most of it DuckDB's own prepare. IGLOO_SHARED_PREPARES=false
// (Options.ExclusivePrepares) prepares each statement with the lock to itself.
//
// What DuckDB loads while a statement is prepared is recorded as before (see
// prepareHeld), with the tables other binds load meanwhile: more tables, never
// fewer, which the result cache reloads and AI caches are tagged by, and both
// stay right.

// prepareChecked is prepareStatement without recording failures, for the
// statements Igloo runs for a user to carry out one of its own commands
// (COPY INTO, materialized views): they are checked exactly like the
// user's own. It holds the catalog lock shared (see Shared prepares).
func (s *Server) prepareChecked(ctx context.Context, sess *session, user auth.User, text string, oids []uint32) (*statement, error) {
	if s.exclusivePrepares {
		return s.prepareHeld(ctx, sess, user, text, oids, catalogExclusive)
	}
	s.metrics.prepare("shared")
	return s.prepareHeld(ctx, sess, user, text, oids, catalogShared)
}
