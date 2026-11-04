package pgwire

import (
	"context"
	"math"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/sqllex"
	"github.com/akkio-inc/igloo/internal/workload"
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
//
// A prepare that runs out of memory beside other statements is tried again
// once it holds the replica's whole memory (see prepareAgainAlone).
func (s *Server) prepareChecked(ctx context.Context, sess *session, user auth.User, text string, oids []uint32) (*statement, error) {
	st, err := s.prepareOnce(ctx, sess, user, text, oids)
	if err == nil || !prepareAgainAlone(text, err) {
		return st, err
	}
	done, _, aerr := sess.wl.controller.Admit(ctx, workload.Limits{Memory: math.MaxInt64})
	if aerr != nil {
		return nil, err
	}
	defer done()
	s.log.Warn("preparing a statement ran out of memory; preparing it again alone", "user", user.Name, "pid", sess.pid, "err", err)
	return s.prepareOnce(ctx, sess, user, text, oids)
}

func (s *Server) prepareOnce(ctx context.Context, sess *session, user auth.User, text string, oids []uint32) (*statement, error) {
	if s.exclusivePrepares {
		return s.prepareHeld(ctx, sess, user, text, oids, catalogExclusive)
	}
	s.metrics.prepare("shared")
	return s.prepareHeld(ctx, sess, user, text, oids, catalogShared)
}

// prepareAgainAlone says whether preparing text, which failed with err, ran
// out of memory and may be tried again alone: DuckDB's bind of a statement
// reads what it needs of its tables, an Iceberg table's manifests among
// them, and runs out of memory there as a run does (see alone.go). Text of
// several statements is not tried again, as preparing it ran all but its
// last.
func prepareAgainAlone(text string, err error) bool {
	return isOutOfMemory(err) && !strings.Contains(sqllex.CodeOnly(strings.TrimRight(strings.TrimSpace(text), "; \t\r\n")), ";")
}
