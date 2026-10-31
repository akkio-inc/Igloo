package erasure

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Views are the materialized views and dynamic tables Igloo refreshes from
// tables' rows (package pgwire's refresh.go). Each keeps its query's
// result as a table of its own, and an aggregating one its groups' partial
// aggregates in a state table: both hold what the person's rows gave, in
// their current rows and their older snapshots, and a refresh, incremental
// or not, leaves the older snapshots. A request has each view that reads
// one of its tables, directly or through other views, replaced, once the
// rows are gone (its views step, after delete): its table made again from
// its query, as its owner, as CREATE OR REPLACE would, and its state
// dropped, so that the next refresh starts afresh. It completes only once
// none is left: a view it cannot replace, because its owner is gone or may
// no longer read its tables, blocks it (see Service.Close).
type Views interface {
	// Rebuild replaces each view whose query reads any of tables, or a
	// view that does, and whose table or state has a snapshot from before
	// since, upstream views first. It returns how many it replaced and
	// those it could not, with why, which are an error.
	Rebuild(ctx context.Context, tables []string, since time.Time) (Rebuilt, error)
	// Stale names the views Rebuild would replace: those that may hold
	// what the person's rows gave.
	Stale(ctx context.Context, tables []string, since time.Time) ([]string, error)
}

// Busy marks err, from Views.Rebuild, as a refresh of a view running
// meanwhile: the request tries again a minute later, which does not count
// as a failed run.
func Busy(err error) error { return contended(err) }

// Rebuilt is what Views.Rebuild did.
type Rebuilt struct {
	Views int
	// Failed are the views it could not replace: schema.name: why.
	Failed []string
}

// rebuildViews is the views step: it replaces every view made from the
// request's tables before the step began, and has every replica empty its
// read cache of the old tables' files (see the cache step).
func (s *Service) rebuildViews(ctx context.Context, r *Request) error {
	v := s.opts.Views
	if v == nil {
		return nil
	}
	if r.ViewsSince.IsZero() {
		r.ViewsSince = s.now()
		if err := s.save(ctx, r); err != nil {
			return err
		}
	}
	res, err := v.Rebuild(ctx, derivedTables(r), r.ViewsSince)
	r.ViewsRebuilt += res.Views
	r.ViewsFailed = nil
	for _, f := range res.Failed {
		r.ViewsFailed = append(r.ViewsFailed, scrub(r, f))
	}
	if res.Views > 0 {
		// The old tables' files are gone, or go with the purge step:
		// replicas' read caches go too.
		r.DeletedThrough = s.now()
	}
	switch {
	case len(r.ViewsFailed) > 0:
		s.log.Error("an erasure request could not replace materialized views or dynamic tables made from its tables, which may hold the "+
			"person's rows", "request", r.ID, "views", strings.Join(r.ViewsFailed, "; "))
		return terminal(fmt.Errorf("it could not replace %d materialized views or dynamic tables made from its tables, which may hold the "+
			"person's rows: %s; once each can be refreshed again (or is dropped), it goes on, or an admin can close it (CLOSE ERASURE REQUEST)",
			len(r.ViewsFailed), strings.Join(r.ViewsFailed, "; ")))
	case err != nil:
		return fmt.Errorf("replace the materialized views and dynamic tables made from its tables: %w", err)
	}
	return nil
}

// viewsRemaining counts, for verification, the views made from the
// request's tables that may still hold what the person's rows gave.
func (s *Service) viewsRemaining(ctx context.Context, r *Request) (int64, error) {
	v := s.opts.Views
	if v == nil {
		return 0, nil
	}
	since := r.ViewsSince
	if since.IsZero() {
		since = s.now()
	}
	stale, err := v.Stale(ctx, derivedTables(r), since)
	return int64(len(stale)), err
}

// leaveViews returns, for a request that is to be closed, the views made
// from its tables that it did not replace, which may hold what the
// person's rows gave and stay.
func (s *Service) leaveViews(ctx context.Context, r *Request) ([]string, error) {
	v := s.opts.Views
	if v == nil {
		return nil, nil
	}
	since := r.ViewsSince
	if since.IsZero() {
		since = s.now()
	}
	return v.Stale(ctx, derivedTables(r), since)
}
