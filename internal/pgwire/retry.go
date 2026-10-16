package pgwire

import (
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// maxCommitRetries bounds how often a conflicting statement is re-run.
const maxCommitRetries = 20

// runWithCommitRetry runs a statement and, when its Iceberg commit loses a
// race with another writer, runs it again on the new table snapshot.
//
// Iceberg commits are optimistic: two sessions (or replicas) appending to the
// same table at once both prepare a commit, and the catalog rejects the
// second. That commit applied nothing, so re-running the statement is safe
// and gives the result the client would have got had it run a moment later.
// Igloo only does this outside explicit transactions and for statements that
// send no rows: in a transaction the client must decide, and rows already
// sent cannot be taken back. Clients then see SQLSTATE 40001 and can retry.
func runWithCommitRetry(ctx context.Context, sess *session, st *statement, run func() error) error {
	backoff := 20 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err := run()
		if err == nil || attempt == maxCommitRetries || sess.inTx || st.kind != command || !commitConflict(err) {
			return err
		}

		// Full jitter keeps competing writers from retrying in lockstep.
		wait := time.Duration(rand.Int64N(int64(backoff)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		backoff = min(2*backoff, time.Second)
	}
}

// commitConflict reports whether err is an Iceberg commit rejected because
// the table changed underneath it. Other HTTP 409s from the catalog, such as
// dropping a schema that still has tables, are not worth retrying.
func commitConflict(err error) bool {
	var de *duckdb.Error
	if !errors.As(err, &de) || de.Type != duckdb.ErrorTypeTransaction {
		return false
	}
	return strings.Contains(de.Msg, "CatalogCommitConflicts")
}
