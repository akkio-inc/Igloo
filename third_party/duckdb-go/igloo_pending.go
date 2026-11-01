package duckdb

import (
	"time"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// How long a statement's thread sleeps while DuckDB has no task for it, at
// first and at most.
const (
	pendingWaitMin = 20 * time.Microsecond
	pendingWaitMax = time.Millisecond
)

// runPending carries out pending's tasks on the calling thread until its
// result is ready, for mapping.ExecutePending to return, or it fails, when it
// returns DuckDB's message.
//
// mapping.ExecutePending does the same itself, but while DuckDB's worker
// threads hold all of the statement's tasks it asks for one again and again
// without a pause, and takes a lock each time: a core for every statement
// waiting on its workers, to do nothing. A statement whose one worker
// finalized a large aggregate used two cores for the work of one. Here the
// thread sleeps while there is no task for it, a little longer each time, up
// to a millisecond, which is also the longest a finished statement's result
// waits to be seen.
func runPending(pending mapping.PendingResult) (failed bool, message string) {
	wait := pendingWaitMin
	for {
		switch mapping.PendingExecuteTask(pending) {
		case mapping.PendingStateResultReady:
			return false, ""
		case mapping.PendingStateError:
			return true, mapping.PendingError(pending)
		case mapping.PendingStateNoTasksAvailable:
			time.Sleep(wait)
			wait = min(wait*2, pendingWaitMax)
		default:
			wait = pendingWaitMin
		}
	}
}
