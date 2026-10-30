// Package tasks keeps Igloo's scheduled work and its history: tasks (one SQL
// statement on a schedule), pipes (COPY INTO from a location, run every
// minute or on a schedule), materialized views (a query kept as a table
// and refreshed on demand or on a schedule) and alerts (a query on a
// schedule that sends a notification when it returns rows). It also keeps
// the load history of COPY INTO, which makes sure a file is loaded into a
// table once.
//
// Definitions and history live in the metadata database (PostgresStore,
// schema igloo_tasks) so every replica shares them, or in memory
// (MemoryStore) without one. The Scheduler runs work when it is due, on
// one replica at a time; what a run does is up to a Runner, which runs it
// as its owner (package pgwire).
package tasks

import (
	"context"
	"errors"
	"hash/fnv"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
)

// Kinds of scheduled work.
const (
	KindTask             = "task"
	KindPipe             = "pipe"
	KindMaterializedView = "materialized view"
	KindAlert            = "alert"
)

// States of a job.
const (
	StateStarted   = "started"
	StateSuspended = "suspended"
)

// Outcomes of a run.
const (
	OutcomeRunning   = "running"
	OutcomeSucceeded = "succeeded"
	OutcomeFailed    = "failed"
	// OutcomeSkipped is a scheduled run that did not start because the
	// previous one was still running.
	OutcomeSkipped = "skipped"
)

// Triggers of a run.
const (
	TriggerSchedule = "schedule"
	TriggerManual   = "manual"
)

// Statuses of a file in the load history.
const (
	// LoadLoading is a load in progress. One found while nobody loads the
	// table was cut short by a crash and becomes LoadUnknown.
	LoadLoading = "loading"
	LoadLoaded  = "loaded"
	// LoadPartial is a file loaded without the rows that could not be
	// read (ON_ERROR continue).
	LoadPartial = "partially loaded"
	LoadFailed  = "failed"
	// LoadUnknown is a file whose load Igloo was stopped in the middle of,
	// or whose commit failed without saying whether it was applied: it may
	// or may not be in the table, so it is not loaded again unless forced.
	LoadUnknown = "unknown"
)

var (
	// ErrExists is returned for a job whose name is taken.
	ErrExists = errors.New("already exists")
	// ErrNotFound is returned for a job that does not exist.
	ErrNotFound = errors.New("does not exist")
	// ErrOwnerGone is returned by a Runner whose job's owner has been
	// dropped. The job is suspended.
	ErrOwnerGone = errors.New("the owner no longer exists")
	// ErrIdle is returned by a Runner for a pipe that found nothing to
	// load. Such checks are not recorded.
	ErrIdle = errors.New("nothing to do")
	// ErrRunning is returned by RunNow for a job that is running.
	ErrRunning = errors.New("is already running")
	// ErrSkipped is wrapped by a Runner's error for a run it did not
	// carry out, such as an alert's while its owner is disabled. The run
	// is recorded as skipped, with the error.
	ErrSkipped = errors.New("skipped")
)

// Job is a task, pipe, materialized view or alert.
type Job struct {
	Kind string
	// Name is a task's, pipe's or alert's name, or a materialized view's
	// schema.table.
	Name  string
	Owner string
	// Statement is what runs: a task's SQL, a pipe's COPY INTO, a
	// materialized view's query, or an alert's IF ... THEN ....
	Statement string
	// Schedule and Timezone are as written. A materialized view without a
	// schedule has none.
	Schedule string
	Timezone string
	State    string
	// NextRun is when the job runs next; zero when it does not run on a
	// schedule, or is suspended.
	NextRun   time.Time
	CreatedAt time.Time
	// LastRun is the latest run recorded, if any. Jobs and Job fill it in.
	LastRun *Run
}

// Run is one run of a job.
type Run struct {
	ID   string
	Kind string
	Name string
	// User is the owner the job ran as.
	User    string
	Trigger string
	// Scheduled is when a scheduled run was due; zero for others.
	Scheduled time.Time
	Start     time.Time
	End       time.Time
	Outcome   string
	Error     string
	// Rows is how many rows the run loaded, wrote or returned, or -1.
	Rows int64
}

// Load is one attempt to load one file into a table.
type Load struct {
	ID string
	// Table is the target, as catalog.schema.table.
	Table    string
	File     string
	Size     int64
	Modified time.Time
	Status   string
	// Rows were loaded; Errors are rows that could not be read and were
	// skipped.
	Rows       int64
	Errors     int64
	FirstError string
	Start      time.Time
	End        time.Time
	// User loaded the file; Pipe is the pipe that did, if any.
	User string
	Pipe string
}

// SameVersion reports whether l is a load of the version of its file that
// has size and modified.
func (l Load) SameVersion(size int64, modified time.Time) bool {
	return l.Size == size && l.Modified.Equal(Truncate(modified))
}

// Truncate rounds t down to the microsecond, the precision the stores
// keep, so times compare equal after a round trip.
func Truncate(t time.Time) time.Time { return t.Truncate(time.Microsecond) }

// Store keeps jobs, their runs and the load history. Implementations must
// be safe for concurrent use. Names compare without regard to case.
type Store interface {
	// CreateJob adds j, or with replace replaces a job of the same kind
	// and name. ErrExists if it exists and replace is false.
	CreateJob(ctx context.Context, j Job, replace bool) error
	Job(ctx context.Context, kind, name string) (Job, error)
	// Jobs lists the jobs of kind, with their last runs.
	Jobs(ctx context.Context, kind string) ([]Job, error)
	// SetState changes a job's state and next run.
	SetState(ctx context.Context, kind, name, state string, next time.Time) error
	// SetSchedule changes a job's schedule and next run.
	SetSchedule(ctx context.Context, kind, name, schedule, timezone string, next time.Time) error
	DropJob(ctx context.Context, kind, name string) error
	// DisownJobs gives the jobs of owner, a user who was dropped, to
	// tombstone, a name no user can have, and suspends them, so that a
	// user created later with the same name does not inherit them.
	DisownJobs(ctx context.Context, owner, tombstone string) error
	// Scheduled lists started jobs with a next run, soonest first.
	Scheduled(ctx context.Context) ([]Job, error)
	// Claim moves a started job's next run from due to next, reporting
	// false if another replica did so first.
	Claim(ctx context.Context, kind, name string, due, next time.Time) (bool, error)

	// TryLock takes the lock named key if nobody holds it; Lock waits for
	// it. Locks are shared by every replica. Call release when done.
	TryLock(ctx context.Context, key string) (release func(), ok bool, err error)
	Lock(ctx context.Context, key string) (release func(), err error)
	// TryLease takes the lock named key as TryLock does, as a lease that
	// says whether it is still held: a lock is lost when its connection
	// to the metadata database drops, or when its holder cannot renew it
	// in time. A lease is not taken again until its holder released it or
	// it expired, and its holder takes it for lost before it expires (see
	// PostgresStore.TryLease).
	TryLease(ctx context.Context, key string) (lease Lease, ok bool, err error)

	// WriteRun records r, or updates the run with its ID.
	WriteRun(ctx context.Context, r Run) error
	// AbandonRuns marks a job's runs that are still running as failed:
	// the replica running them stopped.
	AbandonRuns(ctx context.Context, kind, name string) error
	// Runs returns the runs v may see, newest first.
	Runs(ctx context.Context, v audit.Viewer, p audit.Page) ([]Run, error)
	// Prune deletes runs, and failed loads, older than before.
	Prune(ctx context.Context, before time.Time) error
	// LastLoad is when the pipe's latest run that loaded files started,
	// or zero if none of the runs kept did.
	LastLoad(ctx context.Context, pipe string) (time.Time, error)

	// PreviousLoads returns the latest load of each of files into table.
	PreviousLoads(ctx context.Context, table string, files []string) (map[string]Load, error)
	// WriteLoads records loads, or updates those with their IDs.
	WriteLoads(ctx context.Context, ls []Load) error
	DeleteLoads(ctx context.Context, ids []string) error
	// Loads returns the loads v may see, newest first.
	Loads(ctx context.Context, v audit.Viewer, p audit.Page) ([]Load, error)
	// ForgetTable removes the load history and materialized view of a
	// dropped table, catalog.schema.table; RenameTable moves them.
	ForgetTable(ctx context.Context, table string) error
	RenameTable(ctx context.Context, from, to string) error

	Close()
}

// runKey carries the Run a Runner's job runs as.
type runKey struct{}

// RunOf returns the run that ctx, passed to Runner.RunJob, belongs to:
// its ID, trigger and scheduled time.
func RunOf(ctx context.Context) (Run, bool) {
	r, ok := ctx.Value(runKey{}).(Run)
	return r, ok
}

// JobKey names a job's lock.
func JobKey(kind, name string) string { return "job " + kind + "/" + strings.ToLower(name) }

// TableKey names the lock held while loading files into table.
func TableKey(table string) string { return "load " + strings.ToLower(table) }

// Lease is a lock that can tell whether it is still held.
type Lease interface {
	// Held returns an error if the lock was lost.
	Held(ctx context.Context) error
	Release()
}

// lockID turns a lock's name into a Postgres advisory lock key.
func lockID(key string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("igloo " + key))
	return int64(h.Sum64())
}

// viewerSees reports whether v may see a record of user.
func viewerSees(v audit.Viewer, user string) bool { return v.All || v.User == user }

// mvName is the job name of a materialized view stored as table,
// catalog.schema.table.
func mvName(table string) string {
	if _, rest, ok := strings.Cut(table, "."); ok {
		return rest
	}
	return table
}
