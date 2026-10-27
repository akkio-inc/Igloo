// Package erasure carries out requests to erase a person's data from the
// warehouse, as the GDPR and the CCPA require, and keeps a record of each
// that shows it was done.
//
// A request names identifiers by tag value: ERASE SUBJECT WHERE
// pii.email = 'a@b.com' erases the rows whose columns tagged pii = 'email'
// hold a@b.com, and those whose columns tagged pii = 'email:sha256' hold
// its SHA-256 in hex. Values compare as DuckDB's lower(trim(...)) leaves
// them. While a request is open it keeps the identifiers sealed under the
// erasure key, which is not in the metadata database, and finds rows by
// hashing column values with a salt derived from that key; once it
// completes, it keeps neither.
//
// A request runs as a job on the scheduler (package tasks), on one replica
// at a time, through these steps. Each is recorded when it ends, and each
// works from the tables as they are when it runs, so a job that stops
// resumes where it was, and running a step again is harmless:
//
//  1. find: the tables with tagged columns, the tables that share data
//     files with them (clones), and those history says were written from
//     them, which are reported, and erased too if they have the tags. A
//     request that would delete much of a table waits here to be
//     confirmed;
//  2. delete: DELETE the matching rows, as any statement would, which adds
//     delete files beside the data files that hold them;
//  3. rewrite: on every branch and tag, write the data files that hold
//     matching rows again without them, and the manifests that still list
//     the old files, whose column bounds can hold the values;
//  4. expire: expire the snapshots that still reference such files, as
//     recent as they are: time travel to them ends, while older snapshots
//     that do not hold the person's data stay;
//  5. purge: delete from object storage the files that no snapshot
//     references any more, and orphan files old enough to be safe to;
//  6. sharing: delete the Delta Sharing copies made from those files;
//  7. history: redact the identifiers from the query and load history;
//  8. cache: wait until every running replica emptied its read cache;
//  9. verify: look for the identifiers in what is left: table rows, every
//     file any snapshot references, manifest bounds, Parquet statistics,
//     sharing copies, history. Anything found sends the request back to
//     the step that removes it.
package erasure

import (
	"context"
	"errors"
	"time"
)

// JobKind is the kind of the scheduler's jobs that run requests.
const JobKind = "erasure"

// Statuses of a request.
const (
	StatusQueued  = "queued"
	StatusRunning = "running"
	// StatusRetrying is a request whose last run failed; Error says why.
	// It runs again after a while, longer each time it fails.
	StatusRetrying = "retrying"
	// StatusWaiting is a request that waits for something to happen
	// before it can go on: replicas to empty their caches, or unreferenced
	// files that may still be part of a write in progress to grow old
	// enough to delete.
	StatusWaiting = "waiting"
	// StatusHeld is a request that would delete much of a table, which
	// waits for CONFIRM ERASURE REQUEST before it deletes anything.
	StatusHeld = "held"
	// StatusBlocked is a request that has begun deleting and cannot go on
	// until someone fixes what Error says, as when another engine upgraded
	// one of its tables to a format version Igloo cannot rewrite. It never
	// fails: it keeps its identifiers sealed, alerts, and tries again every
	// blockedWait, going on once the cause is gone.
	StatusBlocked = "blocked"
	// StatusCanceled is a request canceled before it deleted anything.
	StatusCanceled  = "canceled"
	StatusCompleted = "completed"
	// StatusFailed is a request that failed too often to go on, or that
	// it could not carry out, before it sent a DELETE; Error says why.
	// Make a new request once that is fixed.
	StatusFailed = "failed"
	// StatusClosed is a blocked request an admin closed (see
	// Service.Close): what it erased stays erased, its tables' Left say
	// what it did not, and it forgot its identifiers.
	StatusClosed = "closed"
)

// Steps of a request, in order.
const (
	StepFind    = "find"
	StepDelete  = "delete"
	StepRewrite = "rewrite"
	StepExpire  = "expire"
	StepPurge   = "purge"
	StepSharing = "sharing"
	StepHistory = "history"
	StepModels  = "models"
	StepCache   = "cache"
	StepVerify  = "verify"
	StepDone    = "done"
)

var steps = []string{StepFind, StepDelete, StepRewrite, StepExpire, StepPurge, StepSharing, StepHistory, StepModels, StepCache, StepVerify,
	StepDone}

func nextStep(step string) string {
	for i, s := range steps {
		if s == step && i+1 < len(steps) {
			return steps[i+1]
		}
	}
	return StepDone
}

var (
	ErrNotFound = errors.New("does not exist")
	// ErrInvalid is a request that cannot be carried out as written.
	ErrInvalid = errors.New("invalid erasure request")
	// ErrBusy means the request is being run on some replica; try again
	// shortly.
	ErrBusy = errors.New("the erasure request is running; try again in a moment")
	// ErrNotYet means the request cannot be closed now (see Service.Close):
	// what it must do first could not be done.
	ErrNotYet = errors.New("the erasure request cannot be closed yet")
)

// Request is an erasure request and its progress. Its record never holds
// the identifiers it erases, or their hashes: only, while it is open, the
// identifiers sealed under the erasure key.
type Request struct {
	ID        string    `json:"id"`
	Requester string    `json:"requester"`
	Created   time.Time `json:"created"`
	Updated   time.Time `json:"updated"`
	Completed time.Time `json:"completed,omitzero"`
	Status    string    `json:"status"`
	// Step is the step it runs next, or done.
	Step     string    `json:"step"`
	Subjects []Subject `json:"subjects"`
	// Salt and Sealed are, while the request is open, a random value that
	// the salt of its hashes is derived from, and its identifiers sealed
	// under the erasure key. Both go when it ends.
	Salt   string `json:"salt,omitempty"`
	Sealed string `json:"sealed,omitempty"`
	// SealedIDs are, while the request is open, the ids that ID domains'
	// dictionaries gave the values it erases, by domain, sealed under the
	// erasure key like the identifiers: what derived sets of ids, such as
	// bitmap indexes, still hold (see IDSets). They go when it ends.
	SealedIDs string `json:"sealed_ids,omitempty"`
	// Tables are what the request erases from.
	Tables []Table `json:"tables"`
	// Downstream are tables history says were written from Tables.
	Downstream []Downstream `json:"downstream"`
	// HistoryRedacted counts history records it redacted, and
	// DerivedPurged what it purged of what was derived from its tables.
	HistoryRedacted int64 `json:"history_redacted"`
	DerivedPurged   int64 `json:"derived_purged,omitempty"`
	// ModelEpoch is the models' erasure epoch its models step moved on to
	// (see Models), in this round of its steps; ModelsRetrained counts the
	// models it trained again, and ModelBlobs are the SHA-256s of the blobs
	// they had, which it deletes. ModelsFailed are the models it could not
	// train again, with why; ModelsLeft, once it was closed, those that may
	// still hold what they learned of the person.
	ModelEpoch      int64    `json:"model_epoch,omitempty"`
	ModelsRetrained int      `json:"models_retrained,omitempty"`
	ModelBlobs      []string `json:"model_blobs,omitempty"`
	ModelsFailed    []string `json:"models_failed,omitempty"`
	ModelsLeft      []string `json:"models_left,omitempty"`
	// WorkersLeft are, once it was closed, the workers of distributed
	// execution that had not purged what it deleted (see workersLeft).
	WorkersLeft  []string      `json:"workers_left,omitempty"`
	Steps        []StepRecord  `json:"steps"`
	Verification *Verification `json:"verification,omitempty"`
	// Error is why the last run failed, while Status is retrying or
	// failed, or what it waits for.
	Error    string `json:"error,omitempty"`
	Attempts int    `json:"attempts"`
	// Failures counts the runs in a row that failed.
	Failures int `json:"failures,omitempty"`
	// Rounds counts how often verification sent it back to an earlier
	// step.
	Rounds int `json:"rounds"`
	// WaitUntil is, while it waits or retries, when it runs next.
	WaitUntil time.Time `json:"wait_until,omitzero"`
	// DeletedThrough is when it last deleted files, which replicas'
	// read caches must have been emptied since.
	DeletedThrough time.Time `json:"deleted_through,omitzero"`
	// ConfirmedBy is who confirmed it after it was held.
	ConfirmedBy string `json:"confirmed_by,omitempty"`
	// ClosedBy is the admin who closed it while it was blocked (see
	// Service.Close).
	ClosedBy string `json:"closed_by,omitempty"`
	// DeleteStarted says it sent its first DELETE, which may have
	// committed even if no answer came: it can no longer be canceled.
	DeleteStarted bool `json:"delete_started,omitempty"`
	// Counted says it counted the rows it matches in its tables, which
	// other requests' holds count too; CountWait is when it began waiting
	// for others made about when it was to count theirs.
	Counted   bool      `json:"counted,omitempty"`
	CountWait time.Time `json:"count_wait,omitzero"`

	// salt is the salt of the request's hashes, derived from Salt and the
	// erasure key, and written the files it wrote, while it runs.
	salt    string
	written map[string]bool
	// domainIDs are SealedIDs unsealed, while it runs.
	domainIDs map[string][]int64
	// fold normalizes values as DuckDB does, for what Go compares.
	fold *folding
	// recheck, if set, is when the request looks again at what it waits
	// for, before WaitUntil (see purge).
	recheck time.Time
}

// nextRun is when r, waiting, runs next: at WaitUntil, or at recheck if
// that comes first.
func (r *Request) nextRun() time.Time {
	if !r.recheck.IsZero() && r.recheck.Before(r.WaitUntil) {
		return r.recheck
	}
	return r.WaitUntil
}

// Subject is a tag value the request erases identifiers of.
type Subject struct {
	Tag   string `json:"tag"`
	Value string `json:"value"`
	// Count is how many identifiers the request names.
	Count int `json:"identifiers"`

	// What follows exists only while the request runs, unsealed from
	// Request.Sealed.
	ids []identifier
	// plain are the salted hashes of the identifiers as columns tagged
	// with Value hold them; digests, by algorithm, those of their hex
	// digests, which columns tagged Value:algorithm hold; prefixes those
	// of what Iceberg truncates long ones to in manifest bounds.
	plain    []string
	digests  map[string][]string
	prefixes []string
}

// Table is a table a request erases from, and what it did there.
type Table struct {
	// Name is the table's name in the catalog, its namespace and name
	// joined with dots; Ident is the same as the catalog has it, and UUID
	// the table's, which tell it from a table of the same name made later.
	Name    string   `json:"table"`
	Ident   []string `json:"identifier,omitempty"`
	UUID    string   `json:"uuid,omitempty"`
	Columns []Column `json:"columns"`
	// Reason is "tagged", or how it shares data files with a tagged table.
	Reason string `json:"reason"`
	// Matched is how many of its rows matched when the request began, of
	// Rows.
	Matched     int64 `json:"rows_matched"`
	Rows        int64 `json:"rows,omitempty"`
	RowsDeleted int64 `json:"rows_deleted"`
	// ValuesNulled counts values of columns to be nulled that it set to
	// NULL.
	ValuesNulled     int64 `json:"values_nulled,omitempty"`
	FilesRewritten   int   `json:"files_rewritten"`
	SnapshotsExpired int   `json:"snapshots_expired"`
	FilesDeleted     int   `json:"files_deleted"`
	// FilesOutside counts files outside the warehouse that held matching
	// rows or only expired snapshots referenced, which the request leaves
	// to whoever keeps them (see the log).
	FilesOutside int `json:"files_outside_warehouse,omitempty"`
	// Refs are the branches and tags it rewrote besides main.
	Refs []string `json:"refs,omitempty"`
	// Steps are when each step last finished for the table.
	Steps map[string]time.Time `json:"steps,omitempty"`
	// Remote says the table is a connection's, in another database
	// (package federation): the request reports it and erases nothing
	// there.
	Remote bool `json:"remote,omitempty"`
	// Blocked says why the request cannot go on with the table now, once
	// it has begun deleting, as when another engine dropped it or made it
	// one the request cannot rewrite; BlockedAt is the step it stopped at.
	// Its other tables go on through every step without it, and the
	// request, then blocked, takes it up again from that step every
	// blockedWait.
	Blocked   string `json:"blocked,omitempty"`
	BlockedAt string `json:"blocked_at,omitempty"`
	// Failures counts the runs in a row that failed with this table, which
	// is set aside after maxFailures of them.
	Failures int `json:"failures,omitempty"`
	// Left says why the request was closed (see Service.Close) without
	// finishing with the table.
	Left string `json:"left,omitempty"`
	// Gone says the table was dropped while the request ran, and none has
	// its name now: the request goes on without it, and its files, under
	// a location no table has any more, go as any such files do (see
	// unowned).
	Gone bool `json:"gone,omitempty"`
}

// Column is a column that holds identifiers.
type Column struct {
	Name  string `json:"name"`
	Tag   string `json:"tag"`
	Value string `json:"value"`
	// Digest is the algorithm whose hex digests of the identifiers the
	// column holds, or empty.
	Digest string `json:"digest,omitempty"`
	// Null says the request sets the column to NULL in matching rows
	// rather than deleting them: the column names the person in rows
	// about someone else (pii = 'email:null').
	Null bool `json:"null,omitempty"`
}

// Downstream is a table written from tables a request erases from.
type Downstream struct {
	Table string   `json:"table"`
	From  []string `json:"from"`
	// Via is what tells: "query history", "materialized view" or "task".
	Via string `json:"via"`
	// Erased says the request erases from it too, because it has the
	// tags; otherwise tag it and ask again.
	Erased bool `json:"erased"`
}

// StepRecord is when a step of a request ran.
type StepRecord struct {
	Step     string    `json:"step"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	Runs     int       `json:"runs"`
}

// Verification is what the last check of a request found: counts, never
// values. A request completes when all of them but Pending are zero.
type Verification struct {
	At time.Time `json:"at"`
	// Rows are matching rows in the tables' current data.
	Rows int64 `json:"rows"`
	// Files are data and delete files that any snapshot references and
	// that hold matching rows, deleted or not.
	Files int64 `json:"files"`
	// Bounds are manifest entries, live or deleted, whose column bounds
	// match.
	Bounds int64 `json:"bounds"`
	// Statistics are Parquet column chunks whose minimum or maximum
	// matches.
	Statistics int64 `json:"statistics"`
	// Storage are files the request deleted that object storage still
	// has.
	Storage int64 `json:"storage"`
	// Orphans are unreferenced files with matching rows that are too new
	// to delete: they may belong to a write in progress.
	Orphans int64 `json:"orphans"`
	// Sharing are Delta Sharing copies with matching rows.
	Sharing int64 `json:"sharing"`
	// History are history records that still hold an identifier.
	History int64 `json:"history"`
	// Derived are entries derived from the tables, such as AI functions'
	// cached answers, that statements begun before the request purged
	// them kept.
	Derived int64 `json:"derived"`
	// Models are models trained from the tables before the request's
	// models step, and the blobs that models it trained again had that
	// object storage still has (see Models).
	Models int64 `json:"models"`
	// Caches are the read caches of running replicas that were emptied
	// since the request deleted files; Pending are those of replicas not
	// running now, which are emptied when they start.
	Caches  int `json:"caches"`
	Pending int `json:"caches_pending"`
	// IDSets are derived sets of ids, such as bitmap indexes' bitmaps,
	// that still hold an id the request erased (see IDSets).
	IDSets int64 `json:"id_sets"`
	// Workers are workers of distributed execution whose read caches
	// outlive them, not running now, that have not purged what the request
	// deleted (see workersLeft).
	Workers int `json:"workers"`
}

// File is a file a request deletes once nothing references it.
type File struct {
	Path  string
	Table string
	// Kind is why: "matched" (it held matching rows), "expired" (only
	// expired snapshots referenced it), "orphan" or "sharing".
	Kind    string
	Deleted time.Time
}

// Cache is a replica's read cache.
type Cache struct {
	ID   string
	Host string
	Seen time.Time
	// Emptied is when it was last emptied of what requests deleted.
	Emptied time.Time
	// Token is the purge token the cache's disk was given when it was last
	// emptied (see Purger.OnDisk), empty for a cache in memory.
	Token string
}

// Store keeps requests and what they delete. Implementations must be safe
// for concurrent use.
type Store interface {
	Create(ctx context.Context, r *Request) error
	// Get returns ErrNotFound for a request that does not exist.
	Get(ctx context.Context, id string) (*Request, error)
	// List returns every request, newest first.
	List(ctx context.Context) ([]*Request, error)
	Save(ctx context.Context, r *Request) error

	// AddFiles records files to delete, keeping those already recorded.
	AddFiles(ctx context.Context, id string, files []File) error
	Files(ctx context.Context, id string) ([]File, error)
	// Deleted records that files were deleted at at.
	Deleted(ctx context.Context, id string, paths []string, at time.Time) error
	// Forget removes the request's files: their paths can hold
	// identifiers, as partition values.
	Forget(ctx context.Context, id string) error

	// Seen records that the replica with read cache cache is running.
	Seen(ctx context.Context, cache, host string, at time.Time) error
	Caches(ctx context.Context) ([]Cache, error)
	// Emptied records that cache was emptied of what requests deleted up
	// to through, and the purge token its disk was given then, if any.
	Emptied(ctx context.Context, cache string, through time.Time, token string) error
	// Claim takes, or renews, holder's claim on cache for ttl, unless
	// another holder's claim on it is current, and returns the purge token
	// last recorded for it (see Emptied). One running replica at a time
	// uses a cache's ID.
	Claim(ctx context.Context, cache, holder string, ttl time.Duration) (ok bool, token string, err error)
	// Release lets holder's claim on cache go.
	Release(ctx context.Context, cache, holder string) error
	// ForgetCache removes cache from the ledger, as a worker's that will
	// not come back (see Service.DeregisterWorker).
	ForgetCache(ctx context.Context, cache string) error
	// LatestDeletion is when any request last deleted files.
	LatestDeletion(ctx context.Context) (time.Time, error)
	Close()
}
