package distributed

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
)

// The coordinator talks to workers over HTTPS (see transport.go):
//
//	POST /v1/tasks   run a fragment; the response is a header line of JSON
//	                 (taskResult), then the result as a Parquet file
//	GET  /v1/status  the worker's version, size and activity (workerStatus)
//	POST /v1/purge   purge what erasure requests deleted (purgeRequest; see
//	                 erasure.go); the response is the worker's status
//
// and, for exchanges on workers' disks (see localx.go), workers and
// coordinators talk to workers over the same connections:
//
//	GET    /v1/exchanges/{qid}/{file}  a file of an exchange the worker wrote
//	POST   /v1/exchanges/{qid}/keep    the statement still runs
//	DELETE /v1/exchanges/{qid}         the statement is done with them
//
// A task is self-contained: the session settings that change what SQL
// means, the views the fragment reads (data files and the table's columns
// by Iceberg field ID) and the fragment itself. Workers keep nothing
// between tasks but DuckDB's caches.

const (
	tasksPath  = "/v1/tasks"
	statusPath = "/v1/status"
	purgePath  = "/v1/purge"
)

// taskRequest is one fragment over one share of the data.
type taskRequest struct {
	// Query identifies the statement, for logs.
	Query string `json:"query"`
	Task  int    `json:"task"`
	// Version is the coordinator's Igloo version; workers refuse others,
	// since every machine must run the same DuckDB.
	Version  string    `json:"version"`
	Settings []setting `json:"settings,omitempty"`
	// Variables are values the coordinator computed, which SQL reads with
	// getvariable() (see values.go).
	Variables []variable `json:"variables,omitempty"`
	Views     []viewDef  `json:"views"`
	SQL       string     `json:"sql"`
	// Columns, if set, rename the result's columns positionally.
	Columns []string `json:"columns,omitempty"`
	// Shuffle, if set, makes the task write its result to this directory
	// in object storage, partitioned by its __b column, instead of sending
	// it back; the response lists the files. Partitions is how many
	// partitions there are, a row group of each of which the writer holds.
	Shuffle    string `json:"shuffle,omitempty"`
	Partitions int    `json:"partitions,omitempty"`
	// Local, if set, makes a shuffle task write its partitions under this
	// directory of the worker's own disk instead, {qid}/s{stage}/{task}/a
	// {attempt}, if Expect, the bytes it is expected to write, fit its share
	// of the free space; otherwise it writes to Shuffle.
	Local  string `json:"local,omitempty"`
	Expect int64  `json:"expect,omitempty"`
	// Worker is the address the coordinator reached the worker at, which
	// tells it which of the exchanges' files on workers' disks are its own;
	// Peers are the workers whose disks those files may be on.
	Worker string   `json:"worker,omitempty"`
	Peers  []string `json:"peers,omitempty"`
	// Erased is the latest deletion of erasure requests the coordinator
	// knew of when the statement began, in Unix nanoseconds: a worker
	// purged through less purges first, and one purged through more
	// refuses the task (see erasure.go).
	Erased int64 `json:"erased,omitempty"`
}

// purgeRequest has a worker purge what erasure requests deleted through
// Through, Unix nanoseconds (see erasure.go).
type purgeRequest struct {
	Through int64 `json:"through"`
}

// setting is a session setting the fragment runs with.
type setting struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// viewDef is a table as a fragment reads it: the data files of its
// snapshot, or of one worker's share of it, read by field ID; or, with
// ByName, files of one partition of the table's rows that workers wrote,
// read by column name.
type viewDef struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
	Files   []string `json:"files"`
	ByName  bool     `json:"by_name,omitempty"`
	// Rows, if not 0, is how many rows the files hold (from Iceberg
	// metadata) or about how many (for partitions), for DuckDB to plan
	// joins by: estimating from Parquet footers, it took a table of 25 rows
	// in 8 files for 8,000 and ordered TPC-H Q5's joins so that it ran eight
	// times as long.
	Rows int64 `json:"rows,omitempty"`
	// Exact says the files hold exactly Rows rows, as the files of an
	// exchange do, which the worker checks before it reads them: a file
	// lost or read twice fails the task rather than its result.
	Exact bool `json:"exact,omitempty"`
	// Deletes are the delete files of those of Files that have any, which
	// the worker applies.
	Deletes []fileDeletes `json:"deletes,omitempty"`
	// Prefetch, for a view of a catalog table's or an exchange's files when
	// the coordinator has prefetching on (Options.Prefetch), is what a
	// worker's prefetcher needs to read them ahead of DuckDB
	// (WorkerOptions.Prefetch).
	Prefetch *viewPrefetch `json:"prefetch,omitempty"`
	// lent are files on the worker's own disk that stand for exchange files
	// of workers' disks (see fetchExchanges), which it may read though they
	// are outside the warehouse; never from a task.
	lent map[string]bool
}

// viewPrefetch is what a task reads of a view's files, for prefetching.
type viewPrefetch struct {
	// Sizes are the sizes of the view's Files, in their order.
	Sizes []int64 `json:"sizes"`
	// Used are the field IDs of the table's columns the task's query may
	// read, with their nested fields' (see Scan.FieldIDs): none when it
	// reads only row counts.
	Used []int `json:"used"`
	// Filtered says the task's query filters the view by one of its
	// columns, so DuckDB may skip row groups: the prefetcher reads less of
	// it ahead (see Scan.Filtered).
	Filtered bool `json:"filtered,omitempty"`
}

// fileDeletes are the delete files of one data file of a view.
type fileDeletes struct {
	File string `json:"file"`
	// Rows is the data file's record count, deleted rows included.
	Rows int64 `json:"rows,omitempty"`
	Deletes
}

// deletesOf returns the delete files of files, for a view over them.
func deletesOf(files []File) []fileDeletes {
	var out []fileDeletes
	for _, f := range files {
		if f.Deletes != nil {
			out = append(out, fileDeletes{File: f.Path, Rows: f.Rows, Deletes: *f.Deletes})
		}
	}
	return out
}

// taskResult heads a task's response.
type taskResult struct {
	// Columns are the result's columns, with their DuckDB types.
	Columns []Column `json:"columns,omitempty"`
	Rows    int64    `json:"rows"`
	// Files are a shuffle task's output files.
	Files []shuffleFile `json:"files,omitempty"`
	Error *taskErr      `json:"error,omitempty"`
}

// shuffleFile is a file of rows a shuffle task wrote for one partition.
type shuffleFile struct {
	Path      string `json:"path"`
	Partition int    `json:"partition"`
	// Rows is how many rows the file holds, and Size its bytes.
	Rows int64 `json:"rows"`
	Size int64 `json:"size"`
}

// taskErr is a failed task.
type taskErr struct {
	Message string `json:"message"`
	// Type is the DuckDB error type, when DuckDB raised it.
	Type int `json:"type,omitempty"`
	// Data says the error came from the data the fragment computes on,
	// such as a failed cast, rather than from running it.
	Data bool `json:"data,omitempty"`
	// Unsupported says no worker can run the task, so the statement runs
	// on the coordinator: its files cannot be read as the table says.
	Unsupported bool `json:"unsupported,omitempty"`
	// OutOfMemory says the task ran out of memory: DuckDB's memory limit,
	// or the worker canceled it because it was running out (ShedMemory).
	OutOfMemory bool `json:"out_of_memory,omitempty"`
	// Bitmap says the task's roaring bitmaps outgrew the memory they may
	// take (the rb extension's error); it is not OutOfMemory.
	Bitmap bool `json:"bitmap,omitempty"`
	// Lost is the worker whose exchange files the task could not fetch:
	// it stopped, or restarted without them.
	Lost string `json:"lost,omitempty"`
	// Throttled says object storage turned the task's requests away for
	// now (see throttled): the same task may well run a moment later.
	Throttled bool `json:"throttled,omitempty"`
	// Erased, if set, says the worker refused the task: it was purged of
	// what erasure requests deleted through Erased, later than the
	// statement began (see erasure.go).
	Erased int64 `json:"erased,omitempty"`
}

func (e *taskErr) Error() string { return e.Message }

// throttledError is a task that failed because object storage turned its
// requests away for now, past DuckDB's own retries (see storageRetries).
type throttledError struct{ msg string }

func (e *throttledError) Error() string { return e.msg }

// throttleSigns are how a request object storage turned away for now
// reads in DuckDB's errors (HTTP Error: Request returned HTTP 503 for HTTP
// HEAD to ...) and in S3's: S3 answers 503 SlowDown when one prefix gets
// more requests than it serves at once, as an exchange's files of a
// statement on 16 workers did at a hundred times the data.
var throttleSigns = []string{
	"HTTP 429", "HTTP 500", "HTTP 502", "HTTP 503", "HTTP 504",
	"SlowDown", "Service Unavailable", "ServiceUnavailable", "TooManyRequests", "Reduce your request rate",
}

// throttled reports whether a task's error is object storage turning its
// requests away for now, rather than anything the task does.
func throttled(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, s := range throttleSigns {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// workerStatus is what a worker reports about itself.
type workerStatus struct {
	// ID names the worker; it decides which files the worker is given, so
	// the same files go to the same worker and its read cache.
	ID          string `json:"id"`
	Version     string `json:"version"`
	Cores       int    `json:"cores"`
	MemoryBytes int64  `json:"memory_bytes"`
	// Slots are the tasks it runs at once (see taskSlots), which share its
	// memory, and Threads its DuckDB threads; zero from a worker that does
	// not say.
	Slots   int `json:"slots,omitempty"`
	Threads int `json:"threads,omitempty"`
	// InstanceType, Region and Spot say what machine it runs on, where its
	// cloud's metadata says (see WorkerOptions).
	InstanceType string `json:"instance_type,omitempty"`
	Region       string `json:"region,omitempty"`
	Spot         bool   `json:"spot,omitempty"`
	// Boot is random to each run of the worker.
	Boot        string  `json:"boot,omitempty"`
	Running     int     `json:"running"`
	IdleSeconds float64 `json:"idle_seconds"`
	// LocalRoom is the room its disk has for exchanges (see localFor): the
	// share of its exchange directory's free space a task may take
	// (localShare), within IGLOO_EXCHANGE_MAX_SIZE.
	LocalRoom int64 `json:"local_room,omitempty"`
	// Kubernetes says it runs in a Kubernetes pod.
	Kubernetes bool `json:"kubernetes,omitempty"`
	// Cache names what the worker keeps of what it read past its tasks,
	// for erasure requests (WorkerErasure.Cache), and Erased is the latest
	// deletion of erasure requests it was purged of, in Unix nanoseconds;
	// empty and zero from a worker that does not say.
	Cache  string `json:"cache,omitempty"`
	Erased int64  `json:"erased,omitempty"`
	// Alone is when it last emptied its caches for having heard from no
	// coordinator for a while (WorkerErasure.Isolated), in Unix
	// nanoseconds; zero for never.
	Alone int64 `json:"alone,omitempty"`
}

// SettingNames are the session settings that change what a query computes.
// The coordinator sends their values with every task.
var SettingNames = []string{
	"TimeZone", "Calendar", "default_collation", "default_null_order", "default_order",
	"integer_division", "ieee_floating_point_ops", "order_by_non_integer_literal",
	"scalar_subquery_error_on_multiple_rows", "lambda_syntax", "preserve_insertion_order",
	"enable_geoparquet_conversion", "binary_as_string", "arrow_large_buffer_size",
}

// ChangedSettingNames are the session settings tasks take over only where
// the session changed them: the rb extension's, which Igloo sets for the
// statements that count distinct IDs in roaring bitmaps, and which a
// task's own rb_agg reads (its memory budget).
var ChangedSettingNames = []string{"rb_count_distinct", "rb_memory_budget"}

func knownSetting(name string) bool {
	eq := func(s string) bool { return strings.EqualFold(s, name) }
	return slices.ContainsFunc(SettingNames, eq) || slices.ContainsFunc(ChangedSettingNames, eq)
}

// DataError reports whether err is an error in the data a statement
// computes on, which it would have raised on one machine too.
func DataError(err error) bool { return dataError(err) }

// ErrExchangeLost is what a statement fails with when a worker that held
// files of its exchanges on its disk was lost before every reader had
// them, also after running again without that worker (see Execute).
var ErrExchangeLost = errors.New("a worker holding exchanged rows on its disk was lost")

// lostError is a worker lost with exchange files on its disk.
type lostError struct {
	worker string
	err    error
}

func (e *lostError) Error() string {
	return fmt.Sprintf("%v: %s: %v", ErrExchangeLost, e.worker, e.err)
}
func (e *lostError) Unwrap() []error { return []error{ErrExchangeLost, e.err} }

// ErrWorkerMemory is what a statement fails with when a task of it ran out
// of memory on a worker, in DuckDB or canceled by the worker's memory watch
// (Worker.ShedMemory), also over smaller tasks (see Execute). Another
// worker would run out of memory the same way, and the coordinator on its
// own sooner, so the statement fails rather than run again.
var ErrWorkerMemory = errors.New("a worker was running out of memory")

// OutOfMemory reports whether err is ErrWorkerMemory.
func OutOfMemory(err error) bool { return errors.Is(err, ErrWorkerMemory) }

// ErrBitmapMemory is what a statement fails with when the roaring bitmaps
// of a task's count(DISTINCT) outgrew the memory they may take on a worker
// (the rb extension's error, which starts bitmapBudgetError). Smaller tasks
// would build the same bitmaps a group, so the statement is not run again
// in parts: the session runs it once more counting with DuckDB's hash.
var ErrBitmapMemory = errors.New("a task's bitmaps did not fit in a worker's memory")

// bitmapBudgetError starts the rb extension's error for bitmaps that do
// not fit.
const bitmapBudgetError = "rb: bitmap memory budget exceeded"

// BitmapMemory reports whether err is ErrBitmapMemory.
func BitmapMemory(err error) bool { return errors.Is(err, ErrBitmapMemory) }

// dataError reports whether a DuckDB error came from the values a fragment
// computes on (a failed cast, a division by zero), which a single machine
// would have raised as well, rather than from reading files.
func dataError(err error) bool {
	var de *duckdb.Error
	if !errors.As(err, &de) {
		return false
	}
	switch de.Type {
	case duckdb.ErrorTypeConversion, duckdb.ErrorTypeInvalidInput, duckdb.ErrorTypeOutOfRange, duckdb.ErrorTypeDivideByZero:
	default:
		return false
	}
	// Reading a data file can fail with a conversion error too, when a
	// file's type does not cast to the table's: that is the machinery's
	// failure, not the statement's. DuckDB words those as below; the query
	// text it quotes after "LINE" may mention anything.
	msg, _, _ := strings.Cut(de.Msg, "\nLINE ")
	msg = strings.ToLower(msg)
	return !strings.Contains(msg, "while reading file") && !strings.Contains(msg, "parquet")
}

// Parquet keeps most DuckDB types as they are. These it does not: it
// writes HUGEINT and UHUGEINT as DOUBLE, drops the microseconds of an
// INTERVAL and the offset of a TIME WITH TIME ZONE, and has no UNION.
// Workers send them as text, and the coordinator casts each column back to
// its type, which also restores ENUMs, fixed-size arrays and the coarser
// timestamps, which Parquet widens.
var textTypes = []string{"HUGEINT", "UHUGEINT", "INTERVAL", "TIME WITH TIME ZONE"}

// transferType returns how a column of type typ crosses in Parquet: as
// itself or as VARCHAR. It fails for nested types holding a type that
// Parquet changes.
func transferType(typ string) (string, error) {
	upper := strings.ToUpper(typ)
	if slices.Contains(textTypes, upper) {
		return "VARCHAR", nil
	}
	if strings.HasPrefix(upper, "UNION") {
		return "", fmt.Errorf("a result column of type %s", typ)
	}
	for _, t := range append(slices.Clone(textTypes), "UNION(") {
		if strings.Contains(upper, t) {
			return "", fmt.Errorf("a result column of type %s", typ)
		}
	}
	return typ, nil
}
