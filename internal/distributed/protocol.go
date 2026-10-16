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
//
// A task is self-contained: the session settings that change what SQL
// means, the views the fragment reads (data files and the table's columns
// by Iceberg field ID) and the fragment itself. Workers keep nothing
// between tasks but DuckDB's caches.

const (
	tasksPath  = "/v1/tasks"
	statusPath = "/v1/status"
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
	Views    []viewDef `json:"views"`
	SQL      string    `json:"sql"`
	// Columns, if set, rename the result's columns positionally.
	Columns []string `json:"columns,omitempty"`
	// Shuffle, if set, makes the task write its result to this directory
	// in object storage, partitioned by its __b column, instead of sending
	// it back; the response lists the files.
	Shuffle string `json:"shuffle,omitempty"`
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
}

func (e *taskErr) Error() string { return e.Message }

// workerStatus is what a worker reports about itself.
type workerStatus struct {
	// ID names the worker; it decides which files the worker is given, so
	// the same files go to the same worker and its read cache.
	ID          string  `json:"id"`
	Version     string  `json:"version"`
	Threads     int     `json:"threads"`
	MemoryBytes int64   `json:"memory_bytes"`
	Running     int     `json:"running"`
	IdleSeconds float64 `json:"idle_seconds"`
}

// SettingNames are the session settings that change what a query computes.
// The coordinator sends their values with every task.
var SettingNames = []string{
	"TimeZone", "Calendar", "default_collation", "default_null_order", "default_order",
	"integer_division", "ieee_floating_point_ops", "order_by_non_integer_literal",
	"scalar_subquery_error_on_multiple_rows", "lambda_syntax", "preserve_insertion_order",
	"enable_geoparquet_conversion", "binary_as_string", "arrow_large_buffer_size",
}

func knownSetting(name string) bool {
	return slices.ContainsFunc(SettingNames, func(s string) bool { return strings.EqualFold(s, name) })
}

// DataError reports whether err is an error in the data a statement
// computes on, which it would have raised on one machine too.
func DataError(err error) bool { return dataError(err) }

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
