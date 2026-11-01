package distributed

import (
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// Workers writing tables' files. A statement that fills a table from a
// query (see pgwire's fillOn) runs the query where auto mode would run it
// as a read. Where each of the last stage's tasks holds rows of the
// statement's result, as a copy's and a grouping completed on workers' do,
// the tasks write those rows themselves, as Parquet with the table's field
// IDs, and the coordinator commits the files in one Iceberg commit, rather
// than fetch every row and write it alone.
//
// What a worker may write is as little as the coordinator can make it:
//
//   - A statement writes under one directory of its own under the table's
//     location, named WriteDirMark and a random suffix. A worker refuses a
//     directory without the mark, or outside the warehouse and the allowed
//     locations.
//   - Each attempt of a task writes a directory of its own under it, whose
//     files count only if the coordinator hears from that attempt. The
//     coordinator refuses files outside it (checkWritten), and reads each
//     file's footer itself before it commits: what is committed is what it
//     read, not what a worker said (see pgwire's tableWrite).
//   - Where the coordinator can have credentials made for it
//     (Options.WriteCredentials: on S3, STS with a session policy), each
//     attempt carries credentials that reach its directory and nothing else,
//     with no right to delete, for an hour at most, which the worker's DuckDB
//     uses for that directory alone. Otherwise the worker writes with its own
//     identity, whose grant the deployment keeps to directories of that name
//     (see the deploy modules): a worker denied the write leaves it to the
//     coordinator (ErrWriteDenied).
//   - Whatever is not committed is deleted when the statement ends, and a
//     note in the catalog proves what a stopped process left (see pgwire's
//     tableWrite). No snapshot references such files, so they are orphan
//     files under the table's location to erasure requests and maintenance,
//     as a stopped commit's of any engine are.

// WriteDirMark starts the names of statements' directories of table files
// workers write.
const WriteDirMark = "igloo-write-"

// WriteColumn is a column of a table's files: its field ID, by name.
type WriteColumn struct {
	Name string `json:"name"`
	ID   int    `json:"id"`
}

// WriteCredentials reach an attempt's directory and nothing else, for the
// worker's DuckDB. They are secrets: never logged, never in an error.
type WriteCredentials struct {
	KeyID        string    `json:"key_id"`
	Secret       string    `json:"secret"`
	SessionToken string    `json:"session_token,omitempty"`
	Region       string    `json:"region,omitempty"`
	Endpoint     string    `json:"endpoint,omitempty"`
	URLStyle     string    `json:"url_style,omitempty"`
	UseSSL       bool      `json:"use_ssl,omitempty"`
	Expires      time.Time `json:"expires,omitzero"`
}

// WriteSource stands for a task's rows in WriteTarget.Select.
const WriteSource = "__igloo_rows_source"

// WriteTarget says where a statement's tasks write its rows, and how: Dir
// is the statement's directory; Select turns the query's result, read
// from WriteSource, into the rows the files hold, with the table's columns'
// names and types, then those Partitions name, which say the file a row
// goes to and are not written; Columns are the table's columns' field IDs.
// FileBytes bounds an unpartitioned file; RowGroup and Compression are the
// table's, if set.
type WriteTarget struct {
	Dir         string
	Select      string
	Columns     []WriteColumn
	Partitions  []string
	FileBytes   int64
	RowGroup    int64
	Compression string
}

// WrittenFile is a file a task wrote, as the worker says: pgwire's commit
// reads the file's footer and compares.
type WrittenFile struct {
	Path string
	Rows int64
	Size int64
}

// writeFilesDir is where in its directory an attempt writes: DuckDB looks at
// the path it writes to before it writes, and credentials reach what is under
// the attempt's directory, not the directory's own name.
const writeFilesDir = "/f"

// writeRequest is a task's part of a WriteTarget: what its SQL does not
// say. Dir is the attempt's directory.
type writeRequest struct {
	Dir         string            `json:"dir"`
	Columns     []WriteColumn     `json:"columns"`
	Partitions  []string          `json:"partitions,omitempty"`
	FileBytes   int64             `json:"file_bytes,omitempty"`
	RowGroup    int64             `json:"row_group,omitempty"`
	Compression string            `json:"compression,omitempty"`
	Credentials *WriteCredentials `json:"credentials,omitempty"`
}

// ErrWriteDenied is what a statement fails with when object storage
// refused a worker the write of a table's files, or a worker could not
// write them for want of what it was given: the coordinator writes the
// table instead, and asks no worker to for a while (writeDeniedFor).
var ErrWriteDenied = errors.New("a worker may not write the table's files")

// WriteDenied reports whether err is ErrWriteDenied.
func WriteDenied(err error) bool { return errors.Is(err, ErrWriteDenied) }

// ErrWriteRefused is what a statement fails with when a worker said it
// wrote a file the coordinator did not ask it to, or other than the file is:
// it runs on the coordinator, and nothing of the statement's tasks is
// committed.
var ErrWriteRefused = errors.New("a worker's files are not what it was asked to write")

// WriteRefused reports whether err is ErrWriteRefused.
func WriteRefused(err error) bool { return errors.Is(err, ErrWriteRefused) }

// writeDeniedFor is how long after a denied write the coordinator asks no
// worker to write: a grant does not come back by itself, and every fill
// would otherwise find out again after its query has run.
const writeDeniedFor = 10 * time.Minute

// writeDenySigns are how object storage refusing a request reads in
// DuckDB's errors.
var writeDenySigns = []string{"HTTP 403", "HTTP 401", "AccessDenied", "Access Denied", "ExpiredToken", "InvalidAccessKeyId",
	"AuthorizationFailure", "AuthorizationPermissionMismatch", "Permission denied", "Forbidden"}

// writeDenied reports whether err, from a task that writes under
// WriteDirMark, is its write being refused, rather than a read of its
// inputs: the message names the directory, as DuckDB's HTTP errors name the
// URL.
func writeDenied(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrWriteDenied) {
		return true
	}
	msg := err.Error()
	if !strings.Contains(msg, WriteDirMark) {
		return false
	}
	return slices.ContainsFunc(writeDenySigns, func(s string) bool { return strings.Contains(msg, s) })
}

// partitionColumn is what a name in writeRequest.Partitions must look like:
// it goes into COPY's text, and into the files' paths.
var partitionColumn = regexp.MustCompile(`^__igloo_p[0-9]+$`)

// writeDirName is what a statement's directory, its last path segment
// under the table's location, looks like.
var writeDirName = regexp.MustCompile(`/` + WriteDirMark + `[0-9a-f]+(/|$)`)

// writeCompressions are the codecs a task may be asked to write with.
var writeCompressions = []string{"snappy", "zstd", "gzip", "lz4", "uncompressed"}

// checkWriteRequest refuses what a worker should not write, whoever asks:
// a directory outside allowed, or not a statement's.
func (w *Worker) checkWriteRequest(wr *writeRequest) error {
	if !w.allowed(wr.Dir+"/") || !writeDirName.MatchString(wr.Dir) {
		return fmt.Errorf("%w: not to %s", ErrWriteDenied, wr.Dir)
	}
	if len(wr.Columns) == 0 {
		return errors.New("a write of a table's files without columns")
	}
	for _, p := range wr.Partitions {
		if !partitionColumn.MatchString(p) {
			return fmt.Errorf("a partition column called %s", p)
		}
	}
	if wr.Compression != "" && !slices.Contains(writeCompressions, wr.Compression) {
		return fmt.Errorf("compression %s", wr.Compression)
	}
	return nil
}

// What a task's COPY holds. DuckDB's Parquet writer gathers rows until a row
// group has 122,880 of them, on each of its threads and for each partition:
// rows of 10.8 KB, 1.3 GB a group, took a worker's DuckDB of 953 MB out of
// memory, whatever the retries. Bounded by bytes, two threads held 345 MiB at
// 64 MiB a group, 188 at 32 and 110 at 16, in the same time and the same files;
// below that, about two vectors of 2,048 rows a thread (writeVectorRows). Over
// partitions it also gathers 524,288 rows a thread before it hands them to the
// partitions' writers: 64 partitions of those rows ran out of memory, and held
// 240 MiB at 16,384 rows and 2 MiB a group. Narrow rows took 10% longer at 16,384
// rows, and half as long again at 4,096.
const (
	// mostWriteRowGroup is Iceberg's own default of a row group's bytes, and
	// leastWriteRowGroup as few as a row group's bytes get: what is written in
	// groups this small on a small worker, compaction merges.
	mostWriteRowGroup  = 128 << 20
	leastWriteRowGroup = 1 << 20
	// mostWriteFiles and leastWriteFiles bound the partitions' files a task
	// keeps open. Past them, DuckDB closes one and later opens another.
	mostWriteFiles, leastWriteFiles = maxPartitions, 8
	// wideWriteRow is the width of rows a task's partitioned COPY allows for
	// before it hands them on: types do not say what a string takes.
	wideWriteRow = 16 << 10
)

// writeRowGroup is the bytes of a row group of each of writers, on a worker
// of memory in shares tasks at once: all of them within an eighth of its share,
// for what the writer holds beside them (about 2.7 times the group) and the
// rest of the task.
func writeRowGroup(memory int64, shares, writers int) int64 {
	if memory <= 0 {
		return mostWriteRowGroup
	}
	return min(max(memory/int64(max(shares, 1))/8/int64(max(writers, 1)), leastWriteRowGroup), mostWriteRowGroup)
}

// writeFilesOpen is how many partitions' files a task keeps open, each with
// an upload's buffer where it writes to object storage (see uploadBuffer):
// those within a quarter of its share of the worker's memory.
func writeFilesOpen(memory int64, shares int, uploads bool) int {
	if memory <= 0 {
		return 100
	}
	if !uploads {
		return mostWriteFiles
	}
	n := memory / int64(max(shares, 1)) / 4 / uploadBuffer
	return int(min(max(n, leastWriteFiles), mostWriteFiles))
}

// writeFlush is how many rows each of a task's threads gathers before it hands
// them to the partitions' writers: as shuffleFlush, but at wideWriteRow.
func writeFlush(memory int64, shares, threads int) int64 {
	const most, least = 524_288, 4_096
	if memory <= 0 {
		return most
	}
	return min(max(memory/int64(max(shares, 1))/4/int64(max(threads, 1))/wideWriteRow, least), most)
}

// writeFiles writes the rows of query, in Parquet with field IDs, under the
// attempt's directory, and lists the files.
func (w *Worker) writeFiles(ctx context.Context, conn *duckdb.Conn, req *taskRequest, query string, cols []Column) (*taskResult, error) {
	wr := req.Write
	if err := w.checkWriteRequest(wr); err != nil {
		return nil, err
	}
	to := wr.Dir + writeFilesDir
	if !strings.Contains(to, "://") {
		// A warehouse on local disk, as in tests: DuckDB creates only the
		// last directory.
		if err := os.MkdirAll(filepath.Dir(to), 0o700); err != nil {
			return nil, err
		}
	}
	if wr.Credentials != nil {
		drop, err := w.writeSecret(ctx, conn, wr)
		if err != nil {
			return nil, err
		}
		defer drop()
	}
	// No order to keep: a statement with one keeps to the coordinator (see
	// rowWise), and its threads need not wait for each other; DuckDB bounds a
	// row group's bytes only then.
	if _, err := conn.ExecContext(ctx, "SET SESSION preserve_insertion_order = false", nil); err != nil {
		return nil, err
	}
	if uploadsInParts(wr.Dir) {
		if _, err := conn.ExecContext(ctx, "SET SESSION s3_uploader_max_filesize = "+quoteLiteral(maxUploadFile), nil); err != nil {
			return nil, err
		}
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = quoteIdent(c.Name)
	}
	ids := make([]string, len(wr.Columns))
	for i, c := range wr.Columns {
		ids[i] = quoteLiteral(c.Name) + ": " + strconv.Itoa(c.ID)
	}
	opts := []string{"FORMAT parquet", "FIELD_IDS {" + strings.Join(ids, ", ") + "}", "RETURN_STATS true", "FILENAME_PATTERN 'part_{i}'"}
	// The task's share of the worker's memory is that of one of shares tasks
	// at once.
	shares := max(w.slotCount/w.taskSlots(req.Slots), 1)
	threads := threadsOf(ctx, conn)
	writers := threads
	if len(wr.Partitions) > 0 {
		// DuckDB cannot bound a partition's files by their size.
		opts = append(opts, "PARTITION_BY ("+strings.Join(wr.Partitions, ", ")+")")
		writers = writeFilesOpen(w.opts.MemoryBytes, shares, uploadsInParts(to))
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION partitioned_write_max_open_files = %d", writers), nil); err != nil {
			return nil, err
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION partitioned_write_flush_threshold = %d",
			writeFlush(w.opts.MemoryBytes, shares, threads)), nil); err != nil {
			return nil, err
		}
	} else if wr.FileBytes > 0 {
		opts = append(opts, "FILE_SIZE_BYTES "+strconv.FormatInt(wr.FileBytes, 10))
	}
	opts = append(opts, "ROW_GROUP_SIZE_BYTES "+strconv.FormatInt(writeRowGroup(w.opts.MemoryBytes, shares, writers), 10))
	if wr.RowGroup > 0 {
		opts = append(opts, "ROW_GROUP_SIZE "+strconv.FormatInt(wr.RowGroup, 10))
	}
	if wr.Compression != "" {
		opts = append(opts, "COMPRESSION "+wr.Compression)
	}
	res, err := conn.QueryContext(ctx, "COPY (SELECT "+strings.Join(names, ", ")+" FROM ("+query+")) TO "+quoteLiteral(to)+
		" ("+strings.Join(opts, ", ")+")", nil)
	if err != nil {
		return nil, err
	}
	defer res.Close() //nolint:errcheck
	out := &taskResult{Files: []shuffleFile{}, Columns: cols}
	dest := make([]driver.Value, len(res.Columns()))
	if len(dest) < 3 {
		return nil, fmt.Errorf("COPY returned %d columns of statistics", len(dest))
	}
	for {
		if err := res.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		f, _ := dest[0].(string)
		rows, rok := asInt64(dest[1])
		size, sok := asInt64(dest[2])
		if !rok || !sok {
			return nil, fmt.Errorf("unexpected statistics of file %s", f)
		}
		out.Files = append(out.Files, shuffleFile{Path: f, Rows: rows, Size: size})
		out.Rows += rows
	}
	return out, nil
}

// writeSecret gives DuckDB wr's credentials for wr's directory, and returns
// what takes them away. DuckDB keeps secrets for the whole database, and a
// path's secret is the one whose scope is longest: the attempt's directory
// is written with these, and every other path as before.
func (w *Worker) writeSecret(ctx context.Context, conn *duckdb.Conn, wr *writeRequest) (func(), error) {
	c := wr.Credentials
	var b [8]byte
	_, _ = rand.Read(b[:])
	name := "igloo_write_" + hex.EncodeToString(b[:])
	scope := strings.TrimSuffix(wr.Dir, "/") + "/"
	opts := []string{"TYPE s3", "KEY_ID " + quoteLiteral(c.KeyID), "SECRET " + quoteLiteral(c.Secret), "SCOPE " + quoteLiteral(scope)}
	if c.SessionToken != "" {
		opts = append(opts, "SESSION_TOKEN "+quoteLiteral(c.SessionToken))
	}
	if c.Region != "" {
		opts = append(opts, "REGION "+quoteLiteral(c.Region))
	}
	if c.Endpoint != "" {
		opts = append(opts, "ENDPOINT "+quoteLiteral(c.Endpoint))
	}
	if c.URLStyle != "" {
		opts = append(opts, "URL_STYLE "+quoteLiteral(c.URLStyle))
	}
	if c.Endpoint != "" {
		opts = append(opts, "USE_SSL "+strconv.FormatBool(c.UseSSL))
	}
	if _, err := conn.ExecContext(ctx, "CREATE OR REPLACE TEMPORARY SECRET "+name+" ("+strings.Join(opts, ", ")+")", nil); err != nil {
		// Never echo the statement: it holds the credentials.
		return nil, fmt.Errorf("%w: could not give DuckDB the credentials", ErrWriteDenied)
	}
	return func() {
		if _, err := conn.ExecContext(context.WithoutCancel(ctx), "DROP SECRET IF EXISTS "+name, nil); err != nil {
			w.log.Warn("could not drop the credentials of a task's write", "err", err)
		}
	}, nil
}

// checkWritten checks the files a task's attempt says it wrote: each under
// its directory, dir, plainly, and together the rows it says. A worker that
// says otherwise is not believed about anything.
func checkWritten(dir string, files []shuffleFile, rows int64) error {
	var listed int64
	seen := make(map[string]bool, len(files))
	for _, f := range files {
		if !UnderDir(f.Path, dir) || !strings.HasSuffix(f.Path, ".parquet") || f.Rows < 0 || f.Size < 0 || seen[f.Path] {
			return fmt.Errorf("%w: %s, for a task asked to write under %s", ErrWriteRefused, f.Path, dir)
		}
		seen[f.Path] = true
		listed += f.Rows
	}
	if listed != rows {
		return fmt.Errorf("%w: files of %d rows, of the %d it says it wrote", ErrWriteRefused, listed, rows)
	}
	return nil
}

// writtenName is what a path may hold below an attempt's directory, which is
// all a task's COPY writes (see writeFiles): no glob character, which DuckDB
// would take for a pattern when it reads the file, no escape, no query.
var writtenName = regexp.MustCompile(`^[A-Za-z0-9._=/-]+$`)

// UnderDir reports whether p is strictly under dir, as a plain path: no
// segment that leads elsewhere, however it is spelled, nothing a store or a
// client would read otherwise than as written.
func UnderDir(p, dir string) bool {
	rest, ok := strings.CutPrefix(p, strings.TrimSuffix(dir, "/")+"/")
	if !ok || !writtenName.MatchString(rest) {
		return false
	}
	for _, seg := range strings.Split(rest, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return path.Clean(rest) == rest
}

// WorkersWrite reports whether the plan's rows can be written by its tasks,
// as auto mode took them to be (see planOn): the coordinator should then
// give it a target (Write).
func (p *Plan) WorkersWrite() bool { return p.writes }

// Write has the plan's tasks write its rows as target says. The target's
// credentials, if any, are made by the coordinator when the statement runs.
func (p *Plan) Write(target *WriteTarget) { p.target = target }

// Wrote says the statement's tasks wrote its rows, Written lists the files,
// and SQL is empty.
func (ex *Execution) Wrote() bool { return ex.wrote }

// Written lists the files the winning attempts of the statement's tasks
// wrote.
func (ex *Execution) Written() []WrittenFile { return ex.written }

// ErasedSince reports whether an erasure request deleted files since the
// statement began, which its tasks may have read: its rows are not to be
// committed (see erasure.go).
func (ex *Execution) ErasedSince() bool { return ex.co != nil && ex.co.erased.Load() > ex.erased }

// workersMayWrite says workers may be asked to write tables' files.
func (co *Coordinator) workersMayWrite() bool {
	if co.opts.NoWorkerWrites {
		return false
	}
	until := co.writeDeniedUntil.Load()
	return until == 0 || time.Now().UnixNano() >= until
}

// noteWriteDenied has workers not be asked to write for a while.
func (co *Coordinator) noteWriteDenied(err error) {
	co.writeDeniedUntil.Store(time.Now().Add(writeDeniedFor).UnixNano())
	co.log.Warn("a worker may not write a table's files; this replica writes tables for a while. Give workers write access to directories named "+
		WriteDirMark+"* under tables' locations, or IGLOO_S3_STS_ROLE_ARN on S3 (docs/distributed-execution.md), or set IGLOO_WORKER_WRITES=off",
		"err", err)
}

// writeRequestFor is what a task of target is to write under dir; dispatch
// names its attempts' directories, and makes their credentials.
func writeRequestFor(target *WriteTarget, dir string) *writeRequest {
	return &writeRequest{Dir: dir, Columns: target.Columns, Partitions: target.Partitions, FileBytes: target.FileBytes, RowGroup: target.RowGroup,
		Compression: target.Compression}
}

// writeCredentials are the credentials the attempt that writes dir carries:
// none where the coordinator cannot make any, when workers use their own
// identity.
func (co *Coordinator) writeCredentials(ctx context.Context, dir string) *WriteCredentials {
	if co.opts.WriteCredentials == nil {
		return nil
	}
	creds, err := co.opts.WriteCredentials(ctx, dir)
	if err != nil {
		co.log.Warn("could not make credentials limited to a task's directory; workers write with their own identity, if it may", "err", err)
		return nil
	}
	return creds
}

// probeWrite has a worker write a file of one row to target's directory,
// to learn before the statement's query runs whether workers may: a denied
// write would otherwise show only once all of it has run.
func (co *Coordinator) probeWrite(ctx context.Context, ws []*worker, target *WriteTarget, qid string, erased int64) error {
	if len(ws) == 0 {
		return nil
	}
	task := &taskRequest{Query: qid, Task: probeTask, Version: co.opts.Version, Erased: erased, SQL: "SELECT 1 AS __igloo_probe", Views: []viewDef{}}
	task.Write = &writeRequest{Dir: target.Dir + "/probe", Columns: []WriteColumn{{Name: "__igloo_probe", ID: 1}}}
	_, _, err := co.dispatch(ctx, task, ws, 0, "", nil)
	if err != nil && ctx.Err() == nil {
		if WriteDenied(err) {
			co.noteWriteDenied(err)
			return err
		}
		return errors.Join(ErrWriteDenied, err)
	}
	return err
}

// probeTask is the number of a task that probes a write.
const probeTask = -1

// writtenBy records the files of a task of a stage whose tasks write the
// statement's rows.
func (e *execution) writtenBy(task *taskRequest, head *taskResult, w *worker) error {
	if err := checkWritten(head.attempt, head.Files, head.Rows); err != nil {
		e.co.log.Error("a worker said it wrote files it was not asked to; the statement runs on the coordinator", "query", task.Query,
			"task", task.Task, "worker", w.addr, "err", err)
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, f := range head.Files {
		e.written = append(e.written, WrittenFile{Path: f.Path, Rows: f.Rows, Size: f.Size})
	}
	return nil
}

// markWrites has the stage whose rows are the statement's result, if its
// plan's combine step only goes over them a row at a time, ready to write
// them (see Stage.writeSQL): the tasks then hold the statement's rows, and
// need nothing of each other's.
func (pl *planner) markWrites(final node) {
	var last *Stage
	for _, s := range pl.stages {
		if s.Out != nil {
			continue
		}
		if last != nil {
			return
		}
		last = s
	}
	if last == nil || last.placeholder == "" || last.merge != nil || last.enough > 0 || !pl.rowWise(final, last) {
		return
	}
	sql, err := pl.p.print(final)
	if err != nil {
		return
	}
	cols := ""
	if len(last.Columns) > 0 {
		names := make([]string, len(last.Columns))
		for i, c := range last.Columns {
			names[i] = quoteIdent(c)
		}
		cols = "(" + strings.Join(names, ", ") + ")"
	}
	last.writeSQL = "WITH " + quoteIdent(last.placeholder) + cols + " AS (" + last.SQL + ") " + sql
}

// rowWise reports whether final, a plan's combine step, reads s's rows and
// nothing else, one row at a time: no grouping, ordering, limit or distinct,
// no aggregate, window or subquery.
func (pl *planner) rowWise(final node, s *Stage) bool {
	if str(final["type"]) != "SELECT_NODE" {
		return false
	}
	from := child(final, "from_table")
	if from == nil || str(from["type"]) != "BASE_TABLE" || str(from["table_name"]) != s.placeholder {
		return false
	}
	if len(list(final, "modifiers")) > 0 || len(list(final, "group_expressions")) > 0 || len(list(final, "group_sets")) > 0 ||
		child(final, "having") != nil || child(final, "qualify") != nil || child(final, "sample") != nil ||
		len(list(child(final, "cte_map"), "map")) > 0 {
		return false
	}
	for _, part := range []any{final["select_list"], final["where_clause"]} {
		if hasClass(part, "WINDOW") || hasClass(part, "SUBQUERY") || pl.hasAggregate(part) {
			return false
		}
	}
	return true
}

// writeSQL is what a task of a stage that writes the statement's rows runs:
// the combine step over the stage's own rows, as target's Select reads it.
func writeSQL(s *Stage, target *WriteTarget) string {
	return strings.Replace(target.Select, WriteSource, s.writeSQL, 1)
}
