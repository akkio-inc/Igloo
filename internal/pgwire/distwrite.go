package pgwire

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"golang.org/x/sync/errgroup"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/distributed"
)

// Workers writing a fill's rows (see distributed's writes.go, and fillOn).
// A tableWrite is one table's end of it, here: it makes the statement's
// directory under the table's location and notes it in the catalog, so that
// what a stopped process leaves can be told from a running statement's;
// it reads the footer of every file a worker says it wrote, and commits the
// files that hold what the worker said, in one Iceberg commit; and it
// deletes whatever was not committed.
//
// A worker's say counts for nothing here but which files to look at: the
// data files committed are made from the files' own footers, read with this
// replica's credentials (table.Transaction.DataFiles), checked against the
// table's schema, and a file whose size or rows are not what the worker
// said, or that is not under the attempt's directory the worker was given
// (distributed.checkWritten), fails the statement's write. A worker can
// write only under the directory, and a file there is committed only as a
// file of the rows of this statement's task.

// tableWrite is a fill's write of a table's files by workers.
type tableWrite struct {
	s    *Server
	sess *session
	// schema and name are the table's; tbl is as last loaded. overwrite, if
	// set, makes the commit replace the table's files, as a full refresh's
	// does (see overwritePlan); otherwise it adds to them.
	schema, name string
	tbl          *table.Table
	overwrite    *overwritePlan
	// gate, if set, may stop the commit once every file's footer has been read,
	// which can take long.
	gate func(context.Context) error
	// sel is the target's Select, and columns its Columns; partitions the
	// names of Select's partition columns.
	sel        string
	columns    []distributed.WriteColumn
	partitions []string
	// dir is the statement's directory while a run of it writes there, and
	// note its note in the catalog; sealed is where the files to commit were
	// copied to, of which the note speaks too; committed says a commit holds
	// them, and left that whether one does cannot be told.
	dir, sealed     string
	note            catalog.Pending
	committed, left bool
}

// noWorkerWrite logs why a fill, of what, takes a way before workers could
// write its files.
func (s *Server) noWorkerWrite(what, why string) {
	if s.dist != nil {
		s.log.Debug("workers will not write a table's files", "table", what, "why", why)
	}
}

// tableWriter returns how workers would write the rows of a query with
// columns src into schema.name, in the order of the table's columns, or
// those of order if set, or nil where they cannot, as it logs: the current
// way writes the table then.
func (s *Server) tableWriter(ctx context.Context, sess *session, schema, name string, src *statement, order []string) *tableWrite {
	if s.dist == nil || s.ddl == nil {
		return nil
	}
	w, why, err := s.newTableWrite(ctx, sess, schema, name, len(src.columns), order)
	switch {
	case err != nil:
		s.log.Debug("workers will not write a table's files", "table", schema+"."+name, "err", err)
		return nil
	case why != "":
		s.log.Debug("workers will not write a table's files", "table", schema+"."+name, "why", why)
		return nil
	}
	return w
}

// newTableWrite makes the tableWrite of a query of n columns into
// schema.name; why says why there is none, if none.
func (s *Server) newTableWrite(ctx context.Context, sess *session, schema, name string, n int, order []string) (*tableWrite, string, error) {
	tbl, err := s.ddl.LoadTable(ctx, schema, name)
	if errors.Is(err, ddl.ErrNotFound) {
		return nil, "the table is not in the catalog", nil
	}
	if err != nil {
		return nil, "", err
	}
	if tbl.Metadata().Version() < 2 {
		return nil, "the table is of Iceberg format version 1", nil
	}
	if so := tbl.SortOrder(); !so.IsUnsorted() {
		return nil, "the table has an Iceberg sort order", nil
	}
	fields := tbl.Schema().Fields()
	var described []columnType
	if err := s.runBound(sess, nil, func() (err error) {
		described, err = describeTable(ctx, sess, quoteName([]string{s.catalogName(), schema, name}))
		return err
	}); err != nil {
		return nil, "", err
	}
	types := make(map[string]string, len(described))
	for _, c := range described {
		types[c.name] = c.typ
	}
	// Which of the query's columns is each of the table's.
	source := make([]int, len(fields))
	switch {
	case len(order) > 0:
		if len(order) != len(fields) || n != len(fields) {
			return nil, "the statement does not write every column of the table", nil
		}
		for i, f := range fields {
			source[i] = slices.IndexFunc(order, func(c string) bool { return strings.EqualFold(c, f.Name) })
			if source[i] < 0 {
				return nil, "the statement does not write every column of the table", nil
			}
		}
	case n != len(fields):
		return nil, "the query's columns are not the table's", nil
	default:
		for i := range source {
			source[i] = i
		}
	}
	w := &tableWrite{s: s, sess: sess, schema: schema, name: name, tbl: tbl}
	aliases := make([]string, n)
	for i := range aliases {
		aliases[i] = "__igloo_c" + strconv.Itoa(i)
	}
	casts := make([]string, len(fields))
	outer := make([]string, 0, len(fields))
	for i, f := range fields {
		if !overwritable(f.Type) {
			return nil, fmt.Sprintf("column %s is of type %s, whose Parquet a task's COPY would not write as Iceberg's", f.Name, f.Type), nil
		}
		if f.Required {
			// COPY checks no NOT NULL, and writes the column as optional.
			return nil, "column " + f.Name + " is NOT NULL", nil
		}
		typ, ok := types[f.Name]
		if !ok {
			return nil, "column " + f.Name + " does not show in the table's description", nil
		}
		casts[i] = "CAST(" + aliases[source[i]] + " AS " + typ + ") AS " + quoteIdent(f.Name)
		outer = append(outer, quoteIdent(f.Name))
		w.columns = append(w.columns, distributed.WriteColumn{Name: f.Name, ID: f.ID})
	}
	spec := tbl.Spec()
	for _, field := range spec.Fields() {
		src, ok := tbl.Schema().FindFieldByID(field.SourceID())
		if !ok {
			return nil, "a partition field reads a column the table does not have", nil
		}
		expr, why := partitionExpr(field.Transform, quoteIdent(src.Name), src.Type)
		if why != "" {
			return nil, "partition field " + field.Name + ": " + why, nil
		}
		if expr == "" {
			continue
		}
		// By its hash: no value of the table in a file's path, and no
		// character in it a store or a client could read otherwise. Two values
		// of one hash in a file make its partition unknown, which refuses it
		// (see fileToDataFile).
		p := "__igloo_p" + strconv.Itoa(len(w.partitions))
		w.partitions = append(w.partitions, p)
		outer = append(outer, "hash("+expr+") AS "+p)
	}
	w.sel = "SELECT " + strings.Join(outer, ", ") + " FROM (SELECT " + strings.Join(casts, ", ") + " FROM (" + distributed.WriteSource +
		") AS __igloo_rows(" + strings.Join(aliases, ", ") + "))"
	return w, "", nil
}

// partitionExpr is the SQL expression, of column col of Iceberg type t,
// whose values divide rows among the files of transform's partitions, as
// the partition values a file's footer gives (see DataFiles); "" for a
// transform that divides nothing. why says why no expression will do: the
// transform's value could not be read from a file's bounds (a bucket's), so
// the current way writes the table.
func partitionExpr(transform iceberg.Transform, col string, t iceberg.Type) (expr, why string) {
	utc := col
	if _, tz := t.(iceberg.TimestampTzType); tz {
		// Iceberg's transforms are of UTC, whatever the session's time zone.
		utc = "timezone('UTC', " + col + ")"
	}
	temporal := func(part string) (string, string) {
		switch t.(type) {
		case iceberg.DateType, iceberg.TimestampType, iceberg.TimestampTzType:
			return "date_trunc('" + part + "', " + utc + ")", ""
		}
		return "", "a " + part + " of type " + t.String()
	}
	switch tr := transform.(type) {
	case iceberg.IdentityTransform:
		switch t.(type) {
		case iceberg.BooleanType, iceberg.Int32Type, iceberg.Int64Type, iceberg.DateType, iceberg.StringType, iceberg.TimestampType,
			iceberg.TimestampTzType:
			return col, ""
		}
		// A decimal's too: iceberg-go cannot write its partition value.
		return "", "identity of type " + t.String()
	case iceberg.VoidTransform:
		return "", ""
	case iceberg.YearTransform:
		return temporal("year")
	case iceberg.MonthTransform:
		return temporal("month")
	case iceberg.DayTransform:
		return temporal("day")
	case iceberg.HourTransform:
		switch t.(type) {
		case iceberg.TimestampType, iceberg.TimestampTzType:
			return temporal("hour")
		}
		return "", "an hour of type " + t.String()
	case iceberg.TruncateTransform:
		width := strconv.Itoa(tr.Width)
		switch t.(type) {
		case iceberg.Int32Type, iceberg.Int64Type:
			return "(" + col + " - (((" + col + " % " + width + ") + " + width + ") % " + width + "))", ""
		case iceberg.StringType:
			return "left(" + col + ", " + width + ")", ""
		}
		return "", "truncate of type " + t.String()
	}
	return "", "transform " + transform.String() + ", whose values a file's bounds do not give"
}

// begin makes the statement's directory, notes it, and returns where tasks
// are to write; a run after one that ended makes another.
func (w *tableWrite) begin(ctx context.Context) (*distributed.WriteTarget, error) {
	s := w.s
	if w.dir == "" {
		// What stopped statements left under the table goes first.
		if err := s.clearStoppedWrites(ctx, w.schema, w.name); err != nil {
			s.log.Warn("could not delete files stopped statements left", "table", w.schema+"."+w.name, "err", err)
		}
		var b [8]byte
		_, _ = rand.Read(b[:])
		dir := strings.TrimSuffix(strings.TrimPrefix(w.tbl.Location(), "file://"), "/") + "/data/" + distributed.WriteDirMark + hex.EncodeToString(b[:])
		note := catalog.NewPending(catalog.PendingFiles, dir, w.name)
		s.writesRunning.Store(note.Key, true)
		if err := s.ddl.NotePending(ctx, w.schema, note); err != nil {
			s.writesRunning.Delete(note.Key)
			return nil, fmt.Errorf("note the files of the statement: %w", err)
		}
		w.dir, w.note, w.committed, w.left = dir, note, false, false
	}
	props := w.tbl.Properties()
	t := &distributed.WriteTarget{Dir: w.dir, Select: w.sel, Columns: w.columns, Partitions: w.partitions}
	t.FileBytes = propertyInt(props, "write.target-file-size-bytes", overwriteFileBytes)
	t.RowGroup = propertyInt(props, "write.parquet.row-group-size", 0)
	switch c := strings.ToLower(props["write.parquet.compression-codec"]); c {
	case "snappy", "zstd", "gzip", "lz4", "uncompressed":
		t.Compression = c
	}
	return t, nil
}

// end deletes what the statement's tasks wrote, which no commit holds, and the
// copies of it if the statement did not commit them. Tasks the statement no
// longer waits for may still finish a file, so it looks again a while later
// (lateWritesAfter), and only then forgets the note; a process that stops before
// that leaves the note for the table's next write. It leaves both where a commit
// may hold the files and that cannot be told, for the table's next write to tell.
func (w *tableWrite) end(ctx context.Context) {
	if w.dir == "" {
		return
	}
	s, schema, name, tbl, dir, sealed, note, committed := w.s, w.schema, w.name, w.tbl, w.dir, w.sealed, w.note, w.committed
	w.dir, w.sealed = "", ""
	if w.left {
		s.writesRunning.Delete(note.Key)
		return
	}
	bg := context.WithoutCancel(ctx)
	failed := func(err error) {
		// The note says what to look at. Files no snapshot holds are orphans to
		// the table's cleanup and to erasure.
		s.log.Warn("could not delete the files a write did not commit; the table's next write or its orphan cleanup deletes them",
			"table", schema+"."+name, "dir", dir, "err", err)
	}
	remove := func(dirs ...string) bool {
		for _, d := range dirs {
			if err := s.deleteRefreshFiles(bg, tbl, d); err != nil {
				failed(err)
				return false
			}
		}
		return true
	}
	dirs := []string{dir}
	if sealed != "" && !committed {
		dirs = append(dirs, sealed)
	}
	if !remove(dirs...) {
		s.writesRunning.Delete(note.Key)
		return
	}
	s.lateWrites.Add(1)
	go func() {
		defer s.lateWrites.Done()
		defer s.writesRunning.Delete(note.Key)
		if !s.waitLateWrites() || !remove(dir) {
			return
		}
		s.forgetRefreshFiles(bg, schema, note)
	}()
}

// sealCopies is how many files a statement copies at a time.
const sealCopies = 32

// sealedDir is the directory the coordinator copies the files of a statement's
// directory to.
func sealedDir(dir string) string {
	parent, name := splitDir(dir)
	return parent + "/" + distributed.SealedDirMark + strings.TrimPrefix(name, distributed.WriteDirMark)
}

// seal copies files out of their tasks' directories, in the store, and returns
// them as copied: what a worker can still do with its credentials to the files
// it wrote, for up to an hour on S3, does not reach what is read and committed
// from here on. A copy takes what the file holds at that moment, so that nothing
// has to be held against what a worker said of it.
func (w *tableWrite) seal(ctx context.Context, files []distributed.WrittenFile) ([]distributed.WrittenFile, error) {
	fsys, err := w.tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	store, ok := fsys.(interface {
		Copy(ctx context.Context, src, dst string) error
	})
	if !ok {
		return nil, fmt.Errorf("the store of %s.%s cannot copy a file", w.schema, w.name)
	}
	w.sealed = sealedDir(w.dir)
	out := make([]distributed.WrittenFile, len(files))
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(sealCopies)
	for i, f := range files {
		out[i] = f
		out[i].Path = w.sealed + "/part_" + strconv.Itoa(i) + ".parquet"
		g.Go(func() error { return store.Copy(ctx, f.Path, out[i].Path) })
	}
	return out, g.Wait()
}

// lateWritesAfter is how long after a statement ended it looks for what its
// tasks still wrote: a task canceled as it finishes a file ends in moments, and
// one on a worker that seemed dead in about as long as the others took.
const lateWritesAfter = time.Minute

// waitLateWrites waits until it is time to look for them, and reports false if
// the server stopped first.
func (s *Server) waitLateWrites() bool {
	if wait := s.lateWritesWait; wait != nil {
		wait()
		return true
	}
	select {
	case <-time.After(lateWritesAfter):
		return true
	case <-s.lateWritesStop:
		return false
	}
}

// checkAdded compares the data files made from footers with what the
// workers said of them, and that a partitioned file's partition is known
// for the rows it holds.
func checkAdded(files []distributed.WrittenFile, added []iceberg.DataFile, spec iceberg.PartitionSpec) error {
	if len(files) != len(added) {
		return fmt.Errorf("%w: %d files read of the %d workers wrote", distributed.ErrWriteRefused, len(added), len(files))
	}
	said := make(map[string]distributed.WrittenFile, len(files))
	for _, f := range files {
		said[f.Path] = f
	}
	for _, df := range added {
		f, ok := said[df.FilePath()]
		if !ok {
			return fmt.Errorf("%w: %s", distributed.ErrWriteRefused, df.FilePath())
		}
		if df.Count() != f.Rows || df.FileSizeBytes() != f.Size {
			return fmt.Errorf("%w: %s holds %d rows in %d bytes, and a worker said %d in %d", distributed.ErrWriteRefused, df.FilePath(),
				df.Count(), df.FileSizeBytes(), f.Rows, f.Size)
		}
		part := df.Partition()
		for _, field := range spec.Fields() {
			if _, void := field.Transform.(iceberg.VoidTransform); void {
				continue
			}
			if v, ok := part[field.FieldID]; (!ok || v == nil) && df.NullValueCounts()[field.SourceID()] < df.Count() {
				return fmt.Errorf("%w: the partition of %s is not known for rows that have a value", distributed.ErrWriteRefused, df.FilePath())
			}
		}
	}
	return nil
}

// commit puts files, as their footers say they are, in the table, in one
// commit, again on a conflict with another commit, and returns their rows.
func (w *tableWrite) commit(ctx context.Context, files []distributed.WrittenFile) (int64, error) {
	s := w.s
	if s.beforeWriteCommit != nil {
		if err := s.beforeWriteCommit(ctx, w.dir); err != nil {
			return 0, err
		}
	}
	var rows int64
	var paths []string
	kept := make([]distributed.WrittenFile, 0, len(files))
	for _, f := range files {
		// However the coordinator came by it, nothing outside the statement's
		// directory is committed.
		if !distributed.UnderDir(f.Path, w.dir) {
			return 0, fmt.Errorf("%w: %s", distributed.ErrWriteRefused, f.Path)
		}
		// A task of no rows wrote a file of none.
		if f.Rows > 0 {
			rows += f.Rows
			paths = append(paths, f.Path)
			kept = append(kept, f)
		}
	}
	if len(paths) > 0 {
		var err error
		if kept, err = w.seal(ctx, kept); err != nil {
			return 0, fmt.Errorf("copy the files workers wrote: %w", err)
		}
		for i, f := range kept {
			paths[i] = f.Path
		}
	}
	check := func(added []iceberg.DataFile, spec iceberg.PartitionSpec) error {
		if err := checkAdded(kept, added, spec); err != nil {
			return err
		}
		if w.gate != nil {
			return w.gate(ctx)
		}
		return nil
	}
	var err error
	if w.overwrite != nil {
		w.left, err = w.overwrite.commitAt(ctx, w.sealed, paths, nil, check)
		w.committed = err == nil
	} else if len(paths) > 0 {
		err = w.add(ctx, paths, check)
		w.committed = err == nil
	} else {
		w.committed = true
	}
	if err != nil {
		return 0, err
	}
	s.ddl.TableChanged(w.schema, w.name)
	s.indexesChanged(w.schema, w.name)
	s.log.Debug("committed files workers wrote", "table", w.schema+"."+w.name, "files", len(paths), "rows", rows)
	return rows, nil
}

// readWritten returns the data files of the files at paths, which workers wrote,
// by footers it checks first (see distfooter.go). Workers write flat tables.
func readWritten(ctx context.Context, tx *table.Transaction, tbl *table.Table, paths []string) ([]iceberg.DataFile, error) {
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	return tx.DataFilesFrom(ctx, &writtenFooters{IO: fsys, columns: len(tbl.Schema().Fields()), depth: 1}, paths)
}

// add commits the files at paths to the table. A conflict with another
// commit, which said so, tries again on the commit's table; one whose answer
// was lost may have been made, which it reads back rather than add them
// twice.
func (w *tableWrite) add(ctx context.Context, paths []string, check func([]iceberg.DataFile, iceberg.PartitionSpec) error) error {
	s, tbl := w.s, w.tbl
	schemaID, spec := tbl.Schema().ID, tbl.Spec()
	var added []iceberg.DataFile
	for attempt := 0; ; attempt++ {
		tx := tbl.NewTransaction()
		if added == nil {
			var err error
			if added, err = readWritten(ctx, tx, tbl, paths); err != nil {
				return fmt.Errorf("read the files workers wrote: %w", err)
			}
			if err := check(added, spec); err != nil {
				return err
			}
		}
		if err := tx.AddDataFiles(ctx, added, nil, table.WithoutAutoNameMapping()); err != nil {
			return err
		}
		_, err := tx.CommitWithoutRetry(ctx)
		if err == nil {
			return nil
		}
		if !errors.Is(err, table.ErrCommitFailed) {
			// Its answer lost, or a cancel cut it short.
			bg := context.WithoutCancel(ctx)
			latest, lerr := s.ddl.LoadTable(bg, w.schema, w.name)
			held := false
			if lerr == nil {
				w.tbl = latest
				held, lerr = holdsFiles(bg, latest, w.sealed)
			}
			switch {
			case lerr != nil:
				w.left = true
				return err
			case !held:
				return err
			}
			s.log.Warn("a commit of files workers wrote was made, though it failed", "table", w.schema+"."+w.name, "err", err)
			return nil
		}
		if attempt+1 >= maxOverwriteAttempts {
			return err
		}
		var lerr error
		if tbl, lerr = s.ddl.LoadTable(ctx, w.schema, w.name); lerr != nil {
			return lerr
		}
		w.tbl = tbl
		if now := tbl.Spec(); tbl.Schema().ID != schemaID || now.ID() != spec.ID() {
			return fmt.Errorf("the columns or partitions of %s.%s changed during the statement", w.schema, w.name)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(mrand.Int64N(int64(10*time.Millisecond) << min(attempt, 5)))):
		}
	}
}

// clearStoppedWrites deletes the files stopped statements' writes left under
// schema.name, and forgets their notes: not those of statements running,
// on this replica or another, whose notes are not dead (see catalog.Pending),
// but those of this replica's that ended and could not tell if they committed.
func (s *Server) clearStoppedWrites(ctx context.Context, schema, name string) error {
	notes, err := s.ddl.PendingNotes(ctx, schema)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, n := range notes {
		if n.Kind != catalog.PendingFiles || !strings.EqualFold(n.Target, name) || !strings.Contains(n.Name, "/"+distributed.WriteDirMark) {
			continue
		}
		if _, running := s.writesRunning.Load(n.Key); running || !n.Dead(now) && !n.Mine() {
			continue
		}
		if err := s.clearNotedFiles(ctx, schema, name, n); err != nil {
			return err
		}
	}
	return nil
}

// insertWriter returns how workers would write the rows of src, the query
// of an INSERT of p, into its table, or nil.
func (s *Server) insertWriter(ctx context.Context, sess *session, p parsedInsert, src *statement) *tableWrite {
	if p.byName {
		return nil
	}
	sc := &scanner{s: p.target}
	name, ok := sc.name()
	if !ok {
		return nil
	}
	target, err := s.newTableTarget(ctx, sess, name)
	if err != nil {
		return nil
	}
	if len(name) < 3 {
		// A name DuckDB resolves: it must be this table it means, not another
		// of the session's search path, or a temporary table of that name.
		db, _, err := currentSchema(ctx, sess.conn)
		if err != nil || !strings.EqualFold(db, s.catalogName()) {
			return nil
		}
		if err := s.runBound(sess, nil, func() error {
			_, err := describeTable(ctx, sess, quoteName([]string{"temp", "main", target.Table}))
			return err
		}); err == nil {
			return nil
		}
	}
	return s.tableWriter(ctx, sess, target.Schema, target.Table, src, p.columns)
}
