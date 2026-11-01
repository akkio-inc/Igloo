package pgwire

import (
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	mrand "math/rand/v2"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/distributed"
)

// Full refreshes by overwrite. A full refresh replaced the rows of the
// view's table with DELETE FROM and INSERT in one transaction, and DuckDB
// writes a position delete for every row it deletes: 20 million rows took
// 8.5 s to delete where CREATE TABLE ... AS wrote them in 0.15 s, and a
// count of the emptied table took 2.2 s, against 13 ms, until compaction.
// Instead a full refresh writes the new rows with DuckDB's COPY, as Parquet
// files with the table's field ids, into a directory of their own under the
// table's data, and puts them in the table in place of every data and
// delete file it has, in one Iceberg commit. Readers, on this replica and
// others, see the old rows or the new ones; the result cache tells by the
// table's new metadata location, as it does a commit of another engine
// (see catalogproxy's TablesVersion). The commit asserts the snapshot it
// replaces, and a conflict, as with compaction, has it replace the files
// of the new one.
//
// The directory is noted in the schema's properties (catalog.PendingFiles)
// from before COPY writes to it until the commit is made or its files are
// deleted. A refresh that fails, or is canceled, deletes them; a stopped
// one leaves the note, and the view's next refresh, or an erasure
// request's replacement of the view, deletes them (see clearRefreshFiles).
// A table whose files the refresh cannot write so (partitioned, or with
// columns of nested, fixed-length or nanosecond types) is written by DELETE
// and INSERT as before.

// errNoOverwrite says a view's table cannot be overwritten (see
// overwritePlan).
var errNoOverwrite = errors.New("the table cannot be overwritten")

// overwriteDir starts the names of full refreshes' directories.
const overwriteDir = "igloo-refresh-"

// overwriteFileBytes is the size of the files a full refresh writes when
// the table does not set write.target-file-size-bytes: Iceberg's default.
const overwriteFileBytes = 512 << 20

// overwritePlan is how a full refresh overwrites the view's table.
type overwritePlan struct {
	r   *refresher
	tbl *table.Table
	// names, ids and types are the table's columns: their names, field ids
	// and DuckDB types.
	names, types []string
	ids          []int
}

// replaceRows makes the view's table hold query's rows, and runs also,
// statements that write the view's state, before it: by overwrite if it
// can, or else as one transaction that deletes the table's rows, inserts
// query's and runs also. It returns the rows written.
func (r *refresher) replaceRows(ctx context.Context, query string, also ...string) (int64, error) {
	o, err := r.overwritePlan(ctx)
	if errors.Is(err, errNoOverwrite) {
		return r.write(ctx, append([]string{"DELETE FROM " + r.qualified, "INSERT INTO " + r.qualified + " " + query}, also...)...)
	}
	if err != nil {
		return -1, err
	}
	if len(also) > 0 {
		if _, err := r.write(ctx, also...); err != nil {
			return -1, err
		}
	}
	return o.run(ctx, query)
}

// overwritePlan returns how to overwrite the view's table, or
// errNoOverwrite.
func (r *refresher) overwritePlan(ctx context.Context) (*overwritePlan, error) {
	if r.s.ddl == nil {
		return nil, errNoOverwrite
	}
	tbl, err := r.s.ddl.LoadTable(ctx, r.schema, r.table)
	if errors.Is(err, ddl.ErrNotFound) {
		return nil, errNoOverwrite
	}
	if err != nil {
		return nil, err
	}
	if !tbl.Spec().IsUnpartitioned() || tbl.Metadata().Version() < 2 {
		return nil, errNoOverwrite
	}
	o := &overwritePlan{r: r, tbl: tbl}
	for _, f := range tbl.Schema().Fields() {
		if !overwritable(f.Type) {
			return nil, errNoOverwrite
		}
		o.names, o.ids = append(o.names, f.Name), append(o.ids, f.ID)
	}
	described, err := r.describe(ctx)
	if err != nil {
		return nil, err
	}
	for _, n := range o.names {
		t, ok := described[n]
		if !ok {
			return nil, errNoOverwrite
		}
		o.types = append(o.types, t)
	}
	return o, nil
}

// overwritable reports whether a column of type t is one whose Parquet, as
// DuckDB's COPY writes it, is that of its Iceberg type: the plain types,
// not fixed, nanosecond timestamps, or nested ones.
func overwritable(t iceberg.Type) bool {
	switch t.(type) {
	case iceberg.BooleanType, iceberg.Int32Type, iceberg.Int64Type, iceberg.Float32Type, iceberg.Float64Type, iceberg.DecimalType,
		iceberg.DateType, iceberg.TimeType, iceberg.TimestampType, iceberg.TimestampTzType, iceberg.StringType, iceberg.UUIDType,
		iceberg.BinaryType:
		return true
	}
	return false
}

// describe returns the DuckDB types of the view's table's columns, by name.
func (r *refresher) describe(ctx context.Context) (map[string]string, error) {
	rows, err := r.sess.conn.QueryContext(ctx, "DESCRIBE "+r.qualified, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	out := map[string]string{}
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		typ, _ := dest[1].(string)
		out[name] = typ
	}
}

// run writes query's rows into a directory of their own and commits them
// in place of the table's files. It returns the rows written.
func (o *overwritePlan) run(ctx context.Context, query string) (rows int64, err error) {
	r, s := o.r, o.r.s
	// What earlier refreshes of the view left, if any, goes first.
	if err := s.clearRefreshFiles(ctx, r.schema, r.table, false); err != nil {
		s.log.Warn("could not delete files an earlier refresh left", "view", r.job.Name, "err", err)
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	dir := strings.TrimSuffix(strings.TrimPrefix(o.tbl.Location(), "file://"), "/") + "/data/" + overwriteDir + hex.EncodeToString(b[:])
	note := catalog.NewPending(catalog.PendingFiles, dir, r.table)
	if err := s.ddl.NotePending(ctx, r.schema, note); err != nil {
		return -1, fmt.Errorf("note the files of the refresh: %w", err)
	}
	committed, leave := false, false
	defer func() {
		if committed {
			s.forgetRefreshFiles(ctx, r.schema, note)
			return
		}
		if leave {
			// For the view's next refresh, which tells.
			return
		}
		// Whatever COPY wrote goes, canceled or not.
		if derr := s.deleteRefreshFiles(context.WithoutCancel(ctx), o.tbl, dir); derr != nil {
			s.log.Warn("could not delete the files of a failed refresh; the view's next refresh deletes them", "view", r.job.Name,
				"dir", dir, "err", derr)
			return
		}
		s.forgetRefreshFiles(ctx, r.schema, note)
	}()
	paths, rows, err := o.copy(ctx, dir, query)
	if err != nil {
		return -1, err
	}
	if s.beforeOverwrite != nil {
		if err := s.beforeOverwrite(ctx, dir); err != nil {
			return -1, err
		}
	}
	if leave, err = o.commitAt(ctx, dir, paths, nil); err != nil {
		return -1, err
	}
	committed = true
	s.ddl.TableChanged(r.schema, r.table)
	s.indexesChanged(r.schema, r.table)
	return rows, nil
}

// commitAt commits the files at paths, of directory dir, as commit does,
// after check, if set, of what their footers say. A commit whose answer was
// lost, or that a cancel cut short, may have been made: its files stay if
// the table holds them; leave says that cannot be told.
func (o *overwritePlan) commitAt(ctx context.Context, dir string, paths []string, check func([]iceberg.DataFile, iceberg.PartitionSpec) error) (leave bool, err error) {
	r, s := o.r, o.r.s
	err = o.commit(ctx, paths, check)
	if err == nil {
		return false, nil
	}
	bg := context.WithoutCancel(ctx)
	tbl, lerr := s.ddl.LoadTable(bg, r.schema, r.table)
	held := false
	if lerr == nil {
		held, lerr = holdsFiles(bg, tbl, dir)
	}
	switch {
	case lerr != nil:
		return true, err
	case !held:
		return false, err
	}
	s.log.Warn("a refresh's commit was made, though it failed", "view", r.job.Name, "err", err)
	return false, nil
}

// copy writes query's rows with COPY into dir, as Parquet with the table's
// field ids, in its columns' types, and returns the files that hold rows
// and how many rows they hold.
func (o *overwritePlan) copy(ctx context.Context, dir, query string) ([]string, int64, error) {
	r := o.r
	aliases := make([]string, len(o.names))
	cols := make([]string, len(o.names))
	ids := make([]string, len(o.names))
	for i, n := range o.names {
		aliases[i] = quoteIdent("__igloo_c" + strconv.Itoa(i))
		cols[i] = "CAST(" + aliases[i] + " AS " + o.types[i] + ") AS " + quoteIdent(n)
		ids[i] = quoteLiteral(n) + ": " + strconv.Itoa(o.ids[i])
	}
	props := o.tbl.Properties()
	opts := []string{"FORMAT parquet", "FIELD_IDS {" + strings.Join(ids, ", ") + "}", "RETURN_STATS true",
		"FILENAME_PATTERN 'part_{i}'", "FILE_SIZE_BYTES " + strconv.FormatInt(propertyInt(props, "write.target-file-size-bytes", overwriteFileBytes), 10)}
	if n := propertyInt(props, "write.parquet.row-group-size", 0); n > 0 {
		opts = append(opts, "ROW_GROUP_SIZE "+strconv.FormatInt(n, 10))
	}
	if c := strings.ToLower(props["write.parquet.compression-codec"]); c == "snappy" || c == "zstd" || c == "gzip" || c == "lz4" || c == "uncompressed" {
		opts = append(opts, "COMPRESSION "+c)
	}
	q := "COPY (SELECT " + strings.Join(cols, ", ") + " FROM (" + query + ") AS __igloo_rows(" + strings.Join(aliases, ", ") + ")) TO " +
		quoteLiteral(dir) + " (" + strings.Join(opts, ", ") + ")"
	var paths, empty []string
	var rows int64
	err := r.s.runBound(r.sess, nil, func() error {
		paths, empty, rows = nil, nil, 0
		res, err := r.sess.conn.QueryContext(ctx, q, nil)
		if err != nil {
			return err
		}
		defer res.Close() //nolint:errcheck
		dest := make([]driver.Value, len(res.Columns()))
		for {
			if err := res.Next(dest); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			path, _ := dest[0].(string)
			n := copiedRows(dest[1])
			if n == 0 {
				empty = append(empty, path)
				continue
			}
			paths, rows = append(paths, path), rows+n
		}
	})
	r.measured(q)
	if err != nil {
		return nil, 0, err
	}
	if len(empty) > 0 {
		// A file of no rows, which COPY writes for a query of none.
		fs, err := o.tbl.FS(ctx)
		if err != nil {
			return nil, 0, err
		}
		for _, p := range empty {
			if err := fs.Remove(p); err != nil {
				return nil, 0, err
			}
		}
	}
	return paths, rows, nil
}

// maxOverwriteAttempts bounds the commits a full refresh tries when others
// commit to the table meanwhile.
const maxOverwriteAttempts = 10

// commit puts the files at paths in the table in place of every data and
// delete file of its current snapshot, in one snapshot. On a conflict it
// does so again on the new current snapshot. check, if set, checks what
// the files' footers say before anything is committed.
func (o *overwritePlan) commit(ctx context.Context, paths []string, check func([]iceberg.DataFile, iceberg.PartitionSpec) error) error {
	r, tbl := o.r, o.tbl
	schemaID := tbl.Schema().ID
	var added []iceberg.DataFile
	for attempt := 0; ; attempt++ {
		tx := tbl.NewTransaction()
		if added == nil && len(paths) > 0 {
			var err error
			if added, err = tx.DataFiles(ctx, paths); err != nil {
				return fmt.Errorf("read the files the refresh wrote: %w", err)
			}
			if check != nil {
				if err := check(added, tbl.Spec()); err != nil {
					return err
				}
			}
		}
		data, deletes, err := currentFiles(ctx, tbl)
		if err != nil {
			return err
		}
		props := iceberg.Properties{"igloo.operation": "refresh"}
		switch {
		case len(data) == 0 && len(deletes) == 0 && len(added) == 0:
			return nil
		case len(data) == 0 && len(deletes) == 0:
			err = tx.ReplaceDataFilesWithDataFiles(ctx, nil, added, props, table.WithoutAutoNameMapping())
		default:
			err = tx.ReplaceFiles(ctx, data, added, deletes, props, table.WithoutAutoNameMapping())
		}
		if err != nil {
			return err
		}
		_, err = tx.CommitWithoutRetry(ctx)
		if err == nil || !errors.Is(err, table.ErrCommitFailed) || attempt+1 >= maxOverwriteAttempts {
			return err
		}
		if tbl, err = r.s.ddl.LoadTable(ctx, r.schema, r.table); err != nil {
			return err
		}
		if tbl.Schema().ID != schemaID {
			return fmt.Errorf("the columns of %s changed during its refresh", r.job.Name)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(mrand.Int64N(int64(10*time.Millisecond) << min(attempt, 5)))):
		}
	}
}

// currentFiles returns the data files and the delete files of tbl's
// current snapshot.
func currentFiles(ctx context.Context, tbl *table.Table) (data, deletes []iceberg.DataFile, err error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil, nil, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, nil, err
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, nil, err
	}
	for _, m := range manifests {
		entries, err := m.FetchEntries(fs, true)
		if err != nil {
			return nil, nil, err
		}
		for _, e := range entries {
			if df := e.DataFile(); df.ContentType() == iceberg.EntryContentData {
				data = append(data, df)
			} else {
				deletes = append(deletes, df)
			}
		}
	}
	return data, deletes, nil
}

// clearRefreshFiles deletes the files full refreshes of schema.table left
// in their directories (see catalog.PendingFiles) and forgets their notes:
// those of stopped processes, and this process's own, or, with all, every
// one, which the caller knows no refresh of the view is writing. Files a
// snapshot of the table holds stay, a refresh having committed them before
// it could forget its note. old are locations the table had, whose directories
// are its too.
func (s *Server) clearRefreshFiles(ctx context.Context, schema, name string, all bool, old ...string) error {
	if s.ddl == nil {
		return nil
	}
	notes, err := s.ddl.PendingNotes(ctx, schema)
	if err != nil {
		return err
	}
	now := time.Now()
	var tbl *table.Table
	for _, n := range notes {
		if n.Kind != catalog.PendingFiles || !strings.EqualFold(n.Target, name) || !all && !n.Dead(now) && !n.Mine() {
			continue
		}
		if tbl == nil {
			if tbl, err = s.ddl.LoadTable(ctx, schema, name); err != nil && !errors.Is(err, ddl.ErrNotFound) {
				return err
			}
		}
		if err := s.clearNoted(ctx, schema, name, tbl, n, old...); err != nil {
			return err
		}
	}
	return nil
}

// clearNotedFiles is clearNoted of schema.name as it loads now.
func (s *Server) clearNotedFiles(ctx context.Context, schema, name string, n catalog.Pending) error {
	tbl, err := s.ddl.LoadTable(ctx, schema, name)
	if err != nil && !errors.Is(err, ddl.ErrNotFound) {
		return err
	}
	return s.clearNoted(ctx, schema, name, tbl, n)
}

// clearNoted deletes the files note n says a stopped write left under tbl,
// schema.name, nil if gone, or a location it had (old), unless a snapshot holds
// them, and forgets it. A directory that is not one of these locations' own, as
// Igloo notes them (see ownDirectory), is not its to delete: a note is a
// schema's property, and the REST catalog keeps them from clients, but whoever
// reaches the catalog itself can write one.
func (s *Server) clearNoted(ctx context.Context, schema, name string, tbl *table.Table, n catalog.Pending, old ...string) error {
	if tbl != nil && !slices.ContainsFunc(append(old, tbl.Location()), func(loc string) bool { return ownDirectory(loc, n.Name) }) {
		s.log.Warn("a note of a stopped write names a directory that is not the table's; it deletes nothing, and goes",
			"table", schema+"."+name, "dir", n.Name)
		return s.ddl.ForgetPending(ctx, schema, n.Key)
	}
	if tbl != nil {
		held, err := holdsFiles(ctx, tbl, n.Name)
		if err != nil {
			return err
		}
		if !held {
			if err := s.deleteRefreshFiles(ctx, tbl, n.Name); err != nil {
				return err
			}
		}
	}
	// A table that is gone took its location's files with it, or
	// left them to the orphan cleanup with the rest of them.
	if err := s.ddl.ForgetPending(ctx, schema, n.Key); err != nil {
		return err
	}
	s.log.Info("cleared the files a stopped write left", "table", schema+"."+name, "dir", n.Name, "since", n.At)
	return nil
}

// refreshFilesBefore reports whether files a refresh of schema.table wrote
// before since may be left: a note of them from before then.
func (s *Server) refreshFilesBefore(ctx context.Context, schema, name string, since time.Time) (bool, error) {
	if s.ddl == nil {
		return false, nil
	}
	notes, err := s.ddl.PendingNotes(ctx, schema)
	if err != nil {
		return false, err
	}
	for _, n := range notes {
		if n.Kind == catalog.PendingFiles && strings.EqualFold(n.Target, name) && n.At.Before(since) {
			return true, nil
		}
	}
	return false, nil
}

// forgetRefreshFiles forgets note, the refresh's own.
func (s *Server) forgetRefreshFiles(ctx context.Context, schema string, note catalog.Pending) {
	if err := s.ddl.ForgetPending(context.WithoutCancel(ctx), schema, note.Key); err != nil {
		s.log.Debug("could not forget the note of a refresh's files", "schema", schema, "dir", note.Name, "err", err)
	}
}

// deleteRefreshFiles deletes the files in dir, a refresh's directory under
// tbl's location.
func (s *Server) deleteRefreshFiles(ctx context.Context, tbl *table.Table, dir string) error {
	return s.deleteFilesExcept(ctx, tbl, dir, nil)
}

// deleteFilesExcept deletes the files in dir, a refresh's or a statement's
// directory under tbl's location, but those at keep, which a commit holds:
// if it does not find them all there, it does not know what the files it
// finds are called, and deletes none.
func (s *Server) deleteFilesExcept(ctx context.Context, tbl *table.Table, dir string, keep []string) error {
	if parent, name := splitDir(dir); !writeDirName.MatchString(name) || !strings.HasSuffix(parent, "/data") {
		return fmt.Errorf("%s is not a refresh's directory, nor a statement's", dir)
	}
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck
	// Tasks write in directories of their own under it.
	res, err := conn.QueryContext(ctx, "SELECT file FROM glob("+quoteLiteral(dir+"/**")+")", nil)
	if err != nil {
		return err
	}
	var files []string
	dest := make([]driver.Value, 1)
	for {
		err := res.Next(dest)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = res.Close()
			return err
		}
		f, _ := dest[0].(string)
		files = append(files, f)
	}
	_ = res.Close()
	listed := make(map[string]bool, len(files))
	for _, f := range files {
		listed[f] = true
	}
	for _, k := range keep {
		if !listed[k] {
			return fmt.Errorf("a committed file, %s, is not among the files listed under %s", k, dir)
		}
		delete(listed, k)
	}
	files = slices.DeleteFunc(files, func(f string) bool { return !listed[f] })
	if len(files) == 0 {
		return nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := fs.Remove(f); err != nil && !errors.Is(err, iofs.ErrNotExist) {
			return fmt.Errorf("delete %s: %w", f, err)
		}
	}
	return nil
}

// writeDirName is the name of a refresh's directory, or a statement's.
var writeDirName = regexp.MustCompile("^(" + overwriteDir + "|" + distributed.WriteDirMark + ")[0-9a-f]+$")

// ownDirectory reports whether dir is a refresh's directory or a statement's
// of the table at location: directly in its data, as Igloo makes them.
func ownDirectory(location, dir string) bool {
	parent, name := splitDir(dir)
	return writeDirName.MatchString(name) && parent == strings.TrimSuffix(strings.TrimPrefix(location, "file://"), "/")+"/data"
}

// splitDir splits dir, a path or a URL, at its last slash.
func splitDir(dir string) (parent, name string) {
	i := strings.LastIndexByte(dir, '/')
	if i < 0 {
		return "", dir
	}
	return dir[:i], dir[i+1:]
}

// holdsFiles reports whether any snapshot of tbl holds a file in dir.
func holdsFiles(ctx context.Context, tbl *table.Table, dir string) (bool, error) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return false, err
	}
	prefix := strings.TrimSuffix(dir, "/") + "/"
	seen := map[string]bool{}
	for _, snap := range tbl.Metadata().Snapshots() {
		manifests, err := snap.Manifests(fs)
		if err != nil {
			return false, err
		}
		for _, m := range manifests {
			if seen[m.FilePath()] {
				continue
			}
			seen[m.FilePath()] = true
			entries, err := m.FetchEntries(fs, false)
			if err != nil {
				return false, err
			}
			for _, e := range entries {
				if strings.HasPrefix(strings.TrimPrefix(e.DataFile().FilePath(), "file://"), prefix) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// propertyInt reads a table property as an integer, or def.
func propertyInt(props iceberg.Properties, key string, def int64) int64 {
	if n, err := strconv.ParseInt(props[key], 10, 64); err == nil && n > 0 {
		return n
	}
	return def
}

// copiedRows reads the rows COPY says a file holds.
func copiedRows(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case uint64:
		return int64(n)
	case int:
		return int64(n)
	}
	return 0
}
