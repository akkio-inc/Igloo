package pgwire

import (
	"cmp"
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"maps"
	mrand "math/rand/v2"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"golang.org/x/sync/errgroup"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/catalogproxy"
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
// COPY writes a table of plain columns. A partitioned one, or one with columns
// of nested, fixed-length or nanosecond types or that are required, DuckDB's
// own INSERT writes, under the same directory, its commit kept from the
// catalog, and the refresh commits the files it wrote the same way (see
// insert).

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
	// copies says COPY can write the table's files (see overwritable), and
	// names, ids and types are then its columns: their names, field ids and
	// DuckDB types. Otherwise DuckDB's INSERT writes them (see insert).
	copies       bool
	names, types []string
	ids          []int
}

// replaceRows makes the view's table hold query's rows, and then runs also,
// statements that write the view's state: by overwrite if it can, or else as
// one transaction that deletes the table's rows, inserts query's and runs
// also. It returns the rows written.
//
// An overwrite cannot commit with the state's statements, which DuckDB
// commits. A stop or a failure between the two leaves the view's table at
// another snapshot than the job records, which makes the next refresh a full
// one (see baseline), so no change is counted twice in the state, whichever
// has been committed: the state is only ever gone on from when both match
// what the last refresh recorded. query may read the state as it was.
func (r *refresher) replaceRows(ctx context.Context, query string, also ...string) (int64, error) {
	o, err := r.overwritePlan(ctx)
	if errors.Is(err, errNoOverwrite) {
		return r.write(ctx, append([]string{"DELETE FROM " + r.qualified, "INSERT INTO " + r.qualified + " " + query}, also...)...)
	}
	if err != nil {
		return -1, err
	}
	rows, err := o.run(ctx, query)
	if err != nil {
		return -1, err
	}
	if r.s.afterOverwrite != nil {
		if err := r.s.afterOverwrite(ctx); err != nil {
			return -1, err
		}
	}
	if len(also) > 0 {
		if _, err := r.write(ctx, also...); err != nil {
			return -1, err
		}
	}
	return rows, nil
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
	if tbl.Metadata().Version() < 2 {
		return nil, errNoOverwrite
	}
	o := &overwritePlan{r: r, tbl: tbl, copies: tbl.Spec().IsUnpartitioned()}
	for _, f := range tbl.Schema().Fields() {
		if !overwritable(f.Type) || f.Required {
			o.copies = false
		}
		o.names, o.ids = append(o.names, f.Name), append(o.ids, f.ID)
	}
	if !o.copies {
		if _, ok := r.s.engine.(commitHolder); !ok {
			return nil, errNoOverwrite
		}
		return o, nil
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

// commitHolder is the engine that can keep a table's commit from the catalog
// (see catalogproxy.CommitHold).
type commitHolder interface {
	HoldCommit(schema, name, dir string) *catalogproxy.CommitHold
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
	// What DuckDB wrote for its commit that was kept from the catalog: the
	// files of its rows, wherever it wrote them, and its manifests, which
	// no snapshot has.
	var written, manifests []string
	defer func() {
		bg := context.WithoutCancel(ctx)
		s.deleteFiles(bg, o.tbl, manifests)
		if committed {
			s.forgetRefreshFiles(ctx, r.schema, note)
			return
		}
		if leave {
			// For the view's next refresh, which tells.
			return
		}
		s.deleteFiles(bg, o.tbl, written)
		// Whatever was written goes, canceled or not, unless the table
		// holds it: another session's commit, as DuckDB writes the refresh's
		// rows, goes in the same directory.
		if derr := s.clearNotedFiles(bg, r.schema, r.table, note); derr != nil {
			s.log.Warn("could not delete the files of a failed refresh; the view's next refresh deletes them", "view", r.job.Name,
				"dir", dir, "err", derr)
		}
	}()
	var (
		paths []string
		added []iceberg.DataFile
	)
	if o.copies {
		paths, rows, err = o.copy(ctx, dir, query)
	} else {
		added, written, manifests, rows, err = o.insert(ctx, dir, query)
	}
	if err != nil {
		return -1, err
	}
	if s.beforeOverwrite != nil {
		if err := s.beforeOverwrite(ctx, dir); err != nil {
			return -1, err
		}
	}
	if leave, err = o.commitAt(ctx, dir, paths, added, nil); err != nil {
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
func (o *overwritePlan) commitAt(ctx context.Context, dir string, paths []string, added []iceberg.DataFile, check func([]iceberg.DataFile, iceberg.PartitionSpec) error) (leave bool, err error) {
	r, s := o.r, o.r.s
	err = o.commit(ctx, paths, added, check)
	if err == nil {
		return false, nil
	}
	bg := context.WithoutCancel(ctx)
	tbl, lerr := s.ddl.LoadTable(bg, r.schema, r.table)
	held := false
	if lerr == nil {
		held, lerr = holdsFiles(bg, tbl, dir, added...)
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

// insert has DuckDB write query's rows as the table says, under dir, by an
// INSERT whose commit is kept from the catalog (see catalogproxy.CommitHold),
// and returns the data files it wrote, their paths, the manifest list and
// manifests it wrote for them, which no snapshot will have, and the rows.
func (o *overwritePlan) insert(ctx context.Context, dir, query string) (added []iceberg.DataFile, written, manifests []string, rows int64, err error) {
	r := o.r
	hold := r.s.engine.(commitHolder).HoldCommit(r.schema, r.table, dir)
	if hold == nil {
		return nil, nil, nil, 0, fmt.Errorf("another refresh of %s is writing its files", r.job.Name)
	}
	defer hold.Release()
	if _, err = r.exec(ctx, "BEGIN"); err != nil {
		return nil, nil, nil, 0, err
	}
	open := true
	defer func() {
		if open {
			_, _ = r.exec(context.WithoutCancel(ctx), "ROLLBACK")
		}
	}()
	err = r.s.runBound(r.sess, nil, func() (err error) {
		rows, err = r.exec(ctx, "INSERT INTO "+r.qualified+" "+query)
		return err
	})
	if err != nil {
		return nil, nil, nil, 0, err
	}
	hold.Arm(rows)
	if _, err = r.exec(ctx, "COMMIT"); err != nil {
		return nil, nil, nil, 0, err
	}
	open = false
	taken, ok := hold.Taken()
	if !ok {
		if rows == 0 {
			// Nothing to commit, nor to put in the old files' place.
			return nil, nil, nil, 0, nil
		}
		return nil, nil, nil, 0, fmt.Errorf("the commit of the files of %s did not come", r.job.Name)
	}
	manifests = []string{taken.ManifestList}
	fs, err := o.tbl.FS(ctx)
	if err != nil {
		return nil, nil, manifests, 0, err
	}
	snap := table.Snapshot{SnapshotID: taken.SnapshotID, ManifestList: taken.ManifestList}
	list, err := snap.Manifests(fs)
	if err != nil {
		return nil, nil, manifests, 0, err
	}
	for _, m := range list {
		if m.SnapshotID() != taken.SnapshotID {
			continue
		}
		manifests = append(manifests, m.FilePath())
		entries, err := m.FetchEntries(fs, false)
		if err != nil {
			return nil, written, manifests, 0, err
		}
		for _, e := range entries {
			if df := e.DataFile(); e.Status() == iceberg.EntryStatusADDED && df.ContentType() == iceberg.EntryContentData {
				added = append(added, df)
				written = append(written, df.FilePath())
			}
		}
	}
	return added, written, manifests, rows, nil
}

// deleteFiles deletes what a refresh wrote and no snapshot holds.
func (s *Server) deleteFiles(ctx context.Context, tbl *table.Table, paths []string) {
	if len(paths) == 0 {
		return
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		s.log.Debug("could not delete a refresh's files; the orphan cleanup will", "err", err)
		return
	}
	for _, p := range paths {
		if err := fs.Remove(p); err != nil && !errors.Is(err, iofs.ErrNotExist) {
			s.log.Debug("could not delete a refresh's file; the orphan cleanup will", "file", p, "err", err)
		}
	}
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
func (o *overwritePlan) commit(ctx context.Context, paths []string, added []iceberg.DataFile, check func([]iceberg.DataFile, iceberg.PartitionSpec) error) error {
	r, tbl := o.r, o.tbl
	schemaID := tbl.Schema().ID
	for attempt := 0; ; attempt++ {
		tx := tbl.NewTransaction()
		if added == nil && len(paths) > 0 {
			var err error
			if check != nil {
				added, err = readWritten(ctx, tx, tbl, paths)
			} else {
				added, err = tx.DataFiles(ctx, paths)
			}
			if err != nil {
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
		dirs := []string{n.Name}
		if _, name := splitDir(n.Name); strings.HasPrefix(name, distributed.WriteDirMark) {
			// A statement's note also stands for the copies it commits.
			dirs = append(dirs, sealedDir(n.Name))
		}
		for _, dir := range dirs {
			held, err := holdsFiles(ctx, tbl, dir)
			if err != nil {
				return err
			}
			if !held {
				if err := s.deleteRefreshFiles(ctx, tbl, dir); err != nil {
					return err
				}
				s.abortUploads(ctx, tbl, dir)
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

// noSuchDir reports whether DuckDB could not list a directory for not being there:
// an Azure account with a hierarchical namespace says so, where other stores list
// nothing.
func noSuchDir(err error) bool {
	return strings.Contains(err.Error(), "The specified path does not exist")
}

// listedIn is file, which DuckDB's glob listed under dir, as dir spells it: DuckDB
// gives abfss://account.host/container/path for abfss://container@account.host/path,
// which the store's IO reads as a path in the container named for the first.
func listedIn(dir, file string) string {
	u, err := url.Parse(dir)
	if err != nil || u.Scheme != "abfss" || u.User == nil {
		return file
	}
	container := u.User.Username()
	rest, ok := strings.CutPrefix(file, "abfss://"+u.Host+"/"+container+"/")
	if !ok {
		return file
	}
	return "abfss://" + container + "@" + u.Host + "/" + rest
}

// deleteRefreshFiles deletes the files in dir, a refresh's or a statement's
// directory under the table's data.
func (s *Server) deleteRefreshFiles(ctx context.Context, tbl *table.Table, dir string) error {
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
		if noSuchDir(err) {
			return nil
		}
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
			if noSuchDir(err) {
				return nil
			}
			return err
		}
		f, _ := dest[0].(string)
		files = append(files, listedIn(dir, f))
	}
	_ = res.Close()
	if len(files) == 0 {
		return nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return err
	}
	g := new(errgroup.Group)
	g.SetLimit(sealCopies)
	for _, f := range files {
		g.Go(func() error {
			if err := fs.Remove(f); err != nil && !errors.Is(err, iofs.ErrNotExist) {
				return fmt.Errorf("delete %s: %w", f, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if strings.HasPrefix(dir, "abfss://") {
		for _, d := range dirsOf(dir, files) {
			// One that holds something, as a late write's, stays for the second look.
			_ = fs.Remove(d)
		}
	}
	return nil
}

// dirsOf lists the directories from dir down to files, the deepest first: on an Azure
// account with a hierarchical namespace, a directory is a thing of its own, and stays
// when its files go, a dozen of them for every statement otherwise.
func dirsOf(dir string, files []string) []string {
	seen := map[string]bool{}
	for _, f := range files {
		for p := f; strings.HasPrefix(p, dir+"/"); {
			p = p[:strings.LastIndexByte(p, '/')]
			seen[p] = true
		}
	}
	dirs := slices.Collect(maps.Keys(seen))
	slices.SortFunc(dirs, func(a, b string) int {
		if c := cmp.Compare(len(b), len(a)); c != 0 {
			return c
		}
		return cmp.Compare(a, b)
	})
	return dirs
}

// writeDirName is the name of a refresh's directory, or a statement's.
var writeDirName = regexp.MustCompile("^(" + overwriteDir + "|" + distributed.WriteDirMark + "|" + distributed.SealedDirMark + ")[0-9a-f]+$")

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

// holdsFiles reports whether any snapshot of tbl holds a file in dir, or any
// of files.
func holdsFiles(ctx context.Context, tbl *table.Table, dir string, files ...iceberg.DataFile) (bool, error) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return false, err
	}
	prefix := strings.TrimSuffix(dir, "/") + "/"
	named := make(map[string]bool, len(files))
	for _, f := range files {
		named[f.FilePath()] = true
	}
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
				path := e.DataFile().FilePath()
				if named[path] || strings.HasPrefix(strings.TrimPrefix(path, "file://"), prefix) {
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
