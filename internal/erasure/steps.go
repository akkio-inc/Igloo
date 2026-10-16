package erasure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"gocloud.dev/gcerrors"

	"github.com/akkio-inc/igloo/internal/policy"
)

const (
	// orphanAge is how old an unreferenced file must be before a request
	// deletes it, as table maintenance waits: younger ones may belong to a
	// write that has not committed yet.
	orphanAge = 72 * time.Hour
	// maxRewriteBytes bounds the data one rewrite commit replaces, as for
	// compaction: a commit that loses to a writer costs at most this.
	maxRewriteBytes = 8 << 30
	// commitAttempts bounds the commits a step tries per table in one run.
	commitAttempts = 10
	// sharingDir holds Delta Sharing's copies of data files with their
	// deletes applied, under the warehouse root, by table UUID.
	sharingDir = ".igloo/sharing/"
)

// Kinds of files a request deletes.
const (
	fileMatched = "matched"
	fileExpired = "expired"
	fileOrphan  = "orphan"
	fileSharing = "sharing"
)

// operation marks the snapshots requests commit.
var operation = iceberg.Properties{"igloo.operation": "erase"}

// load loads a request's table, recording a table that is gone.
func (s *Service) load(ctx context.Context, t *Table) (*table.Table, bool, error) {
	tbl, err := s.opts.Catalog.LoadTable(ctx, ident(t.Name))
	if errors.Is(err, icecat.ErrNoSuchTable) {
		t.Missing = true
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("load %s: %w", t.Name, err)
	}
	t.Missing = false
	return tbl, true, nil
}

// loaded is a request's table as the catalog has it now.
type loaded struct {
	t   *Table
	tbl *table.Table
}

// tables loads the request's tables that exist.
func (s *Service) tables(ctx context.Context, r *Request) ([]loaded, error) {
	var out []loaded
	for i := range r.Tables {
		if r.Tables[i].Remote {
			continue
		}
		tbl, ok, err := s.load(ctx, &r.Tables[i])
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, loaded{&r.Tables[i], tbl})
		}
	}
	return out, nil
}

// find works out the tables r erases from: those with tagged columns, and
// those that share data files with them.
func (s *Service) find(ctx context.Context, r *Request) error {
	snap, err := s.opts.Policies(ctx)
	if err != nil {
		return err
	}
	byName := map[string]*Table{}
	for _, ct := range snap.Catalog.ColumnTags {
		for _, sub := range r.Subjects {
			digest, ok := holds(ct.Value, sub.Value)
			if ct.Tag != sub.Tag || !ok {
				continue
			}
			name := policy.TableName(ct.Catalog, ct.Schema, ct.Table)
			key := strings.ToLower(name)
			if byName[key] == nil {
				byName[key] = &Table{Name: name, Reason: "tagged"}
				if ct.Catalog != "" {
					byName[key].Reason = "a table of connection " + ct.Catalog + ", which Igloo reads but cannot erase from: erase there"
					byName[key].Remote = true
				}
			}
			byName[key].Columns = append(byName[key].Columns, Column{Name: ct.Column, Tag: ct.Tag, Value: sub.Value, Digest: digest})
		}
	}
	if !slices.ContainsFunc(slices.Collect(maps.Values(byName)), func(t *Table) bool { return !t.Remote }) {
		// The tags were unset since the request was made: completing
		// it would erase nothing.
		return errors.New("no column is tagged with the request's tag values any more; tag the columns again")
	}
	r.Tables = nil
	for _, key := range slices.Sorted(maps.Keys(byName)) {
		r.Tables = append(r.Tables, *byName[key])
	}
	sources, err := s.tables(ctx, r)
	if err != nil {
		return err
	}
	clones, err := s.clones(ctx, r, sources)
	if err != nil {
		return err
	}
	r.Tables = append(r.Tables, clones...)
	if r.Downstream, err = s.downstream(ctx, r); err != nil {
		return fmt.Errorf("lineage: %w", err)
	}
	return nil
}

// Properties of a zero-copy clone (see package clone): sharesProperty
// lists, as a JSON array, the locations of other tables whose files its
// metadata may reference, the table it was cloned from and those that one
// shares files with in turn; sourceProperty names the table it was cloned
// from, and stays once it references none of that table's files.
const (
	sharesProperty = "igloo.clone.shares-files-of"
	sourceProperty = "igloo.clone.source"
)

// shares are the locations whose files tbl says it may reference.
func shares(tbl *table.Table) []string {
	var locs []string
	if v := tbl.Properties()[sharesProperty]; v != "" {
		_ = json.Unmarshal([]byte(v), &locs)
	}
	for i, l := range locs {
		locs[i] = strings.TrimRight(l, "/")
	}
	return locs
}

// clones finds the tables that share data files with sources, which a
// file must be rewritten in, and its snapshots expired in, before it can
// go: zero-copy clones of them, the tables they are clones of, and so on.
// A clone says what it was made from and shares, and its columns are the
// source's, by field ID; a clone that no longer shares files still holds
// copies of its source's rows. Tables that reference another's files
// without saying so are found by their manifests, among those with a
// column named like a tagged one, and their columns of those names are
// taken to hold the same.
func (s *Service) clones(ctx context.Context, r *Request, sources []loaded) ([]Table, error) {
	idents, err := s.opts.Catalog.Tables(ctx)
	if err != nil {
		return nil, err
	}
	var others []*table.Table
	for _, id := range idents {
		if slices.ContainsFunc(r.Tables, func(t Table) bool { return strings.EqualFold(t.Name, tableName(id)) }) {
			continue
		}
		tbl, err := s.opts.Catalog.LoadTable(ctx, id)
		if errors.Is(err, icecat.ErrNoSuchTable) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", tableName(id), err)
		}
		others = append(others, tbl)
	}
	var out []Table
	read := map[*table.Table]*tableFiles{}
	for len(sources) > 0 {
		var found []loaded
		for _, src := range sources {
			for i := 0; i < len(others); i++ {
				o := others[i]
				t, err := s.related(ctx, src, o, read)
				if err != nil {
					return nil, err
				}
				if t == nil {
					continue
				}
				out = append(out, *t)
				found = append(found, loaded{&out[len(out)-1], o})
				others = slices.Delete(others, i, i+1)
				i--
			}
		}
		// Clones of clones, and their sources, share files too.
		sources = found
	}
	return out, nil
}

// related returns other as a table the request erases from too, if it
// shares data files with src, or nil.
func (s *Service) related(ctx context.Context, src loaded, other *table.Table, read map[*table.Table]*tableFiles) (*Table, error) {
	name := tableName(other.Identifier())
	srcLoc, otherLoc := strings.TrimRight(src.tbl.Location(), "/"), strings.TrimRight(other.Location(), "/")
	switch {
	case slices.Contains(shares(other), srcLoc) || strings.EqualFold(other.Properties()[sourceProperty], src.t.Name):
		return &Table{Name: name, Columns: sameFields(src, other), Reason: "a clone of " + src.t.Name}, nil
	case slices.Contains(shares(src.tbl), otherLoc) || strings.EqualFold(src.tbl.Properties()[sourceProperty], name):
		return &Table{Name: name, Columns: sameFields(src, other), Reason: src.t.Name + " is a clone of it"}, nil
	}
	var cols []Column
	for _, c := range src.t.Columns {
		if f, ok := other.Schema().FindFieldByNameCaseInsensitive(c.Name); ok {
			c.Name = f.Name
			cols = append(cols, c)
		}
	}
	if len(cols) == 0 {
		return nil, nil
	}
	tf := read[other]
	if tf == nil {
		var err error
		if tf, err = readFiles(ctx, other); err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		read[other] = tf
	}
	if slices.ContainsFunc(slices.Collect(maps.Keys(tf.live(false))), func(p string) bool { return within(p, srcLoc) }) {
		return &Table{Name: name, Columns: cols, Reason: "shares data files with " + src.t.Name}, nil
	}
	return nil, nil
}

// sameFields are src's tagged columns as other names them: the fields
// with the same IDs, as a clone's metadata copies its source's.
func sameFields(src loaded, other *table.Table) []Column {
	var out []Column
	for _, f := range fields(src.tbl, src.t) {
		c := f.Column
		for _, sc := range append([]*iceberg.Schema{other.Schema()}, other.Metadata().Schemas()...) {
			if n, ok := sc.FindColumnName(f.id); ok {
				c.Name = n
				break
			}
		}
		out = append(out, c)
	}
	return out
}

// deleteRows deletes the matching rows of every table, as a DELETE does:
// queries no longer see them, but their files still hold them.
func (s *Service) deleteRows(ctx context.Context, r *Request) error {
	tables, err := s.tables(ctx, r)
	if err != nil {
		return err
	}
	for _, l := range tables {
		if cond := currentCondition(r.Salt, fields(l.tbl, l.t), r.Subjects); cond != "" {
			n, err := s.rows.Delete(ctx, l.tbl, cond)
			if err != nil {
				return fmt.Errorf("delete from %s: %w", l.t.Name, err)
			}
			l.t.RowsDeleted += n
		}
		l.t.done(StepDelete, s.now())
		if err := s.save(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// rewrite writes every current data file that holds matching rows again
// with its deletes applied, which leaves the rows out, and then the
// current manifests that still list such files as deleted.
func (s *Service) rewrite(ctx context.Context, r *Request) error {
	for i := range r.Tables {
		t := &r.Tables[i]
		if t.Remote {
			continue
		}
		for attempt := 1; ; attempt++ {
			tbl, ok, err := s.load(ctx, t)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			n, err := s.rewriteTable(ctx, r, t, tbl)
			if errors.Is(err, table.ErrCommitFailed) && attempt < commitAttempts {
				continue
			}
			if err != nil {
				return fmt.Errorf("rewrite %s: %w", t.Name, err)
			}
			if t.FilesRewritten += n; n == 0 {
				break
			}
		}
		t.done(StepRewrite, s.now())
		if err := s.save(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// rewriteTable rewrites tbl's current files that hold matching rows and
// returns how many it replaced. Once none does, it rewrites the manifests
// that still list such files, and returns 0.
func (s *Service) rewriteTable(ctx context.Context, r *Request, t *Table, tbl *table.Table) (int, error) {
	if tbl.CurrentSnapshot() == nil {
		return 0, nil
	}
	flds := fields(tbl, t)
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return 0, err
	}
	var paths []string
	for _, task := range tasks {
		if scannable(task.File, flds) {
			paths = append(paths, task.File.FilePath())
		}
		for _, d := range task.EqualityDeleteFiles {
			if scannable(d, flds) && !slices.Contains(paths, d.FilePath()) {
				paths = append(paths, d.FilePath())
			}
		}
	}
	matched, err := s.matchingFiles(ctx, r.Salt, paths, scanColumns(flds, r.Subjects))
	if err != nil {
		return 0, err
	}
	if err := s.record(ctx, r, t, fileMatched, slices.Collect(maps.Keys(matched))); err != nil {
		return 0, err
	}
	// A data file goes when it holds matches, or when an equality delete
	// that does applies to it: once all of those are rewritten, the delete
	// is dead and goes too.
	var selected []table.FileScanTask
	for _, task := range tasks {
		if matched[task.File.FilePath()] > 0 || slices.ContainsFunc(task.EqualityDeleteFiles, func(d iceberg.DataFile) bool {
			return matched[d.FilePath()] > 0
		}) {
			selected = append(selected, task)
		}
	}
	if len(selected) == 0 {
		bad, err := s.matched(ctx, r)
		if err != nil {
			return 0, err
		}
		_, err = s.forget(ctx, tbl, bad)
		return 0, err
	}
	return s.rewriteFiles(ctx, tbl, selected)
}

// rewriteFiles writes the tasks' data files again with their deletes
// applied, a partition at a time and at most maxRewriteBytes per commit,
// and returns how many files it replaced.
func (s *Service) rewriteFiles(ctx context.Context, tbl *table.Table, tasks []table.FileScanTask) (int, error) {
	byPartition := map[string]*table.CompactionTaskGroup{}
	for _, task := range tasks {
		key := partitionKey(task.File)
		g := byPartition[key]
		if g == nil {
			g = &table.CompactionTaskGroup{PartitionKey: key}
			byPartition[key] = g
		}
		g.Tasks = append(g.Tasks, task)
		g.TotalSizeBytes += task.File.FileSizeBytes()
	}
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return 0, err
	}
	replaced := 0
	keys := slices.Sorted(maps.Keys(byPartition))
	for len(keys) > 0 {
		var (
			groups []table.CompactionTaskGroup
			total  int64
			paths  = map[string]struct{}{}
		)
		for len(keys) > 0 && (len(groups) == 0 || total+byPartition[keys[0]].TotalSizeBytes <= maxRewriteBytes) {
			g := byPartition[keys[0]]
			groups, total, keys = append(groups, *g), total+g.TotalSizeBytes, keys[1:]
			for _, task := range g.Tasks {
				paths[task.File.FilePath()] = struct{}{}
			}
		}
		dead, err := compaction.CollectDeadEqualityDeletes(ctx, fsys, tbl.CurrentSnapshot(), paths)
		if err != nil {
			return replaced, fmt.Errorf("find dead equality deletes: %w", err)
		}
		tx := tbl.NewTransaction()
		res, err := tx.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{ExtraDeleteFilesToRemove: dead, SnapshotProps: operation})
		if err != nil {
			return replaced, err
		}
		if tbl, err = tx.Commit(ctx); err != nil {
			return replaced, err
		}
		replaced += res.RemovedDataFiles
	}
	return replaced, nil
}

// partitionKey groups files of one partition, as compaction does.
func partitionKey(df iceberg.DataFile) string {
	part := df.Partition()
	var b strings.Builder
	fmt.Fprintf(&b, "%d", df.SpecID())
	for _, id := range slices.Sorted(maps.Keys(part)) {
		fmt.Fprintf(&b, "/%d=%v", id, part[id])
	}
	return b.String()
}

// forget writes the current snapshot's manifests again without the bad
// files, which are no longer live, so they list them as deleted, with
// their column bounds. It returns the table as committed.
func (s *Service) forget(ctx context.Context, tbl *table.Table, bad map[string]bool) (*table.Table, error) {
	tf, err := readCurrent(ctx, tbl)
	if err != nil {
		return nil, err
	}
	mentioned := map[string]struct{}{}
	for _, m := range tf.manifests[tf.current()] {
		for _, e := range tf.entries[m] {
			if !bad[e.file.FilePath()] {
				continue
			}
			if e.live() {
				return nil, fmt.Errorf("%s still holds matching rows", base(e.file.FilePath()))
			}
			mentioned[e.file.FilePath()] = struct{}{}
		}
	}
	if len(mentioned) == 0 {
		return tbl, nil
	}
	tx := tbl.NewTransaction()
	if err := tx.ForgetFiles(ctx, mentioned, operation); err != nil {
		return nil, err
	}
	return tx.Commit(ctx)
}

// readCurrent reads the current snapshot's manifests only.
func readCurrent(ctx context.Context, tbl *table.Table) (*tableFiles, error) {
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	tf := &tableFiles{tbl: tbl, entries: map[string][]entry{}, manifests: map[int64][]string{}}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return tf, nil
	}
	manifests, err := snap.Manifests(fsys)
	if err != nil {
		return nil, err
	}
	for _, m := range manifests {
		tf.manifests[snap.SnapshotID] = append(tf.manifests[snap.SnapshotID], m.FilePath())
		for e, err := range m.Entries(fsys, false) {
			if err != nil {
				return nil, err
			}
			tf.entries[m.FilePath()] = append(tf.entries[m.FilePath()], entry{status: e.Status(), file: e.DataFile()})
		}
	}
	return tf, nil
}

// matched are the files the request found matching rows in, in any of its
// tables: a clone references its source's files.
func (s *Service) matched(ctx context.Context, r *Request) (map[string]bool, error) {
	files, err := s.opts.Store.Files(ctx, r.ID)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, f := range files {
		if f.Kind == fileMatched {
			out[f.Path] = true
		}
	}
	return out, nil
}

// record notes files of t to delete once nothing references them.
func (s *Service) record(ctx context.Context, r *Request, t *Table, kind string, paths []string) error {
	files := make([]File, len(paths))
	for i, p := range paths {
		files[i] = File{Path: p, Table: t.Name, Kind: kind}
	}
	return s.opts.Store.AddFiles(ctx, r.ID, files)
}

// listings lists the files under the locations of a request's tables,
// each once.
type listings struct {
	fsys      iceio.IO
	locations []string
	listed    map[string]map[string]time.Time
}

func newListings(ctx context.Context, tables []loaded) (*listings, error) {
	l := &listings{listed: map[string]map[string]time.Time{}}
	for _, t := range tables {
		l.locations = append(l.locations, t.tbl.Location())
	}
	if len(tables) > 0 {
		fsys, err := tables[0].tbl.FS(ctx)
		if err != nil {
			return nil, err
		}
		l.fsys = fsys
	}
	return l, nil
}

// list lists the files under dir.
func (l *listings) list(dir string) (map[string]time.Time, error) {
	if files, ok := l.listed[dir]; ok {
		return files, nil
	}
	files, err := listFiles(l.fsys, dir)
	if err != nil {
		return nil, err
	}
	l.listed[dir] = files
	return files, nil
}

// exists reports whether p, under the location of one of the tables, is
// in storage. Files elsewhere count as gone.
func (l *listings) exists(p string) (bool, error) {
	for _, loc := range l.locations {
		if within(p, loc) {
			files, err := l.list(loc)
			if err != nil {
				return false, err
			}
			if _, ok := files[p]; ok {
				return true, nil
			}
		}
	}
	return false, nil
}

// bad are the files of a table that held identifiers: those the request
// found matching rows in, those older snapshots reference that do, and
// those whose manifest bounds may hold one, unless a scan of them found
// none. It scans the files older snapshots reference that it has not yet.
func (s *Service) bad(ctx context.Context, r *Request, t *Table, tf *tableFiles, flds []field, l *listings) (map[string]bool, error) {
	known, err := s.matched(ctx, r)
	if err != nil {
		return nil, err
	}
	current := tf.live(true)
	scanned := map[string]bool{}
	var paths []string
	for p, df := range tf.live(false) {
		if !scannable(df, flds) {
			continue
		}
		if current[p] != nil {
			scanned[p] = true
			continue
		}
		if exists, err := l.exists(p); err != nil {
			return nil, err
		} else if exists && !known[p] {
			paths, scanned[p] = append(paths, p), true
		}
	}
	matched, err := s.matchingFiles(ctx, r.Salt, paths, scanColumns(flds, r.Subjects))
	if err != nil {
		return nil, err
	}
	if err := s.record(ctx, r, t, fileMatched, slices.Collect(maps.Keys(matched))); err != nil {
		return nil, err
	}
	for p := range matched {
		known[p] = true
	}
	for p := range tf.boundMatches(newMatcher(r), flds) {
		if !scanned[p] {
			known[p] = true
		}
	}
	return known, nil
}

// expire expires, in every table, the snapshots that reference files that
// held identifiers, however recent: time travel to them ends. Snapshots
// that reference none stay, older ones included. The current snapshot is
// first made to reference none.
func (s *Service) expire(ctx context.Context, r *Request) error {
	tables, err := s.tables(ctx, r)
	if err != nil {
		return err
	}
	l, err := newListings(ctx, tables)
	if err != nil {
		return err
	}
	for _, lt := range tables {
		t, tbl := lt.t, lt.tbl
		for attempt := 1; ; attempt++ {
			n, err := s.expireTable(ctx, r, t, tbl, l)
			if errors.Is(err, table.ErrCommitFailed) && attempt < commitAttempts {
				if tbl, err = s.opts.Catalog.LoadTable(ctx, tbl.Identifier()); err == nil {
					continue
				}
			}
			if err != nil {
				return fmt.Errorf("expire snapshots of %s: %w", t.Name, err)
			}
			t.SnapshotsExpired += n
			break
		}
		t.done(StepExpire, s.now())
		if err := s.save(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) expireTable(ctx context.Context, r *Request, t *Table, tbl *table.Table, l *listings) (int, error) {
	flds := fields(tbl, t)
	tf, err := readFiles(ctx, tbl)
	if err != nil {
		return 0, err
	}
	bad, err := s.bad(ctx, r, t, tf, flds, l)
	if err != nil {
		return 0, err
	}
	if tf.mentions(tf.current(), bad) {
		if tbl, err = s.forget(ctx, tbl, bad); err != nil {
			return 0, err
		}
		if tf, err = readFiles(ctx, tbl); err != nil {
			return 0, err
		}
	}
	var expired, retained []int64
	for _, snap := range tbl.Metadata().Snapshots() {
		if snap.SnapshotID != tf.current() && tf.mentions(snap.SnapshotID, bad) {
			expired = append(expired, snap.SnapshotID)
		} else {
			retained = append(retained, snap.SnapshotID)
		}
	}
	if len(expired) == 0 {
		return 0, nil
	}
	// What only the expired snapshots reference goes once they do.
	kept := tf.kept(retained)
	var gone []string
	for p := range tf.files(expired) {
		if !kept[p] {
			gone = append(gone, p)
		}
	}
	if err := s.record(ctx, r, t, fileExpired, gone); err != nil {
		return 0, err
	}
	updates := []table.Update{table.NewRemoveSnapshotsUpdate(expired, false)}
	for name, ref := range tbl.Metadata().Refs() {
		if name != table.MainBranch && slices.Contains(expired, ref.SnapshotID) {
			updates = append(updates, table.NewRemoveSnapshotRefUpdate(name))
		}
	}
	current := tf.current()
	reqs := []table.Requirement{table.AssertTableUUID(tbl.Metadata().TableUUID()), table.AssertRefSnapshotID(table.MainBranch, &current)}
	if _, _, err := s.opts.Catalog.CommitTable(ctx, tbl.Identifier(), reqs, updates); err != nil {
		return 0, err
	}
	s.log.Info("expired snapshots holding erased data", "request", r.ID, "table", t.Name, "snapshots", len(expired))
	return len(expired), nil
}

// purge deletes from object storage the files the request recorded, once
// none of its tables references them, and the tables' orphan files old
// enough to be safe to delete. A newer orphan that holds matching rows
// makes the request wait until it is old enough. It holds the locks of the
// tables' locations meanwhile, which making a clone of one takes too, so
// no clone that references a file it deletes is made.
func (s *Service) purge(ctx context.Context, r *Request) error {
	release, err := s.lockLocations(ctx, r)
	if err != nil {
		return err
	}
	defer release()
	tables, referenced, err := s.referenced(ctx, r)
	if err != nil {
		return err
	}
	files, err := s.opts.Store.Files(ctx, r.ID)
	if err != nil {
		return err
	}
	var gone []File
	going := map[string]bool{}
	for _, f := range files {
		if f.Deleted.IsZero() && !referenced[f.Path] {
			gone, going[f.Path] = append(gone, f), true
		}
	}
	var waitUntil time.Time
	for _, l := range tables {
		old, young, err := s.orphans(ctx, l.tbl, referenced)
		if err != nil {
			return fmt.Errorf("orphan files of %s: %w", l.t.Name, err)
		}
		for _, p := range old {
			if !going[p] {
				gone = append(gone, File{Path: p, Table: l.t.Name, Kind: fileOrphan})
			}
		}
		maps.DeleteFunc(young, func(p string, _ time.Time) bool { return going[p] })
		matched, err := s.matchingFiles(ctx, r.Salt, parquet(slices.Collect(maps.Keys(young))), scanColumns(fields(l.tbl, l.t), r.Subjects))
		if err != nil {
			return err
		}
		for p := range matched {
			if at := young[p].Add(orphanAge); waitUntil.IsZero() || at.Before(waitUntil) {
				waitUntil = at
			}
		}
	}
	if err := s.unshared(ctx, r, gone); err != nil {
		return err
	}
	if err := s.delete(ctx, r, tables, gone); err != nil {
		return err
	}
	for _, l := range tables {
		l.t.done(StepPurge, s.now())
	}
	if !waitUntil.IsZero() {
		s.log.Info("erasure waits until a recent unreferenced file with matching rows is old enough to delete",
			"request", r.ID, "until", waitUntil)
		r.wait(waitUntil)
	}
	return nil
}

// unshared checks that no table the request does not erase from
// references one of files, as a table that shares files with one of its
// tables without saying so, or by columns of other names, may: deleting
// the file would break that table, which holds the rows too.
func (s *Service) unshared(ctx context.Context, r *Request, files []File) error {
	if len(files) == 0 {
		return nil
	}
	paths := map[string]bool{}
	for _, f := range files {
		paths[f.Path] = true
	}
	idents, err := s.opts.Catalog.Tables(ctx)
	if err != nil {
		return err
	}
	for _, id := range idents {
		name := tableName(id)
		if slices.ContainsFunc(r.Tables, func(t Table) bool { return strings.EqualFold(t.Name, name) }) {
			continue
		}
		tbl, err := s.opts.Catalog.LoadTable(ctx, id)
		if errors.Is(err, icecat.ErrNoSuchTable) {
			continue
		}
		if err != nil {
			return fmt.Errorf("load %s: %w", name, err)
		}
		tf, err := readFiles(ctx, tbl)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		for p := range tf.referenced() {
			if paths[p] {
				return fmt.Errorf("%s references %s, a file the request deletes, but the request does not erase from %[1]s: "+
					"tag the columns of %[1]s that hold the identifiers and make a new request for them; this one goes on once that one is done", name, base(p))
			}
		}
	}
	return nil
}

// lockLocations takes the locks of the locations of r's tables
// (Options.Locks).
func (s *Service) lockLocations(ctx context.Context, r *Request) (release func(), err error) {
	if s.opts.Locks == nil {
		return func() {}, nil
	}
	tables, err := s.tables(ctx, r)
	if err != nil {
		return nil, err
	}
	var locs []string
	for _, l := range tables {
		locs = append(locs, l.tbl.Location())
	}
	release, ok, err := s.opts.Locks.Lock(ctx, locs, false)
	if err != nil {
		return nil, fmt.Errorf("lock the tables' locations: %w", err)
	}
	if !ok {
		return nil, errors.New("a clone of one of the tables is being made; deleting files once it is")
	}
	return release, nil
}

// referenced loads r's tables, and clones of them made since the request
// found its tables, which it adds to them, and returns them with every
// file any of them references.
func (s *Service) referenced(ctx context.Context, r *Request) ([]loaded, map[string]bool, error) {
	tables, err := s.tables(ctx, r)
	if err != nil {
		return nil, nil, err
	}
	added, err := s.clones(ctx, r, tables)
	if err != nil {
		return nil, nil, err
	}
	if len(added) > 0 {
		for _, t := range added {
			s.log.Info("erasure found a table made since it began that shares files with its tables",
				"request", r.ID, "table", t.Name, "reason", t.Reason)
		}
		r.Tables = append(r.Tables, added...)
		if tables, err = s.tables(ctx, r); err != nil {
			return nil, nil, err
		}
	}
	referenced := map[string]bool{}
	for _, l := range tables {
		tf, err := readFiles(ctx, l.tbl)
		if err != nil {
			return nil, nil, err
		}
		maps.Copy(referenced, tf.referenced())
	}
	return tables, referenced, nil
}

// orphans lists the files under tbl's location that referenced leaves out:
// those old enough to delete, and the others with their times. Only
// tables in Igloo's warehouse have orphans: a table registered from
// elsewhere may share its location with other data.
func (s *Service) orphans(ctx context.Context, tbl *table.Table, referenced map[string]bool) ([]string, map[string]time.Time, error) {
	young := map[string]time.Time{}
	if s.opts.Warehouse == "" || !within(tbl.Location(), s.opts.Warehouse) {
		return nil, young, nil
	}
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return nil, nil, err
	}
	listed, err := listFiles(fsys, tbl.Location())
	if err != nil {
		return nil, nil, err
	}
	// A listing that spelled paths otherwise than the metadata would make
	// every file look unreferenced: refuse rather than delete live files.
	if current := tbl.CurrentSnapshot(); current != nil {
		tf, err := readCurrent(ctx, tbl)
		if err != nil {
			return nil, nil, err
		}
		for p := range tf.live(true) {
			if _, ok := listed[p]; within(p, tbl.Location()) && !ok {
				return nil, nil, fmt.Errorf("listing %s does not show %s, which the table references", tbl.Location(), base(p))
			}
		}
	}
	var old []string
	now := s.now()
	for p, mod := range listed {
		switch {
		case referenced[p]:
		case now.Sub(mod) >= orphanAge:
			old = append(old, p)
		default:
			young[p] = mod
		}
	}
	return old, young, nil
}

// parquet keeps the Parquet files of paths.
func parquet(paths []string) []string {
	return slices.DeleteFunc(paths, func(p string) bool { return !strings.HasSuffix(p, ".parquet") })
}

// delete deletes files from object storage and records it.
func (s *Service) delete(ctx context.Context, r *Request, tables []loaded, files []File) error {
	if len(files) == 0 {
		return nil
	}
	if len(tables) == 0 {
		return errors.New("none of the request's tables exists to delete files through")
	}
	if err := s.opts.Store.AddFiles(ctx, r.ID, files); err != nil {
		return err
	}
	fsys, err := tables[0].tbl.FS(ctx)
	if err != nil {
		return err
	}
	var paths []string
	for _, f := range files {
		if err := fsys.Remove(f.Path); err != nil && !notExist(err) {
			return errors.Join(fmt.Errorf("delete %s: %w", base(f.Path), err), s.opts.Store.Deleted(ctx, r.ID, paths, s.now()))
		}
		paths = append(paths, f.Path)
		if i := slices.IndexFunc(r.Tables, func(t Table) bool { return t.Name == f.Table }); i >= 0 {
			r.Tables[i].FilesDeleted++
		}
	}
	return s.opts.Store.Deleted(ctx, r.ID, paths, s.now())
}

// listFiles lists the files under dir with their modification times.
func listFiles(fsys iceio.IO, dir string) (map[string]time.Time, error) {
	walker, ok := fsys.(iceio.ListableIO)
	if !ok {
		return nil, errors.New("the warehouse's storage cannot list files")
	}
	out := map[string]time.Time{}
	err := walker.WalkDir(strings.TrimSuffix(dir, "/"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if notExist(err) {
				return fs.SkipDir
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if notExist(err) {
				return nil
			}
			return err
		}
		out[p] = info.ModTime()
		return nil
	})
	return out, err
}

func notExist(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || gcerrors.Code(err) == gcerrors.NotFound
}

// sharing deletes the Delta Sharing copies of the tables' data files that
// hold matching rows; Delta Sharing writes them again, from the files
// that remain, when a recipient needs them.
func (s *Service) sharing(ctx context.Context, r *Request) error {
	tables, err := s.tables(ctx, r)
	if err != nil {
		return err
	}
	var gone []File
	for _, l := range tables {
		matched, err := s.sharedCopies(ctx, r, l)
		if err != nil {
			return err
		}
		for _, p := range matched {
			gone = append(gone, File{Path: p, Table: l.t.Name, Kind: fileSharing})
		}
		l.t.done(StepSharing, s.now())
	}
	return s.delete(ctx, r, tables, gone)
}

// sharedCopies returns the table's Delta Sharing copies that hold matching
// rows.
func (s *Service) sharedCopies(ctx context.Context, r *Request, l loaded) ([]string, error) {
	if s.opts.Warehouse == "" {
		return nil, nil
	}
	fsys, err := l.tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	listed, err := listFiles(fsys, s.opts.Warehouse+sharingDir+l.tbl.Metadata().TableUUID().String()+"/")
	if err != nil {
		return nil, err
	}
	matched, err := s.matchingFiles(ctx, r.Salt, parquet(slices.Collect(maps.Keys(listed))), scanColumns(fields(l.tbl, l.t), r.Subjects))
	if err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(matched)), nil
}

// history redacts the identifiers from the history stores.
func (s *Service) history(ctx context.Context, r *Request) error {
	n, err := s.redact(ctx, r)
	r.HistoryRedacted += n
	return err
}

func (s *Service) redact(ctx context.Context, r *Request) (int64, error) {
	m := newMatcher(r)
	var total int64
	for _, h := range s.opts.History {
		n, err := h.Redact(ctx, m)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

const (
	// liveWindow is how recently a replica must have been seen to count
	// as running: one that was must drop a request's files before the
	// request completes.
	liveWindow = 2 * time.Minute
	// cacheWait is how long a request waits for replicas at a time.
	cacheWait = 20 * time.Second
	// staleCache is how long a disk cache not seen is still expected back.
	staleCache = 30 * 24 * time.Hour
)

// cache waits until every running replica dropped the files the request
// deleted from its read cache.
func (s *Service) cache(ctx context.Context, r *Request) error {
	if s.opts.Purger != nil {
		if err := s.opts.Purger.Once(ctx); err != nil {
			return err
		}
	}
	_, _, waiting, err := s.caches(ctx, r)
	if err != nil {
		return err
	}
	if len(waiting) > 0 {
		s.log.Info("erasure waits for replicas to drop deleted files from their read caches", "request", r.ID,
			"replicas", strings.Join(waiting, ", "))
		r.wait(s.now().Add(cacheWait))
	}
	return nil
}

// caches counts the caches that dropped r's deleted files, and those of
// replicas not running now, which will when they start, and names the
// running replicas that have yet to.
func (s *Service) caches(ctx context.Context, r *Request) (dropped, pending int, waiting []string, err error) {
	files, err := s.opts.Store.Files(ctx, r.ID)
	if err != nil {
		return 0, 0, nil, err
	}
	var latest time.Time
	for _, f := range files {
		if f.Deleted.After(latest) {
			latest = f.Deleted
		}
	}
	if latest.IsZero() {
		return 0, 0, nil, nil
	}
	caches, err := s.opts.Store.Caches(ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	purges, err := s.opts.Store.Purges(ctx, r.ID)
	if err != nil {
		return 0, 0, nil, err
	}
	now := s.now()
	for _, c := range caches {
		switch {
		case !purges[c.ID].Before(latest):
			dropped++
		case now.Sub(c.Seen) < liveWindow:
			waiting = append(waiting, c.Host)
		case !strings.HasPrefix(c.ID, MemoryCache) && now.Sub(c.Seen) < staleCache:
			pending++
		}
	}
	sort.Strings(waiting)
	return dropped, pending, waiting, nil
}

// verify looks for the identifiers in what is left, and returns the step
// to run next: done when it finds nothing, or else the step that removes
// what it found.
func (s *Service) verify(ctx context.Context, r *Request) (string, error) {
	m := newMatcher(r)
	v := &Verification{At: s.now()}
	files, err := s.opts.Store.Files(ctx, r.ID)
	if err != nil {
		return "", err
	}
	tables, referenced, err := s.referenced(ctx, r)
	if err != nil {
		return "", err
	}
	l, err := newListings(ctx, tables)
	if err != nil {
		return "", err
	}
	for _, lt := range tables {
		if err := s.verifyTable(ctx, r, lt, m, referenced, l, v); err != nil {
			return "", fmt.Errorf("verify %s: %w", lt.t.Name, err)
		}
	}
	for _, f := range files {
		if f.Deleted.IsZero() {
			continue
		}
		if exists, err := l.exists(f.Path); err != nil {
			return "", err
		} else if exists {
			v.Storage++
		}
	}
	// What history replicas recorded since the history step is redacted
	// too, and then nothing may be left.
	n, err := s.redact(ctx, r)
	if err != nil {
		return "", err
	}
	r.HistoryRedacted += n
	if v.History, err = s.redact(ctx, r); err != nil {
		return "", err
	}
	var waiting []string
	if v.Caches, v.Pending, waiting, err = s.caches(ctx, r); err != nil {
		return "", err
	}
	r.Verification = v

	next := StepDone
	switch {
	case v.Rows > 0 || v.Files > 0 || v.Bounds > 0 || v.Statistics > 0:
		next = StepDelete
	case v.Storage > 0 || v.Orphans > 0:
		next = StepPurge
	case v.Sharing > 0:
		next = StepSharing
	case v.History > 0:
		next = StepHistory
	case len(waiting) > 0:
		next = StepCache
	}
	if next == StepDone {
		return next, nil
	}
	if r.Rounds++; r.Rounds > maxRounds {
		return "", fmt.Errorf("after %d rounds, verification still finds %s", maxRounds, v)
	}
	s.log.Info("erasure verification found more to remove", "request", r.ID, "found", v.String(), "next", next)
	return next, nil
}

func (s *Service) verifyTable(ctx context.Context, r *Request, lt loaded, m *matcher, referenced map[string]bool, l *listings, v *Verification) error {
	t, tbl := lt.t, lt.tbl
	flds := fields(tbl, t)
	cols := scanColumns(flds, r.Subjects)
	if cond := currentCondition(r.Salt, flds, r.Subjects); cond != "" {
		n, err := s.rows.Count(ctx, tbl, cond)
		if err != nil {
			return err
		}
		v.Rows += n
	}
	tf, err := readFiles(ctx, tbl)
	if err != nil {
		return err
	}
	var data, scanned []string
	for p, df := range tf.live(false) {
		if scannable(df, flds) {
			scanned = append(scanned, p)
			if df.ContentType() == iceberg.EntryContentData {
				data = append(data, p)
			}
		}
	}
	matched, err := s.matchingFiles(ctx, r.Salt, scanned, cols)
	if err != nil {
		return err
	}
	v.Files += int64(len(matched))
	// A bound that may hold an identifier counts unless the file's rows
	// say otherwise: truncated bounds can match without the value.
	for p := range tf.boundMatches(m, flds) {
		if !slices.Contains(scanned, p) || matched[p] > 0 {
			v.Bounds++
		}
	}
	n, err := s.matchingStatistics(ctx, m, data, cols)
	if err != nil {
		return err
	}
	v.Statistics += n
	_, young, err := s.orphans(ctx, tbl, referenced)
	if err != nil {
		return err
	}
	orphans, err := s.matchingFiles(ctx, r.Salt, parquet(slices.Collect(maps.Keys(young))), cols)
	if err != nil {
		return err
	}
	v.Orphans += int64(len(orphans))
	copies, err := s.sharedCopies(ctx, r, lt)
	if err != nil {
		return err
	}
	v.Sharing += int64(len(copies))
	return nil
}

// String summarizes what v found, for errors and logs.
func (v *Verification) String() string {
	var parts []string
	for _, c := range []struct {
		n    int64
		what string
	}{
		{v.Rows, "rows"}, {v.Files, "files"}, {v.Bounds, "manifest bounds"}, {v.Statistics, "Parquet statistics"},
		{v.Storage, "deleted files in storage"}, {v.Orphans, "recent orphan files"}, {v.Sharing, "sharing copies"},
		{v.History, "history records"},
	} {
		if c.n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", c.n, c.what))
		}
	}
	if len(parts) == 0 {
		return "nothing"
	}
	return strings.Join(parts, ", ")
}
