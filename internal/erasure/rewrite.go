package erasure

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"

	"github.com/akkio-inc/igloo/internal/deletefiles"
)

// The rewrite step removes matching rows from the files of every branch
// and tag. It reads the files that hold them with their deletes applied,
// leaves out the rows that match, or sets the columns tagged to be nulled
// to NULL in them, and writes what is left in the same commit that
// replaces the files: rows that arrived since the delete step, or that its
// DELETE could not name, as in a column renamed by another engine, go too.
//
// Its commits are never retried by iceberg-go, which would bring the
// replaced files back (see its IGLOO_PATCHES.md). A commit that loses to
// appends is made again on the new head with the files already written; one
// that loses to anything else is planned again. A table that other writers
// keep changing faster than that makes the request wait and try again
// later, without counting it as a failure.

const (
	// maxPasses bounds how often a ref is rewritten in one run: each pass
	// rewrites what still matches, so more mean matching rows keep
	// arriving.
	maxPasses = 8
	// tagBranch starts the names of the branches requests rewrite tags'
	// snapshots on.
	tagBranch = "igloo-erasure-"
	// rebaseAttempts bounds how often one commit is made again on a head
	// that only appends moved, in one run: a table appended to more often
	// than that makes the request try again later.
	rebaseAttempts = 10
	// maxMoves bounds how often, in one run, a tag is found moved or its
	// branch gone while the request rewrites it.
	maxMoves = 5
)

// errRefGone is a branch dropped while a request rewrote it.
var errRefGone = errors.New("the branch was dropped")

// refNames are tbl's refs: main, the other branches, then the tags, each
// by name, but none of the requests' own branches, which come with their
// tags.
func refNames(tbl *table.Table) []string {
	var branches, tags []string
	for name, ref := range tbl.Metadata().Refs() {
		switch {
		case name == table.MainBranch, strings.HasPrefix(name, tagBranch):
		case ref.SnapshotRefType == table.TagRef:
			tags = append(tags, name)
		default:
			branches = append(branches, name)
		}
	}
	slices.Sort(branches)
	slices.Sort(tags)
	var out []string
	if _, ok := refs(tbl)[table.MainBranch]; ok {
		out = append(out, table.MainBranch)
	}
	return append(append(out, branches...), tags...)
}

// branchFor is the branch request r rewrites tag's snapshot id on. It is
// named after the tag and the snapshot, so that a branch a stopped run
// left is taken up again only for the same tag at the same snapshot.
func branchFor(r *Request, tag string, id int64) string {
	sum := sha256.Sum256([]byte(tag))
	return tagBranch + strings.ToLower(r.ID) + "-" + hex.EncodeToString(sum[:4]) + "-" + strconv.FormatInt(id, 10)
}

// ownBranch reports whether ref name is one of r's branches.
func ownBranch(r *Request, name string) bool {
	own := tagBranch + strings.ToLower(r.ID)
	return name == own || strings.HasPrefix(name, own+"-")
}

// at is tbl as a table whose current snapshot is snapshot id: what scans
// and commits work from. iceberg-go builds a commit to any branch on the
// current snapshot, so a commit to another branch is made on this.
func (s *Service) at(tbl *table.Table, id int64) (*table.Table, error) {
	if cur := tbl.CurrentSnapshot(); cur != nil && cur.SnapshotID == id {
		return tbl, nil
	}
	b, err := table.MetadataBuilderFromBase(tbl.Metadata(), tbl.MetadataLocation())
	if err != nil {
		return nil, err
	}
	if err := b.SetSnapshotRef(table.MainBranch, id, table.BranchRef); err != nil {
		return nil, err
	}
	meta, err := b.Build()
	if err != nil {
		return nil, err
	}
	fsF := func(ctx context.Context) (iceio.IO, error) { return tbl.FS(ctx) }
	return table.New(tbl.Identifier(), meta, tbl.MetadataLocation(), fsF, s.opts.Catalog), nil
}

// pass rewrites what view's current snapshot holds of a request, committing
// to branch, and returns whether it committed; with commit false it only
// says whether it would.
type pass func(ctx context.Context, view *table.Table, branch string, commit bool) (bool, error)

// onRef runs pass on ref name of t's table until it changes nothing: on a
// branch itself, and for a tag, on a branch of the request's own at the
// tag's snapshot, whose head then becomes the tag's. A tag moved or
// dropped meanwhile is someone's choice, which stands: the branch goes,
// and a moved tag is rewritten where it points now.
func (s *Service) onRef(ctx context.Context, r *Request, t *Table, name string, p pass) error {
	for moves := 0; ; moves++ {
		if moves > maxMoves {
			return contended(fmt.Errorf("tag %s of %s kept moving while the request rewrote it", name, t.Name))
		}
		tbl, err := s.reload(ctx, t)
		if err != nil {
			return err
		}
		ref, ok := refs(tbl)[name]
		if !ok {
			// Dropped meanwhile: nothing is left to rewrite but a branch
			// of the request's for it.
			return s.sweep(ctx, r, t, false)
		}
		if ref.SnapshotRefType != table.TagRef {
			if err := s.passes(ctx, t, name, p); !errors.Is(err, errRefGone) {
				return err
			}
			return nil
		}
		original := ref.SnapshotID
		branch := branchFor(r, name, original)
		if _, exists := refs(tbl)[branch]; !exists {
			view, err := s.at(tbl, original)
			if err != nil {
				return err
			}
			if needed, err := p(ctx, view, branch, false); err != nil || !needed {
				return err
			}
			err = s.commitRefs(ctx, tbl,
				[]table.Requirement{table.AssertRefSnapshotID(name, &original), table.AssertRefSnapshotID(branch, nil)},
				table.NewSetSnapshotRefUpdate(branch, original, table.BranchRef, 0, 0, 0))
			if errors.Is(err, table.ErrCommitFailed) {
				continue
			}
			if err != nil {
				return fmt.Errorf("start rewriting tag %s: %w", name, err)
			}
		}
		if err := s.passes(ctx, t, branch, p); errors.Is(err, errRefGone) {
			continue
		} else if err != nil {
			return err
		}
		if tbl, err = s.reload(ctx, t); err != nil {
			return err
		}
		tag, ok := refs(tbl)[name]
		head, rewritten := refs(tbl)[branch]
		if !ok || tag.SnapshotID != original || !rewritten {
			if err := s.sweep(ctx, r, t, false); err != nil {
				return err
			}
			continue
		}
		// The tag points at the rewritten snapshot, with its retention, if
		// it still points where it did.
		var maxAge int64
		if tag.MaxRefAgeMs != nil {
			maxAge = *tag.MaxRefAgeMs
		}
		err = s.commitRefs(ctx, tbl,
			[]table.Requirement{table.AssertRefSnapshotID(name, &original), table.AssertRefSnapshotID(branch, &head.SnapshotID)},
			table.NewSetSnapshotRefUpdate(name, head.SnapshotID, table.TagRef, maxAge, 0, 0), table.NewRemoveSnapshotRefUpdate(branch))
		if errors.Is(err, table.ErrCommitFailed) {
			continue
		}
		if err != nil {
			return fmt.Errorf("move tag %s to its rewritten snapshot: %w", name, err)
		}
		if !slices.Contains(t.Refs, name) {
			t.Refs = append(t.Refs, name)
		}
		return nil
	}
}

// passes runs p on branch until it changes nothing. A commit that loses
// to another is planned again, as often as commitAttempts.
func (s *Service) passes(ctx context.Context, t *Table, branch string, p pass) error {
	for n, conflicts := 1, 0; ; {
		tbl, err := s.reload(ctx, t)
		if err != nil {
			return err
		}
		head, ok := refs(tbl)[branch]
		if !ok {
			return errRefGone
		}
		view, err := s.at(tbl, head.SnapshotID)
		if err != nil {
			return err
		}
		changed, err := p(ctx, view, branch, true)
		var c contentionError
		if errors.As(err, &c) {
			return err
		}
		if errors.Is(err, table.ErrCommitFailed) {
			if conflicts++; conflicts >= commitAttempts {
				return contended(fmt.Errorf("rewrite %s of %s: other writers' commits kept coming first: %w", branch, t.Name, err))
			}
			continue
		}
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		if n++; n > maxPasses {
			return fmt.Errorf("matching rows keep arriving in %s, on %s: stop the writes that add them, and the request goes on", t.Name, branch)
		}
	}
}

// sweep drops r's branches of t's table that no tag needs any more: those
// of tags dropped or moved, or, with all, every one.
func (s *Service) sweep(ctx context.Context, r *Request, t *Table, all bool) error {
	for attempt := 0; ; attempt++ {
		tbl, err := s.reload(ctx, t)
		if err != nil {
			return err
		}
		current := refs(tbl)
		needed := map[string]bool{}
		for name, ref := range current {
			if ref.SnapshotRefType == table.TagRef && !all {
				needed[branchFor(r, name, ref.SnapshotID)] = true
			}
		}
		var (
			reqs    []table.Requirement
			updates []table.Update
		)
		for name, ref := range current {
			if ownBranch(r, name) && !needed[name] {
				id := ref.SnapshotID
				reqs = append(reqs, table.AssertRefSnapshotID(name, &id))
				updates = append(updates, table.NewRemoveSnapshotRefUpdate(name))
			}
		}
		if len(updates) == 0 {
			return nil
		}
		err = s.commitRefs(ctx, tbl, reqs, updates...)
		if errors.Is(err, table.ErrCommitFailed) && attempt < commitAttempts {
			continue
		}
		if err != nil {
			return fmt.Errorf("drop the request's branches of %s: %w", t.Name, err)
		}
		s.log.Info("dropped branches an erasure request rewrote tags on", "request", r.ID, "table", t.Name, "branches", len(updates))
		return nil
	}
}

func (s *Service) commitRefs(ctx context.Context, tbl *table.Table, reqs []table.Requirement, updates ...table.Update) error {
	reqs = append([]table.Requirement{table.AssertTableUUID(tbl.Metadata().TableUUID())}, reqs...)
	_, _, err := s.opts.Catalog.CommitTable(ctx, tbl.Identifier(), reqs, updates)
	return err
}

// rewriteRef rewrites the files of view's current snapshot that hold
// matching rows, and removes equality deletes that hold them and apply to
// no file, committing to branch; once none is left, it rewrites the
// manifests that still list files that held them. It reports whether it
// committed, or, with commit false, would have.
func (s *Service) rewriteRef(ctx context.Context, r *Request, t *Table, view *table.Table, branch string, commit bool) (bool, error) {
	if view.CurrentSnapshot() == nil {
		return false, nil
	}
	flds := fields(view, t)
	if err := checkFields(t, flds); err != nil {
		return false, err
	}
	tasks, err := view.Scan().PlanFiles(ctx)
	if err != nil {
		return false, err
	}
	attached := map[string]bool{}
	var paths []string
	for _, task := range tasks {
		if scannable(task.File) {
			paths = append(paths, task.File.FilePath())
		}
		for _, d := range task.EqualityDeleteFiles {
			attached[d.FilePath()] = true
		}
	}
	head, err := readCurrent(ctx, view)
	if err != nil {
		return false, err
	}
	live := head.live(true)
	deletes := map[string]iceberg.DataFile{}
	for p, df := range live {
		if df.ContentType() == iceberg.EntryContentEqDeletes && scannable(df) {
			deletes[p] = df
			paths = append(paths, p)
		}
	}
	matched, err := s.matchingFiles(ctx, r.salt, paths, scanColumns(flds, r.Subjects))
	if err != nil {
		return false, err
	}
	for p := range matched {
		if r.written[p] {
			return false, fmt.Errorf("%s, which the request wrote without the matching rows, holds some: "+
				"Igloo and DuckDB compare a value differently; report this", base(p))
		}
	}
	if err := s.record(ctx, r, t, fileMatched, slices.Collect(maps.Keys(matched))); err != nil {
		return false, err
	}
	// A data file goes when it holds matches, or when an equality delete
	// that does applies to it: once all of those are rewritten, the delete
	// is dead and goes too. One that applies to no file goes at once.
	var (
		selected []table.FileScanTask
		dangling []iceberg.DataFile
	)
	for _, task := range tasks {
		if matched[task.File.FilePath()] > 0 || slices.ContainsFunc(task.EqualityDeleteFiles, func(d iceberg.DataFile) bool {
			return matched[d.FilePath()] > 0
		}) {
			selected = append(selected, task)
		}
	}
	for p, df := range deletes {
		if matched[p] > 0 && !attached[p] {
			dangling = append(dangling, df)
		}
	}
	if len(selected) > 0 || len(dangling) > 0 {
		if !commit {
			return true, nil
		}
		return true, s.rewriteFiles(ctx, r, t, view, branch, flds, selected, dangling, live)
	}
	bad, err := s.matched(ctx, r)
	if err != nil {
		return false, err
	}
	if !head.mentions(head.current(), bad) {
		return false, nil
	}
	if !commit {
		return true, nil
	}
	return true, s.forget(ctx, view, branch, bad)
}

// batch is one commit of a rewrite: the data files it replaces and those
// that replace them, and the delete files it removes.
type batch struct {
	old, added, removed []iceberg.DataFile
	stats               filterStats
}

// rewriteFiles writes the tasks' data files again with their deletes
// applied and without matching rows, a partition at a time and at most
// maxRewriteBytes per commit. Each commit removes the equality deletes
// that apply to no file left, those that apply to no file at all, and
// the position deletes that delete rows only of files it replaces or of
// files no longer live (see package deletefiles); live are the files live
// in view's current snapshot.
func (s *Service) rewriteFiles(ctx context.Context, r *Request, t *Table, view *table.Table, branch string, flds []field,
	tasks []table.FileScanTask, dangling []iceberg.DataFile, live map[string]iceberg.DataFile) error {
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
	fsys, err := view.FS(ctx)
	if err != nil {
		return err
	}
	filter := newRowFilter(r, flds)
	keys := slices.Sorted(maps.Keys(byPartition))
	for len(keys) > 0 || len(dangling) > 0 {
		var (
			groups []*table.CompactionTaskGroup
			total  int64
			paths  = map[string]struct{}{}
			tasks  []table.FileScanTask
			b      batch
		)
		for len(keys) > 0 && (len(groups) == 0 || total+byPartition[keys[0]].TotalSizeBytes <= maxRewriteBytes) {
			g := byPartition[keys[0]]
			groups, total, keys = append(groups, g), total+g.TotalSizeBytes, keys[1:]
			for _, task := range g.Tasks {
				paths[task.File.FilePath()] = struct{}{}
			}
			tasks = append(tasks, g.Tasks...)
		}
		for _, g := range groups {
			added, err := s.rewriteGroup(ctx, view, g, filter, &b.stats)
			if err != nil {
				return err
			}
			for _, task := range g.Tasks {
				b.old = append(b.old, task.File)
			}
			b.added = append(b.added, added...)
			for _, df := range added {
				r.written[df.FilePath()] = true
			}
		}
		dead, err := compaction.CollectDeadEqualityDeletes(ctx, fsys, view.CurrentSnapshot(), paths)
		if err != nil {
			return fmt.Errorf("find dead equality deletes: %w", err)
		}
		rewritten, current := deletefiles.Paths{}, deletefiles.Paths{}
		for p := range paths {
			rewritten.Add(p)
		}
		for p := range live {
			current.Add(p)
		}
		safe, err := s.targets.Removable(ctx, fsys, tasks, rewritten, current)
		if err != nil {
			return fmt.Errorf("find the position deletes the rewrite may remove: %w", err)
		}
		removed := map[string]bool{}
		for _, d := range slices.Concat(dead, dangling, safe) {
			if !removed[d.FilePath()] {
				removed[d.FilePath()] = true
				b.removed = append(b.removed, d)
			}
		}
		dangling = nil
		committed, err := s.commitBatch(ctx, t, view, branch, &b)
		if err != nil {
			return err
		}
		t.FilesRewritten += len(paths)
		t.RowsDeleted += b.stats.dropped
		t.ValuesNulled += b.stats.nulled
		if branch != table.MainBranch && !strings.HasPrefix(branch, tagBranch) && !slices.Contains(t.Refs, branch) {
			t.Refs = append(t.Refs, branch)
		}
		s.log.Info("rewrote files holding erased data", "request", r.ID, "table", t.Name, "branch", branch,
			"files", len(paths), "rows", b.stats.dropped, "nulled", b.stats.nulled)
		head := refs(committed)[branch]
		if view, err = s.at(committed, head.SnapshotID); err != nil {
			return err
		}
	}
	return nil
}

// commitBatch commits b to branch on view's current snapshot. When
// another commit came first and only appended to the branch since, it
// commits b again on the new head, with the files it wrote: appends add
// data files only, and none of them is one b replaces or a delete that
// applies to one. Otherwise it returns table.ErrCommitFailed, and the
// rewrite is planned again; after rebaseAttempts, a contention error, and
// the request tries again later.
func (s *Service) commitBatch(ctx context.Context, t *Table, view *table.Table, branch string, b *batch) (*table.Table, error) {
	for attempt := 0; ; attempt++ {
		tx := view.NewTransactionOnBranch(branch)
		rw := tx.NewRewrite(operation)
		rw.Apply(b.old, b.added, b.removed)
		if err := rw.Commit(ctx); err != nil {
			return nil, err
		}
		committed, err := tx.CommitWithoutRetry(ctx)
		if err == nil || !errors.Is(err, table.ErrCommitFailed) {
			return committed, err
		}
		if attempt+1 >= rebaseAttempts {
			return nil, contended(fmt.Errorf("rewrite %s of %s: other writers' appends kept coming first: %w", branch, t.Name, err))
		}
		tbl, rerr := s.reload(ctx, t)
		if rerr != nil {
			return nil, rerr
		}
		head, ok := refs(tbl)[branch]
		if !ok {
			return nil, errRefGone
		}
		if !appendedSince(tbl, view.CurrentSnapshot().SnapshotID, head.SnapshotID) {
			return nil, err
		}
		if view, err = s.at(tbl, head.SnapshotID); err != nil {
			return nil, err
		}
		// A little jitter keeps the commit from meeting a steady writer's
		// at the same moment each time.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(rand.Int64N(int64(10*time.Millisecond) << min(attempt, 4)))):
		}
	}
}

// appendedSince reports whether every snapshot from head back to base, in
// tbl, is an append.
func appendedSince(tbl *table.Table, base, head int64) bool {
	for id := head; id != base; {
		snap := tbl.SnapshotByID(id)
		if snap == nil || snap.Summary == nil || snap.Summary.Operation != table.OpAppend || snap.ParentSnapshotID == nil {
			return false
		}
		id = *snap.ParentSnapshotID
	}
	return true
}

// rewriteGroup writes a group's files again, filtered.
func (s *Service) rewriteGroup(ctx context.Context, view *table.Table, g *table.CompactionTaskGroup, filter *rowFilter, stats *filterStats) ([]iceberg.DataFile, error) {
	schema, records, err := view.Scan().ReadTasks(ctx, g.Tasks)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", g.PartitionKey, err)
	}
	var filterErr error
	filtered := func(yield func(arrow.RecordBatch, error) bool) {
		for rec, err := range records {
			if err != nil {
				yield(nil, err)
				return
			}
			out, err := filter.apply(ctx, rec, stats)
			rec.Release()
			if err != nil {
				filterErr = err
				yield(nil, err)
				return
			}
			if out.NumRows() == 0 {
				out.Release()
				continue
			}
			if !yield(out, nil) {
				return
			}
		}
	}
	var added []iceberg.DataFile
	for df, err := range table.WriteRecords(ctx, view, schema, iter.Seq2[arrow.RecordBatch, error](filtered), table.WithClusteredWrite()) {
		if err != nil {
			return nil, errors.Join(err, filterErr)
		}
		added = append(added, df)
	}
	return added, filterErr
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

// forget writes view's current manifests again without the bad files,
// which are no longer live there, so they no longer list them as
// deleted, with their column bounds, committing to branch. The commit is
// never retried by iceberg-go: one that loses is planned again.
func (s *Service) forget(ctx context.Context, view *table.Table, branch string, bad pathSet) error {
	tf, err := readCurrent(ctx, view)
	if err != nil {
		return err
	}
	mentioned := map[string]struct{}{}
	for _, m := range tf.manifests[tf.current()] {
		for _, e := range tf.entries[m] {
			if !bad.has(e.file.FilePath()) {
				continue
			}
			if e.live() {
				return fmt.Errorf("%s still holds matching rows", base(e.file.FilePath()))
			}
			mentioned[e.file.FilePath()] = struct{}{}
		}
	}
	if len(mentioned) == 0 {
		return nil
	}
	tx := view.NewTransactionOnBranch(branch)
	if err := tx.ForgetFiles(ctx, mentioned, operation); err != nil {
		return err
	}
	_, err = tx.CommitWithoutRetry(ctx)
	return err
}

// rowFilter leaves the rows that match out of records, or sets their
// columns to be nulled to NULL, comparing values as DuckDB's scans do:
// their text, as CAST(... AS VARCHAR) or, for bytes, hex() writes it,
// lower-cased and trimmed.
type rowFilter struct {
	salt string
	fold *folding
	cols []filterColumn
}

type filterColumn struct {
	name   string
	hashes map[string]bool
	null   bool
}

type filterStats struct{ dropped, nulled int64 }

func newRowFilter(r *Request, flds []field) *rowFilter {
	f := &rowFilter{salt: r.salt, fold: r.fold}
	for _, fl := range flds {
		if fl.current == "" {
			// A dropped column is not written again: its values go with it.
			continue
		}
		i := slices.IndexFunc(r.Subjects, func(s Subject) bool { return s.Tag == fl.Tag && s.Value == fl.Value })
		if i < 0 {
			continue
		}
		hashes := map[string]bool{}
		for _, h := range r.Subjects[i].hashes(fl.Digest) {
			hashes[h] = true
		}
		f.cols = append(f.cols, filterColumn{name: fl.current, hashes: hashes, null: fl.Null})
	}
	return f
}

// apply returns rec filtered; rec stays the caller's to release.
func (f *rowFilter) apply(ctx context.Context, rec arrow.RecordBatch, stats *filterStats) (arrow.RecordBatch, error) {
	n := int(rec.NumRows())
	drop := make([]bool, n)
	nulls := map[int][]bool{}
	for _, c := range f.cols {
		idx := rec.Schema().FieldIndices(c.name)
		if len(idx) != 1 {
			continue
		}
		col := rec.Column(idx[0])
		hit := make([]bool, n)
		found := false
		for i := range n {
			if col.IsNull(i) {
				continue
			}
			text, err := valueText(col, i)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", c.name, err)
			}
			if c.hashes[salted(f.salt, f.fold.normal(text))] {
				hit[i], found = true, true
			}
		}
		if !found {
			continue
		}
		if c.null {
			nulls[idx[0]] = hit
			continue
		}
		for i, h := range hit {
			drop[i] = drop[i] || h
		}
	}
	cols := rec.Columns()
	out := make([]arrow.Array, len(cols))
	for i, col := range cols {
		if hit, ok := nulls[i]; ok {
			var err error
			if out[i], err = withNulls(col, hit, drop); err != nil {
				return nil, err
			}
			for j, h := range hit {
				if h && !drop[j] {
					stats.nulled++
				}
			}

			continue
		}
		col.Retain()
		out[i] = col
	}
	nulled := array.NewRecordBatch(rec.Schema(), out, rec.NumRows())
	for _, a := range out {
		a.Release()
	}
	dropped := 0
	for _, d := range drop {
		if d {
			dropped++
		}
	}
	if dropped == 0 {
		return nulled, nil
	}
	defer nulled.Release()
	stats.dropped += int64(dropped)
	keep := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer keep.Release()
	for _, d := range drop {
		keep.Append(!d)
	}
	mask := keep.NewBooleanArray()
	defer mask.Release()
	return compute.FilterRecordBatch(ctx, nulled, mask, compute.DefaultFilterOptions())
}

// valueText is a column value as DuckDB's scans compare it.
func valueText(col arrow.Array, i int) (string, error) {
	switch a := col.(type) {
	case *array.String:
		return a.Value(i), nil
	case *array.LargeString:
		return a.Value(i), nil
	case *array.StringView:
		return a.Value(i), nil
	case *extensions.UUIDArray:
		return a.Value(i).String(), nil
	case *array.Binary:
		return hex.EncodeToString(a.Value(i)), nil
	case *array.LargeBinary:
		return hex.EncodeToString(a.Value(i)), nil
	case *array.FixedSizeBinary:
		return hex.EncodeToString(a.Value(i)), nil
	case *array.Int32:
		return strconv.FormatInt(int64(a.Value(i)), 10), nil
	case *array.Int64:
		return strconv.FormatInt(a.Value(i), 10), nil
	}
	return "", fmt.Errorf("cannot compare values of type %s with identifiers", col.DataType())
}

// withNulls is col with NULL where hit says, except in rows to drop.
func withNulls(col arrow.Array, hit, drop []bool) (arrow.Array, error) {
	flat, err := array.Concatenate([]arrow.Array{col}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	defer flat.Release()
	data := flat.Data()
	n := data.Len()
	valid := memory.NewResizableBuffer(memory.DefaultAllocator)
	valid.Resize(int(bitutil.BytesForBits(int64(n))))
	bits := valid.Bytes()
	nullCount := 0
	for i := range n {
		if flat.IsValid(i) && !(hit[i] && !drop[i]) {
			bitutil.SetBit(bits, i)
		} else {
			bitutil.ClearBit(bits, i)
			nullCount++
		}
	}
	buffers := slices.Clone(data.Buffers())
	buffers[0] = valid
	out := array.NewData(data.DataType(), n, buffers, data.Children(), nullCount, 0)
	defer out.Release()
	valid.Release()
	return array.MakeFromData(out), nil
}

// refs are a table's branches and tags, by name.
func refs(tbl *table.Table) map[string]table.SnapshotRef { return maps.Collect(tbl.Metadata().Refs()) }
