package erasure

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"

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
)

// The rewrite step removes matching rows from the files of every branch
// and tag. It reads the files that hold them with their deletes applied,
// leaves out the rows that match, or sets the columns tagged to be nulled
// to NULL in them, and writes what is left in the same commit that
// replaces the files: rows that arrived since the delete step, or that its
// DELETE could not name, as in a column renamed by another engine, go too.

const (
	// maxPasses bounds how often a ref is rewritten in one run: each pass
	// rewrites what still matches, so more mean matching rows keep
	// arriving.
	maxPasses = 8
	// tagBranch starts the name of the branch a request rewrites a tag's
	// snapshot on.
	tagBranch = "igloo-erasure-"
)

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

// onRef runs pass on ref name of t's table until it changes nothing: on
// the branch, or for a tag, on a branch of the request's own at the tag's
// snapshot, whose head then becomes the tag's. pass returns whether it
// committed; with commit false it only says whether it would.
func (s *Service) onRef(ctx context.Context, r *Request, t *Table, name string,
	pass func(ctx context.Context, view *table.Table, branch string, commit bool) (bool, error)) error {
	tbl, err := s.reload(ctx, t)
	if err != nil {
		return err
	}
	ref, ok := refs(tbl)[name]
	if !ok {
		return nil
	}
	branch := name
	if ref.SnapshotRefType == table.TagRef {
		branch = tagBranch + strings.ToLower(r.ID)
		if _, exists := refs(tbl)[branch]; !exists {
			view, err := s.at(tbl, ref.SnapshotID)
			if err != nil {
				return err
			}
			if needed, err := pass(ctx, view, branch, false); err != nil || !needed {
				return err
			}
			if err := s.commitRefs(ctx, tbl,
				[]table.Requirement{table.AssertRefSnapshotID(name, &ref.SnapshotID), table.AssertRefSnapshotID(branch, nil)},
				table.NewSetSnapshotRefUpdate(branch, ref.SnapshotID, table.BranchRef, 0, 0, 0)); err != nil {
				return fmt.Errorf("start rewriting tag %s: %w", name, err)
			}
		}
	}
	for n, conflicts := 1, 0; ; {
		tbl, err := s.reload(ctx, t)
		if err != nil {
			return err
		}
		head, ok := refs(tbl)[branch]
		if !ok {
			return fmt.Errorf("branch %s of %s was dropped while the request rewrote it", branch, t.Name)
		}
		view, err := s.at(tbl, head.SnapshotID)
		if err != nil {
			return err
		}
		changed, err := pass(ctx, view, branch, true)
		if errors.Is(err, table.ErrCommitFailed) && conflicts < commitAttempts {
			conflicts++
			continue
		}
		if err != nil {
			return err
		}
		if !changed {
			break
		}
		if n++; n > maxPasses {
			return fmt.Errorf("matching rows keep arriving in %s, on %s: stop the writes that add them, and the request goes on", t.Name, name)
		}
	}
	if branch == name {
		return nil
	}
	// The tag now points at the rewritten snapshot, with its retention.
	tbl, err = s.reload(ctx, t)
	if err != nil {
		return err
	}
	head := refs(tbl)[branch].SnapshotID
	tag := refs(tbl)[name]
	var maxAge int64
	if tag.MaxRefAgeMs != nil {
		maxAge = *tag.MaxRefAgeMs
	}
	if err := s.commitRefs(ctx, tbl,
		[]table.Requirement{table.AssertRefSnapshotID(name, &tag.SnapshotID), table.AssertRefSnapshotID(branch, &head)},
		table.NewSetSnapshotRefUpdate(name, head, table.TagRef, maxAge, 0, 0), table.NewRemoveSnapshotRefUpdate(branch)); err != nil {
		return fmt.Errorf("move tag %s to its rewritten snapshot: %w", name, err)
	}
	if !slices.Contains(t.Refs, name) {
		t.Refs = append(t.Refs, name)
	}
	return nil
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
	deletes := map[string]iceberg.DataFile{}
	for p, df := range head.live(true) {
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
		return true, s.rewriteFiles(ctx, r, t, view, branch, flds, selected, dangling)
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

// rewriteFiles writes the tasks' data files again with their deletes
// applied and without matching rows, a partition at a time and at most
// maxRewriteBytes per commit, and removes the dead equality deletes and
// those that apply to no file.
func (s *Service) rewriteFiles(ctx context.Context, r *Request, t *Table, view *table.Table, branch string, flds []field,
	tasks []table.FileScanTask, dangling []iceberg.DataFile) error {
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
		)
		for len(keys) > 0 && (len(groups) == 0 || total+byPartition[keys[0]].TotalSizeBytes <= maxRewriteBytes) {
			g := byPartition[keys[0]]
			groups, total, keys = append(groups, g), total+g.TotalSizeBytes, keys[1:]
			for _, task := range g.Tasks {
				paths[task.File.FilePath()] = struct{}{}
			}
		}
		tx := view.NewTransactionOnBranch(branch)
		rw := tx.NewRewrite(operation)
		var stats filterStats
		for _, g := range groups {
			added, err := s.rewriteGroup(ctx, view, g, filter, &stats)
			if err != nil {
				return err
			}
			var old []iceberg.DataFile
			for _, task := range g.Tasks {
				old = append(old, task.File)
			}
			rw.Apply(old, added, table.CollectSafePositionDeletes(g.Tasks))
			for _, df := range added {
				r.written[df.FilePath()] = true
			}
		}
		dead, err := compaction.CollectDeadEqualityDeletes(ctx, fsys, view.CurrentSnapshot(), paths)
		if err != nil {
			return fmt.Errorf("find dead equality deletes: %w", err)
		}
		removed := map[string]bool{}
		for _, d := range append(dead, dangling...) {
			if !removed[d.FilePath()] {
				removed[d.FilePath()] = true
				rw.DeleteFile(d)
			}
		}
		dangling = nil
		if err := rw.Commit(ctx); err != nil {
			return err
		}
		committed, err := tx.Commit(ctx)
		if err != nil {
			return err
		}
		t.FilesRewritten += len(paths)
		t.RowsDeleted += stats.dropped
		t.ValuesNulled += stats.nulled
		if branch != table.MainBranch && !strings.HasPrefix(branch, tagBranch) && !slices.Contains(t.Refs, branch) {
			t.Refs = append(t.Refs, branch)
		}
		s.log.Info("rewrote files holding erased data", "request", r.ID, "table", t.Name, "branch", branch,
			"files", len(paths), "rows", stats.dropped, "nulled", stats.nulled)
		head := refs(committed)[branch]
		if view, err = s.at(committed, head.SnapshotID); err != nil {
			return err
		}
	}
	return nil
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
// deleted, with their column bounds, committing to branch.
func (s *Service) forget(ctx context.Context, view *table.Table, branch string, bad map[string]bool) error {
	tf, err := readCurrent(ctx, view)
	if err != nil {
		return err
	}
	mentioned := map[string]struct{}{}
	for _, m := range tf.manifests[tf.current()] {
		for _, e := range tf.entries[m] {
			if !bad[e.file.FilePath()] {
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
	_, err = tx.Commit(ctx)
	return err
}

// rowFilter leaves the rows that match out of records, or sets their
// columns to be nulled to NULL, comparing values as DuckDB's scans do:
// their text, as CAST(... AS VARCHAR) or, for bytes, hex() writes it,
// lower-cased and trimmed.
type rowFilter struct {
	salt string
	cols []filterColumn
}

type filterColumn struct {
	name   string
	hashes map[string]bool
	null   bool
}

type filterStats struct{ dropped, nulled int64 }

func newRowFilter(r *Request, flds []field) *rowFilter {
	f := &rowFilter{salt: r.salt}
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
			if c.hashes[salted(f.salt, goNormal(text))] {
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
