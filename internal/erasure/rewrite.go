package erasure

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
	"github.com/akkio-inc/igloo/internal/filerewrite"
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

// ownSchema reports whether branch is rewritten with its head's own schema
// (see at): every branch but main is.
func ownSchema(branch string) bool { return branch != table.MainBranch }

// ownBranch reports whether ref name is one of r's branches.
func ownBranch(r *Request, name string) bool {
	own := tagBranch + strings.ToLower(r.ID)
	return name == own || strings.HasPrefix(name, own+"-")
}

// at is tbl as a table whose current snapshot is snapshot id: what scans
// and commits work from. iceberg-go builds a commit to any branch on the
// current snapshot, so a commit to another branch is made on this.
//
// With own, its current schema is the snapshot's own, as a tag's snapshot
// is read, and as Igloo (AT (BRANCH => ...) reads the branch's head) and
// PyIceberg read a branch's, so that a rewrite of any ref but main reads
// and writes every column its snapshot has, and the snapshot that replaces
// it keeps that schema, as iceberg-go gives a new snapshot the current
// one. With the table's current schema, a column dropped since the
// snapshot was made went from every row of the ref, the person's or not.
// Main is read with the table's current schema, as every engine reads it.
//
// The view's partition spec stays the table's default; a rewrite writes
// each file with a spec its schema has every column of (see writeSpec).
func (s *Service) at(tbl *table.Table, id int64, own bool) (*table.Table, error) {
	current := tbl.Metadata().CurrentSchema().ID
	schema := current
	if snap := tbl.SnapshotByID(id); own && snap != nil && snap.SchemaID != nil {
		schema = *snap.SchemaID
	}
	if cur := tbl.CurrentSnapshot(); cur != nil && cur.SnapshotID == id && schema == current {
		return tbl, nil
	}
	b, err := table.MetadataBuilderFromBase(tbl.Metadata(), tbl.MetadataLocation())
	if err != nil {
		return nil, err
	}
	if err := b.SetSnapshotRef(table.MainBranch, id, table.BranchRef); err != nil {
		return nil, err
	}
	if schema != current {
		if err := b.SetCurrentSchemaID(schema); err != nil {
			return nil, err
		}
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
			view, err := s.at(tbl, original, true)
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
		view, err := s.at(tbl, head.SnapshotID, ownSchema(branch))
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
	cols := scanColumns(flds, r.Subjects)
	matched, err := s.matchingFiles(ctx, r.salt, paths, cols)
	if err != nil {
		return false, err
	}
	// A position delete file can hold the rows it deletes, the person's
	// too: it goes once every file it deletes rows of is rewritten, so
	// those are rewritten with it, or at once if none of them is left.
	rowsHeld, err := s.matchingDeleteRows(ctx, r.salt, positionDeletes(live), cols)
	if err != nil {
		return false, err
	}
	fsys, err := view.FS(ctx)
	if err != nil {
		return false, err
	}
	for p, n := range rowsHeld {
		matched[p] += n
	}
	targeted, gone, err := s.rowCopies(ctx, fsys, slices.Sorted(maps.Keys(rowsHeld)), live, tasks)
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
	var selected []table.FileScanTask
	dangling := gone
	for _, task := range tasks {
		if matched[task.File.FilePath()] > 0 || targeted.Has(task.File.FilePath()) || slices.ContainsFunc(task.EqualityDeleteFiles, func(d iceberg.DataFile) bool {
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

// rowCopies sorts the position delete files held, which hold matching
// rows: targeted are the data files they delete rows of, which are
// rewritten so that they can go with them (see rewriteFiles), and gone are
// those that delete rows of no live data file, as when another engine
// rewrote those files and left the deletes, as copy-on-write does, or that
// apply to no file the scan planned: they delete nothing, and go at once.
// Paths compare however spelled (see deletefiles.Paths), which can only
// take a target for live, and keep its delete file until it is rewritten.
func (s *Service) rowCopies(ctx context.Context, fsys iceio.IO, held []string, live map[string]iceberg.DataFile,
	tasks []table.FileScanTask) (deletefiles.Paths, []iceberg.DataFile, error) {
	data := deletefiles.Paths{}
	for p, df := range live {
		if df.ContentType() == iceberg.EntryContentData {
			data.Add(p)
		}
	}
	attached := map[string]bool{}
	for _, task := range tasks {
		for _, d := range task.DeleteFiles {
			attached[d.FilePath()] = true
		}
	}
	targeted := deletefiles.Paths{}
	var gone []iceberg.DataFile
	for _, p := range held {
		targets, err := s.targets.Of(ctx, fsys, live[p])
		if err != nil {
			return nil, nil, err
		}
		applies := false
		for t := range targets {
			targeted.Add(t)
			applies = applies || data.Has(t)
		}
		if !applies || !attached[p] {
			gone = append(gone, live[p])
		}
	}
	return targeted, gone, nil
}

// removable are the position delete files of tasks that a commit which
// replaces the tasks' data files may remove (see
// deletefiles.Targets.Removable): those that delete rows only of those
// files, spelled as their manifests and the delete files spell them, or of
// files that are not live, however spelled. live are the files live in the
// snapshot the commit replaces them in.
func (s *Service) removable(ctx context.Context, fsys iceio.IO, tasks []table.FileScanTask, live map[string]iceberg.DataFile) ([]iceberg.DataFile, error) {
	current := deletefiles.Paths{}
	for p := range live {
		current.Add(p)
	}
	return s.targets.Removable(ctx, fsys, tasks, current)
}

// batch is one commit of a rewrite: the data files it replaces and those
// that replace them, written with partition spec spec, and the delete
// files it removes.
type batch struct {
	old, added, removed []iceberg.DataFile
	spec                int
	stats               filterStats
}

// rewriteFiles writes the tasks' data files again with their deletes
// applied and without matching rows, a partition at a time and at most
// maxRewriteBytes per commit, each with a partition spec view's schema has
// the columns of (see writeSpec), and one spec per commit: iceberg-go
// lists the files a commit adds in manifests of the spec the table writes
// with (see withSpec). Each commit removes the equality deletes that apply
// to no file left, those that apply to no file at all, and the position
// deletes that delete rows only of files it replaces or of files no longer
// live (see removable); live are the files live in view's current
// snapshot.
func (s *Service) rewriteFiles(ctx context.Context, r *Request, t *Table, view *table.Table, branch string, flds []field,
	tasks []table.FileScanTask, dangling []iceberg.DataFile, live map[string]iceberg.DataFile) error {
	byPartition := map[string]*table.CompactionTaskGroup{}
	specs := map[string]int{}
	for _, task := range tasks {
		key := partitionKey(task.File)
		g := byPartition[key]
		if g == nil {
			spec, err := writeSpec(view, int(task.File.SpecID()))
			if err != nil {
				return terminal(fmt.Errorf("rewrite %s of %s: %w", branch, t.Name, err))
			}
			g = &table.CompactionTaskGroup{PartitionKey: key}
			byPartition[key], specs[key] = g, spec
		}
		g.Tasks = append(g.Tasks, task)
		g.TotalSizeBytes += task.File.FileSizeBytes()
	}
	fsys, err := view.FS(ctx)
	if err != nil {
		return err
	}
	live = maps.Clone(live)
	filter := newRowFilter(r, flds)
	keys := slices.SortedFunc(maps.Keys(byPartition), func(a, b string) int {
		return cmp.Or(cmp.Compare(specs[a], specs[b]), strings.Compare(a, b))
	})
	for len(keys) > 0 || len(dangling) > 0 {
		var (
			groups []*table.CompactionTaskGroup
			total  int64
			paths  = map[string]struct{}{}
			tasks  []table.FileScanTask
			b      = batch{spec: view.Metadata().DefaultPartitionSpec()}
		)
		if len(keys) > 0 {
			b.spec = specs[keys[0]]
		}
		for len(keys) > 0 && specs[keys[0]] == b.spec && (len(groups) == 0 || total+byPartition[keys[0]].TotalSizeBytes <= maxRewriteBytes) {
			g := byPartition[keys[0]]
			groups, total, keys = append(groups, g), total+g.TotalSizeBytes, keys[1:]
			for _, task := range g.Tasks {
				paths[task.File.FilePath()] = struct{}{}
			}
			tasks = append(tasks, g.Tasks...)
		}
		writing, err := s.withSpec(view, b.spec)
		if err != nil {
			return err
		}
		for _, g := range groups {
			added, err := s.rewriteGroup(ctx, writing, fsys, g, filter, &b.stats)
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
		safe, err := s.removable(ctx, fsys, tasks, live)
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
		// What the commit replaced and removed is no longer live for the
		// next one, and what it added is.
		for _, df := range slices.Concat(b.old, b.removed) {
			delete(live, df.FilePath())
		}
		for _, df := range b.added {
			live[df.FilePath()] = df
		}
		head := refs(committed)[branch]
		if view, err = s.at(committed, head.SnapshotID, ownSchema(branch)); err != nil {
			return err
		}
	}
	return nil
}

// writeSpec is the partition spec a rewrite of view writes the files of
// spec id again with: their own, if view's schema has every column it
// partitions by, or else the table's default spec, or else any other of
// the table's specs, if the schema has their columns. A ref read with a
// schema older than the table's (see at) can lack a column the table has
// been partitioned by since, and iceberg-go cannot write files of a spec
// without its columns.
func writeSpec(view *table.Table, id int) (int, error) {
	meta, schema := view.Metadata(), view.Schema()
	candidates := []int{id, meta.DefaultPartitionSpec()}
	for _, spec := range meta.PartitionSpecs() {
		candidates = append(candidates, spec.ID())
	}
	for _, c := range candidates {
		if spec := meta.PartitionSpecByID(c); spec != nil && specFits(*spec, schema) {
			return c, nil
		}
	}
	return 0, fmt.Errorf("%w: its files of partition spec %d cannot be written again: no partition spec of the table "+
		"partitions by columns that the schema it is read with (%d) has", ErrInvalid, id, schema.ID)
}

// specFits reports whether schema has every column spec partitions by, of
// a type its transform applies to.
func specFits(spec iceberg.PartitionSpec, schema *iceberg.Schema) bool {
	if spec.IsUnpartitioned() {
		return true
	}
	for _, f := range spec.Fields() {
		for _, id := range f.SourceIDs {
			src, ok := schema.FindFieldByID(id)
			if !ok || !f.Transform.CanTransform(src.Type) {
				return false
			}
		}
	}
	return true
}

// withSpec is view writing new files with partition spec id: iceberg-go
// writes files, and the manifests that list those a commit adds, with the
// table's default spec. Only the commit's snapshot reaches the catalog,
// not the default spec.
func (s *Service) withSpec(view *table.Table, id int) (*table.Table, error) {
	if view.Metadata().DefaultPartitionSpec() == id {
		return view, nil
	}
	b, err := table.MetadataBuilderFromBase(view.Metadata(), view.MetadataLocation())
	if err != nil {
		return nil, err
	}
	if err := b.SetDefaultSpecID(id); err != nil {
		return nil, err
	}
	meta, err := b.Build()
	if err != nil {
		return nil, err
	}
	fsF := func(ctx context.Context) (iceio.IO, error) { return view.FS(ctx) }
	return table.New(view.Identifier(), meta, view.MetadataLocation(), fsF, s.opts.Catalog), nil
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
		writing, err := s.withSpec(view, b.spec)
		if err != nil {
			return nil, err
		}
		tx := writing.NewTransactionOnBranch(branch)
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
		if view, err = s.at(tbl, head.SnapshotID, ownSchema(branch)); err != nil {
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
// rewriteGroup writes g's files again without the rows filter takes out,
// reading one file at a time within Options.Memory, as compaction does (see
// package filerewrite). A group that needs more fails the request for good:
// trying again fails the same way until IGLOO_MAINTENANCE_MEMORY is raised.
func (s *Service) rewriteGroup(ctx context.Context, view *table.Table, fsys iceio.IO, g *table.CompactionTaskGroup, filter *rowFilter, stats *filterStats) ([]iceberg.DataFile, error) {
	budget := cmp.Or(s.opts.Memory, filerewrite.DefaultMemory)
	ctx, _, stop := filerewrite.Metered(ctx, budget)
	defer stop(nil)
	added, _, err := filerewrite.Rewrite(ctx, view, fsys, g.Tasks, budget, func(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
		out, err := filter.apply(ctx, rec, stats)
		rec.Release()
		if err != nil || out.NumRows() > 0 {
			return out, err
		}
		out.Release()
		return nil, nil
	})
	if errors.Is(err, filerewrite.ErrOverBudget) {
		return nil, terminal(fmt.Errorf("rewrite %d files: %w", len(g.Tasks), err))
	}
	if err != nil {
		return nil, err
	}
	return added, nil
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
