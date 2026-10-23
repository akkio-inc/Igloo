package maintenance

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/google/uuid"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/filerewrite"
)

// Clustering. A table with a clustering key (ALTER TABLE ... SET SORTED BY,
// or Snowflake's CLUSTER BY; see catalog.ClusteringOrder) is clustered
// instead of compacted: its files whose ranges of the key's first column
// overlap are written again, sorted, so that a filter on that column reads
// few files and Iceberg's manifests skip the rest. On a table of 1.4
// billion rows written unsorted, a filter on one value read 140 files;
// clustered, it read 3.
//
// Which files are rewritten, from the manifests' bounds of the first column:
//
//   - Files whose ranges overlap another file's. Ranges that only touch, as
//     those of the files of one sorted rewrite do, do not count, so a
//     clustered table is not rewritten again.
//   - Runs of small files (under 3/4 of the target size) next to each other
//     in the order, which merging keeps apart from the other files.
//   - Files with as many delete files as compaction rewrites them for.
//
// They are rewritten in islands, runs of such files with no other file
// between them in the order, each into its own sorted files, so the new
// files overlap none of the files left as they were. A pass rewrites up to
// maxCompactBytes, island by island; larger backlogs take several passes.
// An island whose rewrite would cost much more than what overlaps in it,
// such as the whole table around an insert whose keys are spread over it,
// waits: its fresh files are sorted into runs of their own once enough
// have gathered, and the runs merged once they stack too deep (see
// tieredGroups and depth.go).
// Each island is sorted within Options.Memory, range by range, spilling to
// Options.SpillDir (see filerewrite.Sorted), and committed as compaction
// commits: deletes applied, position and equality deletes that no file needs
// any more removed, without retrying on a conflict. A table whose clustering
// does not fit in Options.Memory is compacted unsorted instead (see
// compactOrCluster). Partitioned tables, and orders with transforms (bucket,
// day, ...) or nested columns, are compacted as before.

// clusterRetry is how long maintenance compacts a table whose clustering
// needed more memory than it may use unsorted, when the table has not
// changed, before it tries to cluster the table again.
const clusterRetry = 6 * time.Hour

// clusterFailure is when clustering a table last needed more memory than
// Options.Memory, and the table's snapshot as that pass left it.
type clusterFailure struct {
	snapshot int64
	at       time.Time
}

// compactOrCluster rewrites tbl's files: it clusters a table with a sort
// order it can cluster by (see cluster), and compacts the others. A table
// whose clustering needs more memory than maintenance may use is compacted
// unsorted instead, as tables were before clustering, so that small files
// and deletes do not pile up in it; clustering is tried again once the table
// has changed since, or clusterRetry later.
func (m *Maintainer) compactOrCluster(ctx context.Context, tbl *table.Table, log *slog.Logger) (*table.Table, error) {
	fields, first, ok := clusterFields(tbl)
	if !ok {
		return m.compact(ctx, tbl, log)
	}
	key := name(tbl.Identifier())
	if f, failed := m.clusterFailed[key]; failed {
		if snapshotID(tbl) == f.snapshot && time.Since(f.at) < clusterRetry {
			log.Debug("compacting the table unsorted: clustering it needed more memory than maintenance may use, and it has not changed since")
			return m.compact(ctx, tbl, log)
		}
		delete(m.clusterFailed, key)
	}
	committed, err := m.cluster(ctx, tbl, fields, first, log)
	if !errors.Is(err, errOverBudget) {
		return committed, err
	}
	log.Warn("clustering the table needs more memory than maintenance may use; compacting it unsorted this time",
		"err", err, "memory_limit", m.memory(), "retry", "when the table changes, or in "+clusterRetry.String())
	m.opts.Metrics.clusteringFellBack()
	committed, err = m.compact(ctx, tbl, log)
	left := tbl
	if err == nil && committed != nil {
		left = committed
	}
	// The table as this pass left it: its own compaction is not a change.
	m.clusterFailed[key] = clusterFailure{snapshot: snapshotID(left), at: time.Now()}
	return committed, err
}

// snapshotID is the ID of tbl's current snapshot, or 0 if it has none.
func snapshotID(tbl *table.Table) int64 {
	if s := tbl.CurrentSnapshot(); s != nil {
		return s.SnapshotID
	}
	return 0
}

// clusterFields returns the order maintenance clusters tbl by, or false:
// the table's clustering key (see catalog.ClusteringOrder), when the table
// is not partitioned and every field of the key is a top-level column of a
// primitive type, as it is.
func clusterFields(tbl *table.Table) ([]filerewrite.SortField, int, bool) {
	meta := tbl.Metadata()
	order := catalog.ClusteringOrder(meta)
	if len(order) == 0 || !meta.PartitionSpec().IsUnpartitioned() {
		return nil, 0, false
	}
	schema := tbl.Schema()
	var fields []filerewrite.SortField
	firstID := 0
	for i, f := range order {
		if _, ok := f.Transform.(iceberg.IdentityTransform); !ok || len(f.SourceIDs) > 1 {
			return nil, 0, false
		}
		field, ok := schema.FindFieldByID(f.SourceID())
		if !ok {
			return nil, 0, false
		}
		if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
			return nil, 0, false
		}
		if top, ok := schema.FindFieldByName(field.Name); !ok || top.ID != field.ID {
			return nil, 0, false
		}
		if i == 0 {
			firstID = field.ID
		}
		fields = append(fields, filerewrite.SortField{Name: field.Name, Descending: f.Direction == table.SortDESC,
			NullsFirst: f.NullOrder == table.NullsFirst})
	}
	return fields, firstID, len(fields) > 0
}

// fileSpan is a data file's range of the sort order's first column.
type fileSpan struct {
	task   table.FileScanTask
	lo, hi iceberg.Literal
	// bounded says the manifest gave both bounds; allNull that the file
	// holds only NULLs of the column, which have no bounds.
	bounded, allNull bool
}

// spans reads the first column's range of each of tasks' files.
func spans(tasks []table.FileScanTask, schema *iceberg.Schema, id int) []fileSpan {
	field, _ := schema.FindFieldByID(id)
	out := make([]fileSpan, len(tasks))
	for i, t := range tasks {
		out[i] = fileSpan{task: t}
		if n, ok := t.File.NullValueCounts()[id]; ok && n == t.File.Count() {
			out[i].allNull = true
			continue
		}
		lob, okl := t.File.LowerBoundValues()[id]
		hib, okh := t.File.UpperBoundValues()[id]
		if !okl || !okh {
			continue
		}
		lo, errl := iceberg.LiteralFromBytes(field.Type, lob)
		hi, errh := iceberg.LiteralFromBytes(field.Type, hib)
		if errl != nil || errh != nil {
			continue
		}
		if _, ok := compareLiterals(lo, hi); ok {
			out[i].lo, out[i].hi, out[i].bounded = lo, hi, true
		}
	}
	// Files without bounds first, then by range.
	slices.SortStableFunc(out, func(a, b fileSpan) int {
		switch {
		case !a.bounded && !b.bounded:
			return 0
		case !a.bounded:
			return -1
		case !b.bounded:
			return 1
		}
		if c, _ := compareLiterals(a.lo, b.lo); c != 0 {
			return c
		}
		c, _ := compareLiterals(a.hi, b.hi)
		return c
	})
	return out
}

// overlapping marks the files of s (sorted by spans) whose ranges overlap
// another's: share more than a bound. A file without bounds may overlap any;
// one of NULLs only overlaps none, as its rows all sort together.
func overlapping(s []fileSpan) []bool {
	out := make([]bool, len(s))
	widest := -1 // the file reaching furthest so far
	for i, f := range s {
		if !f.bounded {
			out[i] = !f.allNull && len(s) > 1
			continue
		}
		if widest >= 0 {
			if c, _ := compareLiterals(f.lo, s[widest].hi); c < 0 {
				out[i], out[widest] = true, true
			}
		}
		if widest < 0 {
			widest = i
		} else if c, _ := compareLiterals(f.hi, s[widest].hi); c > 0 {
			widest = i
		}
	}
	return out
}

// countTrue counts the true values of bs.
func countTrue(bs []bool) int {
	n := 0
	for _, b := range bs {
		if b {
			n++
		}
	}
	return n
}

// What clustering waits for (see tieredGroups); variables so that tests
// can set them.
var (
	// clusterMaxDepth is the depth of the files clustering wrote past which
	// passes merge them island by island, whatever their tiers.
	clusterMaxDepth = 16
	// tierFanIn is how many runs of a level are sorted into one of the next
	// (see tieredGroups).
	tierFanIn = 4
	// cheapFactor and cheapTargets: an island is rewritten at once when that
	// rewrites at most cheapFactor times the bytes in it that overlap, or
	// cheapTargets files of the target size.
	cheapFactor  int64 = 4
	cheapTargets int64 = 2
	// Fresh files that wait are sorted together once they take freshShare
	// of the table or freshMaxBytes, whichever is less, number
	// freshMaxFiles, or stack deeper than freshMaxDepth.
	freshShare          = 0.05
	freshMaxBytes int64 = 4 << 30
	freshMaxFiles       = 100
	// freshMaxDepth: fresh files are sorted too once they stack deeper than
	// this.
	freshMaxDepth = 8
)

// sortedBy reports whether clustering wrote f sorted by fields.
func sortedBy(f iceberg.DataFile, fields []filerewrite.SortField) bool {
	return filerewrite.WrittenSorted(f.FilePath(), fields)
}

// islandGroups returns the islands of s's picked files, runs of them with no
// other file between them in the order, that take accepts (all, if take is
// nil), in the order, up to limit bytes in all. An island gets two files
// however large, so that each pass merges something; one file with too few
// deletes to rewrite for is left out, as it is already as sorted as
// rewriting it alone would make it.
func islandGroups(s []fileSpan, pick []bool, limit int64, take func(a, b int) bool) []table.CompactionTaskGroup {
	var (
		groups []table.CompactionTaskGroup
		total  int64
	)
	for i, full := 0, false; i < len(s) && !full; {
		if !pick[i] {
			i++
			continue
		}
		end := i
		for end < len(s) && pick[end] {
			end++
		}
		if take != nil && !take(i, end) {
			i = end
			continue
		}
		var g table.CompactionTaskGroup
		for ; i < end; i++ {
			if (len(groups) > 0 && len(g.Tasks) == 0 || len(g.Tasks) >= 2) && total+g.TotalSizeBytes+s[i].task.File.FileSizeBytes() > limit {
				full = true
				break
			}
			g.Tasks = append(g.Tasks, s[i].task)
			g.TotalSizeBytes += s[i].task.File.FileSizeBytes()
		}
		i = end
		if len(g.Tasks) == 0 {
			break
		}
		if len(g.Tasks) > 1 || hasDeletes(g.Tasks[0]) {
			groups = append(groups, g)
			total += g.TotalSizeBytes
		}
	}
	return groups
}

// hasDeletes reports whether a file has deletes to apply.
func hasDeletes(t table.FileScanTask) bool {
	return len(t.DeleteFiles)+len(t.EqualityDeleteFiles) > 0
}

// fileRun is the run a file of clustering's belongs to, if any: its level
// and name (see filerewrite.RunOf).
type fileRun struct {
	level int
	name  string
	ok    bool
}

// runGroup is a group a pass rewrites, and the run directory its files go
// to ("" for none: the clustered files themselves).
type runGroup struct {
	group table.CompactionTaskGroup
	run   string
}

// tieredGroups returns the groups a pass rewrites while the files
// clustering wrote stack no deeper than clusterMaxDepth.
//
// An island of overlapping files is rewritten when that is cheap next to
// the bytes in it that overlap (its bytes less those of the largest set of
// its files that overlap none of each other), into clustered files: a table
// written unsorted, an insert of one audience that falls within a file or
// two, or runs that have grown to a quarter of what they overlap. So are
// islands picked for deletes or small neighbors.
//
// In the other islands, such as one an insert whose keys are spread over
// the whole table makes of it, rewriting the island would rewrite every
// file in it. Fresh files (not written by clustering) wait instead, though
// every filter on the key reads each of them: once they stack deeper than
// freshMaxDepth, take freshShare of the table, or number freshMaxFiles,
// they are sorted together, alone, into a run of level 0. Runs are tiered,
// as log-structured stores tier theirs: tierFanIn runs of one level are
// sorted together into a run of the next. Each run deepens the table by
// one, so it stacks at most tierFanIn-1 runs of each level, whose sizes
// grow by tierFanIn a level, and the bytes of a row are rewritten once a
// level until its run is big enough for its island to be cheap.
func tieredGroups(s []fileSpan, pick, over, sorted []bool, runs []fileRun, target, limit int64) []runGroup {
	var tableBytes int64
	for _, f := range s {
		tableBytes += f.task.File.FileSizeBytes()
	}
	var fresh []int
	var freshBytes int64
	// The files of each run that waits, by level.
	waiting := map[int]map[string][]int{}
	var groups []runGroup
	for _, g := range islandGroups(s, pick, limit, func(a, b int) bool {
		var bytes int64
		overlaps := false
		for i := a; i < b; i++ {
			bytes += s[i].task.File.FileSizeBytes()
			overlaps = overlaps || over[i]
		}
		if !overlaps || bytes <= max(cheapFactor*(bytes-apartBytes(s[a:b])), cheapTargets*target) {
			return true
		}
		for i := a; i < b; i++ {
			switch {
			case !over[i]:
			case !sorted[i]:
				fresh = append(fresh, i)
				freshBytes += s[i].task.File.FileSizeBytes()
			case runs[i].ok:
				if waiting[runs[i].level] == nil {
					waiting[runs[i].level] = map[string][]int{}
				}
				waiting[runs[i].level][runs[i].name] = append(waiting[runs[i].level][runs[i].name], i)
			}
		}
		return false
	}) {
		groups = append(groups, runGroup{group: g})
	}
	var total int64
	for _, g := range groups {
		total += g.group.TotalSizeBytes
	}
	// add puts files into one group, up to what the pass may still rewrite,
	// for a run of level level; one file written sorted is left alone.
	add := func(files []int, level int) {
		var g table.CompactionTaskGroup
		unsorted := false
		for _, i := range files {
			size := s[i].task.File.FileSizeBytes()
			if (len(groups) > 0 || len(g.Tasks) >= 2) && total+g.TotalSizeBytes+size > limit {
				break
			}
			g.Tasks = append(g.Tasks, s[i].task)
			g.TotalSizeBytes += size
			unsorted = unsorted || !sorted[i]
		}
		if len(g.Tasks) >= 2 || len(g.Tasks) == 1 && (unsorted || hasDeletes(g.Tasks[0])) {
			groups = append(groups, runGroup{group: g, run: filerewrite.RunDir(level)})
			total += g.TotalSizeBytes
		}
	}
	enough := min(int64(freshShare*float64(tableBytes)), freshMaxBytes)
	if len(fresh) > 0 && (freshBytes >= enough || len(fresh) >= freshMaxFiles || freshDepth(s, fresh) > freshMaxDepth) {
		add(fresh, 0)
	}
	levels := slices.Sorted(maps.Keys(waiting))
	for _, level := range levels {
		if len(waiting[level]) < tierFanIn {
			continue
		}
		var files []int
		for _, name := range slices.Sorted(maps.Keys(waiting[level])) {
			files = append(files, waiting[level][name]...)
		}
		slices.Sort(files)
		add(files, level+1)
	}
	return groups
}

// freshDepth returns how deep the files of s that fresh lists stack among
// themselves.
func freshDepth(s []fileSpan, fresh []int) int {
	in := make([]bool, len(s))
	for _, i := range fresh {
		in[i] = true
	}
	_, top := depthOf(s, in)
	return top
}

// apartBytes returns the bytes of the largest set of files of s (sorted by
// spans) whose ranges overlap none of each other's (they may touch). Files
// without bounds are left out.
func apartBytes(s []fileSpan) int64 {
	var idx []int
	for i, f := range s {
		if f.bounded {
			idx = append(idx, i)
		}
	}
	cmpLit := func(a, b iceberg.Literal) int { c, _ := compareLiterals(a, b); return c }
	slices.SortStableFunc(idx, func(a, b int) int { return cmpLit(s[a].hi, s[b].hi) })
	// best[k] is the most bytes of files apart among the first k by end.
	best := make([]int64, len(idx)+1)
	for k, i := range idx {
		// The files that end at or before this one starts.
		j := sort.Search(k, func(j int) bool { return cmpLit(s[idx[j]].hi, s[i].lo) > 0 })
		best[k+1] = max(best[k], best[j]+s[i].task.File.FileSizeBytes())
	}
	return best[len(idx)]
}

// cluster rewrites tbl's files as the comment at the top says, ordered by
// fields, whose first column has field id first. It returns the table as
// committed.
func (m *Maintainer) cluster(ctx context.Context, tbl *table.Table, fields []filerewrite.SortField, first int, log *slog.Logger) (*table.Table, error) {
	if tbl.CurrentSnapshot() == nil {
		return tbl, nil
	}
	cfg := compaction.DefaultConfig()
	if target, err := strconv.ParseInt(tbl.Properties()[table.WriteTargetFileSizeBytesKey], 10, 64); err == nil && target > 0 {
		cfg.TargetFileSizeBytes = target
		cfg.MinFileSizeBytes = target * 3 / 4
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil || len(tasks) < 1 {
		return tbl, err
	}
	s := spans(tasks, tbl.Schema(), first)
	over := overlapping(s)
	pick := slices.Clone(over)
	for i, f := range s {
		if len(f.task.DeleteFiles)+len(f.task.EqualityDeleteFiles) >= cfg.DeleteFileThreshold {
			pick[i] = true
		}
	}
	// Runs of small files next to each other in the order.
	small := func(i int) bool {
		return s[i].bounded && !over[i] && s[i].task.File.FileSizeBytes() < cfg.MinFileSizeBytes
	}
	for i := 0; i < len(s); {
		j := i
		for j < len(s) && small(j) {
			j++
		}
		if j-i >= 2 {
			for k := i; k < j; k++ {
				pick[k] = true
			}
		}
		i = max(j, i+1)
	}

	// Files clustering wrote, and the runs among them (see tieredGroups).
	everyFile, sorted := make([]bool, len(s)), make([]bool, len(s))
	runs := make([]fileRun, len(s))
	for i, f := range s {
		everyFile[i] = true
		sorted[i] = sortedBy(f.task.File, fields)
		runs[i].level, runs[i].name, runs[i].ok = filerewrite.RunOf(f.task.File.FilePath(), fields)
	}
	depth, maxDepth := depthOf(s, everyFile)
	_, sortedDepth := depthOf(s, sorted)
	m.opts.Metrics.clusteringDepth(name(tbl.Identifier()), depth, maxDepth)

	limit := m.rewriteLimit()
	var groups []runGroup
	if sortedDepth > clusterMaxDepth {
		// The files clustering wrote stack too deep, as tiers should keep
		// them from: merge them, island by island.
		for _, g := range islandGroups(s, pick, limit, nil) {
			groups = append(groups, runGroup{group: g})
		}
	} else {
		groups = tieredGroups(s, pick, over, sorted, runs, cfg.TargetFileSizeBytes, limit)
	}
	if len(groups) == 0 {
		return tbl, nil
	}
	// Where each group's files go, by its first file.
	runDir := map[string]string{}
	plain := make([]table.CompactionTaskGroup, len(groups))
	for i, g := range groups {
		plain[i] = g.group
		runDir[g.group.Tasks[0].File.FilePath()] = g.run
	}
	var total int64
	rewrite := map[string]struct{}{}
	for _, g := range plain {
		total += g.TotalSizeBytes
		for _, t := range g.Tasks {
			rewrite[t.File.FilePath()] = struct{}{}
		}
	}

	before := countTrue(over)
	log.Info("clustering", "files", len(rewrite), "bytes", total, "groups", len(groups), "overlapping_files", before,
		"depth", fmt.Sprintf("%.2f", depth), "max_depth", maxDepth, "sorted_by", fields[0].Name, "memory_limit", m.memory())
	start := time.Now()
	write := m.sortedWriter(fields, runDir)
	if m.sorted != nil {
		write = m.sorted(fields)
	}
	committed, res, err := m.rewriteGroups(ctx, tbl, plain, rewrite, "cluster", write)
	if err != nil || res.removed == 0 {
		return committed, err
	}
	after := -1
	if tasks, err := committed.Scan().PlanFiles(ctx); err == nil {
		after = countTrue(overlapping(spans(tasks, committed.Schema(), first)))
	}
	m.opts.Metrics.clustered(res.removed, res.added)
	log.Info("clustered",
		"files_before", res.removed, "files_after", res.added, "delete_files_removed", res.deletes,
		"overlapping_files_before", before, "overlapping_files_after", after,
		"bytes_before", res.bytesBefore, "bytes_after", res.bytesAfter,
		"took", time.Since(start).Round(time.Millisecond), "peak_memory", res.peak)
	return committed, nil
}

// spillPrefix starts the names of clustering's spill directories.
const spillPrefix = "igloo-cluster-"

// rewriteLimit is how many bytes of files a pass rewrites at most.
func (m *Maintainer) rewriteLimit() int64 {
	if m.maxRewrite > 0 {
		return m.maxRewrite
	}
	return maxCompactBytes
}

// sortedWriter writes a group's files sorted by fields, into the run
// directory runDir gives for its first file if any, spilling under
// Options.SpillDir. Spill directories a day old, which a clustering that
// Igloo died in left behind, are removed first.
func (m *Maintainer) sortedWriter(fields []filerewrite.SortField, runDir map[string]string) groupWriter {
	return func(ctx context.Context, tbl *table.Table, fs iceio.IO, g table.CompactionTaskGroup, budget int64) ([]iceberg.DataFile, filerewrite.Layout, error) {
		base := cmp.Or(m.opts.SpillDir, os.TempDir())
		if old, err := filepath.Glob(filepath.Join(base, spillPrefix+"*")); err == nil {
			for _, d := range old {
				if st, err := os.Stat(d); err == nil && time.Since(st.ModTime()) > 24*time.Hour {
					_ = os.RemoveAll(d)
				}
			}
		}
		dir, err := os.MkdirTemp(base, spillPrefix)
		if err != nil {
			return nil, filerewrite.Layout{}, fmt.Errorf("spill directory: %w", err)
		}
		defer os.RemoveAll(dir) //nolint:errcheck
		return filerewrite.Sorted(ctx, tbl, fs, g.Tasks, budget, fields, dir, runDir[g.Tasks[0].File.FilePath()])
	}
}

// compareLiterals compares two bounds of one column; ok is false for
// values of different or unknown types.
func compareLiterals(a, b iceberg.Literal) (int, bool) {
	switch av := a.(type) {
	case iceberg.TypedLiteral[bool]:
		return compareTyped(av, b)
	case iceberg.TypedLiteral[int32]:
		return compareTyped(av, b)
	case iceberg.TypedLiteral[int64]:
		return compareTyped(av, b)
	case iceberg.TypedLiteral[float32]:
		return compareTyped(av, b)
	case iceberg.TypedLiteral[float64]:
		return compareTyped(av, b)
	case iceberg.TypedLiteral[iceberg.Date]:
		return compareTyped(av, b)
	case iceberg.TypedLiteral[iceberg.Time]:
		return compareTyped(av, b)
	case iceberg.TypedLiteral[iceberg.Timestamp]:
		return compareTyped(av, b)
	case iceberg.TypedLiteral[iceberg.TimestampNano]:
		return compareTyped(av, b)
	case iceberg.TypedLiteral[string]:
		return compareTyped(av, b)
	case iceberg.TypedLiteral[[]byte]:
		return compareTyped(av, b)
	case iceberg.TypedLiteral[uuid.UUID]:
		return compareTyped(av, b)
	case iceberg.TypedLiteral[iceberg.Decimal]:
		return compareTyped(av, b)
	}
	return 0, false
}

func compareTyped[T iceberg.LiteralType](a iceberg.TypedLiteral[T], b iceberg.Literal) (int, bool) {
	bt, ok := b.(iceberg.TypedLiteral[T])
	if !ok {
		return 0, false
	}
	return a.Comparator()(a.Value(), bt.Value()), true
}
