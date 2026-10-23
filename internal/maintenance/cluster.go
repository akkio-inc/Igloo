package maintenance

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/google/uuid"

	"github.com/akkio-inc/igloo/internal/filerewrite"
)

// Clustering. A table with a default sort order (ALTER TABLE ... SET SORTED
// BY, or Snowflake's CLUSTER BY) is clustered instead of compacted: its
// files whose ranges of the order's first column overlap are written again,
// sorted, so that a filter on that column reads few files and Iceberg's
// manifests skip the rest. On a table of 1.4 billion rows written unsorted,
// a filter on one value read 140 files; clustered, it read 3.
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
// Each island is sorted within Options.Memory, range by range, spilling to
// Options.SpillDir (see filerewrite.Sorted), and committed as compaction
// commits: deletes applied, position and equality deletes that no file needs
// any more removed, without retrying on a conflict. Partitioned tables, and
// orders with transforms (bucket, day, ...) or nested columns, are compacted
// as before.

// clusterFields returns the order maintenance clusters tbl by, or false:
// the table's default sort order, when the table is not partitioned and
// every field of the order is a top-level column of a primitive type, as it
// is.
func clusterFields(tbl *table.Table) ([]filerewrite.SortField, int, bool) {
	meta := tbl.Metadata()
	order := meta.SortOrder()
	if order.IsUnsorted() || !meta.PartitionSpec().IsUnpartitioned() {
		return nil, 0, false
	}
	schema := tbl.Schema()
	var fields []filerewrite.SortField
	firstID := 0
	for i, f := range order.Fields() {
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

// cluster rewrites tbl's files as the comment at the top says, ordered by
// fields, whose first column has field id first. It returns the table as
// committed.
func (m *Maintainer) cluster(ctx context.Context, tbl *table.Table, fields []filerewrite.SortField, first int, log *slog.Logger) (*table.Table, error) {
	if tbl.CurrentSnapshot() == nil {
		return tbl, nil
	}
	if id, ok := m.tooBig[name(tbl.Identifier())]; ok {
		if id == tbl.CurrentSnapshot().SnapshotID {
			log.Debug("not clustering the table: it needed more memory than maintenance may use, and has not changed since")
			return tbl, nil
		}
		delete(m.tooBig, name(tbl.Identifier()))
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

	// Islands of picked files, up to maxCompactBytes.
	var (
		groups  []table.CompactionTaskGroup
		total   int64
		rewrite = map[string]struct{}{}
	)
	for i := 0; i < len(s); {
		if !pick[i] {
			i++
			continue
		}
		var g table.CompactionTaskGroup
		for ; i < len(s) && pick[i]; i++ {
			if len(groups) > 0 && total+g.TotalSizeBytes+s[i].task.File.FileSizeBytes() > maxCompactBytes {
				break
			}
			g.Tasks = append(g.Tasks, s[i].task)
			g.TotalSizeBytes += s[i].task.File.FileSizeBytes()
		}
		if len(g.Tasks) == 0 {
			break
		}
		// One file with too few deletes to rewrite for is already as
		// sorted as rewriting it alone would make it.
		if len(g.Tasks) > 1 || len(g.Tasks[0].DeleteFiles)+len(g.Tasks[0].EqualityDeleteFiles) > 0 {
			groups = append(groups, g)
			total += g.TotalSizeBytes
			for _, t := range g.Tasks {
				rewrite[t.File.FilePath()] = struct{}{}
			}
		}
		if total >= maxCompactBytes {
			break
		}
	}
	if len(groups) == 0 {
		return tbl, nil
	}

	before := countTrue(over)
	log.Info("clustering", "files", len(rewrite), "bytes", total, "islands", len(groups), "overlapping_files", before,
		"sorted_by", fields[0].Name, "memory_limit", m.memory())
	start := time.Now()
	committed, res, err := m.rewriteGroups(ctx, tbl, groups, rewrite, "cluster", m.sortedWriter(fields))
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

// sortedWriter writes a group's files sorted by fields, spilling under
// Options.SpillDir. Spill directories a day old, which a clustering that
// Igloo died in left behind, are removed first.
func (m *Maintainer) sortedWriter(fields []filerewrite.SortField) groupWriter {
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
		return filerewrite.Sorted(ctx, tbl, fs, g.Tasks, budget, fields, dir)
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
