package maintenance

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/filerewrite"
)

// Row groups for inserts. Compaction and clustering size the row groups
// they write from the files' bytes a row (see filerewrite.RowGroupRows),
// but most of a table's rows are written by DuckDB's inserts, in groups of
// DuckDB's 122,880 rows unless the table's write.parquet.row-group-size
// says otherwise: row groups of a few hundred kilobytes for narrow rows,
// which a cold scan reads with a request each. So maintenance sets that
// property on the tables Igloo creates (rowGroupsProperty, which the
// catalog proxy sets), from the table's bytes a row, as for a rewrite.
// DuckDB holds a row group, uncompressed, on every thread while it writes:
// the rows are at most Options.Memory over Options.Threads rows as wide as
// the schema's types make them (filerewrite.RowWidth). Maintenance records
// the size it set in rowGroupsProperty, after "auto:", and leaves alone a
// size it did not set, a user's. The proxy records its first size there too
// (see catalogproxy's firstRowGroups), which maintenance then corrects.
const (
	rowGroupsProperty = "igloo.row-groups"
	duckDBRowGroups   = "write.parquet.row-group-size"
)

// sizeRowGroups sets the row group size DuckDB's inserts into tbl write,
// if Igloo created tbl and nobody else set it, and it changed.
func (m *Maintainer) sizeRowGroups(ctx context.Context, tbl *table.Table, log *slog.Logger) error {
	props := tbl.Properties()
	marker, ok := props[rowGroupsProperty]
	if !ok || !strings.HasPrefix(marker, "auto") {
		return nil
	}
	set := strings.TrimPrefix(strings.TrimPrefix(marker, "auto"), ":")
	current := props[duckDBRowGroups]
	if current != "" && current != set {
		return nil
	}
	size, records, err := tableBytes(ctx, tbl)
	if err != nil || size <= 0 || records <= 0 {
		return err
	}
	width := filerewrite.RowWidth(tbl.Schema().Fields())
	writers := max(int64(m.opts.Threads), openPartitions(tbl.Metadata().PartitionSpec()), 1)
	limit := max(m.memory()/writers/max(width, 1), filerewrite.DefaultRowGroupRows)
	rows := min(filerewrite.TargetRows(size, records), limit)
	// The bytes a row drift with every insert; a size within 15% of the
	// one in force is not worth a commit, unless that one no longer fits
	// in memory.
	in := int64(filerewrite.DefaultRowGroupRows)
	if n, err := strconv.ParseInt(current, 10, 64); err == nil && n > 0 {
		in = n
	}
	if in <= limit && 100*abs(rows-in) < 15*in {
		return nil
	}
	want := strconv.FormatInt(rows, 10)
	tx := tbl.NewTransaction()
	if err := tx.SetProperties(iceberg.Properties{duckDBRowGroups: want, rowGroupsProperty: "auto:" + want}); err != nil {
		return err
	}
	if _, err := tx.Commit(ctx); err != nil {
		return err
	}
	log.Info("sized the row groups inserts write", "rows", rows, "bytes_a_row", size/records, "row_width", width)
	return nil
}

// A new table starts with row groups of half a million rows, before anyone knows how
// many it will have (see catalogproxy's firstRowGroups), and the statements that load a
// small one write it in a few: a table of a million rows, in 2 to 4. DuckDB reads a
// row group on one thread, so scanning it took as long on 64 cores as on 32: over TPC-H
// at scale factor 100, the one query that reads all of such a table's columns, twice,
// took 0.51 s on tables loaded so, or 0.58 s if one statement had written it, and
// 0.47 s with 9 row groups, and no other query moved. So a pass rewrites what is small
// enough to aim at filerewrite.TableRowGroups row groups, once it knows the table's bytes,
// if its row groups hold half as many rows again as it aims at.

// splitRowGroups rewrites tbl, if Igloo created it and nobody else set its row group
// size, if it is small and its row groups too few: splitChecked holds the snapshot
// looked at last. It returns the table as committed.
func (m *Maintainer) splitRowGroups(ctx context.Context, tbl *table.Table, log *slog.Logger) (*table.Table, error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil || !strings.HasPrefix(tbl.Properties()[rowGroupsProperty], "auto") || !tbl.Metadata().PartitionSpec().IsUnpartitioned() {
		return tbl, nil
	}
	key := name(tbl.Identifier())
	if m.splitChecked[key] == snap.SnapshotID {
		return tbl, nil
	}
	if _, _, clustered := clusterFields(tbl); clustered {
		return tbl, nil
	}
	size, records, err := tableBytes(ctx, tbl)
	if err != nil || size <= 0 || records <= 0 {
		return tbl, err
	}
	if size > filerewrite.TableRowGroups*filerewrite.TargetRowGroupBytes {
		m.splitChecked[key] = snap.SnapshotID
		return tbl, nil
	}
	want := filerewrite.TargetRows(size, records)
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return tbl, err
	}
	// A file has a row group at least, and most tables that many files or more.
	coarse := func(groups int64) bool { return 2*records > 3*want*groups }
	groups := int64(len(tasks))
	if !coarse(groups) {
		m.splitChecked[key] = snap.SnapshotID
		return tbl, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return tbl, err
	}
	groups = 0
	rewrite := make(map[string]struct{}, len(tasks))
	for _, t := range tasks {
		n, err := filerewrite.RowGroups(fs, t.File.FilePath())
		if err != nil {
			return tbl, err
		}
		groups += int64(n)
		rewrite[t.File.FilePath()] = struct{}{}
	}
	if !coarse(groups) {
		m.splitChecked[key] = snap.SnapshotID
		return tbl, nil
	}
	start := time.Now()
	g := table.CompactionTaskGroup{Tasks: tasks, TotalSizeBytes: size}
	committed, res, err := m.rewriteGroups(ctx, tbl, []table.CompactionTaskGroup{g}, rewrite, "split row groups", rewriteGroup)
	if err != nil || res.removed == 0 {
		return committed, err
	}
	m.opts.Metrics.compacted(res.removed, res.added)
	log.Info("split a small table's row groups", "rows", records, "row_groups_before", groups, "rows_a_group", want,
		"files_before", res.removed, "files_after", res.added, "bytes_before", res.bytesBefore, "bytes_after", res.bytesAfter,
		"took", time.Since(start).Round(time.Millisecond), "peak_memory", res.peak)
	return committed, nil
}

// openPartitionFiles is how many files of a partitioned write DuckDB keeps
// open (engine.OpenPartitionFiles).
const openPartitionFiles = 1024

// openPartitions is how many partitions' files an insert into a table of
// spec holds open at once, each with a row group: the product of its
// buckets, or as many as DuckDB keeps open for other transforms, whose
// values are many; 1 for a table that is not partitioned.
func openPartitions(spec iceberg.PartitionSpec) int64 {
	n := int64(1)
	for _, f := range spec.Fields() {
		b, ok := f.Transform.(iceberg.BucketTransform)
		if !ok {
			return openPartitionFiles
		}
		n *= int64(max(b.NumBuckets, 1))
		if n >= openPartitionFiles {
			return openPartitionFiles
		}
	}
	return n
}

// tableBytes returns the compressed bytes and the rows of tbl's data
// files: from its current snapshot's summary, or, since DuckDB's commits
// leave the files' size out of theirs, from the files.
func tableBytes(ctx context.Context, tbl *table.Table) (size, rows int64, err error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0, 0, nil
	}
	if snap.Summary != nil {
		size, _ = strconv.ParseInt(snap.Summary.Properties["total-files-size"], 10, 64)
		rows, _ = strconv.ParseInt(snap.Summary.Properties["total-records"], 10, 64)
		if size > 0 && rows > 0 {
			return size, rows, nil
		}
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return 0, 0, err
	}
	size, rows = 0, 0
	for _, t := range tasks {
		size += t.File.FileSizeBytes()
		rows += t.File.Count()
	}
	return size, rows, nil
}

func abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}
