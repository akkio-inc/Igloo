package maintenance

import (
	"context"
	"log/slog"
	"strconv"
	"strings"

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
