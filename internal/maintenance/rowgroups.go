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
// the schema's types make them (rowWidth). Maintenance records the size it
// set in rowGroupsProperty, after "auto:", and leaves alone a size it did
// not set, a user's.
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
	width := rowWidth(tbl.Schema().Fields())
	limit := max(m.memory()/int64(max(m.opts.Threads, 1))/max(width, 1), filerewrite.DefaultRowGroupRows)
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

// rowWidth guesses the bytes a row of fields takes uncompressed, as DuckDB
// holds it while it writes a row group.
func rowWidth(fields []iceberg.NestedField) int64 {
	var n int64
	for _, f := range fields {
		n += typeWidth(f.Type)
	}
	return n
}

func typeWidth(t iceberg.Type) int64 {
	switch t := t.(type) {
	case *iceberg.StructType:
		return rowWidth(t.FieldList)
	case *iceberg.ListType, *iceberg.MapType:
		return 64
	case iceberg.BooleanType:
		return 1
	case iceberg.Int32Type, iceberg.Float32Type, iceberg.DateType:
		return 4
	case iceberg.Int64Type, iceberg.Float64Type, iceberg.TimeType, iceberg.TimestampType, iceberg.TimestampTzType:
		return 8
	case iceberg.DecimalType, iceberg.UUIDType:
		return 16
	case iceberg.FixedType:
		return int64(t.Len())
	}
	// Strings and binary: a short string inline, a long one out of line.
	// Four short strings a row took 127 bytes more a row in groups of 4
	// million rows than in DuckDB's (14.6 GB on 16 threads, not 6.7).
	return 32
}
