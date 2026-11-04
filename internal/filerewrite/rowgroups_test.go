package filerewrite

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// TestTargetRows checks the rows a rewrite aims a row group at: as many as
// make TargetRowGroupBytes at the files' bytes a row, up to MaxRowGroupRows,
// or a sixteenth of a smaller table, but no fewer than DuckDB's 122,880 for
// that, unless the table sets a limit.
func TestTargetRows(t *testing.T) {
	for _, c := range []struct {
		name       string
		size, rows int64
		want       int64
	}{
		{"narrow rows: 2.9 bytes each", 2_454_907_540, 832_597_840, 5_690_101},
		{"wide rows: 190 bytes each", 7_597_826_246, 40_000_000, 88_326},
		{"bitmaps of 2.5 MB each", 2_000_000_000, 800, 6},
		{"rows wider than the target", 1 << 30, 16, 1},
		{"nothing to measure", 0, 0, DefaultRowGroupRows},
		{"a byte a row", 1 << 40, 1 << 40, TargetRowGroupBytes},
		{"a small table of wide rows", 80_000_000, 1_000_000, DefaultRowGroupRows},
		{"a small table of narrow rows", 24_000_000, 8_000_000, 500_000},
		{"just big enough to aim at 16 MiB", 16 * TargetRowGroupBytes, 3_000_000, 187_500},
		{"almost nothing a row", 1 << 20, 1 << 40, MaxRowGroupRows},
	} {
		if got := TargetRows(c.size, c.rows); got != c.want {
			t.Errorf("%s: %d rows, want %d", c.name, got, c.want)
		}
	}
	if got := RowGroupRows(iceberg.Properties{table.ParquetRowGroupLimitKey: "5000"}, nil); got != 5000 {
		t.Errorf("a table's own limit: %d", got)
	}
}
