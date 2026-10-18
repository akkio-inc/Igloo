package catalog

import (
	"testing"

	"github.com/apache/iceberg-go"
)

// A table's rows are its records less the rows its position deletes
// delete, as summaries that count every kind of delete say, such as those
// of Java's and iceberg-go's merge-on-read updates. Summaries that leave
// a kind of delete out, as DuckDB's leave out equality deletes, are left
// to the manifests (manifestTotals), and equality deletes leave the count
// unknown, not a guess.
func TestLiveRows(t *testing.T) {
	for _, c := range []struct {
		name    string
		summary iceberg.Properties
		want    int64
	}{
		{"appended, by DuckDB", iceberg.Properties{"total-records": "2000", "total-delete-files": "0", "total-position-deletes": "0"}, 2000},
		{"appended, by Java", iceberg.Properties{"total-records": "2000", "total-delete-files": "0", "total-position-deletes": "0",
			"total-equality-deletes": "0"}, 2000},
		{"updated in place, by Java", iceberg.Properties{"total-records": "4000", "total-delete-files": "1",
			"total-position-deletes": "2000", "total-equality-deletes": "0"}, 2000},
		{"a materialized view refreshed twice", iceberg.Properties{"total-records": "33", "total-delete-files": "2",
			"total-position-deletes": "22", "total-equality-deletes": "0"}, 11},
		{"every row deleted", iceberg.Properties{"total-records": "11", "total-delete-files": "1", "total-position-deletes": "11",
			"total-equality-deletes": "0"}, 0},
		{"updated in place, by DuckDB, which counts no equality deletes", iceberg.Properties{"total-records": "4000",
			"total-delete-files": "1", "total-position-deletes": "2000"}, -1},
		{"no delete counts, no delete files", iceberg.Properties{"total-records": "7", "total-delete-files": "0"}, 7},
		{"no delete counts at all", iceberg.Properties{"total-records": "7"}, -1},
		{"delete files, uncounted", iceberg.Properties{"total-records": "7", "total-delete-files": "1"}, -1},
		{"equality deletes", iceberg.Properties{"total-records": "100", "total-delete-files": "2",
			"total-position-deletes": "5", "total-equality-deletes": "3"}, -1},
		{"no record count", iceberg.Properties{"total-delete-files": "0"}, -1},
		{"no summary", nil, -1},
	} {
		if got := liveRows(c.summary); got != c.want {
			t.Errorf("%s: liveRows = %d, want %d", c.name, got, c.want)
		}
	}
}
