package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/icebergtest"
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

// A table's LastModified is when its metadata last changed, as Snowflake's
// LAST_ALTERED is the last DDL or DML: a table made without rows has one,
// and a change to a column or to the table's properties moves it, with no
// snapshot. It was the current snapshot's time, so neither did: an app that
// rebuilt what it derived from a table when LAST_ALTERED moved never saw a
// dropped column.
func TestTableLastModified(t *testing.T) {
	ctx := context.Background()
	fake := icebergtest.NewCatalog(t.TempDir())
	t.Cleanup(fake.Close)
	c, err := Open(ctx, config.Config{Catalog: config.Catalog{URI: fake.URL, Warehouse: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateNamespace(ctx, table.Identifier{"s"}, nil); err != nil {
		t.Fatal(err)
	}
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.Int64})
	if _, err := c.CreateTable(ctx, Ident("s", "t"), schema); err != nil {
		t.Fatal(err)
	}
	last := func(step string) time.Time {
		t.Helper()
		m, err := c.TableMetadata(ctx, "s", "t", false)
		if err != nil {
			t.Fatal(err)
		}
		if m.LastModified.IsZero() {
			t.Fatalf("%s: LastModified is zero", step)
		}
		return m.LastModified
	}
	before := last("made without rows")
	text := "the key"
	for _, change := range []struct {
		name string
		do   func() error
	}{
		{"a column's comment", func() error { return c.SetColumnComment(ctx, "s", "t", "a", &text) }},
		{"the table's comment", func() error { return c.SetTableComment(ctx, "s", "t", &text) }},
	} {
		time.Sleep(5 * time.Millisecond)
		if err := change.do(); err != nil {
			t.Fatal(err)
		}
		if after := last(change.name); !after.After(before) {
			t.Errorf("after %s, LastModified is %v, not after %v", change.name, after, before)
		} else {
			before = after
		}
	}
}
