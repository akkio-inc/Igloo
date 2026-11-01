package maintenance

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
)

// A table an open transaction block has read keeps its files: compacting
// them would make the block's COMMIT fail, as dbt's incremental models'
// did. A block that starts reading the table while compaction writes its
// files makes compaction give way before it commits; once no block reads
// it, the table is compacted.
func TestOpenTransactionsHoldRewrites(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var commits [][]string
	for i := range 6 {
		commits = append(commits, []string{fmt.Sprintf("p%d@example.com", i)})
	}
	cat := people(t, iceberg.Properties{}, commits...)
	ident := cat.tbl.Identifier()
	start := cat.tbl.CurrentSnapshot().SnapshotID

	var asked []string
	busy := func(int) bool { return true }
	m := New(nil, "", Options{Retention: 7 * 24 * time.Hour, Busy: func(table string) bool {
		asked = append(asked, table)
		return busy(len(asked))
	}}, log)
	if errs := m.rewrite(ctx, ident, cat.tbl, 0, false, log); len(errs) > 0 {
		t.Fatal(errs)
	}
	if got := cat.tbl.CurrentSnapshot().SnapshotID; got != start || len(asked) != 1 || asked[0] != "crm.people" {
		t.Fatalf("with a block open: snapshot %d (was %d), asked %v", got, start, asked)
	}

	// The block opens after the pass looked, while the files are written.
	asked, busy = nil, func(n int) bool { return n > 1 }
	if errs := m.rewrite(ctx, ident, cat.tbl, 0, false, log); len(errs) > 0 {
		t.Fatal(errs)
	}
	if got := cat.tbl.CurrentSnapshot().SnapshotID; got != start || len(asked) != 2 {
		t.Fatalf("with a block opened during the rewrite: snapshot %d (was %d), asked %v", got, start, asked)
	}

	asked, busy = nil, func(int) bool { return false }
	if errs := m.rewrite(ctx, ident, cat.tbl, 0, false, log); len(errs) > 0 {
		t.Fatal(errs)
	}
	if got := len(dataFiles(t, cat.tbl)); cat.tbl.CurrentSnapshot().SnapshotID == start || got != 1 {
		t.Fatalf("with no block open: %d data files, snapshot unchanged %v", got, cat.tbl.CurrentSnapshot().SnapshotID == start)
	}
	if got := emails(t, cat.tbl); len(got) != 6 {
		t.Errorf("rows: %v", got)
	}
}
