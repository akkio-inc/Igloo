package maintenance

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/clone/clonetest"
)

// Branches and tags, a table's and a clone's, keep the snapshots they point
// at past the snapshot retention, with every file those read, until they
// are dropped, as in Java's Iceberg. iceberg-go dropped every ref older than
// the retention, seven days by default, so maintenance made them vanish
// after a week. A ref whose own max-ref-age-ms has passed goes.
func TestBranchesAndTagsOutliveRetention(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	store := clone.NewMemoryStore()
	src := fragmented(t, cat)
	first := src.Metadata().Snapshots()[0].SnapshotID
	commitRefs := func(ident table.Identifier, updates ...table.Update) {
		t.Helper()
		if _, _, err := cat.CommitTable(ctx, ident, nil, updates); err != nil {
			t.Fatal(err)
		}
	}
	commitRefs(src.Identifier(),
		table.NewSetSnapshotRefUpdate("audit", first, table.TagRef, 0, 0, 0),
		table.NewSetSnapshotRefUpdate("staging", src.CurrentSnapshot().SnapshotID, table.BranchRef, 0, 0, 0),
		table.NewSetSnapshotRefUpdate("short-lived", first, table.TagRef, 1, 0, 0))
	want := []int64{1, 3, 4, 5, 6}

	// The clone's tag reads its source's files.
	dst := clone1(t, cat, store, "events", "events")
	commitRefs(dst.Identifier(), table.NewSetPropertiesUpdate(noHistory),
		table.NewSetSnapshotRefUpdate("frozen", dst.CurrentSnapshot().SnapshotID, table.TagRef, 0, 0, 0))
	clonetest.Append(t, cat.Load(t, "dev", "events"), 7)

	// Every snapshot is older than the retention, and every file old
	// enough for the orphan cleanup; maintenance runs twice, compacting,
	// expiring and cleaning up.
	time.Sleep(10 * time.Millisecond)
	age(t, cat.Dir)
	maintainer(cat, store).RunOnce(ctx)
	age(t, cat.Dir)
	maintainer(cat, store).RunOnce(ctx)

	src = cat.Load(t, "prod", "events")
	dst = cat.Load(t, "dev", "events")
	refsOf := func(tbl *table.Table) map[string]int64 {
		out := map[string]int64{}
		for name, ref := range tbl.Metadata().Refs() {
			out[name] = ref.SnapshotID
		}
		return out
	}
	srcRefs, dstRefs := refsOf(src), refsOf(dst)
	if _, ok := srcRefs["short-lived"]; ok {
		t.Errorf("a tag past its own max-ref-age-ms was kept")
	}
	for _, c := range []struct {
		tbl  *table.Table
		refs map[string]int64
		name string
		want []int64
	}{
		{src, srcRefs, "audit", []int64{1}},
		{src, srcRefs, "staging", want},
		{dst, dstRefs, "frozen", want},
	} {
		id, ok := c.refs[c.name]
		if !ok {
			t.Errorf("%s of %v was dropped", c.name, c.tbl.Identifier())
			continue
		}
		if c.tbl.SnapshotByID(id) == nil {
			t.Errorf("%s points at an expired snapshot", c.name)
			continue
		}
		if got := clonetest.IDs(t, c.tbl, table.WithSnapshotID(id)); !slices.Equal(got, c.want) {
			t.Errorf("%s reads %v, want %v", c.name, got, c.want)
		}
	}
	if got := clonetest.IDs(t, dst); !slices.Equal(got, []int64{1, 3, 4, 5, 6, 7}) {
		t.Errorf("the clone reads %v", got)
	}
	if got := clonetest.IDs(t, src); !slices.Equal(got, want) {
		t.Errorf("the source reads %v", got)
	}
}

// Java's Iceberg writes Long.MAX_VALUE milliseconds in
// history.expire.max-ref-age-ms for refs kept forever, which overflowed a
// duration: every branch and tag went.
func TestMaxRefAgeForever(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	tbl, err := cat.Create(table.Identifier{"fin", "ledger"}, clonetest.Schema, iceberg.Properties{
		maxSnapshotAgeProperty: "0", maxRefAgeProperty: "9223372036854775807"})
	if err != nil {
		t.Fatal(err)
	}
	tbl = clonetest.Append(t, tbl, 1)
	first := tbl.CurrentSnapshot().SnapshotID
	tbl = clonetest.Append(t, tbl, 2)
	if _, _, err := cat.CommitTable(ctx, tbl.Identifier(), nil, []table.Update{
		table.NewSetSnapshotRefUpdate("audit", first, table.TagRef, 0, 0, 0)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	m := New(cat, cat.Dir+"/", Options{Interval: time.Hour, Retention: time.Hour}, quiet)
	if _, _, err := m.expire(ctx, cat.Load(t, "fin", "ledger"), quiet, 0, false); err != nil {
		t.Fatal(err)
	}
	kept := false
	for name := range cat.Load(t, "fin", "ledger").Metadata().Refs() {
		kept = kept || name == "audit"
	}
	if !kept {
		t.Errorf("the tag went")
	}
}
