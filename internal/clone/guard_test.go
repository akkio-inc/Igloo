package clone_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/clone/clonetest"
)

func TestReleaseForgetsWhatIsNoLongerShared(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	src := source(t, cat)
	store := clone.NewMemoryStore()
	res, err := clone.NewManager(cat, store, quiet()).Clone(ctx,
		clone.Request{Source: table.Identifier{"prod", "events"}, Target: table.Identifier{"dev", "events"}})
	if err != nil {
		t.Fatal(err)
	}
	id := res.Table.Metadata().TableUUID()

	// The clone declares a location it no longer references anything in.
	elsewhere := cat.Dir + "/elsewhere"
	declared, _ := json.Marshal([]string{src.Location(), elsewhere})
	if _, _, err := cat.CommitTable(ctx, res.Table.Identifier(), nil,
		[]table.Update{table.NewSetPropertiesUpdate(map[string]string{clone.SharesProperty: string(declared)})}); err != nil {
		t.Fatal(err)
	}
	pass := clone.NewGuard(cat, store, quiet()).Begin()
	dst := cat.Load(t, "dev", "events")
	pass.Saw(ctx, dst)
	if shares, _ := store.Of(ctx, id); len(shares) != 2 {
		t.Fatalf("shares the pass recorded: %+v", shares)
	}

	if err := pass.Release(ctx, dst); err != nil {
		t.Fatal(err)
	}
	var locs []string
	_ = json.Unmarshal([]byte(cat.Load(t, "dev", "events").Properties()[clone.SharesProperty]), &locs)
	if !slices.Equal(locs, []string{src.Location()}) {
		t.Fatalf("the clone declares %v", locs)
	}
	shares, _ := store.Of(ctx, id)
	if len(shares) != 1 || shares[0].Location != src.Location() {
		t.Fatalf("shares left: %+v", shares)
	}
}

func TestUnreferencedKeepsOtherTablesFiles(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	src := source(t, cat)
	res, err := clone.NewManager(cat, clone.NewMemoryStore(), quiet()).Clone(ctx,
		clone.Request{Source: table.Identifier{"prod", "events"}, Target: table.Identifier{"dev", "events"}})
	if err != nil {
		t.Fatal(err)
	}
	// The clone moves on, then expires the snapshot it was cloned at: of
	// what only that snapshot read, it would delete nothing, as all of it
	// is its source's.
	pre := clonetest.Append(t, res.Table, 7)
	tx := pre.NewTransaction()
	if err := tx.ExpireSnapshots(table.WithRetainLast(1), table.WithOlderThan(0), table.WithPostCommit(false)); err != nil {
		t.Fatal(err)
	}
	post, err := tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fs, _ := pre.FS(ctx)
	files, err := clone.Unreferenced(fs, pre.Metadata(), post.Metadata(), pre.Location())
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if clone.Under(f, src.Location()) || !clone.Under(f, pre.Location()) {
			t.Errorf("the clone would delete %s", f)
		}
	}
	// The source, expiring all but its current snapshot, would delete the
	// manifest lists of the others, which are its own.
	tx = src.NewTransaction()
	if err := tx.ExpireSnapshots(table.WithRetainLast(1), table.WithOlderThan(0), table.WithPostCommit(false)); err != nil {
		t.Fatal(err)
	}
	srcPost, err := tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	files, err = clone.Unreferenced(fs, src.Metadata(), srcPost.Metadata(), src.Location())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("expiring the source's snapshots leaves nothing to delete")
	}
	for _, s := range srcPost.Metadata().Snapshots() {
		if slices.Contains(files, s.ManifestList) {
			t.Errorf("would delete the manifest list of a retained snapshot")
		}
	}
}
