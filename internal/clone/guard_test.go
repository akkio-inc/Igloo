package clone_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/clone/clonetest"
	"github.com/akkio-inc/igloo/internal/testdb"
)

func TestReaders(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	src := source(t, cat)
	store := clone.NewMemoryStore()
	if _, err := clone.NewManager(cat, store, quiet()).Clone(ctx,
		clone.Request{Source: table.Identifier{"prod", "events"}, Target: table.Identifier{"dev", "events"}}); err != nil {
		t.Fatal(err)
	}
	g := clone.NewGuard(cat, store, quiet())
	readers := func() []string {
		t.Helper()
		names, release, err := g.Readers(ctx, src.Location())
		if err != nil {
			t.Fatal(err)
		}
		// Held: no clone of the source can be made meanwhile.
		if _, ok, _ := store.Lock(ctx, []string{src.Location()}, false); ok {
			t.Fatal("Readers does not hold the source's files")
		}
		release()
		return names
	}
	if got := readers(); !slices.Equal(got, []string{"dev.events"}) {
		t.Fatalf("readers: %v", got)
	}
	// Renamed: found under its new name.
	cat.Rename(table.Identifier{"dev", "events"}, table.Identifier{"dev", "renamed"})
	if got := readers(); !slices.Equal(got, []string{"dev.renamed"}) {
		t.Fatalf("readers after a rename: %v", got)
	}
	// Dropped: gone once its share is too old to be a clone in the making.
	cat.Drop(table.Identifier{"dev", "renamed"})
	if got := readers(); len(got) != 1 {
		t.Fatalf("readers just after a drop: %v", got)
	}
	shares, _ := store.All(ctx)
	for _, s := range shares {
		_ = store.Remove(ctx, s.Clone, s.Location)
		s.Created = s.Created.Add(-2 * time.Hour)
		_ = store.Add(ctx, []clone.Share{s})
	}
	if got := readers(); len(got) != 0 {
		t.Fatalf("readers of a dropped clone: %v", got)
	}
	if shared, err := g.Shared(ctx, src.Location()); err != nil || !shared {
		t.Fatalf("Shared: %v %v", shared, err)
	}
	if got := g.Sources(src.Properties()); len(got) != 0 {
		t.Fatalf("the source counts as a clone: %v", got)
	}
}

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

// lockLoser is a catalog that, when a clone asks where its table goes,
// which it does holding the locks of its source's files, ends the
// metadata database's sessions that hold advisory locks, as a failover or
// an idle session timeout does, and has the guard look for the source's
// clones meanwhile, as a drop of the source does.
type lockLoser struct {
	*clonetest.Catalog
	url, src string
	g        *clone.Guard
	found    []string
}

func (c *lockLoser) NamespaceLocation(ctx context.Context, ns string) (string, error) {
	conn, err := pgx.Connect(ctx, c.url)
	if err != nil {
		return "", err
	}
	defer conn.Close(ctx) //nolint:errcheck
	if _, err := conn.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_locks
		WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			AND pid <> pg_backend_pid()`); err != nil {
		return "", err
	}
	names, release, err := c.g.Readers(ctx, c.src)
	if err != nil {
		return "", err
	}
	release()
	c.found = names
	return c.Catalog.NamespaceLocation(ctx, ns)
}

// TestCloneThatLostItsLock checks a clone whose locks of its source's files
// were lost before its shares were recorded. The guard found no clone of
// the source meanwhile, so a drop of the source would have deleted its
// files, and the clone was made all the same, reading them. Now it fails,
// and leaves nothing.
func TestCloneThatLostItsLock(t *testing.T) {
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	store, err := clone.OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	inner := clonetest.New(t.TempDir())
	src := source(t, inner)
	cat := &lockLoser{Catalog: inner, url: url, src: src.Location()}
	cat.g = clone.NewGuard(cat, store, quiet())

	_, err = clone.NewManager(cat, store, quiet()).Clone(ctx,
		clone.Request{Source: table.Identifier{"prod", "events"}, Target: table.Identifier{"dev", "events"}})
	if len(cat.found) != 0 {
		t.Fatalf("the guard found clones of the source before it was cloned: %v", cat.found)
	}
	if err == nil {
		t.Fatal("a clone that lost its locks was made")
	}
	if _, err := inner.LoadTable(ctx, table.Identifier{"dev", "events"}); err == nil {
		t.Error("the clone is in the catalog")
	}
	if shares, _ := store.Sharers(ctx, src.Location()); len(shares) != 0 {
		t.Errorf("shares of a clone never made: %+v", shares)
	}
}
