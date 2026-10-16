package maintenance

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/clone/clonetest"
)

// A table and its clone share files. Whatever maintenance does to one,
// compaction, snapshot expiry, orphan cleanup, or the table being dropped,
// the other keeps reading every row, until no table needs the files; then
// they are deleted.

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// noHistory makes maintenance expire every snapshot but the current one.
var noHistory = iceberg.Properties{maxSnapshotAgeProperty: "0", table.WriteDeleteModeKey: table.WriteModeMergeOnRead}

// fragmented makes prod.events from six single-row files, and a position
// delete file that removes row 2, so maintenance compacts it.
func fragmented(t *testing.T, cat *clonetest.Catalog) *table.Table {
	t.Helper()
	tbl, err := cat.Create(table.Identifier{"prod", "events"}, clonetest.Schema, noHistory)
	if err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 6; id++ {
		tbl = clonetest.Append(t, tbl, id)
	}
	tbl, err = tbl.Delete(context.Background(), iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func maintainer(cat *clonetest.Catalog, store clone.Store) *Maintainer {
	return New(cat, cat.Dir+"/", Options{Interval: time.Hour, Retention: time.Hour, Clones: clone.NewGuard(cat, store, quiet)}, quiet)
}

// age makes every file under dir four days old, old enough for the orphan
// cleanup.
func age(t *testing.T, dir string) {
	t.Helper()
	old := time.Now().Add(-4 * 24 * time.Hour)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		return os.Chtimes(path, old, old)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func exist(t *testing.T, files []string) []string {
	t.Helper()
	var out []string
	for _, f := range files {
		_, err := os.Stat(f)
		switch {
		case err == nil:
			out = append(out, f)
		case !errors.Is(err, fs.ErrNotExist):
			t.Fatal(err)
		}
	}
	return out
}

// clone1 clones prod.from as dev.to, which keeps a day of history:
// maintenance compacts the clone too, but keeps the snapshot that reads
// its source's files.
func clone1(t *testing.T, cat *clonetest.Catalog, store clone.Store, from, to string) *table.Table {
	t.Helper()
	ctx := context.Background()
	res, err := clone.NewManager(cat, store, quiet).Clone(ctx,
		clone.Request{Source: table.Identifier{"prod", from}, Target: table.Identifier{"dev", to}})
	if err != nil {
		t.Fatal(err)
	}
	day := iceberg.Properties{maxSnapshotAgeProperty: "86400000"}
	if _, _, err := cat.CommitTable(ctx, res.Table.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(day)}); err != nil {
		t.Fatal(err)
	}
	return cat.Load(t, "dev", to)
}

func TestMaintenanceKeepsSharedFiles(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	store := clone.NewMemoryStore()
	src := fragmented(t, cat)
	before := clonetest.Files(t, src)
	clone1(t, cat, store, "events", "events")
	want := []int64{1, 3, 4, 5, 6}

	// The source is compacted and its old snapshots expire: its files are
	// all replaced, but the clone still reads them.
	maintainer(cat, store).RunOnce(ctx)
	src = cat.Load(t, "prod", "events")
	if n := len(src.Metadata().Snapshots()); n != 1 {
		t.Fatalf("source has %d snapshots after maintenance", n)
	}
	if after := clonetest.Files(t, src); len(after) != 1 || slices.Contains(before, after[0]) {
		t.Fatalf("source not compacted: %v", after)
	}
	if got := exist(t, before); len(got) != len(before) {
		t.Fatalf("maintenance deleted files the clone reads: %v of %v are left", got, before)
	}
	if got := clonetest.IDs(t, cat.Load(t, "dev", "events")); !slices.Equal(got, want) {
		t.Fatalf("clone after source maintenance: %v", got)
	}

	// Days later the orphan cleanup finds the files the source no longer
	// uses, and keeps them too. It runs after a restart, with an empty
	// store: the clone's metadata says what it shares.
	age(t, cat.Dir)
	store = clone.NewMemoryStore()
	restarted := maintainer(cat, store)
	restarted.RunOnce(ctx)
	if got := exist(t, before); len(got) != len(before) {
		t.Fatalf("orphan cleanup deleted files the clone reads: %v of %v are left", got, before)
	}
	if got := clonetest.IDs(t, cat.Load(t, "dev", "events")); !slices.Equal(got, want) {
		t.Fatalf("clone after orphan cleanup: %v", got)
	}

	// The clone's snapshot that read them expires in turn: the clone was
	// compacted, so it reads only files of its own. Its maintenance leaves
	// the source's files alone.
	dst := cat.Load(t, "dev", "events")
	if _, _, err := cat.CommitTable(ctx, dst.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(noHistory)}); err != nil {
		t.Fatal(err)
	}
	dst = clonetest.Append(t, cat.Load(t, "dev", "events"), 7)
	restarted.RunOnce(ctx)
	dst = cat.Load(t, "dev", "events")
	if n := len(dst.Metadata().Snapshots()); n != 1 {
		t.Fatalf("clone has %d snapshots after maintenance", n)
	}
	for _, f := range clonetest.Files(t, dst) {
		if !clone.Under(f, dst.Location()) {
			t.Fatalf("compacted clone still reads %s", f)
		}
	}
	if got := exist(t, before); len(got) != len(before) {
		t.Fatalf("the clone's maintenance deleted its source's files: %v of %v are left", got, before)
	}
	if got := clonetest.IDs(t, cat.Load(t, "prod", "events")); !slices.Equal(got, want) {
		t.Fatalf("source after clone maintenance: %v", got)
	}
	if got := clonetest.IDs(t, dst); !slices.Equal(got, []int64{1, 3, 4, 5, 6, 7}) {
		t.Fatalf("clone after its maintenance: %v", got)
	}

	// Now no table reads the source's old data files: they go. The clone
	// may still list a manifest of its source, which stays.
	age(t, cat.Dir)
	maintainer(cat, store).RunOnce(ctx)
	if got := exist(t, before); len(got) != 0 {
		t.Fatalf("files no table needs were kept: %v", got)
	}
	if got := clonetest.IDs(t, cat.Load(t, "dev", "events")); !slices.Equal(got, []int64{1, 3, 4, 5, 6, 7}) {
		t.Fatalf("clone at the end: %v", got)
	}
	if got := clonetest.IDs(t, cat.Load(t, "prod", "events")); !slices.Equal(got, want) {
		t.Fatalf("source at the end: %v", got)
	}
}

func TestDroppedSourceKeepsClone(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	store := clone.NewMemoryStore()
	src := fragmented(t, cat)
	files := clonetest.Files(t, src)
	clone1(t, cat, store, "events", "events")

	// Dropping the source leaves its files, as Igloo never purges; the
	// clone's maintenance, which expires the snapshot that read them,
	// never deletes them.
	cat.Drop(src.Identifier())
	dst := cat.Load(t, "dev", "events")
	if _, _, err := cat.CommitTable(ctx, dst.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(noHistory)}); err != nil {
		t.Fatal(err)
	}
	clonetest.Append(t, cat.Load(t, "dev", "events"), 7)
	age(t, cat.Dir)
	maintainer(cat, store).RunOnce(ctx)
	if got := exist(t, files); len(got) != len(files) {
		t.Fatalf("the clone's maintenance deleted the dropped source's files: %v of %v are left", got, files)
	}
	if got := clonetest.IDs(t, cat.Load(t, "dev", "events")); !slices.Equal(got, []int64{1, 3, 4, 5, 6, 7}) {
		t.Fatalf("clone of a dropped source: %v", got)
	}
}

func TestDroppedCloneReleasesFiles(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	store := clone.NewMemoryStore()
	before := clonetest.Files(t, fragmented(t, cat))
	clone1(t, cat, store, "events", "events")
	maintainer(cat, store).RunOnce(ctx)

	// The clone is dropped. Its shares were recorded moments ago, so this
	// replica cannot tell it from a clone being made, and keeps the files.
	cat.Drop(table.Identifier{"dev", "events"})
	age(t, cat.Dir)
	maintainer(cat, store).RunOnce(ctx)
	if got := exist(t, before); len(got) != len(before) {
		t.Fatalf("files of a just-dropped clone were deleted: %v of %v are left", got, before)
	}

	// An hour later the shares are settled, and the files go.
	old, _ := store.All(ctx)
	for i := range old {
		old[i].Created = time.Now().Add(-2 * time.Hour)
		_ = store.Remove(ctx, old[i].Clone, old[i].Location)
	}
	if err := store.Add(ctx, old); err != nil {
		t.Fatal(err)
	}
	maintainer(cat, store).RunOnce(ctx)
	if got := exist(t, before); len(got) != 0 {
		t.Fatalf("files of a dropped clone were kept: %v", got)
	}
	if shares, _ := store.All(ctx); len(shares) != 0 {
		t.Fatalf("a dropped clone's shares are left: %+v", shares)
	}
	if got := clonetest.IDs(t, cat.Load(t, "prod", "events")); !slices.Equal(got, []int64{1, 3, 4, 5, 6}) {
		t.Fatalf("source: %v", got)
	}
}

// failingStore cannot say which clones share a location.
type failingStore struct{ clone.Store }

func (failingStore) Sharers(context.Context, string) ([]clone.Share, error) {
	return nil, errors.New("metadata database unavailable")
}

func TestMaintenanceKeepsFilesWhenUnsure(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	store := clone.NewMemoryStore()
	before := clonetest.Files(t, fragmented(t, cat))
	clone1(t, cat, store, "events", "events")

	// Without the record of what clones share, nothing is deleted...
	maintainer(cat, failingStore{store}).RunOnce(ctx)
	if got := exist(t, before); len(got) != len(before) {
		t.Fatalf("files were deleted without knowing the clones: %v of %v are left", got, before)
	}
	// ...nor while a clone is being made of the table.
	src := cat.Load(t, "prod", "events")
	release, _, err := store.Lock(ctx, []string{src.Location()}, true)
	if err != nil {
		t.Fatal(err)
	}
	cat.Drop(table.Identifier{"dev", "events"})
	for _, s := range must(store.All(ctx)) {
		_ = store.Remove(ctx, s.Clone, s.Location)
	}
	age(t, cat.Dir)
	maintainer(cat, store).RunOnce(ctx)
	if got := exist(t, before); len(got) != len(before) {
		t.Fatalf("files were deleted while a clone was being made: %v of %v are left", got, before)
	}
	release()
	maintainer(cat, store).RunOnce(ctx)
	if got := exist(t, before); len(got) != 0 {
		t.Fatalf("files no table needs were kept: %v", got)
	}

	// A share whose clone the catalog may have renamed keeps its files.
	src = cat.Load(t, "prod", "events")
	recent := []clone.Share{{Clone: uuid.New(), Location: src.Location(), Schema: "dev", Table: "gone", Created: time.Now()}}
	if err := store.Add(ctx, recent); err != nil {
		t.Fatal(err)
	}
	pass := clone.NewGuard(cat, store, quiet).Begin()
	deleted, err := pass.Delete(ctx, src, []string{src.Location() + "/data/x.parquet"},
		func(context.Context, []string) error { t.Fatal("deleted"); return nil })
	if !errors.Is(err, clone.ErrUnsure) || len(deleted) != 0 {
		t.Fatalf("deleted %v, %v", deleted, err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
