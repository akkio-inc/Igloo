package clone_test

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/clone/clonetest"
)

// filesUnder lists the files in dir, a table's location on the local disk.
func filesUnder(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return fs.SkipDir
		}
		if err == nil && !d.IsDir() {
			out = append(out, p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}

var (
	prodEvents = table.Identifier{"prod", "events"}
	devEvents  = table.Identifier{"dev", "events"}
)

// devClone clones prod.events as dev.events and writes rows 7 and 8 to
// the clone, into its own location.
func devClone(t *testing.T, m *clone.Manager, cat *clonetest.Catalog) *table.Table {
	t.Helper()
	if _, err := m.Clone(context.Background(), clone.Request{Source: prodEvents, Target: devEvents}); err != nil {
		t.Fatal(err)
	}
	dev := clonetest.Append(t, cat.Load(t, "dev", "events"), 7, 8)
	if got := clonetest.IDs(t, dev); !slices.Equal(got, []int64{1, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("the dev clone reads %v", got)
	}
	return dev
}

// A clone that replaces a table deletes the replaced table's files, those
// under its own location, and nothing else: when the table replaced is
// itself a clone, the files of its source, which it read, stay, and the
// source reads in full.
func TestPurgeReplacedClone(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	src := source(t, cat)
	m := clone.NewManager(cat, clone.NewMemoryStore(), quiet())
	m.Purging(cat.Dir+"/", nil)
	old := devClone(t, m, cat)
	srcFiles := filesUnder(t, src.Location())
	if len(filesUnder(t, old.Location())) == 0 {
		t.Fatal("the dev clone has no files of its own")
	}

	res, err := m.Clone(ctx, clone.Request{Source: prodEvents, Target: devEvents, Replace: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replaced == nil || res.Replaced.Metadata().TableUUID() != old.Metadata().TableUUID() {
		t.Fatalf("replaced %v, want the dev clone", res.Replaced)
	}
	if len(filesUnder(t, old.Location())) == 0 {
		t.Fatal("the replaced clone's files went before Purge")
	}
	m.Purge(ctx, res.Replaced)

	if left := filesUnder(t, old.Location()); len(left) > 0 {
		t.Errorf("the replaced clone left %v", left)
	}
	if got := filesUnder(t, src.Location()); !slices.Equal(got, srcFiles) {
		t.Errorf("the source's files changed:\n%v\nwant\n%v", got, srcFiles)
	}
	if got := clonetest.IDs(t, cat.Load(t, "prod", "events")); !slices.Equal(got, []int64{1, 3, 4, 5, 6}) {
		t.Errorf("the source reads %v", got)
	}
	if got := clonetest.IDs(t, cat.Load(t, "dev", "events")); !slices.Equal(got, []int64{1, 3, 4, 5, 6}) {
		t.Errorf("the new clone reads %v", got)
	}
	// A replace of a table that does not exist replaces nothing.
	res, err = m.Clone(ctx, clone.Request{Source: prodEvents, Target: table.Identifier{"dev", "fresh"}, Replace: true})
	if err != nil || res.Replaced != nil {
		t.Fatalf("replacing nothing: %v, %v", res.Replaced, err)
	}
}

// A replaced table's files stay when anything may still need them: a
// clone that reads them, whether the Store records it or only its
// properties say so, as after a restart without a metadata database; a
// table of the catalog where they are, as the replaced table is if it was
// renamed before the clone took its name; or a location outside the
// warehouse.
func TestPurgeKeepsWhatOthersNeed(t *testing.T) {
	ctx := context.Background()
	setup := func(t *testing.T) (*clonetest.Catalog, *clone.Manager, *table.Table) {
		cat := clonetest.New(t.TempDir())
		source(t, cat)
		m := clone.NewManager(cat, clone.NewMemoryStore(), quiet())
		return cat, m, devClone(t, m, cat)
	}
	kept := func(t *testing.T, cat *clonetest.Catalog, old *table.Table, before []string) {
		t.Helper()
		if got := filesUnder(t, old.Location()); !slices.Equal(got, before) {
			t.Errorf("the replaced table's files changed:\n%v\nwant\n%v", got, before)
		}
		if got := clonetest.IDs(t, cat.Load(t, "prod", "events")); !slices.Equal(got, []int64{1, 3, 4, 5, 6}) {
			t.Errorf("the source reads %v", got)
		}
	}

	for _, recorded := range []bool{true, false} {
		name := "a clone reads them, recorded"
		if !recorded {
			name = "a clone reads them, as its properties say"
		}
		t.Run(name, func(t *testing.T) {
			cat, m, old := setup(t)
			if _, err := m.Clone(ctx, clone.Request{Source: devEvents, Target: table.Identifier{"dev", "events2"}}); err != nil {
				t.Fatal(err)
			}
			before := filesUnder(t, old.Location())
			res, err := m.Clone(ctx, clone.Request{Source: prodEvents, Target: devEvents, Replace: true})
			if err != nil {
				t.Fatal(err)
			}
			if !recorded {
				m = clone.NewManager(cat, clone.NewMemoryStore(), quiet())
			}
			m.Purging(cat.Dir+"/", nil)
			m.Purge(ctx, res.Replaced)
			kept(t, cat, old, before)
			if got := clonetest.IDs(t, cat.Load(t, "dev", "events2")); !slices.Equal(got, []int64{1, 3, 4, 5, 6, 7, 8}) {
				t.Errorf("the clone of the replaced clone reads %v", got)
			}
		})
	}

	t.Run("a clone being made reads them", func(t *testing.T) {
		cat := clonetest.New(t.TempDir())
		source(t, cat)
		store := clone.NewMemoryStore()
		m := clone.NewManager(cat, store, quiet())
		old := devClone(t, m, cat)
		before := filesUnder(t, old.Location())
		res, err := m.Clone(ctx, clone.Request{Source: prodEvents, Target: devEvents, Replace: true})
		if err != nil {
			t.Fatal(err)
		}
		// Recorded, and not in the catalog yet.
		if err := store.Add(ctx, []clone.Share{{Clone: uuid.New(), Location: old.Location(), Schema: "dev", Table: "making"}}); err != nil {
			t.Fatal(err)
		}
		m.Purging(cat.Dir+"/", nil)
		m.Purge(ctx, res.Replaced)
		kept(t, cat, old, before)
	})

	t.Run("the table is still in the catalog", func(t *testing.T) {
		cat, m, old := setup(t)
		m.Purging(cat.Dir+"/", nil)
		before := filesUnder(t, old.Location())
		cat.Rename(devEvents, table.Identifier{"dev", "moved"})
		m.Purge(ctx, old)
		kept(t, cat, old, before)
		if got := clonetest.IDs(t, cat.Load(t, "dev", "moved")); !slices.Equal(got, []int64{1, 3, 4, 5, 6, 7, 8}) {
			t.Errorf("the renamed table reads %v", got)
		}
	})

	for _, c := range []struct {
		name string
		// The catalog's tables are in root/dir, and the warehouse is
		// warehouse(root).
		dir       string
		warehouse func(root string) string
	}{
		{"outside the warehouse", "tables", func(string) string { return "/elsewhere/" }},
		{"without a warehouse", "tables", func(string) string { return "" }},
		{"in Igloo's own files", ".igloo/tables", func(root string) string { return root + "/" }},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			cat := clonetest.New(filepath.Join(root, c.dir))
			source(t, cat)
			m := clone.NewManager(cat, clone.NewMemoryStore(), quiet())
			old := devClone(t, m, cat)
			before := filesUnder(t, old.Location())
			res, err := m.Clone(ctx, clone.Request{Source: prodEvents, Target: devEvents, Replace: true})
			if err != nil {
				t.Fatal(err)
			}
			m.Purging(c.warehouse(root), nil)
			m.Purge(ctx, res.Replaced)
			kept(t, cat, old, before)
		})
	}
}

// failingStore cannot say which clones read a location.
type failingStore struct{ clone.Store }

func (failingStore) Sharers(context.Context, string) ([]clone.Share, error) {
	return nil, errors.New("metadata database unavailable")
}

// A drop decides as a clone refresh does (Guard.MayPurge): the table being
// dropped is no reason to keep its own files, but a clone that reads them
// is, recorded or only declared in its properties, as when a memory Store
// has not been rebuilt since a restart; so are a location in Igloo's own
// files, or at their root, one outside the warehouse, another table where
// they are, and not being able to tell.
func TestPurgeDecisionForDrops(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cat := clonetest.New(filepath.Join(root, "tables"))
	src := source(t, cat)
	store := clone.NewMemoryStore()
	if _, err := clone.NewManager(cat, store, quiet()).Clone(ctx, clone.Request{Source: prodEvents, Target: devEvents}); err != nil {
		t.Fatal(err)
	}
	other, err := cat.Create(table.Identifier{"prod", "other"}, clonetest.Schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	guard := func(store clone.Store) *clone.Guard {
		g := clone.NewGuard(cat, store, quiet())
		g.SetWarehouse(root + "/")
		return g
	}

	// No clone reads prod.other: it goes, and no clone of it is made
	// until the drop is through.
	v := guard(store).MayPurge(ctx, "drop", table.Identifier{"prod", "other"}, other.Location(), true)
	if !v.Purge || v.Release == nil {
		t.Fatalf("a table no clone reads: %+v", v)
	}
	v.Release()

	for name, s := range map[string]clone.Store{"recorded": store, "only declared": clone.NewMemoryStore()} {
		v := guard(s).MayPurge(ctx, "drop", prodEvents, src.Location(), true)
		if v.Purge || !slices.Equal(v.Clones, []string{"dev.events"}) {
			t.Errorf("a table a clone reads, %s: %+v", name, v)
		}
	}
	v = guard(failingStore{clone.NewMemoryStore()}).MayPurge(ctx, "drop", table.Identifier{"prod", "other"}, other.Location(), true)
	if v.Purge || !v.Unsure {
		t.Errorf("clones that cannot be told: %+v", v)
	}
	// A table renamed is still where its files are.
	v = guard(store).MayPurge(ctx, "drop", table.Identifier{"prod", "gone"}, other.Location(), true)
	if v.Purge {
		t.Errorf("another table is at the location: %+v", v)
	}
	for _, loc := range []string{root + "/.igloo", root + "/.igloo/", root + "/.igloo/sharing/t", "/elsewhere/t", root, ""} {
		if v := guard(clone.NewMemoryStore()).MayPurge(ctx, "drop", table.Identifier{"prod", "x"}, loc, false); v.Purge {
			t.Errorf("location %q: %+v", loc, v)
		}
	}
	if v := clone.NewGuard(cat, store, quiet()).MayPurge(ctx, "drop", table.Identifier{"prod", "other"}, other.Location(), true); v.Purge {
		t.Errorf("without a warehouse: %+v", v)
	}
	g := clone.NewGuard(unlistable{cat}, store, quiet())
	g.SetWarehouse(root + "/")
	if v := g.MayPurge(ctx, "drop", table.Identifier{"prod", "other"}, other.Location(), true); v.Purge || !v.Unsure {
		t.Errorf("a catalog that cannot be read: %+v", v)
	}
}

// unlistable is a catalog that cannot list its tables.
type unlistable struct{ clone.Catalog }

func (unlistable) Tables(context.Context) ([]table.Identifier, error) {
	return nil, errors.New("catalog unavailable")
}
