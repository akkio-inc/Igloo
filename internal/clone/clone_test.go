package clone_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/clone/clonetest"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// source makes prod.events with three data files, rows 1 to 6, and a
// position delete file that removes row 2.
func source(t *testing.T, cat *clonetest.Catalog) *table.Table {
	t.Helper()
	tbl, err := cat.Create(table.Identifier{"prod", "events"}, clonetest.Schema,
		iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead, "comment": "Prod events"})
	if err != nil {
		t.Fatal(err)
	}
	tbl = clonetest.Append(t, tbl, 1, 2)
	tbl = clonetest.Append(t, tbl, 3, 4)
	tbl = clonetest.Append(t, tbl, 5, 6)
	tbl, err = tbl.Delete(context.Background(), iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func TestCloneSharesFilesAndDiverges(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	src := source(t, cat)
	store := clone.NewMemoryStore()
	m := clone.NewManager(cat, store, quiet())

	res, err := m.Clone(ctx, clone.Request{Source: table.Identifier{"prod", "events"}, Target: table.Identifier{"dev", "events"}})
	if err != nil {
		t.Fatal(err)
	}
	dst := res.Table
	if got := clonetest.IDs(t, dst); !slices.Equal(got, []int64{1, 3, 4, 5, 6}) {
		t.Fatalf("clone reads %v", got)
	}

	// The same data and delete files, nothing copied; a table of its own.
	if !slices.Equal(clonetest.Files(t, dst), clonetest.Files(t, src)) {
		t.Fatalf("clone reads other files:\n%v\n%v", clonetest.Files(t, dst), clonetest.Files(t, src))
	}
	if dst.Location() == src.Location() || dst.Metadata().TableUUID() == src.Metadata().TableUUID() {
		t.Fatal("the clone has the source's location or UUID")
	}
	if dst.CurrentSnapshot().ManifestList != src.CurrentSnapshot().ManifestList {
		t.Fatal("the clone does not reuse the source's manifest list")
	}
	if len(dst.Metadata().Snapshots()) != 1 || dst.CurrentSnapshot().ParentSnapshotID != nil {
		t.Fatalf("the clone's history: %v", dst.Metadata().Snapshots())
	}
	if dst.Metadata().LastSequenceNumber() != src.Metadata().LastSequenceNumber() {
		t.Fatalf("last sequence number %d, source %d", dst.Metadata().LastSequenceNumber(), src.Metadata().LastSequenceNumber())
	}
	props := dst.Properties()
	if props["comment"] != "Prod events" || props[clone.SourceProperty] != "prod.events" {
		t.Fatalf("properties: %v", props)
	}
	var shared []string
	if err := json.Unmarshal([]byte(props[clone.SharesProperty]), &shared); err != nil || !slices.Equal(shared, []string{src.Location()}) {
		t.Fatalf("shares %q, want the source's location", props[clone.SharesProperty])
	}
	recorded, _ := store.Sharers(ctx, src.Location())
	if len(recorded) != 1 || recorded[0].Clone != dst.Metadata().TableUUID() || recorded[0].Table != "events" {
		t.Fatalf("recorded shares: %+v", recorded)
	}

	// Writes to either table stay in it: the clone's go to its location.
	dst = clonetest.Append(t, dst, 7)
	src, err = src.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := clonetest.IDs(t, cat.Load(t, "dev", "events")); !slices.Equal(got, []int64{1, 3, 4, 5, 6, 7}) {
		t.Errorf("clone after both changed: %v", got)
	}
	if got := clonetest.IDs(t, cat.Load(t, "prod", "events")); !slices.Equal(got, []int64{3, 4, 5, 6}) {
		t.Errorf("source after both changed: %v", got)
	}
	for _, f := range clonetest.Files(t, dst) {
		if !clone.Under(f, src.Location()) && !clone.Under(f, dst.Location()) {
			t.Errorf("clone file %s is in neither table", f)
		}
	}

	// A clone of the clone shares both tables' files.
	res, err = m.Clone(ctx, clone.Request{Source: table.Identifier{"dev", "events"}, Target: table.Identifier{"dev", "events2"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(res.Table.Properties()[clone.SharesProperty]), &shared); err != nil || len(shared) != 2 {
		t.Fatalf("a clone of a clone shares %q", res.Table.Properties()[clone.SharesProperty])
	}
	if got := clonetest.IDs(t, res.Table); !slices.Equal(got, []int64{1, 3, 4, 5, 6, 7}) {
		t.Errorf("clone of the clone reads %v", got)
	}

	// The name is taken unless replacing.
	_, err = m.Clone(ctx, clone.Request{Source: table.Identifier{"prod", "events"}, Target: table.Identifier{"dev", "events"}})
	if err == nil {
		t.Fatal("cloned over an existing table")
	}
	res, err = m.Clone(ctx, clone.Request{Source: table.Identifier{"prod", "events"}, Target: table.Identifier{"dev", "events"}, Replace: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := clonetest.IDs(t, res.Table); !slices.Equal(got, []int64{3, 4, 5, 6}) {
		t.Errorf("replaced clone reads %v", got)
	}
}

func TestCloneAt(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	src := source(t, cat)
	m := clone.NewManager(cat, clone.NewMemoryStore(), quiet())
	snaps := src.Metadata().Snapshots()
	first := snaps[0]

	for name, at := range map[string]clone.At{
		"snapshot":  {Unit: clone.Snapshot, ID: first.SnapshotID},
		"timestamp": {Unit: clone.Timestamp, Time: time.UnixMilli(first.TimestampMs)},
	} {
		res, err := m.Clone(ctx, clone.Request{Source: table.Identifier{"prod", "events"}, Target: table.Identifier{"dev", name}, At: at})
		if err != nil {
			t.Fatal(err)
		}
		if got := clonetest.IDs(t, res.Table); !slices.Equal(got, []int64{1, 2}) {
			t.Errorf("%s: clone reads %v", name, got)
		}
		if res.Snapshot.SnapshotID != first.SnapshotID {
			t.Errorf("%s: cloned snapshot %d", name, res.Snapshot.SnapshotID)
		}
	}

	if err := m.CreateRef(ctx, table.Identifier{"prod", "events"}, "before_delete", table.TagRef,
		clone.At{Unit: clone.Snapshot, ID: snaps[2].SnapshotID}, false, false); err != nil {
		t.Fatal(err)
	}
	res, err := m.Clone(ctx, clone.Request{Source: table.Identifier{"prod", "events"}, Target: table.Identifier{"dev", "tagged"},
		At: clone.At{Unit: clone.Tag, Ref: "before_delete"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := clonetest.IDs(t, res.Table); !slices.Equal(got, []int64{1, 2, 3, 4, 5, 6}) {
		t.Errorf("clone at a tag reads %v", got)
	}

	for name, at := range map[string]clone.At{
		"missing snapshot": {Unit: clone.Snapshot, ID: 42},
		"too early":        {Unit: clone.Timestamp, Time: time.UnixMilli(first.TimestampMs - 1)},
		"missing tag":      {Unit: clone.Tag, Ref: "nope"},
		"tag as branch":    {Unit: clone.Branch, Ref: "before_delete"},
	} {
		_, err := m.Clone(ctx, clone.Request{Source: table.Identifier{"prod", "events"}, Target: table.Identifier{"dev", "x"}, At: at})
		if !errors.Is(err, clone.ErrNotFound) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestCloneOfEmptyTable(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	if _, err := cat.Create(table.Identifier{"prod", "empty"}, clonetest.Schema, nil); err != nil {
		t.Fatal(err)
	}
	store := clone.NewMemoryStore()
	res, err := clone.NewManager(cat, store, quiet()).Clone(ctx, clone.Request{Source: table.Identifier{"prod", "empty"}, Target: table.Identifier{"dev", "empty"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Table.CurrentSnapshot() != nil || res.Table.Properties()[clone.SharesProperty] != "" {
		t.Fatalf("a clone of an empty table: %v, %v", res.Table.CurrentSnapshot(), res.Table.Properties())
	}
	if all, _ := store.All(ctx); len(all) != 0 {
		t.Fatalf("an empty clone shares %v", all)
	}
	if got := clonetest.IDs(t, clonetest.Append(t, res.Table, 1)); !slices.Equal(got, []int64{1}) {
		t.Fatalf("writing the empty clone: %v", got)
	}
}

func TestBranchesAndTags(t *testing.T) {
	ctx := context.Background()
	cat := clonetest.New(t.TempDir())
	src := source(t, cat)
	m := clone.NewManager(cat, clone.NewMemoryStore(), quiet())
	ident := table.Identifier{"prod", "events"}
	first := src.Metadata().Snapshots()[0].SnapshotID

	if err := m.CreateRef(ctx, ident, "dev", table.BranchRef, clone.At{}, false, false); err != nil {
		t.Fatal(err)
	}
	if err := m.CreateRef(ctx, ident, "v1", table.TagRef, clone.At{Unit: clone.Snapshot, ID: first}, false, false); err != nil {
		t.Fatal(err)
	}
	if err := m.CreateRef(ctx, ident, "v1", table.TagRef, clone.At{}, false, false); !errors.Is(err, clone.ErrExists) {
		t.Errorf("a second tag v1: %v", err)
	}
	if err := m.CreateRef(ctx, ident, "v1", table.TagRef, clone.At{}, false, true); err != nil {
		t.Errorf("IF NOT EXISTS: %v", err)
	}
	if err := m.CreateRef(ctx, ident, "main", table.BranchRef, clone.At{}, true, false); !errors.Is(err, clone.ErrInvalid) {
		t.Errorf("creating main: %v", err)
	}
	refs, err := m.Refs(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, r := range refs {
		names = append(names, r.Name+"/"+string(r.Kind))
	}
	if !slices.Equal(names, []string{"main/branch", "dev/branch", "v1/tag"}) {
		t.Fatalf("refs: %v", names)
	}
	if id, err := m.Resolve(ctx, ident, clone.At{Unit: clone.Tag, Ref: "v1"}); err != nil || id != first {
		t.Fatalf("tag v1 is %d, %v; want %d", id, err, first)
	}

	// Tags and branches keep their snapshots however old: maintenance's
	// retention does not remove them.
	tbl := cat.Load(t, "prod", "events")
	for name, ref := range tbl.Metadata().Refs() {
		if name != "main" && (ref.MaxRefAgeMs == nil || *ref.MaxRefAgeMs < int64(100*365*24*time.Hour/time.Millisecond)) {
			t.Errorf("%s expires: %v", name, ref.MaxRefAgeMs)
		}
	}

	// Another engine writes to the branch; main moves ahead to it.
	tx := tbl.NewTransactionOnBranch("dev")
	sc, _ := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	b.Field(0).(*array.Int64Builder).Append(100)
	b.Field(1).(*array.StringBuilder).Append("branch@example.com")
	rec := b.NewRecordBatch()
	at := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
	if err := tx.AppendTable(ctx, at, 10, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := clonetest.IDs(t, cat.Load(t, "prod", "events")); slices.Contains(got, 100) {
		t.Fatal("a branch write reached main")
	}
	if err := m.FastForward(ctx, ident, "dev", "main"); !errors.Is(err, clone.ErrInvalid) {
		t.Errorf("fast-forwarding a branch to an older one: %v", err)
	}
	if err := m.FastForward(ctx, ident, "main", "dev"); err != nil {
		t.Fatal(err)
	}
	if got := clonetest.IDs(t, cat.Load(t, "prod", "events")); !slices.Equal(got, []int64{1, 3, 4, 5, 6, 100}) {
		t.Fatalf("main after fast-forward: %v", got)
	}
	if err := m.FastForward(ctx, ident, "main", "v1"); !errors.Is(err, clone.ErrNotFound) {
		t.Errorf("fast-forward to a tag: %v", err)
	}

	if err := m.DropRef(ctx, ident, "v1", table.BranchRef, false); !errors.Is(err, clone.ErrNotFound) {
		t.Errorf("dropping a tag as a branch: %v", err)
	}
	if err := m.DropRef(ctx, ident, "v1", table.TagRef, false); err != nil {
		t.Fatal(err)
	}
	if err := m.DropRef(ctx, ident, "v1", table.TagRef, true); err != nil {
		t.Errorf("DROP TAG IF EXISTS: %v", err)
	}
	if err := m.DropRef(ctx, ident, "main", table.BranchRef, false); !errors.Is(err, clone.ErrInvalid) {
		t.Errorf("dropping main: %v", err)
	}
}

func TestUnder(t *testing.T) {
	for _, c := range []struct {
		path, loc string
		want      bool
	}{
		{"s3://b/w/t1/data/a.parquet", "s3://b/w/t1", true},
		{"s3://b/w/t1/data/a.parquet", "s3://b/w/t1/", true},
		{"s3a://b/w/t1/data/a.parquet", "s3://b/w/t1", true},
		{"s3://b/w/t10/data/a.parquet", "s3://b/w/t1", false},
		{"s3://b/w/t1", "s3://b/w/t1", false},
	} {
		if got := clone.Under(c.path, c.loc); got != c.want {
			t.Errorf("Under(%q, %q) = %v", c.path, c.loc, got)
		}
	}
}
