package erasure

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"github.com/akkio-inc/igloo/internal/policy"
)

// clone registers a zero-copy clone of src's current snapshot as name, as
// package clone makes one: src's metadata with only that snapshot, under a
// new UUID and location, saying what it was made from and whose files it
// references.
func (c *testCatalog) clone(t *testing.T, dir string, src *table.Table, name string) *table.Table {
	t.Helper()
	raw, err := json.Marshal(src.Metadata())
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	var all struct {
		Snapshots []map[string]json.RawMessage `json:"snapshots"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &all); err != nil {
		t.Fatal(err)
	}
	set := func(key string, v any) {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		m[key] = b
	}
	snap := src.CurrentSnapshot()
	i := slices.IndexFunc(all.Snapshots, func(s map[string]json.RawMessage) bool {
		return string(s["snapshot-id"]) == strconv.FormatInt(snap.SnapshotID, 10)
	})
	delete(all.Snapshots[i], "parent-snapshot-id")
	id := uuid.New()
	location := filepath.Join(dir, "warehouse", strings.Split(name, ".")[0], id.String())
	shares, _ := json.Marshal(append([]string{src.Location()}, shares(src)...))
	props := map[string]string{}
	for k, v := range src.Properties() {
		if !strings.HasPrefix(k, "igloo.clone.") {
			props[k] = v
		}
	}
	props[sharesProperty] = string(shares)
	props[sourceProperty] = tableName(src.Identifier())
	set("table-uuid", id.String())
	set("location", location)
	set("properties", props)
	set("snapshots", all.Snapshots[i:i+1])
	set("current-snapshot-id", snap.SnapshotID)
	set("snapshot-log", []map[string]int64{{"snapshot-id": snap.SnapshotID, "timestamp-ms": snap.TimestampMs}})
	set("refs", map[string]any{table.MainBranch: map[string]any{"snapshot-id": snap.SnapshotID, "type": table.BranchRef}})
	delete(m, "metadata-log")
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := table.ParseMetadataBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	tbl := table.New(ident(name), meta, c.write(t, location, meta), localFS, c)
	c.mu.Lock()
	c.tables[name] = tbl
	c.mu.Unlock()
	return tbl
}

// testLocks stands in for the locks making a clone takes.
type testLocks struct {
	mu     sync.Mutex
	busy   bool
	locked [][]string
}

func (l *testLocks) Lock(_ context.Context, locations []string, _ bool) (func(), bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.busy {
		return nil, false, nil
	}
	l.locked = append(l.locked, slices.Sorted(slices.Values(locations)))
	return func() {}, true, nil
}

// TestErasureClones erases a person from a tagged table and its zero-copy
// clone, which renamed a column and wrote rows of its own, and from an
// untagged table whose clone is tagged: the bytes go from the files of
// both, which the clones referenced under their sources' locations. A
// tagged table of a connection is reported, not erased from.
func TestErasureClones(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	eng := newEngine(t)
	cat := &testCatalog{tables: map[string]*table.Table{}}

	people := cat.create(t, dir, "crm.people", peopleSchema)
	people = appendRows(t, people,
		[]any{int64(1), "ann@other.com", "dev-ann", "keep"},
		[]any{int64(2), storedEmail, "dev-x", "erase by email"},
		[]any{int64(3), "carl@other.com", storedDevice, "erase by device"},
		[]any{int64(4), "dora@other.com", "dev-dora", "keep"})
	dev := cat.clone(t, dir, people, "dev.people")
	tx := dev.NewTransaction()
	if err := tx.UpdateSchema(true, false).RenameColumn([]string{"email"}, "contact").Commit(); err != nil {
		t.Fatal(err)
	}
	dev, err := tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	appendRows(t, dev, []any{int64(5), storedEmail, "dev-y", "the clone's own"}, []any{int64(6), "fay@other.com", "dev-fay", "keep"})

	events := cat.create(t, dir, "crm.events", peopleSchema)
	events = appendRows(t, events,
		[]any{int64(1), storedEmail, "dev-x", "erase"},
		[]any{int64(2), "gus@other.com", "dev-gus", "keep"})
	cat.clone(t, dir, events, "lab.events")

	policies := policy.NewMemoryStore()
	err = policies.Update(ctx, func(c *policy.Catalog) error {
		now := time.Now()
		if err := c.PutTag(policy.Tag{Name: "pii", AllowedValues: []string{"email", "device_id", "phone"}}, false, now); err != nil {
			return err
		}
		for _, ct := range []policy.ColumnTag{
			{Schema: "crm", Table: "people", Column: "email", Tag: "pii", Value: "email"},
			{Schema: "crm", Table: "people", Column: "device_id", Tag: "pii", Value: "device_id"},
			{Schema: "lab", Table: "events", Column: "email", Tag: "pii", Value: "email"},
			// A table of a connection, in another database.
			{Catalog: "pg", Schema: "public", Table: "users", Column: "email", Tag: "pii", Value: "email"},
			{Catalog: "pg", Schema: "public", Table: "users", Column: "phone", Tag: "pii", Value: "phone"},
		} {
			if err := c.SetColumnTag(ct, now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Before, the person is in files under both sources' locations and
	// the clone's own.
	needles := []string{storedEmail, storedDevice}
	before := strings.Join(search(t, eng, dir, needles), "\n")
	for _, loc := range []string{people.Location(), events.Location(), dev.Location()} {
		if !strings.Contains(before, loc+"/data/") {
			t.Fatalf("before erasing, the search finds nothing under %s:\n%s", loc, before)
		}
	}

	byPerson := iceberg.NewOr(iceberg.EqualTo(iceberg.Reference("email"), storedEmail),
		iceberg.EqualTo(iceberg.Reference("device_id"), storedDevice))
	store := NewMemoryStore()
	locks := &testLocks{busy: true}
	svc := New(Options{
		Store: store, Catalog: cat, Policies: policies.Load, Engine: eng, Key: testKey, CatalogName: "lake",
		Warehouse: filepath.Join(dir, "warehouse") + "/", Locks: locks,
		Purger: NewPurger(store, &readCache{}, MemoryCache+"test", "test", slog.New(slog.DiscardHandler)),
		Rows: testRows{
			"crm.people": byPerson,
			"dev.people": iceberg.NewOr(iceberg.EqualTo(iceberg.Reference("contact"), storedEmail),
				iceberg.EqualTo(iceberg.Reference("device_id"), storedDevice)),
			"crm.events": iceberg.EqualTo(iceberg.Reference("email"), storedEmail),
			"lab.events": iceberg.EqualTo(iceberg.Reference("email"), storedEmail),
			"qa.people":  byPerson,
		},
	})
	if _, err := svc.Submit(ctx, "privacy", []Wanted{{Tag: "pii", Value: "phone", Identifiers: []string{"555"}}}); err == nil {
		t.Errorf("a request for identifiers only a connection's tables hold succeeded")
	}
	r, err := svc.Submit(ctx, "privacy", []Wanted{
		{Tag: "pii", Value: "email", Identifiers: []string{requestedEmail}},
		{Tag: "pii", Value: "device_id", Identifiers: []string{requestedDevice}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// While a clone is being made of one of the tables, no file is
	// deleted. A clone made meanwhile joins the request.
	if err := svc.Run(ctx, r.ID); err == nil || !strings.Contains(err.Error(), "clone") {
		t.Fatalf("run while a clone is being made: %v", err)
	}
	if got, _ := store.Get(ctx, r.ID); got.Status != StatusRetrying || got.Step != StepPurge {
		t.Fatalf("status %s at %s, want retrying at purge", got.Status, got.Step)
	}
	if files, _ := store.Files(ctx, r.ID); slices.ContainsFunc(files, func(f File) bool { return !f.Deleted.IsZero() }) {
		t.Fatalf("files were deleted while a clone was being made: %+v", files)
	}
	people, _ = cat.LoadTable(ctx, ident("crm.people"))
	cat.clone(t, dir, people, "qa.people")
	locks.busy = false
	if err := svc.Run(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := store.Get(ctx, r.ID)
	if got.Status != StatusCompleted {
		t.Fatalf("status %s at %s, error %q, verification %+v", got.Status, got.Step, got.Error, got.Verification)
	}
	if v := got.Verification; v.Rows+v.Files+v.Bounds+v.Statistics+v.Storage+v.Orphans != 0 {
		t.Errorf("verification: %+v", v)
	}
	if len(locks.locked) == 0 || !slices.Contains(locks.locked[0], dev.Location()) || !slices.Contains(locks.locked[0], people.Location()) {
		t.Errorf("locked %v, want every table's location", locks.locked)
	}

	tables := map[string]Table{}
	for _, tt := range got.Tables {
		tables[tt.Name] = tt
	}
	for name, want := range map[string]struct {
		reason string
		rows   int64
	}{
		"crm.people": {"tagged", 2},
		"dev.people": {"a clone of crm.people", 3},
		"lab.events": {"tagged", 1},
		"crm.events": {"lab.events is a clone of it", 1},
		"qa.people":  {"a clone of crm.people", 0},
	} {
		if tt := tables[name]; tt.Reason != want.reason || tt.RowsDeleted != want.rows {
			t.Errorf("%s: reason %q, %d rows deleted; want %q, %d", name, tt.Reason, tt.RowsDeleted, want.reason, want.rows)
		}
	}
	if tt := tables["pg.public.users"]; !tt.Remote || !strings.Contains(tt.Reason, "cannot erase") || len(tt.Steps) > 0 {
		t.Errorf("the connection's table: %+v", tt)
	}
	if cols := tables["dev.people"].Columns; !slices.ContainsFunc(cols, func(c Column) bool { return c.Name == "contact" }) {
		t.Errorf("the clone's columns are not found by field ID: %+v", cols)
	}

	// Not a trace left under any location, and the rest stays.
	if found := search(t, eng, dir, needles); len(found) > 0 {
		t.Errorf("identifiers left in storage:\n%s", strings.Join(found, "\n"))
	}
	for name, want := range map[string]int64{"crm.people": 2, "dev.people": 3, "crm.events": 1, "lab.events": 1, "qa.people": 2} {
		tbl, _ := cat.LoadTable(ctx, ident(name))
		rows, err := tbl.Scan().ToArrowTable(ctx)
		if err != nil {
			t.Fatalf("scan %s: %v", name, err)
		}
		if rows.NumRows() != want {
			t.Errorf("%s has %d rows, want %d", name, rows.NumRows(), want)
		}
		rows.Release()
	}
}
