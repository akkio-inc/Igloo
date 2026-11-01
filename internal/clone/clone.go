// Package clone makes zero-copy clones of Iceberg tables, keeps table
// maintenance from deleting the files clones share, and manages tables'
// branches and tags.
//
// A clone is a new table whose metadata is a copy of its source's at one
// snapshot: the same schemas, partition specs, sort orders, properties and
// sequence numbers, with a new UUID and location and a history that starts
// at that snapshot. The snapshot keeps pointing at the source's manifest
// list, so the manifests, data files and delete files stay where they are
// and nothing is copied: making a clone writes one small metadata file,
// however large the table. Writes to either table afterwards go to that
// table's own location, so the two diverge independently.
//
// Copying the metadata and registering it with the catalog, rather than
// creating a table and committing a snapshot that adds the source's files,
// keeps everything readers depend on exact. Creating a table through the
// catalog assigns fresh column, partition field and spec IDs, which no
// longer match the IDs in the source's data files and manifests once its
// schema or partitioning has changed; and an added snapshot would give
// every file one new sequence number, which changes which delete files
// apply to which data files.
//
// Files a clone shares live under its source's location, so deleting them
// is up to the source's maintenance alone (see Unreferenced), and only once
// no clone needs them (see Guard). Which tables may reference which
// locations is recorded in a Store and, durably, in each clone's
// SharesProperty.
//
// Whatever must find every copy of some data, such as erasing a person's,
// finds the clones that may read a table's files in the metadata database:
// igloo_clones.shares has a row for each clone and location it may read
// under, with clone_uuid (the clone's table-uuid), location (the other
// table's location as the catalog gives it, without a trailing slash;
// indexed), clone_schema and clone_name (where the clone was last seen)
// and created_at. A clone of a clone has rows for its source's location
// and for every location its source shares, so the rows of a table's
// location name its clones' clones too. Each clone's SharesProperty, a
// JSON array of the same locations, is the durable record that
// maintenance rebuilds the rows from, and the one to read without a
// metadata database. Both say where a clone may read, not what: the files
// it reads are those its metadata lists, and its rows for a location go
// once its own maintenance has replaced all it read there. A row's name
// may be stale, after a rename, and a row may outlive its clone, dropped
// or never made, until a later maintenance pass; Guard.Readers resolves
// both, and holds the location's lock while the caller deletes, so that no
// new clone starts reading files that are about to go.
package clone

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	icecat "github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// Properties Igloo sets on a clone.
const (
	// SharesProperty lists, as a JSON array, the locations of other tables
	// whose files the clone's metadata may reference. Maintenance rebuilds
	// the Store from it, and drops a location once the clone references
	// nothing there any more.
	SharesProperty = "igloo.clone.shares-files-of"
	// SourceProperty and SnapshotProperty say what the clone was made from.
	SourceProperty   = "igloo.clone.source"
	SnapshotProperty = "igloo.clone.source-snapshot-id"
)

// Catalog is what clones need of the Iceberg catalog; *catalog.Client
// implements it.
type Catalog interface {
	Tables(ctx context.Context) ([]table.Identifier, error)
	LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error)
	CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error)
	// Register adds the table whose metadata file is metadataLoc as ident,
	// replacing one called ident if replace is set, without deleting its
	// files.
	Register(ctx context.Context, ident table.Identifier, metadataLoc string, replace bool) (*table.Table, error)
	// NamespaceLocation is where the catalog puts a schema's tables.
	NamespaceLocation(ctx context.Context, ns string) (string, error)
}

// Manager makes clones and manages branches and tags.
type Manager struct {
	cat   Catalog
	store Store
	log   *slog.Logger
	// warehouse and storage are where the files of the tables clones
	// replace are deleted, if anywhere (see Purging).
	warehouse string
	storage   func(ctx context.Context, location string) (iceio.IO, error)
}

// NewManager makes clones in cat, recording their shares in store.
func NewManager(cat Catalog, store Store, log *slog.Logger) *Manager {
	return &Manager{cat: cat, store: store, log: log.With("component", "clone")}
}

// TableUUID returns the UUID of the table called ident, which a table made
// again under its name does not keep.
func (m *Manager) TableUUID(ctx context.Context, ident table.Identifier) (string, error) {
	t, err := m.cat.LoadTable(ctx, ident)
	if err != nil {
		return "", err
	}
	return t.Metadata().TableUUID().String(), nil
}

// Exists reports whether the catalog has a table called ident.
func (m *Manager) Exists(ctx context.Context, ident table.Identifier) (bool, error) {
	_, err := m.cat.LoadTable(ctx, ident)
	if errors.Is(err, icecat.ErrNoSuchTable) {
		return false, nil
	}
	return err == nil, err
}

// Tables lists the names of the tables in schema.
func (m *Manager) Tables(ctx context.Context, schema string) ([]string, error) {
	idents, err := m.cat.Tables(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, ident := range idents {
		if strings.EqualFold(strings.Join(icecat.NamespaceFromIdent(ident), "."), schema) {
			out = append(out, icecat.TableNameFromIdent(ident))
		}
	}
	slices.Sort(out)
	return out, nil
}

// Unit says how At picks a snapshot.
type Unit int

const (
	Current   Unit = iota // the table's current snapshot
	Snapshot              // the snapshot with ID ID
	Timestamp             // the current snapshot as of Time
	Branch                // the head of the branch Ref
	Tag                   // the snapshot of the tag Ref
)

// At picks one snapshot of a table, as AT (...) does in SQL.
type At struct {
	Unit Unit
	ID   int64
	Time time.Time
	Ref  string
}

// Pick returns the snapshot of tbl at a, or nil for a table that has none.
func (a At) Pick(tbl *table.Table) (*table.Snapshot, error) {
	meta, name := tbl.Metadata(), strings.Join(tbl.Identifier(), ".")
	switch a.Unit {
	case Current:
		return meta.CurrentSnapshot(), nil
	case Snapshot:
		if s := meta.SnapshotByID(a.ID); s != nil {
			return s, nil
		}
		return nil, fmt.Errorf("%w: %s has no snapshot %d; it may have expired", ErrNotFound, name, a.ID)
	case Timestamp:
		if s := tbl.SnapshotAsOf(a.Time.UnixMilli(), true); s != nil {
			return s, nil
		}
		return nil, fmt.Errorf("%w: %s has no snapshot as old as %s; time travel reaches back as far as the snapshot retention",
			ErrNotFound, name, a.Time.UTC().Format(time.RFC3339))
	}
	kind := table.BranchRef
	if a.Unit == Tag {
		kind = table.TagRef
	}
	for refName, ref := range meta.Refs() {
		if refName == a.Ref && ref.SnapshotRefType == kind {
			if s := meta.SnapshotByID(ref.SnapshotID); s != nil {
				return s, nil
			}
		}
	}
	return nil, fmt.Errorf("%w: %s has no %s %q", ErrNotFound, name, kind, a.Ref)
}

var (
	// ErrNotFound is a snapshot, branch or tag that does not exist.
	ErrNotFound = errors.New("not found")
	// ErrExists is a branch or tag that exists already.
	ErrExists = errors.New("already exists")
	// ErrInvalid is a change that cannot apply, such as a fast-forward
	// of a branch that is not behind.
	ErrInvalid = errors.New("invalid")
)

// Request asks for a clone of Source as Target.
type Request struct {
	Source, Target table.Identifier
	At             At
	// Replace replaces a table called Target, if there is one.
	Replace bool
}

// Result describes a clone made.
type Result struct {
	// Snapshot is the snapshot cloned, nil for a table without one.
	Snapshot *table.Snapshot
	Table    *table.Table
	// Shared are the locations whose files the clone may reference.
	Shared []string
	// Replaced is the table the clone replaced, as last loaded before,
	// whose files are left until Purge deletes them; nil for none.
	Replaced *table.Table
}

// Clone makes a clone. It holds the locks of the locations the clone will
// share files with from before it reads the source until the clone is in
// the catalog and its shares are recorded, so maintenance cannot delete a
// file the clone references in between. A Postgres store's locks are lost
// if their connection drops, so the clone checks that it held them until
// its shares were recorded, after which maintenance keeps the files.
func (m *Manager) Clone(ctx context.Context, r Request) (Result, error) {
	src, err := m.cat.LoadTable(ctx, r.Source)
	if err != nil {
		return Result{}, err
	}
	for attempt := 0; ; attempt++ {
		shared, err := m.sharedLocations(ctx, src)
		if err != nil {
			return Result{}, err
		}
		lease, _, err := m.store.LockLease(ctx, shared, true)
		if err != nil {
			return Result{}, fmt.Errorf("lock %s's files: %w", strings.Join(r.Source, "."), err)
		}
		res, again, err := m.cloneLocked(ctx, r, src, shared, lease)
		lease.Release()
		if !again || attempt == 2 {
			return res, err
		}
		// The source was replaced since it was first read: what it
		// shares may have changed too.
		if src, err = m.cat.LoadTable(ctx, r.Source); err != nil {
			return Result{}, err
		}
	}
}

// sharedLocations are the locations whose files a clone of src may
// reference: src's own, and those src shares itself.
func (m *Manager) sharedLocations(ctx context.Context, src *table.Table) ([]string, error) {
	locs := append([]string{trimSlash(src.Location())}, declaredShares(src.Properties())...)
	recorded, err := m.store.Of(ctx, src.Metadata().TableUUID())
	if err != nil {
		return nil, err
	}
	for _, s := range recorded {
		locs = append(locs, s.Location)
	}
	slices.Sort(locs)
	return slices.Compact(locs), nil
}

func (m *Manager) cloneLocked(ctx context.Context, r Request, first *table.Table, shared []string, lease Lease) (Result, bool, error) {
	src, err := m.cat.LoadTable(ctx, r.Source)
	if err != nil {
		return Result{}, false, err
	}
	if src.Metadata().TableUUID() != first.Metadata().TableUUID() {
		return Result{}, true, fmt.Errorf("%s was replaced while it was being cloned; try again", strings.Join(r.Source, "."))
	}
	snap, err := r.At.Pick(src)
	if err != nil {
		return Result{}, false, err
	}
	schemaID := src.Metadata().CurrentSchema().ID
	if snap != nil && snap.SchemaID != nil && r.At.Unit != Current {
		schemaID = *snap.SchemaID
	}

	id, err := uuid.NewV7()
	if err != nil {
		return Result{}, false, err
	}
	nsLoc, err := m.cat.NamespaceLocation(ctx, strings.Join(icecat.NamespaceFromIdent(r.Target), "."))
	if err != nil {
		return Result{}, false, err
	}
	loc := nsLoc + "/" + id.String()
	props := map[string]string{SourceProperty: strings.Join(r.Source, ".")}
	if snap != nil {
		shares, _ := json.Marshal(shared)
		props[SharesProperty] = string(shares)
		props[SnapshotProperty] = strconv.FormatInt(snap.SnapshotID, 10)
	}
	body, err := metadata(src.Metadata(), snap, schemaID, id, loc, props, time.Now())
	if err != nil {
		return Result{}, false, fmt.Errorf("clone %s: %w", strings.Join(r.Source, "."), err)
	}
	fs, err := src.FS(ctx)
	if err != nil {
		return Result{}, false, err
	}
	wfs, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return Result{}, false, errors.New("the warehouse's storage cannot be written")
	}
	metaLoc := loc + "/metadata/00000-" + uuid.NewString() + ".metadata.json"
	if err := writeFile(wfs, metaLoc, body); err != nil {
		return Result{}, false, fmt.Errorf("write the clone's metadata: %w", err)
	}

	var recorded []Share
	if snap != nil {
		schema, name := strings.Join(icecat.NamespaceFromIdent(r.Target), "."), icecat.TableNameFromIdent(r.Target)
		for _, l := range shared {
			recorded = append(recorded, Share{Clone: id, Location: l, Schema: schema, Table: name})
		}
	}
	undo := func() {
		ctx := context.WithoutCancel(ctx)
		for _, s := range recorded {
			_ = m.store.Remove(ctx, s.Clone, s.Location)
		}
		_ = fs.Remove(metaLoc)
	}
	if err := m.store.Add(ctx, recorded); err != nil {
		undo()
		return Result{}, false, fmt.Errorf("record the clone's shared files: %w", err)
	}
	// Held since before the source was read, nothing deleted its files
	// meanwhile, and whatever deletes them from now on finds the shares.
	if err := lease.Held(ctx); err != nil {
		undo()
		return Result{}, false, fmt.Errorf("%s's files could have been deleted while it was cloned; try again: %w", strings.Join(r.Source, "."), err)
	}
	// The table replaced, if any, as it is right before: the catalog
	// leaves its files, for Purge.
	var replaced *table.Table
	if r.Replace {
		replaced, err = m.cat.LoadTable(ctx, r.Target)
		if err != nil && !errors.Is(err, icecat.ErrNoSuchTable) {
			m.log.Warn("could not tell which table a clone replaces; it is replaced without its files",
				"table", strings.Join(r.Target, "."), "err", err)
		}
		if err != nil {
			replaced = nil
		}
	}
	tbl, err := m.cat.Register(ctx, r.Target, metaLoc, r.Replace)
	if err != nil {
		// The catalog may have registered the clone all the same, its
		// answer lost or the load after it failed; undoing would then
		// delete a live clone's metadata file and shares. Undo only what
		// the catalog does not have.
		got, lerr := m.cat.LoadTable(context.WithoutCancel(ctx), r.Target)
		switch {
		case lerr == nil && got.Metadata().TableUUID() == id:
			m.log.Warn("the catalog registered a clone whose registration failed", "table", strings.Join(r.Target, "."), "err", err)
			tbl = got
		case lerr == nil || errors.Is(lerr, icecat.ErrNoSuchTable):
			undo()
			return Result{}, false, err
		default:
			// A maintenance pass forgets the shares of a clone that was
			// never made.
			return Result{}, false, fmt.Errorf("register the clone: %w; the catalog did not say whether %s was made, so what it needs is kept",
				err, strings.Join(r.Target, "."))
		}
	}
	var snapID int64
	if snap != nil {
		snapID = snap.SnapshotID
	}
	m.log.Info("cloned table", "source", strings.Join(r.Source, "."), "target", strings.Join(r.Target, "."),
		"snapshot", snapID, "location", loc)
	return Result{Snapshot: snap, Table: tbl, Shared: shared, Replaced: replaced}, false, nil
}

func writeFile(fs iceio.WriteFileIO, path string, body []byte) error {
	f, err := fs.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// declaredShares reads SharesProperty.
func declaredShares(props map[string]string) []string {
	var locs []string
	if v := props[SharesProperty]; v != "" {
		_ = json.Unmarshal([]byte(v), &locs)
	}
	for i, l := range locs {
		locs[i] = trimSlash(l)
	}
	return locs
}

// metadata builds the clone's table metadata: src's, with a new UUID,
// location and properties, and only snap, without its parent, as its
// history. Everything else stays as it is, IDs and sequence numbers
// included, so the manifests the snapshot lists read the same in both
// tables and later writes to the clone get higher sequence numbers than
// any file it shares.
func metadata(src table.Metadata, snap *table.Snapshot, schemaID int, id uuid.UUID, loc string, props map[string]string, now time.Time) ([]byte, error) {
	if src.Version() < 2 {
		return nil, fmt.Errorf("format version %d tables cannot be cloned; upgrade the table to format version 2", src.Version())
	}
	for range src.EncryptionKeys() {
		return nil, errors.New("encrypted tables cannot be cloned")
	}
	raw, err := json.Marshal(src)
	if err != nil {
		return nil, err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	set := func(key string, v any) {
		b, _ := json.Marshal(v)
		m[key] = b
	}
	for _, key := range []string{"metadata-log", "statistics", "partition-statistics", "snapshots", "snapshot-log", "refs", "current-snapshot-id"} {
		delete(m, key)
	}
	properties := map[string]string{}
	for k, v := range src.Properties() {
		if !strings.HasPrefix(k, "igloo.clone.") {
			properties[k] = v
		}
	}
	for k, v := range props {
		properties[k] = v
	}
	set("table-uuid", id.String())
	set("location", loc)
	set("properties", properties)
	set("current-schema-id", schemaID)
	updated := now.UnixMilli()
	if snap != nil {
		var all struct {
			Snapshots []map[string]json.RawMessage `json:"snapshots"`
		}
		if err := json.Unmarshal(raw, &all); err != nil {
			return nil, err
		}
		snapshots := all.Snapshots
		i := slices.IndexFunc(snapshots, func(s map[string]json.RawMessage) bool {
			return string(s["snapshot-id"]) == strconv.FormatInt(snap.SnapshotID, 10)
		})
		if i < 0 {
			return nil, fmt.Errorf("snapshot %d is missing from the table's metadata", snap.SnapshotID)
		}
		delete(snapshots[i], "parent-snapshot-id")
		set("snapshots", snapshots[i:i+1])
		set("current-snapshot-id", snap.SnapshotID)
		set("snapshot-log", []map[string]int64{{"snapshot-id": snap.SnapshotID, "timestamp-ms": snap.TimestampMs}})
		set("refs", map[string]any{table.MainBranch: map[string]any{"snapshot-id": snap.SnapshotID, "type": table.BranchRef}})
		updated = max(updated, snap.TimestampMs)
	}
	set("last-updated-ms", updated)
	out, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	// The catalog would refuse metadata that does not parse; find out first.
	parsed, err := table.ParseMetadataBytes(out)
	if err != nil {
		return nil, fmt.Errorf("invalid clone metadata: %w", err)
	}
	if parsed.TableUUID() != id || parsed.LastSequenceNumber() != src.LastSequenceNumber() {
		return nil, errors.New("invalid clone metadata")
	}
	return out, nil
}
