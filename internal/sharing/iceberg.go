package sharing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// version is what a request asks of a table: its current snapshot, the one
// at a Delta version, or the one current at a time.
type version struct {
	number *int64
	at     *time.Time
}

func (v version) timeTravel() bool { return v.number != nil || v.at != nil }

// snapshot is one version of a shared table, as the protocol serves it.
type snapshot struct {
	// tableID is the Iceberg table's UUID, which recipients see as the
	// table's ID.
	tableID string
	comment string
	// version is the Delta version: the Iceberg snapshot's sequence
	// number, which grows by one with each commit (its timestamp for
	// format version 1 tables, which have none). 0 for an empty table.
	version   int64
	timestamp int64
	schema    string
	files     []dataFile
	size      int64
}

// dataFile is a Parquet data file of a snapshot.
type dataFile struct {
	path  string
	size  int64
	count int64
	stats string
}

// id is the file's ID in the protocol: stable for the same file across
// requests, and revealing nothing about where it is stored.
func (f dataFile) id() string {
	h := sha256.Sum256([]byte(f.path))
	return hex.EncodeToString(h[:16])
}

// source reads the catalog's tables for the sharing endpoint.
type source interface {
	// snapshot returns the version of schema.name that v selects, with
	// its files when files is true. Tables that cannot be served fail
	// with an errNotShareable error.
	snapshot(ctx context.Context, schema, name string, v version, files bool) (*snapshot, error)
	// startingVersion returns the first version committed at or after t.
	startingVersion(ctx context.Context, schema, name string, t time.Time) (int64, error)
	// gone tells the source that a file it listed could not be found.
	gone(path string)
}

// icebergSource reads tables from the Iceberg REST catalog.
type icebergSource struct {
	cat *catalog.Client
	// warehouse is the warehouse root with a trailing slash. Only files
	// under it are served.
	warehouse string
	// copies are data files with their deletes applied.
	copies *materializer

	// files caches the file listing of recent snapshots, which never
	// change, so recipients that query a version repeatedly do not make
	// Igloo read its manifests each time.
	mu    sync.Mutex
	files map[string][]dataFile
	order []string
}

// maxCachedSnapshots bounds icebergSource.files.
const maxCachedSnapshots = 64

func newIcebergSource(cat *catalog.Client, warehouse string, files iceio.IO) *icebergSource {
	return &icebergSource{cat: cat, warehouse: warehouse, copies: newMaterializer(files, warehouse), files: map[string][]dataFile{}}
}

func (s *icebergSource) gone(path string) { s.copies.forget(path) }

func (s *icebergSource) load(ctx context.Context, schema, name string) (*table.Table, error) {
	tbl, err := s.cat.LoadTable(ctx, catalog.Ident(schema, name))
	if errors.Is(err, icecat.ErrNoSuchTable) || errors.Is(err, icecat.ErrNoSuchNamespace) {
		return nil, fmt.Errorf("table %s.%s %w", schema, name, ErrNotFound)
	}
	return tbl, err
}

func (s *icebergSource) snapshot(ctx context.Context, schema, name string, v version, withFiles bool) (*snapshot, error) {
	tbl, err := s.load(ctx, schema, name)
	if err != nil {
		return nil, err
	}
	meta := tbl.Metadata()
	qualified := schema + "." + name
	snap, err := selectSnapshot(meta, v)
	if err != nil {
		return nil, err
	}

	sch := meta.CurrentSchema()
	if snap != nil && snap.SchemaID != nil {
		for _, sc := range meta.Schemas() {
			if sc.ID == *snap.SchemaID {
				sch = sc
			}
		}
	}
	if err := checkSchemaHistory(meta.CurrentSchema(), meta.Schemas(), qualified); err != nil {
		return nil, err
	}
	schemaString, err := deltaSchema(sch)
	if err != nil {
		return nil, err
	}

	out := &snapshot{tableID: meta.TableUUID().String(), comment: meta.Properties()["comment"], schema: schemaString}
	if snap == nil {
		return out, nil
	}
	out.version, out.timestamp = deltaVersion(meta, snap), snap.TimestampMs
	if snap.Summary != nil {
		out.size, _ = strconv.ParseInt(snap.Summary.Properties["total-files-size"], 10, 64)
	}
	if !withFiles {
		return out, nil
	}
	key := out.tableID + "/" + strconv.FormatInt(snap.SnapshotID, 10)
	if files, ok := s.cached(key); ok {
		out.files = files
		return out.withFileSizes(), nil
	}
	tasks, err := tbl.Scan(table.WithSnapshotID(snap.SnapshotID)).PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the files of %s: %w", qualified, err)
	}
	files := make([]dataFile, 0, len(tasks))
	for _, t := range tasks {
		if t.File.FileFormat() != iceberg.ParquetFile {
			return nil, notShareable("%s has %s data files; only Parquet files can be shared", qualified, t.File.FileFormat())
		}
		if !strings.HasPrefix(t.File.FilePath(), s.warehouse) {
			return nil, notShareable("%s has data files outside the warehouse, which Igloo does not serve", qualified)
		}
		if hasDeletes(t) {
			f, err := s.copies.copy(ctx, tbl, snap.SnapshotID, t, sch)
			if err != nil {
				return nil, err
			}
			files = append(files, f)
			continue
		}
		files = append(files, dataFile{path: t.File.FilePath(), size: t.File.FileSizeBytes(), count: t.File.Count(), stats: fileStats(t.File, sch)})
	}
	s.remember(key, files)
	out.files = files
	return out.withFileSizes(), nil
}

func hasDeletes(t table.FileScanTask) bool {
	return len(t.DeleteFiles) > 0 || len(t.EqualityDeleteFiles) > 0 || len(t.DeletionVectorFiles) > 0
}

// collect deletes the copies of data files that no current version of the
// given tables needs (see materializer.collect).
func (s *icebergSource) collect(ctx context.Context, tables []SharedTable) (int, error) {
	needed := map[string]bool{}
	seen := map[string]bool{}
	for _, t := range tables {
		key := strings.ToLower(t.SourceSchema + "." + t.SourceTable)
		if seen[key] {
			continue
		}
		seen[key] = true
		tbl, err := s.load(ctx, t.SourceSchema, t.SourceTable)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			// Unsure what it needs: keep everything.
			return 0, err
		}
		snap := tbl.CurrentSnapshot()
		if snap == nil {
			continue
		}
		tasks, err := tbl.Scan(table.WithSnapshotID(snap.SnapshotID)).PlanFiles(ctx)
		if err != nil {
			return 0, err
		}
		for _, task := range tasks {
			if hasDeletes(task) {
				needed[s.copies.copyPath(tbl.Metadata().TableUUID().String(), task)] = true
			}
		}
	}
	return s.copies.collect(needed, time.Now())
}

// withFileSizes fills in the table's size from its files when the snapshot's
// summary did not have it.
func (s *snapshot) withFileSizes() *snapshot {
	if s.size == 0 {
		for _, f := range s.files {
			s.size += f.size
		}
	}
	return s
}

func (s *icebergSource) startingVersion(ctx context.Context, schema, name string, t time.Time) (int64, error) {
	tbl, err := s.load(ctx, schema, name)
	if err != nil {
		return 0, err
	}
	meta := tbl.Metadata()
	var found *table.Snapshot
	for _, snap := range ancestors(meta) {
		if snap.TimestampMs >= t.UnixMilli() {
			found = snap
		}
	}
	if found == nil {
		return 0, badRequest("no version of the table was committed at or after %s", t.UTC().Format(time.RFC3339))
	}
	return deltaVersion(meta, found), nil
}

// selectSnapshot finds the snapshot v asks for: the current one, the
// ancestor whose Delta version is v.number, or the latest ancestor
// committed at or before v.at. Only ancestors of the current snapshot count,
// so versions rolled back are not served. It returns nil for an empty table.
func selectSnapshot(meta table.Metadata, v version) (*table.Snapshot, error) {
	current := meta.CurrentSnapshot()
	switch {
	case v.number != nil:
		if current == nil && *v.number == 0 {
			return nil, nil
		}
		for _, snap := range ancestors(meta) {
			if deltaVersion(meta, snap) == *v.number {
				return snap, nil
			}
		}
		return nil, badRequest("version %d of the table does not exist or has expired", *v.number)
	case v.at != nil:
		for _, snap := range ancestors(meta) {
			if snap.TimestampMs <= v.at.UnixMilli() {
				return snap, nil
			}
		}
		return nil, badRequest("the table has no version as old as %s, or it has expired", v.at.UTC().Format(time.RFC3339))
	}
	return current, nil
}

// ancestors lists the current snapshot and its ancestors, newest first.
func ancestors(meta table.Metadata) []*table.Snapshot {
	var out []*table.Snapshot
	for snap := meta.CurrentSnapshot(); snap != nil && len(out) < 100_000; {
		out = append(out, snap)
		if snap.ParentSnapshotID == nil {
			break
		}
		snap = meta.SnapshotByID(*snap.ParentSnapshotID)
	}
	return out
}

func deltaVersion(meta table.Metadata, snap *table.Snapshot) int64 {
	if meta.Version() < 2 {
		return snap.TimestampMs
	}
	return snap.SequenceNumber
}

func (s *icebergSource) cached(key string) ([]dataFile, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	files, ok := s.files[key]
	return files, ok
}

func (s *icebergSource) remember(key string, files []dataFile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.files[key]; ok {
		return
	}
	if len(s.order) >= maxCachedSnapshots {
		delete(s.files, s.order[0])
		s.order = s.order[1:]
	}
	s.files[key] = files
	s.order = append(s.order, key)
}
