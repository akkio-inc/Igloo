package incremental

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/deletefiles"
)

// Change is what changed in a table between two of its snapshots, by data
// file: rows are added to a table in new data files, and leave it with the
// data files that held them or through position delete files.
type Change struct {
	// From and To are the snapshots.
	From, To int64
	// Added are the data files To has and From had not, whose rows To
	// reads (less those its delete files delete); Removed the data files
	// From had and To has not, whose rows From read.
	Added, Removed []string
	// Touched are data files both have, of which delete files added since
	// From delete rows: the rows From read of them and To does not.
	Touched []string
	// Rows counts the rows of the files of each list, by their metadata
	// (before deletes), and Total the rows To holds; Bytes counts the
	// files' bytes, and TotalBytes those of To's files.
	AddedRows, RemovedRows, TouchedRows, Total int64
	Bytes, TotalBytes                          int64
	// Reason, when set, says why the change cannot be read file by file,
	// so that a refresh reads the table whole.
	Reason string
}

// Empty reports whether nothing changed.
func (c Change) Empty() bool {
	return c.Reason == "" && len(c.Added) == 0 && len(c.Removed) == 0 && len(c.Touched) == 0
}

// AppendOnly reports whether the table only gained rows.
func (c Change) AppendOnly() bool { return len(c.Removed) == 0 && len(c.Touched) == 0 }

// Diff works out what changed in tbl, as named in reasons, between its
// snapshots from and to: to must descend from from. targets reads which
// data files position delete files delete rows of.
func Diff(ctx context.Context, tbl *table.Table, name string, from, to int64, targets *deletefiles.Targets) (Change, error) {
	c := Change{From: from, To: to}
	toSnap := tbl.SnapshotByID(to)
	if toSnap == nil {
		return c, fmt.Errorf("snapshot %d of %s is gone", to, name)
	}
	c.Total, c.TotalBytes = summaryInt(toSnap, "total-records"), summaryInt(toSnap, "total-files-size")
	if from == to {
		return c, nil
	}
	// The snapshots since from, newest first; every snapshot when from is
	// -1, a table that had none.
	var chain []*table.Snapshot
	for s := toSnap; s.SnapshotID != from; {
		chain = append(chain, s)
		if s.ParentSnapshotID == nil {
			s = nil
		} else {
			s = tbl.SnapshotByID(*s.ParentSnapshotID)
		}
		if s == nil && from < 0 && chain[len(chain)-1].ParentSnapshotID == nil {
			break
		}
		if s == nil {
			c.Reason = fmt.Sprintf("the snapshot of %s the last refresh read is no longer in its history (expired, or the table was rolled back or replaced)", name)
			return c, nil
		}
	}
	fromSnap := tbl.SnapshotByID(from)
	if fromSnap == nil && from >= 0 {
		c.Reason = fmt.Sprintf("the snapshot of %s the last refresh read is gone", name)
		return c, nil
	}
	if fromSnap != nil && fromSnap.SchemaID != nil && toSnap.SchemaID != nil && *fromSnap.SchemaID != *toSnap.SchemaID {
		c.Reason = fmt.Sprintf("the columns of %s changed", name)
		return c, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return c, err
	}
	added, removed := map[string]liveFile{}, map[string]liveFile{}
	cancelled := deletefiles.Paths{}
	var deletes, droppedDeletes []iceberg.DataFile
	for _, s := range slices.Backward(chain) {
		manifests, err := s.Manifests(fs)
		if err != nil {
			return c, fmt.Errorf("read the manifests of %s: %w", name, err)
		}
		for _, m := range manifests {
			// A snapshot's own manifests hold the files it added and
			// removed; the others carry earlier snapshots' files.
			if m.SnapshotID() != s.SnapshotID {
				continue
			}
			entries, err := m.FetchEntries(fs, false)
			if err != nil {
				return c, fmt.Errorf("read a manifest of %s: %w", name, err)
			}
			for _, e := range entries {
				if e.SnapshotID() != s.SnapshotID || e.Status() == iceberg.EntryStatusEXISTING {
					continue
				}
				df := e.DataFile()
				path := df.FilePath()
				switch df.ContentType() {
				case iceberg.EntryContentData:
					if format := strings.ToUpper(string(df.FileFormat())); format != "PARQUET" {
						c.Reason = fmt.Sprintf("%s has %s data files", name, format)
						return c, nil
					}
					if e.Status() == iceberg.EntryStatusADDED {
						added[path] = liveFile{path: path, rows: df.Count(), bytes: df.FileSizeBytes()}
						continue
					}
					if _, ok := added[path]; ok {
						delete(added, path)
						cancelled.Add(path)
						continue
					}
					removed[path] = liveFile{path: path, rows: df.Count(), bytes: df.FileSizeBytes()}
				case iceberg.EntryContentPosDeletes:
					if format := strings.ToUpper(string(df.FileFormat())); format != "PARQUET" {
						c.Reason = fmt.Sprintf("%s has deletion vectors", name)
						return c, nil
					}
					if e.Status() == iceberg.EntryStatusADDED {
						deletes = append(deletes, df)
					} else {
						droppedDeletes = append(droppedDeletes, df)
					}
				default:
					c.Reason = fmt.Sprintf("%s has equality delete files", name)
					return c, nil
				}
			}
		}
	}
	c.Added, c.Removed = slices.Sorted(maps.Keys(added)), slices.Sorted(maps.Keys(removed))
	for _, p := range c.Added {
		c.AddedRows += added[p].rows
		c.Bytes += added[p].bytes
	}
	for _, p := range c.Removed {
		c.RemovedRows += removed[p].rows
		c.Bytes += removed[p].bytes
	}
	if len(deletes) == 0 && len(droppedDeletes) == 0 {
		return c, nil
	}
	// The data files of from the new delete files delete rows of, and that
	// to still has: those it added it reads with their deletes, those it
	// removed are read whole as from read them.
	live := map[string]liveFile{}
	if fromSnap != nil {
		if live, err = liveFiles(fs, fromSnap); err != nil {
			return c, fmt.Errorf("list the data files of %s: %w", name, err)
		}
	}
	gone := deletefiles.Paths{}
	for _, p := range c.Removed {
		gone.Add(p)
	}
	touched := map[string]bool{}
	for _, d := range deletes {
		refs, err := targets.Of(ctx, fs, d)
		if err != nil {
			return c, fmt.Errorf("read a delete file of %s: %w", name, err)
		}
		for ref := range refs {
			k := deletefiles.Canonical(ref)
			if f, ok := live[k]; ok && !gone.Has(ref) && !cancelled.Has(ref) {
				touched[f.path] = true
			}
		}
	}
	// A delete file that goes while a data file it deleted rows of stays
	// would bring rows back, which a refresh reads only by reading the
	// table whole.
	for _, d := range droppedDeletes {
		refs, err := targets.Of(ctx, fs, d)
		if err != nil {
			return c, fmt.Errorf("read a delete file of %s: %w", name, err)
		}
		for ref := range refs {
			if _, ok := live[deletefiles.Canonical(ref)]; ok && !gone.Has(ref) {
				c.Reason = fmt.Sprintf("delete files of %s were rewritten", name)
				return c, nil
			}
		}
	}
	c.Touched = slices.Sorted(maps.Keys(touched))
	for _, p := range c.Touched {
		f := live[deletefiles.Canonical(p)]
		// Read twice: as From and To have them.
		c.TouchedRows += f.rows
		c.Bytes += 2 * f.bytes
	}
	return c, nil
}

// liveFile is a data file of a snapshot, as its manifest spells it.
type liveFile struct {
	path        string
	rows, bytes int64
}

// liveFiles returns the data files of snap, by Canonical path.
func liveFiles(fs iceio.IO, snap *table.Snapshot) (map[string]liveFile, error) {
	out := map[string]liveFile{}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, err
	}
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		entries, err := m.FetchEntries(fs, true)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			df := e.DataFile()
			out[deletefiles.Canonical(df.FilePath())] = liveFile{path: df.FilePath(), rows: df.Count(), bytes: df.FileSizeBytes()}
		}
	}
	return out, nil
}

// Size returns the rows and the data files' bytes of tbl's current
// snapshot: its summary's, or, where the summary lacks them, as DuckDB
// writes it, its manifests'.
func Size(tbl *table.Table) (rows, bytes int64, err error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0, 0, nil
	}
	rows, bytes = summaryInt(snap, "total-records"), summaryInt(snap, "total-files-size")
	if rows >= 0 && bytes >= 0 {
		return rows, bytes, nil
	}
	fs, err := tbl.FS(context.Background())
	if err != nil {
		return 0, 0, err
	}
	live, err := liveFiles(fs, snap)
	if err != nil {
		return 0, 0, err
	}
	rows, bytes = 0, 0
	for _, f := range live {
		rows, bytes = rows+f.rows, bytes+f.bytes
	}
	return rows, bytes, nil
}

// summaryInt reads an integer of a snapshot's summary, or -1.
func summaryInt(s *table.Snapshot, key string) int64 {
	if s == nil || s.Summary == nil {
		return -1
	}
	n, err := strconv.ParseInt(s.Summary.Properties[key], 10, 64)
	if err != nil {
		return -1
	}
	return n
}
