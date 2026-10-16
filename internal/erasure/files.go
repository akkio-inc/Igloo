package erasure

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// tableFiles is what a table's snapshots reference, read from its
// manifests: each manifest once, however many snapshots list it.
type tableFiles struct {
	tbl *table.Table
	// entries are each manifest's entries, live and deleted, by path.
	entries map[string][]entry
	// manifests are each snapshot's manifests, by snapshot ID.
	manifests map[int64][]string
}

type entry struct {
	status iceberg.ManifestEntryStatus
	file   iceberg.DataFile
}

func (e entry) live() bool { return e.status != iceberg.EntryStatusDELETED }

func readFiles(ctx context.Context, tbl *table.Table) (*tableFiles, error) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	tf := &tableFiles{tbl: tbl, entries: map[string][]entry{}, manifests: map[int64][]string{}}
	for _, snap := range tbl.Metadata().Snapshots() {
		manifests, err := snap.Manifests(fs)
		if err != nil {
			return nil, fmt.Errorf("read the manifests of snapshot %d: %w", snap.SnapshotID, err)
		}
		for _, m := range manifests {
			tf.manifests[snap.SnapshotID] = append(tf.manifests[snap.SnapshotID], m.FilePath())
			if _, ok := tf.entries[m.FilePath()]; ok {
				continue
			}
			var es []entry
			for e, err := range m.Entries(fs, false) {
				if err != nil {
					return nil, fmt.Errorf("read manifest %s: %w", m.FilePath(), err)
				}
				es = append(es, entry{status: e.Status(), file: e.DataFile()})
			}
			tf.entries[m.FilePath()] = es
		}
	}
	return tf, nil
}

// current is the current snapshot's ID, or 0 for a table without one.
func (tf *tableFiles) current() int64 {
	if s := tf.tbl.CurrentSnapshot(); s != nil {
		return s.SnapshotID
	}
	return 0
}

// mentions reports whether snapshot id's manifests list any of paths,
// live or deleted.
func (tf *tableFiles) mentions(id int64, paths map[string]bool) bool {
	for _, m := range tf.manifests[id] {
		for _, e := range tf.entries[m] {
			if paths[e.file.FilePath()] {
				return true
			}
		}
	}
	return false
}

// live returns the data and delete files live in any snapshot, or only in
// the current one.
func (tf *tableFiles) live(onlyCurrent bool) map[string]iceberg.DataFile {
	out := map[string]iceberg.DataFile{}
	for id, manifests := range tf.manifests {
		if onlyCurrent && id != tf.current() {
			continue
		}
		for _, m := range manifests {
			for _, e := range tf.entries[m] {
				if e.live() {
					out[e.file.FilePath()] = e.file
				}
			}
		}
	}
	return out
}

// referenced is every file the table's metadata references, as orphan
// cleanup counts them: metadata files, statistics, manifest lists,
// manifests, and the files live in them.
func (tf *tableFiles) referenced() map[string]bool {
	meta := tf.tbl.Metadata()
	out := map[string]bool{tf.tbl.MetadataLocation(): true}
	for e := range meta.PreviousFiles() {
		out[e.MetadataFile] = true
	}
	for sf := range meta.Statistics() {
		out[sf.StatisticsPath] = true
	}
	for sf := range meta.PartitionStatistics() {
		out[sf.StatisticsPath] = true
	}
	for _, s := range meta.Snapshots() {
		out[s.ManifestList] = true
	}
	for m := range tf.entries {
		out[m] = true
	}
	for p := range tf.live(false) {
		out[p] = true
	}
	return out
}

// files are the files that snapshots ids reference, apart from the
// metadata files of the table itself.
func (tf *tableFiles) files(ids []int64) map[string]bool {
	out := map[string]bool{}
	for _, id := range ids {
		if s := tf.tbl.SnapshotByID(id); s != nil {
			out[s.ManifestList] = true
		}
		for _, m := range tf.manifests[id] {
			out[m] = true
			for _, e := range tf.entries[m] {
				out[e.file.FilePath()] = true
			}
		}
	}
	for sf := range tf.tbl.Metadata().Statistics() {
		if slices.Contains(ids, sf.SnapshotID) {
			out[sf.StatisticsPath] = true
		}
	}
	return out
}

// kept are the files snapshots ids keep: their manifest lists, their
// manifests and the files live in them, and their statistics.
func (tf *tableFiles) kept(ids []int64) map[string]bool {
	out := map[string]bool{}
	for _, id := range ids {
		if s := tf.tbl.SnapshotByID(id); s != nil {
			out[s.ManifestList] = true
		}
		for _, m := range tf.manifests[id] {
			out[m] = true
			for _, e := range tf.entries[m] {
				if e.live() {
					out[e.file.FilePath()] = true
				}
			}
		}
	}
	for sf := range tf.tbl.Metadata().Statistics() {
		if slices.Contains(ids, sf.SnapshotID) {
			out[sf.StatisticsPath] = true
		}
	}
	return out
}

// field is a column that holds identifiers, as the table's schemas know
// it: by ID, which data files use, by every name it has had, and by its
// current name, which is empty if it was dropped.
type field struct {
	Column
	id      int
	typ     iceberg.Type
	names   []string
	current string
}

// fields finds t's columns in tbl's schemas, the current one first: a
// dropped column's values stay in older files.
func fields(tbl *table.Table, t *Table) []field {
	schemas := slices.Clone(tbl.Metadata().Schemas())
	slices.SortFunc(schemas, func(a, b *iceberg.Schema) int { return b.ID - a.ID })
	var out []field
	for _, c := range t.Columns {
		f := field{Column: c}
		if cur, ok := tbl.Schema().FindFieldByNameCaseInsensitive(c.Name); ok {
			f.id, f.typ, f.current = cur.ID, cur.Type, cur.Name
		} else {
			for _, s := range schemas {
				if old, ok := s.FindFieldByNameCaseInsensitive(c.Name); ok {
					f.id, f.typ = old.ID, old.Type
					break
				}
			}
		}
		if f.id == 0 {
			continue
		}
		for _, s := range schemas {
			if name, ok := s.FindColumnName(f.id); ok && !slices.Contains(f.names, name) {
				f.names = append(f.names, name)
			}
		}
		out = append(out, f)
	}
	return out
}

// binaryType reports whether a column holds bytes, which compare as hex.
func binaryType(t iceberg.Type) bool {
	switch t.(type) {
	case iceberg.BinaryType, iceberg.FixedType:
		return true
	}
	return false
}

// boundValue decodes a manifest bound, in Iceberg's single-value
// serialization, into the text a column value compares as.
func boundValue(t iceberg.Type, b []byte) (string, bool) {
	switch t.(type) {
	case iceberg.StringType:
		return string(b), true
	case iceberg.UUIDType:
		u, err := uuid.FromBytes(b)
		return u.String(), err == nil
	case iceberg.BinaryType, iceberg.FixedType:
		return hex.EncodeToString(b), true
	case iceberg.Int32Type:
		if len(b) == 4 {
			return strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(b))), 10), true
		}
	case iceberg.Int64Type:
		if len(b) == 8 {
			return strconv.FormatInt(int64(binary.LittleEndian.Uint64(b)), 10), true
		}
	}
	return "", false
}

// boundMatches returns the files whose manifest entries, live or deleted,
// have bounds of flds that may hold an identifier.
func (tf *tableFiles) boundMatches(m *matcher, flds []field) map[string]bool {
	out := map[string]bool{}
	for _, es := range tf.entries {
		for _, e := range es {
			for _, f := range flds {
				for _, bounds := range []map[int][]byte{e.file.LowerBoundValues(), e.file.UpperBoundValues()} {
					if v, ok := boundValue(f.typ, bounds[f.id]); ok && bounds[f.id] != nil && m.matchesBound(v) {
						out[e.file.FilePath()] = true
					}
				}
			}
		}
	}
	return out
}

// scannable reports whether a file of the table holds column values worth
// scanning for flds: data files, and equality delete files on one of them.
// Position deletes and deletion vectors hold only positions.
func scannable(df iceberg.DataFile, flds []field) bool {
	switch df.ContentType() {
	case iceberg.EntryContentData:
		return strings.EqualFold(string(df.FileFormat()), "PARQUET")
	case iceberg.EntryContentEqDeletes:
		return strings.EqualFold(string(df.FileFormat()), "PARQUET") &&
			slices.ContainsFunc(flds, func(f field) bool { return slices.Contains(df.EqualityFieldIDs(), f.id) })
	}
	return false
}

// tableName is a table's identifier as a request records it.
func tableName(ident table.Identifier) string { return strings.Join(ident, ".") }

// ident parses a name a request records.
func ident(name string) table.Identifier {
	schema, tbl, _ := strings.Cut(name, ".")
	return table.Identifier{schema, tbl}
}

// within reports whether p is under dir.
func within(p, dir string) bool {
	dir = strings.TrimSuffix(dir, "/") + "/"
	return strings.HasPrefix(p, dir)
}

// base is the file name of p, for logs.
func base(p string) string { return path.Base(p) }
