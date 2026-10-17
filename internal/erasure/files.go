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
func (tf *tableFiles) mentions(id int64, paths pathSet) bool {
	for _, m := range tf.manifests[id] {
		for _, e := range tf.entries[m] {
			if paths.has(e.file.FilePath()) {
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
func (tf *tableFiles) referenced() pathSet {
	meta := tf.tbl.Metadata()
	out := pathSet{}
	out.add(tf.tbl.MetadataLocation())
	for e := range meta.PreviousFiles() {
		out.add(e.MetadataFile)
	}
	for sf := range meta.Statistics() {
		out.add(sf.StatisticsPath)
	}
	for sf := range meta.PartitionStatistics() {
		out.add(sf.StatisticsPath)
	}
	for _, s := range meta.Snapshots() {
		out.add(s.ManifestList)
	}
	for m := range tf.entries {
		out.add(m)
	}
	for p := range tf.live(false) {
		out.add(p)
	}
	return out
}

// files are the files that snapshots ids reference, apart from the
// metadata files of the table itself.
func (tf *tableFiles) files(ids []int64) pathSet {
	out := pathSet{}
	for _, id := range ids {
		if s := tf.tbl.SnapshotByID(id); s != nil {
			out.add(s.ManifestList)
		}
		for _, m := range tf.manifests[id] {
			out.add(m)
			for _, e := range tf.entries[m] {
				out.add(e.file.FilePath())
			}
		}
	}
	for sf := range tf.tbl.Metadata().Statistics() {
		if slices.Contains(ids, sf.SnapshotID) {
			out.add(sf.StatisticsPath)
		}
	}
	return out
}

// kept are the files snapshots ids keep: their manifest lists, their
// manifests and the files live in them, and their statistics.
func (tf *tableFiles) kept(ids []int64) pathSet {
	out := pathSet{}
	for _, id := range ids {
		if s := tf.tbl.SnapshotByID(id); s != nil {
			out.add(s.ManifestList)
		}
		for _, m := range tf.manifests[id] {
			out.add(m)
			for _, e := range tf.entries[m] {
				if e.live() {
					out.add(e.file.FilePath())
				}
			}
		}
	}
	for sf := range tf.tbl.Metadata().Statistics() {
		if slices.Contains(ids, sf.SnapshotID) {
			out.add(sf.StatisticsPath)
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
	// required says the current schema requires a value.
	required bool
}

// fields finds t's columns in tbl's schemas: every field that has had a
// column's name in any of them, as a column renamed, or dropped and added
// again, keeps its values in older files under its ID.
func fields(tbl *table.Table, t *Table) []field {
	schemas := slices.Clone(tbl.Metadata().Schemas())
	slices.SortFunc(schemas, func(a, b *iceberg.Schema) int { return b.ID - a.ID })
	var out []field
	for _, c := range t.Columns {
		var ids []int
		for _, s := range schemas {
			if f, ok := s.FindFieldByNameCaseInsensitive(c.Name); ok && !slices.Contains(ids, f.ID) {
				ids = append(ids, f.ID)
			}
		}
		for _, id := range ids {
			if slices.ContainsFunc(out, func(f field) bool { return f.id == id && f.Column == c }) {
				continue
			}
			f := field{Column: c, id: id}
			if cur, ok := tbl.Schema().FindFieldByID(id); ok {
				f.typ, f.required = cur.Type, cur.Required
				f.current, _ = tbl.Schema().FindColumnName(id)
			}
			for _, s := range schemas {
				if name, ok := s.FindColumnName(id); ok && !slices.Contains(f.names, name) {
					f.names = append(f.names, name)
				}
				if old, ok := s.FindFieldByID(id); ok && f.typ == nil {
					f.typ = old.Type
				}
			}
			out = append(out, f)
		}
	}
	return out
}

// resolved checks that each of t's tagged columns is a field of its table,
// as one of its schemas names it: a column that none has, as after the
// table was replaced by one that names it otherwise, would leave its
// values where they are while the request completed.
func resolved(t *Table, flds []field) error {
	for _, c := range t.Columns {
		if !slices.ContainsFunc(flds, func(f field) bool { return f.Column == c }) {
			return fmt.Errorf("%w: column %s of %s is tagged %s = '%s', but the table has no column of that name, and never had: "+
				"if it was renamed, or the table replaced, tag the column that holds the identifiers now and unset the old tag",
				ErrInvalid, c.Name, t.Name, c.Tag, c.Value)
		}
	}
	return nil
}

// rewritable refuses a table the request could not rewrite: one of
// format version 3, whose deletion vectors iceberg-go cannot read. The
// request would delete its rows and then be unable to rewrite the files
// that still hold them, nor be canceled, and after failing too often
// forget whose rows they were, with them still in the files.
func rewritable(t *Table, tbl *table.Table) error {
	if v := tbl.Metadata().Version(); v >= 3 {
		return fmt.Errorf("%w: %s is an Iceberg table of format version %d, which can hold deletion vectors that Igloo cannot read yet, "+
			"so it could delete the rows but not rewrite the files that hold them; erase from it in an engine that reads them, "+
			"and untag it until then", ErrInvalid, t.Name, v)
	}
	return nil
}

// identifierType reports whether values of type t compare as identifiers do:
// strings, UUIDs, binary values and integers. Lists, structs and maps hold
// identifiers inside values, which a request does not reach.
func identifierType(t iceberg.Type) bool {
	switch t.(type) {
	case iceberg.StringType, iceberg.UUIDType, iceberg.BinaryType, iceberg.FixedType, iceberg.Int32Type, iceberg.Int64Type:
		return true
	}
	return false
}

// checkFields refuses columns a request cannot erase from as asked.
func checkFields(t *Table, flds []field) error {
	for _, f := range flds {
		if !identifierType(f.typ) {
			return fmt.Errorf("%w: column %s of %s is a %s, and only strings, UUIDs, binary values and integers can be compared with identifiers; "+
				"unset its tag %s, and tag a column that holds the identifiers themselves", ErrInvalid, f.Name, t.Name, f.typ, f.Tag)
		}
		if f.Null && f.required {
			return fmt.Errorf("%w: column %s of %s is tagged %s.%s%s, but it is NOT NULL, so it cannot be set to NULL",
				ErrInvalid, f.current, t.Name, f.Tag, f.Value, nullSuffix)
		}
	}
	return nil
}

// typeFamily is the kind of values a column of type t holds, as
// identifiers compare with them: text, a UUID, bytes or an integer.
func typeFamily(t iceberg.Type) string {
	switch t.(type) {
	case iceberg.UUIDType:
		return "uuid"
	case iceberg.BinaryType, iceberg.FixedType:
		return "bytes"
	case iceberg.Int32Type, iceberg.Int64Type:
		return "integer"
	}
	return "text"
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
// scanning: data files, and equality delete files, which may hold whole
// rows (as Flink's do) whatever columns they delete by. Position deletes
// and deletion vectors hold only positions.
func scannable(df iceberg.DataFile) bool {
	switch df.ContentType() {
	case iceberg.EntryContentData, iceberg.EntryContentEqDeletes:
		return strings.EqualFold(string(df.FileFormat()), "PARQUET")
	}
	return false
}

// tableName is a table's identifier as a request shows it: its namespace
// and name joined with dots.
func tableName(ident table.Identifier) string { return strings.Join(ident, ".") }

// identOf is the identifier of a request's table.
func identOf(t *Table) table.Identifier {
	if len(t.Ident) > 0 {
		return t.Ident
	}
	// Names of namespaces have no dots.
	return strings.Split(t.Name, ".")
}

// base is the file name of p, for logs.
func base(p string) string { return path.Base(p) }
