package erasure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"gocloud.dev/gcerrors"

	"github.com/akkio-inc/igloo/internal/policy"
)

const (
	// orphanAge is how old an unreferenced file must be before a request
	// deletes it, as table maintenance waits: younger ones may belong to a
	// write that has not committed yet.
	orphanAge = 72 * time.Hour
	// maxRewriteBytes bounds the data one rewrite commit replaces, as for
	// compaction: a commit that loses to a writer costs at most this.
	maxRewriteBytes = 8 << 30
	// commitAttempts bounds the commits a step tries per table in one run.
	commitAttempts = 10
	// sharingDir holds Delta Sharing's copies of data files with their
	// deletes applied, under the warehouse root, by table UUID.
	sharingDir = ".igloo/sharing/"
	// A request that would delete more than holdFraction of a table's
	// rows, and more than holdRows of them, waits to be confirmed: a
	// mistaken or placeholder identifier matches much of a table.
	holdFraction = 0.01
	holdRows     = 100
)

// Kinds of files a request deletes, or leaves.
const (
	fileMatched = "matched"
	fileExpired = "expired"
	fileOrphan  = "orphan"
	fileSharing = "sharing"
	// fileOutside are files outside the warehouse, which a request leaves:
	// they are someone else's, as files added from a landing bucket are.
	fileOutside = "outside"
)

// operation marks the snapshots requests commit.
var operation = iceberg.Properties{"igloo.operation": "erase"}

// reload loads a request's table, which must still exist: a table that a
// request found and that is gone was dropped or renamed meanwhile, and
// where its data went cannot be told.
func (s *Service) reload(ctx context.Context, t *Table) (*table.Table, error) {
	tbl, err := s.opts.Catalog.LoadTable(ctx, identOf(t))
	if errors.Is(err, icecat.ErrNoSuchTable) {
		return nil, fmt.Errorf("%s does not exist any more: it was dropped or renamed while the request ran; "+
			"tag it under its new name if it has one, and make the request again", t.Name)
	}
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", t.Name, err)
	}
	if id := tbl.Metadata().TableUUID().String(); t.UUID != id {
		if t.UUID != "" {
			// Replaced, as CREATE OR REPLACE TABLE does: what has the
			// name now holds what the old table held.
			s.log.Info("an erasure request's table was replaced; it erases from the new one", "table", t.Name)
		}
		t.UUID = id
	}
	return tbl, nil
}

// loaded is a request's table as the catalog has it now.
type loaded struct {
	t   *Table
	tbl *table.Table
}

// tables loads the request's tables, but connections'.
func (s *Service) tables(ctx context.Context, r *Request) ([]loaded, error) {
	var out []loaded
	for i := range r.Tables {
		if r.Tables[i].Remote {
			continue
		}
		tbl, err := s.reload(ctx, &r.Tables[i])
		if err != nil {
			return nil, err
		}
		out = append(out, loaded{&r.Tables[i], tbl})
	}
	return out, nil
}

// find works out the tables r erases from (see discover), and holds a
// request that would delete much of one until it is confirmed.
func (s *Service) find(ctx context.Context, r *Request) error {
	if err := s.discover(ctx, r); err != nil {
		return err
	}
	if r.ConfirmedBy == "" {
		for _, t := range r.Tables {
			if t.Matched > holdRows && float64(t.Matched) > holdFraction*float64(t.Rows) {
				s.log.Warn("erasure request held: it would delete much of a table; CONFIRM ERASURE REQUEST to go on",
					"request", r.ID, "table", t.Name, "rows", t.Matched, "of", t.Rows)
				r.Status = StatusHeld
				break
			}
		}
	}
	return nil
}

// discover works out the tables r erases from: those with tagged columns,
// and those that share data files with them, and counts their rows and
// those that match.
func (s *Service) discover(ctx context.Context, r *Request) error {
	snap, err := s.opts.Policies(ctx)
	if err != nil {
		return err
	}
	tables, err := s.tagged(snap, r.Subjects)
	if err != nil {
		return err
	}
	if !slices.ContainsFunc(tables, func(t Table) bool { return !t.Remote }) {
		// The tags were unset since the request was made: completing
		// it would erase nothing.
		return errors.New("no column is tagged with the request's tag values any more; tag the columns again")
	}
	r.Tables = tables
	for i := range r.Tables {
		t := &r.Tables[i]
		if t.Remote {
			continue
		}
		if _, err := s.opts.Catalog.LoadTable(ctx, identOf(t)); errors.Is(err, icecat.ErrNoSuchTable) {
			return fmt.Errorf("%s is tagged but does not exist: tags stay with a table's name, so if it was renamed, "+
				"tag it under its new name; if it was dropped, unset its tags", t.Name)
		}
	}
	sources, err := s.tables(ctx, r)
	if err != nil {
		return err
	}
	for _, l := range sources {
		if err := checkFields(l.t, fields(l.tbl, l.t)); err != nil {
			return err
		}
	}
	clones, err := s.clones(ctx, r, sources)
	if err != nil {
		return err
	}
	r.Tables = append(r.Tables, clones...)
	if r.Downstream, err = s.downstream(ctx, r); err != nil {
		return fmt.Errorf("lineage: %w", err)
	}
	return s.count(ctx, r)
}

// tagged are the tables with columns tagged with subjects' tag values, by
// name.
func (s *Service) tagged(snap *policy.Snapshot, subjects []Subject) ([]Table, error) {
	byName := map[string]*Table{}
	for _, ct := range snap.Catalog.ColumnTags {
		for _, sub := range subjects {
			digest, null, ok := holds(ct.Value, sub.Value)
			if ct.Tag != sub.Tag || !ok {
				continue
			}
			name := policy.TableName(ct.Catalog, ct.Schema, ct.Table)
			key := strings.ToLower(name)
			if byName[key] == nil {
				byName[key] = &Table{Name: name, Ident: []string{ct.Schema, ct.Table}, Reason: "tagged"}
				if ct.Catalog != "" {
					byName[key].Ident = nil
					byName[key].Reason = "a table of connection " + ct.Catalog + ", which Igloo reads but cannot erase from: erase there"
					byName[key].Remote = true
				}
			}
			byName[key].Columns = append(byName[key].Columns, Column{Name: ct.Column, Tag: ct.Tag, Value: sub.Value, Digest: digest, Null: null})
		}
	}
	var out []Table
	for _, key := range slices.Sorted(maps.Keys(byName)) {
		out = append(out, *byName[key])
	}
	return out, nil
}

// count counts the tables' rows, and those that match.
func (s *Service) count(ctx context.Context, r *Request) error {
	tables, err := s.tables(ctx, r)
	if err != nil {
		return err
	}
	for _, l := range tables {
		cond := currentCondition(r.salt, fields(l.tbl, l.t), r.Subjects, true)
		if cond == "" {
			continue
		}
		if l.t.Matched, err = s.rows.Count(ctx, l.tbl, cond); err != nil {
			return fmt.Errorf("count the matching rows of %s: %w", l.t.Name, err)
		}
		l.t.Rows = rowCount(l.tbl)
	}
	return nil
}

// rowCount is how many rows a table's current snapshot has, as its
// summary says, less its position deletes: about as many as it has.
func rowCount(tbl *table.Table) int64 {
	snap := tbl.CurrentSnapshot()
	if snap == nil || snap.Summary == nil {
		return 0
	}
	n, _ := strconv.ParseInt(snap.Summary.Properties["total-records"], 10, 64)
	d, _ := strconv.ParseInt(snap.Summary.Properties["total-position-deletes"], 10, 64)
	return max(n-d, 0)
}

// Properties of a zero-copy clone (see package clone): sharesProperty
// lists, as a JSON array, the locations of other tables whose files its
// metadata may reference, the table it was cloned from and those that one
// shares files with in turn; sourceProperty names the table it was cloned
// from, and stays once it references none of that table's files.
const (
	sharesProperty = "igloo.clone.shares-files-of"
	sourceProperty = "igloo.clone.source"
)

// shares are the locations whose files tbl says it may reference.
func shares(tbl *table.Table) []string {
	var locs []string
	if v := tbl.Properties()[sharesProperty]; v != "" {
		_ = json.Unmarshal([]byte(v), &locs)
	}
	for i, l := range locs {
		locs[i] = strings.TrimRight(l, "/")
	}
	return locs
}

// uuids are the UUIDs of the request's tables that it loaded.
func uuids(tables []loaded) map[string]bool {
	out := map[string]bool{}
	for _, l := range tables {
		out[l.tbl.Metadata().TableUUID().String()] = true
	}
	return out
}

// others loads the catalog's tables that are not among ours. A table that
// cannot be loaded may share files, so that fails the step.
func (s *Service) others(ctx context.Context, ours map[string]bool) ([]*table.Table, error) {
	idents, err := s.opts.Catalog.Tables(ctx)
	if err != nil {
		return nil, err
	}
	var out []*table.Table
	for _, id := range idents {
		tbl, err := s.opts.Catalog.LoadTable(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("load %s, which may share files with the request's tables: %w", tableName(id), err)
		}
		if !ours[tbl.Metadata().TableUUID().String()] {
			out = append(out, tbl)
		}
	}
	return out, nil
}

// clones finds the tables that share data files with sources, which a
// file must be rewritten in, and its snapshots expired in, before it can
// go: zero-copy clones of them, the tables they are clones of, and so on.
// A clone says what it was made from and shares, and its columns are the
// source's, by field ID; a clone that no longer shares files still holds
// copies of its source's rows. Tables that reference another's files
// without saying so are found by their manifests, among those with a
// column named like a tagged one, and their columns of those names are
// taken to hold the same.
func (s *Service) clones(ctx context.Context, r *Request, sources []loaded) ([]Table, error) {
	others, err := s.others(ctx, uuids(sources))
	if err != nil {
		return nil, err
	}
	var out []Table
	read := map[*table.Table]*tableFiles{}
	for len(sources) > 0 {
		var found []loaded
		for _, src := range sources {
			for i := 0; i < len(others); i++ {
				o := others[i]
				t, err := s.related(ctx, src, o, read)
				if err != nil {
					return nil, err
				}
				if t == nil {
					continue
				}
				out = append(out, *t)
				found = append(found, loaded{&out[len(out)-1], o})
				others = slices.Delete(others, i, i+1)
				i--
			}
		}
		// Clones of clones, and their sources, share files too.
		sources = found
	}
	return out, nil
}

// related returns other as a table the request erases from too, if it
// shares data files with src, or nil.
func (s *Service) related(ctx context.Context, src loaded, other *table.Table, read map[*table.Table]*tableFiles) (*Table, error) {
	name := tableName(other.Identifier())
	t := &Table{Name: name, Ident: other.Identifier(), UUID: other.Metadata().TableUUID().String()}
	srcLoc, otherLoc := strings.TrimRight(src.tbl.Location(), "/"), strings.TrimRight(other.Location(), "/")
	switch {
	case slices.Contains(shares(other), srcLoc) || strings.EqualFold(other.Properties()[sourceProperty], src.t.Name):
		t.Columns, t.Reason = sameFields(src, other), "a clone of "+src.t.Name
		return t, nil
	case slices.Contains(shares(src.tbl), otherLoc) || strings.EqualFold(src.tbl.Properties()[sourceProperty], name):
		t.Columns, t.Reason = sameFields(src, other), src.t.Name+" is a clone of it"
		return t, nil
	}
	for _, c := range src.t.Columns {
		if f, ok := other.Schema().FindFieldByNameCaseInsensitive(c.Name); ok {
			c.Name = f.Name
			t.Columns = append(t.Columns, c)
		}
	}
	if len(t.Columns) == 0 {
		return nil, nil
	}
	tf := read[other]
	if tf == nil {
		var err error
		if tf, err = readFiles(ctx, other); err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		read[other] = tf
	}
	if slices.ContainsFunc(slices.Collect(maps.Keys(tf.live(false))), func(p string) bool { return within(p, srcLoc) }) {
		t.Reason = "shares data files with " + src.t.Name
		return t, nil
	}
	return nil, nil
}

// sameFields are src's tagged columns as other names them: the fields
// with the same IDs, as a clone's metadata copies its source's.
func sameFields(src loaded, other *table.Table) []Column {
	var out []Column
	for _, f := range fields(src.tbl, src.t) {
		c := f.Column
		for _, sc := range append([]*iceberg.Schema{other.Schema()}, other.Metadata().Schemas()...) {
			if n, ok := sc.FindColumnName(f.id); ok {
				c.Name = n
				break
			}
		}
		if !slices.Contains(out, c) {
			out = append(out, c)
		}
	}
	return out
}

// deleteRows deletes the matching rows of every table, as a DELETE does:
// queries no longer see them, but their files still hold them. Columns to
// be nulled are nulled when the files are rewritten.
func (s *Service) deleteRows(ctx context.Context, r *Request) error {
	tables, err := s.tables(ctx, r)
	if err != nil {
		return err
	}
	for _, l := range tables {
		if cond := currentCondition(r.salt, fields(l.tbl, l.t), r.Subjects, false); cond != "" {
			n, err := s.rows.Delete(ctx, l.tbl, cond)
			if err != nil {
				return fmt.Errorf("delete from %s: %w", l.t.Name, err)
			}
			l.t.RowsDeleted += n
		}
		l.t.done(StepDelete, s.now())
		if err := s.save(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// rewrite removes the matching rows from the files of every branch and tag
// of every table (see rewrite.go), and then the manifests that still list
// the old files.
func (s *Service) rewrite(ctx context.Context, r *Request) error {
	tables, err := s.tables(ctx, r)
	if err != nil {
		return err
	}
	for _, l := range tables {
		t := l.t
		for _, name := range refNames(l.tbl) {
			if err := s.onRef(ctx, r, t, name, func(ctx context.Context, view *table.Table, branch string, commit bool) (bool, error) {
				return s.rewriteRef(ctx, r, t, view, branch, commit)
			}); err != nil {
				return fmt.Errorf("rewrite %s: %w", t.Name, err)
			}
		}
		t.done(StepRewrite, s.now())
		if err := s.save(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// readCurrent reads the current snapshot's manifests only.
func readCurrent(ctx context.Context, tbl *table.Table) (*tableFiles, error) {
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	tf := &tableFiles{tbl: tbl, entries: map[string][]entry{}, manifests: map[int64][]string{}}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return tf, nil
	}
	manifests, err := snap.Manifests(fsys)
	if err != nil {
		return nil, err
	}
	for _, m := range manifests {
		tf.manifests[snap.SnapshotID] = append(tf.manifests[snap.SnapshotID], m.FilePath())
		for e, err := range m.Entries(fsys, false) {
			if err != nil {
				return nil, err
			}
			tf.entries[m.FilePath()] = append(tf.entries[m.FilePath()], entry{status: e.Status(), file: e.DataFile()})
		}
	}
	return tf, nil
}

// matched are the files the request found matching rows in, in any of its
// tables: a clone references its source's files.
func (s *Service) matched(ctx context.Context, r *Request) (map[string]bool, error) {
	files, err := s.opts.Store.Files(ctx, r.ID)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, f := range files {
		if f.Kind == fileMatched || f.Kind == fileOutside {
			out[f.Path] = true
		}
	}
	return out, nil
}

// record notes files of t to delete once nothing references them. Files
// outside the warehouse are noted to be left, and logged: whoever keeps
// them deletes them.
func (s *Service) record(ctx context.Context, r *Request, t *Table, kind string, paths []string) error {
	known, err := s.opts.Store.Files(ctx, r.ID)
	if err != nil {
		return err
	}
	var files []File
	for _, p := range paths {
		f := File{Path: p, Table: t.Name, Kind: kind}
		if !within(p, s.opts.Warehouse) {
			f.Kind = fileOutside
			if !slices.ContainsFunc(known, func(k File) bool { return k.Path == p }) {
				t.FilesOutside++
				s.log.Warn("an erasure request leaves a file outside the warehouse that held erased data or was referenced only by snapshots it expired; "+
					"delete it where it is", "request", r.ID, "table", t.Name, "path", p)
			}
		}
		files = append(files, f)
	}
	return s.opts.Store.AddFiles(ctx, r.ID, files)
}

// listings lists the files under the locations of a request's tables,
// each once.
type listings struct {
	fsys      iceio.IO
	locations []string
	listed    map[string]map[string]time.Time
}

func newListings(ctx context.Context, tables []loaded) (*listings, error) {
	l := &listings{listed: map[string]map[string]time.Time{}}
	for _, t := range tables {
		l.locations = append(l.locations, t.tbl.Location())
	}
	if len(tables) > 0 {
		fsys, err := tables[0].tbl.FS(ctx)
		if err != nil {
			return nil, err
		}
		l.fsys = fsys
	}
	return l, nil
}

// list lists the files under dir.
func (l *listings) list(dir string) (map[string]time.Time, error) {
	if files, ok := l.listed[dir]; ok {
		return files, nil
	}
	files, err := listFiles(l.fsys, dir)
	if err != nil {
		return nil, err
	}
	l.listed[dir] = files
	return files, nil
}

// exists reports whether p, under the location of one of the tables, is
// in storage. Files elsewhere count as gone.
func (l *listings) exists(p string) (bool, error) {
	for _, loc := range l.locations {
		if within(p, loc) {
			files, err := l.list(loc)
			if err != nil {
				return false, err
			}
			if _, ok := files[p]; ok {
				return true, nil
			}
		}
	}
	return false, nil
}

// bad are the files of a table that held identifiers: those the request
// found matching rows in, those older snapshots reference that do, and
// those whose manifest bounds may hold one, unless a scan of them found
// none. It scans the files older snapshots reference that it has not yet,
// and records what it finds.
func (s *Service) bad(ctx context.Context, r *Request, t *Table, tf *tableFiles, flds []field, l *listings) (map[string]bool, error) {
	known, err := s.matched(ctx, r)
	if err != nil {
		return nil, err
	}
	scanned := map[string]bool{}
	var paths []string
	for p, df := range tf.live(false) {
		if !scannable(df) {
			continue
		}
		if exists, err := l.exists(p); err != nil {
			return nil, err
		} else if exists {
			scanned[p] = true
			if !known[p] {
				paths = append(paths, p)
			}
		}
	}
	matched, err := s.matchingFiles(ctx, r.salt, paths, scanColumns(flds, r.Subjects))
	if err != nil {
		return nil, err
	}
	found := slices.Collect(maps.Keys(matched))
	for p := range tf.boundMatches(newMatcher(r), flds) {
		if !scanned[p] && !known[p] {
			found = append(found, p)
		}
	}
	if err := s.record(ctx, r, t, fileMatched, found); err != nil {
		return nil, err
	}
	for _, p := range found {
		known[p] = true
	}
	return known, nil
}

// expire expires, in every table, the snapshots that reference files that
// held identifiers, however recent: time travel to them ends. Snapshots
// that reference none stay, older ones included, and so do the snapshots
// branches and tags point at, which the rewrite step made reference none.
func (s *Service) expire(ctx context.Context, r *Request) error {
	tables, err := s.tables(ctx, r)
	if err != nil {
		return err
	}
	l, err := newListings(ctx, tables)
	if err != nil {
		return err
	}
	for _, lt := range tables {
		t := lt.t
		for attempt := 1; ; attempt++ {
			n, err := s.expireTable(ctx, r, t, l)
			if errors.Is(err, table.ErrCommitFailed) && attempt < commitAttempts {
				continue
			}
			if err != nil {
				return fmt.Errorf("expire snapshots of %s: %w", t.Name, err)
			}
			t.SnapshotsExpired += n
			break
		}
		t.done(StepExpire, s.now())
		if err := s.save(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) expireTable(ctx context.Context, r *Request, t *Table, l *listings) (int, error) {
	tbl, err := s.reload(ctx, t)
	if err != nil {
		return 0, err
	}
	tf, err := readFiles(ctx, tbl)
	if err != nil {
		return 0, err
	}
	bad, err := s.bad(ctx, r, t, tf, fields(tbl, t), l)
	if err != nil {
		return 0, err
	}
	// A ref whose snapshot still mentions such a file, as one whose bounds
	// only now told, is rewritten first.
	for name, ref := range tbl.Metadata().Refs() {
		if tf.mentions(ref.SnapshotID, bad) {
			if err := s.onRef(ctx, r, t, name, func(ctx context.Context, view *table.Table, branch string, commit bool) (bool, error) {
				return s.rewriteRef(ctx, r, t, view, branch, commit)
			}); err != nil {
				return 0, err
			}
		}
	}
	if tbl, err = s.reload(ctx, t); err != nil {
		return 0, err
	}
	if tf, err = readFiles(ctx, tbl); err != nil {
		return 0, err
	}
	heads := map[int64]bool{}
	var reqs []table.Requirement
	for name, ref := range tbl.Metadata().Refs() {
		if tf.mentions(ref.SnapshotID, bad) {
			return 0, fmt.Errorf("%s still references a file that held matching rows", name)
		}
		heads[ref.SnapshotID] = true
		id := ref.SnapshotID
		reqs = append(reqs, table.AssertRefSnapshotID(name, &id))
	}
	var expired, retained []int64
	for _, snap := range tbl.Metadata().Snapshots() {
		if !heads[snap.SnapshotID] && tf.mentions(snap.SnapshotID, bad) {
			expired = append(expired, snap.SnapshotID)
		} else {
			retained = append(retained, snap.SnapshotID)
		}
	}
	if len(expired) == 0 {
		return 0, nil
	}
	// What only the expired snapshots reference goes once they do.
	kept := tf.kept(retained)
	var gone []string
	for p := range tf.files(expired) {
		if !kept[p] {
			gone = append(gone, p)
		}
	}
	if err := s.record(ctx, r, t, fileExpired, gone); err != nil {
		return 0, err
	}
	// Every ref is asserted: a commit to any of them meanwhile could make
	// an expired snapshot its parent.
	if err := s.commitRefs(ctx, tbl, reqs, table.NewRemoveSnapshotsUpdate(expired, false)); err != nil {
		return 0, err
	}
	s.log.Info("expired snapshots holding erased data", "request", r.ID, "table", t.Name, "snapshots", len(expired))
	return len(expired), nil
}

// purge deletes from object storage the files the request recorded, once
// none of its tables references them, and the tables' orphan files old
// enough to be safe to delete. A newer orphan that holds matching rows
// makes the request wait until it is old enough. It holds the locks of the
// tables' locations meanwhile, which making a clone of one takes too, so
// no clone that references a file it deletes is made.
func (s *Service) purge(ctx context.Context, r *Request) error {
	release, err := s.lockLocations(ctx, r)
	if err != nil {
		return err
	}
	defer release()
	tables, referenced, err := s.referenced(ctx, r)
	if err != nil {
		return err
	}
	files, err := s.opts.Store.Files(ctx, r.ID)
	if err != nil {
		return err
	}
	var gone []File
	going := map[string]bool{}
	for _, f := range files {
		if f.Deleted.IsZero() && f.Kind != fileOutside && !referenced[f.Path] {
			gone, going[f.Path] = append(gone, f), true
		}
	}
	var waitUntil time.Time
	for _, l := range tables {
		old, young, err := s.orphans(ctx, l.tbl, referenced)
		if err != nil {
			return fmt.Errorf("orphan files of %s: %w", l.t.Name, err)
		}
		for _, p := range old {
			if !going[p] {
				gone = append(gone, File{Path: p, Table: l.t.Name, Kind: fileOrphan})
			}
		}
		maps.DeleteFunc(young, func(p string, _ time.Time) bool { return going[p] })
		matched, err := s.matchingFiles(ctx, r.salt, parquet(slices.Collect(maps.Keys(young))), scanColumns(fields(l.tbl, l.t), r.Subjects))
		if err != nil {
			return err
		}
		for p := range matched {
			if at := young[p].Add(orphanAge); waitUntil.IsZero() || at.Before(waitUntil) {
				waitUntil = at
			}
		}
	}
	if err := s.unshared(ctx, r, tables, gone); err != nil {
		return err
	}
	if err := s.delete(ctx, r, tables, gone); err != nil {
		return err
	}
	for _, l := range tables {
		l.t.done(StepPurge, s.now())
	}
	if !waitUntil.IsZero() {
		s.log.Info("erasure waits until a recent unreferenced file with matching rows is old enough to delete",
			"request", r.ID, "until", waitUntil)
		r.wait(waitUntil)
	}
	return nil
}

// unshared checks that no table the request does not erase from
// references one of files, as a table that shares files with one of its
// tables without saying so, or by columns of other names, may: deleting
// the file would break that table, which holds the rows too.
func (s *Service) unshared(ctx context.Context, r *Request, tables []loaded, files []File) error {
	if len(files) == 0 {
		return nil
	}
	paths := map[string]bool{}
	for _, f := range files {
		paths[f.Path] = true
	}
	others, err := s.others(ctx, uuids(tables))
	if err != nil {
		return err
	}
	for _, tbl := range others {
		name := tableName(tbl.Identifier())
		tf, err := readFiles(ctx, tbl)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		for p := range tf.referenced() {
			if paths[p] {
				return fmt.Errorf("%s references %s, a file the request deletes, but the request does not erase from %[1]s: "+
					"tag the columns of %[1]s that hold the identifiers and make a new request for them; this one goes on once that one is done", name, base(p))
			}
		}
	}
	return nil
}

// lockLocations takes the locks of the locations of r's tables
// (Options.Locks).
func (s *Service) lockLocations(ctx context.Context, r *Request) (release func(), err error) {
	if s.opts.Locks == nil {
		return func() {}, nil
	}
	tables, err := s.tables(ctx, r)
	if err != nil {
		return nil, err
	}
	var locs []string
	for _, l := range tables {
		locs = append(locs, l.tbl.Location())
	}
	release, ok, err := s.opts.Locks.Lock(ctx, locs, false)
	if err != nil {
		return nil, fmt.Errorf("lock the tables' locations: %w", err)
	}
	if !ok {
		return nil, errors.New("a clone of one of the tables is being made; deleting files once it is")
	}
	return release, nil
}

// referenced loads r's tables, and clones of them made since the request
// found its tables, which it adds to them, and returns them with every
// file any of them references.
func (s *Service) referenced(ctx context.Context, r *Request) ([]loaded, map[string]bool, error) {
	tables, err := s.tables(ctx, r)
	if err != nil {
		return nil, nil, err
	}
	added, err := s.clones(ctx, r, tables)
	if err != nil {
		return nil, nil, err
	}
	if len(added) > 0 {
		for _, t := range added {
			s.log.Info("erasure found a table made since it began that shares files with its tables",
				"request", r.ID, "table", t.Name, "reason", t.Reason)
		}
		r.Tables = append(r.Tables, added...)
		if tables, err = s.tables(ctx, r); err != nil {
			return nil, nil, err
		}
	}
	referenced := map[string]bool{}
	for _, l := range tables {
		tf, err := readFiles(ctx, l.tbl)
		if err != nil {
			return nil, nil, err
		}
		maps.Copy(referenced, tf.referenced())
	}
	return tables, referenced, nil
}

// orphans lists the files under tbl's location that referenced leaves out:
// those old enough to delete, and the others with their times. Only
// tables in Igloo's warehouse have orphans: a table registered from
// elsewhere may share its location with other data.
func (s *Service) orphans(ctx context.Context, tbl *table.Table, referenced map[string]bool) ([]string, map[string]time.Time, error) {
	young := map[string]time.Time{}
	if !within(tbl.Location(), s.opts.Warehouse) {
		return nil, young, nil
	}
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return nil, nil, err
	}
	listed, err := listFiles(fsys, tbl.Location())
	if err != nil {
		return nil, nil, err
	}
	// A listing that spelled paths otherwise than the metadata would make
	// every file look unreferenced: refuse rather than delete live files.
	if current := tbl.CurrentSnapshot(); current != nil {
		tf, err := readCurrent(ctx, tbl)
		if err != nil {
			return nil, nil, err
		}
		for p := range tf.live(true) {
			if _, ok := listed[p]; within(p, tbl.Location()) && !ok {
				return nil, nil, fmt.Errorf("listing %s does not show %s, which the table references", tbl.Location(), base(p))
			}
		}
	}
	var old []string
	now := s.now()
	for p, mod := range listed {
		switch {
		case referenced[p]:
		case now.Sub(mod) >= orphanAge:
			old = append(old, p)
		default:
			young[p] = mod
		}
	}
	return old, young, nil
}

// parquet keeps the Parquet files of paths.
func parquet(paths []string) []string {
	return slices.DeleteFunc(paths, func(p string) bool { return !strings.HasSuffix(p, ".parquet") })
}

// delete deletes files from object storage and records it. It deletes
// only files in the warehouse, with Igloo's own credentials: others were
// recorded as left (see record).
func (s *Service) delete(ctx context.Context, r *Request, tables []loaded, files []File) error {
	files = slices.DeleteFunc(slices.Clone(files), func(f File) bool { return !within(f.Path, s.opts.Warehouse) })
	if len(files) == 0 {
		return nil
	}
	if len(tables) == 0 {
		return errors.New("none of the request's tables exists to delete files through")
	}
	if err := s.opts.Store.AddFiles(ctx, r.ID, files); err != nil {
		return err
	}
	fsys, err := tables[0].tbl.FS(ctx)
	if err != nil {
		return err
	}
	var paths []string
	defer func() {
		// Whatever happens, replicas must empty their caches of what went.
		if len(paths) > 0 {
			r.DeletedThrough = s.now()
		}
	}()
	for _, f := range files {
		if err := fsys.Remove(f.Path); err != nil && !notExist(err) {
			return errors.Join(fmt.Errorf("delete %s: %w", base(f.Path), err), s.opts.Store.Deleted(ctx, r.ID, paths, s.now()))
		}
		paths = append(paths, f.Path)
		if i := slices.IndexFunc(r.Tables, func(t Table) bool { return t.Name == f.Table }); i >= 0 {
			r.Tables[i].FilesDeleted++
		}
	}
	return s.opts.Store.Deleted(ctx, r.ID, paths, s.now())
}

// listFiles lists the files under dir with their modification times.
func listFiles(fsys iceio.IO, dir string) (map[string]time.Time, error) {
	walker, ok := fsys.(iceio.ListableIO)
	if !ok {
		return nil, errors.New("the warehouse's storage cannot list files")
	}
	out := map[string]time.Time{}
	err := walker.WalkDir(strings.TrimSuffix(dir, "/"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if notExist(err) {
				return fs.SkipDir
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if notExist(err) {
				return nil
			}
			return err
		}
		out[p] = info.ModTime()
		return nil
	})
	return out, err
}

func notExist(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || gcerrors.Code(err) == gcerrors.NotFound
}

// sharing deletes the Delta Sharing copies of data files that hold
// matching rows; Delta Sharing writes them again, from the files that
// remain, when a recipient needs them.
func (s *Service) sharing(ctx context.Context, r *Request) error {
	tables, err := s.tables(ctx, r)
	if err != nil {
		return err
	}
	copies, err := s.sharedCopies(ctx, r, tables)
	if err != nil {
		return err
	}
	var gone []File
	for _, c := range copies {
		gone = append(gone, File{Path: c.path, Table: c.table, Kind: fileSharing})
	}
	for _, l := range tables {
		l.t.done(StepSharing, s.now())
	}
	return s.delete(ctx, r, tables, gone)
}

type sharedCopy struct{ path, table string }

// sharedCopies returns the Delta Sharing copies that hold matching rows:
// those of the tables, and those under other tables' UUIDs, such as the
// copies of a table since replaced, which are looked in by the names of
// every table's columns.
func (s *Service) sharedCopies(ctx context.Context, r *Request, tables []loaded) ([]sharedCopy, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	fsys, err := tables[0].tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	listed, err := listFiles(fsys, s.opts.Warehouse+sharingDir)
	if err != nil {
		return nil, err
	}
	byDir := map[string][]string{}
	for p := range listed {
		dir, _, _ := strings.Cut(strings.TrimPrefix(p, s.opts.Warehouse+sharingDir), "/")
		if strings.HasSuffix(p, ".parquet") {
			byDir[dir] = append(byDir[dir], p)
		}
	}
	var (
		out     []sharedCopy
		anyCols []scanColumn
	)
	for _, l := range tables {
		cols := scanColumns(fields(l.tbl, l.t), r.Subjects)
		for _, c := range cols {
			c.id = 0
			anyCols = append(anyCols, c)
		}
		id := l.tbl.Metadata().TableUUID().String()
		matched, err := s.matchingFiles(ctx, r.salt, byDir[id], cols)
		if err != nil {
			return nil, err
		}
		for _, p := range slices.Sorted(maps.Keys(matched)) {
			out = append(out, sharedCopy{p, l.t.Name})
		}
		delete(byDir, id)
	}
	for _, dir := range slices.Sorted(maps.Keys(byDir)) {
		matched, err := s.matchingFiles(ctx, r.salt, byDir[dir], anyCols)
		if err != nil {
			return nil, err
		}
		for _, p := range slices.Sorted(maps.Keys(matched)) {
			out = append(out, sharedCopy{p, tables[0].t.Name})
		}
	}
	return out, nil
}

// history redacts the identifiers from the history stores.
func (s *Service) history(ctx context.Context, r *Request) error {
	n, err := s.redact(ctx, newRedactor(r))
	r.HistoryRedacted += n
	return err
}

func (s *Service) redact(ctx context.Context, x interface {
	Redact(string) (string, bool)
}) (int64, error) {
	var total int64
	for _, h := range s.opts.History {
		n, err := h.Redact(ctx, x)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// counter counts the history records a redactor would change, changing
// none.
type counter struct {
	x *redactor
	n int64
}

func (c *counter) Redact(text string) (string, bool) {
	if _, ok := c.x.Redact(text); ok {
		c.n++
	}
	return text, false
}

const (
	// liveWindow is how recently a replica must have been seen to count
	// as running: one that was must empty its cache before the request
	// completes.
	liveWindow = 2 * time.Minute
	// cacheWait is how long a request waits for replicas at a time.
	cacheWait = 20 * time.Second
	// staleCache is how long a disk cache not seen is still expected back.
	staleCache = 30 * 24 * time.Hour
)

// cache waits until every running replica emptied its read cache since
// the request last deleted files.
func (s *Service) cache(ctx context.Context, r *Request) error {
	if s.opts.Purger != nil {
		if err := s.opts.Purger.Once(ctx); err != nil {
			return err
		}
	}
	_, _, waiting, err := s.caches(ctx, r)
	if err != nil {
		return err
	}
	if len(waiting) > 0 {
		s.log.Info("erasure waits for replicas to empty their read caches", "request", r.ID,
			"replicas", strings.Join(waiting, ", "))
		r.wait(s.now().Add(cacheWait))
	}
	return nil
}

// caches counts the caches emptied since r deleted files, and those of
// replicas not running now, which will be when they start, and names the
// running replicas that have yet to.
func (s *Service) caches(ctx context.Context, r *Request) (emptied, pending int, waiting []string, err error) {
	if r.DeletedThrough.IsZero() {
		return 0, 0, nil, nil
	}
	caches, err := s.opts.Store.Caches(ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	now := s.now()
	for _, c := range caches {
		switch {
		case !c.Emptied.Before(r.DeletedThrough):
			emptied++
		case now.Sub(c.Seen) < liveWindow:
			waiting = append(waiting, c.Host)
		case !strings.HasPrefix(c.ID, MemoryCache) && now.Sub(c.Seen) < staleCache:
			pending++
		}
	}
	sort.Strings(waiting)
	return emptied, pending, waiting, nil
}

// verify looks for the identifiers in what is left, and returns the step
// to run next: done when it finds nothing, or else the step that removes
// what it found.
func (s *Service) verify(ctx context.Context, r *Request) (string, error) {
	m := newMatcher(r)
	v := &Verification{At: s.now()}
	files, err := s.opts.Store.Files(ctx, r.ID)
	if err != nil {
		return "", err
	}
	tables, referenced, err := s.referenced(ctx, r)
	if err != nil {
		return "", err
	}
	l, err := newListings(ctx, tables)
	if err != nil {
		return "", err
	}
	for _, lt := range tables {
		if err := s.verifyTable(ctx, r, lt, m, referenced, l, v); err != nil {
			return "", fmt.Errorf("verify %s: %w", lt.t.Name, err)
		}
	}
	for _, f := range files {
		if f.Deleted.IsZero() {
			continue
		}
		if exists, err := l.exists(f.Path); err != nil {
			return "", err
		} else if exists {
			v.Storage++
		}
	}
	copies, err := s.sharedCopies(ctx, r, tables)
	if err != nil {
		return "", err
	}
	v.Sharing = int64(len(copies))
	// What replicas recorded since the history step is redacted too, and
	// then nothing may be left.
	x := newRedactor(r)
	n, err := s.redact(ctx, x)
	if err != nil {
		return "", err
	}
	r.HistoryRedacted += n
	c := &counter{x: x}
	if _, err := s.redact(ctx, c); err != nil {
		return "", err
	}
	v.History = c.n
	var waiting []string
	if v.Caches, v.Pending, waiting, err = s.caches(ctx, r); err != nil {
		return "", err
	}
	r.Verification = v

	next := StepDone
	switch {
	case v.Rows > 0 || v.Files > 0 || v.Bounds > 0 || v.Statistics > 0:
		next = StepDelete
	case v.Storage > 0 || v.Orphans > 0:
		next = StepPurge
	case v.Sharing > 0:
		next = StepSharing
	case v.History > 0:
		next = StepHistory
	case len(waiting) > 0:
		next = StepCache
	}
	if next == StepDone {
		return next, nil
	}
	if r.Rounds++; r.Rounds > maxRounds {
		return "", terminal(fmt.Errorf("after %d rounds, verification still finds %s", maxRounds, v))
	}
	s.log.Info("erasure verification found more to remove", "request", r.ID, "found", v.String(), "next", next)
	return next, nil
}

func (s *Service) verifyTable(ctx context.Context, r *Request, lt loaded, m *matcher, referenced map[string]bool, l *listings, v *Verification) error {
	t, tbl := lt.t, lt.tbl
	flds := fields(tbl, t)
	cols := scanColumns(flds, r.Subjects)
	if cond := currentCondition(r.salt, flds, r.Subjects, true); cond != "" {
		n, err := s.rows.Count(ctx, tbl, cond)
		if err != nil {
			return err
		}
		v.Rows += n
	}
	tf, err := readFiles(ctx, tbl)
	if err != nil {
		return err
	}
	var data, scanned []string
	for p, df := range tf.live(false) {
		if scannable(df) {
			scanned = append(scanned, p)
			if df.ContentType() == iceberg.EntryContentData {
				data = append(data, p)
			}
		}
	}
	matched, err := s.matchingFiles(ctx, r.salt, scanned, cols)
	if err != nil {
		return err
	}
	v.Files += int64(len(matched))
	// A bound that may hold an identifier counts unless the file's rows
	// say otherwise: truncated bounds can match without the value.
	for p := range tf.boundMatches(m, flds) {
		if !slices.Contains(scanned, p) || matched[p] > 0 {
			v.Bounds++
		}
	}
	n, err := s.matchingStatistics(ctx, m, data, cols)
	if err != nil {
		return err
	}
	v.Statistics += n
	_, young, err := s.orphans(ctx, tbl, referenced)
	if err != nil {
		return err
	}
	orphans, err := s.matchingFiles(ctx, r.salt, parquet(slices.Collect(maps.Keys(young))), cols)
	if err != nil {
		return err
	}
	v.Orphans += int64(len(orphans))
	return nil
}

// String summarizes what v found, for errors and logs.
func (v *Verification) String() string {
	var parts []string
	for _, c := range []struct {
		n    int64
		what string
	}{
		{v.Rows, "rows"}, {v.Files, "files"}, {v.Bounds, "manifest bounds"}, {v.Statistics, "Parquet statistics"},
		{v.Storage, "deleted files in storage"}, {v.Orphans, "recent orphan files"}, {v.Sharing, "sharing copies"},
		{v.History, "history records"},
	} {
		if c.n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", c.n, c.what))
		}
	}
	if len(parts) == 0 {
		return "nothing"
	}
	return strings.Join(parts, ", ")
}
