package erasure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"gocloud.dev/gcerrors"

	"github.com/akkio-inc/igloo/internal/clone"
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
	// mistaken or placeholder identifier matches much of a table. Rows
	// that other requests made within holdWindow matched in the table
	// count too, so splitting a request does not get past the hold; and a
	// request that matches every row of a table is held whatever its size.
	holdFraction = 0.01
	holdRows     = 100
	holdWindow   = 24 * time.Hour
	// A request waits as long as countWait for the other requests made
	// within holdWindow of it to count their rows, looking again every
	// countRecheck, and is held after that.
	countWait    = time.Hour
	countRecheck = time.Minute
	// stagedWait is how long a request waits for a snapshot staged by
	// write-audit-publish that holds matching rows to be published or
	// discarded, counted from when the request was made: as long as
	// maintenance keeps such a snapshot by default, or the table's
	// history.expire.max-snapshot-age-ms if longer, but never more than
	// stagedWaitMax, so that the request completes within the month the
	// GDPR allows.
	stagedWait    = 7 * 24 * time.Hour
	stagedWaitMax = 21 * 24 * time.Hour
	// stagedRecheck is how often a request that waits for one looks again.
	stagedRecheck = 10 * time.Minute
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
	if r.ConfirmedBy != "" {
		return nil
	}
	// The counts are saved first, so that requests that look at this one
	// while it waits for them see its counts, and none waits for another
	// that waits for it.
	r.Counted = true
	if err := s.save(ctx, r); err != nil {
		return err
	}
	recent, err := s.recent(ctx, r)
	if err != nil {
		return err
	}
	var uncounted []string
	for _, o := range recent {
		if !counted(o) {
			uncounted = append(uncounted, o.ID)
		}
	}
	for _, t := range r.Tables {
		if t.Remote {
			continue
		}
		// What other recent requests matched in the table is gone from it
		// now, or will be.
		matched, rows := t.Matched, t.Rows
		for _, o := range recent {
			for _, ot := range o.Tables {
				if ot.Matched > 0 && sameTable(ot, t) {
					matched += ot.Matched
					rows += ot.Matched
				}
			}
		}
		whole := t.Matched > 0 && t.Matched >= t.Rows
		if whole || matched > holdRows && float64(matched) > holdFraction*float64(rows) {
			s.log.Warn("erasure request held: it would delete much of a table, with what recent requests deleted there; "+
				"someone other than its requester must CONFIRM ERASURE REQUEST for it to go on",
				"request", r.ID, "table", t.Name, "rows", t.Matched, "of", t.Rows, "recent_requests_rows", matched-t.Matched)
			r.Status = StatusHeld
			return nil
		}
	}
	if len(uncounted) == 0 {
		return nil
	}
	// Requests made about when this one was that have yet to count their
	// rows, as when they wait behind another, could take it over the hold
	// once they do, whichever runs first: it waits for them, and is held
	// if they still have not after countWait.
	if r.CountWait.IsZero() {
		r.CountWait = s.now()
	}
	if s.now().Sub(r.CountWait) >= countWait {
		s.log.Warn("erasure request held: requests made about when it was have not counted their rows; "+
			"someone other than its requester must CONFIRM ERASURE REQUEST for it to go on",
			"request", r.ID, "requests", strings.Join(uncounted, ", "))
		r.Status = StatusHeld
		return nil
	}
	r.Error = "waiting: requests made within a day of this one have yet to count their rows, which its hold counts too: " +
		strings.Join(uncounted, ", ")
	r.wait(s.now().Add(countRecheck))
	return nil
}

// recent are the requests other than r made within holdWindow of it,
// before or after, but those canceled and those someone confirmed.
func (s *Service) recent(ctx context.Context, r *Request) ([]*Request, error) {
	all, err := s.opts.Store.List(ctx)
	if err != nil {
		return nil, err
	}
	var out []*Request
	for _, o := range all {
		d := r.Created.Sub(o.Created)
		if o.ID != r.ID && o.Status != StatusCanceled && o.ConfirmedBy == "" && d <= holdWindow && d >= -holdWindow {
			out = append(out, o)
		}
	}
	return out, nil
}

// counted reports whether o counted the rows it matches: it did once it
// went past the find step, in which it counts them.
func counted(o *Request) bool { return o.Counted || o.Step != StepFind }

// sameTable reports whether a and b are the same table: by UUID when both
// know it, and otherwise by name.
func sameTable(a, b Table) bool {
	if a.UUID != "" && b.UUID != "" {
		return a.UUID == b.UUID
	}
	return strings.EqualFold(a.Name, b.Name)
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
	families := map[string]map[string]bool{}
	for _, l := range sources {
		flds := fields(l.tbl, l.t)
		if err := resolved(l.t, flds); err != nil {
			return err
		}
		if err := checkFields(l.t, flds); err != nil {
			return err
		}
		for _, f := range flds {
			key := f.Tag + "." + f.Value
			if f.Digest != "" {
				key += ":" + f.Digest
			}
			if families[key] == nil {
				families[key] = map[string]bool{}
			}
			families[key][typeFamily(f.typ)] = true
		}
	}
	for value, fams := range families {
		if len(fams) > 1 {
			s.log.Warn("an erasure request's tag value is on columns of different kinds of values; identifiers compare as text in each "+
				"(UUIDs also as hex in binary columns), so one given in a form some of them do not hold finds nothing there",
				"request", r.ID, "tag_value", value, "kinds", strings.Join(slices.Sorted(maps.Keys(fams)), ", "))
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
	if err := s.rewritables(ctx, r); err != nil {
		return err
	}
	return s.count(ctx, r)
}

// rewritables checks that every table of r can be rewritten (see
// rewritable), as they are now, before the request deletes anything.
func (s *Service) rewritables(ctx context.Context, r *Request) error {
	tables, err := s.tables(ctx, r)
	if err != nil {
		return err
	}
	for _, l := range tables {
		if err := rewritable(l.t, l.tbl); err != nil {
			return terminal(err)
		}
	}
	return nil
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

// shares are the locations whose files tbl, a zero-copy clone, says it may
// reference: the table it was cloned from, and those that one shares files
// with in turn (see package clone).
func shares(tbl *table.Table) []string {
	var locs []string
	if v := tbl.Properties()[clone.SharesProperty]; v != "" {
		_ = json.Unmarshal([]byte(v), &locs)
	}
	for i, l := range locs {
		locs[i] = strings.TrimRight(l, "/")
	}
	return locs
}

// refused reports whether err is the catalog refusing a table for good,
// rather than failing to answer.
func refused(err error) bool {
	return errors.Is(err, rest.ErrForbidden) || errors.Is(err, rest.ErrUnauthorized) || errors.Is(err, rest.ErrBadRequest)
}

// uuids are the UUIDs of the request's tables that it loaded.
func uuids(tables []loaded) map[string]bool {
	out := map[string]bool{}
	for _, l := range tables {
		out[l.tbl.Metadata().TableUUID().String()] = true
	}
	return out
}

// others loads the catalog's tables that are not among ours. One the
// catalog refuses for good (forbidden, unauthorized, a bad request) is
// left out, and logged: nothing can read it either, so deleting files it
// references breaks nothing that works, and one broken table must not
// stop every request. Any other error, as the catalog unavailable or
// failing for a moment, stops the run, which tries again later without
// counting it as a failure: leaving the table out could delete files it
// references. One named as one of ours fails.
func (s *Service) others(ctx context.Context, ours []loaded) ([]*table.Table, error) {
	uuid := uuids(ours)
	idents, err := s.opts.Catalog.Tables(ctx)
	if err != nil {
		return nil, err
	}
	var out []*table.Table
	for _, id := range idents {
		tbl, err := s.opts.Catalog.LoadTable(ctx, id)
		if errors.Is(err, icecat.ErrNoSuchTable) {
			continue
		}
		if err != nil {
			if slices.ContainsFunc(ours, func(l loaded) bool { return strings.EqualFold(l.t.Name, tableName(id)) }) {
				return nil, fmt.Errorf("load %s: %w", tableName(id), err)
			}
			if !refused(err) {
				return nil, unavailable(fmt.Errorf("load %s, to check whether it shares files with the request's tables: %w", tableName(id), err))
			}
			s.log.Warn("an erasure request could not load a table to check whether it shares files with the request's tables; it is left out",
				"table", tableName(id), "err", err)
			continue
		}
		if !uuid[tbl.Metadata().TableUUID().String()] {
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
// copies of its source's rows, and still names its source. Tables that reference another's files
// without saying so are found by their manifests, among those with a
// column named like a tagged one, and their columns of those names are
// taken to hold the same.
func (s *Service) clones(ctx context.Context, r *Request, sources []loaded) ([]Table, error) {
	others, err := s.others(ctx, sources)
	if err != nil {
		return nil, err
	}
	var out []Table
	for len(sources) > 0 {
		var found []loaded
		for _, src := range sources {
			for i := 0; i < len(others); i++ {
				o := others[i]
				t, err := s.related(ctx, src, o)
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
func (s *Service) related(ctx context.Context, src loaded, other *table.Table) (*Table, error) {
	name := tableName(other.Identifier())
	t := &Table{Name: name, Ident: other.Identifier(), UUID: other.Metadata().TableUUID().String()}
	srcLoc, otherLoc := src.tbl.Location(), other.Location()
	sharesLoc := func(locs []string, loc string) bool {
		return slices.ContainsFunc(locs, func(l string) bool { return canonical(l) == canonical(loc) })
	}
	switch {
	case sharesLoc(shares(other), srcLoc) || strings.EqualFold(other.Properties()[clone.SourceProperty], src.t.Name):
		t.Columns, t.Reason = sameFields(src, other), "a clone of "+src.t.Name
		return t, nil
	case sharesLoc(shares(src.tbl), otherLoc) || strings.EqualFold(src.tbl.Properties()[clone.SourceProperty], name):
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
	dirs, err := s.dirsOf(ctx, other)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	if dirs.live.under(srcLoc) {
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
	if !r.DeleteStarted {
		// A table that became one the request cannot rewrite since it found
		// its tables stops it while it can still be canceled.
		if err := s.rewritables(ctx, r); err != nil {
			return err
		}
		// A DELETE whose answer is lost may have committed: from here on,
		// the request cannot be canceled.
		r.DeleteStarted = true
		if err := s.save(ctx, r); err != nil {
			return err
		}
	}
	for _, l := range tables {
		if cond := currentCondition(r.salt, fields(l.tbl, l.t), r.Subjects, false); cond != "" {
			n, err := s.rows.Delete(ctx, l.tbl, cond)
			if commitConflict(err) {
				return contended(fmt.Errorf("delete from %s: other writers' commits kept coming first: %w", l.t.Name, err))
			}
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
		if err := s.sweep(ctx, r, t, false); err != nil {
			return err
		}
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
func (s *Service) matched(ctx context.Context, r *Request) (pathSet, error) {
	files, err := s.opts.Store.Files(ctx, r.ID)
	if err != nil {
		return nil, err
	}
	out := pathSet{}
	for _, f := range files {
		if f.Kind == fileMatched || f.Kind == fileOutside {
			out.add(f.Path)
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
	ctx       context.Context
	io        *fileIO
	locations []string
	// listed are the files under each location, by canonical path.
	listed map[string]map[string]time.Time
}

func newListings(ctx context.Context, tables []loaded, warehouse string) (*listings, error) {
	l := &listings{ctx: ctx, io: newFileIO(tables, warehouse), listed: map[string]map[string]time.Time{}}
	for _, t := range tables {
		l.locations = append(l.locations, t.tbl.Location())
	}
	return l, nil
}

// fileIO opens the file IO of a request's tables, each once, and picks
// the one a path is read, listed or deleted with: that of the table whose
// location holds it, or, for a path under none, as the warehouse's sharing
// copies are, that of a table in the warehouse. A table's IO carries the
// endpoint and credentials of its own location, which another's may not
// reach, as for tables in other buckets.
type fileIO struct {
	tables    []loaded
	warehouse string
	open      map[*table.Table]iceio.IO
}

func newFileIO(tables []loaded, warehouse string) *fileIO {
	return &fileIO{tables: tables, warehouse: warehouse, open: map[*table.Table]iceio.IO{}}
}

func (x *fileIO) forPath(ctx context.Context, p string) (iceio.IO, error) {
	var (
		best *table.Table
		size = -1
	)
	for _, l := range x.tables {
		if loc := canonical(l.tbl.Location()); (within(p, loc) || canonical(p) == loc) && len(loc) > size {
			best, size = l.tbl, len(loc)
		}
	}
	for _, l := range x.tables {
		if best == nil && within(l.tbl.Location(), x.warehouse) {
			best = l.tbl
		}
	}
	if best == nil {
		if len(x.tables) == 0 {
			return nil, errors.New("none of the request's tables exists to reach files through")
		}
		best = x.tables[0].tbl
	}
	if fsys, ok := x.open[best]; ok {
		return fsys, nil
	}
	fsys, err := best.FS(ctx)
	if err != nil {
		return nil, err
	}
	x.open[best] = fsys
	return fsys, nil
}

// list lists the files under dir.
func (l *listings) list(dir string) (map[string]time.Time, error) {
	if files, ok := l.listed[dir]; ok {
		return files, nil
	}
	fsys, err := l.io.forPath(l.ctx, dir)
	if err != nil {
		return nil, err
	}
	listed, err := listFiles(fsys, dir)
	if err != nil {
		return nil, err
	}
	files := map[string]time.Time{}
	for p, mod := range listed {
		files[canonical(p)] = mod
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
			if _, ok := files[canonical(p)]; ok {
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
func (s *Service) bad(ctx context.Context, r *Request, t *Table, tf *tableFiles, flds []field, l *listings) (pathSet, error) {
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
			if !known.has(p) {
				paths = append(paths, p)
			}
		}
	}
	cols := scanColumns(flds, r.Subjects)
	matched, err := s.matchingFiles(ctx, r.salt, paths, cols)
	if err != nil {
		return nil, err
	}
	// Position delete files that hold the person's rows (see rewriteRef).
	var deletes []string
	for _, p := range positionDeletes(tf.live(false)) {
		if exists, err := l.exists(p); err != nil {
			return nil, err
		} else if exists && !known.has(p) {
			deletes = append(deletes, p)
		}
	}
	held, err := s.matchingDeleteRows(ctx, r.salt, deletes, cols)
	if err != nil {
		return nil, err
	}
	maps.Copy(matched, held)
	found := slices.Collect(maps.Keys(matched))
	for p := range tf.boundMatches(newMatcher(r), flds) {
		if !scanned[p] && !known.has(p) {
			found = append(found, p)
		}
	}
	if err := s.record(ctx, r, t, fileMatched, found); err != nil {
		return nil, err
	}
	for _, p := range found {
		known.add(p)
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
	l, err := newListings(ctx, tables, s.opts.Warehouse)
	if err != nil {
		return err
	}
	var waitUntil time.Time
	for _, lt := range tables {
		t := lt.t
		for attempt := 1; ; attempt++ {
			n, stagedUntil, err := s.expireTable(ctx, r, t, l)
			if errors.Is(err, table.ErrCommitFailed) {
				if attempt < commitAttempts {
					continue
				}
				err = contended(err)
			}
			if err != nil {
				return fmt.Errorf("expire snapshots of %s: %w", t.Name, err)
			}
			t.SnapshotsExpired += n
			if !stagedUntil.IsZero() && (waitUntil.IsZero() || stagedUntil.Before(waitUntil)) {
				waitUntil = stagedUntil
			}
			break
		}
		if err := s.save(ctx, r); err != nil {
			return err
		}
	}
	if !waitUntil.IsZero() {
		r.wait(waitUntil)
		return nil
	}
	for _, lt := range tables {
		lt.t.done(StepExpire, s.now())
	}
	return nil
}

// staged reports whether snap is a snapshot staged by write-audit-publish
// (Spark's wap.id) that no branch or tag has, and that is not published:
// someone's unpublished writes, which publishing adds to a branch later.
// Publishing fast-forwards a branch to the snapshot, which then has it,
// or cherry-picks it, making a copy on the branch whose summary names
// the same wap.id as published-wap-id; either way the branch holds its
// rows, which the request rewrites there.
func staged(tbl *table.Table, snap table.Snapshot, ancestors map[int64]bool) bool {
	if ancestors[snap.SnapshotID] || snap.Summary == nil {
		return false
	}
	wap := snap.Summary.Properties["wap.id"]
	if wap == "" {
		return false
	}
	for id := range ancestors {
		if a := tbl.SnapshotByID(id); a != nil && a.Summary != nil &&
			(a.Summary.Properties["published-wap-id"] == wap || a.Summary.Properties["wap.id"] == wap) {
			return false
		}
	}
	return true
}

// stagedUntil is how long r waits for t's staged snapshots (see
// stagedWait).
func stagedUntil(r *Request, tbl *table.Table) time.Time {
	wait := stagedWait
	if ms, err := strconv.ParseInt(tbl.Properties()["history.expire.max-snapshot-age-ms"], 10, 64); err == nil && ms > 0 {
		switch {
		case ms >= int64(stagedWaitMax/time.Millisecond):
			wait = stagedWaitMax
		case time.Duration(ms)*time.Millisecond > wait:
			wait = time.Duration(ms) * time.Millisecond
		}
	}
	return r.Created.Add(wait)
}

// ancestors are the snapshots some branch or tag has: its own, and those
// before it.
func ancestors(tbl *table.Table) map[int64]bool {
	out := map[int64]bool{}
	for _, ref := range tbl.Metadata().Refs() {
		for id := ref.SnapshotID; !out[id]; {
			out[id] = true
			snap := tbl.SnapshotByID(id)
			if snap == nil || snap.ParentSnapshotID == nil {
				break
			}
			id = *snap.ParentSnapshotID
		}
	}
	return out
}

// expireTable expires t's snapshots that reference files that held
// identifiers, and returns how many, and, if a snapshot staged by
// write-audit-publish holds some, when to look again: it is left until it
// is published, and rewritten then, or discarded, for as long as
// stagedWait, and expired after that, as maintenance would.
func (s *Service) expireTable(ctx context.Context, r *Request, t *Table, l *listings) (int, time.Time, error) {
	tbl, err := s.reload(ctx, t)
	if err != nil {
		return 0, time.Time{}, err
	}
	tf, err := readFiles(ctx, tbl)
	if err != nil {
		return 0, time.Time{}, err
	}
	bad, err := s.bad(ctx, r, t, tf, fields(tbl, t), l)
	if err != nil {
		return 0, time.Time{}, err
	}
	// A ref whose snapshot still mentions such a file, as one whose bounds
	// only now told, is rewritten first.
	for name, ref := range tbl.Metadata().Refs() {
		if tf.mentions(ref.SnapshotID, bad) {
			if err := s.onRef(ctx, r, t, name, func(ctx context.Context, view *table.Table, branch string, commit bool) (bool, error) {
				return s.rewriteRef(ctx, r, t, view, branch, commit)
			}); err != nil {
				return 0, time.Time{}, err
			}
		}
	}
	if tbl, err = s.reload(ctx, t); err != nil {
		return 0, time.Time{}, err
	}
	if tf, err = readFiles(ctx, tbl); err != nil {
		return 0, time.Time{}, err
	}
	heads := map[int64]bool{}
	for name, ref := range tbl.Metadata().Refs() {
		if tf.mentions(ref.SnapshotID, bad) {
			return 0, time.Time{}, fmt.Errorf("%s still references a file that held matching rows", name)
		}
		heads[ref.SnapshotID] = true
	}
	var (
		expired, retained []int64
		waitUntil         time.Time
		reachable         = ancestors(tbl)
		now               = s.now()
		deadline          = stagedUntil(r, tbl)
	)
	for _, snap := range tbl.Metadata().Snapshots() {
		switch {
		case heads[snap.SnapshotID] || !tf.mentions(snap.SnapshotID, bad):
			retained = append(retained, snap.SnapshotID)
		case staged(tbl, snap, reachable) && now.Before(deadline):
			// Expiring it would discard everyone's unpublished rows in it.
			retained = append(retained, snap.SnapshotID)
			at := deadline
			if recheck := now.Add(stagedRecheck); recheck.Before(at) {
				at = recheck
			}
			if waitUntil.IsZero() || at.Before(waitUntil) {
				waitUntil = at
			}
			s.log.Warn("erasure waits for a snapshot staged by write-audit-publish that holds matching rows to be published or discarded",
				"request", r.ID, "table", t.Name, "snapshot", snap.SnapshotID, "wap_id", snap.Summary.Properties["wap.id"],
				"expired_after", deadline)
			r.Error = fmt.Sprintf("waiting: snapshot %d of %s, staged by write-audit-publish (wap.id %s), holds matching rows; "+
				"publish it, and the request rewrites it, or discard it; after %s the request expires it",
				snap.SnapshotID, t.Name, snap.Summary.Properties["wap.id"], deadline.UTC().Format(time.RFC3339))
		default:
			expired = append(expired, snap.SnapshotID)
		}
	}
	if len(expired) == 0 {
		return 0, waitUntil, nil
	}
	// What only the expired snapshots reference goes once they do.
	kept := tf.kept(retained)
	var gone []string
	for c, p := range tf.files(expired) {
		if _, ok := kept[c]; !ok {
			gone = append(gone, p)
		}
	}
	if err := s.record(ctx, r, t, fileExpired, gone); err != nil {
		return 0, time.Time{}, err
	}
	// Every ref is asserted, as the catalog has them just before the
	// commit, so that none made meanwhile at an expired snapshot is lost
	// with it, and no commit to one makes an expired snapshot its parent.
	fresh, err := s.reload(ctx, t)
	if err != nil {
		return 0, time.Time{}, err
	}
	var reqs []table.Requirement
	for name, ref := range fresh.Metadata().Refs() {
		if slices.Contains(expired, ref.SnapshotID) {
			return 0, time.Time{}, fmt.Errorf("%w: %s now points at a snapshot the request expires", table.ErrCommitFailed, name)
		}
		id := ref.SnapshotID
		reqs = append(reqs, table.AssertRefSnapshotID(name, &id))
	}
	if err := s.commitRefs(ctx, fresh, reqs, table.NewRemoveSnapshotsUpdate(expired, false)); err != nil {
		return 0, time.Time{}, err
	}
	if after, err := s.reload(ctx, t); err == nil {
		for name := range fresh.Metadata().Refs() {
			if _, ok := refs(after)[name]; !ok {
				s.log.Error("a branch or tag went while an erasure request expired snapshots; if it was made just then at one of them, it is lost",
					"request", r.ID, "table", t.Name, "ref", name)
			}
		}
	}
	s.log.Info("expired snapshots holding erased data", "request", r.ID, "table", t.Name, "snapshots", len(expired))
	return len(expired), waitUntil, nil
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
	going := pathSet{}
	for _, f := range files {
		if f.Deleted.IsZero() && f.Kind != fileOutside && !referenced.has(f.Path) {
			gone = append(gone, f)
			going.add(f.Path)
		}
	}
	var waitUntil time.Time
	for _, l := range tables {
		o, err := s.orphans(ctx, l.tbl, referenced)
		if err != nil {
			return fmt.Errorf("orphan files of %s: %w", l.t.Name, err)
		}
		for _, p := range o.old {
			if !going.has(p) {
				gone = append(gone, File{Path: p, Table: l.t.Name, Kind: fileOrphan})
			}
		}
		cols := scanColumns(fields(l.tbl, l.t), r.Subjects)
		young := o.young
		maps.DeleteFunc(young, func(p string, _ time.Time) bool { return going.has(p) })
		matched, err := s.matchingFiles(ctx, r.salt, parquet(slices.Collect(maps.Keys(young))), cols)
		if err != nil {
			return err
		}
		for p := range matched {
			if at := young[p].Add(orphanAge); waitUntil.IsZero() || at.Before(waitUntil) {
				waitUntil = at
			}
		}
		unsure, err := s.matchingFiles(ctx, r.salt, parquet(slices.DeleteFunc(o.unsure, going.has)), cols)
		if err != nil {
			return err
		}
		if len(unsure) > 0 {
			p := slices.Sorted(maps.Keys(unsure))[0]
			return fmt.Errorf("%s, under %s, holds matching rows, and no file of the table is spelled as it is, but one has its name: "+
				"it may be that file, spelled otherwise, so the request does not delete it; if nothing references it, delete it", base(p), l.t.Name)
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
	paths, dirs := pathSet{}, dirSet{}
	for _, f := range files {
		paths.add(f.Path)
		dirs.add(f.Path)
	}
	others, err := s.others(ctx, tables)
	if err != nil {
		return err
	}
	for _, tbl := range others {
		name := tableName(tbl.Identifier())
		// Only a table that references files in the directories of those
		// the request deletes needs its manifests read.
		od, err := s.dirsOf(ctx, tbl)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		if !od.referenced.meets(dirs) {
			continue
		}
		tf, err := readFiles(ctx, tbl)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		for _, p := range tf.referenced() {
			if paths.has(p) {
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
func (s *Service) referenced(ctx context.Context, r *Request) ([]loaded, pathSet, error) {
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
	referenced := pathSet{}
	for _, l := range tables {
		tf, err := readFiles(ctx, l.tbl)
		if err != nil {
			return nil, nil, err
		}
		maps.Copy(referenced, tf.referenced())
	}
	return tables, referenced, nil
}

// orphanFiles are the files under a table's location that nothing
// references: old are old enough to delete, young are not, by when they
// were written. unsure are files no referenced file is, as canonical
// spells paths, but one has the name of, in another directory: the
// metadata can spell a location in ways canonical does not know (another
// bucket alias or endpoint), so they are not deleted.
type orphanFiles struct {
	old    []string
	young  map[string]time.Time
	unsure []string
}

// orphans lists the files under tbl's location that referenced leaves out.
// Only tables in Igloo's warehouse have orphans: a table registered from
// elsewhere may share its location with other data.
func (s *Service) orphans(ctx context.Context, tbl *table.Table, referenced pathSet) (*orphanFiles, error) {
	o := &orphanFiles{young: map[string]time.Time{}}
	if !within(tbl.Location(), s.opts.Warehouse) {
		return o, nil
	}
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	listed, err := listFiles(fsys, tbl.Location())
	if err != nil {
		return nil, err
	}
	listedSet, listedNames := pathSet{}, map[string]bool{}
	for p := range listed {
		listedSet.add(p)
		listedNames[path.Base(p)] = true
	}
	// A listing that spelled paths otherwise than the metadata, in a way
	// canonical does not know, would make every file look unreferenced:
	// refuse rather than delete live files. A live file is looked for in
	// the listing if it is under the location, or if a listed file has its
	// name.
	if current := tbl.CurrentSnapshot(); current != nil {
		tf, err := readCurrent(ctx, tbl)
		if err != nil {
			return nil, err
		}
		for p := range tf.live(true) {
			if !listedSet.has(p) && (within(p, tbl.Location()) || listedNames[path.Base(p)]) {
				return nil, fmt.Errorf("listing %s does not show %s, which the table references, as the table spells it", tbl.Location(), base(p))
			}
		}
	}
	names := map[string]bool{}
	for _, p := range referenced {
		names[path.Base(p)] = true
	}
	now := s.now()
	for p, mod := range listed {
		switch {
		case referenced.has(p):
		case names[path.Base(p)]:
			o.unsure = append(o.unsure, p)
		case now.Sub(mod) >= orphanAge:
			o.old = append(o.old, p)
		default:
			o.young[p] = mod
		}
	}
	return o, nil
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
	fio := newFileIO(tables, s.opts.Warehouse)
	var paths []string
	defer func() {
		// Whatever happens, replicas must empty their caches of what went.
		// The time is kept as the metadata database keeps times, to the
		// microsecond (see caches).
		if len(paths) > 0 {
			r.DeletedThrough = s.now().Truncate(time.Microsecond)
		}
	}()
	for _, f := range files {
		fsys, err := fio.forPath(ctx, f.Path)
		if err != nil {
			return errors.Join(err, s.opts.Store.Deleted(ctx, r.ID, paths, s.now()))
		}
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
	fsys, err := newFileIO(tables, s.opts.Warehouse).forPath(ctx, s.opts.Warehouse+sharingDir)
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
	// Replicas record that they emptied their caches through the latest
	// deletion as the metadata database has it, to the microsecond: a
	// request's own time, to the nanosecond, could be after it for ever.
	through := r.DeletedThrough.Truncate(time.Microsecond)
	now := s.now()
	for _, c := range caches {
		switch {
		case !c.Emptied.Before(through):
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
	l, err := newListings(ctx, tables, s.opts.Warehouse)
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

func (s *Service) verifyTable(ctx context.Context, r *Request, lt loaded, m *matcher, referenced pathSet, l *listings, v *Verification) error {
	t, tbl := lt.t, lt.tbl
	flds := fields(tbl, t)
	if err := resolved(t, flds); err != nil {
		return err
	}
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
	held, err := s.matchingDeleteRows(ctx, r.salt, positionDeletes(tf.live(false)), cols)
	if err != nil {
		return err
	}
	v.Files += int64(len(matched) + len(held))
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
	o, err := s.orphans(ctx, tbl, referenced)
	if err != nil {
		return err
	}
	orphans, err := s.matchingFiles(ctx, r.salt, parquet(append(slices.Collect(maps.Keys(o.young)), o.unsure...)), cols)
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
