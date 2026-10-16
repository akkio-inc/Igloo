package clone

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// Table maintenance deletes two kinds of files: those only expired
// snapshots referenced, and orphans, files under a table's location that
// no snapshot references. A clone references files under its source's
// location, so a source's maintenance must not delete a file a clone still
// references, and a clone's must never delete its source's files at all.
// Unreferenced takes care of the second; Guard of the first.
//
// Each pass over the tables starts a Pass, which notes every table it
// loads: the clones among them record their shares (SharesProperty) in the
// Store again, so a Store that lost them (a memory store after a restart)
// has them back before anything is deleted. Deleting a table's files then
// takes the lock of its location, which cloning it holds too, lists the
// clones that share the location, and keeps every file one of them
// references in any of its snapshots. Whenever Igloo cannot tell for sure
// whether a clone still exists or what it references, it keeps the files;
// a later pass, or the orphan cleanup, deletes them once it can.

// settle is how long a share is kept, whatever the catalog says, after it
// is recorded: a clone being made records its shares before it is in the
// catalog.
const settle = time.Hour

var (
	// ErrBusy means a table's location was locked by a clone being made
	// of it, so its files were kept for now.
	ErrBusy = errors.New("a clone of the table is being made")
	// ErrUnsure means the pass could not tell whether a clone of the
	// table still exists, as when it was dropped or renamed during the
	// pass or made in the last hour, so the table's files were kept for
	// now.
	ErrUnsure = errors.New("cannot tell yet whether a clone still needs the table's files")
)

// Guard keeps maintenance from deleting files clones share.
type Guard struct {
	cat   Catalog
	store Store
	log   *slog.Logger
}

// NewGuard guards the tables of cat, whose shares store keeps.
func NewGuard(cat Catalog, store Store, log *slog.Logger) *Guard {
	return &Guard{cat: cat, store: store, log: log.With("component", "maintenance")}
}

// Sources returns the locations of other tables whose files a table with
// these properties reads: none for a table that is no clone, or no longer
// reads its source's files.
func (g *Guard) Sources(properties map[string]string) []string { return declaredShares(properties) }

// Shared reports whether clones may read files under location, as far as
// the Store knows.
func (g *Guard) Shared(ctx context.Context, location string) (bool, error) {
	shares, err := g.store.Sharers(ctx, location)
	return len(shares) > 0, err
}

// Readers takes the lock of location, which making a clone of the table
// there takes too, and names the clones that read files under it, or may:
// every clone the catalog has, under the name it was recorded with or
// another, and every clone recorded within the hour, which may still be
// being made. Until release is called no clone of the table is made, so a
// caller that finds none may delete the table's files meanwhile.
func (g *Guard) Readers(ctx context.Context, location string) (names []string, release func(), err error) {
	release, _, err = g.store.Lock(ctx, []string{location}, true)
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) ([]string, func(), error) {
		release()
		return nil, nil, err
	}
	shares, err := g.store.Sharers(ctx, location)
	if err != nil {
		return fail(err)
	}
	var live map[uuid.UUID]table.Identifier
	for _, sh := range shares {
		tbl, err := g.cat.LoadTable(ctx, table.Identifier{sh.Schema, sh.Table})
		switch {
		case err == nil && tbl.Metadata().TableUUID() == sh.Clone:
			names = append(names, sh.Schema+"."+sh.Table)
			continue
		case err != nil && !errors.Is(err, icecat.ErrNoSuchTable):
			return fail(err)
		}
		// Renamed, or dropped: look for it under every name.
		if live == nil {
			if live, err = g.tables(ctx); err != nil {
				return fail(err)
			}
		}
		if ident, ok := live[sh.Clone]; ok {
			names = append(names, strings.Join(ident, "."))
		} else if time.Since(sh.Created) <= settle {
			names = append(names, sh.Schema+"."+sh.Table)
		}
	}
	slices.Sort(names)
	return slices.Compact(names), release, nil
}

// tables maps the UUID of every table of the catalog to its name.
func (g *Guard) tables(ctx context.Context) (map[uuid.UUID]table.Identifier, error) {
	idents, err := g.cat.Tables(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[uuid.UUID]table.Identifier, len(idents))
	for _, ident := range idents {
		tbl, err := g.cat.LoadTable(ctx, ident)
		if errors.Is(err, icecat.ErrNoSuchTable) {
			// Renamed since it was listed: it may be the clone sought.
			return nil, fmt.Errorf("%w: the catalog changed while it was read", ErrUnsure)
		}
		if err != nil {
			return nil, err
		}
		out[tbl.Metadata().TableUUID()] = ident
	}
	return out, nil
}

// Pass is one maintenance pass's view of the catalog: which tables exist,
// by UUID, and whether it saw all of them.
type Pass struct {
	g     *Guard
	start time.Time
	live  map[uuid.UUID]table.Identifier
	seen  map[string]bool
	// missing tables were listed but gone when loaded: dropped, or
	// renamed, in which case they are listed again under the new name.
	missing bool
	// failed means some table could not be loaded or the Store could not
	// be updated, so the pass cannot tell a dropped clone from one it did
	// not see.
	failed bool
}

// Begin starts a pass. A nil Guard gives a pass that only keeps tables'
// maintenance away from files outside their own location.
func (g *Guard) Begin() *Pass {
	return &Pass{g: g, start: time.Now(), live: map[uuid.UUID]table.Identifier{}, seen: map[string]bool{}}
}

func identKey(ident table.Identifier) string { return strings.Join(ident, "\x00") }

// Saw notes a table the pass loaded, and records the shares it declares.
func (p *Pass) Saw(ctx context.Context, tbl *table.Table) {
	p.seen[identKey(tbl.Identifier())] = true
	p.live[tbl.Metadata().TableUUID()] = tbl.Identifier()
	if p.g == nil {
		return
	}
	locs := declaredShares(tbl.Properties())
	if len(locs) == 0 {
		return
	}
	ident := tbl.Identifier()
	shares := make([]Share, len(locs))
	for i, l := range locs {
		shares[i] = Share{Clone: tbl.Metadata().TableUUID(), Location: l,
			Schema: strings.Join(icecat.NamespaceFromIdent(ident), "."), Table: icecat.TableNameFromIdent(ident)}
	}
	if err := p.g.store.Add(ctx, shares); err != nil {
		p.g.log.Warn("could not record a clone's shared files; keeping every table's files this pass",
			"table", strings.Join(ident, "."), "err", err)
		p.failed = true
	}
}

// Missing notes that a table the pass listed was gone when it was loaded.
func (p *Pass) Missing() { p.missing = true }

// Failed notes that a table could not be loaded.
func (p *Pass) Failed() { p.failed = true }

// Settle runs once every listed table was loaded. If some were gone, it
// lists the tables again and loads those the pass has not seen, which
// finds tables renamed during the pass.
func (p *Pass) Settle(ctx context.Context) {
	if p.g == nil || !p.missing || p.failed {
		return
	}
	idents, err := p.g.cat.Tables(ctx)
	if err != nil {
		p.failed = true
		return
	}
	for _, ident := range idents {
		if p.seen[identKey(ident)] {
			continue
		}
		tbl, err := p.g.cat.LoadTable(ctx, ident)
		if err != nil {
			// Still changing: the next pass will know.
			p.failed = true
			continue
		}
		p.Saw(ctx, tbl)
	}
}

// Delete deletes those of files that are inside tbl's location and that
// no clone references, with remove, and returns them. It holds the lock of
// tbl's location meanwhile, so no clone of tbl can be made until it is
// done.
func (p *Pass) Delete(ctx context.Context, tbl *table.Table, files []string, remove func(context.Context, []string) error) ([]string, error) {
	loc := trimSlash(tbl.Location())
	var own []string
	for _, f := range files {
		if Under(f, loc) {
			own = append(own, f)
		}
	}
	if len(own) == 0 {
		return nil, nil
	}
	if p.g == nil {
		return own, remove(ctx, own)
	}
	release, ok, err := p.g.store.Lock(ctx, []string{loc}, false)
	if err != nil {
		return nil, fmt.Errorf("lock the table's files: %w", err)
	}
	if !ok {
		return nil, ErrBusy
	}
	defer release()
	needed, err := p.needed(ctx, tbl, loc)
	if err != nil {
		return nil, err
	}
	var deletable []string
	for _, f := range own {
		if !needed.has(f) {
			deletable = append(deletable, f)
		}
	}
	if kept := len(own) - len(deletable); kept > 0 {
		p.g.log.Info("kept files that clones share", "table", strings.Join(tbl.Identifier(), "."), "files", kept)
	}
	if len(deletable) == 0 {
		return nil, nil
	}
	return deletable, remove(ctx, deletable)
}

// needed returns the files under loc that clones of tbl reference.
func (p *Pass) needed(ctx context.Context, tbl *table.Table, loc string) (fileSet, error) {
	shares, err := p.g.store.Sharers(ctx, loc)
	if err != nil {
		return nil, fmt.Errorf("list the table's clones: %w", err)
	}
	out := fileSet{}
	if len(shares) == 0 {
		return out, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	r := newReader(fs)
	r.within = loc
	for _, sh := range shares {
		c, err := p.sharer(ctx, sh)
		if errors.Is(err, ErrUnsure) {
			return nil, fmt.Errorf("%w: %s.%s", ErrUnsure, sh.Schema, sh.Table)
		}
		if err != nil {
			return nil, fmt.Errorf("load clone %s.%s, so the table's files are kept: %w", sh.Schema, sh.Table, err)
		}
		if c == nil {
			p.g.log.Info("a clone that shared files was dropped", "table", strings.Join(tbl.Identifier(), "."),
				"clone", sh.Schema+"."+sh.Table)
			if err := p.g.store.Remove(ctx, sh.Clone, sh.Location); err != nil {
				return nil, err
			}
			continue
		}
		if err := r.referenced(c.Metadata(), out); err != nil {
			return nil, fmt.Errorf("read the metadata of clone %s.%s: %w", sh.Schema, sh.Table, err)
		}
	}
	return out, nil
}

// sharer loads the clone of a share. It returns nil for a clone that is
// gone: one the pass did not see and the catalog does not have under its
// last name, recorded long enough ago to have been in the catalog when the
// pass listed it.
func (p *Pass) sharer(ctx context.Context, sh Share) (*table.Table, error) {
	load := func(ident table.Identifier) (*table.Table, bool, error) {
		tbl, err := p.g.cat.LoadTable(ctx, ident)
		switch {
		case errors.Is(err, icecat.ErrNoSuchTable):
			return nil, false, nil
		case err != nil:
			return nil, false, err
		}
		return tbl, tbl.Metadata().TableUUID() == sh.Clone, nil
	}
	if ident, ok := p.live[sh.Clone]; ok {
		tbl, same, err := load(ident)
		if err != nil || same {
			return tbl, err
		}
		return nil, ErrUnsure
	}
	tbl, same, err := load(table.Identifier{sh.Schema, sh.Table})
	if err != nil || same {
		return tbl, err
	}
	if !p.failed && p.start.Sub(sh.Created) > settle {
		return nil, nil
	}
	return nil, ErrUnsure
}

// Release forgets the locations a clone no longer references anything
// under, after its snapshots expired: maintenance of the tables there need
// not read its metadata any more.
func (p *Pass) Release(ctx context.Context, tbl *table.Table) error {
	if p.g == nil {
		return nil
	}
	id := tbl.Metadata().TableUUID()
	locs := declaredShares(tbl.Properties())
	recorded, err := p.g.store.Of(ctx, id)
	if err != nil {
		return err
	}
	for _, s := range recorded {
		locs = append(locs, s.Location)
	}
	slices.Sort(locs)
	locs = slices.Compact(locs)
	if len(locs) == 0 {
		return nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return err
	}
	refs := fileSet{}
	if err := newReader(fs).referenced(tbl.Metadata(), refs); err != nil {
		return err
	}
	var still, done []string
	for _, l := range locs {
		if refs.under(l) {
			still = append(still, l)
		} else {
			done = append(done, l)
		}
	}
	if len(done) == 0 {
		return nil
	}
	// The property goes first: Saw records shares from it again.
	update := table.Update(table.NewRemovePropertiesUpdate([]string{SharesProperty}))
	if len(still) > 0 {
		b, _ := json.Marshal(still)
		update = table.NewSetPropertiesUpdate(map[string]string{SharesProperty: string(b)})
	}
	if _, _, err := p.g.cat.CommitTable(ctx, tbl.Identifier(), []table.Requirement{table.AssertTableUUID(id)}, []table.Update{update}); err != nil {
		return err
	}
	for _, l := range done {
		if err := p.g.store.Remove(ctx, id, l); err != nil {
			return err
		}
	}
	p.g.log.Info("a clone no longer shares files", "table", strings.Join(tbl.Identifier(), "."), "locations", len(done))
	return nil
}

// Sweep forgets the shares of clones that are gone, such as clones of
// tables that were dropped, whose locations no maintenance visits.
func (p *Pass) Sweep(ctx context.Context) {
	if p.g == nil || p.failed {
		return
	}
	all, err := p.g.store.All(ctx)
	if err != nil {
		p.g.log.Warn("could not list clones' shared files", "err", err)
		return
	}
	for _, sh := range all {
		if _, ok := p.live[sh.Clone]; ok {
			continue
		}
		if c, err := p.sharer(ctx, sh); c != nil || err != nil {
			continue
		}
		if err := p.g.store.Remove(ctx, sh.Clone, sh.Location); err != nil {
			p.g.log.Warn("could not forget a dropped clone's shared files", "clone", sh.Schema+"."+sh.Table, "err", err)
		}
	}
}
