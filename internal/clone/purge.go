package clone

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"sync"

	icecat "github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"gocloud.dev/gcerrors"
	"golang.org/x/sync/errgroup"
)

// iglooDir holds Igloo's own files in the warehouse, which are no table's.
const iglooDir = ".igloo"

// SetWarehouse says where the warehouse's files are, with a trailing
// slash: only tables there have their files deleted with them (see
// Purges.Decide). Without it, none has.
func (g *Guard) SetWarehouse(root string) { g.warehouse = root }

// PurgeVerdict says whether a table's files may be deleted with it: all
// those under its location, as the catalog's purge deletes them, and
// nothing elsewhere.
type PurgeVerdict struct {
	// Purge says they may. Release must then be called once they are
	// deleted, or once the drop that has the catalog delete them is
	// through: until then no clone of the table is made, as long as Held
	// says so. A caller that deletes the files itself checks Held before
	// each batch, and stops once it fails (see Guard.Readers).
	Purge   bool
	Release func()
	Held    func(context.Context) error
	// Otherwise Why says why they are kept, Clones names the clones that
	// read them if that is why, and Unsure says Igloo could not tell.
	Why    string
	Clones []string
	Unsure bool
}

// Purges decides whether tables' files may be deleted with them. It is
// the one decision for every way Igloo deletes or purges a table's
// files: a drop in SQL (CREATE OR REPLACE TABLE ... AS drops too), a drop
// through the Iceberg REST endpoint, and the table a clone replaces. It
// reads the catalog's tables once, for the first decision that needs them.
type Purges struct {
	g      *Guard
	once   sync.Once
	tables []catalogTable
	err    error
}

// Purges starts deciding whether tables' files may be deleted.
func (g *Guard) Purges() *Purges { return &Purges{g: g} }

// MayPurge decides for one table (see Purges.Decide).
func (g *Guard) MayPurge(ctx context.Context, what string, name table.Identifier, location string, inCatalog bool) PurgeVerdict {
	return g.Purges().Decide(ctx, what, name, location, inCatalog)
}

// Decide decides whether the files of the table called name, under
// location, may be deleted with it. inCatalog says the catalog still has
// it there, under that name, as for a drop not yet made. They are kept
// when
//   - the location is not in the warehouse, or is in Igloo's own files
//     there (.igloo/);
//   - a table of the catalog, other than this one, is at the location, or
//     within it or around it: the table itself, renamed, for one;
//   - a table says it reads files there (SharesProperty), as every clone
//     does whether or not the Store has its record yet;
//   - Readers names a clone that reads there, or may, as one being made;
//   - Igloo cannot tell any of these.
//
// Only the last two need nothing but the Store, which is what a drop
// asks when the Store is Authoritative: it has every clone, and the
// catalog (Lakekeeper) keeps live tables' locations apart, so no table
// but the one dropped is at, within or around its location. Otherwise,
// and for a table no longer in the catalog (a clone refresh's, which may
// have been renamed in the moment before the clone took its name), Decide
// loads every table of the catalog, once for all of p's decisions.
//
// Decide logs why it keeps them, naming what would have deleted them
// (what: "drop", "clone refresh"). The catalog is read before the lock
// is taken: a clone made since is in the Store, as making one records it
// under the lock before the catalog has it.
func (p *Purges) Decide(ctx context.Context, what string, name table.Identifier, location string, inCatalog bool) PurgeVerdict {
	g, tbl, loc := p.g, strings.Join(name, "."), trimSlash(location)
	keep := func(v PurgeVerdict, attrs ...any) PurgeVerdict {
		attrs = append([]any{"table", tbl, "location", loc, "by", what}, attrs...)
		if v.Unsure {
			g.log.Warn("could not tell whether a table's files may be deleted with it; they stay: "+v.Why, attrs...)
		} else {
			g.log.Info("a table's files stay: "+v.Why, attrs...)
		}
		return v
	}
	switch {
	case g.warehouse == "":
		return keep(PurgeVerdict{Why: "Igloo has no warehouse to delete files in"})
	case loc == "":
		return keep(PurgeVerdict{Why: "where they are is not known", Unsure: true})
	case !Under(loc, g.warehouse):
		return keep(PurgeVerdict{Why: "they are outside the warehouse"})
	case Under(loc+"/", g.warehouse+iglooDir):
		return keep(PurgeVerdict{Why: "they are among Igloo's own files (" + iglooDir + "/)"})
	}
	if v, ok := p.fromCatalog(ctx, tbl, loc, inCatalog); !ok {
		return keep(v)
	}
	names, lease, err := g.Readers(ctx, loc)
	if err != nil {
		return keep(PurgeVerdict{Why: "the clones that read them could not be told", Unsure: true}, "err", err)
	}
	if len(names) > 0 {
		lease.Release()
		return keep(PurgeVerdict{Why: "clones read them", Clones: names}, "clones", strings.Join(names, ", "))
	}
	return PurgeVerdict{Purge: true, Release: lease.Release, Held: lease.Held}
}

// fromCatalog checks, when the Store alone cannot tell (see Decide), that
// no table of the catalog but tbl itself (when inCatalog) is at, within
// or around loc, and that none says it reads files there; ok is false,
// and v says why, when one is or does, or the catalog cannot be read.
func (p *Purges) fromCatalog(ctx context.Context, tbl, loc string, inCatalog bool) (v PurgeVerdict, ok bool) {
	if inCatalog && p.g.store.Authoritative() {
		return PurgeVerdict{}, true
	}
	p.once.Do(func() { p.tables, p.err = p.g.catalogTables(ctx) })
	if p.err != nil {
		p.g.log.Warn("could not read the catalog's tables", "err", p.err)
		return PurgeVerdict{Why: "the catalog's tables could not be read", Unsure: true}, false
	}
	overlaps := func(l string) bool {
		return canonical(trimSlash(l)) == canonical(loc) || Under(l, loc) || Under(loc, l)
	}
	var clones []string
	for _, t := range p.tables {
		if inCatalog && t.name == tbl && canonical(t.own) == canonical(loc) {
			continue
		}
		if overlaps(t.own) {
			return PurgeVerdict{Why: "another table of the catalog, " + t.name + ", is where they are"}, false
		}
		if slices.ContainsFunc(t.reads, overlaps) {
			clones = append(clones, t.name)
		}
	}
	if len(clones) > 0 {
		return PurgeVerdict{Why: "clones read them (" + strings.Join(clones, ", ") + ")", Clones: clones}, false
	}
	return PurgeVerdict{}, true
}

// catalogTable is a table of the catalog: its name, where it is, and
// where it says it reads.
type catalogTable struct {
	name  string
	own   string
	reads []string
}

// catalogTables loads every table of the catalog. A table listed but gone
// when loaded may have been renamed to a name not listed, so the catalog
// is read again; if it changes again, catalogTables gives up.
func (g *Guard) catalogTables(ctx context.Context) ([]catalogTable, error) {
	for attempt := 0; ; attempt++ {
		out, err := g.loadTables(ctx)
		if !errors.Is(err, ErrUnsure) || attempt == 2 {
			return out, err
		}
	}
}

func (g *Guard) loadTables(ctx context.Context) ([]catalogTable, error) {
	idents, err := g.cat.Tables(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]catalogTable, len(idents))
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(8)
	for i, ident := range idents {
		eg.Go(func() error {
			tbl, err := g.cat.LoadTable(ctx, ident)
			if errors.Is(err, icecat.ErrNoSuchTable) {
				return fmt.Errorf("%w: the catalog changed while it was read", ErrUnsure)
			}
			if err != nil {
				return err
			}
			out[i] = catalogTable{name: strings.Join(ident, "."), own: trimSlash(tbl.Location()), reads: declaredShares(tbl.Properties())}
			return nil
		})
	}
	return out, eg.Wait()
}

// Purging has the Manager delete the files of the tables clones replace
// (see Purge), in warehouse, with storage, which opens it with Igloo's
// own credentials; without storage the replaced table's credentials are
// used. With warehouse "", tables clones replace keep their files.
func (m *Manager) Purging(warehouse string, storage func(ctx context.Context, location string) (iceio.IO, error)) {
	m.warehouse, m.storage = warehouse, storage
}

// Purge deletes the files of the tables clones replaced (Result.Replaced),
// which the catalog leaves when a clone takes a table's name, as a drop
// does: those under each table's own location, and only those, when
// Purges.Decide says they may go, holding the lock it takes until they
// are gone, and keeping those left if it loses it. The files a replaced
// clone read of its source are under the source's location, and stay.
// Otherwise the files stay, and Decide logs why; erasure requests delete
// what is left of tables dropped or replaced (package erasure). Purge logs
// its failures too: the clones were made all the same.
func (m *Manager) Purge(ctx context.Context, replaced ...*table.Table) {
	ctx = context.WithoutCancel(ctx)
	g := &Guard{cat: m.cat, store: m.store, log: m.log, warehouse: m.warehouse}
	p := g.Purges()
	for _, old := range replaced {
		if old == nil {
			continue
		}
		v := p.Decide(ctx, "clone refresh", old.Identifier(), old.Location(), false)
		if !v.Purge {
			continue
		}
		name, loc := strings.Join(old.Identifier(), "."), trimSlash(old.Location())
		n, err := m.deleteUnder(ctx, old, loc, v.Held)
		v.Release()
		if err != nil {
			m.log.Warn("could not delete all the files of a table a clone replaced", "table", name, "location", loc, "deleted", n, "err", err)
			continue
		}
		m.log.Info("deleted the files of a table a clone replaced", "table", name, "location", loc, "files", n)
	}
}

// deleteUnder deletes the files under loc, old's location, a batch at a
// time while held says the lock of loc is held, and returns how many it
// deleted.
func (m *Manager) deleteUnder(ctx context.Context, old *table.Table, loc string, held func(context.Context) error) (int, error) {
	var fsys iceio.IO
	var err error
	if m.storage != nil {
		fsys, err = m.storage(ctx, loc)
	} else {
		fsys, err = old.FS(ctx)
	}
	if err != nil {
		return 0, err
	}
	walker, ok := fsys.(iceio.ListableIO)
	if !ok {
		return 0, errors.New("the warehouse's storage cannot list files")
	}
	var files []string
	err = walker.WalkDir(loc, func(p string, d fs.DirEntry, err error) error {
		switch {
		case err != nil && notExist(err):
			return fs.SkipDir
		case err != nil:
			return err
		case !d.IsDir() && Under(p, loc):
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("list the files: %w", err)
	}
	n := 0
	_, err = deleteHeld(ctx, held, files, func(ctx context.Context, batch []string) error {
		if bulk, ok := fsys.(iceio.BulkRemovableIO); ok {
			deleted, err := bulk.DeleteFiles(ctx, batch)
			n += len(deleted)
			return err
		}
		for _, p := range batch {
			if err := fsys.Remove(p); err != nil && !notExist(err) {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}

func notExist(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || gcerrors.Code(err) == gcerrors.NotFound
}
