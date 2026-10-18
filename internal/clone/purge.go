package clone

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	icecat "github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"gocloud.dev/gcerrors"
)

// iglooDir holds Igloo's own files in the warehouse, which are no table's.
const iglooDir = ".igloo"

// Purging has the Manager delete the files of the tables clones replace
// (see Purge). warehouse is where the warehouse's files are, with a
// trailing slash, and storage opens it with Igloo's own credentials;
// without it the replaced table's credentials are used. With warehouse
// "", tables clones replace keep their files.
func (m *Manager) Purging(warehouse string, storage func(ctx context.Context, location string) (iceio.IO, error)) {
	m.warehouse, m.storage = warehouse, storage
}

// Purge deletes the files of the tables clones replaced (Result.Replaced),
// which the catalog leaves when a clone takes a table's name, as a drop
// that no clone's files hold back does (see pgwire's catalogguard.go). It
// deletes the files under each table's own location, and only those: the
// files it referenced elsewhere, those of the table it was cloned from if
// it was a clone, belong to other tables. And it deletes them only if
//   - the location is in the warehouse, outside Igloo's own files there;
//   - no table of the catalog is there, or within it or around it, as
//     the replaced table is if it was renamed before the clone took its
//     name;
//   - no table says it reads files there (SharesProperty), and no clone
//     the Store records does (Guard.Readers), whose lock Purge holds
//     while it deletes, so that no clone starts reading them meanwhile.
//
// Otherwise the files stay, and Purge logs why; erasure requests delete
// what is left of tables dropped or replaced (package erasure). It logs
// its failures too: the clones were made all the same.
func (m *Manager) Purge(ctx context.Context, replaced ...*table.Table) {
	if m.warehouse == "" {
		return
	}
	ctx = context.WithoutCancel(ctx)
	var todo []*table.Table
	for _, old := range replaced {
		if old == nil {
			continue
		}
		name, loc := strings.Join(old.Identifier(), "."), trimSlash(old.Location())
		switch {
		case loc == "":
			m.log.Warn("could not tell where a table a clone replaced keeps its files; it is replaced without them", "table", name)
		case !Under(loc, m.warehouse) || Under(loc+"/", m.warehouse+iglooDir):
			m.log.Info("a table a clone replaced outside the warehouse keeps its files", "table", name, "location", loc)
		default:
			todo = append(todo, old)
		}
	}
	if len(todo) == 0 {
		return
	}
	tables, err := m.catalogTables(ctx)
	if err != nil {
		for _, old := range todo {
			m.log.Warn("could not tell whether other tables use the files of a table a clone replaced; it is replaced without them",
				"table", strings.Join(old.Identifier(), "."), "location", trimSlash(old.Location()), "err", err)
		}
		return
	}
	for _, old := range todo {
		m.purge(ctx, old, tables)
	}
}

// catalogTable is where a table of the catalog is, and where it reads.
type catalogTable struct {
	name  string
	own   string
	reads []string
}

// catalogTables loads every table of the catalog. A table listed but gone
// when loaded may have been renamed to a name not listed, so the catalog
// is read again; if it changes again, catalogTables gives up.
func (m *Manager) catalogTables(ctx context.Context) ([]catalogTable, error) {
	for attempt := 0; ; attempt++ {
		out, err := m.loadTables(ctx)
		if !errors.Is(err, ErrUnsure) || attempt == 2 {
			return out, err
		}
	}
}

func (m *Manager) loadTables(ctx context.Context) ([]catalogTable, error) {
	idents, err := m.cat.Tables(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]catalogTable, 0, len(idents))
	for _, ident := range idents {
		tbl, err := m.cat.LoadTable(ctx, ident)
		if errors.Is(err, icecat.ErrNoSuchTable) {
			return nil, fmt.Errorf("%w: the catalog changed while it was read", ErrUnsure)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, catalogTable{name: strings.Join(ident, "."), own: trimSlash(tbl.Location()), reads: declaredShares(tbl.Properties())})
	}
	return out, nil
}

// purge deletes the files under old's location, unless a table of tables
// is there or reads there, or a clone recorded in the Store reads there.
func (m *Manager) purge(ctx context.Context, old *table.Table, tables []catalogTable) {
	name, loc := strings.Join(old.Identifier(), "."), trimSlash(old.Location())
	overlaps := func(l string) bool {
		return canonical(trimSlash(l)) == canonical(loc) || Under(l, loc) || Under(loc, l)
	}
	for _, t := range tables {
		if overlaps(t.own) {
			m.log.Warn("a table a clone replaced keeps its files: a table of the catalog is where they are",
				"table", name, "location", loc, "other", t.name)
			return
		}
		for _, r := range t.reads {
			if overlaps(r) {
				m.log.Info("a table a clone replaced keeps its files: clones read them", "table", name, "location", loc, "clones", t.name)
				return
			}
		}
	}
	names, release, err := (&Guard{cat: m.cat, store: m.store, log: m.log}).Readers(ctx, loc)
	if err != nil {
		m.log.Warn("could not tell whether clones read the files of a table a clone replaced; it is replaced without them",
			"table", name, "location", loc, "err", err)
		return
	}
	defer release()
	if len(names) > 0 {
		m.log.Info("a table a clone replaced keeps its files: clones read them", "table", name, "location", loc,
			"clones", strings.Join(names, ", "))
		return
	}
	n, err := m.deleteUnder(ctx, old, loc)
	if err != nil {
		m.log.Warn("could not delete all the files of a table a clone replaced", "table", name, "location", loc, "deleted", n, "err", err)
		return
	}
	m.log.Info("deleted the files of a table a clone replaced", "table", name, "location", loc, "files", n)
}

// deleteUnder deletes the files under loc, old's location, and returns
// how many it deleted.
func (m *Manager) deleteUnder(ctx context.Context, old *table.Table, loc string) (int, error) {
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
	if bulk, ok := fsys.(iceio.BulkRemovableIO); ok {
		deleted, err := bulk.DeleteFiles(ctx, files)
		return len(deleted), err
	}
	n := 0
	for _, p := range files {
		if err := fsys.Remove(p); err != nil && !notExist(err) {
			return n, err
		}
		n++
	}
	return n, nil
}

func notExist(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || gcerrors.Code(err) == gcerrors.NotFound
}
