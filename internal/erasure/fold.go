package erasure

import (
	"context"
	"database/sql/driver"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/deletefiles"
)

// folding is DuckDB's lower(trim(...)), as tables Go applies: the
// characters lower() maps to others, and those trim() removes. Column
// values compare as DuckDB normalizes them, so what Go compares, in
// history, in manifest bounds and in the rows a rewrite filters, is
// normalized by the same tables: Go's own differ from DuckDB's for
// characters newer Unicode versions added.
type folding struct {
	lower map[rune]string
	trim  map[rune]bool
}

// loadFolding reads DuckDB's tables, one code point at a time.
func loadFolding(ctx context.Context, engine Engine) (*folding, error) {
	conn, err := engine.Connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck
	f := &folding{lower: map[rune]string{}, trim: map[rune]bool{}}
	const points = "FROM range(1, 1114112) t(i) WHERE (i < 55296 OR i > 57343)"
	err = query(ctx, conn, "SELECT i, lower(chr(i::INTEGER)) "+points+" AND lower(chr(i::INTEGER)) <> chr(i::INTEGER)",
		func(row []driver.Value) error {
			i, _ := row[0].(int64)
			l, _ := row[1].(string)
			f.lower[rune(i)] = l
			return nil
		})
	if err != nil {
		return nil, err
	}
	err = query(ctx, conn, "SELECT i "+points+" AND trim(chr(i::INTEGER) || 'a' || chr(i::INTEGER)) = 'a'",
		func(row []driver.Value) error {
			i, _ := row[0].(int64)
			f.trim[rune(i)] = true
			return nil
		})
	return f, err
}

// normal is s as DuckDB's lower(trim(s)) leaves it. Without tables, Go's
// approximation stands in.
func (f *folding) normal(s string) string {
	if f == nil {
		return strings.ToLower(strings.TrimFunc(s, func(r rune) bool { return unicode.Is(unicode.Zs, r) }))
	}
	s = strings.TrimFunc(s, func(r rune) bool { return f.trim[r] })
	b := make([]byte, 0, len(s))
	for _, r := range s {
		b = f.appendLower(b, r)
	}
	return string(b)
}

// appendLower appends r lower-cased to b.
func (f *folding) appendLower(b []byte, r rune) []byte {
	if f == nil {
		return utf8.AppendRune(b, unicode.ToLower(r))
	}
	if l, ok := f.lower[r]; ok {
		return append(b, l...)
	}
	return utf8.AppendRune(b, r)
}

// canonical spells a file's location the one way locations compare in
// (see deletefiles.Canonical).
func canonical(p string) string { return deletefiles.Canonical(p) }

// pathSet is a set of file locations, which compare as canonical spells
// them, each as one of its spellings.
type pathSet map[string]string

func (s pathSet) add(p string)      { s[canonical(p)] = p }
func (s pathSet) has(p string) bool { _, ok := s[canonical(p)]; return ok }

// within reports whether p is under dir.
func within(p, dir string) bool {
	return strings.HasPrefix(canonical(p)+"/", canonical(dir)+"/") && canonical(p) != canonical(dir)
}

// dirSet is a set of directories of files, as canonical spells them.
type dirSet map[string]bool

func (d dirSet) add(p string) { d[path.Dir(canonical(p))] = true }

// under reports whether one of the directories is loc or under it.
func (d dirSet) under(loc string) bool {
	loc = canonical(loc)
	for dir := range d {
		if dir == loc || strings.HasPrefix(dir, loc+"/") {
			return true
		}
	}
	return false
}

// meets reports whether d and o have a directory in common.
func (d dirSet) meets(o dirSet) bool {
	for dir := range o {
		if d[dir] {
			return true
		}
	}
	return false
}

// tableDirs are the directories of the files a table's metadata
// references: those live in any snapshot, and all of them (see
// tableFiles.referenced).
type tableDirs struct{ live, referenced dirSet }

// maxDirs bounds how many tables' directories Service.dirs keeps.
const maxDirs = 10_000

// dirsOf returns the directories of the files tbl references, which tell
// whether it may reference a file without its manifests being read again:
// a metadata file never changes, so they are kept by its location. Every
// request looks through the catalog's other tables for files they share
// with its own, several times; most never change between requests.
func (s *Service) dirsOf(ctx context.Context, tbl *table.Table) (*tableDirs, error) {
	key := tbl.MetadataLocation()
	s.cacheMu.Lock()
	d, ok := s.dirs[key]
	s.cacheMu.Unlock()
	if ok && key != "" {
		return d, nil
	}
	tf, err := readFiles(ctx, tbl)
	if err != nil {
		return nil, err
	}
	d = &tableDirs{live: dirSet{}, referenced: dirSet{}}
	for p := range tf.live(false) {
		d.live.add(p)
	}
	for _, p := range tf.referenced() {
		d.referenced.add(p)
	}
	s.cacheMu.Lock()
	if len(s.dirs) >= maxDirs {
		clear(s.dirs)
	}
	s.dirs[key] = d
	s.cacheMu.Unlock()
	return d, nil
}
