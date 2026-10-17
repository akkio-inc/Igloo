package erasure

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/duckdb/duckdb-go/v2"
)

// Engine opens DuckDB connections that no policy applies to: an erasure
// must see the values masking policies hide, and every row. *engine.Engine
// is one.
type Engine interface {
	Connect(ctx context.Context) (*duckdb.Conn, error)
}

// Rows deletes and counts a table's rows that match a condition, in SQL on
// the table's current columns.
type Rows interface {
	Delete(ctx context.Context, tbl *table.Table, cond string) (int64, error)
	Count(ctx context.Context, tbl *table.Table, cond string) (int64, error)
}

// catalogRows runs DELETE and SELECT on the catalog's tables in DuckDB,
// as any statement would: a DELETE writes delete files.
type catalogRows struct {
	engine  Engine
	catalog string
}

// maxCommitRetries bounds how often a DELETE that loses a commit race with
// another writer is run again, as Igloo does for clients.
const maxCommitRetries = 20

func (c catalogRows) name(tbl *table.Table) string {
	parts := append([]string{c.catalog}, tbl.Identifier()...)
	for i, p := range parts {
		parts[i] = quoteIdent(p)
	}
	return strings.Join(parts, ".")
}

func (c catalogRows) Delete(ctx context.Context, tbl *table.Table, cond string) (int64, error) {
	conn, err := c.engine.Connect(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close() //nolint:errcheck
	backoff := 20 * time.Millisecond
	for attempt := 0; ; attempt++ {
		res, err := conn.ExecContext(ctx, "DELETE FROM "+c.name(tbl)+" WHERE "+cond, nil)
		if err == nil {
			return res.RowsAffected()
		}
		if !commitConflict(err) || attempt == maxCommitRetries {
			return 0, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, time.Second)
	}
}

func (c catalogRows) Count(ctx context.Context, tbl *table.Table, cond string) (int64, error) {
	conn, err := c.engine.Connect(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close() //nolint:errcheck
	var n int64
	err = query(ctx, conn, "SELECT count(*) FROM "+c.name(tbl)+" WHERE "+cond, func(row []driver.Value) error {
		n, _ = row[0].(int64)
		return nil
	})
	return n, err
}

func commitConflict(err error) bool {
	var de *duckdb.Error
	return errors.As(err, &de) && de.Type == duckdb.ErrorTypeTransaction && strings.Contains(de.Msg, "CatalogCommitConflicts")
}

// scanColumn is a column to look for identifiers in: by field ID in files
// that have them, unless id is 0, and otherwise by the names it has had.
type scanColumn struct {
	id     int
	names  []string
	binary bool
	hashes []string
}

func scanColumns(flds []field, subjects []Subject) []scanColumn {
	var out []scanColumn
	for _, f := range flds {
		i := slices.IndexFunc(subjects, func(s Subject) bool { return s.Tag == f.Tag && s.Value == f.Value })
		if i < 0 {
			continue
		}
		out = append(out, scanColumn{id: f.id, names: f.names, binary: binaryType(f.typ), hashes: subjects[i].hashes(f.Digest)})
	}
	return out
}

// condition is the SQL condition a row matches on: the salted hash of one
// of its columns, compared as identifiers are, is one of that column's
// hashes. refs are the columns as the statement names them.
func condition(salt string, cols []scanColumn, refs []string) string {
	var parts []string
	for i, c := range cols {
		value := "lower(trim(CAST(" + refs[i] + " AS VARCHAR)))"
		if c.binary {
			value = "lower(hex(" + refs[i] + "))"
		}
		hashes := make([]string, len(c.hashes))
		for j, h := range c.hashes {
			hashes[j] = quoteLiteral(h)
		}
		parts = append(parts, "sha256("+quoteLiteral(salt)+" || "+value+") IN ("+strings.Join(hashes, ", ")+")")
	}
	if len(parts) == 0 {
		return ""
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

// currentCondition is the condition on a table's current columns, those
// to be nulled too if withNull, or "" when none of them is left.
func currentCondition(salt string, flds []field, subjects []Subject, withNull bool) string {
	var (
		cols []scanColumn
		refs []string
	)
	for _, f := range flds {
		if f.current == "" || f.Null && !withNull {
			continue
		}
		c := scanColumns([]field{f}, subjects)
		if len(c) == 1 {
			cols, refs = append(cols, c[0]), append(refs, quoteIdent(f.current))
		}
	}
	return condition(salt, cols, refs)
}

// scanBatch is how many files one scan reads.
const scanBatch = 200

// fileLayout is where a Parquet file keeps the columns scanned for.
type fileLayout struct {
	// ids says the file has field IDs, as Iceberg's files do; otherwise
	// columns are found by name.
	ids bool
	// names are the file's top-level columns, by lower-case name, with
	// their field IDs.
	names map[string]int
}

// layouts reads the schemas of Parquet files.
func layouts(ctx context.Context, conn *duckdb.Conn, paths []string) (map[string]*fileLayout, error) {
	out := map[string]*fileLayout{}
	err := query(ctx, conn, "SELECT file_name, name, field_id FROM parquet_schema("+pathList(paths)+")", func(row []driver.Value) error {
		file, _ := row[0].(string)
		name, _ := row[1].(string)
		l := out[file]
		if l == nil {
			l = &fileLayout{names: map[string]int{}}
			out[file] = l
		}
		var id int
		switch v := row[2].(type) {
		case int32:
			id, l.ids = int(v), true
		case int64:
			id, l.ids = int(v), true
		}
		l.names[strings.ToLower(name)] = id
		return nil
	})
	return out, err
}

// matchingFiles returns, for each of paths, which are Parquet files, how
// many of its rows match on cols, deleted or not, leaving out files with
// none.
func (s *Service) matchingFiles(ctx context.Context, salt string, paths []string, cols []scanColumn) (map[string]int64, error) {
	out := map[string]int64{}
	if len(paths) == 0 || len(cols) == 0 {
		return out, nil
	}
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck
	for batch := range slices.Chunk(paths, scanBatch) {
		original := map[string]string{}
		for _, p := range batch {
			original[duckdbPath(p)] = p
		}
		files, err := layouts(ctx, conn, batch)
		if err != nil {
			return nil, err
		}
		count := func(stmt string) error {
			return query(ctx, conn, stmt, func(row []driver.Value) error {
				file, _ := row[0].(string)
				n, _ := row[1].(int64)
				if p, ok := original[file]; ok {
					out[p] += n
				}
				return nil
			})
		}
		// Files with field IDs, in one scan that maps the IDs to names.
		var byID []string
		byName := map[string][]string{}
		for _, p := range batch {
			l := files[duckdbPath(p)]
			switch {
			case l == nil:
			case l.ids && !slices.ContainsFunc(cols, func(c scanColumn) bool { return c.id == 0 }):
				byID = append(byID, p)
			default:
				var present []string
				for _, c := range cols {
					if name := c.nameIn(l); name != "" {
						present = append(present, name)
					}
				}
				if len(present) > 0 {
					key := strings.Join(present, "\x00")
					byName[key] = append(byName[key], p)
				}
			}
		}
		if len(byID) > 0 {
			var (
				entries []string
				refs    []string
			)
			for i, c := range cols {
				typ := "VARCHAR"
				if c.binary {
					typ = "BLOB"
				}
				entries = append(entries, fmt.Sprintf("%d: {name: 'c%d', type: '%s', default_value: NULL}", c.id, i, typ))
				refs = append(refs, fmt.Sprintf("c%d", i))
			}
			stmt := "SELECT filename, count(*) FROM read_parquet(" + pathList(byID) + ", filename = true, schema = MAP {" +
				strings.Join(entries, ", ") + "}) WHERE " + condition(salt, cols, refs) + " GROUP BY ALL"
			if err := count(stmt); err != nil {
				return nil, err
			}
		}
		// Files without, by the names the columns have had.
		for _, group := range byName {
			l := files[duckdbPath(group[0])]
			var (
				present []scanColumn
				refs    []string
			)
			for _, c := range cols {
				if name := c.nameIn(l); name != "" {
					present, refs = append(present, c), append(refs, quoteIdent(name))
				}
			}
			stmt := "SELECT filename, count(*) FROM read_parquet(" + pathList(group) + ", filename = true, union_by_name = true) WHERE " +
				condition(salt, present, refs) + " GROUP BY ALL"
			if err := count(stmt); err != nil {
				return nil, err
			}
		}
	}
	for p, n := range out {
		if n == 0 {
			delete(out, p)
		}
	}
	return out, nil
}

// nameIn is the column's name in a file without field IDs, or "".
func (c scanColumn) nameIn(l *fileLayout) string {
	for _, n := range c.names {
		if _, ok := l.names[strings.ToLower(n)]; ok {
			return n
		}
	}
	return ""
}

// matchingStatistics counts the column chunks of Parquet files whose
// minimum or maximum, as the footer records them, is an identifier.
func (s *Service) matchingStatistics(ctx context.Context, m *matcher, paths []string, cols []scanColumn) (int64, error) {
	if len(paths) == 0 || len(cols) == 0 {
		return 0, nil
	}
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close() //nolint:errcheck
	var n int64
	for batch := range slices.Chunk(paths, scanBatch) {
		files, err := layouts(ctx, conn, batch)
		if err != nil {
			return 0, err
		}
		err = query(ctx, conn, "SELECT file_name, path_in_schema, stats_min_value, stats_max_value, stats_min, stats_max FROM parquet_metadata("+
			pathList(batch)+")", func(row []driver.Value) error {
			file, _ := row[0].(string)
			column, _ := row[1].(string)
			l := files[file]
			if l == nil || !slices.ContainsFunc(cols, func(c scanColumn) bool {
				id, ok := l.names[strings.ToLower(column)]
				return !c.binary && (l.ids && ok && id == c.id || !l.ids && slices.ContainsFunc(c.names, func(name string) bool {
					return strings.EqualFold(name, column)
				}))
			}) {
				return nil
			}
			for _, v := range row[2:] {
				if text, ok := v.(string); ok && m.matches(text) {
					n++
					return nil
				}
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	return n, nil
}

// query runs stmt and calls each with every row.
func query(ctx context.Context, conn *duckdb.Conn, stmt string, each func([]driver.Value) error) error {
	rows, err := conn.QueryContext(ctx, stmt, nil)
	if err != nil {
		return err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := each(dest); err != nil {
			return err
		}
	}
}

// duckdbPath is a path as DuckDB reports it back: without file://.
func duckdbPath(p string) string { return strings.TrimPrefix(p, "file://") }

func pathList(paths []string) string {
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = quoteLiteral(p)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
