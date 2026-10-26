package ddl

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/apache/iceberg-go/table"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// RewriteViewRefs points references to views, such as lake.sales.v and
// sales.v, at their local copies, which DuckDB can run. Unqualified names
// need no rewriting: sessions' search path includes the default schema's
// local copies. Literals, comments and everything else are left as written.
func (m *Manager) RewriteViewRefs(query string) string {
	if len(*m.known.Load()) == 0 {
		return query
	}
	return m.rewrite(query)
}

func (m *Manager) rewrite(query string) string {
	local := quoteIdent(m.eng.LocalDatabase())
	query = rewriteNames(query, func(catalog, schema, name string) bool {
		return strings.EqualFold(catalog, m.Catalog()) && m.IsView(schema, name)
	}, local)
	// The catalog's tables by the catalog's spelling of their names.
	// DuckDB-Iceberg names its entry for a table as it was last looked up,
	// so a view whose body spells a table another way had every hold of the
	// catalog lock look the table up again by the catalog's name, before it
	// lists anything (see the engine's respell), and every bind of the view
	// spell it back.
	query = respellNames(query, func(catalog, schema, name string) (string, bool) {
		if !strings.EqualFold(catalog, m.Catalog()) {
			return "", false
		}
		return m.eng.TableSpelling([]string{schema}, name)
	})
	return qualifyNames(query, m.IsView, local)
}

// qualifyTables puts the catalog's name before the schema-qualified names
// of its tables in a view's SQL, so sales.orders is stored as
// lake.sales.orders. DuckDB binds a view's schema.name in the view's own
// database, and the local copies of views are in a database without the
// tables. PostgreSQL also fixes a view's tables when it is created.
func (m *Manager) qualifyTables(ctx context.Context, query string) (string, error) {
	var pairs []key
	qualifyNames(query, func(schema, name string) bool {
		pairs = append(pairs, key{schema, name})
		return false
	}, "")
	if len(pairs) == 0 {
		return query, nil
	}
	// Most pairs are alias.column; only those naming a schema can be tables.
	namespaces, err := m.cat.ListNamespaces(ctx, nil)
	if err != nil {
		return "", err
	}
	schemas := map[string]bool{}
	for _, ns := range namespaces {
		if len(ns) == 1 {
			schemas[ns[0]] = true
		}
	}
	tables := map[key]bool{}
	for _, p := range pairs {
		if _, seen := tables[p]; seen {
			continue
		}
		tables[p] = false
		if !schemas[p.schema] || m.IsView(p.schema, p.name) {
			continue
		}
		exists, err := m.cat.CheckTableExists(ctx, catalog.Ident(p.schema, p.name))
		if err != nil {
			return "", err
		}
		tables[p] = exists
	}
	return qualifyNames(query, func(schema, name string) bool {
		return tables[key{schema, name}]
	}, quoteIdent(m.Catalog())), nil
}

// qualifyOwnSchema puts the view's schema before the unqualified names in
// query, a view's SQL, of the tables and views in schema, the view's own:
// a view in sales that reads orders reads sales.orders, as in Snowflake and
// as the view's Iceberg default namespace (its schema) says, wherever it is
// loaded. Tables get the catalog's name too, as qualifyTables gives them.
// Without this a view created by a session whose search path found orders
// in sales could not be loaded anywhere else: replicas load their local
// copies of views on a connection whose search path is the default schema.
// The names of the query's CTEs are left alone, and so are names schema has
// no table or view of, which resolve as before. conn parses query.
func (m *Manager) qualifyOwnSchema(ctx context.Context, conn *duckdb.Conn, schema, query string) (string, error) {
	refs, err := unqualifiedTables(ctx, conn, query)
	if err != nil || len(refs) == 0 {
		return query, err
	}
	tables := map[string]bool{}
	for ident, err := range m.cat.ListTables(ctx, table.Identifier{schema}) {
		if err != nil {
			return query, err
		}
		tables[strings.ToLower(ident[len(ident)-1])] = true
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].at > refs[j].at })
	for _, r := range refs {
		var prefix string
		switch {
		case tables[strings.ToLower(r.name)]:
			prefix = quoteIdent(m.Catalog()) + "." + quoteIdent(schema) + "."
		case m.IsView(schema, r.name):
			prefix = quoteIdent(schema) + "."
		default:
			continue
		}
		query = query[:r.at] + prefix + query[r.at:]
	}
	return query, nil
}

// tableRef is a table name as written in a query, and its byte offset.
type tableRef struct {
	name string
	at   int
}

// unqualifiedTables returns the tables a query reads that it names without
// a schema, other than its CTEs, as DuckDB's parser finds them. A query
// DuckDB cannot parse has none: running it reports why.
func unqualifiedTables(ctx context.Context, conn *duckdb.Conn, query string) ([]tableRef, error) {
	rows, err := conn.QueryContext(ctx, "SELECT json_serialize_sql($1::VARCHAR)", []driver.NamedValue{{Ordinal: 1, Value: query}})
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}
	var tree any
	switch v := dest[0].(type) {
	case string:
		err = json.Unmarshal([]byte(v), &tree)
	case []byte:
		err = json.Unmarshal(v, &tree)
	default:
		tree = v
	}
	if err != nil {
		return nil, err
	}
	if t, ok := tree.(map[string]any); ok && t["error"] == true {
		return nil, nil
	}
	ctes := map[string]bool{}
	var refs []tableRef
	var walk func(any)
	walk = func(n any) {
		switch n := n.(type) {
		case map[string]any:
			if ctm, ok := n["cte_map"].(map[string]any); ok {
				entries, _ := ctm["map"].([]any)
				for _, e := range entries {
					if e, ok := e.(map[string]any); ok {
						if name, ok := e["key"].(string); ok {
							ctes[strings.ToLower(name)] = true
						}
					}
				}
			}
			if n["type"] == "BASE_TABLE" && n["catalog_name"] == "" && n["schema_name"] == "" {
				name, _ := n["table_name"].(string)
				at, ok := n["query_location"].(float64)
				if name != "" && ok && at >= 0 && int(at) < len(query) && namedAt(query, int(at), name) {
					refs = append(refs, tableRef{name: name, at: int(at)})
				}
			}
			for _, v := range n {
				walk(v)
			}
		case []any:
			for _, v := range n {
				walk(v)
			}
		}
	}
	walk(tree)
	refs = slices.DeleteFunc(refs, func(r tableRef) bool { return ctes[strings.ToLower(r.name)] })
	return refs, nil
}

// namedAt reports whether query has name, quoted or not, at byte at.
func namedAt(query string, at int, name string) bool {
	rest := query[at:]
	if strings.HasPrefix(rest, `"`) {
		return strings.HasPrefix(rest, quoteIdent(name))
	}
	return len(rest) >= len(name) && strings.EqualFold(rest[:len(name)], name)
}

// qualifyNames puts prefix and a dot before every two-part name
// schema.name that match accepts. Parts of longer names are left alone.
func qualifyNames(query string, match func(schema, name string) bool, prefix string) string {
	toks := nameTokens(query)
	isName := func(i int) bool { return i >= 0 && i < len(toks) && !toks[i].dot && !toks[i].brk }
	isDot := func(i int) bool { return i >= 0 && i < len(toks) && toks[i].dot }

	var out strings.Builder
	last := 0
	for i := range toks {
		if !isName(i) || isDot(i-1) || !isDot(i+1) || !isName(i+2) || isDot(i+3) {
			continue
		}
		if !match(toks[i].name, toks[i+2].name) {
			continue
		}
		out.WriteString(query[last:toks[i].start])
		out.WriteString(prefix + ".")
		last = toks[i].start
	}
	if last == 0 {
		return query
	}
	out.WriteString(query[last:])
	return out.String()
}

// token is an identifier or a dot in a query; anything else is a break.
type token struct {
	start, end int
	name       string
	dot, brk   bool
}

// rewriteNames replaces the first part of every three-part name
// catalog.schema.name that match accepts with replacement.
func rewriteNames(query string, match func(catalog, schema, name string) bool, replacement string) string {
	toks := nameTokens(query)
	isName := func(i int) bool { return i < len(toks) && !toks[i].dot && !toks[i].brk }
	isDot := func(i int) bool { return i < len(toks) && toks[i].dot }

	var out strings.Builder
	last := 0
	for i := range toks {
		if !isName(i) || (i > 0 && isDot(i-1)) {
			continue
		}
		if !isDot(i+1) || !isName(i+2) || !isDot(i+3) || !isName(i+4) {
			continue
		}
		if !match(toks[i].name, toks[i+2].name, toks[i+4].name) {
			continue
		}
		out.WriteString(query[last:toks[i].start])
		out.WriteString(replacement)
		last = toks[i].end
	}
	if last == 0 {
		return query
	}
	out.WriteString(query[last:])
	return out.String()
}

// respellNames writes the last part of every three-part name
// catalog.schema.name as spell returns it, quoted, where spell knows it and
// it spells the name another way.
func respellNames(query string, spell func(catalog, schema, name string) (string, bool)) string {
	toks := nameTokens(query)
	isName := func(i int) bool { return i < len(toks) && !toks[i].dot && !toks[i].brk }
	isDot := func(i int) bool { return i < len(toks) && toks[i].dot }

	var out strings.Builder
	last := 0
	for i := range toks {
		if !isName(i) || (i > 0 && isDot(i-1)) {
			continue
		}
		if !isDot(i+1) || !isName(i+2) || !isDot(i+3) || !isName(i+4) {
			continue
		}
		spelled, ok := spell(toks[i].name, toks[i+2].name, toks[i+4].name)
		if !ok || spelled == toks[i+4].name {
			continue
		}
		out.WriteString(query[last:toks[i+4].start])
		out.WriteString(quoteIdent(spelled))
		last = toks[i+4].end
	}
	if last == 0 {
		return query
	}
	out.WriteString(query[last:])
	return out.String()
}

// nameTokens lists the identifiers and dots in query, in order, with breaks
// for anything else that could separate them. Whitespace and comments do
// not break a name: lake . public . v is one name.
func nameTokens(query string) []token {
	var toks []token
	pos := 0
	for _, s := range sqllex.Segments(query) {
		start := pos
		pos += len(s.Text)
		switch s.Kind {
		case sqllex.Ident:
			name := s.Text
			if len(name) >= 2 {
				name = strings.ReplaceAll(name[1:len(name)-1], `""`, `"`)
			}
			toks = append(toks, token{start: start, end: pos, name: name})
		case sqllex.String:
			toks = append(toks, token{brk: true})
		case sqllex.Code:
			for i := 0; i < len(s.Text); {
				c := s.Text[i]
				switch {
				case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
					i++
				case c == '.':
					toks = append(toks, token{start: start + i, end: start + i + 1, dot: true})
					i++
				case sqllex.IdentByte(c) && !(c >= '0' && c <= '9'):
					j := i
					for j < len(s.Text) && sqllex.IdentByte(s.Text[j]) {
						j++
					}
					toks = append(toks, token{start: start + i, end: start + j, name: s.Text[i:j]})
					i = j
				default:
					toks = append(toks, token{brk: true})
					i++
				}
			}
		}
	}
	return toks
}
