package ddl

import (
	"context"
	"strings"

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
