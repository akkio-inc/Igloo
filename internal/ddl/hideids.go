package ddl

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// visibleStar is a star over a table's columns other than those that hold ID
// domains' ids (see iddomain.HiddenColumn).
var visibleStar = "COLUMNS(lambda c: NOT starts_with(lower(c), " + quoteLiteral(iddomain.HiddenColumn("")) + "))"

// hideIDs makes query, a view's SQL, read each table it names through a
// subquery of the table's columns other than the columns of ids:
//
//	FROM lake.crm.people AS p  ->  FROM (SELECT COLUMNS(...) FROM lake.crm.people) AS p
//
// Statements read member tables as if they did not have those columns, a
// star included (docs/sql.md), and the local copy of a view must too: DuckDB
// expands a star in a view's body when it binds it, so without this a view's
// columns, and the names a column list gives them, would count the columns
// of ids when the view is created and not when statements expand it. Every
// table is wrapped, member or not, so that a table joining an ID domain
// later changes nothing. A table's version (AT) moves inside the subquery
// with it; names of the query's CTEs are left alone. A query DuckDB cannot
// parse is returned as is: creating the view reports why. UnhideIDs undoes
// this in a definition as DuckDB prints it.
func hideIDs(ctx context.Context, conn *duckdb.Conn, query string) (string, error) {
	refs, err := tableRefs(ctx, conn, query)
	if err != nil || len(refs) == 0 {
		return query, err
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].at > refs[j].at })
	for _, r := range refs {
		end, ok := nameEnd(query, r.at)
		if !ok {
			continue
		}
		name := query[r.at:end]
		if r.versioned {
			atStart, atEnd, ok := versionClause(query, end)
			if !ok {
				return "", errors.New("cannot place the version of table " + name + " in the view")
			}
			// The alias between the name and AT stays after the subquery.
			repl := "(SELECT " + visibleStar + " FROM " + name + " " + query[atStart:atEnd] + ")" + strings.TrimRight(query[end:atStart], " \t\r\n")
			if r.alias == "" {
				repl += " AS " + quoteIdent(r.name)
			}
			query = query[:r.at] + repl + query[atEnd:]
			continue
		}
		repl := "(SELECT " + visibleStar + " FROM " + name + ")"
		if r.alias == "" {
			// The table's own name stays usable as a qualifier.
			repl += " AS " + quoteIdent(r.name)
		}
		query = query[:r.at] + repl + query[end:]
	}
	return query, nil
}

// fromRef is a table a query reads, as DuckDB's parser finds it.
type fromRef struct {
	name      string
	alias     string
	at        int
	versioned bool
	// bareName says the name has no schema or catalog, as a CTE's.
	bareName bool
}

// tableRefs returns the tables query reads, by name, other than its CTEs.
func tableRefs(ctx context.Context, conn *duckdb.Conn, query string) ([]fromRef, error) {
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
	var refs []fromRef
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
			if n["type"] == "BASE_TABLE" {
				name, _ := n["table_name"].(string)
				alias, _ := n["alias"].(string)
				at, ok := n["query_location"].(float64)
				bare := n["catalog_name"] == "" && n["schema_name"] == ""
				if name != "" && ok && at >= 0 && int(at) < len(query) {
					refs = append(refs, fromRef{name: name, alias: alias, at: int(at), versioned: n["at_clause"] != nil, bareName: bare})
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
	out := refs[:0]
	for _, r := range refs {
		if r.bareName && ctes[strings.ToLower(r.name)] {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

// nameEnd returns where the dotted name starting at byte at in query ends.
func nameEnd(query string, at int) (int, bool) {
	toks := nameTokens(query[at:])
	if len(toks) == 0 || toks[0].dot || toks[0].brk || toks[0].start != 0 {
		return 0, false
	}
	end := toks[0].end
	for i := 1; i+1 < len(toks) && toks[i].dot && !toks[i+1].dot && !toks[i+1].brk; i += 2 {
		end = toks[i+1].end
	}
	return at + end, true
}

// versionClause finds AT (...) after a table's name ending at from, past
// an optional alias and column names: [AS] alias [(columns)] AT (...).
func versionClause(query string, from int) (start, end int, ok bool) {
	i := skipSpace(query, from)
	word := func(i int) (string, int) {
		j := i
		for j < len(query) && sqllex.IdentByte(query[j]) {
			j++
		}
		return query[i:j], j
	}
	if w, j := word(i); strings.EqualFold(w, "AS") {
		i = skipSpace(query, j)
	}
	if w, j := word(i); w != "" && !strings.EqualFold(w, "AT") {
		i = skipSpace(query, j)
	} else if i < len(query) && query[i] == '"' {
		j := i + 1
		for j < len(query) {
			if query[j] == '"' {
				if j+1 < len(query) && query[j+1] == '"' {
					j += 2
					continue
				}
				break
			}
			j++
		}
		i = skipSpace(query, j+1)
	}
	if i < len(query) && query[i] == '(' {
		close, ok := matchParen(query, i)
		if !ok {
			return 0, 0, false
		}
		i = skipSpace(query, close+1)
	}
	w, j := word(i)
	if !strings.EqualFold(w, "AT") {
		return 0, 0, false
	}
	k := skipSpace(query, j)
	if k >= len(query) || query[k] != '(' {
		return 0, 0, false
	}
	close, ok := matchParen(query, k)
	if !ok {
		return 0, 0, false
	}
	return i, close + 1, true
}

func skipSpace(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r' || s[i] == '\f') {
		i++
	}
	return i
}

// matchParen returns the index of the parenthesis closing the one at open,
// skipping string literals and quoted identifiers.
func matchParen(s string, open int) (int, bool) {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '\'', '"':
			q := s[i]
			i++
			for i < len(s) {
				if s[i] == q {
					if i+1 < len(s) && s[i+1] == q {
						i += 2
						continue
					}
					break
				}
				i++
			}
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return 0, false
}

// UnhideIDs undoes hideIDs in sql, a view's definition as DuckDB prints it,
// giving back the tables as the view's SQL names them, for code that reads
// a view's body and applies its own rules to the tables (Igloo's policy and
// bitmap rewrites read member tables without their columns of ids anyway).
func UnhideIDs(sql string) string {
	const open = "(SELECT COLUMNS("
	marker := quoteLiteral(iddomain.HiddenColumn(""))
	var out strings.Builder
	last := 0
	for i := 0; i < len(sql); {
		j := strings.Index(sql[i:], open)
		if j < 0 {
			break
		}
		start := i + j
		argsOpen := start + len(open) - 1
		argsClose, ok := matchParen(sql, argsOpen)
		if !ok {
			break
		}
		args := sql[argsOpen : argsClose+1]
		rest := sql[argsClose+1:]
		const from = " FROM "
		if !strings.Contains(args, "starts_with") || !strings.Contains(args, marker) || !strings.HasPrefix(rest, from) {
			i = start + len(open)
			continue
		}
		close, ok := matchParen(sql, start)
		if !ok {
			break
		}
		inner := strings.TrimSpace(sql[argsClose+1+len(from) : close])
		out.WriteString(sql[last:start])
		out.WriteString(inner)
		last = close + 1
		// hideIDs named an unaliased table's subquery after the table; the
		// table needs no alias of its own name.
		if n := addedAlias(sql[last:], inner); n > 0 {
			last += n
		}
		i = last
	}
	if last == 0 {
		return sql
	}
	out.WriteString(sql[last:])
	return out.String()
}

// addedAlias returns the length of " AS name" at the start of rest when name
// is the last part of the table name that inner, a table and perhaps its
// version, starts with; 0 otherwise.
func addedAlias(rest, inner string) int {
	const as = " AS "
	if !strings.HasPrefix(rest, as) {
		return 0
	}
	end, ok := nameEnd(inner, 0)
	if !ok {
		return 0
	}
	toks := nameTokens(inner[:end])
	table := toks[len(toks)-1].name
	aliasToks := nameTokens(rest[len(as):])
	if len(aliasToks) == 0 || aliasToks[0].dot || aliasToks[0].brk || aliasToks[0].start != 0 {
		return 0
	}
	if len(aliasToks) > 1 && aliasToks[1].dot {
		return 0
	}
	n := len(as) + aliasToks[0].end
	if !strings.EqualFold(aliasToks[0].name, table) || strings.HasPrefix(rest[n:], "(") {
		// Another name, or one that names the table's columns too.
		return 0
	}
	return n
}
