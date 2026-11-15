package snowflake

import "strings"

// duckdbKeywords are DuckDB's keywords, in lower case, by their category in
// duckdb_keywords(): reserved, type_function, column_name or unreserved
// (see keywords.go).
var duckdbKeywords = func() map[string]string {
	m := map[string]string{}
	for category, words := range duckdbKeywordList {
		for _, w := range strings.Fields(words) {
			m[w] = category
		}
	}
	return m
}()

// notTableName reports whether DuckDB's grammar cannot read the word w,
// unquoted, as a table's name or alias, a CTE's name, a name in their
// column lists or a column's qualifier. It reads there an identifier, an
// unreserved keyword or a column-name keyword (PostgreSQL's ColId), not a
// reserved keyword or a type or function name keyword: AT, LEFT, SEMI.
func notTableName(w string) bool {
	switch duckdbKeywords[strings.ToLower(w)] {
	case "reserved", "type_function":
		return true
	}
	return false
}

// notBareLabel reports whether DuckDB's grammar cannot read the word w,
// unquoted and without AS, as a select item's alias: there it reads only
// an identifier, not any keyword, where after AS it reads any word.
func notBareLabel(w string) bool {
	_, ok := duckdbKeywords[strings.ToLower(w)]
	return ok
}

// quoteKeywordNames quotes, in place, at every level of a statement, the
// names that Snowflake reads as identifiers where DuckDB reads a keyword:
//
//   - A table's alias, with or without AS, after FROM, JOIN, a comma in a
//     FROM clause, UPDATE, MERGE INTO, and USING in MERGE and DELETE; a CTE's
//     name; the names in their column lists; a table named by one word,
//     such as a CTE; and a column's qualifier, a word before a dot that no
//     dot or colon comes before. FROM t at CROSS JOIN u failed with "syntax
//     error at or near CROSS": DuckDB read AT as the start of its AT
//     (VERSION => ...), where Snowflake's AT clause takes a parenthesis, and
//     it reads t semi JOIN u, an inner join in Snowflake, as a semi join.
//     These are quoted as notTableName says, a table's name in upper case,
//     as Snowflake reads it.
//   - A select item's alias without AS, as notBareLabel says, which DuckDB
//     refuses (SELECT a year FROM t) or reads as an operator (a notnull). It
//     is quoted as written, the name DuckDB would give its column, which
//     CREATE TABLE ... AS keeps.
//
// DuckDB matches names without regard to case, quoted or not, so a quoted
// name names what the word would. A word Snowflake reads as a clause where
// an alias could be (t WHERE, t AT (...), t ASOF JOIN) stays as it is. Each
// quoted name is marked bare: the translation reads it as the unquoted word
// it was (see aliasKey and upperAlias). GRANT and REVOKE are left as they
// are: they name privileges (SELECT) and roles, not aliases, and Igloo reads
// them itself.
func quoteKeywordNames(nodes []*node) {
	if w := firstSig(nodes).word(); w == "GRANT" || w == "REVOKE" {
		return
	}
	var walk func(nodes []*node, joins bool)
	walk = func(nodes []*node, joins bool) {
		joined := quoteNamesAt(nodes, joins)
		for _, n := range nodes {
			if n.kind == kGroup {
				walk(n.kids, joined[n])
			}
		}
	}
	walk(nodes, false)
}

// quoteNamesAt quotes the names at one level of a statement (see
// quoteKeywordNames). joins says the level is a parenthesized join of
// tables, (a JOIN b ON ...), which starts with a table reference. It
// returns the groups at that level that are such joins.
func quoteNamesAt(nodes []*node, joins bool) map[*node]bool {
	idx := sig(nodes)
	at := func(k int) *node {
		if k >= 0 && k < len(idx) {
			return nodes[idx[k]]
		}
		return nil
	}
	// quote quotes a table's name or alias, in upper case, and nameList the
	// names of a column list, as written.
	quote := func(k int) {
		if n := at(k); n != nil && n.kind == kWord && notTableName(n.tok.text) {
			nodes[idx[k]] = bareQuoted(n, strings.ToUpper(n.tok.text))
		}
	}
	nameList := func(g *node) {
		for _, item := range splitArgs(g.kids) {
			if i := sig(item); len(i) == 1 && item[i[0]].kind == kWord && notTableName(item[i[0]].tok.text) {
				item[i[0]] = bareQuoted(item[i[0]], item[i[0]].tok.text)
			}
		}
	}
	// Qualifiers.
	for k := range idx {
		if at(k+1).isOp(".") && !at(k-1).isOp(".") && !at(k-1).isOp(":") {
			quote(k)
		}
	}
	// alias reads the alias that may come at idx[k], after a table
	// reference, and returns the index just past it.
	alias := func(k int) int {
		a := k
		if at(k).word() == "AS" {
			a = k + 1
		} else if !aliasAfterTable(at(k), at(k+1)) {
			return k
		}
		if n := at(a); n == nil || n.kind != kWord && n.kind != kQuoted {
			return k
		}
		quote(a)
		if g := at(a + 1); g != nil && g.isGroup('(') {
			nameList(g)
			return a + 2
		}
		return a + 1
	}
	joined := map[*node]bool{}
	// table reads the table reference at idx[k] and its alias, and returns
	// the index just past them.
	table := func(k int) int {
		if at(k).word() == "LATERAL" {
			k++
		}
		switch n := at(k); {
		case n == nil:
			return k
		case n.isGroup('('):
			if f := firstSig(n.kids); f != nil && (f.kind == kQuoted || f.kind == kWord && !startsQuery(n) && f.word() != "TABLE") {
				joined[n] = true
			}
			k++
		case n.word() == "TABLE" && at(k+1).isGroup('('):
			k += 2
		case n.kind == kWord || n.kind == kQuoted:
			start := k
			for k++; at(k).isOp(".") && at(k+1) != nil && (at(k+1).kind == kWord || at(k+1).kind == kQuoted); k += 2 {
			}
			if at(k).isGroup('(') {
				k++ // a table function's arguments
			} else if k == start+1 {
				quote(start) // a table named by one word, as a CTE is
			}
		default:
			return k
		}
		// Time travel: AT (...), BEFORE (...), CHANGES (...) and END (...).
		for {
			switch at(k).word() {
			case "AT", "BEFORE", "CHANGES", "END":
				if at(k + 1).isGroup('(') {
					k += 2
					continue
				}
			}
			break
		}
		return alias(k)
	}
	first := at(0).word()
	expect, from, selected := joins, joins, false
	for k := 0; k < len(idx); k++ {
		if expect {
			expect = false
			if next := table(k); next > k {
				k = next - 1
				continue
			}
		}
		n := at(k)
		w := n.word()
		switch {
		case w == "SELECT":
			selected, from = true, false
			quoteBareLabels(nodes, idx[k])
		case w == "FROM" && (selected || first == "UPDATE" || at(k-1).word() == "DELETE") && at(k-1).word() != "DISTINCT",
			w == "JOIN",
			w == "UPDATE" && k == 0,
			w == "INTO" && at(k-1).word() == "MERGE",
			w == "USING" && (first == "MERGE" || first == "DELETE") && !(at(k+1).isGroup('(') && !startsQuery(at(k+1))):
			expect, from = true, true
		case n.isOp(",") && from:
			expect = true
		case w == "WITH":
			k = cteNames(at, k+1, quote, nameList) - 1
		case (w == "PIVOT" || w == "UNPIVOT") && at(k+1).isGroup('('):
			k = alias(k+2) - 1
		case fromEnds[w]:
			from = false
		}
	}
	return joined
}

// fromEnds are the words that end a FROM clause, after which a comma no
// longer starts a table reference.
var fromEnds = map[string]bool{
	"WHERE": true, "GROUP": true, "HAVING": true, "QUALIFY": true, "ORDER": true, "LIMIT": true, "OFFSET": true,
	"FETCH": true, "WINDOW": true, "UNION": true, "EXCEPT": true, "MINUS": true, "INTERSECT": true, "SET": true,
	"WHEN": true,
}

// aliasAfterTable reports whether n, after a table reference and before
// next, is the table's alias written without AS: a name, unless Snowflake
// reads it as a clause there. AT, BEFORE, CHANGES and END are clauses only
// before a parenthesis (see clauses), and ASOF only before JOIN. SEMI, ANTI
// and POSITIONAL, which start joins in DuckDB, are names in Snowflake.
func aliasAfterTable(n, next *node) bool {
	switch {
	case n == nil:
		return false
	case n.kind == kQuoted:
		return true
	case n.kind != kWord:
		return false
	}
	switch w := n.word(); w {
	case "AT", "BEFORE", "CHANGES", "END":
		return !next.isGroup('(')
	case "ASOF":
		return next.word() != "JOIN"
	case "SEMI", "ANTI", "POSITIONAL":
		return true
	default:
		return !notAliasWords[w] && !itemEnds[w] && !fromEnds[w] && w != "MATCH_RECOGNIZE" && w != "CONNECT" && w != "START" &&
			w != "DIRECTED"
	}
}

// cteNames quotes the names of a WITH clause's CTEs, and the names in their
// column lists, from idx[k], just past WITH: [RECURSIVE] name [(columns)]
// AS [[NOT] MATERIALIZED] (...), and the next after a comma. It returns the
// index just past the last CTE it read, or k when what follows is not one.
func cteNames(at func(int) *node, k int, quote func(int), nameList func(*node)) int {
	if at(k).word() == "RECURSIVE" {
		k++
	}
	for {
		name := at(k)
		if name == nil || name.kind != kWord && name.kind != kQuoted {
			return k
		}
		j := k + 1
		var cols *node
		if at(j).isGroup('(') {
			cols = at(j)
			j++
		}
		if at(j).word() != "AS" {
			return k
		}
		j++
		if at(j).word() == "NOT" {
			j++
		}
		if at(j).word() == "MATERIALIZED" {
			j++
		}
		if !at(j).isGroup('(') {
			return k
		}
		quote(k)
		if cols != nil {
			nameList(cols)
		}
		k = j + 1
		if !at(k).isOp(",") {
			return k
		}
		k++
	}
}

// quoteBareLabels quotes the aliases written without AS of the select list
// of the SELECT at nodes[sel] that DuckDB cannot read so (see notBareLabel),
// as written.
func quoteBareLabels(nodes []*node, sel int) {
	start := sel + 1
	if q := nextSig(nodes, sel); q >= 0 && (nodes[q].word() == "DISTINCT" || nodes[q].word() == "ALL") {
		start = q + 1
	}
	if q := nextSig(nodes, start-1); q >= 0 && nodes[q].word() == "TOP" {
		if n := nextSig(nodes, q); n >= 0 {
			start = n + 1
		}
	}
	end := len(nodes)
	for j := start; j < len(nodes); j++ {
		w := nodes[j].word()
		if (w == "GROUP" || w == "ORDER") && !nodes[nextSigOr(nodes, j)].isWord("BY") {
			continue // WITHIN GROUP (...)
		}
		if selectEnd[w] || nodes[j].isOp(";") {
			end = j
			break
		}
	}
	for _, item := range splitArgs(nodes[start:end]) {
		if i, bare := selectAlias(item); i >= 0 && bare && item[i].kind == kWord && notBareLabel(item[i].tok.text) {
			item[i] = bareQuoted(item[i], item[i].tok.text)
		}
	}
}

// bareQuoted is n, a word, as the quoted identifier name, marked bare (see
// token.bare).
func bareQuoted(n *node, name string) *node {
	return &node{kind: kQuoted, tok: token{kind: kQuoted, text: quoteIdent(name), val: name, pos: n.pos, end: n.end, bare: true},
		pos: n.pos, end: n.end}
}

// firstSig is the first significant node of nodes, or nil.
func firstSig(nodes []*node) *node {
	if i := sig(nodes); len(i) > 0 {
		return nodes[i[0]]
	}
	return nil
}

// startsQuery reports whether the group g holds a query: (SELECT ...),
// (WITH ...) or (VALUES ...).
func startsQuery(g *node) bool {
	switch firstSig(g.kids).word() {
	case "SELECT", "WITH", "VALUES":
		return true
	}
	return false
}
