package pgwire

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Shared subqueries. Generated reports often select twice from the same
// derived table, as in a top-N and a bottom-N of one report:
//
//	SELECT * FROM (SELECT ... FROM (<report>) AS t0 WHERE ... ORDER BY x DESC LIMIT 10) AS a
//	UNION SELECT * FROM (SELECT ... FROM (<report>) AS t0 WHERE ... ORDER BY x LIMIT 5) AS b
//
// DuckDB plans each copy on its own and computes the report twice: its
// common_subplan optimizer misses copies that hold common table expressions
// of their own, or that have different filters pushed into them. Igloo
// computes such a subquery once instead, as a common table expression
// marked MATERIALIZED that the copies read:
//
//	WITH _igloo_shared_1 AS MATERIALIZED (<report>)
//	SELECT * FROM (SELECT ... FROM _igloo_shared_1 AS t0 WHERE ... LIMIT 10) AS a UNION ...
//
// A subquery is shared when it is a derived table (a parenthesized query
// right after FROM or JOIN, not LATERAL) whose text repeats, ignoring case
// outside literals and quoted names, white space and comments, and when
// computing it once gives each copy the rows it would have had:
//
//   - It calls no volatile function (random(), uuid(), nextval() and the
//     like, or a macro that calls one) and samples nothing.
//   - The tables it names resolve as they did: none is named like a common
//     table expression defined outside it, which the copies might have seen
//     and the shared one would not. Common table expressions it defines
//     itself go with it.
//   - It refers to no column of the query around it. DuckDB runs a derived
//     table that does as a lateral join, and binding the shared one then
//     fails, which leaves the statement as written (see prepareChecked).
//   - The order of its rows does not show: a materialized common table
//     expression does not keep the order its query had, so each copy must be
//     read by a SELECT that orders or groups its rows again, with no window
//     function, which could number them in the order they come.
//   - The statement has no ? parameters, whose numbering follows the text.
//
// Statements are rewritten after Igloo's checks of the files and functions
// they name, before DuckDB prepares them; everything after that, policies
// and grants above all, sees the statement as it runs. The shared copy names
// nothing the copies did not. Query history keeps the statement as the
// client sent it.

// sharedPasses bounds the passes over one statement: each shares the
// outermost repeated subqueries, and the next ones repeated inside those.
const sharedPasses = 4

// sharedPrefix starts the names of the common table expressions shared
// subqueries become.
const sharedPrefix = "_igloo_shared_"

// shareSubqueries returns query with each subquery it repeats computed
// once, or "" when there is none to share.
func (s *Server) shareSubqueries(ctx context.Context, sess *session, query string) string {
	if sess == nil || sess.conn == nil {
		return ""
	}
	parse := func(q string) (any, error) { return parseStatement(ctx, sess.conn, q) }
	volatileNames := func() (map[string]bool, error) {
		rows, err := queryStrings(ctx, sess, volatileFunctionsQuery)
		if err != nil {
			return nil, err
		}
		return volatileSet(rows), nil
	}
	out, err := shareRepeated(query, parse, volatileNames)
	if err != nil {
		s.log.Debug("could not share repeated subqueries", "err", err)
		return ""
	}
	return out
}

// volatileFunctionsQuery lists DuckDB's volatile functions (one name a row)
// and, after them, each macro that is not built in as its name, a tab and
// its definition.
const volatileFunctionsQuery = `SELECT system.main.lower(function_name) FROM system.main.duckdb_functions()
	WHERE stability = 'VOLATILE'
	UNION ALL
	SELECT system.main.lower(function_name) || chr(9) || coalesce(macro_definition, '') FROM system.main.duckdb_functions()
	WHERE function_type IN ('macro', 'table_macro') AND NOT internal`

// volatileSet reads volatileFunctionsQuery's rows: the volatile functions,
// and the macros that call one, directly or through other macros. A macro
// counts as calling a function when its definition has the function's name
// as a word, which errs toward volatile.
func volatileSet(rows []string) map[string]bool {
	out := map[string]bool{}
	for k := range volatile {
		out[k] = true
	}
	macros := map[string][]string{}
	for _, r := range rows {
		name, def, isMacro := strings.Cut(r, "\t")
		if !isMacro {
			out[name] = true
			continue
		}
		for _, w := range sqllex.CodeWords(def) {
			macros[name] = append(macros[name], strings.ToLower(w))
		}
	}
	for changed := true; changed; {
		changed = false
		for name, words := range macros {
			if out[name] {
				continue
			}
			for _, w := range words {
				if out[w] {
					out[name], changed = true, true
					break
				}
			}
		}
	}
	return out
}

// shareRepeated rewrites text, a statement, so that each subquery it
// repeats is computed once, parsing with parse as json_serialize_sql does.
// volatileNames returns the functions whose calls keep a subquery as
// written; it is called only when there is a subquery to share. It returns
// "" when there is nothing to share or the statement is not a query.
func shareRepeated(text string, parse func(string) (any, error), volatileNames func() (map[string]bool, error)) (string, error) {
	prefix, body := explainPrefix(text)
	var vol map[string]bool
	changed := false
	for range sharedPasses {
		groups := repeatedSubqueries(body)
		if len(groups) == 0 {
			break
		}
		if vol == nil {
			var err error
			if vol, err = volatileNames(); err != nil {
				return "", err
			}
		}
		tree, err := parse(body)
		if err != nil {
			// Not a query, or not one statement.
			break
		}
		next, ok := shareGroups(body, tree, groups, vol)
		if !ok {
			break
		}
		body, changed = next, true
	}
	if !changed {
		return "", nil
	}
	return prefix + body, nil
}

// explainPrefix splits off EXPLAIN [ANALYZE] [(options)] from the start of
// text.
func explainPrefix(text string) (prefix, body string) {
	toks := codeTokens(text)
	if len(toks) == 0 || toks[0].word != "EXPLAIN" {
		return "", text
	}
	i := 1
	if i < len(toks) && (toks[i].word == "ANALYZE" || toks[i].word == "ANALYSE") {
		i++
	}
	if i < len(toks) && toks[i].punct == '(' {
		end := matchParen(text, toks[i].start)
		if end < 0 {
			return "", text
		}
		return text[:end+1], text[end+1:]
	}
	if i >= len(toks) {
		return "", text
	}
	return text[:toks[i].start], text[toks[i].start:]
}

// codeTok is a word or punctuation character of SQL code, where it is in a
// text. Literals, quoted names and comments are not tokens.
type codeTok struct {
	start, end int
	// word is an unquoted word, upper-cased; punct a punctuation character.
	word  string
	punct byte
}

// codeTokens splits the SQL code of text into words and punctuation.
func codeTokens(text string) []codeTok {
	masked := maskCode(text)
	var out []codeTok
	for i := 0; i < len(masked); {
		c := masked[i]
		switch {
		case sqllex.IdentByte(c):
			j := i
			for j < len(masked) && sqllex.IdentByte(masked[j]) {
				j++
			}
			out = append(out, codeTok{start: i, end: j, word: strings.ToUpper(masked[i:j])})
			i = j
		case isSpace(c):
			i++
		default:
			out = append(out, codeTok{start: i, end: i + 1, punct: c})
			i++
		}
	}
	return out
}

// subquerySpan is a derived table's parenthesized query, text[start:end],
// parentheses included; tok is the index of its opening parenthesis among
// the text's tokens.
type subquerySpan struct {
	start, end, tok int
}

// repeatedSubqueries finds the derived tables of text whose queries repeat,
// grouped by their text, longest first. Statements with ? parameters have
// none.
func repeatedSubqueries(text string) [][]subquerySpan {
	toks := codeTokens(text)
	byKey := map[string][]subquerySpan{}
	var keys []string
	for i, t := range toks {
		if t.punct == '?' {
			return nil
		}
		if t.punct != '(' || i == 0 || i+1 >= len(toks) {
			continue
		}
		if prev := toks[i-1].word; prev != "FROM" && prev != "JOIN" {
			continue
		}
		switch toks[i+1].word {
		case "SELECT", "WITH", "FROM":
		default:
			continue
		}
		end := matchParen(text, t.start)
		if end < 0 {
			continue
		}
		key := normalizedQuery(text[t.start : end+1])
		if _, ok := byKey[key]; !ok {
			keys = append(keys, key)
		}
		byKey[key] = append(byKey[key], subquerySpan{start: t.start, end: end + 1, tok: i})
	}
	var groups [][]subquerySpan
	for _, k := range keys {
		if g := byKey[k]; len(g) > 1 && !overlapping(g) {
			groups = append(groups, g)
		}
	}
	// Longest first, so a group inside another is seen after it.
	sort.SliceStable(groups, func(i, j int) bool {
		return groups[i][0].end-groups[i][0].start > groups[j][0].end-groups[j][0].start
	})
	return groups
}

// overlapping reports whether spans overlap one another.
func overlapping(spans []subquerySpan) bool {
	for i, a := range spans {
		for _, b := range spans[i+1:] {
			if a.start < b.end && b.start < a.end {
				return true
			}
		}
	}
	return false
}

// normalizedQuery is a query's text as it compares with others: code
// upper-cased, each stretch of white space and comments one space, and
// literals and quoted names as written.
func normalizedQuery(text string) string {
	var b strings.Builder
	space := false
	write := func(s string) {
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteString(s)
	}
	for _, seg := range sqllex.Segments(text) {
		switch seg.Kind {
		case sqllex.Comment:
			space = true
		case sqllex.Code:
			for i := 0; i < len(seg.Text); {
				if isSpace(seg.Text[i]) {
					space = true
					i++
					continue
				}
				j := i
				for j < len(seg.Text) && !isSpace(seg.Text[j]) {
					j++
				}
				write(strings.ToUpper(seg.Text[i:j]))
				i = j
			}
		default:
			write(seg.Text)
		}
	}
	return b.String()
}

// parsedQuery is what shareGroups needs of a statement's parse.
type parsedQuery struct {
	// ctes are the common table expressions defined anywhere, by
	// lower-cased name, each with where its query starts in the text (-1 if
	// that is not known).
	ctes []parsedCTE
	// tables are the one-part table names (lower-cased) and where they are.
	tables []namedAt
	// functions are the functions and macros called, lower-cased.
	functions []namedAt
	recursive bool
}

type parsedCTE struct {
	name  string
	start int
}

type namedAt struct {
	name string
	at   int
}

// readParse collects a statement's parse for shareGroups.
func readParse(tree any) parsedQuery {
	var pq parsedQuery
	walkMaps(tree, func(m map[string]any) {
		if str(m["type"]) == "RECURSIVE_CTE_NODE" {
			pq.recursive = true
		}
		if cm, ok := m["cte_map"].(map[string]any); ok {
			for _, e := range asList(cm["map"]) {
				em, _ := e.(map[string]any)
				value, _ := em["value"].(map[string]any)
				pq.ctes = append(pq.ctes, parsedCTE{name: strings.ToLower(str(em["key"])), start: minLocation(value["query"])})
			}
		}
		switch str(m["type"]) {
		case "BASE_TABLE":
			if str(m["catalog_name"]) == "" && str(m["schema_name"]) == "" {
				pq.tables = append(pq.tables, namedAt{strings.ToLower(str(m["table_name"])), location(m["query_location"])})
			}
		case "TABLE_FUNCTION":
			if f, ok := m["function"].(map[string]any); ok {
				pq.functions = append(pq.functions, namedAt{strings.ToLower(str(f["function_name"])), location(f["query_location"])})
			}
		}
		if str(m["class"]) == "FUNCTION" || str(m["class"]) == "WINDOW" || str(m["class"]) == "AGGREGATE" {
			if name := str(m["function_name"]); name != "" {
				pq.functions = append(pq.functions, namedAt{strings.ToLower(name), location(m["query_location"])})
			}
		}
	})
	return pq
}

// minLocation returns the first place in the text that part of a parse is
// at, or -1.
func minLocation(n any) int {
	first := -1
	walkMaps(n, func(m map[string]any) {
		if at := location(m["query_location"]); at >= 0 && (first < 0 || at < first) {
			first = at
		}
	})
	return first
}

// shareGroups rewrites text, whose parse is tree, to compute each group of
// repeated subqueries that may be shared once. It reports false when it
// shares none.
func shareGroups(text string, tree any, groups [][]subquerySpan, vol map[string]bool) (string, bool) {
	pq := readParse(tree)
	if pq.recursive {
		return "", false
	}
	toks := codeTokens(text)
	in := func(at int, g []subquerySpan) bool {
		for _, sp := range g {
			if at >= sp.start && at < sp.end {
				return true
			}
		}
		return false
	}
	var edits []edit
	var defs []string
	var shared [][]subquerySpan
	taken := strings.ToLower(text)
	n := 0
	for _, g := range groups {
		// One inside a group shared already is shared, if it repeats there,
		// on the next pass.
		inside := false
		for _, o := range shared {
			for _, sp := range g {
				inside = inside || in(sp.start, o)
			}
		}
		if inside || !shareable(toks, g, pq, vol, in) {
			continue
		}
		shared = append(shared, g)
		var name string
		for {
			n++
			name = fmt.Sprintf("%s%d", sharedPrefix, n)
			if !strings.Contains(taken, name) {
				break
			}
		}
		for _, sp := range g {
			edits = append(edits, edit{sp.start, sp.end, name})
		}
		// The query ends a line, in case it ends with a -- comment.
		q := text[g[0].start+1 : g[0].end-1]
		defs = append(defs, name+" AS MATERIALIZED ("+q+"\n)")
	}
	if len(defs) == 0 {
		return "", false
	}
	// The shared queries name no common table expression defined outside
	// them, so they come first.
	list := strings.Join(defs, ", ")
	if len(toks) > 0 && toks[0].word == "WITH" {
		at := toks[0].end
		if len(toks) > 1 && toks[1].word == "RECURSIVE" {
			at = toks[1].end
		}
		edits = append(edits, edit{at, at, " " + list + ","})
	} else {
		edits = append(edits, edit{0, 0, "WITH " + list + "\n"})
	}
	out, err := applyEdits(text, edits)
	return out, err == nil
}

// shareable reports whether the repeated subqueries g may be computed once
// (see the comment at the top).
func shareable(toks []codeTok, g []subquerySpan, pq parsedQuery, vol map[string]bool,
	in func(int, []subquerySpan) bool) bool {
	first := g[0]
	reads := false
	for _, t := range toks {
		if t.start < first.start || t.start >= first.end {
			continue
		}
		switch t.word {
		case "FROM":
			reads = true
		case "SAMPLE", "TABLESAMPLE":
			return false
		}
	}
	if !reads {
		return false
	}
	for _, f := range pq.functions {
		if vol[f.name] && (f.at < 0 || in(f.at, g)) {
			return false
		}
	}
	// Names of common table expressions defined outside the copies.
	outside := map[string]bool{}
	for _, c := range pq.ctes {
		if c.start < 0 {
			// Where it is is unknown: count it as outside.
			outside[c.name] = true
		} else if !in(c.start, g) {
			outside[c.name] = true
		}
	}
	for _, t := range pq.tables {
		if outside[t.name] && (t.at < 0 || in(t.at, g)) {
			return false
		}
	}
	for _, sp := range g {
		if !reordered(toks, sp) {
			return false
		}
	}
	return true
}

// reordered reports whether the SELECT that reads the derived table sp
// orders or groups its rows again, after it at its own depth (an ORDER BY
// after a UNION, EXCEPT or INTERSECT orders the set operation's rows; a
// GROUP BY there is another SELECT's), and whether no window function is
// at that depth, so that the order the derived table's rows come in does
// not show.
func reordered(toks []codeTok, sp subquerySpan) bool {
	// The start of the parenthesized expression the derived table is in
	// (or of the text).
	depth, from := 0, 0
	for i := sp.tok - 1; i >= 0; i-- {
		switch toks[i].punct {
		case ')':
			depth++
		case '(':
			depth--
		}
		if depth < 0 {
			from = i + 1
			break
		}
	}
	ordered, setOp := false, false
	depth = 0
	for i := from; i < len(toks); i++ {
		t := toks[i]
		switch {
		case t.punct == '(':
			depth++
		case t.punct == ')':
			depth--
		case t.punct == ';':
			depth = -1
		}
		if depth < 0 {
			break
		}
		if depth > 0 || t.start < sp.end {
			if depth == 0 && t.word == "OVER" {
				return false
			}
			continue
		}
		by := i+1 < len(toks) && toks[i+1].word == "BY"
		switch t.word {
		case "OVER":
			return false
		case "UNION", "EXCEPT", "INTERSECT":
			setOp = true
		case "ORDER":
			ordered = ordered || by
		case "GROUP":
			ordered = ordered || by && !setOp
		}
	}
	return ordered
}
