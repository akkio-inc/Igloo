package pgwire

import (
	"context"
	"regexp"
	"strings"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Whole counts. A table preview such as Acme's
//
//	SELECT * FROM (SELECT *, COUNT(*) OVER () AS total FROM t) SAMPLE (50 ROWS)
//
// makes DuckDB hold every row of t before it hands on the first: a window
// function's input is materialized whole, and COUNT(*) OVER () needs all
// of it. In a SELECT that groups nothing, aggregates nothing and has no
// other window function, COUNT(*) OVER () is on every row the number of
// rows that the SELECT's FROM and WHERE produce, so Igloo writes it as
// that number, (SELECT count(*) FROM ... WHERE ...), which DuckDB counts
// once, without holding the rows. FROM and WHERE are the client's text,
// copied, so names resolve as before; Igloo's checks and policies see both
// copies. Where the copy could produce other rows than the original (a
// sample, random(), LIMIT or a window function in the FROM, WHERE or a
// common table expression, a parameter), or the SELECT is anything else,
// the statement is left as written.

// emptyOver finds OVER () in SQL code, before parsing.
var emptyOver = regexp.MustCompile(`(?i)\bover\s*\(\s*\)`)

// wholeCountPasses bounds how many nested SELECTs one statement has
// rewritten, one a pass.
const wholeCountPasses = 8

// rewriteWholeCounts rewrites the whole counts of a SELECT statement.
func (s *Server) rewriteWholeCounts(ctx context.Context, sess *session, text string) string {
	if sess == nil || sess.conn == nil {
		return text
	}
	return wholeCounts(text, func(q string) (any, error) { return parseStatement(ctx, sess.conn, q) })
}

// wholeCounts rewrites the whole counts of text, a SELECT statement,
// which parse parses as json_serialize_sql does.
func wholeCounts(text string, parse func(string) (any, error)) string {
	if !strings.Contains(strings.ToLower(text), "count") || !emptyOver.MatchString(maskCode(text)) {
		return text
	}
	for range wholeCountPasses {
		tree, err := parse(text)
		if err != nil {
			return text
		}
		next, ok := rewriteWholeCount(text, tree)
		if !ok {
			return text
		}
		text = next
	}
	return text
}

// rewriteWholeCount rewrites the whole counts of one SELECT of text, whose
// parse is tree: one whose FROM and WHERE hold no other whole count, so
// that the copies hold the rewritten text.
func rewriteWholeCount(text string, tree any) (string, bool) {
	stmt, _ := tree.(map[string]any)
	if stmt == nil || hasNode(stmt, func(m map[string]any) bool { return str(m["type"]) == "RECURSIVE_CTE_NODE" }) {
		return "", false
	}
	// Common table expressions, wherever they are, may be read by a FROM.
	var ctes []any
	walkMaps(stmt, func(m map[string]any) {
		if cm, ok := m["cte_map"].(map[string]any); ok {
			for _, e := range asList(cm["map"]) {
				em, _ := e.(map[string]any)
				ctes = append(ctes, em["value"])
			}
		}
	})
	masked := maskCode(text)
	var found []*wholeCount
	walkMaps(stmt, func(m map[string]any) {
		if str(m["type"]) == "SELECT_NODE" {
			if wc := findWholeCount(text, masked, m, ctes); wc != nil {
				found = append(found, wc)
			}
		}
	})
	for _, wc := range found {
		inner := false
		for _, o := range found {
			for _, w := range o.windows {
				inner = inner || o != wc && w.start >= wc.from && w.start < wc.end
			}
		}
		if inner {
			continue
		}
		// Each copy ends a line, in case it ends with a -- comment.
		count := "(SELECT count(*) FROM " + strings.TrimSpace(text[wc.fromBody:wc.fromEnd]) + "\n"
		if wc.where >= 0 {
			count += "WHERE " + strings.TrimSpace(text[wc.where:wc.end]) + "\n"
		}
		count += ")"
		var edits []edit
		for _, w := range wc.windows {
			e := edit{w.start, w.end, count}
			if w.name != "" {
				e.text += " AS " + quoteIdent(w.name)
			}
			edits = append(edits, e)
		}
		out, err := applyEdits(text, edits)
		return out, err == nil
	}
	return "", false
}

// wholeCount is a SELECT whose COUNT(*) OVER () Igloo rewrites.
type wholeCount struct {
	windows []countSpan
	// from is where FROM is; the FROM clause's body is fromBody to
	// fromEnd, and the WHERE clause's, if where >= 0, where to end.
	from, fromBody, fromEnd, where, end int
}

// countSpan is one COUNT(*) OVER () in a text. name, when the select item
// has no alias, is the column name DuckDB gives it.
type countSpan struct {
	start, end int
	name       string
}

// findWholeCount returns the whole counts of the SELECT node n, or nil
// when it has none or they must stay as written.
func findWholeCount(text, masked string, n map[string]any, ctes []any) *wholeCount {
	from, _ := n["from_table"].(map[string]any)
	if from == nil || strings.HasPrefix(str(from["type"]), "EMPTY") ||
		len(asList(n["group_expressions"])) > 0 || len(asList(n["group_sets"])) > 0 ||
		n["having"] != nil || n["qualify"] != nil || n["sample"] != nil ||
		n["aggregate_handling"] != nil && str(n["aggregate_handling"]) != "STANDARD_HANDLING" {
		return nil
	}
	wc := &wholeCount{where: -1}
	aliases := map[string]bool{}
	for _, item := range asList(n["select_list"]) {
		m, _ := item.(map[string]any)
		if alias := str(m["alias"]); alias != "" {
			aliases[strings.ToLower(alias)] = true
		}
		if isWholeCount(m) {
			span, ok := countAt(masked, location(m["query_location"]))
			if !ok {
				return nil
			}
			if str(m["alias"]) == "" {
				span.name = strings.ToLower(str(m["function_name"])) + "() OVER ()"
			}
			wc.windows = append(wc.windows, span)
			continue
		}
		if !plainItem(m) {
			return nil
		}
	}
	if len(wc.windows) == 0 {
		return nil
	}
	for _, mod := range asList(n["modifiers"]) {
		m, _ := mod.(map[string]any)
		switch str(m["type"]) {
		case "ORDER_MODIFIER":
			for _, o := range asList(m["orders"]) {
				om, _ := o.(map[string]any)
				if e, _ := om["expression"].(map[string]any); !plainItem(e) {
					return nil
				}
			}
		case "LIMIT_MODIFIER", "LIMIT_PERCENT_MODIFIER":
		default:
			return nil
		}
	}
	where, _ := n["where_clause"].(map[string]any)
	// The rows FROM and WHERE produce must be the same each time they are
	// read.
	for _, part := range append([]any{from, where}, ctes...) {
		if !repeatable(part) {
			return nil
		}
	}
	// A WHERE may name a select item's alias, which the copy would not have.
	if where != nil && hasNode(where, func(m map[string]any) bool {
		names := asList(m["column_names"])
		return str(m["class"]) == "COLUMN_REF" && len(names) == 1 && aliases[strings.ToLower(str(names[0]))]
	}) {
		return nil
	}

	// The clauses' text: FROM after the select list, at its depth, which
	// is a whole count's.
	wc.from = keywordAfter(masked, wc.windows[len(wc.windows)-1].end, "FROM")
	if wc.from < 0 || maxLocation(n["select_list"]) >= wc.from {
		return nil
	}
	wc.fromBody = wc.from + len("FROM")
	stop, word := clauseEnd(masked, wc.fromBody)
	wc.fromEnd, wc.end = stop, stop
	if word == "WHERE" {
		wc.where = stop + len("WHERE")
		wc.end, _ = clauseEnd(masked, wc.where)
	}
	// Check them against the parse.
	if (where != nil) != (wc.where >= 0) || !within(from, wc.fromBody, wc.fromEnd) ||
		where != nil && !within(where, wc.where, wc.end) {
		return nil
	}
	return wc
}

// isWholeCount reports whether a select item is COUNT(*) OVER (), with
// nothing in the parentheses.
func isWholeCount(m map[string]any) bool {
	name := strings.ToLower(str(m["function_name"]))
	return str(m["class"]) == "WINDOW" && str(m["type"]) == "WINDOW_AGGREGATE" &&
		(name == "count" || name == "count_star") && str(m["schema"]) == "" && str(m["catalog"]) == "" &&
		len(asList(m["children"])) == 0 && len(asList(m["partitions"])) == 0 && len(asList(m["orders"])) == 0 &&
		len(asList(m["arg_orders"])) == 0 && m["filter_expr"] == nil && m["distinct"] != true && m["ignore_nulls"] != true &&
		str(m["start"]) == "UNBOUNDED_PRECEDING" && str(m["end"]) == "CURRENT_ROW_RANGE" &&
		(m["exclude_clause"] == nil || str(m["exclude_clause"]) == "NO_OTHER") &&
		m["start_expr"] == nil && m["end_expr"] == nil && m["offset_expr"] == nil && m["default_expr"] == nil
}

// plainItem reports whether an expression is made of columns, constants,
// casts and operators only: no function, which could be an aggregate and
// make the SELECT one, no window and no subquery.
func plainItem(e map[string]any) bool {
	if e == nil {
		return true
	}
	return !hasNode(e, func(m map[string]any) bool {
		switch str(m["class"]) {
		case "", "STAR", "COLUMN_REF", "CONSTANT", "CAST", "OPERATOR", "COMPARISON", "CONJUNCTION", "CASE", "BETWEEN", "COLLATE":
			return false
		case "FUNCTION":
			return m["is_operator"] != true
		}
		return true
	})
}

// volatile are DuckDB's functions whose result differs from one call to
// the next.
var volatile = map[string]bool{"random": true, "setseed": true, "uuid": true, "gen_random_uuid": true,
	"uuidv4": true, "uuidv7": true, "nextval": true, "currval": true}

// repeatable reports whether part of a parse gives the same rows each time
// it runs: no sample, volatile function, LIMIT (which may take any of the
// rows), window function (which may number ties either way) or parameter.
func repeatable(part any) bool {
	return !hasNode(part, func(m map[string]any) bool {
		switch {
		case m["sample"] != nil, volatile[strings.ToLower(str(m["function_name"]))]:
			return true
		case str(m["class"]) == "WINDOW", str(m["class"]) == "PARAMETER":
			return true
		}
		t := str(m["type"])
		return t == "LIMIT_MODIFIER" || t == "LIMIT_PERCENT_MODIFIER"
	})
}

// countAt reads COUNT(*) OVER () or count_star() OVER () at start in a
// masked text.
func countAt(masked string, start int) (countSpan, bool) {
	if start < 0 || start >= len(masked) {
		return countSpan{}, false
	}
	i := start
	word := func() string {
		i = skipSpace(masked, i)
		j := i
		for j < len(masked) && sqllex.IdentByte(masked[j]) {
			j++
		}
		w := masked[i:j]
		i = j
		return strings.ToLower(w)
	}
	punct := func(c byte) bool {
		i = skipSpace(masked, i)
		if i < len(masked) && masked[i] == c {
			i++
			return true
		}
		return false
	}
	switch word() {
	case "count":
		if !punct('(') || !punct('*') || !punct(')') {
			return countSpan{}, false
		}
	case "count_star":
		if !punct('(') || !punct(')') {
			return countSpan{}, false
		}
	default:
		return countSpan{}, false
	}
	if word() != "over" || !punct('(') || !punct(')') {
		return countSpan{}, false
	}
	return countSpan{start: start, end: i}, true
}

// keywordAfter returns where the word kw is in masked SQL code at the
// depth of parentheses of at, or -1 if that depth ends first.
func keywordAfter(masked string, at int, kw string) int {
	depth := 0
	for i := max(at, 0); i < len(masked); {
		c := masked[i]
		switch {
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth < 0 {
				return -1
			}
		case c == ';':
			return -1
		case sqllex.IdentByte(c):
			j := i
			for j < len(masked) && sqllex.IdentByte(masked[j]) {
				j++
			}
			if depth == 0 && strings.EqualFold(masked[i:j], kw) && (i == 0 || !sqllex.IdentByte(masked[i-1])) {
				return i
			}
			i = j
			continue
		}
		i++
	}
	return -1
}

// clauseEnds are the words that end a FROM or WHERE clause.
var clauseEnds = map[string]bool{"WHERE": true, "GROUP": true, "HAVING": true, "WINDOW": true, "QUALIFY": true,
	"ORDER": true, "LIMIT": true, "OFFSET": true, "FETCH": true, "UNION": true, "EXCEPT": true, "INTERSECT": true,
	"TABLESAMPLE": true, "RETURNING": true}

// clauseEnd returns where the clause whose body starts at from ends in
// masked SQL code: at the next clause's word, which it returns too, at a
// parenthesis that closes, at a semicolon or at the end.
func clauseEnd(masked string, from int) (int, string) {
	depth := 0
	for i := from; i < len(masked); {
		c := masked[i]
		switch {
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth < 0 {
				return i, ""
			}
		case c == ';' && depth == 0:
			return i, ""
		case sqllex.IdentByte(c) && (i == 0 || !sqllex.IdentByte(masked[i-1])):
			j := i
			for j < len(masked) && sqllex.IdentByte(masked[j]) {
				j++
			}
			w := strings.ToUpper(masked[i:j])
			if depth == 0 && (clauseEnds[w] || w == "USING" && strings.EqualFold(nextWord(masked, j), "SAMPLE")) {
				return i, w
			}
			i = j
			continue
		}
		i++
	}
	return len(masked), ""
}

// nextWord returns the word after white space at i in masked SQL code.
func nextWord(masked string, i int) string {
	i = skipSpace(masked, i)
	j := i
	for j < len(masked) && sqllex.IdentByte(masked[j]) {
		j++
	}
	return masked[i:j]
}

func skipSpace(s string, i int) int {
	for i < len(s) && isSpace(s[i]) {
		i++
	}
	return i
}

// maskCode returns text with every string literal, quoted identifier and
// comment blanked out, byte for byte, so that positions stay.
func maskCode(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for _, seg := range sqllex.Segments(text) {
		if seg.Kind == sqllex.Code {
			b.WriteString(seg.Text)
		} else {
			b.WriteString(strings.Repeat(" ", len(seg.Text)))
		}
	}
	return b.String()
}

// within reports whether every position a part of a parse gives is in
// [start, end).
func within(part map[string]any, start, end int) bool {
	ok := true
	walkMaps(part, func(m map[string]any) {
		if loc := location(m["query_location"]); loc >= 0 && (loc < start || loc >= end) {
			ok = false
		}
	})
	return ok
}

// maxLocation is the last position a part of a parse gives, or -1.
func maxLocation(part any) int {
	last := -1
	walkMaps(part, func(m map[string]any) { last = max(last, location(m["query_location"])) })
	return last
}

// walkMaps calls fn on every object in a parse.
func walkMaps(n any, fn func(map[string]any)) {
	switch n := n.(type) {
	case []any:
		for _, c := range n {
			walkMaps(c, fn)
		}
	case map[string]any:
		fn(n)
		for _, c := range n {
			walkMaps(c, fn)
		}
	}
}

// hasNode reports whether some object in a parse satisfies fn.
func hasNode(n any, fn func(map[string]any) bool) bool {
	found := false
	walkMaps(n, func(m map[string]any) { found = found || fn(m) })
	return found
}
