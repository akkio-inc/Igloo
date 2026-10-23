package pgwire

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Pair counts. Snowflake's COUNT(DISTINCT a, b) counts the distinct pairs
// in which neither value is NULL; its translation for DuckDB counts the
// distinct values of a struct,
//
//	count(DISTINCT CASE WHEN a IS NOT NULL AND b IS NOT NULL THEN (a, b) END)
//
// and generated reports put it beside count(DISTINCT a) in one GROUP BY:
//
//	SELECT k AS k, count(DISTINCT a) AS people, count(DISTINCT CASE ... (a, d) END) AS person_days FROM ... GROUP BY k
//
// DuckDB keeps a hash table of the distinct structs per group beside one of
// the distinct values of a, each holding the group's keys again, and hashing
// and comparing structs is slow: on 92 million rows such an aggregate took
// half a report's time. Igloo groups by the keys and the pair's values first,
// which makes each pair one row, and then counts per key:
//
//	SELECT _igloo_g1 AS k, count(DISTINCT _igloo_d1) AS people,
//	       count(*) FILTER (WHERE _igloo_d1 IS NOT NULL AND _igloo_d2 IS NOT NULL) AS person_days
//	FROM (SELECT k AS _igloo_g1, a AS _igloo_d1, d AS _igloo_d2 FROM ... GROUP BY k, a, d) AS _igloo_pairs
//	GROUP BY _igloo_g1
//
// That holds in a SELECT whose every item has an alias and is a GROUP BY
// key, a constant (perhaps cast), or an expression of constants, operators,
// nullif and coalesce over these aggregates: the count of distinct tuples (two or
// more values, all of one tuple), count(DISTINCT x) of one of its values,
// count(*), count(x), min(x) and max(x), with no FILTER or ORDER BY. Each
// count of rows becomes a sum of the inner select's counts, cast back to
// BIGINT; min and max take the inner select's. The SELECT has a plain GROUP
// BY of expressions (no ALL, positions, grouping sets, ROLLUP or CUBE), and
// no HAVING, QUALIFY, window function, DISTINCT, ORDER BY, LIMIT, sample,
// subquery or parameter of its own; it is not inside an expression. FROM,
// WHERE and the keys are the client's text, moved into the inner select
// unchanged, so names resolve as before.

// pairCountPasses bounds how many SELECTs one statement has rewritten, one
// a pass. A statement that repeats a part needs every copy rewritten, or
// the copies differ and are computed apart (see shared.go).
const pairCountPasses = 64

// pairCountPrefix starts the names the rewrite gives the inner select and
// its columns.
const pairCountPrefix = "_igloo_pair"

// distinctCase finds count(DISTINCT CASE WHEN in SQL code, before parsing.
var distinctCase = regexp.MustCompile(`(?i)\bcount\s*\(\s*distinct\s+case\s+when\b`)

// rewritePairCounts rewrites the pair counts of a SELECT statement.
func (s *Server) rewritePairCounts(ctx context.Context, sess *session, text string) string {
	if sess == nil || sess.conn == nil {
		return text
	}
	return pairCounts(text, func(q string) (any, error) { return parseStatement(ctx, sess.conn, q) })
}

// pairCounts rewrites the pair counts of text, a SELECT statement, which
// parse parses as json_serialize_sql does.
func pairCounts(text string, parse func(string) (any, error)) string {
	if !distinctCase.MatchString(maskCode(text)) || strings.Contains(strings.ToLower(text), pairCountPrefix) {
		return text
	}
	for range pairCountPasses {
		tree, err := parse(text)
		if err != nil {
			return text
		}
		next, ok := rewritePairCount(text, tree)
		if !ok {
			return text
		}
		text = next
	}
	return text
}

// rewritePairCount rewrites the first SELECT of text (whose parse is tree)
// that has pair counts to rewrite.
func rewritePairCount(text string, tree any) (string, bool) {
	stmt, _ := tree.(map[string]any)
	if stmt == nil {
		return "", false
	}
	masked := maskCode(text)
	pieces := pieceText{text: text, bare: maskComments(text)}
	var out string
	found := false
	walkSelects(stmt, false, func(m map[string]any) bool {
		if found {
			return false
		}
		if r, ok := pairCountSelect(pieces, masked, m); ok {
			out, found = r, true
			return false
		}
		return true
	})
	return out, found
}

// walkSelects calls fn on each SELECT node of a parse that is not inside an
// expression (a scalar, EXISTS or IN subquery), outermost first, until fn
// returns false.
func walkSelects(n any, inExpr bool, fn func(map[string]any) bool) bool {
	switch n := n.(type) {
	case []any:
		for _, c := range n {
			if !walkSelects(c, inExpr, fn) {
				return false
			}
		}
	case map[string]any:
		if str(n["class"]) == "SUBQUERY" {
			inExpr = true
		}
		if str(n["type"]) == "SELECT_NODE" && !inExpr && !fn(n) {
			return false
		}
		for _, c := range n {
			if !walkSelects(c, inExpr, fn) {
				return false
			}
		}
	}
	return true
}

// pairAggregate is one aggregate call in a select item: where it is in the
// text, and what replaces it in the outer select.
type pairAggregate struct {
	start, end int
	outer      string
}

// pairCountSelect rewrites the SELECT node m of text if it has pair counts
// to rewrite (see the comment at the top).
func pairCountSelect(pieces pieceText, masked string, m map[string]any) (string, bool) {
	text := pieces.text
	from, _ := m["from_table"].(map[string]any)
	groups := asList(m["group_expressions"])
	if from == nil || strings.HasPrefix(str(from["type"]), "EMPTY") || len(groups) == 0 || len(asList(m["group_sets"])) > 1 ||
		m["having"] != nil || m["qualify"] != nil || m["sample"] != nil || len(asList(m["modifiers"])) > 0 ||
		m["aggregate_handling"] != nil && str(m["aggregate_handling"]) != "STANDARD_HANDLING" {
		return "", false
	}
	items := asList(m["select_list"])
	if len(items) == 0 || hasNode(items, func(e map[string]any) bool {
		c := str(e["class"])
		return c == "WINDOW" || c == "SUBQUERY" || c == "PARAMETER" || c == "STAR"
	}) || hasNode(groups, func(e map[string]any) bool {
		c := str(e["class"])
		return c == "WINDOW" || c == "SUBQUERY" || c == "PARAMETER" || c == "STAR"
	}) {
		return "", false
	}
	// A constant key is a position in the select list (GROUP BY 1).
	for _, g := range groups {
		if gm, _ := g.(map[string]any); str(gm["class"]) == "CONSTANT" {
			return "", false
		}
	}
	if hasNode(m["where_clause"], func(e map[string]any) bool { return str(e["class"]) == "PARAMETER" }) ||
		hasNode(from, func(e map[string]any) bool { return str(e["class"]) == "PARAMETER" }) {
		return "", false
	}

	// The tuple counted: the values of every count of distinct tuples, which
	// must be one tuple.
	var tuple []any
	sameTuple := true
	for _, it := range items {
		walkMaps(it, func(e map[string]any) {
			if vals, ok := distinctTuple(e); ok {
				if tuple == nil {
					tuple = vals
				} else if !sameSet(tuple, vals) {
					sameTuple = false
				}
			}
		})
	}
	if tuple == nil || !sameTuple {
		return "", false
	}

	// The text of the select list's items, of FROM, WHERE and GROUP BY.
	selectAt, itemSpans, ok := selectItemSpans(masked, m, items)
	if !ok {
		return "", false
	}
	fromAt := keywordAfter(masked, itemSpans[len(itemSpans)-1][1], "FROM")
	if fromAt < 0 || maxLocation(items) >= fromAt {
		return "", false
	}
	fromBody := fromAt + len("FROM")
	fromEnd, word := clauseEnd(masked, fromBody)
	whereBody, whereEnd := -1, -1
	if word == "WHERE" {
		whereBody = fromEnd + len("WHERE")
		whereEnd, word = clauseEnd(masked, whereBody)
	}
	where, _ := m["where_clause"].(map[string]any)
	if word != "GROUP" || (where != nil) != (whereBody >= 0) || !within(from, fromBody, fromEnd) ||
		where != nil && !within(where, whereBody, whereEnd) {
		return "", false
	}
	groupAt := fromEnd
	if whereBody >= 0 {
		groupAt = whereEnd
	}
	byAt := skipSpace(masked, groupAt+len("GROUP"))
	if !strings.EqualFold(nextWord(masked, byAt), "BY") {
		return "", false
	}
	groupBody := byAt + len("BY")
	groupEnd, after := clauseEnd(masked, groupBody)
	switch after {
	case "", "UNION", "EXCEPT", "INTERSECT":
	default:
		return "", false
	}
	groupSpans := splitAtCommas(masked, groupBody, groupEnd)
	if len(groupSpans) != len(groups) {
		return "", false
	}
	for i, g := range groups {
		gm, _ := g.(map[string]any)
		// ROLLUP (k) parses as one set of the keys, its empty set left out.
		switch strings.ToUpper(nextWord(masked, groupSpans[i][0])) {
		case "ROLLUP", "CUBE", "GROUPING":
			return "", false
		}
		if !within(gm, groupSpans[i][0], groupSpans[i][1]) {
			return "", false
		}
	}
	tupleText := make([]string, len(tuple))
	for i, v := range tuple {
		vm, _ := v.(map[string]any)
		t, ok := exprText(pieces, masked, vm)
		if !ok {
			return "", false
		}
		tupleText[i] = t
	}

	// The outer select's items, and the inner select's partial aggregates.
	name := func(kind string, i int) string { return fmt.Sprintf("%s_%s%d", pairCountPrefix, kind, i) }
	var partials []string
	var outerItems []string
	for i, it := range items {
		im, _ := it.(map[string]any)
		start, end := itemSpans[i][0], itemSpans[i][1]
		if str(im["alias"]) == "" {
			return "", false
		}
		as := aliasAt(masked, start, end)
		if as < 0 {
			return "", false
		}
		key := -1
		for j, g := range groups {
			if sameExpr(im, g) {
				key = j
				break
			}
		}
		switch {
		case key >= 0:
			outerItems = append(outerItems, name("g", key+1)+" "+pieces.trim(as, end))
			continue
		case !hasNode(im, isAggregateCall):
			// A constant, perhaps cast: no column, no aggregate.
			if hasNode(im, func(e map[string]any) bool { return str(e["class"]) == "COLUMN_REF" }) || !outsideAggregates(im) {
				return "", false
			}
			outerItems = append(outerItems, pieces.trim(start, end))
			continue
		}
		// Outside its aggregates an item may hold constants and operators
		// only.
		if !outsideAggregates(im) {
			return "", false
		}
		var aggs []pairAggregate
		ok := true
		walkAggregates(im, func(a map[string]any) {
			if !ok {
				return
			}
			loc := location(a["query_location"])
			callEnd := callEndAt(masked, loc)
			if loc < start || callEnd < 0 || callEnd > as {
				ok = false
				return
			}
			outer, partial, good := pairOuter(text[loc:callEnd], a, tuple, len(partials)+1, name)
			if !good {
				ok = false
				return
			}
			if partial != "" {
				partials = append(partials, partial)
			}
			aggs = append(aggs, pairAggregate{loc, callEnd, outer})
		})
		if !ok || len(aggs) == 0 {
			return "", false
		}
		var edits []edit
		for _, a := range aggs {
			edits = append(edits, edit{a.start - start, a.end - start, a.outer})
		}
		lo, hi := pieces.bounds(start, end)
		for k := range edits {
			edits[k].start -= lo - start
			edits[k].end -= lo - start
		}
		item, err := applyEdits(text[lo:hi], edits)
		if err != nil {
			return "", false
		}
		outerItems = append(outerItems, item)
	}

	// The inner select: the keys, the tuple's values and the partial
	// aggregates, grouped by the keys and the values.
	var inner, innerGroups, outerGroups []string
	for i, sp := range groupSpans {
		g := pieces.trim(sp[0], sp[1])
		inner = append(inner, g+" AS "+name("g", i+1))
		innerGroups = append(innerGroups, g)
		outerGroups = append(outerGroups, name("g", i+1))
	}
	for i, v := range tupleText {
		inner = append(inner, v+" AS "+name("d", i+1))
		innerGroups = append(innerGroups, v)
	}
	inner = append(inner, partials...)
	// The FROM, WHERE and keys keep their lines, in case one ends with a --
	// comment.
	q := "SELECT " + strings.Join(outerItems, ", ") + " FROM (SELECT " + strings.Join(inner, ", ") +
		" FROM " + pieces.trim(fromBody, fromEnd) + "\n"
	if whereBody >= 0 {
		q += "WHERE " + pieces.trim(whereBody, whereEnd) + "\n"
	}
	// The inner select's name is the same in every SELECT rewritten (each
	// is its own scope), so that two copies of a statement's part stay
	// alike and shared subqueries (see shared.go) still find them.
	q += "GROUP BY " + strings.Join(innerGroups, ",\n") + "\n) AS " + pairCountPrefix + "s" +
		" GROUP BY " + strings.Join(outerGroups, ", ")
	if after != "" {
		q += " "
	}
	out, err := applyEdits(text, []edit{{selectAt, groupEnd, q}})
	return out, err == nil
}

// isAggregateCall reports whether an expression is a call of an aggregate
// the rewrite knows, or of another function that might be one: any
// function but operators, operatorMacros and the scalar functions in
// pairScalars.
func isAggregateCall(e map[string]any) bool {
	if str(e["class"]) != "FUNCTION" || e["is_operator"] == true {
		return false
	}
	name := strings.ToLower(str(e["function_name"]))
	return !pairScalars[name] && !operatorMacros[name]
}

// pairScalars are the scalar functions an item may apply to its
// aggregates; row builds the counted tuple.
var pairScalars = map[string]bool{"nullif": true, "coalesce": true, "row": true}

// walkAggregates calls fn on each aggregate call in an item, not looking
// inside them.
func walkAggregates(n any, fn func(map[string]any)) {
	switch n := n.(type) {
	case []any:
		for _, c := range n {
			walkAggregates(c, fn)
		}
	case map[string]any:
		if isAggregateCall(n) {
			fn(n)
			return
		}
		for _, c := range n {
			walkAggregates(c, fn)
		}
	}
}

// outsideAggregates reports whether an item holds only constants, casts,
// operators, CASE, nullif and coalesce outside its aggregate calls: no
// column, whose name the outer select would not know.
func outsideAggregates(n any) bool {
	ok := true
	var walk func(n any)
	walk = func(n any) {
		switch n := n.(type) {
		case []any:
			for _, c := range n {
				walk(c)
			}
		case map[string]any:
			if isAggregateCall(n) {
				return
			}
			switch str(n["class"]) {
			case "", "CONSTANT", "CAST", "OPERATOR", "COMPARISON", "CONJUNCTION", "CASE", "BETWEEN":
			case "FUNCTION":
				if name := strings.ToLower(str(n["function_name"])); n["is_operator"] != true && !pairScalars[name] && !operatorMacros[name] {
					ok = false
				}
			default:
				ok = false
			}
			for _, c := range n {
				walk(c)
			}
		}
	}
	walk(n)
	return ok
}

// distinctTuple returns the values of a count of distinct tuples in the
// form the translation of COUNT(DISTINCT a, b, ...) has:
// count(DISTINCT CASE WHEN a IS NOT NULL AND b IS NOT NULL ... THEN (a, b, ...) END),
// with two or more values, all different columns.
func distinctTuple(e map[string]any) ([]any, bool) {
	if str(e["class"]) != "FUNCTION" || strings.ToLower(str(e["function_name"])) != "count" || e["distinct"] != true ||
		e["filter"] != nil || len(asList(orderBys(e))) > 0 {
		return nil, false
	}
	args := asList(e["children"])
	if len(args) != 1 {
		return nil, false
	}
	c, _ := args[0].(map[string]any)
	checks := asList(c["case_checks"])
	if str(c["class"]) != "CASE" || len(checks) != 1 {
		return nil, false
	}
	if el, ok := c["else_expr"].(map[string]any); ok && !(str(el["class"]) == "CONSTANT" && isNullConstant(el)) {
		return nil, false
	}
	check, _ := checks[0].(map[string]any)
	when, _ := check["when_expr"].(map[string]any)
	then, _ := check["then_expr"].(map[string]any)
	if then == nil || str(then["class"]) != "FUNCTION" || strings.ToLower(str(then["function_name"])) != "row" {
		return nil, false
	}
	vals := asList(then["children"])
	if len(vals) < 2 {
		return nil, false
	}
	for i, v := range vals {
		vm, _ := v.(map[string]any)
		if vm == nil || str(vm["class"]) != "COLUMN_REF" {
			return nil, false
		}
		for _, w := range vals[:i] {
			if sameExpr(v, w) {
				return nil, false
			}
		}
	}
	// The WHEN is the values' IS NOT NULL, joined by AND, in any order.
	var tested []any
	var collect func(x map[string]any) bool
	collect = func(x map[string]any) bool {
		switch {
		case str(x["class"]) == "CONJUNCTION" && str(x["type"]) == "CONJUNCTION_AND":
			for _, k := range asList(x["children"]) {
				km, _ := k.(map[string]any)
				if km == nil || !collect(km) {
					return false
				}
			}
			return true
		case str(x["class"]) == "OPERATOR" && str(x["type"]) == "OPERATOR_IS_NOT_NULL" && len(asList(x["children"])) == 1:
			tested = append(tested, asList(x["children"])[0])
			return true
		}
		return false
	}
	if when == nil || !collect(when) || !sameSet(tested, vals) {
		return nil, false
	}
	return vals, true
}

// orderBys returns an aggregate's own ORDER BY items.
func orderBys(e map[string]any) any {
	ob, _ := e["order_bys"].(map[string]any)
	return ob["orders"]
}

// isNullConstant reports whether a constant is NULL.
func isNullConstant(c map[string]any) bool {
	v, _ := c["value"].(map[string]any)
	return v != nil && v["is_null"] == true
}

// pairOuter returns what replaces the aggregate call a (whose text is call)
// in the outer select, and the partial aggregate the inner select adds for
// it, if any; i numbers the partial.
func pairOuter(call string, a map[string]any, tuple []any, i int, name func(string, int) string) (outer, partial string, ok bool) {
	if a["filter"] != nil || len(asList(orderBys(a))) > 0 {
		return "", "", false
	}
	fn := strings.ToLower(str(a["function_name"]))
	args := asList(a["children"])
	if vals, isTuple := distinctTuple(a); isTuple {
		var conds []string
		for _, v := range vals {
			for k, w := range tuple {
				if sameExpr(v, w) {
					conds = append(conds, name("d", k+1)+" IS NOT NULL")
				}
			}
		}
		return "count(*) FILTER (WHERE " + strings.Join(conds, " AND ") + ")", "", true
	}
	if a["distinct"] == true {
		if fn != "count" || len(args) != 1 {
			return "", "", false
		}
		for k, w := range tuple {
			if sameExpr(args[0], w) {
				return "count(DISTINCT " + name("d", k+1) + ")", "", true
			}
		}
		return "", "", false
	}
	for _, arg := range args {
		am, _ := arg.(map[string]any)
		if am == nil || !plainItem(am) || hasNode(am, func(m map[string]any) bool { return str(m["class"]) == "STAR" }) {
			return "", "", false
		}
	}
	p := name("p", i)
	switch {
	case fn == "count_star" && len(args) == 0, fn == "count" && len(args) == 1:
		return "CAST(sum(" + p + ") AS BIGINT)", call + " AS " + p, true
	case (fn == "min" || fn == "max") && len(args) == 1:
		return fn + "(" + p + ")", call + " AS " + p, true
	}
	return "", "", false
}

// sameExpr reports whether two parts of a parse are the same expression:
// equal but for where they are in the text and their aliases.
func sameExpr(a, b any) bool {
	switch a := a.(type) {
	case map[string]any:
		bm, ok := b.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range a {
			if k == "query_location" || k == "alias" {
				continue
			}
			if !sameExpr(v, bm[k]) {
				return false
			}
		}
		for k := range bm {
			if _, ok := a[k]; !ok && k != "query_location" && k != "alias" {
				return false
			}
		}
		return true
	case []any:
		bl, ok := b.([]any)
		if !ok || len(a) != len(bl) {
			return false
		}
		for i := range a {
			if !sameExpr(a[i], bl[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}

// sameSet reports whether two lists of expressions hold the same ones.
func sameSet(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		found := false
		for _, y := range b {
			found = found || sameExpr(x, y)
		}
		if !found {
			return false
		}
	}
	return true
}

// selectItemSpans finds where the SELECT of node m is and its items' text:
// each item from its start to the comma or the end of the select list that
// follows it, the alias included.
func selectItemSpans(masked string, m map[string]any, items []any) (int, [][2]int, bool) {
	first, _ := items[0].(map[string]any)
	firstAt := minLocation(first)
	if firstAt < 0 {
		return 0, nil, false
	}
	// SELECT is the word right before the first item, perhaps after an
	// opening parenthesis of the item.
	i := firstAt
	for i > 0 && (isSpace(masked[i-1]) || masked[i-1] == '(') {
		i--
	}
	selectAt := i - len("SELECT")
	if selectAt < 0 || !strings.EqualFold(masked[selectAt:i], "SELECT") || selectAt > 0 && isIdentChar(masked[selectAt-1]) {
		return 0, nil, false
	}
	listStart := i
	// FROM is the first one at the select list's depth.
	fromAt := keywordAfter(masked, listStart, "FROM")
	if fromAt < 0 {
		return 0, nil, false
	}
	spans := splitAtCommas(masked, listStart, fromAt)
	if len(spans) != len(items) {
		return 0, nil, false
	}
	for k, it := range items {
		im, _ := it.(map[string]any)
		if !within(im, spans[k][0], spans[k][1]) {
			return 0, nil, false
		}
	}
	return selectAt, spans, true
}

// splitAtCommas splits masked SQL code from start to end at the commas
// outside parentheses.
func splitAtCommas(masked string, start, end int) [][2]int {
	var out [][2]int
	depth, from := 0, start
	for i := start; i < end; i++ {
		switch masked[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, [2]int{from, i})
				from = i + 1
			}
		}
	}
	return append(out, [2]int{from, end})
}

// aliasAt returns where the AS of an item's alias is, outside parentheses,
// in masked SQL code from start to end, or -1 if the item has no AS.
func aliasAt(masked string, start, end int) int {
	at := -1
	depth := 0
	for i := start; i < end; i++ {
		switch c := masked[i]; {
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && (c == 'a' || c == 'A') && i+2 <= end && strings.EqualFold(masked[i:i+2], "AS") &&
			(i == start || !isIdentChar(masked[i-1])) && (i+2 == end || !isIdentChar(masked[i+2])):
			at = i
		}
	}
	return at
}

// callEndAt returns where a function call that starts at at in masked SQL
// code ends, just past its closing parenthesis, or -1.
func callEndAt(masked string, at int) int {
	if at < 0 || at >= len(masked) {
		return -1
	}
	i := at
	for i < len(masked) && isIdentChar(masked[i]) {
		i++
	}
	i = skipSpace(masked, i)
	if i >= len(masked) || masked[i] != '(' {
		return -1
	}
	end := matchParen(masked, i)
	if end < 0 {
		return -1
	}
	return end + 1
}

// exprText returns the text of one of the tuple's values, a column, from
// where it is inside a count of distinct tuples to the comma or parenthesis
// that ends it.
func exprText(pieces pieceText, masked string, e map[string]any) (string, bool) {
	start := minLocation(e)
	if start < 0 {
		return "", false
	}
	// A value inside (a, b): it ends at the comma or the closing
	// parenthesis at its depth.
	depth := 0
	for i := start; i < len(masked); i++ {
		switch masked[i] {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return pieces.trim(start, i), within(e, start, i)
			}
			depth--
		case ',':
			if depth == 0 {
				return pieces.trim(start, i), within(e, start, i)
			}
		}
	}
	return "", false
}

// isIdentChar reports whether c can be part of an unquoted SQL name.
func isIdentChar(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// pieceText is a statement's text, and the same text with its comments
// blanked out, byte for byte: pieces of the statement move without the
// comments around them, which could end a line that no longer ends there.
type pieceText struct {
	text, bare string
}

// bounds returns start and end moved past the white space and comments at
// either end of text[start:end].
func (p pieceText) bounds(start, end int) (int, int) {
	for start < end && isSpace(p.bare[start]) {
		start++
	}
	for end > start && isSpace(p.bare[end-1]) {
		end--
	}
	return start, end
}

// trim returns text[start:end] without the white space and comments at
// either end.
func (p pieceText) trim(start, end int) string {
	lo, hi := p.bounds(start, end)
	return p.text[lo:hi]
}

// maskComments returns text with every comment blanked out, byte for byte.
func maskComments(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for _, seg := range sqllex.Segments(text) {
		if seg.Kind == sqllex.Comment {
			b.WriteString(strings.Repeat(" ", len(seg.Text)))
		} else {
			b.WriteString(seg.Text)
		}
	}
	return b.String()
}
