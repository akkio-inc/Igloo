package pgwire

import (
	"strings"

	"github.com/akkio-inc/igloo/internal/iddomain"
)

// Matching the counts bitmap indexes answer. A bitmap index of a table's
// member column c, ON (g...), answers
//
//	SELECT k..., count(DISTINCT t.c) FROM tbl AS t [WHERE p] [GROUP BY k...]
//
// where the keys k and the filter p refer to no column of t but the index's
// groups (any expression of them: a predicate on a group column holds or
// not for all of a group's rows alike), from its bitmaps: the bitmaps of
// the groups p keeps, OR-ed together by k. The count may be Igloo's count
// through ids, count(DISTINCT CAST(t._igloo_id_c AS UINTEGER)), the same
// count. The same count of the members also in a set another index holds,
//
//	... FROM tbl AS t JOIN a ON a.x = t.c ...     (or WHERE t.c IN (SELECT x FROM a ...))
//
// with a a CTE, a subquery or a table that is SELECT [DISTINCT] x FROM tbl2
// [WHERE q] over a member column x of tbl2 that an index of tbl2 holds, by
// groups q refers to only, is the cardinality of the AND of the two ORs. A
// table it answers must be a base table of the catalog, read as of the
// snapshot its bitmaps are of (see the rewrite).

// indexRel is a table a matched count reads: as written, the alias it reads
// it under, the member column counted or restricted by, and the columns
// beside it the count's keys and filter name (lower case, sorted, once
// each).
type indexRel struct {
	// elements, when set, asks for an index of that list column's
	// elements (ON (ELEMENTS(column))), whose groups no other relation
	// matches; it is also among groups.
	elements string
	parts    []string
	alias    string
	column   string
	groups   []string
	// at says the reference reads the table at a version, as Igloo pins
	// tables it counts through ids (see idcount.go).
	at bool
	// where is the relation's own filter, for a restriction, node its
	// SELECT, and innerAlias the alias its table has in it.
	where      map[string]any
	node       map[string]any
	innerAlias string
}

// indexCount is a count(DISTINCT) of a SELECT that names a member column,
// or might: whether bitmap indexes could answer it, and if not, why.
type indexCount struct {
	node   map[string]any
	call   map[string]any
	target indexRel
	// restrict is the set the count's members must also be in, if any;
	// restrictIn says the WHERE names it (t.c IN (SELECT ...)) rather
	// than a join.
	restrict   *indexRel
	restrictIn bool
	// miss says why indexes cannot answer the count; empty for a match.
	miss string
}

// matchIndexCounts classifies every count(DISTINCT) in the SELECTs of a
// statement's parse that is not inside an expression.
func matchIndexCounts(tree any) []indexCount {
	var out []indexCount
	matchSelects(tree, nil, false, func(m map[string]any, sc *cteScope) {
		out = append(out, matchSelectCounts(m, sc)...)
	})
	return out
}

// matchSelects calls fn on each SELECT node of a parse not inside an
// expression, with the CTEs it sees.
func matchSelects(n any, sc *cteScope, inExpr bool, fn func(map[string]any, *cteScope)) {
	switch n := n.(type) {
	case []any:
		for _, c := range n {
			matchSelects(c, sc, inExpr, fn)
		}
	case map[string]any:
		if cm, ok := n["cte_map"].(map[string]any); ok {
			for _, e := range asList(cm["map"]) {
				em, _ := e.(map[string]any)
				key, _ := em["key"].(string)
				value, _ := em["value"].(map[string]any)
				def := &cteDef{body: value["query"], scope: sc}
				sc = &cteScope{defs: map[string]*cteDef{strings.ToLower(key): def}, parent: sc}
				matchSelects(value["query"], def.scope, inExpr, fn)
			}
		}
		if str(n["class"]) == "SUBQUERY" {
			inExpr = true
		}
		if str(n["type"]) == "SELECT_NODE" && !inExpr {
			fn(n, sc)
		}
		for k, v := range n {
			if k != "cte_map" {
				matchSelects(v, sc, inExpr, fn)
			}
		}
	}
}

// countArg returns the column a count(DISTINCT) counts, its qualifier and
// whether it is Igloo's count through ids; ok is false for a count of
// anything else.
func countArg(call map[string]any) (qualifier, column string, ids bool, ok bool) {
	if str(call["class"]) != "FUNCTION" || !strings.EqualFold(str(call["function_name"]), "count") || call["distinct"] != true {
		return "", "", false, false
	}
	args := asList(call["children"])
	if len(args) != 1 {
		return "", "", false, false
	}
	arg, _ := args[0].(map[string]any)
	if str(arg["class"]) == "CAST" {
		ct, _ := arg["cast_type"].(map[string]any)
		if !strings.EqualFold(str(ct["id"]), "UINTEGER") {
			return "", "", false, false
		}
		arg, _ = arg["child"].(map[string]any)
		ids = true
	}
	if str(arg["class"]) != "COLUMN_REF" {
		return "", "", false, false
	}
	names := asList(arg["column_names"])
	column = str(names[len(names)-1])
	if len(names) > 1 {
		qualifier = str(names[len(names)-2])
	}
	if ids {
		if !iddomain.IsHidden(column) {
			return "", "", false, false
		}
		column = column[len(iddomain.HiddenColumn("")):]
	}
	return qualifier, column, ids, true
}

// isCountDistinct reports whether e is a count(DISTINCT ...) call.
func isCountDistinct(e map[string]any) bool {
	return str(e["class"]) == "FUNCTION" && strings.EqualFold(str(e["function_name"]), "count") && e["distinct"] == true
}

// matchSelectCounts classifies the counts of distinct values of SELECT node
// m.
func matchSelectCounts(m map[string]any, sc *cteScope) []indexCount {
	var calls []map[string]any
	for _, it := range asList(m["select_list"]) {
		walkMaps(it, func(e map[string]any) {
			if isCountDistinct(e) {
				calls = append(calls, e)
			}
		})
	}
	if len(calls) == 0 {
		return nil
	}
	miss := func(why string) []indexCount {
		out := make([]indexCount, len(calls))
		for i, c := range calls {
			out[i] = indexCount{node: m, call: c, miss: why}
		}
		return out
	}
	// One count, of one member column, and no other aggregate.
	qual, col, _, ok := countArg(calls[0])
	if !ok {
		return miss("counts an expression, not a column")
	}
	for _, c := range calls[1:] {
		q, c2, _, ok := countArg(c)
		if !ok || !strings.EqualFold(q, qual) || !strings.EqualFold(c2, col) {
			return miss("counts more than one column")
		}
	}
	// FROM: the counted table, alone or joined to the set its members must
	// be in.
	from, _ := m["from_table"].(map[string]any)
	var target indexRel
	var restrict *indexRel
	restrictIn := false
	var joinCond map[string]any
	switch str(from["type"]) {
	case "BASE_TABLE":
		t, why := baseRel(from, sc)
		if why != "" {
			return miss(why)
		}
		target = t
	case "JOIN":
		if !strings.EqualFold(str(from["join_type"]), "INNER") || !strings.EqualFold(str(from["ref_type"]), "REGULAR") || len(asList(from["using_columns"])) > 0 {
			return miss("joins other than with an inner join on the member column")
		}
		left, _ := from["left"].(map[string]any)
		right, _ := from["right"].(map[string]any)
		joinCond, _ = from["condition"].(map[string]any)
		// The counted table is the side the count's qualifier names.
		for _, pair := range [][2]map[string]any{{left, right}, {right, left}} {
			t, why := baseRel(pair[0], sc)
			if why != "" || qual != "" && !strings.EqualFold(qual, t.alias) {
				continue
			}
			r, why := setRel(pair[1], sc)
			if why != "" {
				return miss(why)
			}
			target, restrict = t, &r
			break
		}
		if restrict == nil {
			return miss("counts a relation that is not a table of the catalog, or joins it to more than one set")
		}
	default:
		return miss("counts rows of a derived relation (a CTE, subquery, unnest or function), not a table")
	}
	target.column = col
	if qual != "" && !strings.EqualFold(qual, target.alias) {
		return miss("counts a column of another relation than its table")
	}
	other := false
	for _, it := range asList(m["select_list"]) {
		walkMaps(it, func(e map[string]any) {
			if isAggregateCall(e) && !isCountDistinct(e) {
				other = true
			}
		})
	}
	if other {
		return miss("has other aggregates beside the count")
	}
	if m["having"] != nil || m["qualify"] != nil || m["sample"] != nil || len(asList(m["group_sets"])) > 1 {
		return miss("has HAVING, QUALIFY, a sample or grouping sets")
	}
	if hasNode(m["select_list"], func(e map[string]any) bool {
		c := str(e["class"])
		return c == "WINDOW" || c == "SUBQUERY" || c == "STAR"
	}) {
		return miss("has window functions, subqueries or * in its select list")
	}

	// The join holds the counted column equal to the set's: a table's,
	// the column the join names.
	if restrict != nil {
		if restrict.column == "" {
			restrict.column = joinColumn(joinCond, *restrict)
		}
		if !joinsOn(joinCond, target, *restrict) {
			return miss("joins the set on other than the member column")
		}
	}
	// WHERE: the filter on the table's groups, and perhaps the set as an
	// IN subquery.
	var conjs []any
	if w, _ := m["where_clause"].(map[string]any); w != nil {
		conjs = conjunctList(w)
	}
	for _, c := range conjs {
		cm, _ := c.(map[string]any)
		if r, ok := inSubquerySet(cm, target, sc); ok {
			if restrict != nil {
				return miss("restricts the count by more than one set")
			}
			restrict, restrictIn = r, true
			continue
		}
		cols, ok := relColumns(cm, target)
		if !ok {
			return miss("filters on columns of other relations, or on the counted column")
		}
		target.groups = addNames(target.groups, cols...)
	}
	// Keys and select items: the table's group columns and constants.
	for _, g := range asList(m["group_expressions"]) {
		gm, _ := g.(map[string]any)
		if str(gm["class"]) == "CONSTANT" {
			return miss("groups by a position in the select list")
		}
		cols, ok := relColumns(gm, target)
		if !ok {
			return miss("groups by columns of other relations, or by the counted column")
		}
		target.groups = addNames(target.groups, cols...)
	}
	for _, it := range asList(m["select_list"]) {
		im, _ := it.(map[string]any)
		if hasNode(im, isCountDistinct) {
			continue
		}
		cols, ok := relColumns(im, target)
		if !ok {
			return miss("selects columns of other relations, or the counted column")
		}
		target.groups = addNames(target.groups, cols...)
	}
	// ORDER BY and LIMIT: by the select list's names, positions or group
	// columns, never the counted column or an aggregate.
	for _, mod := range asList(m["modifiers"]) {
		mm, _ := mod.(map[string]any)
		switch str(mm["type"]) {
		case "ORDER_MODIFIER":
			for _, o := range asList(mm["orders"]) {
				om, _ := o.(map[string]any)
				e, _ := om["expression"].(map[string]any)
				if hasNode(e, isAggregateCall) {
					return miss("orders by an aggregate")
				}
				if str(e["class"]) == "COLUMN_REF" && len(asList(e["column_names"])) == 1 {
					// a name of the select list, or a group column
					continue
				}
				if _, ok := relColumns(e, target); !ok {
					return miss("orders by columns of other relations, or by the counted column")
				}
			}
		case "LIMIT_MODIFIER", "LIMIT_PERCENT_MODIFIER", "DISTINCT_MODIFIER":
		default:
			return miss("has modifiers the rewrite does not take")
		}
	}
	out := make([]indexCount, len(calls))
	for i, c := range calls {
		out[i] = indexCount{node: m, call: c, target: target, restrict: restrict, restrictIn: restrictIn}
	}
	return out
}

// baseRel reads a FROM reference that must be a table of the catalog: not a
// CTE, a subquery or a function.
func baseRel(ref map[string]any, sc *cteScope) (indexRel, string) {
	if str(ref["type"]) != "BASE_TABLE" {
		return indexRel{}, "counts rows of a derived relation (a CTE, subquery, unnest or function), not a table"
	}
	if ref["sample"] != nil {
		return indexRel{}, "reads a sample of the table"
	}
	var parts []string
	for _, k := range []string{"catalog_name", "schema_name", "table_name"} {
		if p := str(ref[k]); p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) == 1 && sc.find(parts[0]) != nil {
		return indexRel{}, "counts rows of a derived relation (a CTE, subquery, unnest or function), not a table"
	}
	alias := str(ref["alias"])
	if alias == "" {
		alias = parts[len(parts)-1]
	}
	return indexRel{parts: parts, alias: alias, at: ref["at_clause"] != nil}, ""
}

// setRel reads the relation a count's members must also be in: a table, or
// a CTE or subquery that selects one column of a table, perhaps filtered.
func setRel(ref map[string]any, sc *cteScope) (indexRel, string) {
	var node map[string]any
	alias := str(ref["alias"])
	switch str(ref["type"]) {
	case "BASE_TABLE":
		parts := []string{}
		for _, k := range []string{"catalog_name", "schema_name", "table_name"} {
			if p := str(ref[k]); p != "" {
				parts = append(parts, p)
			}
		}
		if len(parts) == 1 {
			if d := sc.find(parts[0]); d != nil {
				q, _ := d.body.(map[string]any)
				node, _ = q["node"].(map[string]any)
				if alias == "" {
					alias = parts[0]
				}
				break
			}
		}
		r, why := baseRel(ref, sc)
		r.innerAlias = r.alias
		return r, why
	case "SUBQUERY":
		q, _ := ref["subquery"].(map[string]any)
		node, _ = q["node"].(map[string]any)
	default:
		return indexRel{}, "joins a derived relation that is not one column of a table"
	}
	r, why := setNode(node, sc)
	if why != "" {
		return indexRel{}, why
	}
	r.innerAlias, r.alias = r.alias, alias
	return r, ""
}

// setNode reads SELECT [DISTINCT] x FROM tbl [WHERE q], a set of a table's
// member values filtered by its other columns.
func setNode(node map[string]any, sc *cteScope) (indexRel, string) {
	if str(node["type"]) != "SELECT_NODE" {
		return indexRel{}, "restricts by a set that is not one SELECT of a table"
	}
	if len(asList(node["group_expressions"])) > 0 || node["having"] != nil || node["qualify"] != nil || node["sample"] != nil {
		return indexRel{}, "restricts by a set that groups or samples"
	}
	for _, mod := range asList(node["modifiers"]) {
		if mm, _ := mod.(map[string]any); str(mm["type"]) != "DISTINCT_MODIFIER" {
			return indexRel{}, "restricts by a set with ORDER BY or LIMIT"
		}
	}
	items := asList(node["select_list"])
	if len(items) != 1 {
		return indexRel{}, "restricts by a set of more than one column"
	}
	from, _ := node["from_table"].(map[string]any)
	inner := sc
	if cm, ok := node["cte_map"].(map[string]any); ok && len(asList(cm["map"])) > 0 {
		return indexRel{}, "restricts by a set with CTEs of its own"
	}
	t, why := baseRel(from, inner)
	if why != "" {
		return indexRel{}, "restricts by a set that is not one table's"
	}
	item, _ := items[0].(map[string]any)
	if str(item["class"]) == "CAST" {
		ct, _ := item["cast_type"].(map[string]any)
		if !strings.EqualFold(str(ct["id"]), "UINTEGER") {
			return indexRel{}, "restricts by a set of an expression"
		}
		item, _ = item["child"].(map[string]any)
	}
	if str(item["class"]) != "COLUMN_REF" {
		return indexRel{}, "restricts by a set of an expression"
	}
	names := asList(item["column_names"])
	t.column = str(names[len(names)-1])
	if iddomain.IsHidden(t.column) {
		t.column = t.column[len(iddomain.HiddenColumn("")):]
	}
	t.node, t.innerAlias = node, t.alias
	if w, _ := node["where_clause"].(map[string]any); w != nil {
		cols, ok := relColumns(w, t)
		if !ok {
			return indexRel{}, "restricts by a set filtered on other relations' columns"
		}
		t.groups = addNames(t.groups, cols...)
		t.where = w
	}
	return t, ""
}

// joinsOn reports whether cond is t.c = r.x, either way round.
func joinsOn(cond map[string]any, t, r indexRel) bool {
	if str(cond["type"]) != "COMPARE_EQUAL" {
		return false
	}
	l, _ := cond["left"].(map[string]any)
	rr, _ := cond["right"].(map[string]any)
	is := func(e map[string]any, rel indexRel, col string) bool {
		if str(e["class"]) == "CAST" {
			e, _ = e["child"].(map[string]any)
		}
		if str(e["class"]) != "COLUMN_REF" {
			return false
		}
		names := asList(e["column_names"])
		name := str(names[len(names)-1])
		if iddomain.IsHidden(name) {
			name = name[len(iddomain.HiddenColumn("")):]
		}
		return strings.EqualFold(name, col) && (len(names) == 1 || strings.EqualFold(str(names[len(names)-2]), rel.alias))
	}
	return is(l, t, t.column) && is(rr, r, r.column) || is(rr, t, t.column) && is(l, r, r.column)
}

// joinColumn returns the column of rel that an equality names, or "".
func joinColumn(cond map[string]any, rel indexRel) string {
	if str(cond["type"]) != "COMPARE_EQUAL" {
		return ""
	}
	for _, side := range []string{"left", "right"} {
		e, _ := cond[side].(map[string]any)
		if str(e["class"]) == "CAST" {
			e, _ = e["child"].(map[string]any)
		}
		names := asList(e["column_names"])
		if str(e["class"]) == "COLUMN_REF" && len(names) > 1 && strings.EqualFold(str(names[len(names)-2]), rel.alias) {
			name := str(names[len(names)-1])
			if iddomain.IsHidden(name) {
				name = name[len(iddomain.HiddenColumn("")):]
			}
			return name
		}
	}
	return ""
}

// inSubquerySet reads t.c IN (SELECT x FROM tbl2 [WHERE q]).
func inSubquerySet(c map[string]any, t indexRel, sc *cteScope) (*indexRel, bool) {
	if str(c["class"]) != "SUBQUERY" || str(c["subquery_type"]) != "ANY" || str(c["comparison_type"]) != "COMPARE_EQUAL" {
		return nil, false
	}
	child, _ := c["child"].(map[string]any)
	if str(child["class"]) == "CAST" {
		child, _ = child["child"].(map[string]any)
	}
	if str(child["class"]) != "COLUMN_REF" {
		return nil, false
	}
	names := asList(child["column_names"])
	name := str(names[len(names)-1])
	if iddomain.IsHidden(name) {
		name = name[len(iddomain.HiddenColumn("")):]
	}
	if !strings.EqualFold(name, t.column) || len(names) > 1 && !strings.EqualFold(str(names[len(names)-2]), t.alias) {
		return nil, false
	}
	q, _ := c["subquery"].(map[string]any)
	node, _ := q["node"].(map[string]any)
	r, why := setNode(node, sc)
	if why != "" {
		return nil, false
	}
	return &r, true
}

// relColumns returns the columns of rel expression e names, lower case;
// ok is false when it names a column of another relation, the member
// column, a subquery correlated with the query or a parameter.
func relColumns(e any, rel indexRel) ([]string, bool) {
	var cols []string
	ok := true
	var visit func(n any, inSub bool)
	visit = func(n any, inSub bool) {
		switch n := n.(type) {
		case []any:
			for _, c := range n {
				visit(c, inSub)
			}
		case map[string]any:
			switch str(n["class"]) {
			case "PARAMETER":
				ok = false
				return
			case "SUBQUERY":
				// An uncorrelated subquery is a constant to the filter
				// (ver = (SELECT max(ver) FROM metadata WHERE ...)); one
				// that names the relation is not allowed.
				q, _ := n["subquery"].(map[string]any)
				if hasNode(q, func(m map[string]any) bool {
					if str(m["class"]) != "COLUMN_REF" {
						return false
					}
					names := asList(m["column_names"])
					return len(names) > 1 && strings.EqualFold(str(names[len(names)-2]), rel.alias)
				}) {
					ok = false
					return
				}
				visit(n["child"], inSub)
				return
			case "COLUMN_REF":
				if inSub {
					return
				}
				names := asList(n["column_names"])
				name := strings.ToLower(str(names[len(names)-1]))
				if len(names) > 1 && !strings.EqualFold(str(names[len(names)-2]), rel.alias) {
					ok = false
					return
				}
				if strings.EqualFold(name, rel.column) || iddomain.IsHidden(name) {
					ok = false
					return
				}
				cols = append(cols, name)
				return
			}
			for k, v := range n {
				if k == "query_location" {
					continue
				}
				visit(v, inSub)
			}
		}
	}
	visit(e, false)
	return cols, ok
}

// conjunctList splits an AND into its conjuncts.
func conjunctList(e map[string]any) []any {
	if str(e["class"]) == "CONJUNCTION" && str(e["type"]) == "CONJUNCTION_AND" {
		var out []any
		for _, c := range asList(e["children"]) {
			cm, _ := c.(map[string]any)
			out = append(out, conjunctList(cm)...)
		}
		return out
	}
	return []any{e}
}

func addNames(to []string, names ...string) []string {
	for _, n := range names {
		n = strings.ToLower(n)
		found := false
		for _, t := range to {
			if t == n {
				found = true
				break
			}
		}
		if !found {
			to = append(to, n)
		}
	}
	return to
}
