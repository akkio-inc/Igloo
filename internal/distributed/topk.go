package distributed

import (
	"strconv"
	"strings"
)

// Ranked groups kept to the first k. A report that ranks the groups an
// aggregation computes, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o), and
// keeps those that rank within the first k sent every group to the
// coordinator, which ranked them: at a hundred times the data, its
// workers had sent 1.07 GB of groups when the spool filled up. The stage
// that completes an aggregation's groups, partition by partition, now
// keeps only the rows that may rank within the first k: a row's rank among
// its task's rows is at most its rank among all of them, as are its
// RANK() and DENSE_RANK(), so a row within the first k among all is within
// them among its task's. It filters by RANK() for ROW_NUMBER(), which
// keeps every row tied with the kth, so that the coordinator chooses among
// ties from all of them, as one machine would. The coordinator ranks what
// arrives as the statement says.
//
// markTopK finds such statements before planning: a SELECT reading a
// relation read once, which ranks the rows of another relation read once
// (a common table expression or a subquery), with a WHERE conjunct that
// keeps rows ranked within the first k, or meeting conditions that do not
// depend on the ranks. It marks each aggregation whose rows make up that
// other relation, directly or through a UNION ALL and projections, with
// the condition its rows must meet (topKMark), in terms of its own select
// list; the stage that completes its groups applies the condition as
// QUALIFY (see qualifyTopK).

// topKMark marks an aggregation's SELECT with the condition its rows must
// meet to reach a statement that ranks them (see markTopK). It never
// reaches SQL: print removes it.
const topKMark = "__igloo_topk"

// maxTopKDepth bounds the projections markTopK follows down to an
// aggregation.
const maxTopKDepth = 8

// markTopK marks the aggregations whose rows a statement ranks and keeps the
// first few of (see topKMark), in query node q.
func (pl *planner) markTopK(q node) {
	defs := map[string]node{}
	dup := map[string]bool{}
	walk(q, func(n node) bool {
		for _, e := range list(child(n, "cte_map"), "map") {
			en, _ := e.(node)
			name := strings.ToLower(str(en["key"]))
			if _, seen := defs[name]; seen {
				dup[name] = true
			}
			defs[name] = child(child(child(en, "value"), "query"), "node")
		}
		return true
	})
	refs := map[string]int{}
	walk(q, func(n node) bool {
		if str(n["type"]) == "BASE_TABLE" && str(n["schema_name"]) == "" && str(n["catalog_name"]) == "" {
			refs[strings.ToLower(str(n["table_name"]))]++
		}
		return true
	})
	// readOnce returns the query a FROM item reads: a subquery's, or that of
	// a common table expression nothing else reads.
	readOnce := func(ref node) node {
		switch str(ref["type"]) {
		case "SUBQUERY":
			return subqueryNode(ref)
		case "BASE_TABLE":
			name := strings.ToLower(str(ref["table_name"]))
			if str(ref["schema_name"]) == "" && str(ref["catalog_name"]) == "" && refs[name] == 1 && !dup[name] {
				return defs[name]
			}
		}
		return nil
	}
	walk(q, func(s node) bool {
		if str(s["type"]) == "SELECT_NODE" {
			pl.markConsumer(s, readOnce)
		}
		return true
	})
}

// markConsumer marks the aggregations under s, if s keeps the rows a
// relation it reads ranks within the first k.
func (pl *planner) markConsumer(s node, readOnce func(node) node) {
	r := readOnce(child(s, "from_table"))
	if !projectionOnly(r) || pl.hasAggregate(r["select_list"]) {
		return
	}
	items := list(r, "select_list")
	names := outputColumns(r, &scope{})
	if len(names) != len(items) {
		return
	}
	for _, c := range conjuncts(child(s, "where_clause")) {
		if keep := pl.rankedKeep(c, names, items); keep != nil {
			pl.markRows(readOnce(child(r, "from_table")), keep, readOnce, 0)
			return
		}
	}
}

// projectionOnly reports whether q is a SELECT that computes its select
// list over the rows of one FROM item, keeping them all.
func projectionOnly(q node) bool {
	if str(q["type"]) != "SELECT_NODE" || child(q, "where_clause") != nil || len(list(q, "group_expressions")) > 0 ||
		child(q, "having") != nil || child(q, "qualify") != nil || len(list(q, "modifiers")) > 0 || child(q, "sample") != nil ||
		str(q["aggregate_handling"]) != "STANDARD_HANDLING" || len(list(child(q, "cte_map"), "map")) > 0 {
		return false
	}
	from := child(q, "from_table")
	return str(from["type"]) == "BASE_TABLE" || str(from["type"]) == "SUBQUERY"
}

// rankedKeep returns, for a conjunct c over a relation of columns names
// computed by items, the condition a row of what that relation reads must
// meet to pass c, in terms of the relation's items: c's terms that compare
// a rank to a constant k become the row's rank among its task's rows within
// k; the others stay, and may not read a rank. Nil if c keeps no rows by
// rank, or reads what cannot be said so.
func (pl *planner) rankedKeep(c node, names []string, items []any) node {
	// One window in all: another's ranks, or this one's of rows kept for
	// another reason, would be computed over the rows kept, not all of
	// them. A row kept within the first k has every row ahead of it in its
	// partition kept too, so its rank comes out as over all rows.
	windows := 0
	walk(items, func(n node) bool {
		if str(n["class"]) == "WINDOW" {
			windows++
		}
		return true
	})
	if windows != 1 {
		return nil
	}
	var out, others []node
	var window node
	ranked := false
	for _, t := range disjuncts(c) {
		if i, k, ok := rankBound(t, names); ok {
			if window != nil {
				return nil
			}
			w, cond := rankWindow(bare(items[i].(node)))
			if w == nil {
				return nil
			}
			window = w
			if cond != nil {
				others = append(others, cond)
			}
			e, err := pl.localRank(w, k)
			if err != nil {
				return nil
			}
			if cond != nil {
				// CASE WHEN cond THEN v ELSE rank END within k: cond, or the
				// rank within k.
				if e, err = pl.orOf([]node{clone(cond).(node), e}); err != nil {
					return nil
				}
			}
			out = append(out, e)
			ranked = true
			continue
		}
		e, ok := substituteColumns(t, names, items, true)
		if !ok {
			return nil
		}
		out = append(out, e)
		others = append(others, e)
	}
	if !ranked {
		return nil
	}
	// The rows kept otherwise must be whole partitions of the window, whose
	// ranks then come out as over all rows: the conditions read only
	// columns it partitions by.
	partitions := map[string]bool{}
	for _, p := range list(window, "partitions") {
		if pn := p.(node); str(pn["class"]) == "COLUMN_REF" {
			parts := columnNames(pn)
			partitions[strings.ToLower(parts[len(parts)-1])] = true
		}
	}
	for _, o := range others {
		whole := true
		walk(o, func(n node) bool {
			if str(n["class"]) == "COLUMN_REF" {
				parts := columnNames(n)
				whole = whole && partitions[strings.ToLower(parts[len(parts)-1])]
			}
			return whole
		})
		if !whole {
			return nil
		}
	}
	e, err := pl.orOf(out)
	if err != nil {
		return nil
	}
	return e
}

// disjuncts flattens e's ORs.
func disjuncts(e node) []node {
	if str(e["class"]) == "CONJUNCTION" && str(e["type"]) == "CONJUNCTION_OR" {
		var out []node
		for _, c := range list(e, "children") {
			out = append(out, disjuncts(c.(node))...)
		}
		return out
	}
	return []node{e}
}

// rankBound reports whether t keeps the rows whose column i of names is
// within k: col <= k, col < k + 1, or the same the other way around.
func rankBound(t node, names []string) (int, int64, bool) {
	if str(t["class"]) != "COMPARISON" {
		return 0, 0, false
	}
	col, bound, typ := child(t, "left"), child(t, "right"), str(t["type"])
	if str(col["class"]) != "COLUMN_REF" {
		col, bound = bound, col
		switch typ {
		case "COMPARE_GREATERTHANOREQUALTO":
			typ = "COMPARE_LESSTHANOREQUALTO"
		case "COMPARE_GREATERTHAN":
			typ = "COMPARE_LESSTHAN"
		default:
			typ = ""
		}
	}
	k, ok := intConstant(bound)
	if str(col["class"]) != "COLUMN_REF" || !ok {
		return 0, 0, false
	}
	switch typ {
	case "COMPARE_LESSTHANOREQUALTO":
	case "COMPARE_LESSTHAN":
		k--
	default:
		return 0, 0, false
	}
	parts := columnNames(col)
	for i, n := range names {
		if strings.EqualFold(n, parts[len(parts)-1]) {
			return i, k, k >= 0
		}
	}
	return 0, 0, false
}

// rankWindow returns the ranking window an item computes, and the
// condition under which it computes a constant instead: ROW_NUMBER(),
// RANK() or DENSE_RANK() over an order, alone or as the ELSE of a CASE
// with one WHEN whose value is a constant. Nil if the item is otherwise.
func rankWindow(e node) (w, cond node) {
	if str(e["class"]) == "CASE" {
		checks := list(e, "case_checks")
		if len(checks) != 1 {
			return nil, nil
		}
		check := checks[0].(node)
		if str(child(check, "then_expr")["class"]) != "CONSTANT" || hasClass(child(check, "when_expr"), "WINDOW") ||
			hasClass(child(check, "when_expr"), "SUBQUERY") {
			return nil, nil
		}
		w, _ = rankWindow(child(e, "else_expr"))
		if w == nil {
			return nil, nil
		}
		return w, child(check, "when_expr")
	}
	switch strings.ToLower(str(e["function_name"])) {
	case "row_number", "rank", "dense_rank":
	default:
		return nil, nil
	}
	if str(e["class"]) != "WINDOW" || len(list(e, "orders")) == 0 || len(list(e, "children")) > 0 || e["distinct"] == true ||
		e["ignore_nulls"] == true || child(e, "filter_expr") != nil {
		return nil, nil
	}
	for _, part := range []any{e["partitions"], e["orders"]} {
		if hasClass(part, "WINDOW") || hasClass(part, "SUBQUERY") {
			return nil, nil
		}
	}
	return e, nil
}

// localRank is the condition that a row ranks within k of w's partition
// among the rows it is evaluated over: RANK() for ROW_NUMBER() and RANK(),
// DENSE_RANK() for DENSE_RANK(), over w's partitions and order.
func (pl *planner) localRank(w node, k int64) (node, error) {
	fn := "rank"
	if strings.EqualFold(str(w["function_name"]), "dense_rank") {
		fn = "dense_rank"
	}
	e, err := pl.p.expr(fn + "() OVER () <= " + strconv.FormatInt(k, 10))
	if err != nil {
		return nil, err
	}
	r := child(e, "left")
	r["partitions"], r["orders"] = clone(w["partitions"]), clone(w["orders"])
	return e, nil
}

// orOf joins terms with OR, flat, as DuckDB parses them.
func (pl *planner) orOf(in []node) (node, error) {
	var terms []node
	for _, t := range in {
		terms = append(terms, disjuncts(t)...)
	}
	if len(terms) == 1 {
		return terms[0], nil
	}
	e, err := pl.p.expr("true OR false")
	if err != nil {
		return nil, err
	}
	children := make([]any, len(terms))
	for i, t := range terms {
		children[i] = t
	}
	e["children"] = children
	return e, nil
}

// substituteColumns returns e with each column of names, by its last name,
// replaced with the item that computes it; ok is false if e reads another
// column, a subquery, or, with noWindows, an item that computes a window.
func substituteColumns(e node, names []string, items []any, noWindows bool) (node, bool) {
	ok := true
	out := rewrite(clone(e), func(n node) (node, bool) {
		if !ok {
			return nil, false
		}
		switch str(n["class"]) {
		case "SUBQUERY", "STAR", "LAMBDA", "PARAMETER":
			ok = false
			return nil, false
		case "COLUMN_REF":
			parts := columnNames(n)
			for i, name := range names {
				if strings.EqualFold(name, parts[len(parts)-1]) {
					item := bare(items[i].(node))
					if noWindows && hasClass(item, "WINDOW") {
						ok = false
						return nil, false
					}
					return item, true
				}
			}
			ok = false
		}
		return nil, false
	})
	if !ok {
		return nil, false
	}
	return out.(node), true
}

// markRows marks the aggregations whose rows make up query q with keep, a
// condition over q's columns as names in its select list: q itself, the
// branches of a UNION ALL, or what a projection reads.
func (pl *planner) markRows(q node, keep node, readOnce func(node) node, depth int) {
	if q == nil || depth > maxTopKDepth {
		return
	}
	switch str(q["type"]) {
	case "SET_OPERATION_NODE":
		if str(q["setop_type"]) != "UNION" || q["setop_all"] != true || len(list(q, "modifiers")) > 0 {
			return
		}
		// The union's columns are named as its first branch's.
		first := q
		for str(first["type"]) == "SET_OPERATION_NODE" {
			first = child(first, "left")
		}
		names := outputColumns(first, &scope{})
		if names == nil {
			return
		}
		var branches func(n node) []node
		branches = func(n node) []node {
			if str(n["type"]) == "SET_OPERATION_NODE" && str(n["setop_type"]) == "UNION" && n["setop_all"] == true && len(list(n, "modifiers")) == 0 {
				return append(branches(child(n, "left")), branches(child(n, "right"))...)
			}
			return []node{n}
		}
		for _, b := range branches(q) {
			items := list(b, "select_list")
			if str(b["type"]) != "SELECT_NODE" || len(items) != len(names) {
				continue
			}
			if k, ok := substituteColumns(byPosition(keep, names), positions(len(names)), items, false); ok {
				pl.markSelect(b, k, readOnce, depth+1)
			}
		}
	case "SELECT_NODE":
		names := outputColumns(q, &scope{})
		items := list(q, "select_list")
		if len(names) != len(items) {
			return
		}
		if k, ok := substituteColumns(keep, names, items, false); ok {
			pl.markSelect(q, k, readOnce, depth+1)
		}
	}
}

// markSelect marks q, a SELECT, with keep, a condition over its FROM's
// columns: if q aggregates, q itself; if it only projects what a relation
// read once computes, that relation's aggregations.
func (pl *planner) markSelect(q, keep node, readOnce func(node) node, depth int) {
	if projectionOnly(q) && !pl.hasAggregate(q["select_list"]) {
		pl.markRows(readOnce(child(q, "from_table")), keep, readOnce, depth+1)
		return
	}
	if str(q["type"]) != "SELECT_NODE" || child(q, "qualify") != nil || len(list(q, "modifiers")) > 0 || len(list(q, "group_sets")) > 1 ||
		str(q["aggregate_handling"]) != "STANDARD_HANDLING" ||
		len(list(q, "group_expressions")) == 0 && !pl.hasAggregate(q["select_list"]) {
		return
	}
	q[topKMark] = keep
}

// positions names columns by their positions, for byPosition.
func positions(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "__igloo_col_" + strconv.Itoa(i)
	}
	return out
}

// byPosition returns e with each column of names, by its last name, read
// by its position instead (see positions): a union's branches name their
// columns as they like.
func byPosition(e node, names []string) node {
	return rewrite(clone(e), func(n node) (node, bool) {
		if str(n["class"]) != "COLUMN_REF" {
			return nil, false
		}
		parts := columnNames(n)
		for i, name := range names {
			if strings.EqualFold(name, parts[len(parts)-1]) {
				c := clone(n).(node)
				c["column_names"] = []any{"__igloo_col_" + strconv.Itoa(i)}
				return c, true
			}
		}
		return nil, false
	}).(node)
}

// qualifyTopK returns the QUALIFY a stage that completes an aggregation's
// groups may apply, from the aggregation's mark (see markTopK), with
// rewritten saying it in terms of the stage's columns; nil without a mark,
// or when it cannot be said so.
func qualifyTopK(sel node, rewritten func(node) (node, error)) node {
	keep := child(sel, topKMark)
	if keep == nil {
		return nil
	}
	q, err := rewritten(clone(keep).(node))
	if err != nil || !overStage(q) {
		return nil
	}
	return q
}

// overStage reports whether a QUALIFY reads only a stage's own columns (as
// overPartials), in its ranking windows too.
func overStage(e node) bool {
	ok := true
	walk(e, func(n node) bool {
		switch str(n["class"]) {
		case "COLUMN_REF":
			parts := columnNames(n)
			if len(parts) != 1 || !strings.HasPrefix(parts[0], "__") {
				ok = false
			}
		case "SUBQUERY", "PARAMETER", "LAMBDA", "STAR":
			ok = false
		case "WINDOW":
			switch strings.ToLower(str(n["function_name"])) {
			case "rank", "dense_rank":
			default:
				ok = false
			}
		}
		return ok
	})
	return ok
}

// stripTopK returns n without marks of markTopK, a copy if it had any.
func stripTopK(n node) node {
	marked := false
	walk(n, func(m node) bool {
		if _, ok := m[topKMark]; ok {
			marked = true
		}
		return !marked
	})
	if !marked {
		return n
	}
	c := clone(n).(node)
	walk(c, func(m node) bool {
		delete(m, topKMark)
		return true
	})
	return c
}
