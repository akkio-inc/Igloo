package distributed

import "strings"

// Orders nothing depends on. A report's last SELECT often sorts its rows,
// and the client's query reads it as a subquery that filters them and
// sorts them again, by an order of its own, before it takes the first:
// the report's ORDER BY changes nothing. Nor does that of a subquery or
// common table expression an aggregation, DISTINCT or UNION reads, whose
// rows come in no order of their inputs'. But a sorted subquery read by a
// SELECT that passes its rows on as they come, to the client or to a
// LIMIT without ORDER BY, keeps the rows in its order on one machine (an
// order DuckDB keeps as it reads, though SQL does not promise it), and so
// does a call that sees its rows in the order they come: list() or
// string_agg() without ORDER BY of their own, first(), row_number() OVER
// () and the like. dropOrders removes the ORDER BY, without LIMIT or
// OFFSET, of every subquery in FROM and common table expression whose
// order nothing above depends on, so that a relation workers compute need
// not sort what is written to an exchange in no order, and a subquery
// that only sorted passes its rows through.

// orderedCalls see their rows in the order they come, without an ORDER BY
// of their own.
var orderedCalls = map[string]bool{
	"list": true, "array_agg": true, "string_agg": true, "group_concat": true, "listagg": true,
	"first": true, "last": true, "any_value": true, "arbitrary": true,
	"arg_min": true, "arg_max": true, "argmin": true, "argmax": true, "min_by": true, "max_by": true,
	"row_number": true, "rank": true, "dense_rank": true, "percent_rank": true, "cume_dist": true, "ntile": true,
	"lag": true, "lead": true, "first_value": true, "last_value": true, "nth_value": true,
}

// orderDrop holds what dropOrders found of the common table expressions
// in scope: for each, whether a reader's result depends on its order.
type orderDrop struct {
	pl     *planner
	scopes []map[string]*bool
}

// dropOrders removes the ORDER BY clauses of stmt that nothing depends on
// (see the top of this file). Its result's order is the client's.
func (pl *planner) dropOrders(stmt node) {
	d := &orderDrop{pl: pl}
	d.query(stmt, true)
}

// query visits query node q, whose order matters if matters says so.
func (d *orderDrop) query(q node, matters bool) {
	if q == nil {
		return
	}
	entries := list(child(q, "cte_map"), "map")
	scope := map[string]*bool{}
	for _, e := range entries {
		f := false
		scope[strings.ToLower(str(e.(node)["key"]))] = &f
	}
	d.scopes = append(d.scopes, scope)
	defer func() { d.scopes = d.scopes[:len(d.scopes)-1] }()

	switch str(q["type"]) {
	case "SELECT_NODE":
		inner := d.selectNode(q, matters)
		d.from(child(q, "from_table"), inner)
		d.expressions(q)
	case "SET_OPERATION_NODE":
		inner := matters
		if !matters {
			dropOrder(q)
		}
		switch {
		case hasModifier(q, "ORDER_MODIFIER"):
			inner = false
		case str(q["setop_type"]) != "UNION" || q["setop_all"] != true:
			// Distinct rows, which come in no order of their branches'.
			inner = false
		}
		if hasModifier(q, "LIMIT_MODIFIER") && !hasModifier(q, "ORDER_MODIFIER") {
			inner = true
		}
		d.query(child(q, "left"), inner)
		d.query(child(q, "right"), inner)
	default:
		// Whatever else it is (a recursive common table expression), what
		// it reads keeps its order.
		walk(q, func(n node) bool {
			if str(n["type"]) == "BASE_TABLE" {
				d.from(n, true)
			}
			return true
		})
	}
	// Its common table expressions, last first: each is read by the query
	// and by those after it, whose readers decided whether its order
	// matters.
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i].(node)
		def := child(child(child(e, "value"), "query"), "node")
		d.scopes[len(d.scopes)-1] = scopeUpTo(entries, i, scope)
		d.query(def, *scope[strings.ToLower(str(e["key"]))])
	}
}

// scopeUpTo is scope as the common table expression at index i of entries
// sees it: those before it, and the ones around.
func scopeUpTo(entries []any, i int, scope map[string]*bool) map[string]*bool {
	out := map[string]*bool{}
	for _, e := range entries[:i] {
		k := strings.ToLower(str(e.(node)["key"]))
		out[k] = scope[k]
	}
	return out
}

// selectNode drops sel's own ORDER BY if its order does not matter, and
// returns whether the order of the rows its FROM clause reads does.
func (d *orderDrop) selectNode(sel node, matters bool) bool {
	if !matters {
		dropOrder(sel)
	}
	if orderedCallsIn(sel) || hasModifier(sel, "LIMIT_MODIFIER") && !hasModifier(sel, "ORDER_MODIFIER") ||
		hasModifier(sel, "LIMIT_PERCENT_MODIFIER") && !hasModifier(sel, "ORDER_MODIFIER") {
		return true
	}
	for _, m := range list(sel, "modifiers") {
		mod := m.(node)
		if str(mod["type"]) == "DISTINCT_MODIFIER" && len(list(mod, "distinct_on_targets")) > 0 && !hasModifier(sel, "ORDER_MODIFIER") {
			// DISTINCT ON keeps each group's first row.
			return true
		}
	}
	switch {
	case hasModifier(sel, "ORDER_MODIFIER"), hasModifier(sel, "DISTINCT_MODIFIER"),
		len(list(sel, "group_expressions")) > 0, str(sel["aggregate_handling"]) == "FORCE_AGGREGATES",
		d.pl.hasAggregate(sel["select_list"]) || d.pl.hasAggregate(child(sel, "having")):
		return false
	}
	return matters
}

// from visits the subqueries and references to common table expressions
// of a FROM clause, whose order matters if matters says so.
func (d *orderDrop) from(ref node, matters bool) {
	switch str(ref["type"]) {
	case "JOIN":
		d.from(child(ref, "left"), matters)
		d.from(child(ref, "right"), matters)
		d.expressionsIn(child(ref, "condition"))
	case "SUBQUERY":
		d.query(subqueryNode(ref), matters)
	case "BASE_TABLE":
		if str(ref["schema_name"]) != "" || str(ref["catalog_name"]) != "" {
			return
		}
		name := strings.ToLower(str(ref["table_name"]))
		for i := len(d.scopes) - 1; i >= 0; i-- {
			if f, ok := d.scopes[i][name]; ok {
				if matters {
					*f = true
				}
				return
			}
		}
	case "TABLE_FUNCTION":
		d.expressionsIn(child(ref, "function"))
	}
}

// expressions visits the subquery expressions of sel's clauses.
func (d *orderDrop) expressions(sel node) {
	for k, v := range sel {
		switch k {
		case "from_table", "cte_map":
			continue
		}
		d.expressionsIn(v)
	}
}

// expressionsIn visits the subquery expressions in v, whose order is kept:
// what they read keeps its order too.
func (d *orderDrop) expressionsIn(v any) {
	walk(v, func(n node) bool {
		if str(n["class"]) == "SUBQUERY" {
			d.query(subqueryNode(n), true)
			return false
		}
		return true
	})
}

// dropOrder removes q's ORDER BY if it has no LIMIT or OFFSET.
func dropOrder(q node) {
	mods := list(q, "modifiers")
	if !hasModifier(q, "ORDER_MODIFIER") || hasModifier(q, "LIMIT_MODIFIER") || hasModifier(q, "LIMIT_PERCENT_MODIFIER") {
		return
	}
	kept := []any{}
	for _, m := range mods {
		if str(m.(node)["type"]) != "ORDER_MODIFIER" {
			kept = append(kept, m)
		}
	}
	q["modifiers"] = kept
}

// hasModifier says whether q has a modifier of type t.
func hasModifier(q node, t string) bool {
	for _, m := range list(q, "modifiers") {
		if str(m.(node)["type"]) == t {
			return true
		}
	}
	return false
}

// orderedCallsIn says whether sel calls a function that sees its rows in
// the order they come (orderedCalls), without an ORDER BY of its own.
func orderedCallsIn(sel node) bool {
	found := false
	for k, v := range sel {
		switch k {
		case "from_table", "cte_map", "modifiers":
			continue
		}
		walk(v, func(n node) bool {
			if found || str(n["class"]) == "SUBQUERY" {
				return false
			}
			name := strings.ToLower(str(n["function_name"]))
			switch str(n["class"]) {
			case "FUNCTION", "AGGREGATE":
				if orderedCalls[name] && len(list(child(n, "order_bys"), "orders")) == 0 {
					found = true
				}
			case "WINDOW":
				if orderedCalls[name] && len(list(n, "orders")) == 0 {
					found = true
				}
			}
			return !found
		})
	}
	return found
}
