package distributed

import (
	"strconv"
	"strings"
)

// Scalar subqueries of a statement's select list. TPC-DS q09 computes its
// columns from fifteen scalar subqueries, each a count or average of a range
// of store_sales, over a FROM of one row of another table:
//
//	SELECT CASE WHEN (SELECT count(*) FROM store_sales WHERE ...) > 74129
//	  THEN (SELECT avg(...) FROM store_sales WHERE ...) ELSE ... END AS bucket1, ...
//	FROM reason WHERE r_reason_sk = 1
//
// A statement's own select list is computed on the coordinator, over what
// the stages send it, where a subquery would read its tables whole, so the
// planner kept such a statement on one machine. A subquery that stands on
// its own and aggregates without groups has one row, always, and one value
// for every row of the statement: it moves to the statement's FROM, cross
// joined, as a relation the workers compute as they do any aggregate:
//
//	SELECT CASE WHEN __igloo_lifted_0.v > 74129 THEN __igloo_lifted_1.v ELSE ... END AS bucket1, ...
//	FROM reason, (SELECT count(*) AS v FROM store_sales WHERE ...) AS __igloo_lifted_0, ...
//	WHERE r_reason_sk = 1
//
// One that may have no row, or more than one, would give no row or several
// there, where it gives NULL or fails: it stays where it is. So does one
// that reads no more than the coordinator may read itself (see
// coordinatorReads), which it computes sooner than a stage would.

// liftedPrefix starts the names of the relations lifted subqueries become.
const liftedPrefix = "__igloo_lifted_"

// liftScalars moves the scalar subqueries that stand on their own, aggregate
// without groups and read more than the coordinator may read itself out of
// the select list of stmt, a statement with a FROM clause that neither groups
// nor aggregates nor computes window functions, into relations its FROM is
// cross joined to (see the top of this file). It returns how many it moved.
func (pl *planner) liftScalars(stmt node) int {
	from := child(stmt, "from_table")
	if str(stmt["type"]) != "SELECT_NODE" || from == nil || str(from["type"]) == "EMPTY" || str(from["type"]) == "EMPTY_FROM" ||
		len(list(stmt, "group_expressions")) > 0 || pl.hasAggregate(stmt["select_list"]) ||
		hasClass(stmt["select_list"], "WINDOW") || child(stmt, "having") != nil || child(stmt, "qualify") != nil ||
		child(stmt, "sample") != nil || str(stmt["aggregate_handling"]) != "STANDARD_HANDLING" {
		return 0
	}
	for _, m := range list(stmt, "modifiers") {
		if str(m.(node)["type"]) == "DISTINCT_MODIFIER" {
			return 0
		}
	}
	scope := cteScope(nil).with(stmt)
	var subs []node
	moved := rewrite(stmt["select_list"], func(n node) (node, bool) {
		if str(n["class"]) != "SUBQUERY" || str(n["subquery_type"]) != "SCALAR" {
			return nil, false
		}
		q := subqueryNode(n)
		if !pl.oneRow(q) || hasVolatile(q) {
			return nil, false
		}
		if b, err := pl.readBytes(q, scope, 0); err != nil || b <= pl.coordinatorReads() {
			return nil, false
		}
		if cols, err := pl.relationColumns(q, scope, nil); err != nil || len(cols) != 1 {
			return nil, false
		}
		name := liftedPrefix + strconv.Itoa(len(subs))
		subs = append(subs, q)
		return node{"class": "COLUMN_REF", "type": "COLUMN_REF", "alias": str(n["alias"]), "column_names": []any{name, "v"}}, true
	})
	if len(subs) == 0 {
		return 0
	}
	for i, q := range subs {
		q = clone(q).(node)
		list(q, "select_list")[0].(node)["alias"] = "v"
		ref, err := pl.p.template("SELECT * FROM (SELECT 1) AS " + liftedPrefix + strconv.Itoa(i))
		if err != nil {
			return 0
		}
		lifted := child(ref, "from_table")
		lifted["subquery"].(node)["node"] = q
		cross, err := pl.p.template("SELECT * FROM (SELECT 1) AS a CROSS JOIN (SELECT 1) AS b")
		if err != nil {
			return 0
		}
		j := child(cross, "from_table")
		j["left"], j["right"] = from, lifted
		from = j
	}
	stmt["select_list"] = moved
	stmt["from_table"] = from
	return len(subs)
}

// oneRow reports whether query node q has exactly one row, whatever its
// tables hold: a SELECT of one aggregate of all of its rows, which no
// HAVING, LIMIT or OFFSET may leave without it.
func (pl *planner) oneRow(q node) bool {
	return str(q["type"]) == "SELECT_NODE" && len(list(q, "select_list")) == 1 && len(list(q, "group_expressions")) == 0 &&
		pl.hasAggregate(q["select_list"]) && !hasClass(q["select_list"], "WINDOW") && child(q, "having") == nil &&
		child(q, "qualify") == nil && len(list(q, "modifiers")) == 0 && str(q["aggregate_handling"]) == "STANDARD_HANDLING"
}

// hasVolatile reports whether v calls a volatile function.
func hasVolatile(v any) bool {
	found := false
	walk(v, func(n node) bool {
		if str(n["class"]) == "FUNCTION" && volatileFunctions[strings.ToLower(str(n["function_name"]))] {
			found = true
		}
		return !found
	})
	return found
}
