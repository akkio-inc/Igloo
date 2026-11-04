package distributed

import (
	"database/sql"
	"maps"
	"slices"
	"strings"
)

// Branches a filter leaves nothing of. A report computes sections of a profile
// as a union, each branch naming its section with a constant, and a filter
// around it keeps a few sections: one machine's optimizer drops the branches of
// the others, and their scans with them, while the planner planned every
// branch. One such report at ten times the data took 15 s on one machine and
// 160 s on 16 workers, which joined fact tables of the sections the filter
// dropped to its audience and exchanged 30 GB for none of its rows.
//
// So before planning, a filter on a column of a subquery or of a common table
// expression read once is followed down to the unions that make the column: it
// passes through a SELECT that names the column as it is, groups by it alone or
// with others in one grouping set, and computes no window function that does
// not partition by it, to the side of an inner or cross join it comes from. The
// branches of a union whose column is a constant the filter rejects (false or
// NULL) leave the union, whose first branch left casts its columns to the
// union's types and names, so that its columns stay as they were. The filter
// stays where it was, and rows it rejects are all that go.

// pruneBranches drops the branches of unions in stmt that the filters around
// them leave nothing of (see the top of this file), and returns how many.
func (pl *planner) pruneBranches(stmt node) int {
	b := &brancher{pl: pl}
	b.query(stmt, nil)
	return b.dropped
}

type brancher struct {
	pl *planner
	// dropped counts the branches left out.
	dropped int
}

// query visits the filters of query node q and of the queries in it.
func (b *brancher) query(q node, scope cteScope) {
	if q == nil {
		return
	}
	scope = scope.with(q)
	switch str(q["type"]) {
	case "SELECT_NODE":
		b.filters(q, scope)
		walkFrom(child(q, "from_table"), func(sub node) { b.query(sub, scope) })
		for _, e := range []any{q["select_list"], q["where_clause"], q["having"], q["qualify"]} {
			b.subqueries(e, scope)
		}
	case "SET_OPERATION_NODE":
		b.query(child(q, "left"), scope)
		b.query(child(q, "right"), scope)
	}
	for _, e := range list(child(q, "cte_map"), "map") {
		def := scope.find(str(e.(node)["key"]))
		if def != nil {
			b.query(def.query(), def.scope)
		}
	}
}

// subqueries visits the queries of subquery expressions in v.
func (b *brancher) subqueries(v any, scope cteScope) {
	walk(v, func(n node) bool {
		if str(n["class"]) == "SUBQUERY" {
			b.query(subqueryNode(n), scope)
			return false
		}
		return true
	})
}

// walkFrom calls fn with the query of each subquery in a FROM clause.
func walkFrom(from node, fn func(node)) {
	if from == nil {
		return
	}
	switch str(from["type"]) {
	case "SUBQUERY":
		fn(subqueryNode(from))
	case "JOIN":
		walkFrom(child(from, "left"), fn)
		walkFrom(child(from, "right"), fn)
	}
}

// filters follows each conjunct of sel's WHERE clause that reads one column of
// a relation of its FROM clause down to that relation (see restrict).
func (b *brancher) filters(sel node, scope cteScope) {
	for _, c := range conjuncts(child(sel, "where_clause")) {
		var ref node
		one := true
		walk(c, func(n node) bool {
			switch str(n["class"]) {
			case "COLUMN_REF":
				if ref != nil && !slices.EqualFunc(columnNames(ref), columnNames(n), strings.EqualFold) {
					one = false
				}
				ref = n
			case "SUBQUERY", "PARAMETER", "WINDOW", "AGGREGATE", "LAMBDA":
				one = false
			}
			return one
		})
		if !one || ref == nil {
			continue
		}
		rel, at, relScope, ok := b.source(sel, ref, scope)
		if !ok {
			continue
		}
		pred := rewrite(clone(c), func(n node) (node, bool) {
			if str(n["class"]) == "COLUMN_REF" {
				return node{"class": "COLUMN_REF", "type": "COLUMN_REF", "alias": "", "column_names": []any{placeholder}}, true
			}
			return nil, false
		}).(node)
		b.restrict(rel, at, pred, relScope)
	}
}

// placeholder stands for the column a filter reads, in the filter as restrict
// carries it down.
const placeholder = "__igloo_filtered"

// restrict reports whether every row of query node q fails pred on its column
// at, and drops the branches of unions in q of which that holds, as long as q
// is read nowhere else.
func (b *brancher) restrict(q node, at int, pred node, scope cteScope) bool {
	scope = scope.with(q)
	switch str(q["type"]) {
	case "SET_OPERATION_NODE":
		branches := unionBranches(q, str(q["setop_type"]), q["setop_all"])
		if branches == nil {
			return false
		}
		keep := make([]bool, len(branches))
		kept := 0
		for i, br := range branches {
			if keep[i] = !b.restrict(br, at, pred, scope); keep[i] {
				kept++
			}
		}
		if kept == 0 {
			return true
		}
		if kept < len(branches) && b.rebuild(q, branches, keep, scope) {
			b.dropped += len(branches) - kept
		}
		return false
	case "SELECT_NODE":
		items := list(q, "select_list")
		if at >= len(items) {
			return false
		}
		item := items[at].(node)
		if constant(item) {
			keep, known := b.holds(pred, item)
			return known && !keep
		}
		if str(item["class"]) != "COLUMN_REF" || !b.passes(q, item) {
			return false
		}
		rel, relAt, relScope, ok := b.source(q, item, scope)
		if !ok {
			return false
		}
		// A side of an inner or cross join whose rows all fail leaves the
		// join none.
		return b.restrict(rel, relAt, pred, relScope)
	}
	return false
}

// passes reports whether a filter on column ref of sel's FROM clause, named as
// it is by sel, may go below sel: sel limits no rows, groups by ref in its one
// grouping set if it groups or aggregates at all, and every window function
// it computes partitions by ref.
func (b *brancher) passes(sel node, ref node) bool {
	if hasModifier(sel, "LIMIT_MODIFIER") || hasModifier(sel, "LIMIT_PERCENT_MODIFIER") {
		return false
	}
	same := func(e any) bool {
		n, ok := e.(node)
		return ok && str(n["class"]) == "COLUMN_REF" && slices.EqualFunc(columnNames(n), columnNames(ref), strings.EqualFold)
	}
	groups := list(sel, "group_expressions")
	if len(groups) > 0 || hasClass(sel["select_list"], "AGGREGATE") || hasClass(sel["having"], "AGGREGATE") {
		if len(list(sel, "group_sets")) > 1 || !slices.ContainsFunc(groups, same) {
			return false
		}
	}
	ok := true
	for _, v := range []any{sel["select_list"], sel["qualify"]} {
		walk(v, func(n node) bool {
			if str(n["class"]) == "WINDOW" && !slices.ContainsFunc(list(n, "partitions"), same) {
				ok = false
			}
			return ok
		})
	}
	return ok
}

// source returns the query of the relation of sel's FROM clause that column ref
// reads, the index of the column among its columns, and the scope it is in: a
// subquery, or a common table expression read nowhere else, that is a side of
// inner and cross joins only. ok is false for any other.
func (b *brancher) source(sel node, ref node, scope cteScope) (q node, at int, inScope cteScope, ok bool) {
	names := columnNames(ref)
	var qualifier string
	switch len(names) {
	case 1:
	case 2:
		qualifier = names[0]
	default:
		return nil, 0, nil, false
	}
	column := names[len(names)-1]
	var leaves []node
	var outer bool
	var visit func(n node, nullable bool)
	visit = func(n node, nullable bool) {
		switch str(n["type"]) {
		case "JOIN":
			jt, rt := str(n["join_type"]), str(n["ref_type"])
			if rt != "REGULAR" && rt != "CROSS" {
				outer = true
				return
			}
			switch jt {
			case "INNER":
				visit(child(n, "left"), nullable)
				visit(child(n, "right"), nullable)
			case "LEFT":
				visit(child(n, "left"), nullable)
				visit(child(n, "right"), true)
			default:
				outer = true
			}
		default:
			if nullable {
				n = node{"type": "NULLABLE", "of": n}
			}
			leaves = append(leaves, n)
		}
	}
	visit(child(sel, "from_table"), false)
	if outer {
		return nil, 0, nil, false
	}
	var leaf node
	for _, l := range leaves {
		alias := str(l["alias"])
		if alias == "" {
			alias = str(l["table_name"])
		}
		if qualifier == "" && len(leaves) == 1 || qualifier != "" && strings.EqualFold(alias, qualifier) {
			leaf = l
		}
	}
	if leaf == nil || str(leaf["type"]) == "NULLABLE" {
		return nil, 0, nil, false
	}
	var cols []string
	switch str(leaf["type"]) {
	case "SUBQUERY":
		q, inScope = subqueryNode(leaf), scope
		for _, a := range list(leaf, "column_name_alias") {
			cols = append(cols, str(a))
		}
	case "BASE_TABLE":
		if str(leaf["schema_name"]) != "" || str(leaf["catalog_name"]) != "" {
			return nil, 0, nil, false
		}
		def := scope.find(str(leaf["table_name"]))
		if def == nil || b.pl.uses(def) != 1 {
			return nil, 0, nil, false
		}
		q, inScope = def.query(), def.scope
		for _, a := range list(def.entry, "aliases") {
			cols = append(cols, str(a))
		}
	default:
		return nil, 0, nil, false
	}
	out := outputNames(q)
	copy(out, cols)
	at = slices.IndexFunc(out, func(n string) bool { return strings.EqualFold(n, column) })
	if at < 0 || slices.IndexFunc(out[at+1:], func(n string) bool { return strings.EqualFold(n, column) }) >= 0 {
		return nil, 0, nil, false
	}
	return q, at, inScope, true
}

// outputNames returns the names of query node q's columns, "" where a column's
// name is not its alias or its column's.
func outputNames(q node) []string {
	for str(q["type"]) == "SET_OPERATION_NODE" {
		q = child(q, "left")
	}
	var out []string
	for _, item := range list(q, "select_list") {
		e := item.(node)
		switch {
		case str(e["alias"]) != "":
			out = append(out, str(e["alias"]))
		case str(e["class"]) == "COLUMN_REF":
			names := columnNames(e)
			out = append(out, names[len(names)-1])
		case str(e["class"]) == "STAR":
			return nil
		default:
			out = append(out, "")
		}
	}
	return out
}

// constant reports whether e is a constant expression: it reads no column,
// calls no aggregate, window, volatile or time function and holds no subquery
// or parameter.
func constant(e node) bool {
	ok := true
	walk(e, func(n node) bool {
		switch str(n["class"]) {
		case "COLUMN_REF", "SUBQUERY", "PARAMETER", "WINDOW", "AGGREGATE", "LAMBDA", "STAR":
			ok = false
		case "FUNCTION":
			name := strings.ToLower(str(n["function_name"]))
			if _, t := timeFunctions[name]; t || volatileFunctions[name] {
				ok = false
			}
		}
		return ok
	})
	return ok
}

// holds evaluates pred with value in its column's place, and reports whether
// it is true; known is false where it cannot be evaluated here.
func (b *brancher) holds(pred, value node) (keep, known bool) {
	e := rewrite(clone(pred), func(n node) (node, bool) {
		if str(n["class"]) == "COLUMN_REF" && slices.Equal(columnNames(n), []string{placeholder}) {
			v := clone(value).(node)
			v["alias"] = ""
			return v, true
		}
		return nil, false
	}).(node)
	sel, err := b.pl.p.parse("SELECT NULL AS __igloo_holds")
	if err != nil {
		return false, false
	}
	e["alias"] = "__igloo_holds"
	sel["select_list"] = []any{e}
	text, err := b.pl.p.print(sel)
	if err != nil {
		return false, false
	}
	var v sql.NullBool
	if err := b.pl.p.db.QueryRowContext(b.pl.ctx, "SELECT CAST(("+text+") AS BOOLEAN)").Scan(&v); err != nil {
		return false, false
	}
	return v.Valid && v.Bool, true
}

// unionBranches returns the branches of q, a tree of unions of one kind (setop,
// all) with no modifiers or WITH clauses of their own, left to right; nil for
// any other node.
func unionBranches(q node, setop string, all any) []node {
	if str(q["type"]) != "SET_OPERATION_NODE" {
		return []node{q}
	}
	if setop != "UNION" || str(q["setop_type"]) != setop || q["setop_all"] != all || len(list(q, "modifiers")) > 0 ||
		len(list(child(q, "cte_map"), "map")) > 0 {
		return nil
	}
	left := unionBranches(child(q, "left"), setop, all)
	right := unionBranches(child(q, "right"), setop, all)
	if left == nil || right == nil {
		return nil
	}
	return append(left, right...)
}

// rebuild makes q, a union of branches (see unionBranches), the union of
// those keep says, some of them: the first casts its columns to q's types,
// under q's names, so that q's columns stay as they were, and the one branch
// left of a UNION that is not ALL keeps its rows distinct. q stays as it was,
// and rebuild returns false, if it cannot be described, or the first branch
// left is no plain SELECT of as many columns, or the one left of such a UNION
// has modifiers.
func (b *brancher) rebuild(q node, branches []node, keep []bool, scope cteScope) bool {
	cols, err := b.pl.describe(q, scope)
	at := slices.Index(keep, true)
	first := branches[at]
	items := list(first, "select_list")
	if err != nil || str(first["type"]) != "SELECT_NODE" || len(items) != len(cols) || hasClass(items, "STAR") {
		return false
	}
	var distinct any
	if q["setop_all"] != true && !slices.Contains(keep[at+1:], true) {
		// A UNION's rows are distinct, the one branch left's too.
		d, err := b.pl.p.parse("SELECT DISTINCT 1")
		if err != nil || len(list(first, "modifiers")) > 0 {
			return false
		}
		distinct = clone(d["modifiers"])
	}
	cast := make([]any, len(items))
	for i, it := range items {
		e, err := b.pl.p.expr("CAST(__igloo_x AS " + cols[i].Type + ")")
		if err != nil {
			return false
		}
		v := clone(it).(node)
		v["alias"] = ""
		cast[i] = aliased(rewrite(e, func(n node) (node, bool) {
			if str(n["class"]) == "COLUMN_REF" && slices.Equal(columnNames(n), []string{"__igloo_x"}) {
				return v, true
			}
			return nil, false
		}).(node), cols[i].Name)
	}
	first["select_list"] = cast
	if distinct != nil {
		first["modifiers"] = distinct
	}
	// The union of the branches left, as DuckDB parses it: it nests unions in
	// its own way, and a tree nested another way does not print as itself.
	tree := first
	if slices.Contains(keep[at+1:], true) {
		var texts []string
		for i, br := range branches {
			if !keep[i] {
				continue
			}
			text, err := b.pl.p.print(br)
			if err != nil {
				return false
			}
			texts = append(texts, "("+text+")")
		}
		op := " UNION ALL "
		if q["setop_all"] != true {
			op = " UNION "
		}
		if tree, err = b.pl.p.parse(strings.Join(texts, op)); err != nil {
			return false
		}
	}
	clear(q)
	maps.Copy(q, tree)
	return true
}
