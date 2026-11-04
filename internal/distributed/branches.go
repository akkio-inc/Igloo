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
// expression read once is followed down to the unions that make the column,
// through what it provably commutes with (see passes), to the side of an inner
// or cross join it comes from, UNION ALL and UNION. The branches of a union
// whose column is a constant the filter rejects (false or NULL) leave the
// union, whose first branch left casts its columns to the union's types and
// names, so that its columns stay as they were, and keeps its rows distinct if
// it is the one left of a UNION. The filter stays where it was, and rows it
// rejects are all that go.
//
// It also passes through a star that selects the column, as Snowflake's
// FLATTEN, translated, does, and reads the fields of a struct column. Where the
// column is a CASE whose arms are each a constant, or a struct whose field the
// filter reads is, the SELECT that computes it keeps only the rows of the arms
// the filter keeps: a report's FLATTEN made rows of five kinds of each visit,
// one kind a summary of the others that a filter around the report dropped,
// after the workers had computed and counted it.

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
			case "FUNCTION":
				// Evaluated here, on constants, it must say what it says there.
				name := strings.ToLower(str(n["function_name"]))
				if _, t := timeFunctions[name]; t || volatileFunctions[name] {
					one = false
				}
			}
			return one
		})
		if !one || ref == nil {
			continue
		}
		rel, col, relScope, ok := b.source(sel, ref, scope)
		if !ok {
			continue
		}
		pred := rewrite(clone(c), func(n node) (node, bool) {
			if str(n["class"]) == "COLUMN_REF" {
				return node{"class": "COLUMN_REF", "type": "COLUMN_REF", "alias": "", "column_names": []any{placeholder}}, true
			}
			return nil, false
		}).(node)
		b.restrict(rel, col, pred, relScope)
	}
}

// filtered is the column of a query restrict follows a filter to: at, its index
// among the query's columns, or -1 where a star makes it unknown, and name; and
// path, the fields of the struct it is that the filter reads, outermost first.
type filtered struct {
	at   int
	name string
	path []string
}

// placeholder stands for the column a filter reads, in the filter as restrict
// carries it down.
const placeholder = "__igloo_filtered"

// restrict reports whether every row of query node q fails pred on its column
// c, and drops the branches of unions in q of which that holds, and the rows of
// CASE arms (see arms), as long as q is read nowhere else.
func (b *brancher) restrict(q node, c filtered, pred node, scope cteScope) bool {
	scope = scope.with(q)
	switch str(q["type"]) {
	case "SET_OPERATION_NODE":
		branches := unionBranches(q, str(q["setop_type"]), q["setop_all"])
		if branches == nil || c.at < 0 {
			return false
		}
		keep := make([]bool, len(branches))
		kept := 0
		for i, br := range branches {
			// The branches' columns by position: their names are the first's.
			if keep[i] = !b.restrict(br, filtered{at: c.at, path: c.path}, pred, scope); keep[i] {
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
		item := selected(q, c)
		if item == nil {
			return false
		}
		if v := fieldOf(item, c.path); v != nil && constant(v) {
			keep, known := b.holds(pred, v)
			return known && !keep
		}
		if str(item["class"]) == "CASE" {
			return b.arms(q, item, c.path, pred)
		}
		if str(item["class"]) != "COLUMN_REF" || !b.passes(q, item) {
			return false
		}
		rel, col, relScope, ok := b.source(q, item, scope)
		if !ok {
			return false
		}
		col.path = append(col.path, c.path...)
		// A side of an inner or cross join whose rows all fail leaves the
		// join none.
		return b.restrict(rel, col, pred, relScope)
	}
	return false
}

// selected returns the expression of SELECT node sel's column c: the item at
// its index, if sel selects no star, or else the item named so, or a reference
// to the column of that name of the relation a star selects; nil if unknown.
func selected(sel node, c filtered) node {
	items := list(sel, "select_list")
	var star node
	for _, it := range items {
		if e := it.(node); str(e["class"]) == "STAR" {
			if star != nil {
				return nil
			}
			star = e
		}
	}
	if star == nil {
		if c.at < 0 || c.at >= len(items) {
			return nil
		}
		return items[c.at].(node)
	}
	if c.name == "" || star["columns"] == true || len(list(star, "exclude_list")) > 0 || len(list(star, "replace_list")) > 0 ||
		len(list(star, "rename_list")) > 0 || child(star, "expr") != nil {
		return nil
	}
	for _, it := range items {
		if e := it.(node); str(e["class"]) != "STAR" && strings.EqualFold(itemName(e), c.name) {
			return e
		}
	}
	names := []any{c.name}
	if rel := str(star["relation_name"]); rel != "" {
		names = []any{rel, c.name}
	}
	return node{"class": "COLUMN_REF", "type": "COLUMN_REF", "alias": "", "column_names": names}
}

// fieldOf returns the field path of e, a struct made by struct_pack, as far as
// e says it: e itself for no path, NULL for a field of NULL; nil if unknown.
func fieldOf(e node, path []string) node {
	for _, f := range path {
		switch {
		case str(e["class"]) == "CONSTANT" && child(e, "value") != nil && child(e, "value")["is_null"] == true:
			return e
		case str(e["class"]) == "FUNCTION" && strings.EqualFold(str(e["function_name"]), "struct_pack"):
			i := slices.IndexFunc(list(e, "children"), func(a any) bool { return strings.EqualFold(str(a.(node)["alias"]), f) })
			if i < 0 {
				return nil
			}
			e = list(e, "children")[i].(node)
		default:
			return nil
		}
	}
	return e
}

// arms reports whether every row of SELECT node sel fails pred on CASE
// expression item, the field path of it, each of whose arms, ELSE included, is
// a constant there: so if pred rejects them all. If it rejects only some, sel
// keeps only the rows of the others, with a WHERE condition of the same arms,
// each true or false. A CASE with an arm of any other value, as a masking
// policy's, stays as it is. sel must compute rows one for one, of arms whose
// conditions read no subquery, and call no volatile function.
func (b *brancher) arms(sel, item node, path []string, pred node) bool {
	if len(list(sel, "group_expressions")) > 0 || len(list(sel, "modifiers")) > 0 || hasClass(sel["select_list"], "AGGREGATE") ||
		hasClass(sel["select_list"], "WINDOW") || child(sel, "having") != nil || child(sel, "qualify") != nil ||
		child(sel, "sample") != nil || volatileIn(sel) {
		return false
	}
	checks := list(item, "case_checks")
	values := make([]node, 0, len(checks)+1)
	for _, c := range checks {
		when := child(c.(node), "when_expr")
		if hasClass(when, "SUBQUERY") || hasClass(when, "WINDOW") || hasClass(when, "AGGREGATE") {
			return false
		}
		values = append(values, child(c.(node), "then_expr"))
	}
	values = append(values, child(item, "else_expr"))
	keep := make([]bool, len(values))
	kept := 0
	for i, v := range values {
		// Kinds, each arm a constant: a CASE of any other value, such as a
		// masking policy's, stays as it is.
		f := fieldOf(v, path)
		if f == nil || !constant(f) {
			return false
		}
		k, known := b.holds(pred, f)
		if keep[i] = !known || k; keep[i] {
			kept++
		}
	}
	switch kept {
	case 0:
		return true
	case len(values):
		return false
	}
	yes, err := b.pl.p.expr("true")
	if err != nil {
		return false
	}
	no, err := b.pl.p.expr("false")
	if err != nil {
		return false
	}
	cond := clone(item).(node)
	cond["alias"] = ""
	pick := func(i int) node {
		if keep[i] {
			return clone(yes).(node)
		}
		return clone(no).(node)
	}
	for i, c := range list(cond, "case_checks") {
		c.(node)["then_expr"] = pick(i)
	}
	cond["else_expr"] = pick(len(values) - 1)
	if b.pl.setWhere(sel, append(conjuncts(child(sel, "where_clause")), cond)) != nil {
		return false
	}
	b.dropped += len(values) - kept
	return false
}

// passes reports whether a filter on column ref of sel's FROM clause, named as
// it is by sel, commutes with sel, and so may go below it: it passes only what
// keeps or drops each row by the rows of its own value of ref. That is a
// projection of the column, beside others and unnests of others, with DISTINCT
// or ORDER BY or neither; a GROUP BY of it in sel's one grouping set, HAVING
// included; and window functions partitioned by it. Anything else stops it:
// LIMIT, DISTINCT ON, QUALIFY, SAMPLE, aggregates of groups without it, window
// functions not partitioned by it, and volatile functions anywhere in sel.
// Leaving out two kinds of rows it passed before this list, a DISTINCT ON and
// the duplicates a UNION's one branch left had, returned wrong rows.
func (b *brancher) passes(sel node, ref node) bool {
	if child(sel, "sample") != nil || child(sel, "qualify") != nil || volatileIn(sel) {
		return false
	}
	for _, m := range list(sel, "modifiers") {
		switch mod := m.(node); str(mod["type"]) {
		case "ORDER_MODIFIER":
		case "DISTINCT_MODIFIER":
			if len(list(mod, "distinct_on_targets")) > 0 {
				return false
			}
		default:
			return false
		}
	}
	same := func(e any) bool {
		n, ok := e.(node)
		return ok && str(n["class"]) == "COLUMN_REF" && slices.EqualFunc(columnNames(n), columnNames(ref), strings.EqualFold)
	}
	groups := list(sel, "group_expressions")
	if len(groups) > 0 || hasClass(sel["select_list"], "AGGREGATE") || child(sel, "having") != nil {
		if len(list(sel, "group_sets")) != 1 || !slices.ContainsFunc(groups, same) {
			return false
		}
	}
	ok := true
	walk(sel["select_list"], func(n node) bool {
		if str(n["class"]) == "WINDOW" && !slices.ContainsFunc(list(n, "partitions"), same) {
			ok = false
		}
		return ok
	})
	return ok
}

// volatileIn reports whether SELECT node sel calls a volatile function in its
// columns, its WHERE, GROUP BY or HAVING.
func volatileIn(sel node) bool {
	for _, v := range []any{sel["select_list"], sel["where_clause"], sel["group_expressions"], sel["having"]} {
		found := false
		walk(v, func(n node) bool {
			if str(n["class"]) == "FUNCTION" && volatileFunctions[strings.ToLower(str(n["function_name"]))] {
				found = true
			}
			return !found
		})
		if found {
			return true
		}
	}
	return false
}

// source returns the query of the relation of sel's FROM clause that column ref
// reads, the column, the fields of it ref reads after the qualifier and the
// column, and the scope it is in: a subquery, or a common table expression read
// nowhere else, that is a side of inner and cross joins only. ok is false for any
// other.
func (b *brancher) source(sel node, ref node, scope cteScope) (q node, c filtered, inScope cteScope, ok bool) {
	names := columnNames(ref)
	if len(names) == 0 {
		return nil, filtered{}, nil, false
	}
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
		return nil, filtered{}, nil, false
	}
	aliasOf := func(l node) string {
		if str(l["type"]) == "NULLABLE" {
			l = child(l, "of")
		}
		if a := str(l["alias"]); a != "" {
			return a
		}
		return str(l["table_name"])
	}
	// The qualifier, if the first name is a relation's; then the column, and
	// the fields of it.
	var leaf node
	rest := names
	for _, l := range leaves {
		if len(names) > 1 && strings.EqualFold(aliasOf(l), names[0]) {
			leaf, rest = l, names[1:]
		}
	}
	if leaf == nil && len(leaves) == 1 {
		leaf = leaves[0]
	}
	if leaf == nil || str(leaf["type"]) == "NULLABLE" || child(leaf, "sample") != nil {
		return nil, filtered{}, nil, false
	}
	c = filtered{name: rest[0], path: slices.Clone(rest[1:])}
	var cols []string
	switch str(leaf["type"]) {
	case "SUBQUERY":
		q, inScope = subqueryNode(leaf), scope
		for _, a := range list(leaf, "column_name_alias") {
			cols = append(cols, str(a))
		}
	case "BASE_TABLE":
		if str(leaf["schema_name"]) != "" || str(leaf["catalog_name"]) != "" {
			return nil, filtered{}, nil, false
		}
		def := scope.find(str(leaf["table_name"]))
		if def == nil || b.pl.uses(def) != 1 {
			return nil, filtered{}, nil, false
		}
		q, inScope = def.query(), def.scope
		for _, a := range list(def.entry, "aliases") {
			cols = append(cols, str(a))
		}
	default:
		return nil, filtered{}, nil, false
	}
	out := outputNames(q)
	if out == nil {
		// A star: known by name, unless the relation renames its columns.
		if len(cols) > 0 {
			return nil, filtered{}, nil, false
		}
		c.at = -1
		return q, c, inScope, true
	}
	copy(out, cols)
	c.at = slices.IndexFunc(out, func(n string) bool { return strings.EqualFold(n, c.name) })
	if c.at < 0 || slices.IndexFunc(out[c.at+1:], func(n string) bool { return strings.EqualFold(n, c.name) }) >= 0 {
		return nil, filtered{}, nil, false
	}
	return q, c, inScope, true
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
		if str(e["class"]) == "STAR" {
			return nil
		}
		out = append(out, itemName(e))
	}
	return out
}

// itemName is the name of a column a select list's item e makes: its alias, or
// its column's name; "" otherwise.
func itemName(e node) string {
	switch {
	case str(e["alias"]) != "":
		return str(e["alias"])
	case str(e["class"]) == "COLUMN_REF":
		names := columnNames(e)
		return names[len(names)-1]
	}
	return ""
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
