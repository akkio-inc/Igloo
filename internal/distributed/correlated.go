package distributed

import (
	"slices"
	"strconv"
	"strings"
)

// Correlated subqueries of tables too large for every worker to read. A
// subquery that reads such a table for each row of its statement's FROM runs
// on one machine, or every task reads the table whole. Where its WHERE only
// makes columns of its FROM equal to columns of the statement's, and otherwise
// filters its own rows, it means a join on those columns, which workers read
// by partition (see partition.go):
//
//	EXISTS (SELECT 1 FROM l WHERE l.k = o.k AND f)      o SEMI JOIN (SELECT k FROM l WHERE f) d ON o.k = d.k
//	NOT EXISTS (SELECT 1 FROM l WHERE l.k = o.k AND f)  o ANTI JOIN (SELECT k FROM l WHERE f) d ON o.k = d.k
//	(SELECT avg(x) * 3 FROM l WHERE l.k = o.k AND f)    o LEFT JOIN (SELECT k, avg(x) * 3 AS v FROM l WHERE f GROUP BY k) d ON o.k = d.k, d.v
//
// A row whose key is NULL matches nothing either way, and the join keeps or
// drops it as the subquery would. Over no rows, the scalar subquery's value is
// NULL, as the join's is, for min, max, sum and avg and arithmetic on them; a
// count is 0, for which the join's NULL becomes 0. Any other aggregate, or
// expression that gives a value over no rows, stays as it is.
//
// NOT IN is not among them: one NULL in its subquery's rows makes it hold for
// no row, which a task reading a partition of those rows cannot know.

// correlation is what a correlated subquery of the shape above reads.
type correlation struct {
	// sel is the subquery's SELECT.
	sel node
	// outer and inner are the columns its WHERE makes equal, the statement's
	// and its own, in pairs.
	outer, inner []node
	// where are the conditions of its WHERE on its own columns alone.
	where []node
}

// correlationOf returns q's correlation, or nil if it has not that shape. ctes
// are the common table expressions it sees.
func (pl *planner) correlationOf(q node, ctes cteScope) *correlation {
	if str(q["type"]) != "SELECT_NODE" || len(list(child(q, "cte_map"), "map")) > 0 || len(list(q, "modifiers")) > 0 ||
		child(q, "sample") != nil || child(q, "qualify") != nil || child(q, "having") != nil || len(list(q, "group_expressions")) > 0 ||
		str(q["aggregate_handling"]) != "STANDARD_HANDLING" || hasClass(q["select_list"], "WINDOW") || hasClass(q["select_list"], "SUBQUERY") {
		return nil
	}
	from := child(q, "from_table")
	names := map[string]bool{}
	if !fromNames(from, names) {
		return nil
	}
	star, err := pl.p.parse("SELECT * FROM (SELECT 1) AS v")
	if err != nil {
		return nil
	}
	star["from_table"] = clone(from)
	cols, err := pl.describe(star, ctes)
	if err != nil {
		return nil
	}
	own := map[string]bool{}
	for _, c := range cols {
		own[strings.ToLower(c.Name)] = true
	}
	// inner says a column is the subquery's, ok that that is known.
	inner := func(ref node) (is, ok bool) {
		parts := columnNames(ref)
		switch len(parts) {
		case 1:
			return own[strings.ToLower(parts[0])], true
		case 2:
			return names[strings.ToLower(parts[0])] || own[strings.ToLower(parts[0])], true
		}
		return false, false
	}
	allInner := func(v any) bool {
		all := true
		walk(v, func(n node) bool {
			if str(n["class"]) == "COLUMN_REF" {
				if is, ok := inner(n); !is || !ok {
					all = false
				}
			}
			return all
		})
		return all
	}
	if !allInner(q["select_list"]) {
		return nil
	}
	c := &correlation{sel: q}
	for _, cond := range conjuncts(child(q, "where_clause")) {
		if hasClass(cond, "SUBQUERY") {
			return nil
		}
		if allInner(cond) {
			c.where = append(c.where, cond)
			continue
		}
		l, r := child(cond, "left"), child(cond, "right")
		if str(cond["class"]) != "COMPARISON" || str(cond["type"]) != "COMPARE_EQUAL" || str(l["class"]) != "COLUMN_REF" || str(r["class"]) != "COLUMN_REF" {
			return nil
		}
		li, lok := inner(l)
		ri, rok := inner(r)
		switch {
		case !lok || !rok || li == ri:
			return nil
		case li:
			c.inner, c.outer = append(c.inner, l), append(c.outer, r)
		default:
			c.inner, c.outer = append(c.inner, r), append(c.outer, l)
		}
	}
	if len(c.outer) == 0 {
		return nil
	}
	return c
}

// fromNames adds the names a FROM clause's items qualify their columns with
// to names, and reports whether it knows them all.
func fromNames(ref node, names map[string]bool) bool {
	switch str(ref["type"]) {
	case "JOIN":
		return fromNames(child(ref, "left"), names) && fromNames(child(ref, "right"), names)
	case "BASE_TABLE":
		name := str(ref["alias"])
		if name == "" {
			name = str(ref["table_name"])
		}
		names[strings.ToLower(name)] = true
		return true
	case "SUBQUERY":
		if str(ref["alias"]) == "" {
			return false
		}
		names[strings.ToLower(str(ref["alias"]))] = true
		return true
	}
	return false
}

// readsOuter reports whether sel's FROM clause binds a correlation's outer
// columns: the subquery may read a query further out.
func (pl *planner) readsOuter(sel node, c *correlation, ctes cteScope) bool {
	q, err := pl.p.parse("SELECT 1 FROM (SELECT 1) AS v")
	if err != nil {
		return false
	}
	q["from_table"] = clone(sel["from_table"])
	items := make([]any, len(c.outer))
	for i, e := range c.outer {
		items[i] = clone(e)
	}
	q["select_list"] = items
	_, err = pl.describe(q, ctes)
	return err == nil
}

// setWhere makes q's WHERE the conjunction of conds.
func (pl *planner) setWhere(q node, conds []node) error {
	where, err := pl.andOf(conds)
	if err != nil {
		return err
	}
	q["where_clause"] = nil
	if where != nil {
		q["where_clause"] = where
	}
	return nil
}

// andOf is the conjunction of conds, or nil for none.
func (pl *planner) andOf(conds []node) (node, error) {
	switch len(conds) {
	case 0:
		return nil, nil
	case 1:
		return clone(conds[0]).(node), nil
	}
	and, err := pl.p.template("SELECT 1 WHERE true AND true")
	if err != nil {
		return nil, err
	}
	w := clone(child(and, "where_clause")).(node)
	children := make([]any, len(conds))
	for i, c := range conds {
		children[i] = clone(c)
	}
	w["children"] = children
	return w, nil
}

// joinOn joins sel's FROM clause to sub, of the columns names, as kind (SEMI,
// ANTI or LEFT), on c's outer columns equal to its first columns.
func (pl *planner) joinOn(sel node, kind, alias string, sub node, names []any, c *correlation) (node, error) {
	join, err := pl.p.fromRef("SELECT 1 FROM a SEMI JOIN (SELECT 1) AS b(k) ON a.x = b.k")
	if err != nil {
		return nil, err
	}
	right := child(join, "right")
	child(right, "subquery")["node"] = sub
	right["alias"], right["column_name_alias"] = alias, names
	template := child(join, "condition")
	conds := make([]node, len(c.outer))
	for i, e := range c.outer {
		cond := clone(template).(node)
		cond["left"] = clone(e)
		child(cond, "right")["column_names"] = []any{alias, names[i]}
		conds[i] = cond
	}
	on, err := pl.andOf(conds)
	if err != nil {
		return nil, err
	}
	join["condition"], join["join_type"], join["left"] = on, kind, sel["from_table"]
	sel["from_table"] = join
	return right, nil
}

// innerItem reports whether the columns outer are all qualified by the name of
// one table or subquery of FROM clause from, which inner and cross joins only
// join to the others.
func innerItem(from node, outer []node) bool {
	name := ""
	for _, e := range outer {
		parts := columnNames(e)
		if len(parts) != 2 || name != "" && !strings.EqualFold(name, parts[0]) {
			return false
		}
		name = parts[0]
	}
	found := 0
	var visit func(n node) bool
	visit = func(n node) bool {
		switch str(n["type"]) {
		case "JOIN":
			jt, rt := str(n["join_type"]), str(n["ref_type"])
			switch {
			case rt != "REGULAR" && rt != "CROSS":
				return false
			case jt == "SEMI" || jt == "ANTI":
				// Another EXISTS made a join: its rows are its left side's.
				return visit(child(n, "left"))
			case jt != "INNER":
				return false
			}
			return visit(child(n, "left")) && visit(child(n, "right"))
		case "BASE_TABLE", "SUBQUERY":
			alias := str(n["alias"])
			if alias == "" {
				alias = str(n["table_name"])
			}
			if strings.EqualFold(alias, name) {
				found++
			}
			return true
		}
		return false
	}
	return visit(from) && found == 1
}

// keyColumns are the names of a correlated subquery's keys as a join reads them.
func keyColumns(n int) []any {
	names := make([]any, n)
	for i := range names {
		names[i] = "__igloo_k" + strconv.Itoa(i)
	}
	return names
}

// existsOf returns the EXISTS subquery cond is, or is the negation of.
func existsOf(cond node) (q node, negated bool) {
	if str(cond["class"]) == "OPERATOR" && str(cond["type"]) == "OPERATOR_NOT" {
		children := list(cond, "children")
		if len(children) != 1 {
			return nil, false
		}
		negated = true
		cond = children[0].(node)
	}
	if str(cond["class"]) != "SUBQUERY" || str(cond["subquery_type"]) != "EXISTS" {
		return nil, false
	}
	return subqueryNode(cond), negated
}

// existsJoin makes a condition of sel, EXISTS or NOT EXISTS over a table too
// large for every worker to read, the join it means, and reports whether it did.
func (pl *planner) existsJoin(sel, cond node, ctes cteScope) (bool, error) {
	q, negated := existsOf(cond)
	if q == nil {
		return false, nil
	}
	c := pl.correlationOf(q, ctes)
	if c == nil || !pl.readsOuter(sel, c, ctes) {
		return false, nil
	}
	if t := str(child(sel, "from_table")["type"]); negated && t != "BASE_TABLE" && t != "SUBQUERY" && !innerItem(child(sel, "from_table"), c.outer) {
		// The left of an anti join read by partition must be partitioned too:
		// one item, or the item of an inner join's that its key is a column of
		// (see addOuterJoin), as TPC-DS q69's customers, joined to their
		// addresses and demographics, are kept by their key.
		return false, nil
	}
	sub := clone(q).(node)
	items := make([]any, len(c.inner))
	for i, e := range c.inner {
		items[i] = clone(e)
	}
	sub["select_list"] = items
	if err := pl.setWhere(sub, c.where); err != nil {
		return false, err
	}
	if bytes, err := pl.estimate(sub, ctes); err != nil || bytes <= pl.opts.maxBroadcast {
		return false, nil
	}
	kind := "SEMI"
	if negated {
		kind = "ANTI"
	}
	alias := "__igloo_in" + strconv.Itoa(pl.ins)
	pl.ins++
	right, err := pl.joinOn(sel, kind, alias, sub, keyColumns(len(c.inner)), c)
	if err != nil {
		return false, err
	}
	if pl.forced == nil {
		pl.forced = map[uintptr]bool{}
	}
	pl.forced[identity(right)] = true
	return true, nil
}

// scalarValue reports whether e, a correlated scalar subquery's value, is NULL
// over no rows, or as count says 0: min, max, sum or avg, or arithmetic on them
// and constants, or one count.
func (pl *planner) scalarValue(e node) (count, ok bool) {
	if str(e["class"]) == "FUNCTION" && pl.isAggregate(e) {
		switch strings.ToLower(str(e["function_name"])) {
		case "count", "count_star":
			return true, true
		}
	}
	return false, pl.nullOverNoRows(e) && pl.hasAggregate(e)
}

func (pl *planner) nullOverNoRows(e node) bool {
	switch str(e["class"]) {
	case "CONSTANT":
		return true
	case "CAST":
		return pl.nullOverNoRows(child(e, "child"))
	case "FUNCTION":
		name := strings.ToLower(str(e["function_name"]))
		if pl.isAggregate(e) {
			return name == "min" || name == "max" || name == "sum" || name == "avg"
		}
		if e["is_operator"] != true || !slices.Contains([]string{"+", "-", "*", "/"}, name) {
			return false
		}
		for _, a := range list(e, "children") {
			if !pl.nullOverNoRows(a.(node)) {
				return false
			}
		}
		return true
	}
	return false
}

// scalarJoins makes the correlated scalar subqueries of sel's WHERE and, if it
// does not group, of its select list, over tables too large for every worker
// to read, the joins they mean. Their rows keep the names they had.
func (pl *planner) scalarJoins(sel node, ctes cteScope) error {
	if !hasClass(sel["where_clause"], "SUBQUERY") && !hasClass(sel["select_list"], "SUBQUERY") {
		return nil
	}
	if str(child(sel, "from_table")["type"]) == "EMPTY" {
		return nil
	}
	join := func(n node) (node, bool) {
		if str(n["class"]) != "SUBQUERY" {
			return nil, false
		}
		// Whatever it is, what is inside it is its own.
		same := n
		if str(n["subquery_type"]) != "SCALAR" {
			return same, true
		}
		q := subqueryNode(n)
		items := list(q, "select_list")
		if len(items) != 1 || str(q["type"]) != "SELECT_NODE" {
			return same, true
		}
		value := items[0].(node)
		count, ok := pl.scalarValue(value)
		if !ok {
			return same, true
		}
		c := pl.correlationOf(q, ctes)
		if c == nil || !pl.readsOuter(sel, c, ctes) {
			return same, true
		}
		sub := clone(q).(node)
		var selected, groups, set []any
		for i, e := range c.inner {
			selected = append(selected, bare(e))
			groups = append(groups, bare(e))
			set = append(set, jsonInt(i))
		}
		sub["select_list"] = append(selected, bare(value))
		sub["group_expressions"], sub["group_sets"] = groups, []any{set}
		if err := pl.setWhere(sub, c.where); err != nil {
			return same, true
		}
		if bytes, err := pl.estimate(sub, ctes); err != nil || bytes <= pl.opts.maxBroadcast {
			return same, true
		}
		alias := "__igloo_sc" + strconv.Itoa(pl.ins)
		pl.ins++
		names := append(keyColumns(len(c.inner)), "__igloo_v")
		if _, err := pl.joinOn(sel, "LEFT", alias, sub, names, c); err != nil {
			return same, true
		}
		text := "SELECT " + quoteIdent(alias) + ".__igloo_v"
		if count {
			text = "SELECT COALESCE(" + quoteIdent(alias) + ".__igloo_v, 0)"
		}
		tmpl, err := pl.p.parse(text)
		if err != nil {
			return same, true
		}
		out := list(tmpl, "select_list")[0].(node)
		out["alias"] = n["alias"]
		return out, true
	}
	from, where, selected := sel["from_table"], sel["where_clause"], sel["select_list"]
	grouped := len(list(sel, "group_expressions")) > 0 || str(sel["aggregate_handling"]) != "STANDARD_HANDLING" || pl.hasAggregate(selected)
	inList := !grouped && !hasClass(selected, "STAR") && hasClass(selected, "SUBQUERY")
	var cols []Column
	if inList {
		named := clone(sel).(node)
		named["cte_map"] = node{"map": []any{}}
		var err error
		if cols, err = pl.describe(named, ctes); err != nil {
			inList = false
		}
	}
	pl.undo = append(pl.undo, func() { sel["from_table"], sel["where_clause"], sel["select_list"] = from, where, selected })
	sel["where_clause"] = rewrite(clone(where), join)
	if !inList {
		return nil
	}
	items := clone(selected).([]any)
	for i, item := range items {
		items[i] = rewrite(item, join)
		if e := items[i].(node); str(e["alias"]) == "" && readsScalarJoin(e) && i < len(cols) {
			e["alias"] = cols[i].Name
		}
	}
	sel["select_list"] = items
	return nil
}

// readsScalarJoin reports whether e reads a join scalarJoins added.
func readsScalarJoin(e node) bool {
	found := false
	walk(e, func(n node) bool {
		if str(n["class"]) == "COLUMN_REF" {
			for _, p := range columnNames(n) {
				if strings.HasPrefix(p, "__igloo_sc") {
					found = true
				}
			}
		}
		return !found
	})
	return found
}
