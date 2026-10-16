package distributed

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// aggregating reports whether the candidate groups or aggregates.
func (c *candidate) aggregating(pl *planner) bool {
	sel := c.sel
	if len(list(sel, "group_expressions")) > 0 || str(sel["aggregate_handling"]) == "FORCE_AGGREGATES" || child(sel, "having") != nil {
		return true
	}
	return pl.hasAggregate(sel["select_list"]) || pl.hasAggregate(child(sel, "qualify")) || pl.hasAggregate(sel["modifiers"])
}

// aggregation is a candidate SELECT taken apart for an aggregate job.
type aggregation struct {
	pl *planner
	c  *candidate
	// items are the select list; aliases maps each explicit alias, lower
	// case, to its item.
	items   []node
	aliases map[string]int
	// groups are the group expressions with positions and aliases
	// replaced by what they stand for; keys are their canonical forms.
	groups    []node
	groupKeys []string
	// sets are the grouping sets, as indexes into groups.
	sets []any
	// aggs are the distinct aggregate calls, by canonical form.
	aggs    []*aggCall
	aggKeys map[string]*aggCall
	// distinct is the argument of the aggregates over DISTINCT values, if
	// any; the workers group by it too.
	distinct    node
	distinctKey string
	// partials are the fragment's partial aggregate expressions.
	partials []node
}

// aggCall is one aggregate call and how it is computed in two steps.
type aggCall struct {
	call node
	// partials are the indexes of its partial aggregates.
	partials []int
	// kind says how the coordinator combines them.
	kind string
	// typ is the call's result type, for casting the combined value back.
	typ string
}

// aggregateJob builds a job for a candidate that aggregates: workers group
// and aggregate their rows, the coordinator combines their groups.
func (pl *planner) aggregateJob(c *candidate) (*jobBuild, error) {
	a := &aggregation{pl: pl, c: c, aliases: map[string]int{}, aggKeys: map[string]*aggCall{}}
	for i, item := range list(c.sel, "select_list") {
		e := item.(node)
		if str(e["class"]) == "STAR" {
			return nil, unsupported("* in an aggregation")
		}
		a.items = append(a.items, e)
		if al := strings.ToLower(str(e["alias"])); al != "" {
			if _, dup := a.aliases[al]; dup {
				return nil, unsupported("two select items called %s", al)
			}
			a.aliases[al] = i
		}
	}
	if len(a.items) != len(c.columns) {
		return nil, unsupported("the select list has %d items for %d columns", len(a.items), len(c.columns))
	}
	if err := a.groupBy(); err != nil {
		return nil, err
	}
	if err := a.collect(); err != nil {
		return nil, err
	}
	if err := a.types(); err != nil {
		return nil, err
	}
	fragment, err := a.fragment()
	if err != nil {
		return nil, err
	}
	combine, err := a.combine()
	if err != nil {
		return nil, err
	}
	return &jobBuild{fragment: fragment, combine: combine}, nil
}

// groupBy resolves the GROUP BY clause: positions and aliases name select
// items, and GROUP BY ALL groups by every item that reads columns without
// aggregating them, as DuckDB does.
func (a *aggregation) groupBy() error {
	sel := a.c.sel
	exprs := list(sel, "group_expressions")
	if str(sel["aggregate_handling"]) == "FORCE_AGGREGATES" && len(exprs) == 0 {
		var set []any
		for _, item := range a.items {
			if a.pl.hasAggregate(item) || hasClass(item, "WINDOW") || !hasClass(item, "COLUMN_REF") {
				if a.pl.hasAggregate(item) && hasNonAggregateColumn(a.pl, item) {
					return unsupported("GROUP BY ALL with an item that mixes aggregates and columns")
				}
				continue
			}
			set = append(set, jsonInt(len(a.groups)))
			a.groups = append(a.groups, bare(item))
		}
		a.sets = []any{set}
	} else {
		for _, g := range exprs {
			e, err := a.groupRef(g.(node))
			if err != nil {
				return err
			}
			a.groups = append(a.groups, e)
		}
		a.sets = list(sel, "group_sets")
	}
	for _, g := range a.groups {
		if a.pl.hasAggregate(g) || hasClass(g, "WINDOW") {
			return unsupported("an aggregate in GROUP BY")
		}
		a.groupKeys = append(a.groupKeys, a.key(g))
	}
	return nil
}

// groupRef resolves one GROUP BY expression. DuckDB reads an integer as a
// position in the select list, and a bare name as a column of the FROM
// clause if there is one, and otherwise as a select item's alias.
func (a *aggregation) groupRef(g node) (node, error) {
	if k, ok := intConstant(g); ok {
		if k < 1 || int(k) > len(a.items) {
			return nil, unsupported("GROUP BY position %d", k)
		}
		return bare(a.items[k-1]), nil
	}
	if str(g["class"]) == "COLUMN_REF" {
		if parts := columnNames(g); len(parts) == 1 {
			name := strings.ToLower(parts[0])
			has, certain := a.c.scope.hasColumn(name)
			i, isAlias := a.aliases[name]
			switch {
			case has:
			case isAlias && !certain:
				return nil, unsupported("cannot tell whether GROUP BY %s names a column or an alias", name)
			case isAlias:
				return bare(a.items[i]), nil
			}
		}
	}
	return a.substituteAliases(clone(g).(node))
}

// substituteAliases replaces the select aliases an expression in WHERE or
// GROUP BY uses with what they stand for: DuckDB lets those clauses use an
// alias where no column of the FROM clause has that name.
func (a *aggregation) substituteAliases(e node) (node, error) {
	return substituteAliases(e, a.c.scope, a.items, a.aliases)
}

func substituteAliases(e node, sc *scope, items []node, aliases map[string]int) (node, error) {
	var err error
	out := rewrite(e, func(n node) (node, bool) {
		switch {
		case err != nil:
			return nil, false
		case str(n["class"]) == "SUBQUERY":
			// Names inside are resolved against the subquery first.
			return n, true
		case str(n["class"]) != "COLUMN_REF":
			return nil, false
		}
		parts := columnNames(n)
		if len(parts) != 1 {
			return n, true
		}
		name := strings.ToLower(parts[0])
		i, isAlias := aliases[name]
		if !isAlias {
			return n, true
		}
		has, certain := sc.hasColumn(name)
		switch {
		case has:
			return n, true
		case !certain:
			err = unsupported("cannot tell whether %s names a column or an alias", name)
			return n, true
		}
		if hasClass(items[i], "COLUMN_REF") && aliasCycle(items[i], name) {
			err = unsupported("alias %s refers to itself", name)
			return n, true
		}
		return bare(items[i]), true
	})
	if err != nil {
		return nil, err
	}
	return out.(node), nil
}

func aliasCycle(e node, name string) bool {
	found := false
	walk(e, func(n node) bool {
		if str(n["class"]) == "COLUMN_REF" {
			if p := columnNames(n); len(p) == 1 && strings.EqualFold(p[0], name) {
				found = true
			}
		}
		return !found
	})
	return found
}

// hasNonAggregateColumn reports whether e reads a column outside aggregate
// calls.
func hasNonAggregateColumn(pl *planner, e node) bool {
	found := false
	walk(e, func(n node) bool {
		switch {
		case found || pl.isAggregate(n):
			return false
		case str(n["class"]) == "COLUMN_REF":
			found = true
		}
		return !found
	})
	return found
}

// bare returns a copy of a select item without its alias.
func bare(e node) node {
	c := clone(e).(node)
	c["alias"] = ""
	return c
}

// key is an expression's canonical form, for telling whether two
// expressions are the same: positions and aliases are left out, names
// are lower-cased, and a column is qualified by the FROM item that owns
// it, so x and t.x match when t is the only item with a column x.
func (a *aggregation) key(e node) string {
	return exprKey(e, a.c.scope)
}

func exprKey(e node, sc *scope) string {
	c := clone(e).(node)
	walk(c, func(n node) bool {
		if isExpr(n) {
			n["alias"] = ""
		}
		if str(n["class"]) == "COLUMN_REF" {
			parts := columnNames(n)
			for i := range parts {
				parts[i] = strings.ToLower(parts[i])
			}
			if len(parts) == 1 {
				if it := sc.owner(parts[0]); it != nil {
					parts = []string{it.name, parts[0]}
				}
			}
			names := make([]any, len(parts))
			for i, p := range parts {
				names[i] = p
			}
			n["column_names"] = names
		}
		if str(n["class"]) == "FUNCTION" {
			n["function_name"] = strings.ToLower(str(n["function_name"]))
		}
		return true
	})
	return canonical(c)
}

// --- aggregate calls --------------------------------------------------------

// Ways the coordinator combines partial aggregates.
const (
	// combineSame applies the aggregate again: sum of sums, min of mins.
	combineSame = "same"
	// combineCount sums counts, which are 0 rather than NULL over nothing.
	combineCount = "count"
	// combineAvg divides the sum of sums by the sum of counts.
	combineAvg = "avg"
	// combineDistinct computes the aggregate over the distinct values
	// the workers grouped by.
	combineDistinct = "distinct"
)

// combinable maps the aggregates that combine by applying another
// aggregate to their partial results to that aggregate.
var combinable = map[string]string{
	"sum": "sum", "min": "min", "max": "max", "bool_and": "bool_and", "bool_or": "bool_or",
	"bit_and": "bit_and", "bit_or": "bit_or", "bit_xor": "bit_xor", "any_value": "any_value",
	"fsum": "fsum", "sumkahan": "fsum", "kahan_sum": "fsum", "product": "product",
	"count": "sum", "count_star": "sum", "count_if": "sum", "countif": "sum",
}

// collect finds the aggregate calls of the parts the coordinator evaluates
// and works out how to compute each in two steps.
func (a *aggregation) collect() error {
	sel := a.c.sel
	var err error
	visit := func(v any) {
		walk(v, func(n node) bool {
			if err != nil || !a.pl.isAggregate(n) {
				return err == nil
			}
			err = a.add(n)
			return false
		})
	}
	visit(sel["select_list"])
	visit(child(sel, "having"))
	visit(child(sel, "qualify"))
	for _, m := range list(sel, "modifiers") {
		if str(m.(node)["type"]) == "ORDER_MODIFIER" {
			visit(m)
		}
	}
	return err
}

// add plans one aggregate call.
func (a *aggregation) add(call node) error {
	key := a.key(call)
	if _, ok := a.aggKeys[key]; ok {
		return nil
	}
	name := strings.ToLower(str(call["function_name"]))
	args := list(call, "children")
	if call["export_state"] == true {
		return unsupported("EXPORT_STATE")
	}
	if len(list(child(call, "order_bys"), "orders")) > 0 {
		return unsupported("ORDER BY inside %s()", name)
	}
	if a.pl.hasAggregate(args) || hasClass(args, "WINDOW") {
		return unsupported("nested aggregates")
	}
	ac := &aggCall{call: call}

	switch {
	case call["distinct"] == true:
		if len(args) != 1 || child(call, "filter") != nil {
			return unsupported("%s(DISTINCT ...) with several arguments or a FILTER", name)
		}
		arg, err := a.substituteAliases(clone(args[0]).(node))
		if err != nil {
			return err
		}
		k := a.key(arg)
		if a.distinct == nil {
			a.distinct, a.distinctKey = arg, k
		} else if k != a.distinctKey {
			return unsupported("DISTINCT aggregates over different expressions")
		}
		ac.kind = combineDistinct

	case name == "avg" || name == "mean":
		if len(args) != 1 {
			return unsupported("%s with %d arguments", name, len(args))
		}
		ac.kind = combineAvg
		ac.partials = []int{a.partial(call, "sum"), a.partial(call, "count")}

	default:
		if _, ok := combinable[name]; !ok {
			return unsupported("%s() cannot be computed in parts", name)
		}
		if (name == "min" || name == "max") && len(args) != 1 {
			return unsupported("%s with %d arguments", name, len(args))
		}
		ac.kind = combineSame
		if name == "count" || name == "count_star" || name == "count_if" || name == "countif" {
			ac.kind = combineCount
		}
		ac.partials = []int{a.partial(call, "")}
	}
	a.aggKeys[key] = ac
	a.aggs = append(a.aggs, ac)
	return nil
}

// partial adds a partial aggregate: call itself, or call's arguments and
// FILTER with another function.
func (a *aggregation) partial(call node, function string) int {
	p := clone(call).(node)
	p["alias"] = ""
	if function != "" {
		p["function_name"] = function
	}
	a.partials = append(a.partials, p)
	return len(a.partials) - 1
}

// types works out the result type of each aggregate call, which its
// combined value is cast back to, by binding them in the session.
func (a *aggregation) types() error {
	if len(a.aggs) == 0 {
		return nil
	}
	probe, err := a.pl.p.template("SELECT 1")
	if err != nil {
		return err
	}
	var items []any
	for _, ac := range a.aggs {
		items = append(items, bare(ac.call))
	}
	probe["select_list"] = items
	probe["from_table"] = unmarked(child(a.c.sel, "from_table"))
	text, err := a.pl.p.print(probe)
	if err != nil {
		return unsupported("%v", err)
	}
	cols, err := a.pl.env.Describe(a.pl.ctx, text)
	if err != nil {
		return unsupported("describe aggregates: %v", err)
	}
	if len(cols) != len(a.aggs) {
		return unsupported("aggregate types")
	}
	for i, ac := range a.aggs {
		ac.typ = cols[i].Type
		if ac.kind == combineAvg && ac.typ != "DOUBLE" {
			return unsupported("avg() of %s", ac.typ)
		}
	}
	return nil
}

// --- the fragment -----------------------------------------------------------

// fragment builds the SELECT workers run: the candidate's FROM and WHERE,
// grouped by its group expressions (all grouping sets' columns at once)
// and the DISTINCT argument, with the partial aggregates.
//
//	SELECT <group> AS __g0, ..., <distinct arg> AS __dk, <partial> AS __a0, ...
//	FROM <the candidate's FROM> WHERE <its WHERE>
//	GROUP BY <group>, ..., <distinct arg>
func (a *aggregation) fragment() (node, error) {
	f, err := a.pl.p.template("SELECT 1")
	if err != nil {
		return nil, err
	}
	var items, groups []any
	var set []any
	for i, g := range a.groups {
		items = append(items, aliased(g, "__g"+strconv.Itoa(i)))
		groups = append(groups, clone(g))
		set = append(set, jsonInt(i))
	}
	if a.distinct != nil {
		items = append(items, aliased(a.distinct, "__dk"))
		groups = append(groups, clone(a.distinct))
		set = append(set, jsonInt(len(a.groups)))
	}
	for i, p := range a.partials {
		q, err := a.substituteAliasesInArgs(p)
		if err != nil {
			return nil, err
		}
		items = append(items, aliased(q, "__a"+strconv.Itoa(i)))
	}
	f["select_list"] = items
	f["from_table"] = clone(child(a.c.sel, "from_table"))
	if w := child(a.c.sel, "where_clause"); w != nil {
		if f["where_clause"], err = a.substituteAliases(clone(w).(node)); err != nil {
			return nil, err
		}
	}
	if len(groups) > 0 {
		f["group_expressions"] = groups
		f["group_sets"] = []any{set}
	}
	return f, nil
}

// substituteAliasesInArgs substitutes aliases in a partial aggregate's
// arguments and FILTER, which DuckDB also resolves against the select
// list when no column has the name.
func (a *aggregation) substituteAliasesInArgs(p node) (node, error) {
	c := clone(p).(node)
	args := list(c, "children")
	for i, arg := range args {
		e, err := a.substituteAliases(arg.(node))
		if err != nil {
			return nil, err
		}
		args[i] = e
	}
	if f := child(c, "filter"); f != nil {
		e, err := a.substituteAliases(f)
		if err != nil {
			return nil, err
		}
		c["filter"] = e
	}
	return c, nil
}

func aliased(e node, alias string) node {
	c := clone(e).(node)
	c["alias"] = alias
	return c
}

// --- the combine step -------------------------------------------------------

// combine builds the SELECT that replaces the candidate: the candidate
// itself, evaluated over the partial results, with group expressions read
// from the __g columns and aggregates combined from the __a columns. Each
// item is cast to the type and named as the candidate's, so the rest of
// the statement sees what it would have.
func (a *aggregation) combine() (node, error) {
	sel := a.c.sel
	out, err := a.pl.p.template("SELECT 1")
	if err != nil {
		return nil, err
	}
	var items []any
	for i, item := range a.items {
		e, err := a.rewrite(bare(item), "select")
		if err != nil {
			return nil, err
		}
		cast, err := a.pl.cast(e, a.c.columns[i].Type)
		if err != nil {
			return nil, err
		}
		cast["alias"] = a.c.columns[i].Name
		items = append(items, cast)
	}
	out["select_list"] = items
	if len(a.groups) > 0 {
		var groups []any
		for i := range a.groups {
			ref, err := a.pl.p.expr(quoteIdent("__g" + strconv.Itoa(i)))
			if err != nil {
				return nil, err
			}
			groups = append(groups, ref)
		}
		out["group_expressions"] = groups
		out["group_sets"] = clone(a.sets)
	}
	for _, part := range []string{"having", "qualify"} {
		if h := child(sel, part); h != nil {
			e, err := a.rewrite(clone(h).(node), part)
			if err != nil {
				return nil, err
			}
			out[part] = e
		}
	}
	mods := []any{}
	for _, m := range list(sel, "modifiers") {
		mod := clone(m).(node)
		if str(mod["type"]) == "ORDER_MODIFIER" {
			for _, o := range list(mod, "orders") {
				ord := o.(node)
				e := child(ord, "expression")
				if a.orderKeeps(e) {
					continue
				}
				r, err := a.rewrite(e, "order")
				if err != nil {
					return nil, err
				}
				ord["expression"] = r
			}
		}
		mods = append(mods, mod)
	}
	out["modifiers"] = mods
	return out, nil
}

// orderKeeps reports whether an ORDER BY item stays as written: a position
// or a select alias, which refer to the combine step's own select list,
// where DuckDB looks first.
func (a *aggregation) orderKeeps(e node) bool {
	if _, ok := intConstant(e); ok {
		return true
	}
	if str(e["class"]) == "STAR" {
		// ORDER BY ALL: every column of the select list.
		return true
	}
	if str(e["class"]) == "COLUMN_REF" {
		if parts := columnNames(e); len(parts) == 1 {
			_, isAlias := a.aliases[strings.ToLower(parts[0])]
			return isAlias
		}
	}
	return false
}

// rewrite turns an expression of the candidate's select list, HAVING,
// QUALIFY or ORDER BY into one over the partial results: group expressions
// become __g columns and aggregate calls their combined partials. Names
// left over must be select aliases, which the combine step defines too.
func (a *aggregation) rewrite(e node, part string) (node, error) {
	var err error
	out := rewrite(e, func(n node) (node, bool) {
		if err != nil || !isExpr(n) {
			return nil, false
		}
		k := a.key(n)
		if i := slices.Index(a.groupKeys, k); i >= 0 {
			r, perr := a.pl.p.expr(quoteIdent("__g" + strconv.Itoa(i)))
			if perr != nil {
				err = perr
				return nil, false
			}
			r["alias"] = n["alias"]
			return r, true
		}
		if ac, ok := a.aggKeys[k]; ok && a.pl.isAggregate(n) {
			r, cerr := a.combined(ac)
			if cerr != nil {
				err = cerr
				return nil, false
			}
			r["alias"] = n["alias"]
			return r, true
		}
		switch str(n["class"]) {
		case "COLUMN_REF":
			parts := columnNames(n)
			if len(parts) == 1 {
				name := strings.ToLower(parts[0])
				if _, isAlias := a.aliases[name]; isAlias {
					// In the select list DuckDB prefers a column of the same
					// name; elsewhere it prefers the alias.
					has, certain := a.c.scope.hasColumn(name)
					if part != "select" || (!has && certain) {
						return n, true
					}
				}
			}
			err = unsupported("column %s is neither grouped nor aggregated", strings.Join(parts, "."))
			return nil, false
		case "SUBQUERY", "LAMBDA", "PARAMETER":
			if str(n["class"]) == "PARAMETER" {
				// Parameters are bound when the combine step runs.
				return n, true
			}
			err = unsupported("%s outside WHERE", strings.ToLower(str(n["class"])))
			return nil, false
		case "STAR":
			err = unsupported("* in an aggregation")
			return nil, false
		}
		return nil, false
	})
	if err != nil {
		return nil, err
	}
	return out.(node), nil
}

// combined is the expression that computes an aggregate call from the
// partial results, cast to the call's own type.
func (a *aggregation) combined(ac *aggCall) (node, error) {
	col := func(i int) string { return quoteIdent("__a" + strconv.Itoa(ac.partials[i])) }
	name := strings.ToLower(str(ac.call["function_name"]))
	var text string
	switch ac.kind {
	case combineCount:
		text = "coalesce(sum(" + col(0) + "), 0)"
	case combineAvg:
		text = "CAST(sum(" + col(0) + ") AS DOUBLE) / CAST(sum(" + col(1) + ") AS DOUBLE)"
	case combineSame:
		text = combinable[name] + "(" + col(0) + ")"
	case combineDistinct:
		c := clone(ac.call).(node)
		ref, err := a.pl.p.expr(quoteIdent("__dk"))
		if err != nil {
			return nil, err
		}
		c["children"] = []any{ref}
		c["alias"] = ""
		return a.pl.cast(c, ac.typ)
	default:
		return nil, fmt.Errorf("unknown combine %q", ac.kind)
	}
	e, err := a.pl.p.expr(text)
	if err != nil {
		return nil, err
	}
	return a.pl.cast(e, ac.typ)
}

// cast wraps e in a cast to typ.
func (pl *planner) cast(e node, typ string) (node, error) {
	c, err := pl.p.expr("CAST(NULL AS " + typ + ")")
	if err != nil {
		return nil, unsupported("type %s: %v", typ, err)
	}
	if str(c["class"]) != "CAST" {
		return nil, unsupported("type %s", typ)
	}
	c["child"] = e
	return c, nil
}

// --- jobs over rows ---------------------------------------------------------

// rowsJob builds a job for a top-level SELECT that does not aggregate:
// workers filter and project their rows, and, for ORDER BY with LIMIT,
// keep only their first rows; the coordinator concatenates, sorts and
// limits. It is only worth it when that cuts the rows down: with a WHERE
// clause, DISTINCT, or ORDER BY with LIMIT.
func (pl *planner) rowsJob(c *candidate) (*jobBuild, error) {
	sel := c.sel
	if hasClass(sel["select_list"], "WINDOW") || child(sel, "qualify") != nil {
		return nil, unsupported("window functions over rows")
	}
	var order, limit, distinct node
	for _, m := range list(sel, "modifiers") {
		switch mod := m.(node); str(mod["type"]) {
		case "ORDER_MODIFIER":
			order = mod
		case "LIMIT_MODIFIER":
			limit = mod
		case "DISTINCT_MODIFIER":
			distinct = mod
		}
	}
	if limit != nil && order == nil {
		return nil, unsupported("LIMIT without ORDER BY, which one machine answers fastest")
	}
	if child(sel, "where_clause") == nil && distinct == nil && limit == nil {
		return nil, unsupported("reading every row of %s, which the coordinator would receive anyway", c.split.table)
	}

	items := list(sel, "select_list")
	n := len(c.columns)
	star := hasClass(items, "STAR")
	aliases := map[string]int{}
	for i, item := range items {
		if al := strings.ToLower(str(item.(node)["alias"])); al != "" && !star {
			aliases[al] = i
		}
	}
	f := clone(sel).(node)
	var err error
	if w := child(f, "where_clause"); w != nil {
		var plain []node
		for _, it := range items {
			plain = append(plain, it.(node))
		}
		if f["where_clause"], err = substituteAliases(w, c.scope, plain, aliases); err != nil {
			return nil, err
		}
	}

	// The coordinator sorts by result columns; an ORDER BY expression that
	// is not one is computed by the workers as an extra column.
	var columns []string
	for i := range n {
		columns = append(columns, "__c"+strconv.Itoa(i))
	}
	var orders []node
	if order != nil {
		for _, o := range list(order, "orders") {
			ord := clone(o).(node)
			e := child(ord, "expression")
			if str(e["class"]) == "STAR" {
				// ORDER BY ALL sorts by every result column, here as there.
				if len(list(order, "orders")) > 1 {
					return nil, unsupported("ORDER BY ALL with other items")
				}
				orders = append(orders, ord)
				continue
			}
			col := -1
			if k, ok := intConstant(e); ok && k >= 1 && int(k) <= n {
				col = int(k) - 1
			} else if str(e["class"]) == "COLUMN_REF" && len(columnNames(e)) == 1 {
				col = columnIndex(c.columns, columnNames(e)[0])
			}
			if col < 0 && !star {
				k := exprKey(e, c.scope)
				for i, it := range items {
					if exprKey(bare(it.(node)), c.scope) == k {
						col = i
						break
					}
				}
			}
			name := ""
			if col >= 0 {
				name = columns[col]
			} else {
				if distinct != nil {
					return nil, unsupported("ORDER BY an expression not selected with DISTINCT")
				}
				name = "__o" + strconv.Itoa(len(columns)-n)
				extra, err := substituteAliases(clone(e).(node), c.scope, nil, nil)
				if err != nil {
					return nil, err
				}
				f["select_list"] = append(list(f, "select_list"), aliased(extra, name))
				columns = append(columns, name)
			}
			ref, err := pl.p.expr(quoteIdent(name))
			if err != nil {
				return nil, err
			}
			ord["expression"] = ref
			orders = append(orders, ord)
		}
	}

	// Workers keep their first rows only: LIMIT n + OFFSET of each.
	mods := []any{}
	for _, m := range list(f, "modifiers") {
		mod := m.(node)
		if str(mod["type"]) == "LIMIT_MODIFIER" {
			total, ok := limitRows(mod)
			if !ok {
				return nil, unsupported("LIMIT or OFFSET that is not a number")
			}
			l, err := pl.p.expr(strconv.FormatInt(total, 10))
			if err != nil {
				return nil, err
			}
			mod["limit"], mod["offset"] = l, nil
		}
		mods = append(mods, mod)
	}
	f["modifiers"] = mods

	out, err := pl.p.template("SELECT 1")
	if err != nil {
		return nil, err
	}
	var sels []any
	for i := range n {
		ref, err := pl.p.expr(quoteIdent(columns[i]))
		if err != nil {
			return nil, err
		}
		cast, err := pl.cast(ref, c.columns[i].Type)
		if err != nil {
			return nil, err
		}
		cast["alias"] = c.columns[i].Name
		sels = append(sels, cast)
	}
	out["select_list"] = sels
	outMods := []any{}
	if distinct != nil {
		outMods = append(outMods, clone(distinct))
	}
	if order != nil {
		o := clone(order).(node)
		var list []any
		for _, ord := range orders {
			list = append(list, ord)
		}
		o["orders"] = list
		outMods = append(outMods, o)
	}
	if limit != nil {
		outMods = append(outMods, clone(limit))
	}
	out["modifiers"] = outMods
	return &jobBuild{fragment: f, combine: out, columns: columns}, nil
}

// columnIndex returns the index of the one result column called name, or
// -1.
func columnIndex(cols []Column, name string) int {
	found := -1
	for i, c := range cols {
		if strings.EqualFold(c.Name, name) {
			if found >= 0 {
				return -1
			}
			found = i
		}
	}
	return found
}

// limitRows returns LIMIT plus OFFSET, if both are integer constants.
func limitRows(mod node) (int64, bool) {
	var total int64
	if l := child(mod, "limit"); l != nil {
		n, ok := intConstant(l)
		if !ok || n < 0 {
			return 0, false
		}
		total = n
	}
	if o := child(mod, "offset"); o != nil {
		n, ok := intConstant(o)
		if !ok || n < 0 {
			return 0, false
		}
		total += n
	}
	return total, true
}

func jsonInt(i int) any { return json.Number(strconv.Itoa(i)) }
