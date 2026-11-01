package incremental

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// Queries over queries. What a query reads through a WITH clause, a subquery in
// FROM or UNION ALL is a sum of its tables' rows put through filters,
// projections and joins, as the query's own FROM is, so the delta rule holds
// through them: a change to one reference gives rows through the branches it
// is in, its other references as the rule reads them, and nothing through a
// UNION ALL's other branches. So a plan's sources are the tables at the leaves,
// whatever they are under; its keys and aggregates are written over what its
// own FROM names; and a refresh reads each leaf as its pin says (see pinned).
//
// What is not a sum stays out of it: a subquery that aggregates or has DISTINCT
// is refused, but when all the query does with it is to filter and compute from
// its rows, as a WITH clause that names a report does, which is the aggregate
// itself (see promote). UNION is SELECT DISTINCT over UNION ALL, at the top of a
// query.

// inlineCTEs replaces the names of n's WITH clauses, and those of what it reads,
// with subqueries: every reference computes it, as the query's result does not
// depend on it being kept. scope has the clauses of the queries around n.
func inlineCTEs(n node, scope map[string]node) error {
	if m, _ := n["cte_map"].(node); len(list(m["map"])) > 0 {
		inner := make(map[string]node, len(scope))
		for k, v := range scope {
			inner[k] = v
		}
		for _, e := range list(m["map"]) {
			entry, _ := e.(node)
			def, _ := entry["value"].(node)
			q, _ := def["query"].(node)
			body, _ := q["node"].(node)
			if str(body["type"]) == "RECURSIVE_CTE_NODE" {
				return refuse("it has a recursive WITH clause")
			}
			if body == nil {
				return refuse("it has a WITH clause it cannot read")
			}
			if len(list(def["key_targets"])) > 0 {
				return refuse("it has a WITH clause with USING KEY")
			}
			if err := inlineCTEs(body, inner); err != nil {
				return err
			}
			inner[strings.ToLower(str(entry["key"]))] = def
		}
		scope = inner
		n["cte_map"] = node{"map": []any{}}
	}
	switch str(n["type"]) {
	case "SELECT_NODE":
		if from, _ := n["from_table"].(node); from != nil {
			return inlineFrom(from, scope)
		}
	case "SET_OPERATION_NODE":
		for _, k := range []string{"left", "right"} {
			if c, _ := n[k].(node); c != nil {
				if err := inlineCTEs(c, scope); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func inlineFrom(from node, scope map[string]node) error {
	switch str(from["type"]) {
	case "BASE_TABLE":
		if str(from["catalog_name"]) != "" || str(from["schema_name"]) != "" {
			return nil
		}
		def := scope[strings.ToLower(str(from["table_name"]))]
		if def == nil {
			return nil
		}
		if from["at_clause"] != nil {
			return refuse("it reads a WITH clause at a snapshot (AT)")
		}
		alias := str(from["alias"])
		if alias == "" {
			alias = str(from["table_name"])
		}
		names := list(from["column_name_alias"])
		if len(names) == 0 {
			names = list(def["aliases"])
		}
		sample := from["sample"]
		body, _ := def["query"].(node)
		clear(from)
		from["type"], from["alias"], from["sample"] = "SUBQUERY", alias, sample
		from["subquery"] = cloneAny(body)
		if names == nil {
			names = []any{}
		}
		from["column_name_alias"] = names
	case "JOIN":
		for _, k := range []string{"left", "right"} {
			if c, _ := from[k].(node); c != nil {
				if err := inlineFrom(c, scope); err != nil {
					return err
				}
			}
		}
	case "SUBQUERY":
		q, _ := from["subquery"].(node)
		if body, _ := q["node"].(node); body != nil {
			return inlineCTEs(body, scope)
		}
	}
	return nil
}

// withoutOrder takes modifiers, which may only order: the order of a table's
// rows means nothing, and what reads a subquery's cannot tell.
func withoutOrder(n node) error {
	var kept []any
	for _, m := range list(n["modifiers"]) {
		mod, _ := m.(node)
		switch str(mod["type"]) {
		case "ORDER_MODIFIER":
		case "DISTINCT_MODIFIER":
			kept = append(kept, m)
		default:
			return refuse("it has LIMIT or OFFSET")
		}
	}
	if kept == nil {
		kept = []any{}
	}
	n["modifiers"] = kept
	return nil
}

// topSetOperation returns the query that n, a set operation at the top of a
// query, comes to: SELECT * over its UNION ALL, or for UNION, SELECT DISTINCT.
func (p *Plan) topSetOperation(ctx context.Context, n node) (node, error) {
	if err := withoutOrder(n); err != nil {
		return nil, err
	}
	distinct := str(n["setop_type"]) == "UNION" && n["setop_all"] != true
	if distinct {
		// The UNIONs under it take out nothing it does not.
		var all func(c node)
		all = func(c node) {
			if str(c["type"]) == "SET_OPERATION_NODE" && str(c["setop_type"]) == "UNION" {
				c["setop_all"] = true
				for _, k := range []string{"left", "right"} {
					if s, _ := c[k].(node); s != nil {
						all(s)
					}
				}
			}
		}
		all(n)
	}
	text, err := p.print(ctx, n)
	if err != nil {
		return nil, err
	}
	cols, err := p.env.Describe(ctx, text)
	if err != nil {
		return nil, refuse("its branches could not be read on their own: %v", err)
	}
	names := make([]any, len(cols))
	var sel []string
	for i := range cols {
		name := "__c" + strconv.Itoa(i)
		names[i] = name
		sel = append(sel, name)
	}
	keyword := "SELECT "
	if distinct {
		keyword = "SELECT DISTINCT "
	}
	root, err := p.parse(ctx, keyword+strings.Join(sel, ", ")+" FROM (SELECT 1) AS __igloo_u")
	if err != nil {
		return nil, err
	}
	from := root["from_table"].(node)
	from["subquery"].(node)["node"] = n
	from["column_name_alias"] = names
	return root, nil
}

// linear checks that n, what a subquery in FROM runs, is a sum of its tables'
// rows (see above), and collects them.
func (p *Plan) linear(ctx context.Context, n node, nullable bool, outer string) error {
	switch t := str(n["type"]); t {
	case "SELECT_NODE":
		if err := withoutOrder(n); err != nil {
			return err
		}
		if len(list(n["modifiers"])) > 0 {
			return refuse("a subquery of it has DISTINCT")
		}
		for _, k := range []string{"qualify", "sample"} {
			if n[k] != nil {
				return refuse("a subquery of it has %s", strings.ToUpper(k))
			}
		}
		if len(list(n["group_expressions"])) > 0 || n["having"] != nil || str(n["aggregate_handling"]) == "FORCE_AGGREGATES" {
			return refuse("a subquery of it aggregates, and it reads more than that subquery's rows")
		}
		for _, s := range list(n["select_list"]) {
			if p.hasAggregate(s) {
				return refuse("a subquery of it aggregates, and it reads more than that subquery's rows")
			}
			if err := p.checkExpr(s, false); err != nil {
				return err
			}
		}
		from, _ := n["from_table"].(node)
		if from == nil {
			return refuse("a subquery of it reads no table")
		}
		if w := n["where_clause"]; w != nil {
			if err := p.checkExpr(w, false); err != nil {
				return err
			}
			if p.hasAggregate(w) {
				return refuse("a subquery's WHERE clause has an aggregate")
			}
		}
		if str(from["type"]) == "EMPTY" {
			// A row or two of constants: they never change.
			return nil
		}
		if err := p.sources(ctx, from, nullable, outer); err != nil {
			return err
		}
		return p.checkExpr(from, false)
	case "SET_OPERATION_NODE":
		if err := withoutOrder(n); err != nil {
			return err
		}
		switch {
		case str(n["setop_type"]) != "UNION":
			return refuse("it has %s", strings.ReplaceAll(str(n["setop_type"]), "_", " "))
		case n["setop_all"] != true:
			return refuse("a subquery of it has UNION rather than UNION ALL")
		}
		for _, k := range []string{"left", "right"} {
			c, _ := n[k].(node)
			if c == nil {
				return refuse("a UNION of it has a branch it cannot read")
			}
			if err := p.linear(ctx, c, nullable, outer); err != nil {
				return err
			}
		}
		return nil
	default:
		return refuse("it reads a %s", strings.ToLower(strings.ReplaceAll(strings.TrimSuffix(t, "_NODE"), "_", " ")))
	}
}

// nameUnion has from, a subquery that runs the set operation body, name its
// columns itself: they are its first branch's, and a refresh may read another
// branch alone (see pinned).
func (p *Plan) nameUnion(ctx context.Context, from, body node) error {
	text, err := p.print(ctx, body)
	if err != nil {
		return err
	}
	cols, err := p.env.Describe(ctx, text)
	if err != nil {
		return refuse("a subquery of it could not be read on its own (it may name what its FROM reads): %v", err)
	}
	names := list(from["column_name_alias"])
	for _, c := range cols[min(len(names), len(cols)):] {
		names = append(names, c.Name)
	}
	from["column_name_alias"] = names
	return nil
}

// leaves counts the tables n reads.
func leaves(v any) int {
	n, _ := v.(node)
	switch str(n["type"]) {
	case "BASE_TABLE":
		return 1
	case "JOIN", "SET_OPERATION_NODE":
		return leaves(n["left"]) + leaves(n["right"])
	case "SUBQUERY":
		return leaves(n["subquery"].(node)["node"])
	case "SELECT_NODE":
		return leaves(n["from_table"])
	}
	return 0
}

// scope is a name a query's FROM gives what it reads, and the columns under it.
type scope struct {
	alias string
	cols  []string
}

// scopes lists what from names.
func (p *Plan) scopes(ctx context.Context, from node) ([]scope, error) {
	switch t := str(from["type"]); t {
	case "BASE_TABLE":
		// Refs are in the order sources found them.
		var parts []string
		for _, k := range []string{"catalog_name", "schema_name", "table_name"} {
			if v := str(from[k]); v != "" {
				parts = append(parts, v)
			}
		}
		schema, name, ok, err := p.env.Table(ctx, parts)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, refuse("%s is not a table of the catalog (a view, or a table of another database)", strings.Join(parts, "."))
		}
		cols, err := p.env.Columns(ctx, schema, name)
		if err != nil {
			return nil, err
		}
		alias := str(from["alias"])
		if alias == "" {
			alias = str(from["table_name"])
		}
		return []scope{{alias, cols}}, nil
	case "JOIN":
		l, err := p.scopes(ctx, from["left"].(node))
		if err != nil {
			return nil, err
		}
		r, err := p.scopes(ctx, from["right"].(node))
		if err != nil {
			return nil, err
		}
		return append(l, r...), nil
	case "SUBQUERY":
		text, err := p.print(ctx, from["subquery"].(node)["node"].(node))
		if err != nil {
			return nil, err
		}
		described, err := p.env.Describe(ctx, text)
		if err != nil {
			return nil, refuse("a subquery of it could not be read on its own (it may name what its FROM reads): %v", err)
		}
		cols := make([]string, len(described))
		for i, c := range described {
			cols[i] = c.Name
		}
		for i, a := range list(from["column_name_alias"]) {
			if i < len(cols) {
				cols[i] = str(a)
			}
		}
		return []scope{{str(from["alias"]), cols}}, nil
	}
	return nil, refuse("it reads a %s", strings.ToLower(strings.ReplaceAll(str(from["type"]), "_", " ")))
}

// promote returns root as the aggregate it reads, when all it does is filter and
// compute from the rows of one subquery that aggregates: SELECT a * 2 FROM (SELECT
// g, sum(x) AS a FROM f GROUP BY g) AS s WHERE a > 1 is SELECT sum(x) * 2 FROM f
// GROUP BY g HAVING sum(x) > 1. It returns root itself otherwise.
func (p *Plan) promote(ctx context.Context, root node) (node, error) {
	from, _ := root["from_table"].(node)
	if str(from["type"]) != "SUBQUERY" {
		return root, nil
	}
	inner, _ := from["subquery"].(node)["node"].(node)
	if str(inner["type"]) != "SELECT_NODE" {
		return root, nil
	}
	if err := withoutOrder(root); err != nil {
		return nil, err
	}
	if err := withoutOrder(inner); err != nil {
		return nil, err
	}
	inner, err := p.promote(ctx, inner)
	if err != nil {
		return nil, err
	}
	from["subquery"].(node)["node"] = inner
	if !p.aggregates(inner) {
		return root, nil
	}
	if p.aggregates(root) {
		return nil, refuse("it aggregates what a subquery aggregated")
	}
	for _, k := range []string{"qualify", "sample"} {
		if root[k] != nil || inner[k] != nil {
			return nil, refuse("it has %s", strings.ToUpper(k))
		}
	}
	if inner["from_table"] == nil || str(inner["from_table"].(node)["type"]) == "EMPTY" {
		return nil, refuse("it reads no table")
	}
	if len(list(inner["group_sets"])) > 1 {
		return nil, refuse("it has GROUPING SETS, ROLLUP or CUBE")
	}
	selects := list(inner["select_list"])
	for _, s := range selects {
		if str(s.(node)["class"]) == "STAR" {
			return nil, refuse("a subquery of it aggregates and its select list has *")
		}
	}
	// The aggregate's own names and positions first, as the select list goes.
	inScope, err := p.scopes(ctx, inner["from_table"].(node))
	if err != nil {
		return nil, err
	}
	hasColumn := func(name string) bool {
		for _, s := range inScope {
			for _, c := range s.cols {
				if strings.EqualFold(c, name) {
					return true
				}
			}
		}
		return false
	}
	resolve := func(e any, position bool) (any, error) {
		n, _ := e.(node)
		if position && str(n["class"]) == "CONSTANT" {
			v, _ := n["value"].(node)
			i, err := strconv.Atoi(fmt.Sprint(v["value"]))
			if err != nil || i < 1 || i > len(selects) {
				return nil, refuse("it groups by a constant")
			}
			return cloneAny(selects[i-1]), nil
		}
		return substitute(e, func(names []any) (any, bool, error) {
			if len(names) != 1 {
				return nil, false, nil
			}
			for _, s := range selects {
				sn := s.(node)
				if strings.EqualFold(str(sn["alias"]), str(names[0])) {
					if hasColumn(str(names[0])) {
						if str(sn["class"]) == "COLUMN_REF" && strings.EqualFold(lastName(sn), str(names[0])) {
							return nil, false, nil
						}
						return nil, false, refuse("%s names both a column and an expression of the select list", str(names[0]))
					}
					return cloneAny(sn), true, nil
				}
			}
			return nil, false, nil
		})
	}
	var groups []any
	switch {
	case hasDistinct(inner):
		groups = append(groups, selects...)
		inner["modifiers"] = []any{}
	case str(inner["aggregate_handling"]) == "FORCE_AGGREGATES":
		for _, s := range selects {
			if !p.hasAggregate(s) {
				groups = append(groups, s)
			}
		}
	default:
		for _, g := range list(inner["group_expressions"]) {
			k, err := resolve(g, true)
			if err != nil {
				return nil, err
			}
			groups = append(groups, k)
		}
	}
	having := inner["having"]
	if having != nil {
		if having, err = resolve(having, false); err != nil {
			return nil, err
		}
	}
	// The subquery's columns, as what reads them names them.
	names := list(from["column_name_alias"])
	alias := str(from["alias"])
	output := func(ref []any) (any, bool, error) {
		name := str(ref[len(ref)-1])
		switch {
		case len(ref) > 2:
			return nil, false, refuse("it names a column of a subquery with more than its name")
		case len(ref) == 2 && !strings.EqualFold(str(ref[0]), alias):
			return nil, false, refuse("it names %s, which is not what it reads", joinNames(ref))
		}
		for i, s := range selects {
			sn := s.(node)
			out := str(sn["alias"])
			if out == "" && str(sn["class"]) == "COLUMN_REF" {
				out = lastName(sn)
			}
			if i < len(names) {
				out = str(names[i])
			}
			if strings.EqualFold(out, name) {
				return cloneAny(sn), true, nil
			}
		}
		return nil, false, refuse("%s is not a column it can tell of a subquery that aggregates", name)
	}
	var sel []any
	for _, s := range list(root["select_list"]) {
		sn := s.(node)
		if str(sn["class"]) == "STAR" {
			if len(list(sn["exclude_list"])) > 0 || len(list(sn["replace_list"])) > 0 || len(list(sn["rename_list"])) > 0 {
				return nil, refuse("its select list has * with EXCLUDE or REPLACE")
			}
			for _, c := range selects {
				sel = append(sel, cloneAny(c))
			}
			continue
		}
		e, err := substitute(s, output)
		if err != nil {
			return nil, err
		}
		if a := str(sn["alias"]); a != "" {
			e.(node)["alias"] = a
		}
		sel = append(sel, e)
	}
	if w := root["where_clause"]; w != nil {
		e, err := substitute(w, output)
		if err != nil {
			return nil, err
		}
		if having == nil {
			having = e
		} else if having, err = p.conjunction(ctx, having, e); err != nil {
			return nil, err
		}
	}
	out := clone(inner)
	var set []any
	cleaned := make([]any, len(groups))
	for i, g := range groups {
		c := cloneAny(g).(node)
		c["alias"] = ""
		cleaned[i] = c
		set = append(set, i)
	}
	out["select_list"], out["having"], out["where_clause"] = sel, having, inner["where_clause"]
	out["group_expressions"] = cleaned
	if len(cleaned) > 0 {
		out["group_sets"] = []any{set}
	} else {
		out["group_sets"] = []any{}
	}
	out["aggregate_handling"] = "STANDARD_HANDLING"
	return out, nil
}

// aggregates reports whether n, a SELECT, aggregates or has DISTINCT.
func (p *Plan) aggregates(n node) bool {
	if len(list(n["group_expressions"])) > 0 || n["having"] != nil || str(n["aggregate_handling"]) == "FORCE_AGGREGATES" || hasDistinct(n) {
		return true
	}
	for _, s := range list(n["select_list"]) {
		if p.hasAggregate(s) {
			return true
		}
	}
	return false
}

func hasDistinct(n node) bool {
	for _, m := range list(n["modifiers"]) {
		if mod, _ := m.(node); str(mod["type"]) == "DISTINCT_MODIFIER" {
			return true
		}
	}
	return false
}

func lastName(n node) string {
	names := list(n["column_names"])
	if len(names) == 0 {
		return ""
	}
	return str(names[len(names)-1])
}

// substitute returns a copy of an expression with its column names replaced as
// by says: its value, or false to leave the name. What it cannot see into, a
// subquery's or a lambda's scope, it refuses.
func substitute(v any, by func(names []any) (any, bool, error)) (any, error) {
	switch v := v.(type) {
	case node:
		switch str(v["class"]) {
		case "COLUMN_REF":
			e, ok, err := by(list(v["column_names"]))
			if err != nil {
				return nil, err
			}
			if ok {
				out := e.(node)
				if a := str(v["alias"]); a != "" {
					out["alias"] = a
				}
				return out, nil
			}
			return clone(v), nil
		case "LAMBDA":
			return nil, refuse("it has a lambda over what a subquery aggregated")
		case "SUBQUERY":
			return nil, refuse("it has a subquery")
		}
		out := node{}
		for k, c := range v {
			r, err := substitute(c, by)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, c := range v {
			r, err := substitute(c, by)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	}
	return v, nil
}

// conjunction returns a AND b.
func (p *Plan) conjunction(ctx context.Context, a, b any) (any, error) {
	e, err := p.expr(ctx, "true AND true")
	if err != nil {
		return nil, err
	}
	e["children"] = []any{a, b}
	return e, nil
}
