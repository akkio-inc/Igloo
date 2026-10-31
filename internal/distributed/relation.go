package distributed

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// Relations. A candidate's FROM clause may read, besides catalog tables, a
// common table expression or a subquery that workers compute themselves:
// a relation. Its rows go to an exchange, keyed by the column the
// candidate joins or groups on, and the candidate's stage reads them by
// partition, as it reads a table partitioned for a join. A relation is a
// SELECT that a stage can compute whole (filters and joins of tables and
// other relations, with grouping completed by group), or a UNION ALL of
// such SELECTs, whose branches write to one exchange. A common table
// expression is computed once, whatever reads it.

// cteDef is a common table expression in scope.
type cteDef struct {
	// name is its name, as written; key is its name in lower case.
	name, key string
	// entry is its entry in the WITH clause: aliases, query, materialized.
	entry node
	// scope is what its query sees: the expressions before it, and those
	// of the statement around it.
	scope cteScope
	// owner is the query node whose WITH clause defines it, at index; nil
	// for those the planner makes up.
	owner node
	index int
	// columns are its result columns, once described.
	columns []Column
	// outs are the exchanges its rows were written to, by keys and casts;
	// first is the first of them.
	outs  map[string]*Exchange
	first *Exchange
}

// query returns the CTE's query node.
func (d *cteDef) query() node { return child(child(d.entry, "query"), "node") }

// cteScope is the common table expressions a query sees, innermost last.
type cteScope []*cteDef

// with returns the scope inside query node n: s and n's own common table
// expressions.
func (s cteScope) with(n node) cteScope {
	entries := list(child(n, "cte_map"), "map")
	if len(entries) == 0 {
		return s
	}
	out := make(cteScope, len(s), len(s)+len(entries))
	copy(out, s)
	for i, e := range entries {
		e := e.(node)
		name := str(e["key"])
		d := &cteDef{name: name, key: strings.ToLower(name), entry: child(e, "value"), scope: out[:len(out):len(out)], owner: n, index: i}
		out = append(out, d)
	}
	return out
}

// uses counts the references that read d: those to its name in the body of
// the query that defines it and in the expressions after it, except where
// an inner WITH clause defines the name again. Another query that defines
// a common table expression of the same name, as a statement holding a
// report twice does, does not count; nor does one that reads d through a
// common table expression read as its query in its reader's place
// (inline). It returns -1 for a common table expression the planner made
// up.
func (pl *planner) uses(d *cteDef) int {
	if d.owner == nil {
		return -1
	}
	body := maps.Clone(d.owner)
	body["cte_map"] = node{"map": []any{}}
	n := pl.visible(body, d.name)
	for _, e := range list(child(d.owner, "cte_map"), "map")[d.index+1:] {
		n += pl.visible(e, d.name)
	}
	return n
}

// visible counts the references to a one-part name in v that mean what
// the name means around v: under a query node whose WITH clause defines
// the name, only those in the expressions up to that definition.
func (pl *planner) visible(v any, name string) int {
	n := 0
	walk(v, func(m node) bool {
		if pl.inlined[identity(m)] {
			return false
		}
		if str(m["type"]) == "BASE_TABLE" && str(m["schema_name"]) == "" && str(m["catalog_name"]) == "" &&
			strings.EqualFold(str(m["table_name"]), name) {
			n++
		}
		entries := list(child(m, "cte_map"), "map")
		for i, e := range entries {
			if strings.EqualFold(str(e.(node)["key"]), name) {
				for _, before := range entries[:i+1] {
					n += pl.visible(before, name)
				}
				return false
			}
		}
		return true
	})
	return n
}

// find returns the common table expression a one-part name means, or nil.
func (s cteScope) find(name string) *cteDef {
	key := strings.ToLower(name)
	for i := len(s) - 1; i >= 0; i-- {
		if s[i].key == key {
			return s[i]
		}
	}
	return nil
}

// names returns the names in scope, lower case.
func (s cteScope) names() []string {
	out := make([]string, len(s))
	for i, d := range s {
		out[i] = d.key
	}
	return out
}

// reads are what a candidate's FROM and WHERE read: catalog tables, and
// relations workers compute.
type reads struct {
	uses []*tableUse
	rels []*relUse
	// subs are the subquery expressions of its filters.
	subs []node
}

// relUse is a relation a candidate reads.
type relUse struct {
	// ref is the reference in FROM: a common table expression's name (a
	// BASE_TABLE node) or a subquery.
	ref node
	// q is the relation's query, as scope sees it; def is the common table
	// expression, if it is one.
	q     node
	scope cteScope
	def   *cteDef
	// columns are the relation's result columns, as its exchange holds
	// them.
	columns []Column
	// bytes estimates its size, for deciding whether every task may read it.
	bytes int64
	// splittable, scope and item are as for tables.
	splittable bool
	sc         *scope
	item       *fromItem
	// query says ref is a subquery expression's query, which every task
	// reads in full.
	query bool
	// semi is as for tables.
	semi bool
	// key identifies the relation across plans of the statement (see
	// relationKey); known says its size was measured, and rows are the
	// rows it was measured at.
	key   string
	known bool
	rows  int64
	// out is the exchange its rows are written to, once planned.
	out *Exchange
}

// identity is a node's identity, for sets of nodes.
func identity(n node) uintptr { return reflect.ValueOf(n).Pointer() }

// semiJoins rewrites the conjuncts x IN (SELECT y ...) of a SELECT's WHERE
// clause whose subquery stands on its own (is not correlated) and is too
// large for every worker to read, as an audience's members are, as SEMI
// JOINs to the subquery: FROM ... SEMI JOIN (SELECT y ...) AS
// __igloo_inN(__igloo_k) ON x = __igloo_inN.__igloo_k, which keep the same
// rows. Workers then compute the subquery as a relation keyed by y, and
// read the rest partitioned by x. A candidate that does not qualify puts
// the statement back as it was.
func (pl *planner) semiJoins(sel node, ctes cteScope) error {
	var keep, ins []node
	for _, cond := range conjuncts(child(sel, "where_clause")) {
		if str(cond["class"]) == "SUBQUERY" && str(cond["subquery_type"]) == "ANY" &&
			str(cond["comparison_type"]) == "COMPARE_EQUAL" && str(child(cond, "child")["class"]) == "COLUMN_REF" {
			q := subqueryNode(cond)
			cols, err := pl.relationColumns(q, ctes, nil)
			if err == nil && len(cols) == 1 {
				if bytes, err := pl.estimate(q, ctes); err == nil && bytes > pl.opts.maxBroadcast {
					ins = append(ins, cond)
					continue
				}
			}
		}
		keep = append(keep, cond)
	}
	if len(ins) == 0 {
		return nil
	}
	from, where := sel["from_table"], sel["where_clause"]
	pl.undo = append(pl.undo, func() { sel["from_table"], sel["where_clause"] = from, where })
	switch len(keep) {
	case 0:
		sel["where_clause"] = nil
	case 1:
		sel["where_clause"] = keep[0]
	default:
		w := clone(child(sel, "where_clause")).(node)
		children := make([]any, len(keep))
		for i, k := range keep {
			children[i] = k
		}
		w["children"] = children
		sel["where_clause"] = w
	}
	for _, in := range ins {
		join, err := pl.p.fromRef("SELECT 1 FROM a SEMI JOIN (SELECT 1) AS b(k) ON a.x = b.k")
		if err != nil {
			return err
		}
		alias := "__igloo_in" + strconv.Itoa(pl.ins)
		pl.ins++
		right := child(join, "right")
		child(right, "subquery")["node"] = clone(subqueryNode(in))
		right["alias"], right["column_name_alias"] = alias, []any{"__igloo_k"}
		cond := child(join, "condition")
		cond["left"] = clone(child(in, "child"))
		child(cond, "right")["column_names"] = []any{alias, "__igloo_k"}
		join["left"] = sel["from_table"]
		sel["from_table"] = join
		if pl.forced == nil {
			pl.forced = map[uintptr]bool{}
		}
		pl.forced[identity(right)] = true
	}
	return nil
}

// subqueryRelations makes the subquery expressions of a candidate's
// filters that read a table its FROM reads too, or a table too large for
// every worker to read, relations every task reads whole, when they bind
// on their own (are not correlated) and are small, as
// ver = (SELECT max(ver) FROM t WHERE ...) is.
func (pl *planner) subqueryRelations(c *candidate) {
	for i, sub := range c.subs {
		conflict := false
		for _, u := range c.uses {
			if u.sub != i+1 {
				continue
			}
			if u.table.Bytes > pl.opts.maxBroadcast ||
				slices.ContainsFunc(c.uses, func(v *tableUse) bool { return v.sub == 0 && v.table.key() == u.table.key() }) {
				conflict = true
			}
		}
		if !conflict {
			continue
		}
		q := subqueryNode(sub)
		cols, err := pl.relationColumns(q, c.ctes, nil)
		if err != nil {
			continue
		}
		bytes, err := pl.estimate(q, c.ctes)
		if err != nil || bytes > pl.opts.maxBroadcast {
			continue
		}
		c.uses = slices.DeleteFunc(c.uses, func(u *tableUse) bool { return u.sub == i+1 })
		c.rels = append(c.rels, &relUse{ref: q, q: q, scope: c.ctes, columns: cols, bytes: bytes, query: true,
			item: &fromItem{name: "subquery"}})
	}
}

// --- describing and sizing relations ----------------------------------------

// closure returns the common table expressions of scope that q reads,
// directly or through each other, in the order of their definitions. q's
// own common table expressions are left out: q carries them.
func closure(q node, scope cteScope) ([]*cteDef, error) {
	own := map[string]bool{}
	for _, name := range cteNames(q) {
		own[name] = true
	}
	seen := map[*cteDef]bool{}
	var visit func(n node, sc cteScope, top bool)
	visit = func(n node, sc cteScope, top bool) {
		walk(n, func(m node) bool {
			if str(m["type"]) != "BASE_TABLE" || str(m["schema_name"]) != "" || str(m["catalog_name"]) != "" {
				return true
			}
			name := strings.ToLower(str(m["table_name"]))
			if top && own[name] {
				return true
			}
			if d := sc.find(name); d != nil && !seen[d] {
				seen[d] = true
				visit(d.query(), d.scope, false)
			}
			return true
		})
	}
	visit(q, scope, true)
	var out []*cteDef
	keys := map[string]bool{}
	for _, d := range scope {
		if !seen[d] {
			continue
		}
		if keys[d.key] || own[d.key] {
			return nil, unsupported("two common table expressions called %s", d.name)
		}
		keys[d.key] = true
		out = append(out, d)
	}
	return out, nil
}

// standalone returns a copy of query node q that binds on its own: with
// the common table expressions of scope it reads defined before its own.
func standalone(q node, scope cteScope) (node, error) {
	defs, err := closure(q, scope)
	if err != nil {
		return nil, err
	}
	out := unmarked(q).(node)
	if len(defs) == 0 {
		return out, nil
	}
	var entries []any
	for _, d := range defs {
		entries = append(entries, node{"key": d.name, "value": unmarked(d.entry)})
	}
	entries = append(entries, list(child(out, "cte_map"), "map")...)
	out["cte_map"] = node{"map": entries}
	return out, nil
}

// describe binds query node q, as scope sees it, in the session and
// returns its result columns.
func (pl *planner) describe(q node, scope cteScope) ([]Column, error) {
	s, err := standalone(q, scope)
	if err != nil {
		return nil, err
	}
	text, err := pl.p.print(s)
	if err != nil {
		return nil, unsupported("%v", err)
	}
	cols, err := pl.env.Describe(pl.ctx, text)
	if err != nil {
		return nil, unsupported("describe: %v", err)
	}
	return cols, nil
}

// relationColumns describes a relation's result columns, renamed by
// aliases, and checks that their names are distinct, as an exchange's must
// be.
func (pl *planner) relationColumns(q node, scope cteScope, aliases []any) ([]Column, error) {
	cols, err := pl.describe(q, scope)
	if err != nil {
		return nil, err
	}
	for i, a := range aliases {
		if i < len(cols) {
			cols[i].Name = str(a)
		}
	}
	seen := map[string]bool{}
	for _, c := range cols {
		k := strings.ToLower(c.Name)
		switch {
		case seen[k]:
			return nil, unsupported("a relation with two columns called %s", c.Name)
		case k == "__b":
			// An exchange's partition column.
			return nil, unsupported("a relation with a column called %s", c.Name)
		}
		seen[k] = true
	}
	return cols, nil
}

// estimate guesses the size of the rows of query node q: the compressed
// size of the tables it reads, or nothing for an aggregation without
// GROUP BY, which returns one row.
func (pl *planner) estimate(q node, scope cteScope) (int64, error) {
	switch str(q["type"]) {
	case "SET_OPERATION_NODE":
		sc := scope.with(q)
		var total int64
		for _, side := range []string{"left", "right"} {
			n, err := pl.estimate(child(q, side), sc)
			if err != nil {
				return 0, err
			}
			total += n
		}
		return total, nil
	case "SELECT_NODE":
		if len(list(q, "group_expressions")) == 0 && str(q["aggregate_handling"]) != "FORCE_AGGREGATES" && pl.hasAggregate(q["select_list"]) {
			return 0, nil
		}
		sc := scope.with(q)
		var total int64
		var visit func(ref node) error
		visit = func(ref node) error {
			switch str(ref["type"]) {
			case "JOIN":
				for _, side := range []string{"left", "right"} {
					if err := visit(child(ref, side)); err != nil {
						return err
					}
				}
			case "SUBQUERY":
				n, err := pl.estimate(subqueryNode(ref), sc)
				if err != nil {
					return err
				}
				total += n
			case "BASE_TABLE":
				parts := tableParts(ref)
				if len(parts) == 1 {
					if d := sc.find(parts[0]); d != nil {
						n, err := pl.estimate(d.query(), d.scope)
						if err != nil {
							return err
						}
						total += n
						return nil
					}
				}
				t, err := pl.table(parts)
				if IsUnsupported(err) {
					// A view: the size of what its query reads.
					n, ok, verr := pl.viewBytes(parts, 0)
					if verr != nil {
						return verr
					}
					if ok {
						total += n
						return nil
					}
				}
				if err != nil {
					return err
				}
				total += t.Bytes
			}
			return nil
		}
		if err := visit(child(q, "from_table")); err != nil {
			return 0, err
		}
		return total, nil
	}
	return 0, unsupported("%s", strings.ToLower(str(q["type"])))
}

// viewBytes estimates the size of what a view reads: the catalog tables
// its query names, as the view resolves them, and other views' alike.
func (pl *planner) viewBytes(parts []string, depth int) (int64, bool, error) {
	if depth > maxViewDepth {
		return 0, false, unsupported("views nest too deep")
	}
	def, ok, err := pl.env.ViewDefinition(pl.ctx, parts)
	if err != nil || !ok {
		return 0, false, err
	}
	body, err := pl.p.parse(def.Query)
	if err != nil {
		return 0, false, unsupported("view %s: %v", strings.Join(parts, "."), err)
	}
	var total int64
	walk(body, func(n node) bool {
		if err != nil || str(n["type"]) != "BASE_TABLE" {
			return err == nil
		}
		q, ok, rerr := def.Resolve(pl.ctx, tableParts(n))
		switch {
		case rerr != nil:
			err = rerr
			return false
		case !ok:
			err = unsupported("view %s reads %s, which may mean more than one relation", strings.Join(parts, "."), strings.Join(tableParts(n), "."))
			return false
		}
		t, terr := pl.table(q)
		if IsUnsupported(terr) {
			var b int64
			if b, ok, terr = pl.viewBytes(q, depth+1); terr == nil && ok {
				total += b
				return false
			}
		}
		if terr != nil {
			err = terr
			return false
		}
		total += t.Bytes
		return false
	})
	return total, err == nil, err
}

// relation adds a relation that a FROM clause reads, ref, to sc and rd.
func (pl *planner) relation(ref, q node, qscope cteScope, def *cteDef, splittable bool, sc *scope, rd *reads) error {
	if child(ref, "sample") != nil {
		return unsupported("TABLESAMPLE")
	}
	if child(ref, "at_clause") != nil {
		return unsupported("time travel")
	}
	var cols []Column
	var err error
	if def != nil {
		if def.columns == nil {
			if def.columns, err = pl.relationColumns(q, qscope, list(def.entry, "aliases")); err != nil {
				return err
			}
		}
		cols = def.columns
	} else if cols, err = pl.relationColumns(q, qscope, nil); err != nil {
		return err
	}
	bytes, err := pl.estimate(q, qscope)
	if err != nil {
		return err
	}
	key := pl.relationKey(q, qscope)
	bytes, rows, known := pl.measured(key, bytes, cols)
	item := &fromItem{name: strings.ToLower(str(ref["alias"]))}
	if item.name == "" && def != nil {
		item.name = def.key
	}
	for _, c := range cols {
		item.columns = append(item.columns, strings.ToLower(c.Name))
	}
	renameColumns(item, list(ref, "column_name_alias"))
	sc.items = append(sc.items, item)
	rd.rels = append(rd.rels, &relUse{ref: ref, q: q, scope: qscope, def: def, columns: cols, bytes: bytes,
		splittable: splittable, sc: sc, item: item, key: key, known: known, rows: rows})
	return nil
}

// inline reads a common table expression as the subquery it is, in place
// of ref, when ref is the only reference that reads it and its query only
// filters and projects tables: its reader then divides, partitions or
// reads in full those tables itself, as it would a subquery's, and
// aggregates their rows where it reads them, rather than workers writing
// every row to an exchange first. Every name the query reads must mean in
// ref's scope, ctes, what it means in its own, and it must call no
// volatile function, which each task that reads a table in full would
// call again. A candidate that does not qualify puts ref back, and job
// plans it again reading every common table expression as a relation.
func (pl *planner) inline(ref node, d *cteDef, ctes cteScope) (bool, error) {
	q := d.query()
	if pl.noInline || d.first != nil || str(q["type"]) != "SELECT_NODE" || pl.opaque(q) != "" || rowQuery(q) ||
		child(ref, "sample") != nil || child(ref, "at_clause") != nil || pl.uses(d) != 1 {
		return false, nil
	}
	same := true
	walk(q, func(m node) bool {
		switch {
		case str(m["class"]) == "FUNCTION" && volatileFunctions[strings.ToLower(str(m["function_name"]))]:
			same = false
		case str(m["type"]) == "BASE_TABLE" && str(m["schema_name"]) == "" && str(m["catalog_name"]) == "":
			a, b := d.scope.find(str(m["table_name"])), ctes.find(str(m["table_name"]))
			same = a == b || a != nil && b != nil && identity(a.entry) == identity(b.entry)
		}
		return same
	})
	if !same {
		return false, nil
	}
	sub, err := pl.p.fromRef("SELECT * FROM (SELECT 1) AS v")
	if err != nil {
		return false, err
	}
	child(sub, "subquery")["node"] = clone(q)
	sub["alias"] = str(ref["alias"])
	if sub["alias"] == "" {
		sub["alias"] = d.name
	}
	// The reference's column names, then the expression's.
	aliases := slices.Clone(list(d.entry, "aliases"))
	for i, a := range list(ref, "column_name_alias") {
		if i < len(aliases) {
			aliases[i] = a
		} else {
			aliases = append(aliases, a)
		}
	}
	if aliases == nil {
		aliases = []any{}
	}
	sub["column_name_alias"] = aliases
	original := clone(ref).(node)
	key := identity(d.entry)
	if pl.inlined == nil {
		pl.inlined = map[uintptr]bool{}
	}
	pl.inlined[key] = true
	pl.inlines++
	pl.undo = append(pl.undo, func() {
		clear(ref)
		maps.Copy(ref, original)
		delete(pl.inlined, key)
	})
	clear(ref)
	maps.Copy(ref, sub)
	return true, nil
}

// inlineLocal puts in place of ref, a reference to common table
// expression d, d's query as a subquery, when every task can compute its
// rows itself (see perTaskQuery), with the common table expressions it reads
// in place too, as long as those are such queries. However many
// references read it, none needs a relation: a report's cohorts, listed
// from the bitmaps the coordinator computes first, are joined by every
// task to its share of the report's source, which is divided among the
// workers, rather than keep the statement on one machine for want of a
// table to divide the cohorts by.
func (pl *planner) inlineLocal(ref node, d *cteDef) (bool, error) {
	if child(ref, "sample") != nil || child(ref, "at_clause") != nil || d.first != nil {
		return false, nil
	}
	q, ok, err := pl.expandLocal(d.query(), d.scope, 0)
	if err != nil || !ok {
		return false, err
	}
	sub, err := pl.cteSubquery(q, ref, d)
	if err != nil {
		return false, err
	}
	original := clone(ref).(node)
	pl.undo = append(pl.undo, func() {
		clear(ref)
		maps.Copy(ref, original)
	})
	clear(ref)
	maps.Copy(ref, sub)
	return true, nil
}

// expandLocal returns a copy of query q, of scope, with the references to
// common table expressions in its FROM clauses replaced by their queries,
// recursively, and whether it is then such a query (see perTaskQuery).
func (pl *planner) expandLocal(q node, scope cteScope, depth int) (node, bool, error) {
	if depth > maxViewDepth {
		return nil, false, nil
	}
	out := clone(q).(node)
	ok := true
	var err error
	rewrite(out, func(n node) (node, bool) {
		switch {
		case !ok || err != nil:
			return n, true
		case str(n["class"]) == "SUBQUERY":
			// A subquery expression's tables every task reads in full.
			return n, true
		case str(n["type"]) != "BASE_TABLE":
			return nil, false
		}
		parts := tableParts(n)
		if len(parts) != 1 {
			ok = false
			return n, true
		}
		d := scope.find(parts[0])
		if d == nil || child(n, "sample") != nil || child(n, "at_clause") != nil || d.first != nil {
			ok = false
			return n, true
		}
		inner, iok, ierr := pl.expandLocal(d.query(), d.scope, depth+1)
		if ierr != nil || !iok {
			ok, err = false, ierr
			return n, true
		}
		sub, serr := pl.cteSubquery(inner, n, d)
		if serr != nil {
			err = serr
			return n, true
		}
		return sub, true
	})
	if err != nil || !ok {
		return nil, false, err
	}
	return out, perTaskQuery(out), nil
}

// cteSubquery is the subquery that reads query q of common table
// expression d as reference ref does: under ref's alias, or d's name, with
// ref's column names, then d's.
func (pl *planner) cteSubquery(q, ref node, d *cteDef) (node, error) {
	sub, err := pl.p.fromRef("SELECT * FROM (SELECT 1) AS v")
	if err != nil {
		return nil, err
	}
	child(sub, "subquery")["node"] = q
	sub["alias"] = str(ref["alias"])
	if sub["alias"] == "" {
		sub["alias"] = d.name
	}
	aliases := slices.Clone(list(d.entry, "aliases"))
	for i, a := range list(ref, "column_name_alias") {
		if i < len(aliases) {
			aliases[i] = a
		} else {
			aliases = append(aliases, a)
		}
	}
	if aliases == nil {
		aliases = []any{}
	}
	sub["column_name_alias"] = aliases
	return sub, nil
}

// opaque says why a SELECT in FROM is not transparent, passing its rows
// through one by one, or "" if it is.
func (pl *planner) opaque(sel node) string {
	if len(list(child(sel, "cte_map"), "map")) > 0 || child(sel, "sample") != nil || child(sel, "qualify") != nil ||
		child(sel, "having") != nil || len(list(sel, "group_expressions")) > 0 || len(list(sel, "modifiers")) > 0 ||
		str(sel["aggregate_handling"]) != "STANDARD_HANDLING" {
		return "a subquery in FROM that groups, sorts or limits"
	}
	if pl.hasAggregate(sel["select_list"]) || hasClass(sel["select_list"], "WINDOW") {
		return "a subquery in FROM that aggregates"
	}
	if from := child(sel, "from_table"); from == nil || str(from["type"]) == "EMPTY" {
		return "a subquery without FROM"
	}
	return ""
}

// --- producing relations ----------------------------------------------------

// relationExchange returns the exchange a relation's rows are written to,
// keyed by keys (columns as the candidate names them), hashed as casts
// say, and plans the stages that write it. A common table expression's
// rows are written once: an exchange keyed otherwise is partitioned again
// rather than computed again.
func (pl *planner) relationExchange(r *relUse, keys, casts []string) (*Exchange, error) {
	xkeys := make([]string, len(keys))
	for i, k := range keys {
		j := slices.Index(r.item.columns, k)
		if j < 0 {
			return nil, fmt.Errorf("no column %s in %s", k, r.item.name)
		}
		xkeys[i] = r.columns[j].Name
	}
	sig := strings.Join(xkeys, "\x00") + "\x01" + strings.Join(casts, "\x00")
	if d := r.def; d != nil {
		if x := d.outs[sig]; x != nil {
			r.out = x
			return x, nil
		}
		if d.first != nil && len(d.first.Keys) > 0 {
			// Partitioned otherwise: partition it again. (Written whole, it
			// is small, and computed again.)
			x := pl.exchange("relation", xkeys, casts)
			f, err := pl.p.parse("SELECT * FROM " + quoteIdent("__igloo_r"))
			if err != nil {
				return nil, err
			}
			pl.stage(&Stage{fragment: f, Inputs: []Input{{View: "__igloo_r", Exchange: d.first, Partitioned: true}}, Out: x})
			pl.remember(d, sig, x)
			pl.relationOut(r, x)
			return x, nil
		}
	}
	x := pl.exchange("relation", xkeys, casts)
	if err := pl.produce(r.q, r.scope, x, r.columns, nil); err != nil {
		return nil, err
	}
	if r.def != nil {
		pl.remember(r.def, sig, x)
	}
	pl.relationOut(r, x)
	return x, nil
}

// relationOut records that x holds the rows of relation r.
func (pl *planner) relationOut(r *relUse, x *Exchange) {
	if r.key != "" {
		if pl.relKeys == nil {
			pl.relKeys = map[*Exchange]string{}
		}
		pl.relKeys[x] = r.key
	}
	r.out = x
}

// produce plans the stages that write the rows of query node q, as scope
// sees it, to out, with the columns cols (names and types): a SELECT, or a
// UNION ALL whose branches all write to out. The last stage applies wraps,
// innermost first, to its rows before it writes them.
func (pl *planner) produce(q node, scope cteScope, out *Exchange, cols []Column, wraps []*wrapper) error {
	switch str(q["type"]) {
	case "SET_OPERATION_NODE":
		if str(q["setop_type"]) != "UNION" || q["setop_all"] != true {
			return unsupported("%s in a relation", strings.ToLower(str(q["setop_type"])))
		}
		if len(list(q, "modifiers")) > 0 {
			return unsupported("ORDER BY or LIMIT in a relation")
		}
		sc := scope.with(q)
		for _, side := range []string{"left", "right"} {
			if err := pl.produce(child(q, side), sc, out, cols, wraps); err != nil {
				return err
			}
		}
		return nil
	case "SELECT_NODE":
		if hasClass(q["select_list"], "WINDOW") {
			outer, sc, err := pl.overAll(q, scope)
			if err == nil {
				return pl.produceSelect(outer, sc, out, cols, wraps)
			}
			if !partitionedWindows(q) {
				return err
			}
			// Windows over partitions, which its stage computes from its
			// inputs read by partition (see windowColumns).
		}
		return pl.produceSelect(q, scope, out, cols, wraps)
	}
	return unsupported("%s in a relation", strings.ToLower(str(q["type"])))
}

// overAll rewrites a SELECT whose window functions are aggregates over all
// of its rows, such as min(day) OVER () AS period_start, as the join of
// its rows to the one row of those aggregates, which are relations
// workers compute:
//
//	WITH __igloo_base_N AS (SELECT <columns and arguments> FROM ... WHERE ...),
//	     __igloo_over_N AS (SELECT min(__igloo_a0) AS __igloo_s0 FROM __igloo_base_N)
//	SELECT <items, reading both> FROM __igloo_base_N AS __igloo_base, __igloo_over_N AS __igloo_w
//
// It returns the SELECT and the scope it is read in.
func (pl *planner) overAll(sel node, scope cteScope) (node, cteScope, error) {
	if child(sel, "qualify") != nil || len(list(sel, "modifiers")) > 0 || len(list(sel, "group_expressions")) > 0 ||
		child(sel, "having") != nil || str(sel["aggregate_handling"]) != "STANDARD_HANDLING" || pl.hasAggregate(sel["select_list"]) {
		return nil, nil, unsupported("window functions in a relation")
	}
	pl.overs++
	n := strconv.Itoa(pl.overs)
	baseName, overName := "__igloo_base_"+n, "__igloo_over_"+n
	base := clone(sel).(node)
	var baseItems, overItems, outerItems []any
	keys := map[string]string{}
	column := func(e node, prefix string) (node, error) {
		k := canonical(stripAliases(e))
		name, ok := keys[k]
		if !ok {
			name = prefix + strconv.Itoa(len(baseItems))
			keys[k] = name
			baseItems = append(baseItems, aliased(e, name))
		}
		return pl.p.expr(quoteIdent("__igloo_base") + "." + quoteIdent(name))
	}
	var err error
	for _, item := range list(sel, "select_list") {
		e := clone(item).(node)
		if str(e["class"]) == "STAR" {
			return nil, nil, unsupported("* next to window functions in a relation")
		}
		alias := str(e["alias"])
		r := rewrite(e, func(m node) (node, bool) {
			if err != nil {
				return nil, false
			}
			switch str(m["class"]) {
			case "WINDOW":
				var agg node
				if agg, err = pl.overAggregate(m, column); err != nil {
					return nil, false
				}
				name := "__igloo_s" + strconv.Itoa(len(overItems))
				overItems = append(overItems, aliased(agg, name))
				var ref node
				ref, err = pl.p.expr(quoteIdent("__igloo_w") + "." + quoteIdent(name))
				return ref, err == nil
			case "COLUMN_REF":
				var ref node
				ref, err = column(bare(m), "__igloo_c")
				return ref, err == nil
			case "SUBQUERY", "LAMBDA":
				err = unsupported("%s next to window functions in a relation", strings.ToLower(str(m["class"])))
			}
			return nil, false
		}).(node)
		if err != nil {
			return nil, nil, err
		}
		r["alias"] = alias
		outerItems = append(outerItems, r)
	}
	if len(baseItems) == 0 {
		return nil, nil, unsupported("window functions over no columns in a relation")
	}
	base["select_list"] = baseItems
	over, err := pl.p.parse("SELECT 1 FROM " + quoteIdent(baseName) + " AS __igloo_base")
	if err != nil {
		return nil, nil, err
	}
	over["select_list"] = overItems
	outer, err := pl.p.parse("SELECT 1 FROM " + quoteIdent(baseName) + " AS __igloo_base, " + quoteIdent(overName) + " AS __igloo_w")
	if err != nil {
		return nil, nil, err
	}
	outer["select_list"] = outerItems
	entry := func(q node) node {
		return node{"aliases": []any{}, "query": node{"node": q, "named_param_map": []any{}},
			"materialized": "CTE_MATERIALIZE_DEFAULT", "key_targets": []any{}}
	}
	sc := make(cteScope, len(scope), len(scope)+2)
	copy(sc, scope)
	bd := &cteDef{name: baseName, key: baseName, entry: entry(base), scope: sc[:len(sc):len(sc)]}
	sc = append(sc, bd)
	od := &cteDef{name: overName, key: overName, entry: entry(over), scope: sc[:len(sc):len(sc)]}
	sc = append(sc, od)
	return outer, sc, nil
}

// overAggregate turns a window function over all rows into the aggregate
// it computes, over columns of the base relation that column adds.
func (pl *planner) overAggregate(w node, column func(node, string) (node, error)) (node, error) {
	if str(w["type"]) != "WINDOW_AGGREGATE" || len(list(w, "partitions")) > 0 || len(list(w, "orders")) > 0 ||
		str(w["start"]) != "UNBOUNDED_PRECEDING" || str(w["end"]) != "CURRENT_ROW_RANGE" ||
		str(w["exclude_clause"]) != "NO_OTHER" || len(list(w, "arg_orders")) > 0 || w["ignore_nulls"] == true ||
		child(w, "start_expr") != nil || child(w, "end_expr") != nil || child(w, "offset_expr") != nil || child(w, "default_expr") != nil {
		return nil, unsupported("window function %s() over more than all rows in a relation", strings.ToLower(str(w["function_name"])))
	}
	var args []string
	for _, a := range list(w, "children") {
		ref, err := column(bare(a.(node)), "__igloo_a")
		if err != nil {
			return nil, err
		}
		args = append(args, quoteIdent("__igloo_base")+"."+quoteIdent(columnNames(ref)[1]))
	}
	text := quoteIdent(strings.ToLower(str(w["function_name"]))) + "(" + strings.Join(args, ", ") + ")"
	if f := child(w, "filter_expr"); f != nil {
		ref, err := column(bare(f), "__igloo_a")
		if err != nil {
			return nil, err
		}
		text += " FILTER (WHERE " + quoteIdent("__igloo_base") + "." + quoteIdent(columnNames(ref)[1]) + ")"
	}
	agg, err := pl.p.expr(text)
	if err != nil {
		return nil, unsupported("%v", err)
	}
	if w["distinct"] == true {
		agg["distinct"] = true
	}
	return agg, nil
}

// stripAliases returns a copy of e without aliases, for comparing.
func stripAliases(e node) node {
	c := clone(e).(node)
	walk(c, func(n node) bool {
		if isExpr(n) {
			n["alias"] = ""
		}
		return true
	})
	return c
}

// produceSelect plans the stages that write the rows of a SELECT to out.
func (pl *planner) produceSelect(sel node, scope cteScope, out *Exchange, cols []Column, wraps []*wrapper) error {
	if w, r, err := pl.projection(sel, scope); err != nil {
		return err
	} else if w != nil {
		// It filters and projects a relation nothing else reads: the
		// relation's last stage does that too, rather than a stage of its
		// own reading what that one wrote.
		w.want = cols
		return pl.produce(r.q, r.scope, out, r.columns, append([]*wrapper{w}, wraps...))
	}
	for _, m := range list(sel, "modifiers") {
		if t := str(m.(node)["type"]); t != "DISTINCT_MODIFIER" {
			return unsupported("%s in a relation", strings.ToLower(t))
		}
	}
	windowed := child(sel, "qualify") != nil || hasClass(sel["select_list"], "WINDOW")
	if windowed && (len(list(sel, "modifiers")) > 0 || len(list(sel, "group_expressions")) > 0 || pl.hasAggregate(sel["select_list"])) {
		return unsupported("window functions in a relation that groups")
	}
	work, err := pl.distinctAsGroups(sel)
	if err != nil {
		return err
	}
	c := &candidate{sel: work, relation: true, want: out.Keys}
	if err := pl.analyze(c, scope); err != nil {
		return err
	}
	if windowed {
		if err := pl.windowColumns(c); err != nil {
			return err
		}
		if c.window == nil {
			return unsupported("window functions in a relation")
		}
	}
	if c.columns, err = pl.describe(work, scope); err != nil {
		return err
	}
	if len(c.columns) != len(cols) {
		return unsupported("a relation's branches have %d and %d columns", len(c.columns), len(cols))
	}
	if err := pl.chooseInputs(c); err != nil {
		return err
	}
	if err := c.checkWindows(); err != nil {
		return err
	}
	defer unmark(c)
	j, err := pl.views(c)
	if err != nil {
		return err
	}
	if err := pl.sides(c); err != nil {
		return err
	}
	var b *jobBuild
	if c.aggregating(pl) {
		if b, err = pl.relationAggregate(c); err != nil {
			return err
		}
	} else {
		f := clone(work).(node)
		f["cte_map"] = node{"map": []any{}}
		b = &jobBuild{fragment: f}
	}
	if err := pl.checkFragment(b.fragment); err != nil {
		return err
	}
	// The last stage writes the relation's columns, named and typed as
	// the consumers read them, through the projections it computes too.
	last := &b.fragment
	if b.second != nil {
		last = &b.second.fragment
	}
	f, err := pl.conform(*last, c.columns, cols)
	if err != nil {
		return err
	}
	have := cols
	for _, w := range wraps {
		if f, err = pl.conform(w.apply(f, have), w.have, w.want); err != nil {
			return err
		}
		have = w.want
	}
	if b.second != nil {
		if err := pl.checkFragment(f); err != nil {
			return err
		}
		b.second.columns = names(have)
	} else {
		if len(wraps) > 0 {
			if err := pl.checkFragment(f); err != nil {
				return err
			}
		}
		b.columns = names(have)
	}
	*last = f
	if _, err = pl.emit(j, b, out); err == nil {
		pl.uncertain = append(pl.uncertain, uncertainOf(c)...)
	}
	return err
}

// wrapper is a SELECT that filters and projects the rows of a relation it
// reads alone, which the relation's last stage computes (projection).
type wrapper struct {
	sel node
	// ref is the relation's reference in sel's FROM.
	ref node
	// have are sel's result columns; want are those it writes.
	have, want []Column
	// template is SELECT * FROM (SELECT 1) AS x, for the subquery that
	// reads the relation's rows.
	template node
}

// apply returns w's SELECT over f, whose result columns are inner.
func (w *wrapper) apply(f node, inner []Column) node {
	out := clone(w.sel).(node)
	out["cte_map"] = node{"map": []any{}}
	sub := clone(w.template).(node)
	child(sub, "subquery")["node"] = f
	alias := str(w.ref["alias"])
	if alias == "" {
		alias = str(w.ref["table_name"])
	}
	if alias == "" {
		alias = "__igloo_r"
	}
	sub["alias"] = alias
	var aliases []any
	for i, c := range inner {
		name := c.Name
		if renamed := list(w.ref, "column_name_alias"); i < len(renamed) {
			name = str(renamed[i])
		}
		aliases = append(aliases, name)
	}
	sub["column_name_alias"] = aliases
	out["from_table"] = sub
	return out
}

// projection reports whether sel only filters and projects one relation,
// a subquery or a common table expression nothing else reads, and if so
// returns the wrapper that computes it over the relation's rows.
func (pl *planner) projection(sel node, scope cteScope) (*wrapper, *relUse, error) {
	if len(list(sel, "modifiers")) > 0 || len(list(sel, "group_expressions")) > 0 || child(sel, "having") != nil ||
		child(sel, "qualify") != nil || child(sel, "sample") != nil || str(sel["aggregate_handling"]) != "STANDARD_HANDLING" ||
		pl.hasAggregate(sel["select_list"]) || hasClass(clauses(sel), "WINDOW") || hasClass(clauses(sel), "SUBQUERY") {
		return nil, nil, nil
	}
	ref := child(sel, "from_table")
	if child(ref, "sample") != nil || child(ref, "at_clause") != nil {
		return nil, nil, nil
	}
	sc := scope.with(sel)
	r := &relUse{ref: ref}
	switch str(ref["type"]) {
	case "SUBQUERY":
		r.q, r.scope = subqueryNode(ref), sc
	case "BASE_TABLE":
		parts := tableParts(ref)
		if len(parts) != 1 {
			return nil, nil, nil
		}
		d := sc.find(parts[0])
		if d == nil || d.first != nil || pl.uses(d) != 1 {
			return nil, nil, nil
		}
		r.q, r.scope, r.def = d.query(), d.scope, d
	default:
		return nil, nil, nil
	}
	var aliases []any
	if r.def != nil {
		aliases = list(r.def.entry, "aliases")
	}
	var err error
	if r.columns, err = pl.relationColumns(r.q, r.scope, aliases); err != nil {
		return nil, nil, err
	}
	w := &wrapper{sel: sel, ref: ref}
	if w.have, err = pl.describe(sel, scope); err != nil {
		return nil, nil, err
	}
	if w.template, err = pl.p.fromRef("SELECT * FROM (SELECT 1) AS x"); err != nil {
		return nil, nil, err
	}
	return w, r, nil
}

// distinctAsGroups returns a SELECT DISTINCT as the aggregation it is,
// grouped by its select items, and any other SELECT as it is.
func (pl *planner) distinctAsGroups(sel node) (node, error) {
	mods := list(sel, "modifiers")
	if len(mods) == 0 {
		return sel, nil
	}
	if len(list(mods[0].(node), "distinct_on_targets")) > 0 {
		return nil, unsupported("DISTINCT ON")
	}
	if len(list(sel, "group_expressions")) > 0 || str(sel["aggregate_handling"]) != "STANDARD_HANDLING" ||
		child(sel, "having") != nil || pl.hasAggregate(sel["select_list"]) {
		return nil, unsupported("DISTINCT over an aggregation in a relation")
	}
	out := clone(sel).(node)
	out["modifiers"] = []any{}
	var groups, set []any
	for _, item := range list(out, "select_list") {
		e := item.(node)
		switch str(e["class"]) {
		case "STAR":
			return nil, unsupported("SELECT DISTINCT * in a relation")
		case "CONSTANT":
			// The same in every row; and an integer would be a position.
			continue
		}
		set = append(set, jsonInt(len(groups)))
		groups = append(groups, bare(e))
	}
	if len(groups) == 0 {
		return nil, unsupported("SELECT DISTINCT of constants in a relation")
	}
	out["group_expressions"] = groups
	out["group_sets"] = []any{set}
	return out, nil
}

// conform makes a fragment whose result columns are have return them as
// want's types, which a UNION ALL's branches must agree on.
func (pl *planner) conform(f node, have, want []Column) (node, error) {
	same := true
	for i := range have {
		if have[i].Type != want[i].Type {
			same = false
		}
	}
	if same {
		return f, nil
	}
	w, err := pl.p.template("SELECT 1 FROM (SELECT 1) AS __igloo_c")
	if err != nil {
		return nil, err
	}
	var items, aliases []any
	for i := range have {
		name := "__c" + strconv.Itoa(i)
		aliases = append(aliases, name)
		ref, err := pl.p.expr(quoteIdent(name))
		if err != nil {
			return nil, err
		}
		e := ref
		if have[i].Type != want[i].Type {
			if e, err = pl.cast(ref, want[i].Type); err != nil {
				return nil, err
			}
		}
		items = append(items, aliased(e, name))
	}
	w["select_list"] = items
	from := child(w, "from_table")
	child(from, "subquery")["node"] = f
	from["column_name_alias"] = aliases
	return w, nil
}

func names(cols []Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

// --- savepoints -------------------------------------------------------------

// savepoint is how far planning had come: a candidate that turns out not
// to qualify takes back the stages and exchanges it planned, and puts back
// the views it read as their queries.
type savepoint struct{ stages, exchanges, memo, undo, expanded, uncertain, buckets int }

// memoEntry records an exchange remembered for a common table expression.
type memoEntry struct {
	def   *cteDef
	sig   string
	first bool
}

func (pl *planner) save() savepoint {
	return savepoint{stages: len(pl.stages), exchanges: pl.exchanges, memo: len(pl.memo), undo: len(pl.undo), expanded: pl.expanded,
		uncertain: len(pl.uncertain), buckets: len(pl.bucketNotes)}
}

func (pl *planner) restore(sp savepoint) {
	pl.stages = pl.stages[:sp.stages]
	pl.exchanges = sp.exchanges
	for i := len(pl.memo) - 1; i >= sp.memo; i-- {
		e := pl.memo[i]
		delete(e.def.outs, e.sig)
		if e.first {
			e.def.first = nil
		}
	}
	pl.memo = pl.memo[:sp.memo]
	for len(pl.undo) > sp.undo {
		pl.undo[len(pl.undo)-1]()
		pl.undo = pl.undo[:len(pl.undo)-1]
	}
	pl.expanded = sp.expanded
	pl.uncertain = pl.uncertain[:sp.uncertain]
	pl.bucketNotes = pl.bucketNotes[:sp.buckets]
	for x := range pl.relKeys {
		if x.ID >= sp.exchanges {
			delete(pl.relKeys, x)
		}
	}
}

// remember records that a common table expression's rows are written to x.
func (pl *planner) remember(d *cteDef, sig string, x *Exchange) {
	if d.outs == nil {
		d.outs = map[string]*Exchange{}
	}
	d.outs[sig] = x
	first := d.first == nil
	if first {
		d.first = x
	}
	pl.memo = append(pl.memo, memoEntry{def: d, sig: sig, first: first})
}

// --- the coordinator's common table expressions ------------------------------

// references counts the references to a one-part name in v.
func references(v any, name string) int {
	n := 0
	walk(v, func(m node) bool {
		if str(m["type"]) == "BASE_TABLE" && str(m["schema_name"]) == "" && str(m["catalog_name"]) == "" &&
			strings.EqualFold(str(m["table_name"]), name) {
			n++
		}
		return true
	})
	return n
}

// prune removes the common table expressions nothing reads from every WITH
// clause under v: those whose readers became distributed parts.
func prune(v any) {
	walk(v, func(n node) bool {
		m := child(n, "cte_map")
		for changed := true; changed; {
			changed = false
			entries := list(m, "map")
			for i, e := range entries {
				name := str(e.(node)["key"])
				others := clone(n).(node)
				others["cte_map"] = node{"map": slices.Delete(slices.Clone(entries), i, i+1)}
				if references(others, name) == 0 {
					m["map"] = slices.Delete(slices.Clone(entries), i, i+1)
					changed = true
					break
				}
			}
		}
		return true
	})
}
