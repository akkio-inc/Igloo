package distributed

import (
	"cmp"
	"slices"
	"strings"
)

// Partitioned inputs. When the tables a candidate reads besides the split
// table are too large for every worker to read, or it reads relations that
// workers compute (relation.go), its stage reads its large inputs by
// partition instead, keyed by a column they are joined on:
//
//  1. A table is read by a stage of its own: workers read shares of its
//     files, keep the rows the statement's filters on that table alone let
//     through, and write the columns the statement uses to an exchange
//     keyed by its join key (stage.go). A relation's stages write its rows
//     to an exchange keyed by its join key.
//  2. The candidate's stage then runs one task per partition, with each
//     input's view defined over that partition's files, and the other
//     tables read in full as before.
//
// Every row of an input that can join a row of another has an equal key,
// so lands in the same partition: each task sees every combination of rows
// the joins can produce, once, and the partial results combine as for any
// job.

// keyed is an input a candidate's stage reads by partition.
type keyed struct {
	// use is a table, or rel a relation.
	use  *tableUse
	rel  *relUse
	item *fromItem
	// keys are its key columns, lower case, as the candidate names them;
	// casts are the types they are hashed as, or "".
	keys, casts []string
	// view is the name the fragment reads it as.
	view string
	// stage, for a table, selects the rows its own stage partitions.
	stage node
}

func (k *keyed) String() string {
	if k.use != nil {
		return k.use.table.String()
	}
	return k.item.name
}

// ref is the input's reference in FROM.
func (k *keyed) ref() node {
	if k.use != nil {
		return k.use.ref
	}
	return k.rel.ref
}

// columnType returns the type of a key column of the input.
func (k *keyed) columnType(name string) string {
	if k.use != nil {
		return columnType(k.use.table, name)
	}
	if i := slices.Index(k.item.columns, name); i >= 0 {
		return k.rel.columns[i].Type
	}
	return ""
}

// chooseKeyed plans a candidate whose stage reads its large inputs by
// partition: every relation but those of one row, and the largest tables
// until the rest fit every worker; all of them must be joined, directly
// and by inner joins or WHERE, on equal columns, which become their keys.
func (pl *planner) chooseKeyed(c *candidate) error {
	err := pl.keyInputs(c)
	if u := c.tooDeep; u != nil && IsUnsupported(err) {
		// A large table inside a subquery that passes rows through: the
		// subquery it is in is a relation, written by its own stages. That
		// is the subquery of the FROM clause whose items the stage reads by
		// partition: the candidate's own, or one inside a subquery that
		// passes rows through, as one over a join whose rows are made once
		// per branch of a union pgwire factored is.
		top := c.scope
		for sc := u.scope; sc != nil; sc = sc.outer {
			if sc == c.keyScope && sc != u.scope {
				top = sc
			}
		}
		sc := u.scope
		for sc.outer != nil && sc.outer != top {
			sc = sc.outer
		}
		if rerr := pl.asRelation(c, sc, top); rerr != nil {
			return err
		}
		return pl.chooseKeyed(c)
	}
	return err
}

// asRelation makes a subquery that passes its rows through, whose FROM
// clause is sc, a relation the candidate reads, rather than a window on
// the tables it reads: one of the FROM clause top, the candidate's or one
// inside a subquery of it.
func (pl *planner) asRelation(c *candidate, sc, top *scope) error {
	if sc.ref == nil || sc.outer != top {
		return unsupported("a subquery inside a subquery")
	}
	q := subqueryNode(sc.ref)
	cols, err := pl.relationColumns(q, c.ctes, nil)
	if err != nil {
		return err
	}
	// The tables and relations it reads, in its FROM or WHERE, are read by
	// its stages.
	c.uses = slices.DeleteFunc(c.uses, func(u *tableUse) bool { return contains(q, u.ref) })
	c.rels = slices.DeleteFunc(c.rels, func(r *relUse) bool { return contains(q, r.ref) })
	sc.item.columns = nil
	for _, col := range cols {
		sc.item.columns = append(sc.item.columns, strings.ToLower(col.Name))
	}
	renameColumns(sc.item, list(sc.ref, "column_name_alias"))
	// Its stages write only the columns the candidate reads: a view of a
	// wide table, joined in each of a report's 16 branches on one key and
	// read for three columns, was written whole, 420 columns of 400
	// million rows a branch.
	if used := usedColumns(c, sc); used != nil && len(used) < len(sc.item.columns) {
		pq, err := pl.projectRelation(q, sc.item.columns, used)
		if err != nil {
			return err
		}
		if pcols, err := pl.relationColumns(pq, c.ctes, nil); err == nil {
			q, cols, sc.item.columns = pq, pcols, used
			// The view's columns are named as the candidate reads them.
			sc.ref[viewColumns] = []any{}
		}
	}
	bytes, err := pl.estimate(q, c.ctes)
	if err != nil {
		return err
	}
	key := pl.relationKey(q, c.ctes)
	bytes, rows, known := pl.measured(key, bytes, cols)
	c.rels = append(c.rels, &relUse{ref: sc.ref, q: q, scope: c.ctes, columns: cols, bytes: bytes, splittable: sc.splittable,
		sc: top, item: sc.item, key: key, known: known, rows: rows})
	return nil
}

// usedColumns returns the columns of sc's item, a subquery in c's FROM
// clause, that c refers to, in the item's order, or nil when c may refer
// to all of them: through a star, or a natural join. A name another item
// may own as well counts, and so does a name in a subquery of c, which may
// be correlated. A candidate that refers to none, as count(*) does, still
// reads one.
func usedColumns(c *candidate, sc *scope) []string {
	item := sc.item
	used := map[string]bool{}
	all := false
	walk(c.sel, func(m node) bool {
		if all || identity(m) == identity(sc.ref) {
			// The subquery's own names are its own.
			return false
		}
		switch str(m["class"]) {
		case "STAR":
			if r := strings.ToLower(str(m["relation_name"])); r == "" || r == item.name {
				all = true
			}
		case "COLUMN_REF":
			names := list(m, "column_names")
			if len(names) == 0 {
				break
			}
			first := strings.ToLower(str(names[0]))
			if len(names) > 1 && first == item.name {
				used[strings.ToLower(str(names[1]))] = true
			} else if slices.Contains(item.columns, first) {
				used[first] = true
			}
		}
		if str(m["type"]) == "JOIN" {
			if str(m["ref_type"]) == "NATURAL" {
				all = true
			}
			for _, u := range list(m, "using_columns") {
				used[strings.ToLower(str(u))] = true
			}
		}
		return !all
	})
	if all {
		return nil
	}
	var out []string
	for _, col := range item.columns {
		if used[col] && !slices.Contains(out, col) {
			out = append(out, col)
		}
	}
	if len(out) == 0 && len(item.columns) > 0 {
		out = item.columns[:1]
	}
	return out
}

// projectRelation returns a query of q's rows, whose columns are named all,
// with only those of them named used.
func (pl *planner) projectRelation(q node, all, used []string) (node, error) {
	quote := func(names []string) string {
		quoted := make([]string, len(names))
		for i, n := range names {
			quoted[i] = quoteIdent(n)
		}
		return strings.Join(quoted, ", ")
	}
	pq, err := pl.p.parse("SELECT " + quote(used) + " FROM (SELECT 1) AS __igloo_r(" + quote(all) + ")")
	if err != nil {
		return nil, err
	}
	child(child(pq, "from_table"), "subquery")["node"] = q
	return pq, nil
}

// contains reports whether n is a node of tree v.
func contains(v any, n node) bool {
	const probe = "__igloo_probe"
	n[probe] = true
	defer delete(n, probe)
	found := false
	walk(v, func(m node) bool {
		found = found || m[probe] == true
		return !found
	})
	return found
}

// keyInputs chooses the inputs the candidate's stage reads by partition
// and their keys.
func (pl *planner) keyInputs(c *candidate) error {
	c.split, c.keyed, c.whole, c.keyScope, c.tooDeep = nil, nil, nil, nil, nil
	// Every partitioned input must be an item of one FROM clause, whose
	// joins and WHERE say what they are joined on.
	inScope := func(sc *scope) bool {
		if c.keyScope == nil {
			c.keyScope = sc
		}
		return sc != nil && sc == c.keyScope
	}
	var largest int64
	for _, r := range c.rels {
		largest = max(largest, r.bytes)
		if r.bytes == 0 || r.query {
			// One row, or a filter's subquery: every task reads it.
			c.whole = append(c.whole, r)
			continue
		}
		if !(r.splittable || r.semi) || !inScope(r.sc) {
			return unsupported("%s is read where its rows cannot be divided", r.item.name)
		}
		c.keyed = append(c.keyed, &keyed{rel: r, item: r.item})
	}
	uses := slices.Clone(c.uses)
	slices.SortStableFunc(uses, func(a, b *tableUse) int { return cmp.Compare(b.table.Bytes, a.table.Bytes) })
	var rest int64
	for _, u := range uses {
		rest += u.table.Bytes
		largest = max(largest, u.table.Bytes)
	}
	// The tables whose columns the windows partition by are read by
	// partition whatever their size, so that each task holds whole
	// partitions of them.
	windowed := func(u *tableUse) bool {
		return slices.ContainsFunc(c.window, func(ic itemColumn) bool { return ic.item == u.item })
	}
	slices.SortStableFunc(uses, func(a, b *tableUse) int {
		switch wa, wb := windowed(a), windowed(b); {
		case wa && !wb:
			return -1
		case wb && !wa:
			return 1
		}
		return 0
	})
	for _, u := range uses {
		if rest <= pl.opts.maxBroadcast && (len(c.keyed) > 0 || u.table.Bytes < pl.opts.minBytes) && !windowed(u) {
			break
		}
		if !(u.splittable || u.semi) || !inScope(u.scope) {
			if u.splittable && u.scope != nil && u.scope.ref != nil {
				c.tooDeep = u
			}
			return unsupported("%s, too large for every worker to read, is read where its rows cannot be divided", u.table)
		}
		if len(list(u.ref, "column_name_alias")) > 0 {
			return unsupported("%s with renamed columns", u.table)
		}
		c.keyed = append(c.keyed, &keyed{use: u, item: u.item})
		rest -= u.table.Bytes
	}
	if !pl.opts.force && !c.relation && largest < pl.opts.minBytes {
		return unsupported("its inputs are small (%d bytes)", largest)
	}
	if len(c.keyed) == 0 {
		return unsupported("nothing to divide among workers")
	}
	for _, u := range c.uses {
		if !u.splittable && u.scope == nil && slices.ContainsFunc(c.keyed, func(k *keyed) bool { return k.use != nil && k.use.table.key() == u.table.key() }) {
			return unsupported("%s is read again in a subquery", u.table)
		}
	}

	var keys [][]itemColumn
	if len(c.keyed) == 1 {
		k := c.keyed[0]
		for _, name := range pl.preferredKeys(c, k) {
			keys = append(keys, []itemColumn{{k.item, name}})
		}
	} else {
		keys = joinKeys(c.keyScope)
		if c.window != nil {
			// Keyed by the join's columns that the windows partition by
			// alone, so that each task holds whole partitions of them.
			var by [][]itemColumn
			for _, class := range keys {
				if slices.ContainsFunc(class, func(ic itemColumn) bool { return slices.Contains(c.window, ic) }) {
					by = append(by, class)
				}
			}
			if len(by) > 0 {
				keys = by
			}
		}
	}
	for _, class := range keys {
		var cols []itemColumn
		for _, k := range c.keyed {
			i := slices.IndexFunc(class, func(ic itemColumn) bool { return ic.item == k.item })
			if i < 0 {
				cols = nil
				break
			}
			cols = append(cols, class[i])
		}
		if cols == nil {
			continue
		}
		cast, err := pl.hashCast(c, cols)
		if err != nil {
			return err
		}
		for i, k := range c.keyed {
			k.keys = append(k.keys, cols[i].name)
			k.casts = append(k.casts, cast)
		}
	}
	if len(c.keyed[0].keys) == 0 {
		names := make([]string, len(c.keyed))
		for i, k := range c.keyed {
			names[i] = k.String()
		}
		return unsupported("%s, too large to read in full, are not joined on equal columns", strings.Join(names, " and "))
	}
	return nil
}

// joinKeys returns the classes of columns of the candidate's FROM items
// that its inner joins and WHERE clause make equal (a.x = b.y), in the
// order they appear.
func joinKeys(sc *scope) [][]itemColumn {
	parent := map[itemColumn]itemColumn{}
	var order []itemColumn
	var find func(x itemColumn) itemColumn
	find = func(x itemColumn) itemColumn {
		p, ok := parent[x]
		if !ok {
			parent[x] = x
			order = append(order, x)
			return x
		}
		if p == x {
			return x
		}
		r := find(p)
		parent[x] = r
		return r
	}
	for _, cond := range append(slices.Clone(sc.conds), sc.semiConds...) {
		if str(cond["class"]) != "COMPARISON" || str(cond["type"]) != "COMPARE_EQUAL" {
			continue
		}
		l, r := child(cond, "left"), child(cond, "right")
		if str(l["class"]) != "COLUMN_REF" || str(r["class"]) != "COLUMN_REF" {
			continue
		}
		li, ri := resolveColumn(l, sc), resolveColumn(r, sc)
		if li == nil || ri == nil {
			continue
		}
		a, b := find(itemColumn{li, strings.ToLower(lastName(l))}), find(itemColumn{ri, strings.ToLower(lastName(r))})
		if a != b {
			parent[b] = a
		}
	}
	var classes [][]itemColumn
	index := map[itemColumn]int{}
	for _, x := range order {
		root := find(x)
		i, ok := index[root]
		if !ok {
			i = len(classes)
			index[root] = i
			classes = append(classes, nil)
		}
		classes[i] = append(classes[i], x)
	}
	var out [][]itemColumn
	for _, cl := range classes {
		if len(cl) > 1 {
			out = append(out, cl)
		}
	}
	return out
}

// preferredKeys chooses the key columns of a candidate's only partitioned
// input: those it was written by already, for a relation; the argument of
// its aggregates over DISTINCT values, which partitions then keep apart; its
// group columns, which partitions then keep whole; the columns it is asked
// to be written by; or its first column.
func (pl *planner) preferredKeys(c *candidate, k *keyed) []string {
	if names := windowKeys(c, k); names != nil {
		// Whole partitions of its windows.
		return names
	}
	if k.rel != nil && k.rel.def != nil && k.rel.def.first != nil {
		var names []string
		for _, key := range k.rel.def.first.Keys {
			i := slices.IndexFunc(k.rel.columns, func(col Column) bool { return strings.EqualFold(col.Name, key) })
			if i < 0 {
				names = nil
				break
			}
			names = append(names, k.item.columns[i])
		}
		if len(names) > 0 {
			return names
		}
	}
	own := func(e node) string {
		if str(e["class"]) == "COLUMN_REF" && resolveColumn(e, c.scope) == k.item {
			return strings.ToLower(lastName(e))
		}
		return ""
	}
	var distinct []string
	inParts := true
	walk([]any{c.sel["select_list"], c.sel["having"]}, func(n node) bool {
		if !pl.isAggregate(n) {
			return true
		}
		name := strings.ToLower(str(n["function_name"]))
		args := list(n, "children")
		switch {
		case len(list(child(n, "order_bys"), "orders")) > 0:
			inParts = false
		case n["distinct"] == true:
			if !additiveDistinct[name] {
				inParts = false
			}
			if len(args) == 1 {
				if col := own(args[0].(node)); col != "" && !slices.Contains(distinct, col) {
					distinct = append(distinct, col)
				}
			}
		case partialArgs(name, len(args)) != nil:
			inParts = false
		}
		return false
	})
	// An aggregate that does not compute in parts needs whole groups.
	if len(distinct) == 1 && inParts {
		return distinct
	}
	var groups []string
	for _, g := range list(c.sel, "group_expressions") {
		if name := own(g.(node)); name != "" && !slices.Contains(groups, name) {
			groups = append(groups, name)
		}
	}
	if len(groups) > 0 && len(list(c.sel, "group_sets")) <= 1 {
		return groups
	}
	for _, want := range c.want {
		for _, item := range list(c.sel, "select_list") {
			e := item.(node)
			name := strings.ToLower(str(e["alias"]))
			if name == "" && str(e["class"]) == "COLUMN_REF" {
				name = strings.ToLower(lastName(e))
			}
			if name == strings.ToLower(want) {
				if col := own(e); col != "" {
					return []string{col}
				}
			}
		}
	}
	return k.item.columns[:1]
}

// hashCast returns the type a class of key columns is hashed as: BIGINT
// for integers of any width, whose values DuckDB hashes alike; otherwise
// their own type, which must be the same.
func (pl *planner) hashCast(c *candidate, cols []itemColumn) (string, error) {
	var types []string
	for i, ic := range cols {
		types = append(types, c.keyed[i].columnType(ic.name))
	}
	cast, ok := "", true
	for _, t := range types {
		switch {
		case t == "":
			ok = false
		case slices.Contains(intTypes, t):
			cast = "BIGINT"
		}
	}
	for _, t := range types[1:] {
		if t != types[0] && !(slices.Contains(intTypes, t) && slices.Contains(intTypes, types[0])) {
			ok = false
		}
	}
	if !ok {
		return "", unsupported("join keys of different types (%s)", strings.Join(types, ", "))
	}
	if strings.HasPrefix(types[0], "VARCHAR") {
		// Values equal under a collation do not hash alike.
		collation, err := pl.collation()
		if err != nil {
			return "", err
		}
		if collation != "" {
			return "", unsupported("a text join key under collation %s", collation)
		}
	}
	return cast, nil
}

// intTypes are the integer types DuckDB hashes by value, alike whatever
// their width.
var intTypes = []string{"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER"}

// resolveColumn returns the FROM item a column reference names, or nil
// when that is not certain.
func resolveColumn(ref node, sc *scope) *fromItem {
	parts := columnNames(ref)
	for i := range parts {
		parts[i] = strings.ToLower(parts[i])
	}
	switch len(parts) {
	case 1:
		return sc.owner(parts[0])
	case 2:
		var found *fromItem
		for _, it := range sc.items {
			if it.name == parts[0] && it.columns != nil && slices.Contains(it.columns, parts[1]) {
				if found != nil {
					return nil
				}
				found = it
			}
		}
		return found
	}
	return nil
}

func lastName(ref node) string {
	parts := columnNames(ref)
	return parts[len(parts)-1]
}

func columnType(t *Table, name string) string {
	for _, c := range t.Columns {
		if strings.EqualFold(c.Name, name) {
			return c.Type
		}
	}
	return ""
}

// sides builds the queries that select the rows of each partitioned table:
// the columns the statement uses, after its filters on that table alone.
func (pl *planner) sides(c *candidate) error {
	refs := referencedNames(c.sel)
	for _, k := range c.keyed {
		u := k.use
		if u == nil {
			continue
		}
		var cols []string
		for _, col := range u.table.Columns {
			if refs == nil || refs[strings.ToLower(col.Name)] {
				cols = append(cols, quoteIdent(col.Name))
			}
		}
		alias := str(u.ref["alias"])
		if alias == "" {
			alias = str(u.ref["table_name"])
		}
		stage, err := pl.p.parse("SELECT " + strings.Join(cols, ", ") + " FROM " + quoteIdent(k.view) + " AS " + quoteIdent(alias))
		if err != nil {
			return err
		}
		// The statement's filters on this table alone, so less crosses.
		var filters []any
		for _, cond := range u.scope.conds {
			if onlyColumnsOf(cond, u.item, u.scope) {
				filters = append(filters, clone(cond))
			}
		}
		switch len(filters) {
		case 0:
		case 1:
			stage["where_clause"] = filters[0]
		default:
			and, err := pl.p.template("SELECT 1 WHERE true AND true")
			if err != nil {
				return err
			}
			w := child(and, "where_clause")
			w["children"] = filters
			stage["where_clause"] = w
		}
		k.stage = stage
	}
	return nil
}

// referencedNames returns every name a column reference in n uses, lower
// case, or nil when a * or a NATURAL join may use any column.
func referencedNames(n node) map[string]bool {
	names := map[string]bool{}
	all := false
	walk(clauses(n), func(m node) bool {
		switch {
		case str(m["class"]) == "STAR", str(m["ref_type"]) == "NATURAL":
			all = true
		case str(m["class"]) == "COLUMN_REF":
			for _, p := range columnNames(m) {
				names[strings.ToLower(p)] = true
			}
		}
		for _, u := range list(m, "using_columns") {
			names[strings.ToLower(str(u))] = true
		}
		return !all
	})
	if all {
		return nil
	}
	return names
}

// onlyColumnsOf reports whether cond reads columns of item alone, and
// nothing that depends on more than the row: a filter that can run on
// item's rows before they are partitioned.
func onlyColumnsOf(cond node, item *fromItem, sc *scope) bool {
	ok, any := true, false
	walk(cond, func(m node) bool {
		switch str(m["class"]) {
		case "SUBQUERY", "WINDOW", "PARAMETER", "LAMBDA", "STAR":
			ok = false
		case "COLUMN_REF":
			any = true
			if resolveColumn(m, sc) != item {
				ok = false
			}
		}
		return ok
	})
	return ok && any
}
