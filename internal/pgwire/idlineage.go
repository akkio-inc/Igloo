package pgwire

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/policy"
)

// Counting through ids along a column's lineage (see idcount.go).
//
// A report counts its people where it has them, often several steps from
// the table they come from:
//
//	WITH BASE AS (SELECT v.person, v.day, min(v.day) OVER () AS since FROM v_visits AS v JOIN everyone AS e ON e.person = v.person),
//	EXPANDED AS (SELECT b.person, k.kw FROM (SELECT b.*, unnest(b.keywords) AS kw FROM BASE AS b) AS b)
//	SELECT kw, count(DISTINCT EXPANDED.person) FROM EXPANDED GROUP BY kw
//
// The member column reaches the count unchanged: selected under a name or
// through a star, past joins, filters, windows and unnests of other
// columns, out of a view of the member table. Each row keeps its id along
// the way, so the count counts the ids instead, which a column of their own
// carries up the same steps (_igloo_lineage_N):
//
//   - the member table's reference becomes a subquery that reads it, at the
//     snapshot whose rows all have ids, with its columns and its ids, cast
//     as the count casts them: (SELECT t.* EXCLUDE (ids), CAST(t.ids AS
//     UINTEGER) AS _igloo_lineage_N FROM table AS t AT (VERSION => n)) AS v;
//   - each step that selects the column under a name selects the ids too,
//     as the last of its columns; a step that selects the column through a
//     star carries them as it is;
//   - a view of the member table on the way is expanded in place, as the
//     policy rewrite expands it anyway, with its tables named in full, and
//     the statement analyzed again.
//
// The lineage breaks, and the column is counted as written, where a step
// transforms the column (any expression but the column itself), aggregates
// (GROUP BY, HAVING), combines rows of several queries (UNION and the
// like), renames its columns by position where it cannot tell which is
// which (a CTE's column list; a subquery's, as an expanded view's, is
// followed through its items and stars over tables), or reads the column
// from somewhere it cannot tell (an unqualified star over a join, a table
// function); and where the steps'

// added column could show: a star or a whole-row reference to a CTE on the
// way anywhere but on the way, or a statement that names a column like
// those the steps add, which could read them. Views are expanded only for
// users whose role reads the whole warehouse, as bitmap indexes read
// through them, and only where no policy applies to the view; the member
// table takes the same checks as a count of its column in the same SELECT.

// lineagePrefix starts the names of the columns that carry ids up a
// column's lineage, and of the member tables' references in their
// subqueries.
const lineagePrefix = "_igloo_lineage_"

// lineage traces counted columns of a statement back to member tables.
type lineage struct {
	s       *Server
	ctx     context.Context
	sess    *session
	text    string
	root    map[string]any
	r       *resolver
	members *iddomain.Snapshot
	gov     *policy.Snapshot
	pinned  func(schema, table, column string) (int64, bool)
	// ctes are the statement's common table expressions by lower-case
	// name; nil for a name defined more than once.
	ctes map[string]map[string]any
	// edits are the steps' added columns and the member tables'
	// subqueries; used the member tables counted through ids, at their
	// snapshots; wrapped their references that subqueries replace.
	edits   []edit
	used    map[string]int64
	wrapped map[string]bool
	// counts are the counts' arguments, each the column carrying ids.
	counts []edit
	// memo is the column carrying ids out of each step, by step and column;
	// steps are the SELECTs on the way, starHops those that carry the ids
	// through a star, and chainCTEs the CTEs on the way.
	memo      map[string]string
	steps     map[string]bool
	starHops  map[string]bool
	chainCTEs map[string]bool
	n         int
	// why is why the lineage traced last broke, for the log.
	why string
	// inlined is the statement with a view on the way expanded, to analyze
	// again, when tracing found one.
	inlined string
}

func newLineage(s *Server, ctx context.Context, sess *session, text string, root map[string]any, r *resolver,
	members *iddomain.Snapshot, gov *policy.Snapshot, pinned func(schema, table, column string) (int64, bool)) *lineage {
	lt := &lineage{s: s, ctx: ctx, sess: sess, text: text, root: root, r: r, members: members, gov: gov, pinned: pinned,
		ctes: map[string]map[string]any{}, used: map[string]int64{}, wrapped: map[string]bool{},
		memo: map[string]string{}, steps: map[string]bool{}, starHops: map[string]bool{}, chainCTEs: map[string]bool{}}
	walkMaps(root, func(m map[string]any) {
		cm, ok := m["cte_map"].(map[string]any)
		if !ok {
			return
		}
		for _, e := range asList(cm["map"]) {
			em, _ := e.(map[string]any)
			if em == nil {
				continue
			}
			name := strings.ToLower(str(em["key"]))
			v, _ := em["value"].(map[string]any)
			if _, dup := lt.ctes[name]; dup {
				lt.ctes[name] = nil
				continue
			}
			lt.ctes[name] = v
		}
	})
	return lt
}

// through substitutes, in sel, a counted reference arg (of names) to a
// column of ref, a CTE or subquery that sel reads, with the column that
// carries its ids, reporting false where the lineage breaks (see
// rewriteIDCounts' through). counting records the relations it counts.
func (lt *lineage) through(sel map[string]any, ref *idRef, arg map[string]any, names []any, apply bool, counting map[string]map[string]bool) bool {
	// A statement that names a column like those that carry ids could read
	// them: it counts along no lineage.
	if len(names) == 0 || len(names) > 2 || strings.Contains(strings.ToLower(lt.text), lineagePrefix) {
		return false
	}
	l, ok := lt.trace(sel, ref.node, str(names[len(names)-1]))
	if !ok {
		if lt.inlined == "" && lt.why != "" {
			lt.s.log.Debug("a count along a column's lineage counts the values", "relation", ref.alias, "column", str(names[len(names)-1]), "why", lt.why)
		}
		lt.why = ""
		return false
	}
	loc, ok := nodeAt(arg)
	if !ok {
		return false
	}
	sp, ok := scanName(lt.text, loc)
	if !ok || len(sp.parts) != len(names) {
		return false
	}
	if apply {
		qualifier := quoteIdent(ref.alias)
		if len(names) == 2 {
			qualifier = quoteIdent(sp.parts[0])
		}
		lt.counts = append(lt.counts, edit{sp.start, sp.end, qualifier + "." + quoteIdent(l)})
		if counting[nodeKey(sel)] == nil {
			counting[nodeKey(sel)] = map[string]bool{}
		}
		counting[nodeKey(sel)][ref.alias] = true
	}
	return true
}

// breaks records why a lineage breaks, the first reason found, which is
// the deepest, and returns that it does.
func (lt *lineage) breaks(format string, args ...any) (string, bool) {
	if lt.why == "" {
		lt.why = fmt.Sprintf(format, args...)
	}
	return "", false
}

// nodeKey identifies a parse object.
func nodeKey(m map[string]any) string { return fmt.Sprintf("%p", m) }

// trace returns the column that carries the ids of column col of the
// relation leaf, a CTE's reference or a subquery in sel's FROM, adding
// what carries them there; false, with nothing added, where the lineage
// breaks.
func (lt *lineage) trace(sel, leaf map[string]any, col string) (string, bool) {
	edits, memo := len(lt.edits), len(lt.memo)
	keys := make(map[string]bool, len(lt.memo))
	for k := range lt.memo {
		keys[k] = true
	}
	l, ok := lt.from(leaf, col)
	if !ok {
		lt.edits = lt.edits[:edits]
		if len(lt.memo) != memo {
			for k := range lt.memo {
				if !keys[k] {
					delete(lt.memo, k)
				}
			}
		}
		return "", false
	}
	return l, true
}

// from returns the column that carries the ids of column col out of leaf,
// a relation of a FROM.
func (lt *lineage) from(leaf map[string]any, col string) (string, bool) {
	switch str(leaf["type"]) {
	case "SUBQUERY":
		sub, _ := leaf["subquery"].(map[string]any)
		node, _ := sub["node"].(map[string]any)
		if node == nil {
			return lt.breaks("subquery %s has no query", str(leaf["alias"]))
		}
		// A column list renames the subquery's first columns, as an
		// expanded view's does: the column is the one at its position. The
		// column carrying ids, the last, keeps its name.
		if names := asList(leaf["column_name_alias"]); len(names) > 0 {
			i := slices.IndexFunc(names, func(n any) bool { return strings.EqualFold(str(n), col) })
			if i < 0 {
				return lt.breaks("subquery %s renames its columns by position, and %s is not one of the names", str(leaf["alias"]), col)
			}
			name, ok := lt.nameAt(node, i)
			if !ok {
				return lt.breaks("subquery %s renames its columns by position, which cannot be told apart", str(leaf["alias"]))
			}
			col = name
		}
		return lt.output(node, col)
	case "BASE_TABLE":
		parts := refParts(leaf)
		if len(parts) == 1 {
			if c, ok := lt.ctes[strings.ToLower(parts[0])]; ok {
				if c == nil || len(asList(c["aliases"])) > 0 || len(asList(leaf["column_name_alias"])) > 0 {
					return lt.breaks("CTE %s is defined twice or renames its columns by position", parts[0])
				}
				q, _ := c["query"].(map[string]any)
				node, _ := q["node"].(map[string]any)
				if node == nil {
					return lt.breaks("CTE %s has no query", parts[0])
				}
				lt.chainCTEs[strings.ToLower(parts[0])] = true
				return lt.output(node, col)
			}
		}
		res, err := lt.r.resolve(tableRef{parts: parts}, nil)
		if err != nil || len(res.all) != 1 || len(res.first) != 1 {
			return lt.breaks("%s does not resolve to one table", strings.Join(parts, "."))
		}
		o := res.first[0]
		if o.view {
			lt.inline(leaf, o)
			if lt.inlined == "" {
				return lt.breaks("view %s.%s is not expanded", o.schema, o.name)
			}
			return "", false
		}
		return lt.base(leaf, o, col)
	}
	return lt.breaks("the column comes from a %s", strings.ToLower(str(leaf["type"])))
}

// output returns the column that carries the ids of column col out of the
// SELECT step, adding it to the step where the step names col.
func (lt *lineage) output(step map[string]any, col string) (string, bool) {
	key := nodeKey(step) + "/" + strings.ToLower(col)
	if l, ok := lt.memo[key]; ok {
		return l, true
	}
	if str(step["type"]) != "SELECT_NODE" || len(asList(step["group_expressions"])) > 0 || step["having"] != nil || step["sample"] != nil {
		return lt.breaks("a step that is a %s, groups or samples", strings.ToLower(str(step["type"])))
	}
	if h := str(step["aggregate_handling"]); h != "" && h != "STANDARD_HANDLING" {
		return lt.breaks("a step that groups by all")
	}
	var named, star map[string]any
	stars := 0
	for _, it := range asList(step["select_list"]) {
		im, _ := it.(map[string]any)
		switch str(im["class"]) {
		case "STAR":
			stars++
			star = im
		case "COLUMN_REF":
			cn := asList(im["column_names"])
			name := str(im["alias"])
			if name == "" && len(cn) > 0 {
				name = str(cn[len(cn)-1])
			}
			if strings.EqualFold(name, col) {
				if named != nil {
					return lt.breaks("a step names %s twice", col)
				}
				named = im
			}
		default:
			if strings.EqualFold(str(im["alias"]), col) {
				return lt.breaks("a step transforms %s", col)
			}
		}
	}
	var l string
	switch {
	case named != nil:
		cn := asList(named["column_names"])
		if len(cn) == 0 || len(cn) > 2 {
			return lt.breaks("a step reads %s as a field", col)
		}
		q := ""
		if len(cn) == 2 {
			q = str(cn[0])
		}
		leaf, alias := lt.leaf(step, q)
		if leaf == nil {
			return lt.breaks("a step reads %s from no one relation (%q)", col, q)
		}
		var ok bool
		if l, ok = lt.from(leaf, str(cn[len(cn)-1])); !ok {
			return "", false
		}
		at, ok := lt.selectEnd(step)
		if !ok {
			return lt.breaks("cannot place a column after a step's select list")
		}
		lt.edits = append(lt.edits, edit{at, at, ", " + quoteIdent(alias) + "." + quoteIdent(l) + " AS " + quoteIdent(l) + " "})
	case stars == 1:
		if star["expr"] != nil || star["columns"] == true || star["unpacked"] == true || star["replace_list"] != nil || len(asList(star["rename_list"])) > 0 {
			return lt.breaks("a step's star replaces, renames or picks columns")
		}
		for _, x := range asList(star["exclude_list"]) {
			if xm, ok := x.(map[string]any); ok && strings.EqualFold(str(xm["column_name"]), col) {
				return lt.breaks("a step's star leaves %s out", col)
			} else if s, ok := x.(string); ok && strings.EqualFold(s, col) {
				return lt.breaks("a step's star leaves %s out", col)
			}
		}
		leaf, _ := lt.leaf(step, str(star["relation_name"]))
		if leaf == nil {
			return lt.breaks("a step's star reads no one relation (%q)", str(star["relation_name"]))
		}
		var ok bool
		if l, ok = lt.from(leaf, col); !ok {
			return "", false
		}
		lt.starHops[nodeKey(step)] = true
	default:
		return lt.breaks("a step has no column %s, or several stars", col)
	}
	lt.memo[key] = l
	lt.steps[nodeKey(step)] = true
	return l, true
}

// nameAt returns the name of the column at position i of the SELECT
// step's result: an item's own, or one of a star's over a table of the
// catalog, whose columns other than those of ids it describes.
func (lt *lineage) nameAt(step map[string]any, i int) (string, bool) {
	if str(step["type"]) != "SELECT_NODE" {
		return "", false
	}
	pos := 0
	for _, it := range asList(step["select_list"]) {
		im, _ := it.(map[string]any)
		if str(im["class"]) != "STAR" {
			if pos == i {
				name := str(im["alias"])
				if cn := asList(im["column_names"]); name == "" && str(im["class"]) == "COLUMN_REF" && len(cn) > 0 {
					name = str(cn[len(cn)-1])
				}
				return name, name != ""
			}
			pos++
			continue
		}
		if im["expr"] != nil || im["columns"] == true || im["unpacked"] == true || im["replace_list"] != nil ||
			len(asList(im["rename_list"])) > 0 || len(asList(im["exclude_list"])) > 0 {
			return "", false
		}
		leaf, _ := lt.leaf(step, str(im["relation_name"]))
		if leaf == nil || str(leaf["type"]) != "BASE_TABLE" {
			return "", false
		}
		parts := refParts(leaf)
		if len(parts) == 1 && lt.ctes[strings.ToLower(parts[0])] != nil {
			return "", false
		}
		res, err := lt.r.resolve(tableRef{parts: parts}, nil)
		if err != nil || len(res.all) != 1 || len(res.first) != 1 || res.first[0].view {
			return "", false
		}
		o := res.first[0]
		cols, err := describeTable(lt.ctx, lt.sess, quoteName([]string{o.db, o.schema, o.name}))
		if err != nil {
			return "", false
		}
		for _, c := range cols {
			if iddomain.IsHidden(c.name) {
				continue
			}
			if pos == i {
				return c.name, true
			}
			pos++
		}
	}
	return "", false
}

// leaf returns the relation of step's FROM that qualifier q names, and its
// name there; with q empty, its only one.
func (lt *lineage) leaf(step map[string]any, q string) (map[string]any, string) {
	var found map[string]any
	var name string
	n, matches := 0, 0
	fromRefs(step["from_table"], func(t map[string]any) {
		n++
		alias := str(t["alias"])
		if alias == "" && str(t["type"]) == "BASE_TABLE" {
			alias = str(t["table_name"])
		}
		if q == "" || strings.EqualFold(alias, q) {
			matches++
			found, name = t, alias
		}
	})
	if matches != 1 || q == "" && n != 1 || name == "" {
		return nil, ""
	}
	return found, name
}

// selectEnd returns where a column goes after the last of step's select
// list: before the FROM that follows it, found from a column or star of
// the list, which the parse places in the text.
func (lt *lineage) selectEnd(step map[string]any) (int, bool) {
	start := -1
	for _, it := range asList(step["select_list"]) {
		im, _ := it.(map[string]any)
		if c := str(im["class"]); c != "COLUMN_REF" && c != "STAR" {
			continue
		}
		if l := location(im["query_location"]); l >= 0 && (start < 0 || l < start) {
			start = l
		}
	}
	if start < 0 || start >= len(lt.text) {
		return 0, false
	}
	at := topLevelKeyword(lt.text[start:], "FROM")
	if at < 0 {
		return 0, false
	}
	at += start
	if from, _ := step["from_table"].(map[string]any); from != nil {
		if l := location(from["query_location"]); l >= 0 && l < at {
			return 0, false
		}
	}
	return at, true
}

// base returns the column that carries the ids of member column col of
// the member table o, which leaf reads, replacing leaf with a subquery
// that reads them too (see the top of this file).
func (lt *lineage) base(leaf map[string]any, o object, col string) (string, bool) {
	table := o.schema + "." + o.name
	if !strings.EqualFold(o.db, lt.s.catalogName()) || leaf["at_clause"] != nil || leaf["sample"] != nil || len(asList(leaf["column_name_alias"])) > 0 {
		return lt.breaks("%s is another database's, or read at a version, sampled or renamed", table)
	}
	member := lt.members.MemberOf(o.schema, o.name, col)
	if member == nil {
		return lt.breaks("%s of %s is not a member column", col, table)
	}
	if !countable(lt.gov.Rules(o.schema, o.name), member.Column) {
		return lt.breaks("a policy applies to %s", table)
	}
	snapshot, complete := lt.pinned(o.schema, o.name, member.Column)
	if !complete {
		return lt.breaks("%s's current snapshot lacks ids", table)
	}
	loc, ok := nodeAt(leaf)
	if !ok {
		return lt.breaks("cannot place %s in the text", table)
	}
	sp, ok := scanName(lt.text, loc)
	if !ok {
		return lt.breaks("cannot place %s in the text", table)
	}
	end, ok := afterTableRef(lt.text, leaf)
	if !ok {
		return lt.breaks("cannot place %s's alias in the text", table)
	}
	alias := str(leaf["alias"])
	if alias == "" {
		alias = str(leaf["table_name"])
	}
	lt.n++
	l := lineagePrefix + strconv.Itoa(lt.n)
	inner := quoteIdent(lineagePrefix + "t" + strconv.Itoa(lt.n))
	hidden := quoteIdent(iddomain.HiddenColumn(member.Column))
	lt.edits = append(lt.edits, edit{sp.start, end, "(SELECT " + inner + ".* EXCLUDE (" + hidden + "), CAST(" + inner + "." + hidden +
		" AS UINTEGER) AS " + quoteIdent(l) + " FROM " + quoteName([]string{o.db, o.schema, o.name}) + " AS " + inner +
		" AT (VERSION => " + strconv.FormatInt(snapshot, 10) + ")) AS " + quoteIdent(alias)})
	lt.wrapped[nodeKey(leaf)] = true
	lt.used[strings.ToLower(o.schema+"."+o.name)] = snapshot
	return l, true
}

// inline records the statement with the view o, which leaf reads, expanded
// in place, for users whose role reads the whole warehouse and where no
// policy applies to the view (see the top of this file).
func (lt *lineage) inline(leaf map[string]any, o object) {
	// A view of the catalog is read through its local copy (catalogView).
	if lt.inlined != "" || rank[lt.sess.currentUser().Role] < rank[auth.RoleReader] || !o.catalogView && !strings.EqualFold(o.db, lt.s.catalogName()) ||
		o.external || leaf["at_clause"] != nil || leaf["sample"] != nil || len(asList(leaf["column_name_alias"])) > 0 {
		lt.breaks("view %s.%s: the user's role, the view's database, or how the statement reads it", o.schema, o.name)
		return
	}
	if rules := lt.gov.Rules(o.schema, o.name); rules != nil {
		lt.breaks("a policy applies to view %s.%s", o.schema, o.name)
		return
	}
	cols, body, ok := viewParts(o.sql)
	if !ok {
		lt.breaks("cannot read view %s.%s's definition", o.schema, o.name)
		return
	}
	tree, err := lt.r.serialize(body)
	if err != nil {
		lt.breaks("cannot parse view %s.%s: %v", o.schema, o.name, err)
		return
	}
	stmt, _ := tree.(map[string]any)
	node, _ := stmt["node"].(map[string]any)
	if node == nil {
		node = stmt
	}
	withCTEs := hasNode(node, func(m map[string]any) bool {
		cm, _ := m["cte_map"].(map[string]any)
		return len(asList(cm["map"])) > 0
	})
	if str(node["type"]) != "SELECT_NODE" || withCTEs {
		lt.breaks("view %s.%s is not a SELECT without CTEs", o.schema, o.name)
		return
	}
	// The view's tables, named in full, so that they mean in the statement
	// what they mean in the view.
	var edits []edit
	failed := false
	walkMaps(node, func(t map[string]any) {
		if failed || str(t["type"]) != "BASE_TABLE" {
			return
		}
		parts := refParts(t)
		res, err := lt.r.resolve(tableRef{parts: parts}, &o)
		if err != nil || len(res.all) != 1 || len(res.first) != 1 || res.first[0].view {
			failed = true
			return
		}
		at, ok := nodeAt(t)
		if !ok {
			failed = true
			return
		}
		sp, ok := scanName(body, at)
		if !ok || !sameParts(sp.parts, parts) {
			failed = true
			return
		}
		f := res.first[0]
		full := quoteName([]string{f.db, f.schema, f.name})
		if str(t["alias"]) == "" {
			full += " AS " + quoteIdent(parts[len(parts)-1])
		}
		edits = append(edits, edit{sp.start, sp.end, full})
	})
	if failed {
		lt.breaks("cannot name view %s.%s's tables in full", o.schema, o.name)
		return
	}
	qualified, err := applyEdits(body, edits)
	if err != nil {
		return
	}
	loc, ok := nodeAt(leaf)
	if !ok {
		return
	}
	sp, ok := scanName(lt.text, loc)
	if !ok {
		return
	}
	end, ok := afterTableRef(lt.text, leaf)
	if !ok {
		return
	}
	alias := str(leaf["alias"])
	if alias == "" {
		alias = str(leaf["table_name"])
	}
	repl := "(" + strings.TrimRight(strings.TrimSpace(qualified), ";") + ") AS " + quoteIdent(alias)
	if len(cols) > 0 {
		quoted := make([]string, len(cols))
		for i, c := range cols {
			quoted[i] = quoteIdent(c)
		}
		repl += "(" + strings.Join(quoted, ", ") + ")"
	}
	lt.inlined = lt.text[:sp.start] + repl + lt.text[end:]
}

// leaks reports whether a column the steps add could show: a star, or a
// reference to a whole row, of a CTE on the way in a SELECT that is not
// a step carrying the ids through it, or of a counted relation in the
// SELECT that counts it.
func (lt *lineage) leaks(counting map[string]map[string]bool) bool {
	leak := false
	walkMaps(lt.root, func(m map[string]any) {
		if leak || str(m["type"]) != "SELECT_NODE" {
			return
		}
		var aliases []string
		fromRefs(m["from_table"], func(t map[string]any) {
			parts := refParts(t)
			name := str(t["alias"])
			if name == "" && len(parts) > 0 {
				name = parts[len(parts)-1]
			}
			if str(t["type"]) == "BASE_TABLE" && len(parts) == 1 && lt.chainCTEs[strings.ToLower(parts[0])] || counting[nodeKey(m)][strings.ToLower(name)] {
				aliases = append(aliases, strings.ToLower(name))
			}
		})
		if len(aliases) == 0 {
			return
		}
		carries := lt.starHops[nodeKey(m)] && len(counting[nodeKey(m)]) == 0
		walkOwn(m, func(n map[string]any) {
			switch str(n["class"]) {
			case "STAR":
				rel := strings.ToLower(str(n["relation_name"]))
				for _, a := range aliases {
					if (rel == "" || rel == a) && !carries {
						leak = true
					}
				}
			case "COLUMN_REF":
				if names := asList(n["column_names"]); len(names) == 1 {
					for _, a := range aliases {
						if strings.ToLower(str(names[0])) == a {
							leak = true
						}
					}
				}
			}
		})
	})
	return leak
}

// refParts returns the parts of a table reference's name.
func refParts(t map[string]any) []string {
	var parts []string
	for _, k := range []string{"catalog_name", "schema_name", "table_name"} {
		if v := str(t[k]); v != "" {
			parts = append(parts, v)
		}
	}
	return parts
}
