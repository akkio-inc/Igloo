package distributed

import (
	"slices"
	"strings"
)

// Statements that masking and row access policies apply to. pgwire's rewrite
// reads each protected table through a subquery of its own, which masks its
// columns and leaves out the rows its row access policy hides, and the
// policies read their own tables through subqueries of their own (see
// pgwire's policyrewrite.go): with masks alone,
//
//	(SELECT mask(t.a) AS a, ... FROM lake.s.t AS "__igloo_t_<mark>_1") AS x
//
// and with a row access policy,
//
//	(SELECT CASE WHEN t.ok THEN mask(t.a) END AS a, ...
//	 FROM (SELECT t.a, ..., (policy) AS ok FROM lake.s.t AS t) AS t WHERE t.ok) AS x
//
// where t is "__igloo_t_<mark>_1". Outside that subquery, the statement sees
// only its columns: masked values of the rows the policy keeps. A stage
// whose every read of such a table is through the subquery as the rewrite
// wrote it, from the table's files, therefore writes nothing else to an
// exchange or to the coordinator, whatever it computes from them; and the
// tables the policies read are read only inside the subqueries too, for
// what the policies make of them.
//
// That is what the planner checks of the stages it plans (governedReads),
// for each job and for the plan. A job whose stages would read a protected
// table otherwise, as one that partitions the table's rows by a key would
// (see sides), or that cannot divide or partition the rows within the
// subquery, as a window over them or a join of the table to itself cannot,
// is planned again with each such subquery in it as a relation (forceRoots):
// its stages compute the subquery, masks and filter included, and exchange
// the rows it makes. If its stages still read one otherwise, that part of
// the statement runs on the coordinator, and so does a statement whose plan
// does.

// Governance is an Env whose statement masking and row access policies
// rewrote (see above).
type Governance interface {
	// RewriteMark returns the rewrite's mark: "__igloo_t_" + mark + "_"
	// begins the aliases of the protected tables in its subqueries,
	// "__igloo_p_" + mark + "_" those of the tables its policies read, and no
	// other name of the statement holds it. Empty when no policy applies.
	RewriteMark() string
}

// rewriteMark returns the mark of the policies' rewrite of env's statement,
// lower case, or "".
func rewriteMark(env Env) string {
	if g, ok := env.(Governance); ok {
		return strings.ToLower(g.RewriteMark())
	}
	return ""
}

// governance is what the planner knows of a statement's policies.
type governance struct {
	mark string
	// tables and bodies begin the aliases of the protected tables and of the
	// tables the policies read, lower case.
	tables, bodies string
	// roots are the subqueries of the protected tables, by their tables'
	// aliases, as the rewrite wrote them (see canonicalRoot); "" for one the
	// statement holds in more than one form.
	roots map[string]string
}

// newGovernance records the subqueries of the protected tables of stmt, a
// statement env's policies rewrote, or returns nil when none apply.
func (pl *planner) newGovernance(stmt node) *governance {
	mark := rewriteMark(pl.env)
	if mark == "" {
		return nil
	}
	gov := &governance{mark: mark, tables: "__igloo_t_" + mark + "_", bodies: "__igloo_p_" + mark + "_", roots: map[string]string{}}
	visit(stmt, nil, func(n node, path []node) {
		if str(n["type"]) != "BASE_TABLE" {
			return
		}
		alias := strings.ToLower(str(n["alias"]))
		if !strings.HasPrefix(alias, gov.tables) {
			return
		}
		root := gov.root(path, alias)
		text := ""
		if root != nil {
			text = gov.canonicalRoot(root, pl.originalTable)
		}
		if seen, ok := gov.roots[alias]; ok && seen != text {
			text = ""
		}
		gov.roots[alias] = text
	})
	return gov
}

// originalTable names the table a reference of the statement as written
// reads, for canonicalRoot.
func (pl *planner) originalTable(ref node) string {
	parts := tableParts(ref)
	if t, ok, err := pl.lookupTable(parts); err == nil && ok {
		return "table:" + t.key()
	}
	return "name:" + strings.ToLower(strings.Join(parts, "."))
}

// rootAlias returns the alias of the protected table s reads, when s is
// shaped as a subquery of the rewrite's (see above): it reads the table, or
// the subquery that filters it, under that alias.
func (g *governance) rootAlias(s node) string {
	if str(s["type"]) != "SELECT_NODE" {
		return ""
	}
	from := child(s, "from_table")
	alias := strings.ToLower(str(from["alias"]))
	if !strings.HasPrefix(alias, g.tables) {
		return ""
	}
	switch str(from["type"]) {
	case "BASE_TABLE":
		return alias
	case "SUBQUERY":
		if f := child(subqueryNode(from), "from_table"); str(f["type"]) == "BASE_TABLE" && strings.EqualFold(str(f["alias"]), alias) {
			return alias
		}
	}
	return ""
}

// root returns the outermost of the nodes of path, which a reference is in,
// shaped as the subquery of the protected table of alias.
func (g *governance) root(path []node, alias string) node {
	for _, n := range path {
		if g.rootAlias(n) == alias {
			return n
		}
	}
	return nil
}

// canonicalRoot is the text that tells subqueries of the rewrite's apart: n
// without positions or the planner's marks, its tables known by what reads
// them (table:schema.name for a catalog table's files), as the planner
// points them at the views of its stages, and without the names a relation
// gives its subquery's columns when they are the columns' own (see
// wrapper.apply).
func (g *governance) canonicalRoot(n node, table func(ref node) string) string {
	c := rewrite(clone(n), func(m node) (node, bool) {
		for k := range m {
			if strings.HasPrefix(k, "__igloo_") {
				delete(m, k)
			}
		}
		switch str(m["type"]) {
		case "BASE_TABLE":
			out := clone(m).(node)
			out["table_name"] = table(m)
			delete(out, "schema_name")
			delete(out, "catalog_name")
			// The snapshot it reads is the one the plan pins (see pinVersion).
			delete(out, "at_clause")
			return out, true
		case "SUBQUERY":
			if ownNames(m) {
				m["column_name_alias"] = []any{}
			}
		}
		if mp := child(m, "cte_map"); mp != nil && len(list(mp, "map")) == 0 {
			delete(m, "cte_map")
		}
		return nil, false
	})
	return canonical(c)
}

// ownNames reports whether ref, a subquery in FROM, names its columns as its
// SELECT does.
func ownNames(ref node) bool {
	names := list(ref, "column_name_alias")
	if len(names) == 0 {
		return true
	}
	sel := subqueryNode(ref)
	items := list(sel, "select_list")
	if str(sel["type"]) != "SELECT_NODE" || len(items) != len(names) {
		return false
	}
	for i, it := range items {
		e, _ := it.(node)
		name := str(e["alias"])
		if name == "" && str(e["class"]) == "COLUMN_REF" {
			if cols := list(e, "column_names"); len(cols) > 0 {
				name = str(cols[len(cols)-1])
			}
		}
		if !strings.EqualFold(name, str(names[i])) {
			return false
		}
	}
	return true
}

// mentions reports whether v reads a protected table or a policy's table.
func (g *governance) mentions(v any) bool {
	found := false
	walk(v, func(n node) bool {
		if str(n["type"]) == "BASE_TABLE" && g.ours(str(n["alias"])) {
			found = true
		}
		return !found
	})
	return found
}

func (g *governance) ours(alias string) bool {
	a := strings.ToLower(alias)
	return strings.HasPrefix(a, g.tables) || strings.HasPrefix(a, g.bodies)
}

// governedReads returns the aliases of the protected tables that stages read
// otherwise than through their subqueries as the rewrite wrote them, from
// their files, or whose policies' tables they read otherwise; "" stands for
// a protected table read under another alias.
func (pl *planner) governedReads(stages []*Stage) []string {
	g := pl.gov
	if g == nil {
		return nil
	}
	var bad []string
	fail := func(alias string) {
		if !slices.Contains(bad, alias) {
			bad = append(bad, alias)
		}
	}
	for _, s := range stages {
		views := map[string]*Table{}
		for _, in := range s.Inputs {
			if in.Table != nil {
				views[strings.ToLower(in.View)] = in.Table
			}
		}
		table := func(ref node) string {
			if str(ref["schema_name"]) == "" && str(ref["catalog_name"]) == "" {
				if t := views[strings.ToLower(str(ref["table_name"]))]; t != nil {
					return "table:" + t.key()
				}
			}
			return "name:" + strings.ToLower(strings.Join(tableParts(ref), "."))
		}
		checked := map[uintptr]bool{}
		visit(s.fragment, nil, func(n node, path []node) {
			if str(n["type"]) != "BASE_TABLE" {
				return
			}
			alias := strings.ToLower(str(n["alias"]))
			if !g.ours(alias) {
				if t := views[strings.ToLower(str(n["table_name"]))]; t != nil && t.Governed && str(n["schema_name"]) == "" {
					fail("")
				}
				return
			}
			if !strings.HasPrefix(alias, g.tables) {
				// A table a policy reads: in the subquery of the protected
				// table whose policy it is, the innermost around it.
				alias = ""
				for i := len(path) - 1; i >= 0 && alias == ""; i-- {
					alias = g.rootAlias(path[i])
				}
				if alias == "" {
					fail("")
					return
				}
			}
			root := g.root(path, alias)
			if root == nil {
				fail(alias)
				return
			}
			if checked[identity(root)] {
				return
			}
			want, ok := g.roots[alias]
			if !ok || want == "" || g.canonicalRoot(root, table) != want {
				fail(alias)
				return
			}
			checked[identity(root)] = true
		})
	}
	return bad
}

// forceRoots marks the subqueries of the protected tables in sel, each a
// FROM item of its own, to be read as relations (see forced), and reports
// whether it marked any that were not.
func (pl *planner) forceRoots(sel node) bool {
	g := pl.gov
	marked := false
	walk(sel, func(n node) bool {
		if str(n["type"]) != "SUBQUERY" || g.ours(str(n["alias"])) || g.rootAlias(subqueryNode(n)) == "" || pl.forced[identity(n)] {
			return true
		}
		if pl.forced == nil {
			pl.forced = map[uintptr]bool{}
		}
		pl.forced[identity(n)] = true
		marked = true
		return true
	})
	return marked
}

// visit calls fn for every node under v with the nodes it is in, outermost
// first.
func visit(v any, path []node, fn func(n node, path []node)) {
	switch t := v.(type) {
	case node:
		fn(t, path)
		path = append(path, t)
		for _, c := range t {
			visit(c, path, fn)
		}
	case []any:
		for _, c := range t {
			visit(c, path, fn)
		}
	}
}

// withoutMark is text with the rewrite's mark left out, for keys that must
// be the same from one run of a statement to the next, as its plan key is:
// the mark is new each time the policies rewrite it.
func (pl *planner) withoutMark(text string) string {
	if pl.gov == nil {
		return text
	}
	return strings.ReplaceAll(text, pl.gov.mark, "")
}
