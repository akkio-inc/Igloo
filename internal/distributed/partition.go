package distributed

import (
	"slices"
	"strings"
)

// Partitioned joins. When the split table is joined to a second table too
// large for every worker to read, both are partitioned by a hash of their
// join key instead, through object storage:
//
//  1. Workers read shares of each table's files, keep the rows the
//     statement's filters on that table alone let through, and write the
//     columns the statement uses to <warehouse>/_igloo_shuffle/..., one
//     directory per partition (COPY ... PARTITION_BY).
//  2. One task per partition then runs the job's fragment, unchanged, with
//     the two tables' views defined over that partition's files.
//
// Every row of either table that can join a row of the other has the same
// key, so lands in the same partition: each task sees every pair of rows the
// join can produce, once, and the partial results combine as for any job.
// In stages (stage.go), that is a stage per table writing an exchange
// keyed by its side of the join key, and a stage reading both by partition.

// partitionPlan is how a candidate's two largest tables are partitioned.
type partitionPlan struct {
	other *tableUse
	// keys pair the split table's key columns with the other's.
	keys [][2]string
	// casts are the types both sides of each key are hashed as, or "".
	casts []string
}

// partition says how a job partitions its two largest tables.
type partition struct {
	// sides are the split table and the other.
	sides [2]side
}

// side is one of a partitioned job's tables.
type side struct {
	// view is the name the fragment reads the table as.
	view  string
	table *Table
	// stage selects the rows to partition: the columns the statement uses,
	// after the statement's filters on this table alone.
	stage node
	// keys are its join key columns, hashed as casts say.
	keys, casts []string
}

// partition plans a partitioned join of the candidate's split table and
// other, which must be joined to it directly on equal columns.
func (pl *planner) partition(c *candidate, other *tableUse) (*partitionPlan, error) {
	f := c.split
	if !other.splittable || other.scope == nil || other.scope != f.scope {
		return nil, unsupported("%s and %s, both too large to read in full, are not joined directly", f.table, other.table)
	}
	for _, u := range c.uses {
		if u != other && u.table.key() == other.table.key() {
			return nil, unsupported("%s is read more than once", other.table)
		}
	}
	for _, u := range []*tableUse{f, other} {
		if len(list(u.ref, "column_name_alias")) > 0 {
			return nil, unsupported("%s with renamed columns", u.table)
		}
	}
	p := &partitionPlan{other: other}
	for _, cond := range f.scope.conds {
		if str(cond["class"]) != "COMPARISON" || str(cond["type"]) != "COMPARE_EQUAL" {
			continue
		}
		l, r := child(cond, "left"), child(cond, "right")
		if str(l["class"]) != "COLUMN_REF" || str(r["class"]) != "COLUMN_REF" {
			continue
		}
		li, ri := resolveColumn(l, f.scope), resolveColumn(r, f.scope)
		ln, rn := lastName(l), lastName(r)
		switch {
		case li == f.item && ri == other.item:
			p.keys = append(p.keys, [2]string{ln, rn})
		case li == other.item && ri == f.item:
			p.keys = append(p.keys, [2]string{rn, ln})
		}
	}
	if len(p.keys) == 0 {
		return nil, unsupported("%s and %s, both too large to read in full, are not joined on equal columns", f.table, other.table)
	}
	for _, k := range p.keys {
		tf, tg := columnType(f.table, k[0]), columnType(other.table, k[1])
		cast, ok := hashType(tf, tg)
		if !ok {
			return nil, unsupported("join key %s (%s) = %s (%s) of different types", k[0], tf, k[1], tg)
		}
		if strings.HasPrefix(tf, "VARCHAR") {
			// Values equal under a collation do not hash alike.
			collation, err := pl.collation()
			if err != nil {
				return nil, err
			}
			if collation != "" {
				return nil, unsupported("a text join key under collation %s", collation)
			}
		}
		p.casts = append(p.casts, cast)
	}
	return p, nil
}

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

// hashType returns the type both sides of a join key are hashed as, ""
// when they already share one. DuckDB hashes the integer types by value,
// so they mix; other types must match.
func hashType(a, b string) (string, bool) {
	ints := []string{"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER"}
	switch {
	case a == "" || b == "":
		return "", false
	case a == b:
		return "", true
	case slices.Contains(ints, a) && slices.Contains(ints, b):
		return "BIGINT", true
	}
	return "", false
}

// sides builds the queries that select each partitioned table's rows.
func (pl *planner) sides(c *candidate) (*partition, error) {
	p := c.partition
	refs := referencedNames(c.sel)
	part := &partition{}
	for i, u := range []*tableUse{c.split, p.other} {
		view := str(u.ref[viewMark])
		var keys []string
		for _, pair := range p.keys {
			keys = append(keys, pair[i])
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
		stage, err := pl.p.parse("SELECT " + strings.Join(cols, ", ") + " FROM " + quoteIdent(view) + " AS " + quoteIdent(alias))
		if err != nil {
			return nil, err
		}
		// The statement's filters on this table alone, so less crosses.
		var filters []any
		for _, cond := range c.split.scope.conds {
			if onlyColumnsOf(cond, u.item, c.split.scope) {
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
				return nil, err
			}
			w := child(and, "where_clause")
			w["children"] = filters
			stage["where_clause"] = w
		}
		part.sides[i] = side{view: view, table: u.table, stage: stage, keys: keys, casts: p.casts}
	}
	return part, nil
}

// referencedNames returns every name a column reference in n uses, lower
// case, or nil when a * or a NATURAL join may use any column.
func referencedNames(n node) map[string]bool {
	names := map[string]bool{}
	all := false
	walk(n, func(m node) bool {
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
