package distributed

import (
	"slices"
	"strings"
)

// Partial results a row each. A stage that writes an aggregation's partial
// results for a stage that completes its groups groups its rows first, which
// saves rows only when its groups are fewer than its rows. Grouped by an id, or
// by an id and a day of exploded rows, they are about as many: grouping then
// builds a hash table of every row, which spills, and writes as many rows as it
// read. A report's stage over 10.6 billion exploded rows took 212 s to group
// them into as many groups, and 132 s to write them as they were, with 78 GB
// less spilled.
//
// So such a stage may instead write each row's own partial results, as the
// partial results of a group of that one row, with no GROUP BY: the completing
// stage combines either kind the same, and a task may write either. Grouped,
// tasks count each group's rows (groupRowsColumn). Each worker chooses for its
// tasks of the stage: the first probes a sample of its rows, and a task that
// finds every group of one row has the next ones write a row at a time too (see
// rowprobe.go). A stage whose groups are known to be about as many as its
// rows, by the distinct counts of the one table it reads, starts a row at a
// time everywhere.

// groupRowsColumn is the column of an aggregation's partial results that counts each
// group's rows: 1 for each when a task writes them a row at a time.
const groupRowsColumn = "__igloo_n"

// Test overrides of how a stage writes partial results (planOptions.partials):
// a row at a time from the start wherever it may, or never.
const (
	partialsByRow   = "rows"
	partialsGrouped = "grouped"
)

// rowSpec is how a stage that writes an aggregation's partial results may write
// them a row at a time. types are the partial results' types, by column, which a
// row's own are cast to, so that either way the stage writes the same columns;
// first says to start a row at a time.
type rowSpec struct {
	types map[string]string
	first bool
}

// rowAggregates are the aggregates whose partial result over one row is its
// argument: NULL when the row's FILTER drops it, which each of them leaves out.
var rowAggregates = map[string]bool{
	"min": true, "max": true, "sum": true, "fsum": true, "sumkahan": true, "kahan_sum": true, "any_value": true,
	"bool_and": true, "bool_or": true, "bit_and": true, "bit_or": true, "bit_xor": true,
}

// rowPartial is a partial aggregate call's result over its row alone, cast to typ
// unless typ is "": its argument for rowAggregates, 1 or 0 for counts. It fails
// for any other call, and for one over DISTINCT values or in an order.
func rowPartial(p *parser, call node, typ string) (node, error) {
	name := strings.ToLower(str(call["function_name"]))
	args := list(call, "children")
	if str(call["class"]) != "FUNCTION" || call["distinct"] == true || len(list(child(call, "order_bys"), "orders")) > 0 {
		return nil, unsupported("%s over one row", name)
	}
	filter := child(call, "filter")
	var text string
	switch {
	case rowAggregates[name] && len(args) == 1:
		text = "__igloo_x"
		if filter != nil {
			text = "CASE WHEN __igloo_c THEN __igloo_x END"
		}
	case name == "count" && len(args) == 1:
		text = "CASE WHEN __igloo_x IS NOT NULL THEN 1 ELSE 0 END"
		if filter != nil {
			text = "CASE WHEN __igloo_c AND __igloo_x IS NOT NULL THEN 1 ELSE 0 END"
		}
	case (name == "count" || name == "count_star") && len(args) == 0:
		text = "1"
		if filter != nil {
			text = "CASE WHEN __igloo_c THEN 1 ELSE 0 END"
		}
	case (name == "count_if" || name == "countif") && len(args) == 1:
		text = "CASE WHEN __igloo_x THEN 1 ELSE 0 END"
		if filter != nil {
			text = "CASE WHEN __igloo_c AND __igloo_x THEN 1 ELSE 0 END"
		}
	default:
		return nil, unsupported("%s over one row", name)
	}
	if typ != "" {
		text = "CAST((" + text + ") AS " + typ + ")"
	}
	e, err := p.expr(text)
	if err != nil {
		return nil, err
	}
	return rewrite(e, func(n node) (node, bool) {
		if str(n["class"]) != "COLUMN_REF" {
			return nil, false
		}
		switch names := columnNames(n); {
		case slices.Equal(names, []string{"__igloo_x"}):
			return clone(args[0]).(node), true
		case slices.Equal(names, []string{"__igloo_c"}):
			return clone(filter).(node), true
		}
		return nil, false
	}).(node), nil
}

// rows returns how f, the fragment that writes the aggregation's partial results
// for a stage that completes its groups, may write them a row at a time, and adds
// groupRowsColumn to it; or nil when one of its partial aggregates has no result over
// one row (see rowPartial), or it groups by nothing.
func (a *aggregation) rows(f node) (*rowSpec, error) {
	if len(a.groups) == 0 || len(a.distincts) > 0 || !a.singleSet() || a.pl.opts.partials == partialsGrouped {
		return nil, nil
	}
	for _, p := range a.partials {
		if _, err := rowPartial(a.pl.p, p, ""); err != nil {
			return nil, nil
		}
	}
	n, err := a.pl.p.expr("count(*)")
	if err != nil {
		return nil, err
	}
	items := list(f, "select_list")
	f["select_list"] = append(slices.Clip(items), aliased(n, groupRowsColumn))
	// The types of the partial results, as the session binds them.
	cols, err := a.pl.describe(unmarked(f).(node), a.c.ctes)
	if err != nil {
		f["select_list"] = items
		return nil, nil
	}
	types := map[string]string{}
	for _, c := range cols {
		types[c.Name] = c.Type
	}
	return &rowSpec{types: types, first: a.pl.opts.partials == partialsByRow || a.asManyGroups()}, nil
}

// asManyGroups reports whether the aggregation's groups are known to be about as
// many as its rows: it reads one table, with no joins, and the distinct counts of
// its group expressions' columns, by ANALYZE or the table's sketches, multiply
// to half its rows or more.
func (a *aggregation) asManyGroups() bool {
	if a.c.split == nil || len(a.c.uses) != 1 || len(a.c.rels) > 0 || len(a.c.keyed) > 0 || len(a.c.whole) > 0 {
		return false
	}
	t := a.c.split.table
	if t == nil || t.Rows <= 0 || a.c.split.scope != a.c.scope {
		return false
	}
	tables := map[*fromItem]*Table{a.c.split.item: t}
	groups := 1.0
	for _, g := range a.groups {
		n, ok := groupValues(g, a.c.scope, nil, tables)
		if !ok {
			return false
		}
		groups *= n
	}
	return 2*groups >= float64(t.Rows)
}

// rowFragment is f, a stage's fragment that writes an aggregation's partial
// results (see rowSpec), writing each row's own instead, with no GROUP BY; or nil
// when f is not of that shape.
func (pl *planner) rowFragment(f node, spec *rowSpec) (node, error) {
	if len(list(f, "group_expressions")) == 0 || child(f, "having") != nil || child(f, "qualify") != nil || len(list(f, "modifiers")) > 0 {
		return nil, nil
	}
	c := clone(f).(node)
	items := list(c, "select_list")
	counted := false
	for i, item := range items {
		e := item.(node)
		alias := str(e["alias"])
		switch {
		case alias == groupRowsColumn:
			one, err := pl.p.expr("CAST(1 AS BIGINT)")
			if err != nil {
				return nil, err
			}
			items[i], counted = aliased(one, groupRowsColumn), true
		case strings.HasPrefix(alias, "__a"):
			typ := spec.types[alias]
			if typ == "" {
				return nil, nil
			}
			r, err := rowPartial(pl.p, e, typ)
			if err != nil {
				return nil, nil
			}
			items[i] = aliased(r, alias)
		case strings.HasPrefix(alias, "__g"):
		default:
			return nil, nil
		}
	}
	if !counted {
		return nil, nil
	}
	c["group_expressions"] = []any{}
	c["group_sets"] = []any{}
	return c, nil
}
