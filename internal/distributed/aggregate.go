package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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
	// distincts are the arguments of the aggregates over DISTINCT values,
	// by canonical form. Unless the stage's partitions keep each value
	// apart (aggInPlace), workers group by them too, as __dk columns.
	distincts    []node
	distinctKeys []string
	// partials are the fragment's partial aggregate expressions, as __a
	// columns. dk[i] is, for a partial over the DISTINCT values of an
	// argument, that argument's index in distincts; -1 for the others.
	partials []node
	dk       []int
	// mode says how the groups are completed (see the agg* constants).
	mode string
	// final says the coordinator reads whole groups, as __f columns,
	// rather than partial aggregates: one row a group, which its combine
	// step projects rather than groups again (see combine).
	final bool
	// regroup says the partial results of a stage that computes the
	// aggregates over DISTINCT values (aggInPlace, aggByValue), a row a
	// group and partition, go to an exchange keyed by the groups too, and
	// a last stage completes the groups (groupStage), so that the
	// coordinator reads one row a group.
	regroup bool
	// staged are the select items a second stage that completes the
	// groups (aggByGroup) computes too, as __s columns: those that call
	// a group function (see GroupFunctions).
	staged map[int]bool
}

// aggCall is one aggregate call and how it is computed in two steps.
type aggCall struct {
	call node
	// name is the function's name, lower case.
	name string
	// distinct is the index of its argument in distincts, for an aggregate
	// over DISTINCT values; -1 otherwise.
	distinct int
	// partials are the indexes of its partial aggregates.
	partials []int
	// kind says how the coordinator combines them.
	kind string
	// typ is the call's result type, for casting the combined value back.
	typ string
	// final is the index of its __f column, when a stage computes it whole.
	final int
	// whole, if set, says why it cannot be computed in parts, only by a
	// stage that holds whole groups.
	whole error
	// bitmap says it counts DISTINCT values in roaring bitmaps (see
	// combineBitmap), whose argument is not among distincts.
	bitmap bool
}

// How an aggregation's groups are completed. Workers always aggregate their
// share of the rows first, into partial results.
const (
	// aggGather sends the partial results to the coordinator, which
	// combines them; aggregates over DISTINCT values it computes over the
	// values the workers grouped their rows by.
	aggGather = "gather"
	// aggInPlace is for a stage whose partitions keep each value of every
	// DISTINCT argument in one partition, as a partitioned join on that
	// value does: aggregates over DISTINCT values that add up (count, sum,
	// min, max, ...) are computed by the stage, and the coordinator adds
	// up their partitions' results like any partial aggregates.
	aggInPlace = "in place"
	// aggByValue writes the partial results, grouped by the DISTINCT
	// arguments too, to an exchange keyed by the groups and a DISTINCT
	// argument that fixes the others; a stage then aggregates each
	// partition as in aggInPlace.
	aggByValue = "by value"
	// aggByGroup writes the partial results to an exchange keyed by the
	// groups; a stage then completes the groups of each partition, every
	// aggregate included, and the coordinator only concatenates them.
	aggByGroup = "by group"
	// aggComplete is for a stage whose partitions hold whole groups, as one
	// keyed by the group columns does: the stage computes every aggregate
	// of its groups, any aggregate at all, and the coordinator only
	// concatenates them.
	aggComplete = "complete"
)

// additiveDistinct are the aggregates over DISTINCT values that combine
// over disjoint sets of values, by the function in combinable (avg by its
// sum and count).
var additiveDistinct = map[string]bool{
	"count": true, "sum": true, "min": true, "max": true, "avg": true, "mean": true,
	"bool_and": true, "bool_or": true, "bit_and": true, "bit_or": true, "bit_xor": true,
	"any_value": true, "product": true, "fsum": true, "sumkahan": true, "kahan_sum": true,
}

// aggregateJob builds a job for a candidate that aggregates: workers group
// and aggregate their rows; the coordinator, or a second stage, completes
// the groups.
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
	a.ownTopK()
	if err := a.choose(); err != nil {
		return nil, err
	}
	if err := a.plan(); err != nil {
		return nil, err
	}
	if err := a.types(); err != nil {
		return nil, err
	}
	fragment, err := a.fragment()
	if err != nil {
		return nil, err
	}
	b := &jobBuild{fragment: fragment}
	switch a.mode {
	case aggByValue:
		b.second, err = a.valueStage()
		if err == nil && a.regroup {
			// The value stage's rows, a group's partial results a partition,
			// completed by group.
			b.third, err = a.groupStage()
		}
	case aggInPlace:
		if a.regroup {
			b.second, err = a.groupStage()
		}
	case aggByGroup:
		b.second, err = a.groupStage()
	}
	if err == nil && b.second != nil && !b.second.whole {
		b.rows, err = a.rows(b.fragment)
	}
	if err != nil {
		return nil, err
	}
	if b.combine, err = a.combine(); err != nil {
		return nil, err
	}
	if c.top {
		if keys, order := a.mergeOrder(); keys != nil {
			// The stage that sends the coordinator whole groups sorts them
			// task by task, and the coordinator merges them (see merge.go).
			last := &b.fragment
			if b.third != nil {
				last = &b.third.fragment
			} else if b.second != nil {
				last = &b.second.fragment
			}
			(*last)["modifiers"] = []any{order}
			b.merge = keys
		}
	}
	return b, nil
}

// relationAggregate builds the stages of a relation that aggregates. When
// its stage's partitions hold whole groups, the stage computes the
// relation as written; otherwise the partial results go to an exchange
// keyed by the groups, and a second stage runs the combine step on each
// partition.
func (pl *planner) relationAggregate(c *candidate) (*jobBuild, error) {
	var raw []node
	for _, g := range list(c.sel, "group_expressions") {
		raw = append(raw, g.(node))
	}
	if len(list(c.sel, "group_sets")) <= 1 && c.wholeGroups(raw) {
		f := clone(c.sel).(node)
		f["cte_map"] = node{"map": []any{}}
		// The rows a statement ranks and keeps the first of (see markTopK).
		if keep := child(c.sel, topKMark); keep != nil && child(f, "qualify") == nil {
			f["qualify"] = clone(keep)
		}
		return &jobBuild{fragment: f}, nil
	}
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
	for _, step := range []func() error{a.groupBy, a.collect, a.choose, a.plan, a.types} {
		if err := step(); err != nil {
			return nil, err
		}
	}
	fragment, err := a.fragment()
	if err != nil {
		return nil, err
	}
	combine, err := a.combine()
	if err != nil {
		return nil, err
	}
	combine["from_table"], err = a.pl.p.fromRef("SELECT 1 FROM " + quoteIdent(partialsView))
	if err != nil {
		return nil, err
	}
	if q := qualifyTopK(c.sel, func(e node) (node, error) { return a.rewrite(e, "select") }); q != nil {
		combine["qualify"] = q
	}
	// Without groups, or of grouping sets, whose groups no partition holds
	// whole: its one task computes them over every partial result.
	b := &jobBuild{fragment: fragment, second: &secondStage{fragment: combine, keys: a.groupColumns(), whole: len(a.groups) == 0 || !a.singleSet()}}
	if !b.second.whole {
		if b.rows, err = a.rows(fragment); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// singleSet reports whether the aggregation groups by all of its groups, once:
// no grouping sets.
func (a *aggregation) singleSet() bool {
	return len(a.sets) == 0 || (len(a.sets) == 1 && len(a.sets[0].([]any)) == len(a.groups))
}

// wholeGroups reports whether the partitions the candidate's stage reads
// hold whole groups: the group expressions' values fix the key of one of
// its inputs read by partition.
func (c *candidate) wholeGroups(groups []node) bool {
	if len(c.keyed) == 0 {
		return false
	}
	var fixed []itemColumn
	for _, g := range groups {
		fixed = append(fixed, determined(g, c.scope)...)
	}
	return len(fixed) > 0 && c.fixesKey(fixed)
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
	// combineBitmap counts the values of the union of roaring bitmaps of
	// them: a count(DISTINCT) of 32-bit unsigned IDs, whose workers each
	// send a bitmap of their share's values a group (rb_agg) rather than
	// every value, when the rb extension is loaded and bitmaps are on (see
	// planOptions). The bitmap is CRoaring's portable serialization, a
	// BLOB, the same on every machine.
	combineBitmap = "bitmap"
	// combineList concatenates the lists of list() and array_agg(), whose
	// order is arbitrary without ORDER BY.
	combineList = "list"
	// combineMoments merges each part's count, mean and sum of squared
	// deviations from it, pairwise (Chan, Golub and LeVeque's update), as
	// DuckDB merges its threads' for a variance or standard deviation.
	// Over sums of values and of their squares, the variance of values near
	// a large mean would lose its digits.
	combineMoments = "moments"
)

// moments are the variances and standard deviations, which combine by
// their parts' moments (combineMoments), to whether they are of a sample.
var moments = map[string]bool{
	"var_samp": true, "variance": true, "stddev_samp": true, "stddev": true, "var_pop": false, "stddev_pop": false,
}

// isMoments reports whether an aggregate is one of moments.
func isMoments(name string) bool {
	_, ok := moments[name]
	return ok
}

// mergedMoments is the merge of the parts' moments of combineMoments, of
// partial results n (a count), m (a mean) and v (a population variance):
// a struct of n, the count, and q, the sum of squared deviations. NULL
// where no part counted a value. Its fields are read by struct_extract:
// DuckDB binds a lambda's __x.n in HAVING as a column n.
func mergedMoments(n, m, v string) string {
	f := func(s, field string) string { return "struct_extract(" + s + ", '" + field + "')" }
	xn, xm, xq, yn, ym, yq := f("__x", "n"), f("__x", "m"), f("__x", "q"), f("__y", "n"), f("__y", "m"), f("__y", "q")
	d := "(" + ym + " - " + xm + ")"
	return "list_reduce(list({'n': CAST(" + n + " AS DOUBLE), 'm': " + m + ", 'q': " + v + " * " + n + "}) FILTER (WHERE " + n + " > 0), " +
		"lambda __x, __y: {'n': " + xn + " + " + yn + ", 'm': " + xm + " + " + d + " * " + yn + " / (" + xn + " + " + yn + "), " +
		"'q': " + xq + " + " + yq + " + " + d + " * " + d + " * " + xn + " * " + yn + " / (" + xn + " + " + yn + ")})"
}

// collecting are the aggregates that collect their values in a list,
// which partial lists concatenate into (combineList).
var collecting = map[string]bool{"list": true, "array_agg": true}

// combineLists is the expression that concatenates partial lists: NULL,
// as list() over no values, when every partial is NULL.
func combineLists(col string) string {
	return "flatten(list(" + col + ") FILTER (WHERE " + col + " IS NOT NULL))"
}

// combinable maps the aggregates that combine by applying another
// aggregate to their partial results to that aggregate.
var combinable = map[string]string{
	"sum": "sum", "min": "min", "max": "max", "bool_and": "bool_and", "bool_or": "bool_or",
	"bit_and": "bit_and", "bit_or": "bit_or", "bit_xor": "bit_xor", "any_value": "any_value",
	"fsum": "fsum", "sumkahan": "fsum", "kahan_sum": "fsum", "product": "product",
	"count": "sum", "count_star": "sum", "count_if": "sum", "countif": "sum",
	// Roaring bitmaps: a union of unions.
	"rb_agg": "rb_or_agg", "rb_or_agg": "rb_or_agg",
	// HLL sketches (the datasketches extension, as APPROX_COUNT_DISTINCT
	// and HLL_ACCUMULATE count): a union of sketches of the same size, k,
	// the first argument (see sketchCall).
	"datasketch_hll": "datasketch_hll_union", "datasketch_hll_union": "datasketch_hll_union",
}

// sketches are the aggregates of combinable whose first argument is the
// sketch's size, which their combination takes first too.
var sketches = map[string]bool{"datasketch_hll": true, "datasketch_hll_union": true}

// sketchCall is the call that combines partial sketches, col, of call's
// size: datasketch_hll_union(k, col). Unions of sketches of one size
// estimate as a sketch of all their values does, within its error.
func (pl *planner) sketchCall(call node, col string) (node, error) {
	ref, err := pl.p.expr(quoteIdent(col))
	if err != nil {
		return nil, err
	}
	c := clone(call).(node)
	c["function_name"] = combinable[strings.ToLower(str(call["function_name"]))]
	c["children"] = []any{clone(list(call, "children")[0]), ref}
	c["alias"], c["filter"] = "", nil
	return c, nil
}

// collect finds the aggregate calls of the parts the coordinator evaluates
// and works out how to compute each in two steps.
func (a *aggregation) collect() error {
	sel := a.c.sel
	var err error
	visit := func(v any) {
		walk(v, func(n node) bool {
			if str(n["class"]) == "SUBQUERY" {
				// Its own aggregates, computed with it (see rewrite).
				return false
			}
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

// nullsLeftOut are the aggregates that leave NULL values out, with
// DISTINCT as without.
var nullsLeftOut = map[string]bool{"count": true, "sum": true, "avg": true, "min": true, "max": true}

// distinctFilter writes agg(DISTINCT x) FILTER (WHERE c), of an aggregate
// that leaves NULLs out, as agg(DISTINCT CASE WHEN c THEN x END), which is
// the same: the values the filter drops become NULLs. A report counts
// each group's people among the rows it keeps so; a DISTINCT aggregate
// with a FILTER does not combine across workers.
func (a *aggregation) distinctFilter(call node, name string) error {
	args := list(call, "children")
	if call["distinct"] != true || child(call, "filter") == nil || len(args) != 1 || !nullsLeftOut[name] {
		return nil
	}
	c, err := a.pl.p.expr("CASE WHEN true THEN NULL END")
	if err != nil {
		return err
	}
	checks := list(c, "case_checks")
	if len(checks) != 1 {
		return fmt.Errorf("CASE parsed with %d branches", len(checks))
	}
	check := checks[0].(node)
	check["when_expr"], check["then_expr"] = call["filter"], args[0]
	call["children"], call["filter"] = []any{c}, nil
	return nil
}

// add records one aggregate call.
func (a *aggregation) add(call node) error {
	name := strings.ToLower(str(call["function_name"]))
	if err := a.distinctFilter(call, name); err != nil {
		return err
	}
	key := a.key(call)
	if _, ok := a.aggKeys[key]; ok {
		return nil
	}
	args := list(call, "children")
	if call["export_state"] == true {
		return unsupported("EXPORT_STATE")
	}
	if a.pl.hasAggregate(args) || hasClass(args, "WINDOW") {
		return unsupported("nested aggregates")
	}
	ac := &aggCall{call: call, name: name, distinct: -1}
	switch {
	case len(list(child(call, "order_bys"), "orders")) > 0:
		ac.whole = unsupported("ORDER BY inside %s()", name)
	case call["distinct"] == true && (len(args) != 1 || child(call, "filter") != nil):
		ac.whole = unsupported("%s(DISTINCT ...) with several arguments or a FILTER", name)
	case call["distinct"] == true && name == "count" && a.pl.opts.bitmaps && a.bitmapArg(args[0].(node)):
		// A bitmap a group: no value need cross.
		ac.bitmap = true
	case name == "rb_count_distinct" && len(args) == 1 && call["distinct"] != true:
		// The rb extension's own count of distinct IDs (UINTEGER), which
		// bitmaps a group combine into as count(DISTINCT) does. Its pair
		// form, rb_count_distinct(id, key), has no partials (see
		// checkFunctions).
		ac.bitmap = true
	case name == "rb_count_distinct" && len(args) == 2:
		ac.whole = unsupported("rb_count_distinct(id, key), which has no partials")
	case call["distinct"] == true:
		arg, err := a.substituteAliases(clone(args[0]).(node))
		if err != nil {
			return err
		}
		arg = namedRows(arg)
		k := a.key(arg)
		ac.distinct = slices.Index(a.distinctKeys, k)
		if ac.distinct < 0 {
			ac.distinct = len(a.distincts)
			a.distincts, a.distinctKeys = append(a.distincts, arg), append(a.distinctKeys, k)
		}
	case sketches[name] && (len(args) != 2 || str(args[0].(node)["class"]) != "CONSTANT" || call["distinct"] == true ||
		len(list(child(call, "order_bys"), "orders")) > 0):
		ac.whole = unsupported("%s() of a size that is not a constant", name)
	default:
		ac.whole = partialArgs(name, len(args))
	}
	a.aggKeys[key] = ac
	a.aggs = append(a.aggs, ac)
	return nil
}

// namedRows makes the rows an expression builds, as in count(DISTINCT
// (a, b)), structs with named fields, which are equal where the rows are:
// the type of a row, STRUCT(BIGINT, BIGINT), cannot be written in a cast.
func namedRows(e node) node {
	return rewrite(e, func(n node) (node, bool) {
		if str(n["class"]) != "FUNCTION" || !strings.EqualFold(str(n["function_name"]), "row") || n["is_operator"] == true {
			return nil, false
		}
		r := clone(n).(node)
		r["function_name"] = "struct_pack"
		for i, c := range list(r, "children") {
			c.(node)["alias"] = "v" + strconv.Itoa(i+1)
		}
		for i, c := range list(r, "children") {
			list(r, "children")[i] = namedRows(c.(node))
		}
		return r, true
	}).(node)
}

// partialArgs checks that an aggregate computes in parts, given its
// arguments.
func partialArgs(name string, args int) error {
	switch {
	case name == "avg" || name == "mean":
		if args != 1 {
			return unsupported("%s with %d arguments", name, args)
		}
	case collecting[name]:
		if args != 1 {
			return unsupported("%s with %d arguments", name, args)
		}
	case isMoments(name):
		if args != 1 {
			return unsupported("%s with %d arguments", name, args)
		}
	case combinable[name] == "":
		return unsupported("%s() cannot be computed in parts", name)
	case (name == "min" || name == "max") && args != 1:
		return unsupported("%s with %d arguments", name, args)
	}
	return nil
}

// choose decides how the groups are completed (see the agg* constants).
func (a *aggregation) choose() error {
	// One grouping set of every group expression, or none: grouping sets
	// are applied by the coordinator, over the finest groups.
	single := a.singleSet()
	additive := true
	for _, ac := range a.aggs {
		if ac.distinct >= 0 && !additiveDistinct[ac.name] {
			additive = false
		}
	}
	anchor := a.anchor()
	// Under a collation, values equal to GROUP BY and DISTINCT, such as 'a'
	// and 'A', hash apart: partitions would split their groups.
	collation, err := a.pl.collation()
	if err != nil {
		return err
	}
	if !a.c.relation && single && collation == "" && a.c.wholeGroups(a.groups) {
		a.mode = aggComplete
		return nil
	}
	for _, ac := range a.aggs {
		if ac.whole != nil {
			return ac.whole
		}
	}
	// Workers complete the groups, so that the coordinator reads a row a
	// group, when there may be too many for it to combine every worker's
	// (or every partition's) partial results, or a select item calls a
	// group function.
	complete := single && len(a.groups) > 0 && collation == "" &&
		(a.pl.opts.aggregation == aggByGroup || a.pl.opts.aggregation == "" && (a.manyGroups() || a.groupWork()))
	switch {
	case a.c.relation:
		// The coordinator's combine step runs on workers, over whole groups.
		a.mode = aggGather
		if single && len(a.distincts) > 0 && additive && a.inPlace() {
			a.mode = aggInPlace
		}
		return nil
	case collation != "":
		a.mode = aggGather
	case len(a.distincts) == 0:
		a.mode = aggGather
		if complete {
			a.mode = aggByGroup
		}
	case !single:
		a.mode = aggGather
	case additive && a.inPlace():
		a.mode, a.regroup = aggInPlace, complete
	case a.pl.opts.aggregation == aggGather:
		a.mode = aggGather
	case additive && anchor >= 0 && a.pl.opts.aggregation != aggByGroup:
		a.mode, a.regroup = aggByValue, complete
	case len(a.groups) > 0:
		a.mode = aggByGroup
	default:
		a.mode = aggGather
	}
	if a.mode == aggGather && len(a.distincts) > 1 && anchor < 0 {
		// The workers would send every combination of the values.
		return unsupported("DISTINCT aggregates over different expressions")
	}
	return nil
}

// plan works out how each aggregate call is computed in two steps.
func (a *aggregation) plan() error {
	if a.mode == aggComplete {
		return nil
	}
	for _, ac := range a.aggs {
		if ac.distinct >= 0 && (a.mode == aggGather || a.mode == aggByGroup) {
			ac.kind = combineDistinct
			continue
		}
		if isMoments(ac.name) {
			ac.kind = combineMoments
			ac.partials = []int{a.partial(ac, "count"), a.partial(ac, "avg"), a.partial(ac, "var_pop")}
			continue
		}
		if ac.bitmap {
			ac.kind = combineBitmap
			ac.partials = []int{a.partial(ac, "rb_agg")}
			a.partials[ac.partials[0]]["distinct"] = false
			continue
		}
		switch ac.name {
		case "avg", "mean":
			ac.kind = combineAvg
			ac.partials = []int{a.partial(ac, "sum"), a.partial(ac, "count")}
		case "list", "array_agg":
			ac.kind = combineList
			ac.partials = []int{a.partial(ac, "")}
		default:
			ac.kind = combineSame
			if ac.name == "count" || ac.name == "count_star" || ac.name == "count_if" || ac.name == "countif" {
				ac.kind = combineCount
			}
			ac.partials = []int{a.partial(ac, "")}
		}
	}
	return nil
}

// partial adds a partial aggregate: the call itself, or the call's
// arguments, DISTINCT and FILTER with another function.
func (a *aggregation) partial(ac *aggCall, function string) int {
	p := clone(ac.call).(node)
	p["alias"] = ""
	if function != "" {
		p["function_name"] = function
	}
	a.partials = append(a.partials, p)
	a.dk = append(a.dk, ac.distinct)
	return len(a.partials) - 1
}

// bitmapArg reports whether a count(DISTINCT) argument is a 32-bit
// unsigned integer, as roaring bitmaps hold: a cast to UINTEGER, as the
// integer IDs of people are counted, or a column of an unsigned type that
// widens to it.
func (a *aggregation) bitmapArg(arg node) bool { return a.idValue(arg, a.c.scope, 0) }

// idValue is bitmapArg for an expression e of FROM clause sc: a cast to
// UINTEGER; a column of a table or relation of an unsigned type that
// widens to it, or one of a subquery that passes rows through whose
// expression there is such a value, as a report's member is through the
// common table expressions it reads; or a CASE whose values are such or
// NULL, as distinctFilter writes a FILTER.
func (a *aggregation) idValue(e node, sc *scope, depth int) bool {
	if depth > 4*maxViewDepth {
		return false
	}
	switch str(e["class"]) {
	case "CAST":
		return strings.EqualFold(str(child(e, "cast_type")["id"]), "UINTEGER")
	case "CASE":
		for _, c := range list(e, "case_checks") {
			if !a.idValue(child(c.(node), "then_expr"), sc, depth+1) {
				return false
			}
		}
		el := child(e, "else_expr")
		return el == nil || str(el["class"]) == "CONSTANT" && child(el, "value")["is_null"] == true || a.idValue(el, sc, depth+1)
	case "COLUMN_REF":
		it := resolveColumn(e, sc)
		if it == nil {
			return false
		}
		name := strings.ToLower(lastName(e))
		for _, u := range a.c.uses {
			if u.item == it {
				return idType(columnType(u.table, name))
			}
		}
		i := slices.Index(it.columns, name)
		for _, r := range a.c.rels {
			if r.item == it {
				return i >= 0 && i < len(r.columns) && idType(r.columns[i].Type)
			}
		}
		if inner := a.innerScope(it); inner != nil && i >= 0 {
			sub := subqueryNode(inner.ref)
			item := itemOf(list(sub, "select_list"), i, len(it.columns), name)
			if item == nil {
				return false
			}
			return a.idValue(item, inner, depth+1)
		}
	}
	return false
}

// itemOf returns the select item of a subquery's column i, of n, named
// name: the item at that position, or past a star, as in a report's
// SELECT s.*, CAST(s.id AS UINTEGER) AS member, the item that many places
// on, the star standing for the columns between. nil for a column of the
// star itself, or where the items do not say.
func itemOf(items []any, i, n int, name string) node {
	j, stars := i, 0
	for k, item := range items {
		if str(item.(node)["class"]) != "STAR" {
			continue
		}
		stars++
		width := n - (len(items) - 1)
		switch {
		case width < 0:
			return nil
		case i >= k && i < k+width:
			return nil
		case i >= k+width:
			j = i - width + 1
		}
	}
	if stars > 1 || j >= len(items) {
		return nil
	}
	item := items[j].(node)
	if stars == 1 {
		// Past a star, only an item named so: DuckDB numbers its columns
		// as the items say.
		alias := strings.ToLower(str(item["alias"]))
		if alias == "" && str(item["class"]) == "COLUMN_REF" {
			alias = strings.ToLower(lastName(item))
		}
		if alias != name {
			return nil
		}
	}
	return item
}

// idType reports whether a column type widens to UINTEGER.
func idType(t string) bool {
	switch strings.ToUpper(t) {
	case "UINTEGER", "USMALLINT", "UTINYINT":
		return true
	}
	return false
}

// innerScope returns the FROM clause of the subquery that passes rows
// through whose item is it, among those around the candidate's tables and
// relations.
func (a *aggregation) innerScope(it *fromItem) *scope {
	find := func(sc *scope) *scope {
		for ; sc != nil; sc = sc.outer {
			if sc.item == it && sc.ref != nil {
				return sc
			}
		}
		return nil
	}
	for _, u := range a.c.uses {
		if sc := find(u.scope); sc != nil {
			return sc
		}
	}
	for _, r := range a.c.rels {
		if sc := find(r.sc); sc != nil {
			return sc
		}
	}
	return nil
}

// anchor returns the index of a DISTINCT argument whose value every
// DISTINCT argument's non-NULL value fixes, so that partitioning by it
// keeps every argument's values apart; -1 if there is none.
func (a *aggregation) anchor() int {
	for i, d := range a.distincts {
		if str(d["class"]) != "COLUMN_REF" && len(a.distincts) > 1 {
			continue
		}
		col := resolveColumn(d, a.c.scope)
		ok := true
		for j, e := range a.distincts {
			if j != i && (col == nil || !slices.Contains(determined(e, a.c.scope), itemColumn{col, strings.ToLower(lastName(d))})) {
				ok = false
				break
			}
		}
		if ok {
			return i
		}
	}
	return -1
}

// inPlace reports whether the candidate's stage reads partitions that keep
// each value of every DISTINCT argument in one partition: inputs read by
// partition whose key, for one of them, every argument's value fixes.
func (a *aggregation) inPlace() bool {
	if len(a.c.keyed) == 0 {
		return false
	}
	for _, d := range a.distincts {
		if !a.c.fixesKey(determined(d, a.c.scope)) {
			return false
		}
	}
	return true
}

// fixesKey reports whether values of the columns fixed fix every key
// column of one of the candidate's inputs read by partition, directly or
// through the equalities its joins and WHERE clause hold.
func (c *candidate) fixesKey(fixed []itemColumn) bool {
	if c.keyScope != c.scope {
		// The partitioned inputs are inside a subquery, out of sight.
		return false
	}
	for _, class := range joinKeys(c.scope) {
		if slices.ContainsFunc(class, func(ic itemColumn) bool { return slices.Contains(fixed, ic) }) {
			for _, ic := range class {
				if !slices.Contains(fixed, ic) {
					fixed = append(fixed, ic)
				}
			}
		}
	}
	for _, k := range c.keyed {
		if k.nullable {
			// Its key is NULL for the rows its join matched to none of
			// it, in every partition.
			continue
		}
		all := true
		for _, key := range k.keys {
			if !slices.Contains(fixed, itemColumn{k.item, key}) {
				all = false
			}
		}
		if all {
			return true
		}
	}
	return false
}

// itemColumn is a column of a FROM item, by lower-case name.
type itemColumn struct {
	item *fromItem
	name string
}

// determined returns the columns whose values a non-NULL value of e fixes:
// a column itself, the fields of a row or struct, and what the branches of
// a CASE without ELSE fix when they are all the same expression.
func determined(e node, sc *scope) []itemColumn {
	switch str(e["class"]) {
	case "COLUMN_REF":
		if it := resolveColumn(e, sc); it != nil {
			return []itemColumn{{it, strings.ToLower(lastName(e))}}
		}
	case "FUNCTION":
		switch strings.ToLower(str(e["function_name"])) {
		case "row", "struct_pack":
			var out []itemColumn
			for _, c := range list(e, "children") {
				out = append(out, determined(c.(node), sc)...)
			}
			return out
		}
	case "CASE":
		if el := child(e, "else_expr"); el != nil && !(str(el["class"]) == "CONSTANT" && child(el, "value")["is_null"] == true) {
			return nil
		}
		// Branches that differ could give equal values from different
		// columns: CASE WHEN p THEN (a, b) ELSE (b, a) END.
		checks := list(e, "case_checks")
		for _, c := range checks[min(1, len(checks)):] {
			if canonical(child(c.(node), "then_expr")) != canonical(child(checks[0].(node), "then_expr")) {
				return nil
			}
		}
		if len(checks) > 0 {
			return determined(child(checks[0].(node), "then_expr"), sc)
		}
	}
	return nil
}

// maxGatheredGroups is the most groups whose partial results, every
// task's, the coordinator combines itself (see manyGroups).
const maxGatheredGroups = 10_000

// manyGroups guesses whether the groups may be too many to send every
// worker's partial results to the coordinator, which would combine as many
// rows as there are groups times tasks, or partitions, and hold every
// group: at a hundred times the data, a report's groups ranked on the
// coordinator filled its disk. Partitions that complete their groups send
// it only the groups HAVING, a ranking or ORDER BY ... LIMIT keeps, a row
// each. The groups are few when every group expression takes few values
// (see groupValues), more than maxGatheredGroups together; a column of an
// input the workers divide or partition counts as many, unless ANALYZE
// counted its distinct values. Groups of the tables every worker reads in
// full are few.
func (a *aggregation) manyGroups() bool {
	small := map[*fromItem]bool{}
	tables := map[*fromItem]*Table{}
	for _, u := range a.c.uses {
		if u.scope != a.c.scope {
			continue
		}
		tables[u.item] = u.table
		if u != a.c.split && !slices.ContainsFunc(a.c.keyed, func(k *keyed) bool { return k.use == u }) {
			small[u.item] = true
		}
	}
	groups := 1.0
	for _, g := range a.groups {
		n, ok := groupValues(g, a.c.scope, small, tables)
		if !ok {
			return true
		}
		groups *= n
	}
	return groups > maxGatheredGroups
}

// fewValuesFunctions are the functions whose values are few, whatever
// their arguments': the fields and truncations of dates and times, by
// which reports group a quarter's rows into days or months.
var fewValuesFunctions = map[string]bool{
	"date_trunc": true, "datetrunc": true, "date_part": true, "datepart": true, "extract": true,
	"year": true, "quarter": true, "month": true, "week": true, "weekofyear": true, "yearweek": true,
	"day": true, "dayofmonth": true, "dayofweek": true, "dayofyear": true, "isodow": true, "isoyear": true,
	"weekday": true, "hour": true, "monthname": true, "dayname": true, "last_day": true,
}

// datePartValues is how many values a date's field or truncation is taken
// to take: the days of three years.
const datePartValues = 1000

// groupValues estimates how many values e, a group expression of FROM
// clause sc, takes, or reports that it cannot tell: one for a constant or
// a column of one of small, the tables every worker reads in full; a
// column's distinct count, as ANALYZE keeps it in its table (tables); two
// or three for a comparison or another test; datePartValues for a date or
// time's field or truncation; the sum of a CASE's results; and for any
// other expression the product of the values of the columns it reads.
func groupValues(e node, sc *scope, small map[*fromItem]bool, tables map[*fromItem]*Table) (float64, bool) {
	switch str(e["class"]) {
	case "CONSTANT":
		return 1, true
	case "COLUMN_REF":
		it := resolveColumn(e, sc)
		if it == nil {
			return 0, false
		}
		if small[it] {
			return 1, true
		}
		if t := tables[it]; t != nil {
			if n, ok := t.Distinct[strings.ToLower(lastName(e))]; ok {
				return float64(n), true
			}
		}
		return 0, false
	case "COMPARISON", "CONJUNCTION", "BETWEEN":
		return 3, true
	case "OPERATOR":
		switch str(e["type"]) {
		case "OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL", "OPERATOR_NOT", "COMPARE_IN", "COMPARE_NOT_IN":
			return 3, true
		}
	case "CAST":
		return groupValues(child(e, "child"), sc, small, tables)
	case "CASE":
		total := 0.0
		branches := []node{child(e, "else_expr")}
		for _, c := range list(e, "case_checks") {
			branches = append(branches, child(c.(node), "then_expr"))
		}
		for _, b := range branches {
			if b == nil {
				total++
				continue
			}
			n, ok := groupValues(b, sc, small, tables)
			if !ok {
				return 0, false
			}
			total += n
		}
		return total, true
	case "FUNCTION":
		if fewValuesFunctions[strings.ToLower(str(e["function_name"]))] {
			return datePartValues, true
		}
	}
	// Otherwise at most every combination of the columns it reads.
	values, ok, any := 1.0, true, false
	walk(e, func(n node) bool {
		if str(n["class"]) == "COLUMN_REF" {
			any = true
			v, known := groupValues(n, sc, small, tables)
			ok = ok && known
			values *= v
		}
		return ok
	})
	return values, ok && any
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
	cols, err := a.pl.describe(probe, a.c.ctes)
	if err != nil {
		return err
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
// and, unless the stage keeps their values apart, the DISTINCT arguments,
// with the partial aggregates.
//
//	SELECT <group> AS __g0, ..., <distinct arg> AS __dk0, ..., <partial> AS __a0, ...
//	FROM <the candidate's FROM> WHERE <its WHERE>
//	GROUP BY <group>, ..., <distinct arg>, ...
//
// Partials over DISTINCT values are left to the second stage when it
// computes them (aggByValue).
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
	if a.mode == aggComplete {
		// Every aggregate, whole.
		for k, ac := range a.aggs {
			q, err := a.substituteAliasesInArgs(ac.call)
			if err != nil {
				return nil, err
			}
			items = append(items, aliased(q, "__f"+strconv.Itoa(k)))
			ac.final = k
		}
		a.final = true
		f["select_list"] = items
		f["from_table"] = clone(child(a.c.sel, "from_table"))
		if w := child(a.c.sel, "where_clause"); w != nil {
			if f["where_clause"], err = a.substituteAliases(clone(w).(node)); err != nil {
				return nil, err
			}
		}
		f["group_expressions"] = groups
		f["group_sets"] = []any{set}
		// Whole groups: those HAVING keeps, and of them the rows a statement
		// ranks and keeps the first of (see markTopK), and under ORDER BY ...
		// LIMIT each task's first.
		cut := true
		if h := child(a.c.sel, "having"); h != nil {
			e, err := a.substituteAliases(clone(h).(node))
			if cut = err == nil && workerExpression(e); cut {
				f["having"] = e
			}
		}
		if !cut {
			return f, nil
		}
		if keep := child(a.c.sel, topKMark); keep != nil {
			if q, err := a.substituteAliases(clone(keep).(node)); err == nil {
				f["qualify"] = q
			}
		}
		if mods, ok := a.wholeFirstRows(); ok {
			f["modifiers"] = mods
		}
		return f, nil
	}
	if a.mode != aggInPlace {
		for i, d := range a.distincts {
			items = append(items, aliased(d, "__dk"+strconv.Itoa(i)))
			groups = append(groups, clone(d))
			set = append(set, jsonInt(len(set)))
		}
	}
	for i, p := range a.partials {
		if a.dk[i] >= 0 && a.mode == aggByValue {
			continue
		}
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

// secondStage is the stage that completes an aggregation's groups from
// the partial results, partition by partition.
type secondStage struct {
	fragment node
	// keys are the partial results' columns the exchange is keyed by.
	keys []string
	// whole says the stage reads every partition in one task: the groups of
	// an aggregation without GROUP BY, which has one.
	whole bool
	// columns rename the stage's result columns positionally.
	columns []string
}

// partialsView is the view a second stage reads its partition of the
// partial results as.
const partialsView = "__igloo_partials"

// valueStage builds the second stage of an aggregation by value: each
// partition's partial results aggregated again by group, with the
// aggregates over DISTINCT values computed over the partition's values,
// which no other partition holds.
//
//	SELECT __g0, ..., sum(__a0) AS __a0, ..., count(DISTINCT __dk0) AS __a3, ...
//	FROM __igloo_partials GROUP BY __g0, ...
func (a *aggregation) valueStage() (*secondStage, error) {
	f, err := a.partialsSelect()
	if err != nil {
		return nil, err
	}
	items := list(f, "select_list")
	for i, p := range a.partials {
		var e node
		if d := a.dk[i]; d >= 0 {
			e = clone(p).(node)
			ref, err := a.pl.p.expr(quoteIdent("__dk" + strconv.Itoa(d)))
			if err != nil {
				return nil, err
			}
			e["children"] = []any{ref}
		} else {
			name := strings.ToLower(str(p["function_name"]))
			text := combinable[name] + "(" + quoteIdent("__a"+strconv.Itoa(i)) + ")"
			if collecting[name] {
				text = combineLists(quoteIdent("__a" + strconv.Itoa(i)))
			}
			if combinable[name] == "sum" && name != "sum" {
				// Counts stay BIGINT, as one machine has them.
				text = "CAST(" + text + " AS BIGINT)"
			}
			if sketches[name] {
				e, err = a.pl.sketchCall(p, "__a"+strconv.Itoa(i))
			} else {
				e, err = a.pl.p.expr(text)
			}
			if err != nil {
				return nil, err
			}
		}
		items = append(items, aliased(e, "__a"+strconv.Itoa(i)))
	}
	f["select_list"] = items
	keys := a.groupColumns()
	keys = append(keys, "__dk"+strconv.Itoa(a.anchor()))
	return &secondStage{fragment: f, keys: keys}, nil
}

// groupStage builds the second stage of an aggregation by group: each
// partition holds whole groups, which it completes as the coordinator
// would, every aggregate included, keeping only the groups HAVING keeps
// and, under ORDER BY ... LIMIT, its first rows. The coordinator then reads
// the completed aggregates (final). Select items that call a group
// function the stage computes too (staged), as __s columns; the lists only
// they read stay on the workers.
//
//	SELECT __g0, ..., coalesce(sum(__a0), 0) AS __f0, count(DISTINCT __dk0) AS __f1, ...,
//		f(flatten(list(__a2) ...)) AS __s3, ...
//	FROM __igloo_partials GROUP BY __g0, ... HAVING ... ORDER BY ... LIMIT ...
func (a *aggregation) groupStage() (*secondStage, error) {
	f, err := a.partialsSelect()
	if err != nil {
		return nil, err
	}
	items := list(f, "select_list")
	var staged []any
	if g, ok := a.pl.env.(GroupFunctions); ok {
		for i, item := range a.items {
			if !callsGroupFunction(item, g) {
				continue
			}
			e, err := a.rewrite(bare(item), "select")
			if err != nil || !overPartials(e) || a.pl.checkFunctions(e) != nil {
				continue
			}
			staged = append(staged, aliased(e, "__s"+strconv.Itoa(i)))
			if a.staged == nil {
				a.staged = map[int]bool{}
			}
			a.staged[i] = true
		}
	}
	reads := a.combineReads()
	for k, ac := range a.aggs {
		ac.final = -1
		if ac.kind == combineList && len(a.staged) > 0 && !reads[ac] {
			continue
		}
		e, err := a.combined(ac)
		if err != nil {
			return nil, err
		}
		items = append(items, aliased(e, "__f"+strconv.Itoa(k)))
		ac.final = k
	}
	items = append(items, staged...)
	f["select_list"] = items
	// HAVING and the first rows are worth applying early, when they can be
	// said in terms of the partial results alone; the coordinator applies
	// them again. So are the rows a statement ranks and keeps the first of
	// (see markTopK), which it ranks again. Those cuts come after HAVING,
	// as in SQL: a stage that cannot apply HAVING keeps every group.
	cut := true
	if h := child(a.c.sel, "having"); h != nil {
		e, err := a.rewrite(clone(h).(node), "having")
		if cut = err == nil && overPartials(e); cut {
			f["having"] = e
		}
	}
	if q := qualifyTopK(a.c.sel, func(e node) (node, error) { return a.rewrite(e, "select") }); q != nil && cut {
		f["qualify"] = q
	}
	if mods, ok := a.firstRows(); ok && cut {
		f["modifiers"] = mods
	}
	a.final = true
	return &secondStage{fragment: f, keys: a.groupColumns()}, nil
}

// groupWork reports whether a select item calls a group function (see
// GroupFunctions), which is worth calling where groups are whole.
func (a *aggregation) groupWork() bool {
	g, ok := a.pl.env.(GroupFunctions)
	if !ok {
		return false
	}
	return slices.ContainsFunc(a.items, func(item node) bool { return callsGroupFunction(item, g) })
}

// callsGroupFunction reports whether e calls one of g's group functions.
func callsGroupFunction(e node, g GroupFunctions) bool {
	found := false
	walk(e, func(n node) bool {
		if str(n["class"]) == "FUNCTION" && n["is_operator"] != true && g.GroupFunction(strings.ToLower(str(n["function_name"]))) {
			found = true
		}
		return !found
	})
	return found
}

// combineReads returns the aggregate calls the combine step reads: those
// of the select items the second stage does not compute, HAVING, QUALIFY
// and ORDER BY.
func (a *aggregation) combineReads() map[*aggCall]bool {
	reads := map[*aggCall]bool{}
	visit := func(v any) {
		walk(v, func(n node) bool {
			if !a.pl.isAggregate(n) {
				return true
			}
			if ac, ok := a.aggKeys[a.key(n)]; ok {
				reads[ac] = true
			}
			return false
		})
	}
	for i, item := range a.items {
		if !a.staged[i] {
			visit(item)
		}
	}
	sel := a.c.sel
	visit(child(sel, "having"))
	visit(child(sel, "qualify"))
	visit(sel["modifiers"])
	return reads
}

// partialsSelect starts a second stage's SELECT: the group columns, over
// the partial results, grouped by them.
func (a *aggregation) partialsSelect() (node, error) {
	f, err := a.pl.p.template("SELECT 1 FROM " + quoteIdent(partialsView))
	if err != nil {
		return nil, err
	}
	var items, groups, set []any
	for i, g := range a.groupColumns() {
		ref, err := a.pl.p.expr(quoteIdent(g))
		if err != nil {
			return nil, err
		}
		items = append(items, aliased(ref, g))
		groups = append(groups, clone(ref))
		set = append(set, jsonInt(i))
	}
	f["select_list"] = items
	if len(groups) > 0 {
		f["group_expressions"] = groups
		f["group_sets"] = []any{set}
	}
	return f, nil
}

func (a *aggregation) groupColumns() []string {
	var out []string
	for i := range a.groups {
		out = append(out, "__g"+strconv.Itoa(i))
	}
	return out
}

// workerExpression reports whether e, an expression of a stage's own FROM clause,
// can be computed by the stage alone: it reads no subquery, window, parameter or
// lambda.
func workerExpression(e node) bool {
	return !hasClass(e, "SUBQUERY") && !hasClass(e, "WINDOW") && !hasClass(e, "PARAMETER") && !hasClass(e, "LAMBDA")
}

// overPartials reports whether e reads nothing but partial results'
// columns.
func overPartials(e node) bool {
	ok := true
	walk(e, func(n node) bool {
		switch str(n["class"]) {
		case "COLUMN_REF":
			parts := columnNames(n)
			if len(parts) != 1 || !strings.HasPrefix(parts[0], "__") {
				ok = false
			}
		case "SUBQUERY", "WINDOW", "PARAMETER", "LAMBDA", "STAR":
			ok = false
		}
		return ok
	})
	return ok
}

// topN returns LIMIT plus OFFSET of a candidate whose result is its first
// rows by ORDER BY: no DISTINCT, window function or QUALIFY, which would
// need every group first.
func (a *aggregation) topN() (int64, bool) {
	sel := a.c.sel
	if child(sel, "qualify") != nil || hasClass(sel["select_list"], "WINDOW") || hasClass(sel["modifiers"], "WINDOW") {
		return 0, false
	}
	var order, limit bool
	var total int64
	for _, m := range list(sel, "modifiers") {
		switch mod := m.(node); str(mod["type"]) {
		case "ORDER_MODIFIER":
			order = true
		case "LIMIT_MODIFIER":
			n, ok := limitRows(mod)
			if !ok || child(mod, "limit") == nil {
				return 0, false
			}
			limit, total = true, n
		default:
			return 0, false
		}
	}
	return total, order && limit && total <= maxTopN
}

// firstRows returns the ORDER BY and LIMIT that keep a partition's first
// groups, in terms of the partial results.
func (a *aggregation) firstRows() ([]any, bool) {
	total, ok := a.topN()
	if !ok {
		return nil, false
	}
	var mods []any
	for _, m := range list(a.c.sel, "modifiers") {
		mod := clone(m).(node)
		switch str(mod["type"]) {
		case "ORDER_MODIFIER":
			for _, o := range list(mod, "orders") {
				ord := o.(node)
				e := child(ord, "expression")
				part := "order"
				if k, ok := intConstant(e); ok && k >= 1 && int(k) <= len(a.items) {
					e, part = bare(a.items[k-1]), "select"
				} else if str(e["class"]) == "COLUMN_REF" && len(columnNames(e)) == 1 {
					if i, isAlias := a.aliases[strings.ToLower(columnNames(e)[0])]; isAlias {
						e, part = bare(a.items[i]), "select"
					}
				}
				r, err := a.rewrite(clone(e).(node), part)
				if err != nil || !overPartials(r) {
					return nil, false
				}
				ord["expression"] = r
			}
		case "LIMIT_MODIFIER":
			l, err := a.pl.p.expr(strconv.FormatInt(total, 10))
			if err != nil {
				return nil, false
			}
			mod["limit"], mod["offset"] = l, nil
		}
		mods = append(mods, mod)
	}
	return mods, true
}

// wholeFirstRows returns the ORDER BY and LIMIT that keep the first groups
// of a stage that computes whole groups (aggComplete), in terms of the
// candidate's own FROM clause, which the stage's fragment groups as the
// candidate does.
func (a *aggregation) wholeFirstRows() ([]any, bool) {
	total, ok := a.topN()
	if !ok || child(a.c.sel, topKMark) != nil {
		return nil, false
	}
	var mods []any
	for _, m := range list(a.c.sel, "modifiers") {
		mod := clone(m).(node)
		switch str(mod["type"]) {
		case "ORDER_MODIFIER":
			for _, o := range list(mod, "orders") {
				ord := o.(node)
				e := child(ord, "expression")
				if k, ok := intConstant(e); ok && k >= 1 && int(k) <= len(a.items) {
					e = bare(a.items[k-1])
				} else if str(e["class"]) == "COLUMN_REF" && len(columnNames(e)) == 1 {
					// ORDER BY binds a bare name to the select list's alias before a
					// column of FROM by that name, as firstRows reads it.
					if i, isAlias := a.aliases[strings.ToLower(columnNames(e)[0])]; isAlias {
						e = bare(a.items[i])
					}
				} else if str(e["class"]) == "STAR" {
					return nil, false
				}
				r, err := a.substituteAliases(clone(e).(node))
				if err != nil || hasClass(r, "SUBQUERY") || hasClass(r, "WINDOW") || hasClass(r, "PARAMETER") {
					return nil, false
				}
				ord["expression"] = r
			}
		case "LIMIT_MODIFIER":
			l, err := a.pl.p.expr(strconv.FormatInt(total, 10))
			if err != nil {
				return nil, false
			}
			mod["limit"], mod["offset"] = l, nil
		}
		mods = append(mods, mod)
	}
	return mods, true
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
		var e node
		var err error
		if a.staged[i] {
			// Computed by the second stage (see groupStage).
			e, err = a.pl.p.expr(quoteIdent("__s" + strconv.Itoa(i)))
		} else {
			e, err = a.rewrite(bare(item), "select")
		}
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
	if len(a.groups) > 0 && !a.final {
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
			if part == "having" && a.final {
				// Whole groups, a row each: the combine step only projects
				// them, and keeps those HAVING keeps.
				part = "where_clause"
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
	if a.final {
		// Projected, not grouped: GROUPING() of the one grouping set,
		// which groups by every group column, is 0.
		zero, err := a.pl.p.expr("CAST(0 AS BIGINT)")
		if err != nil {
			return nil, err
		}
		out = rewrite(out, func(n node) (node, bool) {
			if str(n["class"]) != "OPERATOR" || str(n["type"]) != "GROUPING_FUNCTION" {
				return nil, false
			}
			z := clone(zero).(node)
			z["alias"] = n["alias"]
			return z, true
		}).(node)
	}
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
			switch str(n["class"]) {
			case "PARAMETER":
				// Parameters are bound when the combine step runs.
				return n, true
			case "SUBQUERY":
				// A scalar subquery that stands on its own (see analyze),
				// which the coordinator computes, its tables read at the
				// workers' snapshots (see pinReads). It defines the common
				// table expressions it reads, which the statement the
				// coordinator runs does not: TPC-DS q24's HAVING reads
				// ssales, which workers compute for its FROM.
				if !a.c.relation {
					q, serr := standalone(subqueryNode(n), a.c.ctes)
					if serr != nil {
						err = serr
						return nil, false
					}
					s := clone(n).(node)
					child(s, "subquery")["node"] = q
					return s, true
				}
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
// partial results, cast to the call's own type; or, once a second stage
// completed the groups, that reads its result.
func (a *aggregation) combined(ac *aggCall) (node, error) {
	col := func(i int) string { return quoteIdent("__a" + strconv.Itoa(ac.partials[i])) }
	var text string
	switch {
	case a.final && ac.final < 0:
		return nil, fmt.Errorf("aggregate %s was left on the workers", ac.name)
	case a.final:
		// One row per group, which the combine step projects.
		text = quoteIdent("__f" + strconv.Itoa(ac.final))
	case ac.kind == combineCount:
		text = "coalesce(sum(" + col(0) + "), 0)"
	case ac.kind == combineAvg:
		text = "CAST(sum(" + col(0) + ") AS DOUBLE) / CAST(sum(" + col(1) + ") AS DOUBLE)"
	case ac.kind == combineSame && sketches[ac.name]:
		e, err := a.pl.sketchCall(ac.call, "__a"+strconv.Itoa(ac.partials[0]))
		if err != nil {
			return nil, err
		}
		return a.pl.cast(e, ac.typ)
	case ac.kind == combineSame:
		text = combinable[ac.name] + "(" + col(0) + ")"
	case ac.kind == combineBitmap:
		text = "rb_cardinality(rb_or_agg(" + col(0) + "))"
	case ac.kind == combineList:
		text = combineLists(col(0))
	case ac.kind == combineMoments:
		// Of a sample, n - 1 degrees of freedom: none for one value.
		n, least, df := "sum("+col(0)+")", "0", "sum("+col(0)+")"
		if moments[ac.name] {
			least, df = "1", "(sum("+col(0)+") - 1)"
		}
		text = "struct_extract(" + mergedMoments(col(0), col(1), col(2)) + ", 'q') / " + df
		if strings.HasPrefix(ac.name, "stddev") {
			text = "sqrt(" + text + ")"
		}
		text = "CASE WHEN " + n + " > " + least + " THEN " + text + " END"
	case ac.kind == combineDistinct:
		c := clone(ac.call).(node)
		ref, err := a.pl.p.expr(quoteIdent("__dk" + strconv.Itoa(ac.distinct)))
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
// clause, DISTINCT, or LIMIT, with ORDER BY or not (any rows, which its
// stage stops sending once it has enough: see Stage.enough).
// searchesText reports whether a WHERE clause looks a column's text up by a
// pattern or a part of it of at least minSearch characters (x LIKE '%word%',
// contains(x, 'word')), as scans of long text do, which keep few rows: workers
// read the text, and the coordinator what they keep. A pattern of a character
// or two may keep most of them, which would cross for nothing.
func searchesText(where node) bool {
	for _, c := range conjuncts(where) {
		if str(c["class"]) != "FUNCTION" {
			continue
		}
		switch strings.ToLower(str(c["function_name"])) {
		case "~~", "~~*", "contains", "starts_with", "prefix", "suffix", "ends_with", "regexp_matches":
		default:
			continue
		}
		args := list(c, "children")
		if len(args) != 2 {
			continue
		}
		col, pattern := args[0].(node), args[1].(node)
		if str(col["class"]) != "COLUMN_REF" || str(pattern["class"]) != "CONSTANT" {
			continue
		}
		v := child(pattern, "value")
		text := strings.NewReplacer("%", "", "_", "").Replace(str(v["value"]))
		if str(child(v, "type")["id"]) == "VARCHAR" && len(text) >= minSearch {
			return true
		}
	}
	return false
}

// minSearch is the fewest characters of a search that searchesText counts.
const minSearch = 3

func (pl *planner) rowsJob(c *candidate) (*jobBuild, error) {
	sel := c.sel
	if (hasClass(sel["select_list"], "WINDOW") || child(sel, "qualify") != nil || hasClass(sel["modifiers"], "WINDOW")) && c.window == nil {
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
	// Rows that workers compute anyway, a relation's, cross to the
	// coordinator fewer for its filters; so do those of windows workers
	// compute, for its QUALIFY, rather than every row for the coordinator to
	// compute them over.
	filtersRelation := child(sel, "where_clause") != nil && slices.ContainsFunc(c.keyed, func(k *keyed) bool { return k.rel != nil })
	qualifies := c.window != nil && child(sel, "qualify") != nil
	// So do those of a scan that keeps few of the divided table's rows, by
	// a column equal to a constant, in a list or equal to a subquery's
	// value, as auto mode's estimates take it (filtersSelectively): workers
	// read the table, and the coordinator what they keep.
	selective := c.split != nil && child(sel, "where_clause") != nil && (filtersSelectively(node{"where_clause": child(sel, "where_clause")}, c.split.table) || searchesText(child(sel, "where_clause")))
	// And so do those of what workers compute as a whole, to be read as it
	// is (see setOperationJob).
	selective = selective || pl.gathers[identity(sel)]
	// And so do those of a subquery whose rows the coordinator reads to run
	// the rest, as a window over all of them, where they are fewer than
	// the table's, by its filters or its columns: workers read the table in
	// shares, rather than the coordinator alone, and send what they keep.
	feeds := !c.top && c.split != nil && (child(sel, "where_clause") != nil ||
		2*columnsRead(sel, c.split.table).bytes(c.split.table) <= c.split.table.Bytes)
	if limit != nil && order == nil && !pl.opts.force && !selective && !filtersRelation && !qualifies && c.window == nil && len(c.keyed) == 0 {
		// Any rows of a scan: one machine stops at the first it reads,
		// which auto mode's estimates, of the whole scan, do not count.
		return nil, unsupported("LIMIT without ORDER BY, which one machine answers fastest")
	}
	// And so do those sorted by ORDER BY without LIMIT: workers sort their
	// shares, and the coordinator merges them as the client reads them
	// (see merge.go), in memory for a batch of rows a task, where one
	// machine sorts them all, past its memory spilling to disk and taking
	// more memory besides than its limit allows.
	sorts := c.top && order != nil && limit == nil && distinct == nil
	hop := distinct == nil && limit == nil && !qualifies && !filtersRelation && !selective && !feeds
	if hop && !sorts && !pl.opts.writes {
		// Every matching row would cross to the coordinator and wait on its
		// disk before the first reached the client: a hop, not a saving.
		// Workers that write a table's files send it none.
		return nil, unsupported("rows of %s without DISTINCT, aggregates or ORDER BY ... LIMIT", c.largest())
	}
	if limit != nil {
		if total, ok := limitRows(limit); ok && total > maxTopN {
			return nil, unsupported("LIMIT and OFFSET of more than %d rows", maxTopN)
		}
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
	f["cte_map"] = node{"map": []any{}}
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
				return nil, unsupported("LIMIT or OFFSET that is not a number, or too large")
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
	b := &jobBuild{fragment: f, combine: out, columns: columns}
	if limit != nil && order == nil && distinct == nil {
		// Any rows: its tasks stop once those that ended sent enough.
		if total, ok := limitRows(limit); ok && total > 0 {
			b.enough = total
		}
	}
	if c.top && order != nil && limit == nil && distinct == nil {
		// Each task sorts its rows by the ORDER BY it keeps, which the
		// coordinator merges them by.
		for _, ord := range orders {
			e := child(ord, "expression")
			names := columns[:n]
			if str(e["class"]) != "STAR" {
				names = columnNames(e)
			}
			for _, name := range names {
				b.merge = append(b.merge, mergeKey{column: name, order: str(ord["type"]), nulls: str(ord["null_order"])})
			}
		}
	}
	if hop && sorts && !pl.mergeable(b.merge, columns[:n], c.columns) {
		// Sorted only on the coordinator, after every row crossed to it.
		return nil, unsupported("rows of %s ordered by what the coordinator cannot merge", c.largest())
	}
	return b, nil
}

// mergeable reports whether the coordinator will merge rows sorted by keys,
// the result's columns of cols named names, in this session (see
// resolveMerge), rather than sort them.
func (pl *planner) mergeable(keys []mergeKey, names []string, cols []Column) bool {
	var settings []setting
	for _, name := range []string{"preserve_insertion_order", "default_collation", "default_order", "default_null_order"} {
		v, err := pl.env.Setting(pl.ctx, name)
		if err != nil {
			return false
		}
		settings = append(settings, setting{Name: name, Value: v})
	}
	named := make([]Column, len(cols))
	for i, c := range cols {
		named[i] = Column{Name: names[i], Type: c.Type}
	}
	_, ok := resolveMerge(keys, named, settings)
	return ok
}

// mergeOrder returns, for an aggregation whose result is its whole groups
// (final) ordered by ORDER BY without LIMIT, the columns of the stage that
// completes them its tasks sort their rows by, and the ORDER BY that sorts
// them so; nil when an ORDER BY item is not a group expression, an
// aggregate or a select item computing one of them, or the result is
// otherwise.
func (a *aggregation) mergeOrder() ([]mergeKey, node) {
	sel := a.c.sel
	if !a.final || child(sel, "qualify") != nil {
		return nil, nil
	}
	var order node
	for _, m := range list(sel, "modifiers") {
		switch mod := m.(node); str(mod["type"]) {
		case "ORDER_MODIFIER":
			order = clone(mod).(node)
		default:
			return nil, nil
		}
	}
	if order == nil {
		return nil, nil
	}
	var keys []mergeKey
	for _, o := range list(order, "orders") {
		ord := o.(node)
		e := child(ord, "expression")
		staged := -1
		if k, ok := intConstant(e); ok && k >= 1 && int(k) <= len(a.items) {
			e, staged = bare(a.items[k-1]), int(k-1)
		} else if str(e["class"]) == "COLUMN_REF" && len(columnNames(e)) == 1 {
			if i, isAlias := a.aliases[strings.ToLower(columnNames(e)[0])]; isAlias {
				if has, _ := a.c.scope.hasColumn(strings.ToLower(columnNames(e)[0])); !has {
					e, staged = bare(a.items[i]), i
				}
			}
		}
		col := ""
		k := a.key(e)
		if g := slices.Index(a.groupKeys, k); g >= 0 {
			col = "__g" + strconv.Itoa(g)
		} else if ac, ok := a.aggKeys[k]; ok && a.pl.isAggregate(e) && ac.final >= 0 {
			col = "__f" + strconv.Itoa(ac.final)
		} else if staged >= 0 && a.staged[staged] {
			col = "__s" + strconv.Itoa(staged)
		}
		if col == "" {
			return nil, nil
		}
		ref, err := a.pl.p.expr(quoteIdent(col))
		if err != nil {
			return nil, nil
		}
		ord["expression"] = ref
		keys = append(keys, mergeKey{column: col, order: str(ord["type"]), nulls: str(ord["null_order"])})
	}
	return keys, order
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
		if !ok || n < 0 || n > math.MaxInt64-total {
			// Past what workers could be asked for.
			return 0, false
		}
		total += n
	}
	return total, true
}

// maxTopN bounds LIMIT plus OFFSET of a distributed top-N: each worker
// sends up to that many rows, which the coordinator keeps on disk.
const maxTopN = 1_000_000

func jsonInt(i int) any { return json.Number(strconv.Itoa(i)) }

// bitmapSetting is the session's DuckDB setting, the rb extension's, that
// turns roaring bitmaps on for count(DISTINCT) of 32-bit unsigned IDs:
// 'always' (Igloo sets it around the statements it picks; see
// internal/pgwire/bitmaps.go), and absent where the extension is not
// loaded.
const bitmapSetting = "rb_count_distinct"

// bitmapsOn reports whether count(DISTINCT) of 32-bit unsigned IDs may
// combine roaring bitmaps in statements of the session env: its rb
// extension, which every machine loads where any does, is loaded, and the
// session has bitmaps on.
func (co *Coordinator) bitmapsOn(ctx context.Context, env Env) bool {
	if co.bitmaps != nil {
		return *co.bitmaps
	}
	v, err := env.Setting(ctx, bitmapSetting)
	if err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(v), "always")
}
