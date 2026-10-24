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
	// rather than partial aggregates.
	final bool
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
	case aggByGroup:
		b.second, err = a.groupStage()
	}
	if err != nil {
		return nil, err
	}
	if b.combine, err = a.combine(); err != nil {
		return nil, err
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
	return &jobBuild{fragment: fragment, second: &secondStage{fragment: combine, keys: a.groupColumns(), whole: len(a.groups) == 0}}, nil
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
)

// combinable maps the aggregates that combine by applying another
// aggregate to their partial results to that aggregate.
var combinable = map[string]string{
	"sum": "sum", "min": "min", "max": "max", "bool_and": "bool_and", "bool_or": "bool_or",
	"bit_and": "bit_and", "bit_or": "bit_or", "bit_xor": "bit_xor", "any_value": "any_value",
	"fsum": "fsum", "sumkahan": "fsum", "kahan_sum": "fsum", "product": "product",
	"count": "sum", "count_star": "sum", "count_if": "sum", "countif": "sum",
	// Roaring bitmaps: a union of unions.
	"rb_agg": "rb_or_agg", "rb_or_agg": "rb_or_agg",
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

// add records one aggregate call.
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
	single := len(a.sets) == 0 || (len(a.sets) == 1 && len(a.sets[0].([]any)) == len(a.groups))
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
	switch {
	case a.c.relation:
		// The coordinator's combine step runs on workers, over whole groups.
		a.mode = aggGather
		if len(a.distincts) > 0 && additive && a.inPlace() {
			a.mode = aggInPlace
		}
		if !single {
			return unsupported("grouping sets in a relation")
		}
		return nil
	case collation != "":
		a.mode = aggGather
	case len(a.distincts) == 0:
		a.mode = aggGather
		if single && len(a.groups) > 0 && (a.pl.opts.aggregation == aggByGroup || a.pl.opts.aggregation == "" && a.manyGroups()) {
			a.mode = aggByGroup
		}
	case !single:
		a.mode = aggGather
	case additive && a.inPlace():
		a.mode = aggInPlace
	case a.pl.opts.aggregation == aggGather:
		a.mode = aggGather
	case additive && anchor >= 0 && a.pl.opts.aggregation != aggByGroup:
		a.mode = aggByValue
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
func (a *aggregation) bitmapArg(arg node) bool {
	switch str(arg["class"]) {
	case "CAST":
		return strings.EqualFold(str(child(arg, "cast_type")["id"]), "UINTEGER")
	case "COLUMN_REF":
		it := resolveColumn(arg, a.c.scope)
		for _, u := range a.c.uses {
			if it != nil && u.item == it {
				switch strings.ToUpper(columnType(u.table, lastName(arg))) {
				case "UINTEGER", "USMALLINT", "UTINYINT":
					return true
				}
			}
		}
	}
	return false
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

// manyGroups guesses whether the groups are too many to send every
// worker's to the coordinator: the top of an aggregation by a column of a
// table the workers divide, such as spend per device, top ten. Then each
// partition's top groups are all the coordinator needs.
func (a *aggregation) manyGroups() bool {
	if _, ok := a.topN(); !ok {
		return false
	}
	small := map[*fromItem]bool{}
	for _, u := range a.c.uses {
		if u.scope == a.c.scope && u != a.c.split && !slices.ContainsFunc(a.c.keyed, func(k *keyed) bool { return k.use == u }) {
			small[u.item] = true
		}
	}
	for _, g := range a.groups {
		large := false
		walk(g, func(n node) bool {
			if str(n["class"]) == "COLUMN_REF" {
				if it := resolveColumn(n, a.c.scope); it == nil || !small[it] {
					large = true
				}
			}
			return !large
		})
		if large {
			return true
		}
	}
	return false
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
			if combinable[name] == "sum" && name != "sum" {
				// Counts stay BIGINT, as one machine has them.
				text = "CAST(" + text + " AS BIGINT)"
			}
			if e, err = a.pl.p.expr(text); err != nil {
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
// the completed aggregates (final).
//
//	SELECT __g0, ..., coalesce(sum(__a0), 0) AS __f0, count(DISTINCT __dk0) AS __f1, ...
//	FROM __igloo_partials GROUP BY __g0, ... HAVING ... ORDER BY ... LIMIT ...
func (a *aggregation) groupStage() (*secondStage, error) {
	f, err := a.partialsSelect()
	if err != nil {
		return nil, err
	}
	items := list(f, "select_list")
	for k, ac := range a.aggs {
		e, err := a.combined(ac)
		if err != nil {
			return nil, err
		}
		items = append(items, aliased(e, "__f"+strconv.Itoa(k)))
		ac.final = k
	}
	f["select_list"] = items
	// HAVING and the first rows are worth applying early, when they can be
	// said in terms of the partial results alone; the coordinator applies
	// them again.
	if h := child(a.c.sel, "having"); h != nil {
		if e, err := a.rewrite(clone(h).(node), "having"); err == nil && overPartials(e) {
			f["having"] = e
		}
	}
	if mods, ok := a.firstRows(); ok {
		f["modifiers"] = mods
	}
	a.final = true
	return &secondStage{fragment: f, keys: a.groupColumns()}, nil
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
// partial results, cast to the call's own type; or, once a second stage
// completed the groups, that reads its result.
func (a *aggregation) combined(ac *aggCall) (node, error) {
	col := func(i int) string { return quoteIdent("__a" + strconv.Itoa(ac.partials[i])) }
	var text string
	switch {
	case a.final:
		// One row per group: any of its values is the value.
		text = "any_value(" + quoteIdent("__f"+strconv.Itoa(ac.final)) + ")"
	case ac.kind == combineCount:
		text = "coalesce(sum(" + col(0) + "), 0)"
	case ac.kind == combineAvg:
		text = "CAST(sum(" + col(0) + ") AS DOUBLE) / CAST(sum(" + col(1) + ") AS DOUBLE)"
	case ac.kind == combineSame:
		text = combinable[ac.name] + "(" + col(0) + ")"
	case ac.kind == combineBitmap:
		text = "rb_cardinality(rb_or_agg(" + col(0) + "))"
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
	if distinct == nil && limit == nil {
		// Every matching row would cross to the coordinator and wait on its
		// disk before the first reached the client: a hop, not a saving.
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
