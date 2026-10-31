// Package incremental refreshes materialized views and dynamic tables from
// what changed in the tables they read, rather than by running their
// queries again over every row.
//
// Plan works out from a query's parse (DuckDB's json_serialize_sql) whether
// and how its result can be brought up to date from the rows its tables
// gained and lost since the last refresh, and writes the SQL that does it;
// Diff works out from two snapshots of an Iceberg table which data files
// hold those rows. The caller (package pgwire) runs the SQL, with every
// table read at the snapshots it recorded, so that what a refresh read is
// exactly what the next one starts from.
//
// Supported, the rest refreshing in full:
//   - Filters and projections over a table or inner joins of tables
//     (outer joins while the side they fill with NULLs is unchanged): rows
//     appended to a table add the rows the query gives for them, by the
//     delta rule Δ(A⋈B) = ΔA⋈B' + A⋈ΔB, A the old table and B' the new.
//   - GROUP BY (or SELECT DISTINCT, or aggregates of the whole input) with
//     count(*), count(x), sum(x) and avg(x) of integers and decimals,
//     min(x) and max(x), and count(DISTINCT x) of integers kept in roaring
//     bitmaps (Igloo's rb extension). Each group's partial aggregates are
//     kept in a state table; a refresh aggregates the changed rows, merges
//     them into the state, and writes the groups they touched. Rows deleted
//     are subtracted when every aggregate can be (count, sum and avg).
//
// The result always equals a full refresh's, to the bit: sums of
// floating-point values, which a full refresh does not compute the same
// way twice, are not kept in parts.
package incremental

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// node is a node of DuckDB's parse tree.
type node = map[string]any

// Parser turns SQL into DuckDB's parse tree and back, with DuckDB.
type Parser interface {
	// Parse returns json_serialize_sql's document for query.
	Parse(ctx context.Context, query string) (string, error)
	// Print returns the SQL json_deserialize_sql writes for doc.
	Print(ctx context.Context, doc string) (string, error)
}

// Column is a column of a query's result, as DESCRIBE gives it.
type Column struct {
	Name, Type string
}

// Env is what planning needs to know about names, functions and types.
type Env struct {
	Parser Parser
	// Table returns the catalog table, schema and name as the catalog has
	// them, that a table reference's name (one to three parts, unquoted)
	// means; ok is false when it means anything else: a view, a temporary
	// table, a table of another database.
	Table func(ctx context.Context, parts []string) (schema, name string, ok bool, err error)
	// Columns lists the columns of a catalog table.
	Columns func(ctx context.Context, schema, name string) ([]string, error)
	// Describe returns the columns a query returns.
	Describe func(ctx context.Context, query string) ([]Column, error)
	// Aggregate reports whether a function, by its lower-case name, is an
	// aggregate, or a macro that may call one.
	Aggregate func(name string) bool
	// Unstable reports whether a function's value may differ from one run
	// to the next (now(), random(), current_setting(), ...).
	Unstable func(name string) bool
	// Bitmaps says roaring bitmaps (rb_agg, rb_or, rb_cardinality) are
	// available, for count(DISTINCT x).
	Bitmaps bool
}

// Source is a table a query reads, one for each time the query names it.
type Source struct {
	// Schema and Name are the catalog table's.
	Schema, Name string
	// Alias is what the query calls it.
	Alias string
	// Nullable says it is on a side of an outer join that the join fills
	// with NULLs: rows appended to it can remove rows of the result, so a
	// refresh after it changed is full.
	Nullable bool
}

// Table is the source's table as schema.name.
func (s Source) Table() string { return s.Schema + "." + s.Name }

// Plan is how a query's result is refreshed.
type Plan struct {
	// Reason says why the query cannot be refreshed incrementally; empty
	// when it can.
	Reason  string
	Sources []Source
	// Grouped says the query aggregates (GROUP BY, aggregates, DISTINCT);
	// Global that it aggregates its whole input, into one row.
	Grouped, Global bool

	env  Env
	root node
	refs []node
	// keys are the GROUP BY expressions; keyCols the column of the result
	// that holds each, -1 for none.
	keys    []node
	keyCols []int
	// args are the aggregates' arguments, aggs the aggregates and comps
	// the partial aggregates the state keeps.
	args  []node
	aggs  []aggregate
	comps []component
	// final are the result's columns, and having its HAVING, written over
	// the state (see final).
	final  []node
	having node
	// keyTypes and aggTypes are the keys' and aggregates' types.
	keyTypes, aggTypes, argTypes []string
	// templates are parsed expressions, by text.
	templates map[string]node
	// cols are the sources that have each column, by lower-case name.
	cols map[string][]int
}

// aggregate is an aggregate call of the query: count_star, count, sum,
// min, max, avg or count_distinct, of args[arg]; canon is its canonical
// parse, to find it again.
type aggregate struct {
	kind  string
	arg   int
	canon string
}

// component is a partial aggregate the state keeps, in the column name:
// n (the rows of the group), cnt (the non-NULL values of args[arg]), sum,
// min, max, or bm (a bitmap of the values).
type component struct {
	kind string
	arg  int
	name string
}

// Pin says how a refresh reads one of a plan's sources: at a snapshot, or
// through a temporary view of rows that changed.
type Pin struct {
	// Version is the snapshot to read, when View is empty.
	Version int64
	// View is a temporary view to read instead.
	View string
}

// unsupported ends planning with a reason.
type unsupported struct{ reason string }

func (u unsupported) Error() string { return u.reason }

func refuse(format string, args ...any) error { return unsupported{fmt.Sprintf(format, args...)} }

// Analyze plans query, one SELECT in DuckDB's dialect. A query that cannot
// be refreshed incrementally gets a Plan with its Reason; an error means
// the query could not be read at all.
func Analyze(ctx context.Context, env Env, query string) (*Plan, error) {
	p := &Plan{env: env, templates: map[string]node{}}
	err := p.analyze(ctx, query)
	var u unsupported
	if errors.As(err, &u) {
		p.Reason = u.reason
		return p, nil
	}
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Plan) analyze(ctx context.Context, query string) error {
	root, err := p.parse(ctx, query)
	if err != nil {
		return err
	}
	if typ := str(root["type"]); typ != "SELECT_NODE" {
		return refuse("it is not a plain SELECT (%s)", strings.ToLower(strings.TrimSuffix(typ, "_NODE")))
	}
	if m, ok := root["cte_map"].(node); ok && len(list(m["map"])) > 0 {
		return refuse("it has a WITH clause")
	}
	for _, k := range []string{"qualify", "sample"} {
		if root[k] != nil {
			return refuse("it has %s", strings.ToUpper(k))
		}
	}
	distinct := false
	var modifiers []any
	for _, m := range list(root["modifiers"]) {
		mod, _ := m.(node)
		switch str(mod["type"]) {
		case "ORDER_MODIFIER":
			// The order of a table's rows means nothing.
		case "DISTINCT_MODIFIER":
			if len(list(mod["distinct_on_targets"])) > 0 {
				return refuse("it has DISTINCT ON")
			}
			distinct = true
		default:
			return refuse("it has LIMIT or OFFSET")
		}
	}
	root["modifiers"] = modifiers
	p.root = root

	from, _ := root["from_table"].(node)
	if from == nil || str(from["type"]) == "EMPTY" {
		return refuse("it reads no table")
	}
	if err := p.sources(ctx, from, false); err != nil {
		return err
	}
	// Names that one source alone has are qualified with it, so that
	// expressions compare whatever the query wrote.
	cols := map[string][]int{}
	for i, s := range p.Sources {
		names, err := p.env.Columns(ctx, s.Schema, s.Name)
		if err != nil {
			return err
		}
		for _, n := range names {
			k := strings.ToLower(n)
			if !slices.Contains(cols[k], i) {
				cols[k] = append(cols[k], i)
			}
		}
	}
	p.cols = cols
	if err := p.qualifyAll(root, cols); err != nil {
		return err
	}
	if err := p.checkExpr(from, false); err != nil {
		return err
	}
	if w := root["where_clause"]; w != nil {
		if err := p.checkExpr(w, false); err != nil {
			return err
		}
		if p.hasAggregate(w) {
			return refuse("its WHERE clause has an aggregate")
		}
	}

	selects := list(root["select_list"])
	aggregates := false
	for _, s := range selects {
		if err := p.checkExpr(s, true); err != nil {
			return err
		}
		aggregates = aggregates || p.hasAggregate(s)
	}
	if h := root["having"]; h != nil {
		if err := p.checkExpr(h, true); err != nil {
			return err
		}
		aggregates = true
	}
	groups := list(root["group_expressions"])
	all := str(root["aggregate_handling"]) == "FORCE_AGGREGATES"
	if !aggregates && len(groups) == 0 && !distinct && !all {
		for _, s := range selects {
			if sn, _ := s.(node); str(sn["class"]) == "STAR" && (len(list(sn["exclude_list"])) > 0 || len(list(sn["replace_list"])) > 0) {
				return refuse("its select list has * with EXCLUDE or REPLACE")
			}
		}
		return nil
	}
	p.Grouped = true
	if distinct && (aggregates || len(groups) > 0) {
		return refuse("it has DISTINCT with GROUP BY or aggregates")
	}
	for _, s := range selects {
		if str(s.(node)["class"]) == "STAR" {
			return refuse("it aggregates and its select list has *")
		}
	}
	if sets := list(root["group_sets"]); len(sets) > 1 {
		return refuse("it has GROUPING SETS, ROLLUP or CUBE")
	}
	switch {
	case distinct, all:
		for _, s := range selects {
			if !p.hasAggregate(s) {
				p.keys = append(p.keys, s.(node))
			}
		}
	default:
		for _, g := range groups {
			k, err := p.groupKey(g.(node), selects)
			if err != nil {
				return err
			}
			p.keys = append(p.keys, k)
		}
	}
	p.Global = len(p.keys) == 0
	// count(*) of each group keeps it, and says when it is gone.
	p.comps = append(p.comps, component{kind: "n", arg: -1, name: "__c0"})
	for _, s := range selects {
		if err := p.collect(s); err != nil {
			return err
		}
	}
	if h := root["having"]; h != nil {
		if err := p.collect(h); err != nil {
			return err
		}
	}
	for _, s := range selects {
		f, err := p.rewriteFinal(ctx, s.(node))
		if err != nil {
			return err
		}
		f["alias"] = s.(node)["alias"]
		p.final = append(p.final, f)
	}
	if h, _ := root["having"].(node); h != nil {
		f, err := p.rewriteFinal(ctx, h)
		if err != nil {
			return err
		}
		p.having = f
	}
	p.keyCols = make([]int, len(p.keys))
	for i, k := range p.keys {
		p.keyCols[i] = -1
		ck := canon(k)
		for j, s := range selects {
			if canon(s) == ck {
				p.keyCols[i] = j
				break
			}
		}
	}
	return nil
}

// sources collects the tables from reads, in order, and checks its joins.
// nullable says from is on a side an outer join fills with NULLs.
func (p *Plan) sources(ctx context.Context, from node, nullable bool) error {
	switch t := str(from["type"]); t {
	case "BASE_TABLE":
		if from["at_clause"] != nil {
			return refuse("it reads a table at a snapshot (AT)")
		}
		if from["sample"] != nil {
			return refuse("it samples a table")
		}
		if len(list(from["column_name_alias"])) > 0 {
			return refuse("it renames a table's columns in FROM")
		}
		var parts []string
		for _, k := range []string{"catalog_name", "schema_name", "table_name"} {
			if v := str(from[k]); v != "" {
				parts = append(parts, v)
			}
		}
		schema, name, ok, err := p.env.Table(ctx, parts)
		if err != nil {
			return err
		}
		if !ok {
			return refuse("%s is not a table of the catalog (a view, or a table of another database)", strings.Join(parts, "."))
		}
		alias := str(from["alias"])
		if alias == "" {
			alias = str(from["table_name"])
		}
		p.Sources = append(p.Sources, Source{Schema: schema, Name: name, Alias: alias, Nullable: nullable})
		p.refs = append(p.refs, from)
		return nil
	case "JOIN":
		switch str(from["ref_type"]) {
		case "REGULAR", "CROSS", "NATURAL":
		default:
			return refuse("it has a %s join", strings.ToLower(str(from["ref_type"])))
		}
		left, _ := from["left"].(node)
		right, _ := from["right"].(node)
		var ln, rn bool
		switch str(from["join_type"]) {
		case "INNER":
		case "LEFT":
			rn = true
		case "RIGHT":
			ln = true
		case "OUTER", "FULL":
			ln, rn = true, true
		default:
			return refuse("it has a %s join", strings.ToLower(str(from["join_type"])))
		}
		if err := p.sources(ctx, left, nullable || ln); err != nil {
			return err
		}
		return p.sources(ctx, right, nullable || rn)
	default:
		return refuse("it reads a %s", strings.ToLower(strings.ReplaceAll(t, "_", " ")))
	}
}

// qualifyAll qualifies the query's column names (see qualify), and refuses
// names of three or more parts, which a table that stands in for another
// in a refresh would not bind.
func (p *Plan) qualifyAll(root node, cols map[string][]int) error {
	var err error
	var walk func(v any)
	walk = func(v any) {
		switch v := v.(type) {
		case node:
			if str(v["class"]) == "COLUMN_REF" {
				names := list(v["column_names"])
				switch {
				case len(names) == 1:
					if ix := cols[strings.ToLower(str(names[0]))]; len(ix) == 1 {
						v["column_names"] = []any{p.Sources[ix[0]].Alias, names[0]}
					}
				case len(names) >= 3 && err == nil:
					err = refuse("it names a column with its table's schema (%s)", joinNames(names))
				}
				return
			}
			if str(v["class"]) == "LAMBDA" {
				// A lambda's parameters are not columns.
				return
			}
			for k, c := range v {
				if k == "from_table" {
					continue
				}
				walk(c)
			}
		case []any:
			for _, c := range v {
				walk(c)
			}
		}
	}
	walk(root)
	if from, ok := root["from_table"].(node); ok {
		walkJoins(from, func(j node) {
			if c := j["condition"]; c != nil {
				walk(c)
			}
		})
	}
	return err
}

func walkJoins(from node, fn func(node)) {
	if str(from["type"]) != "JOIN" {
		return
	}
	fn(from)
	if l, ok := from["left"].(node); ok {
		walkJoins(l, fn)
	}
	if r, ok := from["right"].(node); ok {
		walkJoins(r, fn)
	}
}

func joinNames(names []any) string {
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = str(n)
	}
	return strings.Join(parts, ".")
}

// checkExpr refuses what an expression (or a join tree's conditions) has
// that a refresh cannot take: subqueries, window functions, parameters,
// unstable functions, and aggregates where aggregates is false.
func (p *Plan) checkExpr(v any, aggregates bool) error {
	switch v := v.(type) {
	case node:
		if t := str(v["type"]); t == "JOIN" {
			if c := v["condition"]; c != nil {
				if err := p.checkExpr(c, false); err != nil {
					return err
				}
				if p.hasAggregate(c) {
					return refuse("a join condition has an aggregate")
				}
			}
			for _, k := range []string{"left", "right"} {
				if err := p.checkExpr(v[k], false); err != nil {
					return err
				}
			}
			return nil
		} else if t == "BASE_TABLE" {
			return nil
		}
		switch c := str(v["class"]); c {
		case "SUBQUERY":
			return refuse("it has a subquery")
		case "WINDOW":
			return refuse("it has a window function")
		case "PARAMETER":
			return refuse("it has a parameter")
		case "FUNCTION":
			name := strings.ToLower(str(v["function_name"]))
			if p.env.Unstable(name) {
				return refuse("it calls %s, whose value can differ from one run to the next", name)
			}
			if slices.Contains([]string{"unnest", "generate_subscripts"}, name) {
				return refuse("it calls %s", name)
			}
			if p.env.Aggregate(name) && !aggregates {
				return refuse("it has an aggregate where none may be")
			}
		}
		for _, c := range v {
			if err := p.checkExpr(c, aggregates); err != nil {
				return err
			}
		}
	case []any:
		for _, c := range v {
			if err := p.checkExpr(c, aggregates); err != nil {
				return err
			}
		}
	}
	return nil
}

// hasAggregate reports whether an expression calls an aggregate.
func (p *Plan) hasAggregate(v any) bool {
	switch v := v.(type) {
	case node:
		if str(v["class"]) == "FUNCTION" && p.env.Aggregate(strings.ToLower(str(v["function_name"]))) {
			return true
		}
		for _, c := range v {
			if p.hasAggregate(c) {
				return true
			}
		}
	case []any:
		for _, c := range v {
			if p.hasAggregate(c) {
				return true
			}
		}
	}
	return false
}

// groupKey returns the expression a GROUP BY item groups by: a select-list
// item for a position (GROUP BY 1) or an item's alias, or the item itself.
func (p *Plan) groupKey(g node, selects []any) (node, error) {
	if str(g["class"]) == "CONSTANT" {
		v, _ := g["value"].(node)
		n, err := strconv.Atoi(fmt.Sprint(v["value"]))
		if err != nil || n < 1 || n > len(selects) {
			return nil, refuse("it groups by a constant")
		}
		return selects[n-1].(node), nil
	}
	if e, ok, err := p.aliasOf(g, selects); ok || err != nil {
		return e, err
	}
	if p.hasAggregate(g) {
		return nil, refuse("it groups by an aggregate")
	}
	return g, nil
}

// aliasOf returns the select-list item that a one-part name means, when it
// names an item's alias and no column of the sources: DuckDB binds GROUP
// BY and HAVING names, and the select list's own, so. A name that is both
// is refused, as DuckDB's choice between them is not Igloo's to guess.
func (p *Plan) aliasOf(n node, selects []any) (node, bool, error) {
	if str(n["class"]) != "COLUMN_REF" {
		return nil, false, nil
	}
	names := list(n["column_names"])
	if len(names) != 1 {
		return nil, false, nil
	}
	name := str(names[0])
	for _, s := range selects {
		sn := s.(node)
		if !strings.EqualFold(str(sn["alias"]), name) || canon(sn) == canon(n) {
			continue
		}
		if len(p.cols[strings.ToLower(name)]) > 0 {
			return nil, false, refuse("%s names both a column and an expression of the select list", name)
		}
		return sn, true, nil
	}
	return nil, false, nil
}

// collect registers the aggregates an expression calls, with their
// arguments and the partial aggregates they need.
func (p *Plan) collect(v any) error {
	switch v := v.(type) {
	case node:
		if str(v["class"]) == "FUNCTION" && p.env.Aggregate(strings.ToLower(str(v["function_name"]))) {
			return p.register(v)
		}
		for _, c := range v {
			if err := p.collect(c); err != nil {
				return err
			}
		}
	case []any:
		for _, c := range v {
			if err := p.collect(c); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Plan) register(f node) error {
	c := canon(f)
	if slices.ContainsFunc(p.aggs, func(a aggregate) bool { return a.canon == c }) {
		return nil
	}
	name := strings.ToLower(str(f["function_name"]))
	if f["filter"] != nil {
		return refuse("%s has a FILTER clause", name)
	}
	if ob, _ := f["order_bys"].(node); ob != nil && len(list(ob["orders"])) > 0 {
		return refuse("%s has an ORDER BY", name)
	}
	if f["export_state"] == true {
		return refuse("%s exports its state", name)
	}
	args := list(f["children"])
	for _, a := range args {
		if p.hasAggregate(a) {
			return refuse("an aggregate's argument has an aggregate")
		}
	}
	distinct := f["distinct"] == true
	kind := ""
	switch {
	case name == "count_star" && len(args) == 0:
		kind = "count_star"
	case name == "count" && len(args) == 1 && distinct:
		kind = "count_distinct"
	case name == "count" && len(args) == 1:
		kind = "count"
	case (name == "sum" || name == "min" || name == "max") && len(args) == 1 && !distinct:
		kind = name
	case (name == "avg" || name == "mean") && len(args) == 1 && !distinct:
		kind = "avg"
	case name == "min" || name == "max":
		// min(DISTINCT x) is min(x).
		if len(args) == 1 {
			kind = name
		}
	}
	if kind == "" {
		if distinct {
			return refuse("it has %s(DISTINCT ...)", name)
		}
		return refuse("it has the aggregate %s, which cannot be kept in parts", name)
	}
	a := aggregate{kind: kind, arg: -1, canon: c}
	if len(args) == 1 {
		a.arg = p.argIndex(args[0].(node))
	}
	p.aggs = append(p.aggs, a)
	need := map[string][]string{
		"count_star": {"n"}, "count": {"cnt"}, "count_distinct": {"bm"}, "sum": {"sum", "cnt"}, "avg": {"sum", "cnt"},
		"min": {"min"}, "max": {"max"},
	}[kind]
	for _, k := range need {
		if k == "n" || slices.ContainsFunc(p.comps, func(c component) bool { return c.kind == k && c.arg == a.arg }) {
			continue
		}
		p.comps = append(p.comps, component{kind: k, arg: a.arg, name: "__c" + strconv.Itoa(len(p.comps))})
	}
	return nil
}

func (p *Plan) argIndex(a node) int {
	c := canon(a)
	for i, x := range p.args {
		if canon(x) == c {
			return i
		}
	}
	p.args = append(p.args, a)
	return len(p.args) - 1
}

// comp returns the column of the component of kind for arg.
func (p *Plan) comp(kind string, arg int) string {
	for _, c := range p.comps {
		if c.kind == kind && (kind == "n" || c.arg == arg) {
			return c.name
		}
	}
	panic("incremental: no component " + kind)
}

// rewriteFinal writes an expression of the select list or HAVING over the
// state, aliased __s: aggregates and GROUP BY expressions become its
// columns, cast to the types the query gives them. Any other column is
// refused: the query reads it outside an aggregate without grouping by it.
func (p *Plan) rewriteFinal(ctx context.Context, n node) (node, error) {
	c := canon(n)
	for i, k := range p.keys {
		if canon(k) == c {
			return p.expr(ctx, fmt.Sprintf("__s.__k%d", i))
		}
	}
	if str(n["class"]) == "FUNCTION" && p.env.Aggregate(strings.ToLower(str(n["function_name"]))) {
		for i, a := range p.aggs {
			if a.canon == c {
				return p.expr(ctx, fmt.Sprintf("__s.__g%d", i))
			}
		}
		return nil, fmt.Errorf("incremental: aggregate not registered")
	}
	if str(n["class"]) == "COLUMN_REF" {
		if e, ok, err := p.aliasOf(n, list(p.root["select_list"])); err != nil {
			return nil, err
		} else if ok && canon(e) != c {
			return p.rewriteFinal(ctx, e)
		}
		return nil, refuse("its select list or HAVING reads %s outside an aggregate", joinNames(list(n["column_names"])))
	}
	out := node{}
	for k, v := range n {
		r, err := p.rewriteChildren(ctx, v)
		if err != nil {
			return nil, err
		}
		out[k] = r
	}
	return out, nil
}

func (p *Plan) rewriteChildren(ctx context.Context, v any) (any, error) {
	switch v := v.(type) {
	case node:
		if _, ok := v["class"]; ok {
			return p.rewriteFinal(ctx, v)
		}
		out := node{}
		for k, c := range v {
			r, err := p.rewriteChildren(ctx, c)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, c := range v {
			r, err := p.rewriteChildren(ctx, c)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	}
	return v, nil
}

// Types reads the types of the plan's keys, aggregates and arguments, and
// refuses aggregates whose values a refresh could not keep exactly: sums
// and averages of floating-point values, averages of decimals, and
// count(DISTINCT x) of anything but integers, or without bitmaps.
func (p *Plan) Types(ctx context.Context) error {
	if p.Reason != "" || !p.Grouped {
		return nil
	}
	pins := make([]Pin, len(p.refs))
	for i := range pins {
		pins[i].Version = -1
	}
	input, err := p.Input(ctx, pins, "")
	if err != nil {
		return err
	}
	cols, err := p.env.Describe(ctx, input)
	if err != nil {
		return err
	}
	if len(cols) != len(p.keys)+len(p.args)+1 {
		return fmt.Errorf("incremental: described %d columns, want %d", len(cols), len(p.keys)+len(p.args)+1)
	}
	for i := range p.keys {
		p.keyTypes = append(p.keyTypes, cols[i].Type)
	}
	for i := range p.args {
		p.argTypes = append(p.argTypes, cols[len(p.keys)+i].Type)
	}
	// The aggregates' own types, as the query computes them.
	if len(p.aggs) > 0 {
		agg, err := p.aggregatesQuery(ctx)
		if err != nil {
			return err
		}
		acols, err := p.env.Describe(ctx, agg)
		if err != nil {
			return err
		}
		if len(acols) != len(p.aggs) {
			return fmt.Errorf("incremental: described %d aggregates, want %d", len(acols), len(p.aggs))
		}
		for _, c := range acols {
			p.aggTypes = append(p.aggTypes, c.Type)
		}
	}
	for _, a := range p.aggs {
		if a.arg < 0 {
			continue
		}
		t := strings.ToUpper(p.argTypes[a.arg])
		switch a.kind {
		case "sum":
			if !exactNumber(t) {
				p.Reason = fmt.Sprintf("it sums values of type %s, whose sum depends on the order it is computed in", t)
				return nil
			}
		case "avg":
			if !integer(t) {
				p.Reason = fmt.Sprintf("it averages values of type %s, which cannot be kept in parts exactly", t)
				return nil
			}
		case "count_distinct":
			if !integer(t) {
				p.Reason = fmt.Sprintf("it counts distinct values of type %s; only integers' are kept in bitmaps", t)
				return nil
			}
			if !p.env.Bitmaps {
				p.Reason = "it counts distinct values, which need the rb extension's bitmaps, and the extension is not loaded"
				return nil
			}
		}
	}
	return nil
}

func exactNumber(t string) bool { return integer(t) || strings.HasPrefix(t, "DECIMAL") }

func integer(t string) bool {
	switch t {
	case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT":
		return true
	}
	return false
}

// Invertible reports whether rows deleted from the plan's tables can be
// subtracted from its result: a projection's rows cannot be told apart, nor
// can a minimum, a maximum or a bitmap lose a value.
func (p *Plan) Invertible() bool {
	if !p.Grouped {
		return false
	}
	for _, a := range p.aggs {
		switch a.kind {
		case "min", "max", "count_distinct":
			return false
		}
	}
	return true
}

// aggregatesQuery is a query that returns the plan's aggregates, as the
// query computes them, for their types.
func (p *Plan) aggregatesQuery(ctx context.Context) (string, error) {
	q := clone(p.root)
	var sel []any
	for i, a := range p.aggs {
		f, err := p.findAggregate(ctx, a)
		if err != nil {
			return "", err
		}
		f = clone(f)
		f["alias"] = fmt.Sprintf("__g%d", i)
		sel = append(sel, f)
	}
	q["select_list"] = sel
	q["having"] = nil
	q["modifiers"] = []any{}
	if len(p.keys) == 0 {
		q["group_expressions"], q["group_sets"] = []any{}, []any{}
	} else {
		var groups, set []any
		for i, k := range p.keys {
			groups = append(groups, clone(k))
			set = append(set, i)
		}
		for _, g := range groups {
			g.(node)["alias"] = ""
		}
		q["group_expressions"], q["group_sets"] = groups, []any{set}
	}
	q["aggregate_handling"] = "STANDARD_HANDLING"
	return p.print(ctx, q)
}

// findAggregate finds the call of an aggregate in the query.
func (p *Plan) findAggregate(_ context.Context, a aggregate) (node, error) {
	var found node
	var walk func(v any)
	walk = func(v any) {
		if found != nil {
			return
		}
		switch v := v.(type) {
		case node:
			if str(v["class"]) == "FUNCTION" && canon(v) == a.canon {
				found = v
				return
			}
			for _, c := range v {
				walk(c)
			}
		case []any:
			for _, c := range v {
				walk(c)
			}
		}
	}
	walk(p.root["select_list"])
	walk(p.root["having"])
	if found == nil {
		return nil, errors.New("incremental: aggregate not found")
	}
	return found, nil
}

// pinned returns a copy of the query with its sources read as pins say.
func (p *Plan) pinned(ctx context.Context, pins []Pin) (node, error) {
	if len(pins) != len(p.refs) {
		return nil, fmt.Errorf("incremental: %d pins for %d sources", len(pins), len(p.refs))
	}
	q := clone(p.root)
	i := 0
	var visit func(t node) error
	visit = func(t node) error {
		switch str(t["type"]) {
		case "BASE_TABLE":
			pin := pins[i]
			src := p.Sources[i]
			i++
			if pin.View != "" {
				t["catalog_name"], t["schema_name"], t["table_name"] = "temp", "main", pin.View
				t["alias"] = src.Alias
				return nil
			}
			if pin.Version < 0 {
				return nil
			}
			at, err := p.expr(ctx, "1 FROM t AT (VERSION => "+strconv.FormatInt(pin.Version, 10)+")")
			if err != nil {
				return err
			}
			t["at_clause"] = at
			return nil
		case "JOIN":
			if err := visit(t["left"].(node)); err != nil {
				return err
			}
			return visit(t["right"].(node))
		}
		return fmt.Errorf("incremental: unexpected %s", str(t["type"]))
	}
	if err := visit(q["from_table"].(node)); err != nil {
		return nil, err
	}
	return q, nil
}

// Query returns the query, its sources read as pins say: a projection's
// rows for the rows of its views, or its whole result at its snapshots.
func (p *Plan) Query(ctx context.Context, pins []Pin) (string, error) {
	q, err := p.pinned(ctx, pins)
	if err != nil {
		return "", err
	}
	return p.print(ctx, q)
}

// Input returns the rows an aggregating query aggregates, its sources read
// as pins say: its GROUP BY expressions as __k0, ..., its aggregates'
// arguments as __a0, ..., and as __s the sign of each row: 1, or the
// column __igloo_sign of the source with alias sign, which is -1 for rows
// deleted.
func (p *Plan) Input(ctx context.Context, pins []Pin, sign string) (string, error) {
	q, err := p.pinned(ctx, pins)
	if err != nil {
		return "", err
	}
	var sel []any
	for i, k := range p.keys {
		c := clone(k)
		c["alias"] = fmt.Sprintf("__k%d", i)
		sel = append(sel, c)
	}
	for i, a := range p.args {
		c := clone(a)
		c["alias"] = fmt.Sprintf("__a%d", i)
		sel = append(sel, c)
	}
	s, err := p.expr(ctx, "1")
	if sign != "" {
		s, err = p.expr(ctx, quoteIdent(sign)+".__igloo_sign")
	}
	if err != nil {
		return "", err
	}
	s = clone(s)
	s["alias"] = "__s"
	sel = append(sel, s)
	q["select_list"] = sel
	q["group_expressions"], q["group_sets"], q["having"] = []any{}, []any{}, nil
	q["aggregate_handling"] = "STANDARD_HANDLING"
	q["modifiers"] = []any{}
	return p.print(ctx, q)
}

// Keys returns the names of the state's key columns.
func (p *Plan) Keys() []string {
	out := make([]string, len(p.keys))
	for i := range p.keys {
		out[i] = fmt.Sprintf("__k%d", i)
	}
	return out
}

// KeyColumns returns, for each GROUP BY expression, the column of the
// result that holds it, or -1; ok is false if any has none.
func (p *Plan) KeyColumns() (cols []int, ok bool) {
	return p.keyCols, !slices.Contains(p.keyCols, -1)
}

// Partials aggregates input (see Input) by its keys into the partial
// aggregates of each group: the state's components, sums split into the
// sums of rows added (_p) and deleted (_m), and with each bitmap the
// count of values it could not hold (_x). With skipEmpty, an aggregate of
// the whole input gives no row when the input has none.
func (p *Plan) Partials(input string, skipEmpty bool) string {
	var cols []string
	for i := range p.keys {
		cols = append(cols, fmt.Sprintf("__k%d", i))
	}
	for _, c := range p.comps {
		a := fmt.Sprintf("__a%d", c.arg)
		switch c.kind {
		case "n":
			cols = append(cols, "CAST(sum(__s) AS BIGINT) AS "+c.name)
		case "cnt":
			cols = append(cols, "CAST(sum(CASE WHEN "+a+" IS NULL THEN 0 ELSE __s END) AS BIGINT) AS "+c.name)
		case "sum":
			cols = append(cols, "sum("+a+") FILTER (WHERE __s > 0) AS "+c.name+"_p", "sum("+a+") FILTER (WHERE __s < 0) AS "+c.name+"_m")
		case "min", "max":
			cols = append(cols, c.kind+"("+a+") AS "+c.name)
		case "bm":
			cols = append(cols, "rb_agg(TRY_CAST("+a+" AS UINTEGER)) AS "+c.name,
				"count(*) FILTER (WHERE "+a+" IS NOT NULL AND TRY_CAST("+a+" AS UINTEGER) IS NULL) AS "+c.name+"_x")
		}
	}
	q := "SELECT " + strings.Join(cols, ", ") + " FROM (" + input + ") AS __i"
	if len(p.keys) > 0 {
		q += " GROUP BY " + strings.Join(p.Keys(), ", ")
	} else if skipEmpty {
		q += " HAVING count(*) > 0"
	}
	return q
}

// Overflow returns a query, over partials rel, that counts what makes the
// refresh unable to keep the state exactly: values a bitmap could not
// hold. Empty when nothing can.
func (p *Plan) Overflow(rel string) string {
	var terms []string
	for _, c := range p.comps {
		if c.kind == "bm" {
			terms = append(terms, "coalesce(sum("+c.name+"_x), 0)")
		}
	}
	if len(terms) == 0 {
		return ""
	}
	return "SELECT CAST(" + strings.Join(terms, " + ") + " AS BIGINT) FROM " + rel
}

// AverageLimit returns a query, over state rel, that counts the groups
// whose sums averages divide are too large for a double to hold exactly,
// which then refresh in full. Empty when no average does.
func (p *Plan) AverageLimit(rel string) string {
	var conds []string
	for _, a := range p.aggs {
		if a.kind == "avg" {
			conds = append(conds, "abs(CAST("+p.comp("sum", a.arg)+" AS HUGEINT)) >= 9007199254740992")
		}
	}
	if len(conds) == 0 {
		return ""
	}
	return "SELECT count(*) FROM " + rel + " WHERE " + strings.Join(conds, " OR ")
}

// StateColumns returns the state table's columns and their types: the
// keys' and the components' as Iceberg can hold them.
func (p *Plan) StateColumns() []Column {
	var out []Column
	for i, k := range p.Keys() {
		out = append(out, Column{Name: k, Type: storable(p.keyTypes[i])})
	}
	for _, c := range p.comps {
		t := "BIGINT"
		switch c.kind {
		case "sum":
			t = storable(sumType(p.argTypes[c.arg]))
		case "min", "max":
			t = storable(p.argTypes[c.arg])
		case "bm":
			t = "BLOB"
		}
		out = append(out, Column{Name: c.name, Type: t})
	}
	return out
}

// sumType is the type DuckDB sums values of type t in.
func sumType(t string) string {
	t = strings.ToUpper(t)
	if strings.HasPrefix(t, "DECIMAL") {
		if _, rest, ok := strings.Cut(t, ","); ok {
			return "DECIMAL(38," + strings.TrimSpace(rest)
		}
		return "DECIMAL(38,0)"
	}
	return "HUGEINT"
}

// storable is the type an Iceberg table keeps values of DuckDB type t in.
func storable(t string) string {
	switch strings.ToUpper(t) {
	case "TINYINT", "SMALLINT", "UTINYINT", "USMALLINT":
		return "INTEGER"
	case "UINTEGER":
		return "BIGINT"
	case "UBIGINT":
		return "DECIMAL(20,0)"
	case "HUGEINT", "UHUGEINT":
		return "DECIMAL(38,0)"
	}
	return t
}

// StateRows returns the state's rows from partials rel, of rows added only.
func (p *Plan) StateRows(rel string) string {
	var cols []string
	cols = append(cols, p.Keys()...)
	for _, c := range p.comps {
		if c.kind == "sum" {
			cols = append(cols, c.name+"_p AS "+c.name)
			continue
		}
		cols = append(cols, c.name)
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM " + rel
}

// Merge returns the state's new rows for the groups of delta, partials of
// the rows that changed, merged with state's rows, aliased s.
func (p *Plan) Merge(state, delta string) string {
	cols := make([]string, 0, len(p.keys)+len(p.comps))
	for _, k := range p.Keys() {
		cols = append(cols, "d."+k)
	}
	for _, c := range p.comps {
		s, d := "s."+c.name, "d."+c.name
		switch c.kind {
		case "n", "cnt":
			cols = append(cols, "coalesce("+s+", 0) + "+d+" AS "+c.name)
		case "sum":
			cols = append(cols, "coalesce("+s+", 0) + coalesce("+d+"_p, 0) - coalesce("+d+"_m, 0) AS "+c.name)
		case "min":
			cols = append(cols, "least("+s+", "+d+") AS "+c.name)
		case "max":
			cols = append(cols, "greatest("+s+", "+d+") AS "+c.name)
		case "bm":
			cols = append(cols, "CASE WHEN "+s+" IS NULL THEN "+d+" WHEN "+d+" IS NULL THEN "+s+" ELSE rb_or("+s+", "+d+") END AS "+c.name)
		}
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM " + delta + " AS d LEFT JOIN " + state + " AS s ON " + p.Match("s", "d")
}

// Match is the condition that rows a and b, of state-shaped relations, are
// of the same group: their keys are not distinct.
func (p *Plan) Match(a, b string) string {
	if len(p.keys) == 0 {
		return "true"
	}
	var conds []string
	for _, k := range p.Keys() {
		conds = append(conds, a+"."+k+" IS NOT DISTINCT FROM "+b+"."+k)
	}
	return strings.Join(conds, " AND ")
}

// Alive is the condition that a state row of alias a has rows: a group
// whose rows were all deleted is gone, but an aggregate of the whole
// input always has its one row.
func (p *Plan) Alive(a string) string {
	if p.Global {
		return "true"
	}
	return a + ".__c0 > 0"
}

// Final returns the query's result from rel, a state-shaped relation.
func (p *Plan) Final(ctx context.Context, rel string) (string, error) {
	q, err := p.parse(ctx, "SELECT 1 FROM __igloo_src AS __s")
	if err != nil {
		return "", err
	}
	var sel []any
	for _, f := range p.final {
		sel = append(sel, clone(f))
	}
	q["select_list"] = sel
	if p.having != nil {
		q["where_clause"] = clone(p.having)
	}
	text, err := p.print(ctx, q)
	if err != nil {
		return "", err
	}
	// The keys and aggregates, as the query gives them, from the state.
	var cols []string
	for i, k := range p.Keys() {
		cols = append(cols, "CAST(__r."+k+" AS "+p.keyTypes[i]+") AS "+k)
	}
	for i, a := range p.aggs {
		var e string
		switch a.kind {
		case "count_star":
			e = "__r.__c0"
		case "count":
			e = "__r." + p.comp("cnt", a.arg)
		case "sum":
			e = "CASE WHEN __r." + p.comp("cnt", a.arg) + " > 0 THEN __r." + p.comp("sum", a.arg) + " END"
		case "min", "max":
			e = "__r." + p.comp(a.kind, a.arg)
		case "avg":
			cnt := "__r." + p.comp("cnt", a.arg)
			e = "CASE WHEN " + cnt + " > 0 THEN CAST(__r." + p.comp("sum", a.arg) + " AS DOUBLE) / " + cnt + " END"
		case "count_distinct":
			e = "rb_cardinality(__r." + p.comp("bm", a.arg) + ")"
		}
		cols = append(cols, "CAST("+e+" AS "+p.aggTypes[i]+") AS __g"+strconv.Itoa(i))
	}
	return "WITH __igloo_src AS (SELECT " + strings.Join(cols, ", ") + " FROM (" + rel + ") AS __r) " + text, nil
}

// parse parses query, one SELECT, into its node.
func (p *Plan) parse(ctx context.Context, query string) (node, error) {
	raw, err := p.env.Parser.Parse(ctx, query)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var doc node
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	if doc["error"] == true {
		return nil, fmt.Errorf("parse: %v", doc["error_message"])
	}
	stmts := list(doc["statements"])
	if len(stmts) != 1 {
		return nil, refuse("it is not one statement")
	}
	stmt, _ := stmts[0].(node)
	if len(list(stmt["named_param_map"])) > 0 {
		return nil, refuse("it has parameters")
	}
	n, _ := stmt["node"].(node)
	if n == nil {
		return nil, errors.New("parse: no query node")
	}
	return n, nil
}

// print writes a query node as SQL.
func (p *Plan) print(ctx context.Context, n node) (string, error) {
	doc := node{"error": false, "statements": []any{node{"node": n, "named_param_map": []any{}}}}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		return "", err
	}
	return p.env.Parser.Print(ctx, b.String())
}

// expr returns the parse of an expression, the first item of SELECT
// text's select list, or, for text with FROM, the first table's AT clause.
func (p *Plan) expr(ctx context.Context, text string) (node, error) {
	if n, ok := p.templates[text]; ok {
		return clone(n), nil
	}
	q, err := p.parse(ctx, "SELECT "+text)
	if err != nil {
		return nil, err
	}
	var n node
	if strings.Contains(text, " FROM ") {
		n, _ = q["from_table"].(node)["at_clause"].(node)
	} else {
		n, _ = list(q["select_list"])[0].(node)
	}
	if n == nil {
		return nil, fmt.Errorf("incremental: no expression in %q", text)
	}
	p.templates[text] = n
	return clone(n), nil
}

// canon is an expression's parse as text, without where and how it was
// written: its location, alias and the case of names.
func canon(v any) string {
	var strip func(v any) any
	strip = func(v any) any {
		switch v := v.(type) {
		case node:
			out := node{}
			for k, c := range v {
				switch k {
				case "query_location", "alias":
					continue
				case "column_names":
					var names []any
					for _, n := range list(c) {
						names = append(names, strings.ToLower(str(n)))
					}
					out[k] = names
					continue
				}
				out[k] = strip(c)
			}
			return out
		case []any:
			out := make([]any, len(v))
			for i, c := range v {
				out[i] = strip(c)
			}
			return out
		}
		return v
	}
	b, _ := json.Marshal(strip(v))
	return string(b)
}

// clone deep-copies a node.
func clone(n node) node {
	return cloneAny(n).(node)
}

func cloneAny(v any) any {
	switch v := v.(type) {
	case node:
		out := make(node, len(v))
		for k, c := range v {
			out[k] = cloneAny(c)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, c := range v {
			out[i] = cloneAny(c)
		}
		return out
	}
	return v
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func list(v any) []any {
	l, _ := v.([]any)
	return l
}

// quoteIdent quotes an identifier for DuckDB.
func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// QuoteIdent quotes an identifier for DuckDB.
func QuoteIdent(s string) string { return quoteIdent(s) }
