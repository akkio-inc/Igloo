package cleanroom

import (
	"context"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// A template is one SELECT over the room's datasets, which Igloo checks
// node by node on DuckDB's parse of it. Anything it does not know is
// refused, so new DuckDB syntax is closed until someone decides it is
// safe. What it accepts:
//
//   - FROM names datasets of the room, each perhaps with an alias, joined
//     with INNER, LEFT, RIGHT or FULL JOIN ... ON. Every join condition
//     equates an identifier of a dataset on one side of the join with one
//     of a dataset on the other (a.id = b.id), and may add conditions on
//     dimensions. No subqueries, CTEs, table functions, set operations,
//     USING, NATURAL, ASOF or LATERAL.
//   - Every column is written alias.column, and must be in its dataset's
//     policy. Identifiers appear only in join conditions and as the bare
//     argument of count, count(DISTINCT ...), approx_count_distinct and
//     a sketch's estimate (datasketch_hll_estimate(datasketch_hll(12,
//     a.id))): never in a result, a filter or a GROUP BY, never inside
//     any other function (substring, hash, list, min, max, any_value...).
//   - Dimensions (the policy's GROUP BY columns) may be shown, grouped by
//     and filtered on, through a short list of scalar functions, but not
//     aggregated: the min or max of one is one person's value.
//   - Aggregates take a bare column, never an expression: count(*), counts
//     of identifiers, and the aggregates a policy allows of its other
//     columns, such as sum(spend). A CASE inside an aggregate would pick
//     out a part of a group that no threshold counts; FILTER (WHERE ...) on
//     dimensions does that instead, and thresholds count it. Across
//     parties, a measure is only summed, within its policy's bounds and
//     with every party's noise (see crossPartyMeasure).
//   - The result is aggregated: GROUP BY dimensions, without ROLLUP, CUBE or
//     GROUPING SETS (a total less the groups shown is the groups left
//     out), or compute only aggregates. Every result column has a name.
//     HAVING, QUALIFY, window functions and DISTINCT may not be used.
//   - Parameters ($name) are declared with a type, appear only in WHERE,
//     each in a condition (of those ANDed together) on one dataset, and
//     are bound as values when the template runs.
//   - ORDER BY names result columns or their positions, and LIMIT is a
//     number.

// Check checks t against the room's datasets, and returns the datasets it
// reads.
func (p *Parser) Check(ctx context.Context, t Template, datasets []*Dataset) ([]*Dataset, error) {
	a, err := p.analyze(ctx, t, datasets)
	if err != nil {
		return nil, err
	}
	return a.datasets(), nil
}

// Databases name, in SQL, where datasets are: the Iceberg catalog, and
// DuckDB's own database, where external tables are.
type Databases struct{ Catalog, Local string }

// Plan checks t against the room's datasets as they are now, and plans a
// run of it with values for its parameters, reading the datasets in dbs.
func (p *Parser) Plan(ctx context.Context, t Template, datasets []*Dataset, dbs Databases, values map[string]any) (*Plan, error) {
	a, err := p.analyze(ctx, t, datasets)
	if err != nil {
		return nil, err
	}
	return p.plan(ctx, a, dbs, t.Parameters, values)
}

func (p *Parser) analyze(ctx context.Context, t Template, datasets []*Dataset) (*analysis, error) {
	if err := CheckParameters(t.Parameters); err != nil {
		return nil, err
	}
	stmt, err := p.parse(ctx, t.SQL)
	if err != nil {
		return nil, err
	}
	a, err := analyze(stmt, datasets, t.Parameters)
	if err != nil {
		return nil, err
	}
	used := map[string]bool{}
	walk(obj(stmt["node"]), func(n node) node {
		if str(n["class"]) == "PARAMETER" {
			used[str(n["identifier"])] = true
		}
		return nil
	})
	for _, q := range t.Parameters {
		if !used[q.Name] {
			return nil, invalid("parameter %s is declared but the template does not use it", q.Name)
		}
	}
	return a, nil
}

// analysis is what Igloo learned of a template while checking it.
type analysis struct {
	// stmt is the parse: {"node": ..., "named_param_map": ...}.
	stmt node
	// refs are the datasets FROM reads, in order.
	refs []*ref
	// outputs are the result's column names.
	outputs []string
	// filters are the distinct predicates of the aggregates' FILTER
	// clauses. Each picks out a part of a result row, whose identifiers
	// the threshold counts as well.
	filters []node
	// keys are the expressions that tell a result's groups apart: GROUP
	// BY's, or GROUP BY ALL's result columns of dimensions.
	keys []node
	// chosen are the conditions of WHERE that have parameters, by the
	// alias of the one dataset each reads: what a requester chooses at
	// run time.
	chosen map[string][]node
	// crossParty says FROM joins datasets of different parties, whose
	// sums of measures are bounded per person and noised (see Plan).
	crossParty bool
	order      []orderItem
	// limit is the template's LIMIT, or -1.
	limit int64
}

// datasets names the datasets the template reads, each once.
func (a *analysis) datasets() []*Dataset {
	var out []*Dataset
	for _, r := range a.refs {
		if !slices.Contains(out, r.dataset) {
			out = append(out, r.dataset)
		}
	}
	return out
}

type ref struct {
	alias   string
	dataset *Dataset
}

type orderItem struct {
	// output is the result column, by index.
	output int
	desc   bool
	// nulls is "", "NULLS FIRST" or "NULLS LAST".
	nulls string
}

// level says what an expression is a value of.
type level int

const (
	// levelConstant are literals and parameters, and expressions of
	// nothing else.
	levelConstant level = iota
	// levelRow is a value of each row: dimensions and expressions of them.
	levelRow
	// levelGroup is an aggregate, or an expression of aggregates.
	levelGroup
)

// place says where an expression is, and what it may have there.
type place struct {
	name       string
	params     bool
	aggregates bool
}

type analyzer struct {
	datasets map[string]*Dataset
	params   map[string]Parameter
	aliases  map[string]*ref
	a        *analysis
	filters  map[string]bool
}

// selectKeys are the parts of a SELECT node; anything else is refused.
var selectKeys = []string{"type", "modifiers", "cte_map", "select_list", "from_table", "where_clause",
	"group_expressions", "group_sets", "aggregate_handling", "having", "sample", "qualify"}

// analyze checks stmt, a template's parse, against the room's datasets and
// the template's declared parameters.
func analyze(stmt node, datasets []*Dataset, params []Parameter) (*analysis, error) {
	z := &analyzer{datasets: map[string]*Dataset{}, params: map[string]Parameter{}, aliases: map[string]*ref{},
		a: &analysis{stmt: stmt, limit: -1, chosen: map[string][]node{}}, filters: map[string]bool{}}
	for _, d := range datasets {
		z.datasets[d.Name] = d
	}
	for _, p := range params {
		z.params[p.Name] = p
	}
	root := obj(stmt["node"])
	if str(root["type"]) != "SELECT_NODE" {
		return nil, invalid("a template is a single SELECT, without UNION, INTERSECT, EXCEPT or VALUES")
	}
	for k, v := range root {
		if !slices.Contains(selectKeys, k) && !empty(v) {
			return nil, invalid("templates cannot use %s", strings.ToLower(strings.ReplaceAll(k, "_", " ")))
		}
	}
	switch {
	case len(arr(obj(root["cte_map"])["map"])) > 0:
		return nil, invalid("templates cannot have WITH clauses; read the datasets directly")
	case !empty(root["having"]):
		return nil, invalid("templates cannot have HAVING")
	case !empty(root["qualify"]):
		return nil, invalid("templates cannot have QUALIFY")
	case !empty(root["sample"]):
		return nil, invalid("templates cannot sample")
	}
	if _, err := z.from(obj(root["from_table"])); err != nil {
		return nil, err
	}
	if len(z.a.refs) == 0 {
		return nil, invalid("a template reads at least one of the room's datasets")
	}
	for _, r := range z.a.refs {
		z.a.crossParty = z.a.crossParty || r.dataset.Collaborator != z.a.refs[0].dataset.Collaborator
	}

	aggregated := false
	var levels []level
	items := arr(root["select_list"])
	for _, item := range items {
		n := obj(item)
		v, err := z.expr(n, place{name: "the SELECT list", aggregates: true})
		if err != nil {
			return nil, err
		}
		aggregated = aggregated || v == levelGroup
		levels = append(levels, v)
		name := str(n["alias"])
		if name == "" && str(n["class"]) == "COLUMN_REF" {
			parts := arr(n["column_names"])
			name = str(parts[len(parts)-1])
		}
		switch {
		case name == "":
			return nil, invalid("name every result column: give %s a name with AS", describe(n))
		case strings.HasPrefix(strings.ToLower(name), "__igloo"):
			return nil, invalid("result column names may not start with __igloo")
		case slices.ContainsFunc(z.a.outputs, func(o string) bool { return strings.EqualFold(o, name) }):
			return nil, invalid("two result columns are named %q", name)
		}
		z.a.outputs = append(z.a.outputs, name)
	}
	if len(z.a.outputs) == 0 {
		return nil, invalid("a template's result needs at least one column")
	}

	if w := obj(root["where_clause"]); w != nil {
		if _, err := z.expr(w, place{name: "WHERE", params: true}); err != nil {
			return nil, err
		}
		if err := z.where(w); err != nil {
			return nil, err
		}
	}

	if len(arr(root["group_sets"])) > 1 {
		return nil, invalid("templates GROUP BY dimensions without ROLLUP, CUBE or GROUPING SETS: a total less the groups shown " +
			"would show the groups left out; write a template for each grouping")
	}
	groups := arr(root["group_expressions"])
	for _, g := range groups {
		n := obj(g)
		if pos, ok := integer(n); ok {
			if pos < 1 || int(pos) > len(levels) || levels[pos-1] != levelRow {
				return nil, invalid("GROUP BY %d must name a result column of dimensions", pos)
			}
			key := clone(obj(items[pos-1]))
			key["alias"] = ""
			z.a.keys = append(z.a.keys, key)
			continue
		}
		if str(n["class"]) == "COLUMN_REF" && len(arr(n["column_names"])) == 1 {
			return nil, invalid("GROUP BY %s: write the column as alias.column", str(arr(n["column_names"])[0]))
		}
		if _, err := z.expr(n, place{name: "GROUP BY"}); err != nil {
			return nil, err
		}
		z.a.keys = append(z.a.keys, clone(n))
	}
	switch str(root["aggregate_handling"]) {
	case "STANDARD_HANDLING":
		if !aggregated && len(groups) == 0 {
			return nil, invalid("a template's result must be aggregated: GROUP BY dimensions, or compute only aggregates")
		}
	case "FORCE_AGGREGATES": // GROUP BY ALL: the result's columns of dimensions
		for i, item := range items {
			if levels[i] == levelRow {
				key := clone(obj(item))
				key["alias"] = ""
				z.a.keys = append(z.a.keys, key)
			}
		}
	default:
		return nil, invalid("templates cannot use %s", strings.ToLower(str(root["aggregate_handling"])))
	}

	for _, m := range arr(root["modifiers"]) {
		if err := z.modifier(obj(m)); err != nil {
			return nil, err
		}
	}
	return z.a, nil
}

// where keeps the conditions of WHERE, of those ANDed together, that have
// parameters. Each must read one dataset, so the population check can
// count what it keeps of that dataset alone: counted on the joined
// datasets, a refusal would tell whether the people a requester's own
// values pick out are in the other party's data.
func (z *analyzer) where(w node) error {
	if str(w["class"]) == "CONJUNCTION" && str(w["type"]) == "CONJUNCTION_AND" {
		for _, c := range arr(w["children"]) {
			if err := z.where(obj(c)); err != nil {
				return err
			}
		}
		return nil
	}
	params := false
	var aliases []string
	walk(w, func(n node) node {
		switch str(n["class"]) {
		case "PARAMETER":
			params = true
		case "COLUMN_REF":
			if parts := arr(n["column_names"]); len(parts) == 2 {
				if alias := strings.ToLower(str(parts[0])); !slices.Contains(aliases, alias) {
					aliases = append(aliases, alias)
				}
			}
		}
		return nil
	})
	switch {
	case !params:
	case len(aliases) != 1:
		return invalid("a condition with parameters reads one dataset's columns, not %d datasets': AND such conditions together, one per dataset",
			len(aliases))
	default:
		z.a.chosen[aliases[0]] = append(z.a.chosen[aliases[0]], w)
	}
	return nil
}

// empty reports whether a parse value is absent: null, false, "", an empty
// list, or an empty ORDER_MODIFIER.
func empty(v any) bool {
	switch v := v.(type) {
	case nil:
		return true
	case bool:
		return !v
	case string:
		return v == ""
	case []any:
		return len(v) == 0
	case node:
		if str(v["type"]) == "ORDER_MODIFIER" {
			return len(arr(v["orders"])) == 0
		}
		if m, ok := v["map"]; ok && len(v) == 1 {
			return len(arr(m)) == 0
		}
	}
	return false
}

func (z *analyzer) modifier(m node) error {
	switch str(m["type"]) {
	case "ORDER_MODIFIER":
		for _, o := range arr(m["orders"]) {
			on := obj(o)
			e := obj(on["expression"])
			item := orderItem{output: -1}
			if pos, ok := integer(e); ok {
				if pos < 1 || int(pos) > len(z.a.outputs) {
					return invalid("ORDER BY %d: the result has %d columns", pos, len(z.a.outputs))
				}
				item.output = int(pos) - 1
			} else if str(e["class"]) == "COLUMN_REF" && len(arr(e["column_names"])) == 1 {
				name := str(arr(e["column_names"])[0])
				item.output = slices.IndexFunc(z.a.outputs, func(o string) bool { return strings.EqualFold(o, name) })
			}
			if item.output < 0 {
				return invalid("ORDER BY names result columns, or their positions")
			}
			switch str(on["type"]) {
			case "DESCENDING":
				item.desc = true
			case "ASCENDING", "ORDER_DEFAULT":
			default:
				return invalid("ORDER BY takes ASC or DESC")
			}
			switch str(on["null_order"]) {
			case "NULLS FIRST", "NULLS LAST":
				item.nulls = str(on["null_order"])
			case "ORDER_DEFAULT", "":
			default:
				return invalid("ORDER BY takes NULLS FIRST or NULLS LAST")
			}
			z.a.order = append(z.a.order, item)
		}
		return nil
	case "LIMIT_MODIFIER":
		if !empty(m["offset"]) {
			return invalid("templates cannot have OFFSET")
		}
		n, ok := integer(obj(m["limit"]))
		if !ok || n < 0 {
			return invalid("LIMIT takes a number")
		}
		z.a.limit = n
		return nil
	case "DISTINCT_MODIFIER":
		return invalid("templates cannot use SELECT DISTINCT; GROUP BY the dimensions instead")
	}
	return invalid("templates cannot use %s", strings.ToLower(strings.TrimSuffix(str(m["type"]), "_MODIFIER")))
}

// from checks the FROM clause, the room's datasets and joins between
// them, and returns the datasets it reads.
func (z *analyzer) from(t node) ([]*ref, error) {
	if t == nil {
		return nil, invalid("a template reads the room's datasets: SELECT ... FROM dataset")
	}
	switch str(t["type"]) {
	case "BASE_TABLE":
		name := strings.ToLower(str(t["table_name"]))
		switch {
		case str(t["catalog_name"]) != "" || str(t["schema_name"]) != "":
			return nil, invalid("templates read the room's datasets by their names in the room, not %s", qualified(t))
		case !empty(t["at_clause"]):
			return nil, invalid("templates cannot travel in time")
		case !empty(t["sample"]):
			return nil, invalid("templates cannot sample")
		case !empty(t["column_name_alias"]):
			return nil, invalid("templates cannot rename a dataset's columns")
		}
		d, ok := z.datasets[name]
		if !ok {
			return nil, invalid("the room has no dataset %q", str(t["table_name"]))
		}
		alias := strings.ToLower(str(t["alias"]))
		if alias == "" {
			alias = name
		}
		if _, dup := z.aliases[alias]; dup {
			return nil, invalid("two datasets in FROM are called %q; give them different aliases", alias)
		}
		r := &ref{alias: alias, dataset: d}
		z.aliases[alias] = r
		z.a.refs = append(z.a.refs, r)
		return []*ref{r}, nil
	case "JOIN":
		switch {
		case str(t["ref_type"]) != "REGULAR":
			return nil, invalid("templates join with JOIN ... ON, not %s joins", strings.ToLower(str(t["ref_type"])))
		case !slices.Contains([]string{"INNER", "LEFT", "RIGHT", "FULL"}, str(t["join_type"])):
			return nil, invalid("templates join with INNER, LEFT, RIGHT or FULL JOIN, not %s", str(t["join_type"]))
		case !empty(t["using_columns"]):
			return nil, invalid("join with ON a.id = b.id, not USING")
		case !empty(t["alias"]), !empty(t["sample"]), !empty(t["duplicate_eliminated_columns"]), !empty(t["delim_flipped"]):
			return nil, invalid("templates cannot alias or sample a join")
		}
		left, err := z.from(obj(t["left"]))
		if err != nil {
			return nil, err
		}
		right, err := z.from(obj(t["right"]))
		if err != nil {
			return nil, err
		}
		cond := obj(t["condition"])
		if cond == nil {
			return nil, invalid("every join needs ON with an identifier of each side: ON a.id = b.id")
		}
		n, err := z.joinCondition(cond, left, right)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, invalid("every join needs an identifier of each side in ON: a.id = b.id")
		}
		return append(left, right...), nil
	}
	return nil, invalid("templates read only the room's datasets, and joins of them; not %s", strings.ToLower(strings.ReplaceAll(str(t["type"]), "_", " ")))
}

func qualified(t node) string {
	var parts []string
	for _, k := range []string{"catalog_name", "schema_name", "table_name"} {
		if s := str(t[k]); s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, ".")
}

// joinCondition checks a join's ON, whose sides read the datasets left and
// right, and counts its identifier equalities: each equates an identifier
// of a dataset of one side with one of the other, or the join would be a
// cross join under another's condition.
func (z *analyzer) joinCondition(n node, left, right []*ref) (int, error) {
	switch {
	case str(n["class"]) == "CONJUNCTION" && str(n["type"]) == "CONJUNCTION_AND":
		total := 0
		for _, c := range arr(n["children"]) {
			k, err := z.joinCondition(obj(c), left, right)
			if err != nil {
				return 0, err
			}
			total += k
		}
		return total, nil
	case str(n["class"]) == "COMPARISON" && str(n["type"]) == "COMPARE_EQUAL":
		l, lok, err := z.column(obj(n["left"]))
		if err != nil {
			return 0, err
		}
		r, rok, err := z.column(obj(n["right"]))
		if err != nil {
			return 0, err
		}
		if lok && rok && l.role == roleIdentifier && r.role == roleIdentifier {
			if !(slices.Contains(left, l.ref) && slices.Contains(right, r.ref)) && !(slices.Contains(left, r.ref) && slices.Contains(right, l.ref)) {
				return 0, invalid("ON %s.%s = %s.%s does not join this join's two sides: equate an identifier of a dataset before JOIN with one of the dataset it joins",
					l.ref.alias, l.name, r.ref.alias, r.name)
			}
			return 1, nil
		}
	}
	_, err := z.expr(n, place{name: "a join condition"})
	return 0, err
}

// columnRef is a column a template names, and what its policy allows.
type columnRef struct {
	ref  *ref
	name string
	role columnRole
}

// column resolves n if it is a column reference. ok is false for anything
// else; err refuses a column the template may not name.
func (z *analyzer) column(n node) (c columnRef, ok bool, err error) {
	if str(n["class"]) != "COLUMN_REF" {
		return c, false, nil
	}
	parts := arr(n["column_names"])
	if len(parts) != 2 {
		var names []string
		for _, p := range parts {
			names = append(names, str(p))
		}
		return c, true, invalid("write column %s as alias.column, naming a dataset of FROM", strings.Join(names, "."))
	}
	alias, name := strings.ToLower(str(parts[0])), str(parts[1])
	r, found := z.aliases[alias]
	if !found {
		return c, true, invalid("%s.%s: FROM has no dataset called %q", str(parts[0]), name, str(parts[0]))
	}
	role, actual := r.dataset.Policy.role(name)
	if role == roleNone {
		return c, true, invalid("column %s of dataset %s is not in its policy, so templates cannot use it", name, r.dataset.Name)
	}
	return columnRef{ref: r, name: actual, role: role}, true, nil
}

// scalarFunctions are the functions templates may call on dimensions,
// constants and aggregates. They read nothing but their arguments.
var scalarFunctions = setOf(
	// Operators.
	"+", "-", "*", "/", "//", "%", "||", "~~", "!~~", "~~*", "!~~*",
	// Numbers.
	"round", "floor", "ceil", "ceiling", "abs", "greatest", "least", "nullif", "trunc",
	// Text.
	"lower", "upper", "trim", "ltrim", "rtrim", "concat", "length", "left", "right", "substring",
	"replace", "split_part", "starts_with", "contains", "prefix", "suffix",
	// Dates and times. INTERVAL 7 DAY parses as to_days(...).
	"date_trunc", "date_part", "datepart", "year", "quarter", "month", "week", "weekofyear", "day",
	"dayofmonth", "dayofweek", "isodow", "dayofyear", "hour", "minute", "date_diff", "datediff",
	"strftime", "time_bucket", "make_date", "last_day",
	"to_years", "to_months", "to_weeks", "to_days", "to_hours", "to_minutes", "to_seconds",
)

// aggregateFunctions are the aggregates templates may call; which columns
// each takes is decided in aggregate.
var aggregateFunctions = setOf(append([]string{"count", "count_star", "approx_count_distinct",
	"datasketch_hll_estimate", "datasketch_theta_estimate"}, AggregateFunctions...)...)

// countFunctions count their argument's values, which may be identifiers.
// Noise, when a policy asks for it, perturbs their results.
var countFunctions = setOf("count", "count_star", "approx_count_distinct", "datasketch_hll_estimate", "datasketch_theta_estimate")

func setOf(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

var (
	comparisons = setOf("COMPARE_EQUAL", "COMPARE_NOTEQUAL", "COMPARE_LESSTHAN", "COMPARE_GREATERTHAN",
		"COMPARE_LESSTHANOREQUALTO", "COMPARE_GREATERTHANOREQUALTO", "COMPARE_DISTINCT_FROM", "COMPARE_NOT_DISTINCT_FROM")
	operators = setOf("OPERATOR_NOT", "OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL", "COMPARE_IN", "COMPARE_NOT_IN",
		"OPERATOR_COALESCE")
)

// expr checks an expression at p and says what it is a value of.
func (z *analyzer) expr(n node, p place) (level, error) {
	if n == nil {
		return levelConstant, invalid("unexpected empty expression in %s", p.name)
	}
	// children checks the parts of an expression that are there (a CASE
	// may have no ELSE), and says what the whole is a value of.
	children := func(list ...any) (level, error) {
		out := levelConstant
		for _, c := range list {
			if obj(c) == nil {
				continue
			}
			v, err := z.expr(obj(c), p)
			if err != nil {
				return levelConstant, err
			}
			out = max(out, v)
		}
		return out, nil
	}
	switch str(n["class"]) {
	case "CONSTANT":
		return levelConstant, nil
	case "PARAMETER":
		name := str(n["identifier"])
		if _, err := strconv.Atoi(name); err == nil {
			return levelConstant, invalid("name parameters ($start_date), and declare them with PARAMETERS (...)")
		}
		if !p.params {
			return levelConstant, invalid("parameters may be used only in WHERE, not in %s", p.name)
		}
		if _, ok := z.params[name]; !ok {
			return levelConstant, invalid("parameter $%s is not declared; add it to PARAMETERS (...)", name)
		}
		return levelConstant, nil
	case "COLUMN_REF":
		c, _, err := z.column(n)
		if err != nil {
			return levelConstant, err
		}
		switch c.role {
		case roleIdentifier:
			return levelConstant, invalid("%s.%s is an identifier: templates use it only to join datasets (ON a.id = b.id) and to count it (count(DISTINCT %[1]s.%[2]s)), not in %s",
				c.ref.alias, c.name, p.name)
		case roleMeasure:
			return levelConstant, invalid("%s.%s may be used only in the aggregates its policy allows: %s", c.ref.alias, c.name,
				c.ref.dataset.Policy.measureAggregates(c.name))
		}
		return levelRow, nil
	case "CAST":
		return children(n["child"])
	case "COMPARISON":
		if !comparisons[str(n["type"])] {
			break
		}
		return children(n["left"], n["right"])
	case "CONJUNCTION":
		return children(arr(n["children"])...)
	case "OPERATOR":
		if !operators[str(n["type"])] {
			break
		}
		return children(arr(n["children"])...)
	case "BETWEEN":
		return children(n["input"], n["lower"], n["upper"])
	case "CASE":
		var parts []any
		for _, c := range arr(n["case_checks"]) {
			parts = append(parts, obj(c)["when_expr"], obj(c)["then_expr"])
		}
		return children(append(parts, n["else_expr"])...)
	case "FUNCTION":
		name := strings.ToLower(str(n["function_name"]))
		switch {
		case str(n["schema"]) != "" || str(n["catalog"]) != "":
			return levelConstant, invalid("call %s without a schema", name)
		case !empty(n["order_bys"]):
			return levelConstant, invalid("%s cannot have ORDER BY in templates", name)
		case !empty(n["export_state"]):
			return levelConstant, invalid("templates cannot export an aggregate's state")
		case aggregateFunctions[name]:
			return z.aggregate(n, name, p)
		case !scalarFunctions[name]:
			return levelConstant, invalid("function %s is not allowed in templates", name)
		case !empty(n["distinct"]) || !empty(n["filter"]):
			return levelConstant, invalid("%s is not an aggregate", name)
		}
		return children(arr(n["children"])...)
	case "SUBQUERY":
		return levelConstant, invalid("templates cannot have subqueries")
	case "WINDOW":
		return levelConstant, invalid("templates cannot use window functions")
	case "STAR":
		return levelConstant, invalid("templates name their columns; * and COLUMNS() are not allowed")
	}
	return levelConstant, invalid("%s is not allowed in templates", describe(n))
}

// Sketches' lg_k, the base-2 logarithm of their size, is between these:
// smaller ones estimate too roughly to be worth having.
const (
	MinSketchLgK = 10
	MaxSketchLgK = 21
)

// MaxFilters is how many different FILTER conditions a template may have:
// thresholds count every combination of them (see Plan).
const MaxFilters = 4

// aggregate checks an aggregate call. Its argument is a bare column, never
// an expression: a CASE inside an aggregate would pick out a part of a
// group that thresholds do not count, as FILTER's parts are counted.
func (z *analyzer) aggregate(n node, name string, p place) (level, error) {
	if !p.aggregates {
		return levelConstant, invalid("%s is an aggregate, which %s cannot have", name, p.name)
	}
	if f := obj(n["filter"]); f != nil {
		if err := z.filter(f); err != nil {
			return levelConstant, err
		}
	}
	distinct := !empty(n["distinct"])
	args := arr(n["children"])
	group := levelGroup
	switch name {
	case "count_star":
		return group, nil
	case "count", "approx_count_distinct":
		if len(args) != 1 {
			return levelConstant, invalid("%s takes one identifier column", name)
		}
		return group, z.counted(obj(args[0]), name)
	case "datasketch_hll_estimate", "datasketch_theta_estimate":
		// estimate(sketch(lg_k, column)): a sketch holds hashes of the
		// identifiers it saw, so it never leaves the room; its estimate
		// does.
		want := strings.TrimSuffix(name, "_estimate")
		var s node
		if len(args) == 1 {
			s = obj(args[0])
		}
		if distinct || !empty(n["filter"]) || s == nil || str(s["class"]) != "FUNCTION" ||
			strings.ToLower(str(s["function_name"])) != want || str(s["schema"]) != "" || str(s["catalog"]) != "" ||
			!empty(s["order_bys"]) || !empty(s["export_state"]) || !empty(s["distinct"]) {
			return levelConstant, invalid("%s takes %s(lg_k, column) directly", name, want)
		}
		if f := obj(s["filter"]); f != nil {
			if err := z.filter(f); err != nil {
				return levelConstant, err
			}
		}
		sargs := arr(s["children"])
		if len(sargs) != 2 {
			return levelConstant, invalid("%s takes lg_k and a column", want)
		}
		if k, ok := integer(obj(sargs[0])); !ok || k < MinSketchLgK || k > MaxSketchLgK {
			return levelConstant, invalid("%s takes lg_k as a number from %d to %d, such as %s(14, a.id)", want, MinSketchLgK, MaxSketchLgK, want)
		}
		return group, z.counted(obj(sargs[1]), want)
	}
	// The aggregates a policy may allow of a column: sum, avg, min, max,
	// median and stddev.
	if distinct {
		return levelConstant, invalid("DISTINCT goes only with count in templates")
	}
	if len(args) != 1 {
		return levelConstant, invalid("%s takes one column", name)
	}
	c, isColumn, err := z.column(obj(args[0]))
	switch {
	case err != nil:
		return levelConstant, err
	case !isColumn:
		return levelConstant, invalid("%s takes a column its dataset's policy allows it of, not an expression; take part of a group with FILTER (WHERE ...)", name)
	case c.role == roleIdentifier:
		return levelConstant, invalid("%s.%s is an identifier, which templates may only count (count(DISTINCT %[1]s.%[2]s)), not take the %s of",
			c.ref.alias, c.name, name)
	case c.role == roleDimension:
		return levelConstant, invalid("%s.%s is a dimension, which templates may GROUP BY and filter on, not take the %s of: that can be one person's value",
			c.ref.alias, c.name, name)
	case !c.ref.dataset.Policy.allows(name, c.name):
		return levelConstant, invalid("the policy of dataset %s allows %s, not %s(%s)", c.ref.dataset.Name,
			c.ref.dataset.Policy.measureAggregates(c.name), name, c.name)
	case z.a.crossParty:
		return group, z.crossPartyMeasure(name, c)
	}
	return group, nil
}

// crossPartyMeasure checks an aggregate of a measure in a template that
// joins datasets of different parties. Unbounded and without noise, a sum
// would answer for one person: a party that sets a column of its own to 1
// for one person and 0 for everyone else learns, from its sum over a large
// group, whether that person is in the other party's data. So only sums
// are allowed, of measures whose policy bounds them, and only when every
// party asks for noise: each person's total in a group is clamped to the
// bounds before it is summed, and the sum gets noise scaled to them. That
// does not bound all one person can do to a result (docs/clean-rooms.md
// says what is under review). The minimum, maximum, median and spread of
// values are one person's, or close to it, and no added noise makes them
// safe; an average is a sum over a count, both of which a template can
// show.
func (z *analyzer) crossPartyMeasure(name string, c columnRef) error {
	if name != "sum" {
		return invalid("%s(%s.%s) is refused in a template that joins datasets of different parties: it shows one person's value, or "+
			"close to it; only sums of bounded measures, with noise, are allowed (an average is sum(...) / count(DISTINCT ...))", name, c.ref.alias, c.name)
	}
	if _, ok := c.ref.dataset.Policy.bound(c.name); !ok {
		return invalid("sum(%s.%s) joins datasets of different parties, so its dataset's policy must bound the column per person: "+
			"BOUNDS (%s BETWEEN lo AND hi) in %s's policy", c.ref.alias, c.name, c.name, c.ref.dataset.Name)
	}
	for _, r := range z.a.refs {
		if r.dataset.Policy.Noise <= 0 {
			return invalid("sum(%s.%s) joins datasets of different parties, so every dataset it reads needs NOISE in its policy, and %s has none",
				c.ref.alias, c.name, r.dataset.Name)
		}
	}
	return nil
}

// counted checks what a count counts: an identifier, bare. Rows are
// counted with count(*), and part of a group with FILTER (WHERE ...),
// which thresholds count; a count of an expression or of a dimension
// counts a part they do not.
func (z *analyzer) counted(n node, fn string) error {
	c, isColumn, err := z.column(n)
	switch {
	case err != nil:
		return err
	case !isColumn || c.role != roleIdentifier:
		return invalid("%s counts an identifier, as in count(DISTINCT a.id), or rows, with count(*); count part of a group with FILTER (WHERE ...)", fn)
	}
	return nil
}

// filter checks the predicate of an aggregate's FILTER and keeps it.
func (z *analyzer) filter(f node) error {
	if _, err := z.expr(f, place{name: "FILTER (WHERE ...)"}); err != nil {
		return err
	}
	if key := canonical(f); !z.filters[key] {
		if len(z.a.filters) == MaxFilters {
			return invalid("a template has at most %d different FILTER conditions", MaxFilters)
		}
		z.filters[key] = true
		z.a.filters = append(z.a.filters, f)
	}
	return nil
}

// describe names a node for messages.
func describe(n node) string {
	switch str(n["class"]) {
	case "FUNCTION":
		return str(n["function_name"]) + "(...)"
	case "COLUMN_REF":
		var names []string
		for _, p := range arr(n["column_names"]) {
			names = append(names, str(p))
		}
		return strings.Join(names, ".")
	case "":
		return "this expression"
	}
	return strings.ToLower(strings.ReplaceAll(str(n["type"]), "_", " "))
}

// typeAliases are other names of the parameter types.
var typeAliases = map[string]string{"TEXT": "VARCHAR", "STRING": "VARCHAR", "INT": "INTEGER", "INT4": "INTEGER",
	"INT8": "BIGINT", "FLOAT8": "DOUBLE", "BOOL": "BOOLEAN", "TIMESTAMPWITHTIMEZONE": "TIMESTAMPTZ"}

// parameterType matches the types parameters may have.
var parameterType = regexp.MustCompile(`^(VARCHAR|BIGINT|INTEGER|DOUBLE|BOOLEAN|DATE|TIMESTAMP|TIMESTAMPTZ|DECIMAL\([1-9][0-9]?(,[0-9][0-9]?)?\))$`)

// CheckParameters validates and normalizes declared parameters.
func CheckParameters(params []Parameter) error {
	for i, p := range params {
		if err := ValidName("parameter", p.Name); err != nil {
			return err
		}
		if slices.ContainsFunc(params[:i], func(q Parameter) bool { return q.Name == p.Name }) {
			return invalid("parameter %s is declared twice", p.Name)
		}
		t := strings.ToUpper(strings.Join(strings.Fields(p.Type), ""))
		if alias, ok := typeAliases[t]; ok {
			t = alias
		}
		if !parameterType.MatchString(t) {
			return invalid("parameter %s has type %s; parameters are VARCHAR, BIGINT, INTEGER, DOUBLE, DECIMAL(p,s), BOOLEAN, DATE, TIMESTAMP or TIMESTAMPTZ",
				p.Name, p.Type)
		}
		params[i].Type = t
	}
	return nil
}
