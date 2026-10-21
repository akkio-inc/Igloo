package cleanroom

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
)

// A run executes three statements that Igloo writes from the template's
// parse:
//
//   - When the template has parameters, a population check counts each
//     identifier of each dataset they filter, in that dataset alone, with
//     and without the conditions that have them. Values that keep, or
//     leave out, between one and threshold-1 identifiers are refused:
//     comparing such a run with one without the filter would show those
//     few people's contribution. Counted on the joined datasets, a refusal
//     would tell whether the people a requester's values pick out are in
//     the other party's data.
//   - The groups: the template as written, with its datasets' tables
//     instead of their names, its parameters bound as typed values, every
//     function qualified with DuckDB's own schema (so no macro can stand in
//     for one), noise on its counts and sums when a policy asks for it, and
//     hidden counts of each dataset's identifiers: in the whole group,
//     within each FILTER of the result's aggregates, and within each
//     combination of the FILTERs and their opposites, which an expression
//     of filtered aggregates could tell apart. A template that sums
//     measures across parties reads its joins through a subquery, so each
//     person's total in a group is clamped to the measure's bounds (see
//     bound).
//   - The result: the groups whose every hidden count reaches its
//     dataset's threshold (each combination none, or at least the
//     threshold), in the template's order, up to the row limit. Nothing
//     says how many groups were left out: that a group of a requester's
//     own data exists only when someone in it matched would tell who.
//
// The groups exist only in the run's own session; nobody sees them.
//
// Noise is Laplace noise with a scale of 1/epsilon on every count of the
// result, and (hi - lo)/epsilon on every bounded sum, drawn from SHA-256 of
// a seed new to each run (bound as a parameter, so it is in no statement's
// text), the aggregate and its group: the same aggregate written twice in a
// run has the same noise, so averaging copies gains nothing, and a run
// costs its budget once per different aggregate.

// Plan is how a run executes a template.
type Plan struct {
	// Reads are the catalog and external tables the run may read.
	Reads []auth.Object
	// Check, when not empty, is the population check. Its columns are a
	// total and a filtered count for each of Checks.
	Check  string
	Checks []CheckColumn
	// Groups computes the result's groups, with the hidden counts. With
	// noise, it takes Seed as one more parameter after Args.
	Groups string
	Seed   string
	// Cost is how much of each dataset's daily budget the run takes: one,
	// or with noise, one for each different count it noises.
	Cost int
	// Cap is the most rows the result has; Keep returns one more when a
	// row limit, not the template's LIMIT, cuts the result short.
	Cap int
	// Args bind the parameters of Check and Groups, as $1, $2..., and
	// Values casts them to their types alone, reading no data, so a value
	// that is not of its type fails there.
	Args   []any
	Values string
	// Outputs are the result's column names.
	Outputs []string
	// Noise is the epsilon of the noise on counts, or 0.
	Noise float64
	// Datasets are the datasets the template reads.
	Datasets []*Dataset

	// keep is the condition a group must meet, and order and limit the
	// result's ORDER BY and LIMIT.
	keep, order, limit string
}

// Keep selects the groups to keep from table, the groups' table, as the
// result's columns in its order.
func (pl *Plan) Keep(table string) string {
	outs := make([]string, len(pl.Outputs))
	for i, o := range pl.Outputs {
		outs[i] = quote(o)
	}
	return "SELECT " + strings.Join(outs, ", ") + " FROM " + table + " WHERE " + pl.keep + pl.order + pl.limit
}

// CheckColumn is a dataset's identifier column, which the population check
// counts.
type CheckColumn struct {
	Dataset   *Dataset
	Alias     string
	Column    string
	Threshold int
}

// Narrowing refuses a run whose filter keeps or leaves out fewer than a
// threshold of a dataset's identifiers, but some.
func (pl *Plan) Narrowing(counts []int64) error {
	if len(counts) != 2*len(pl.Checks) {
		return fmt.Errorf("the population check returned %d counts for %d identifiers", len(counts), len(pl.Checks))
	}
	for i, c := range pl.Checks {
		total, kept := counts[2*i], counts[2*i+1]
		left := total - kept
		k := int64(c.Threshold)
		if kept > 0 && kept < k || left > 0 && left < k {
			return &RefusedError{msg: fmt.Sprintf("these parameter values keep fewer than %d of the distinct %s of dataset %s, or leave out "+
				"fewer than that, but some: Igloo refuses filters that a comparison of runs could tell a few people apart by; widen or drop the filter",
				c.Threshold, c.Column, c.Dataset.Name)}
		}
	}
	return nil
}

// RefusedError is a run Igloo refused to protect the data; the history
// records it.
type RefusedError struct{ msg string }

func (e *RefusedError) Error() string { return e.msg }

// Unfinished is how a run that failed while it read the datasets ends: as
// a refused one, without the error, whose message and code could depend on
// another party's rows.
var Unfinished = &RefusedError{msg: "this run did not finish, and Igloo does not say why: the reason could depend on " +
	"another party's data (Igloo's log has it for the host's admins)"}

// plan writes the statements of a run of a, checked against the room's
// current datasets, with values for its parameters.
func (p *Parser) plan(ctx context.Context, a *analysis, dbs Databases, params []Parameter, values map[string]any) (*Plan, error) {
	pl := &Plan{Outputs: a.outputs, Datasets: a.datasets(), Cap: MaxResultRows}
	for _, d := range pl.Datasets {
		pl.Reads = append(pl.Reads, auth.Object{Schema: d.Schema, Table: d.Table})
		if d.Policy.MaxRows > 0 {
			pl.Cap = min(pl.Cap, d.Policy.MaxRows)
		}
		if d.Policy.Noise > 0 && (pl.Noise == 0 || d.Policy.Noise < pl.Noise) {
			pl.Noise = d.Policy.Noise
		}
	}
	for name := range values {
		if !slices.ContainsFunc(params, func(q Parameter) bool { return q.Name == name }) {
			return nil, invalid("the template has no parameter %s", name)
		}
	}
	// Every declared parameter is used (see analyze), as $1, $2... in the
	// order of the declarations.
	index := map[string]int{}
	var casts []string
	for _, q := range params {
		v, ok := values[q.Name]
		if !ok {
			return nil, invalid("parameter %s (%s) needs a value", q.Name, q.Type)
		}
		arg, err := bindValue(v)
		if err != nil {
			return nil, invalid("parameter %s: %v", q.Name, err)
		}
		pl.Args = append(pl.Args, arg)
		index[q.Name] = len(pl.Args)
		casts = append(casts, fmt.Sprintf("CAST($%d AS %s)", len(pl.Args), q.Type))
	}
	if len(casts) > 0 {
		pl.Values = "SELECT " + strings.Join(casts, ", ")
	}
	typed := map[string]node{}
	for _, q := range params {
		c, err := p.expression(ctx, "CAST(NULL AS "+q.Type+")")
		if err != nil {
			return nil, err
		}
		typed[q.Name] = c
	}
	// finish makes a parse runnable: the datasets' tables, typed
	// parameters, qualified functions.
	finish := func(stmt node) (string, error) {
		root := obj(stmt["node"])
		root["from_table"] = walk(obj(root["from_table"]), func(n node) node {
			if str(n["type"]) != "BASE_TABLE" {
				return nil
			}
			alias := strings.ToLower(str(n["alias"]))
			if alias == "" {
				alias = strings.ToLower(str(n["table_name"]))
			}
			i := slices.IndexFunc(a.refs, func(r *ref) bool { return r.alias == alias })
			if i < 0 {
				return nil
			}
			d := a.refs[i].dataset
			db := dbs.Catalog
			if d.Kind == SourceExternal {
				db = dbs.Local
			}
			n["alias"], n["catalog_name"], n["schema_name"], n["table_name"] = alias, db, d.Schema, d.Table
			return n
		})
		walk(root, func(n node) node {
			switch str(n["class"]) {
			case "PARAMETER":
				name := str(n["identifier"])
				t, ok := typed[name]
				if !ok {
					// The noise's seed, which Igloo numbered itself.
					return nil
				}
				c := clone(t)
				n["identifier"] = strconv.Itoa(index[name])
				c["child"] = n
				return c
			case "FUNCTION":
				if b, _ := n["is_operator"].(bool); !b {
					n["catalog"], n["schema"] = "system", "main"
				}
			}
			return nil
		})
		stmt["named_param_map"] = []any{}
		return p.deparse(ctx, stmt)
	}
	// hidden counts the distinct values of column, SQL text, within
	// filter.
	hidden := func(name, column string, filter node) (node, error) {
		n, err := p.expression(ctx, "count(DISTINCT "+column+") AS "+quote(name))
		if err != nil {
			return nil, err
		}
		if filter != nil {
			n["filter"] = clone(filter)
		}
		return n, nil
	}

	// The groups.
	stmt := clone(a.stmt)
	root := obj(stmt["node"])
	b, err := p.bound(ctx, a, root)
	if err != nil {
		return nil, err
	}
	pl.Cost = 1
	if pl.Noise > 0 {
		if err := p.noise(ctx, pl, b.rewriteAll(a.keys), root, len(params)+1, b); err != nil {
			return nil, err
		}
	}
	root["modifiers"] = []any{}
	var conds []string
	items := arr(root["select_list"])
	add := func(r *ref, col string, filter node, cond string) error {
		name := fmt.Sprintf("__igloo_k_%d", len(conds))
		n, err := hidden(name, b.ref(r.alias, col), filter)
		if err != nil {
			return err
		}
		items = append(items, n)
		conds = append(conds, fmt.Sprintf(cond, quote(name)))
		return nil
	}
	// Each combination of the FILTERs and their opposites, which an
	// expression of filtered aggregates could tell apart: none of a
	// dataset's people, or at least its threshold.
	filters := b.rewriteAll(a.filters)
	atoms, err := p.atoms(ctx, filters)
	if err != nil {
		return nil, err
	}
	for _, r := range a.refs {
		k := r.dataset.Policy.Threshold
		for _, col := range r.dataset.Policy.Join {
			for _, f := range append([]node{nil}, filters...) {
				if err := add(r, col, f, "%s >= "+strconv.Itoa(k)); err != nil {
					return nil, err
				}
			}
			for _, f := range atoms {
				if err := add(r, col, f, "(%[1]s = 0 OR %[1]s >= "+strconv.Itoa(k)+")"); err != nil {
					return nil, err
				}
			}
		}
	}
	root["select_list"] = items
	if pl.Groups, err = finish(stmt); err != nil {
		return nil, err
	}

	// What to keep of them.
	pl.keep = strings.Join(conds, " AND ")
	if len(a.order) > 0 {
		var order []string
		for _, o := range a.order {
			s := quote(a.outputs[o.output])
			if o.desc {
				s += " DESC"
			}
			if o.nulls != "" {
				s += " " + o.nulls
			}
			order = append(order, s)
		}
		pl.order = " ORDER BY " + strings.Join(order, ", ")
	}
	if a.limit >= 0 && a.limit <= int64(pl.Cap) {
		pl.limit = " LIMIT " + strconv.FormatInt(a.limit, 10)
	} else {
		pl.limit = " LIMIT " + strconv.Itoa(pl.Cap+1)
	}

	// The population check: each dataset the parameters filter, alone.
	var checks []string
	for _, r := range a.refs {
		chosen := a.chosen[r.alias]
		if len(chosen) == 0 {
			continue
		}
		filter, err := p.and(ctx, chosen)
		if err != nil {
			return nil, err
		}
		stmt := clone(a.stmt)
		root := obj(stmt["node"])
		var table node
		walk(obj(root["from_table"]), func(n node) node {
			alias := strings.ToLower(str(n["alias"]))
			if alias == "" {
				alias = strings.ToLower(str(n["table_name"]))
			}
			if str(n["type"]) == "BASE_TABLE" && alias == r.alias {
				table = clone(n)
			}
			return nil
		})
		var items []any
		for _, col := range r.dataset.Policy.Join {
			i := len(pl.Checks)
			total, err := hidden(fmt.Sprintf("__igloo_t_%d", i), quote(r.alias)+"."+quote(col), nil)
			if err != nil {
				return nil, err
			}
			kept, err := hidden(fmt.Sprintf("__igloo_f_%d", i), quote(r.alias)+"."+quote(col), filter)
			if err != nil {
				return nil, err
			}
			items = append(items, total, kept)
			pl.Checks = append(pl.Checks, CheckColumn{Dataset: r.dataset, Alias: r.alias, Column: col, Threshold: r.dataset.Policy.Threshold})
		}
		root["from_table"], root["select_list"], root["where_clause"] = table, items, nil
		root["group_expressions"], root["group_sets"], root["modifiers"] = []any{}, []any{}, []any{}
		root["aggregate_handling"] = "STANDARD_HANDLING"
		check, err := finish(stmt)
		if err != nil {
			return nil, err
		}
		checks = append(checks, check)
	}
	switch len(checks) {
	case 0:
	case 1:
		pl.Check = checks[0]
	default:
		for i, c := range checks {
			checks[i] = "(" + c + ") AS __igloo_c_" + strconv.Itoa(i)
		}
		pl.Check = "SELECT * FROM " + strings.Join(checks, ", ")
	}
	return pl, nil
}

// noise adds Laplace noise to every count and bounded sum (see bound) in
// the result's columns of root, a group of keys, drawn from SHA-256 of the
// run's seed ($seed), the aggregate and its group, and works out the run's
// cost: one per different aggregate.
func (p *Parser) noise(ctx context.Context, pl *Plan, keys []node, root node, seed int, b *bounding) error {
	var buf [32]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return err
	}
	pl.Seed = hex.EncodeToString(buf[:])
	noised := map[string]bool{}
	var err error
	items := arr(root["select_list"])
	for i, item := range items {
		items[i] = walk(obj(item), func(n node) node {
			if err != nil || str(n["class"]) != "FUNCTION" {
				return nil
			}
			var text string
			sum, bounded := b.sum(n)
			switch {
			case bounded:
				noised[sum.key] = true
				text = noiseOf(sum.key, len(keys), sum.width/pl.Noise, seed, false)
			case countFunctions[strings.ToLower(str(n["function_name"]))]:
				digest := sha256.Sum256([]byte(canonical(n)))
				key := hex.EncodeToString(digest[:])
				noised[key] = true
				text = noiseOf(key, len(keys), 1/pl.Noise, seed, true)
			default:
				return nil
			}
			var wrapped node
			if wrapped, err = p.expression(ctx, text); err != nil {
				return nil
			}
			wrapped["alias"] = n["alias"]
			n["alias"] = ""
			return walk(wrapped, func(m node) node {
				if str(m["class"]) != "COLUMN_REF" || len(arr(m["column_names"])) != 1 {
					return nil
				}
				name := str(arr(m["column_names"])[0])
				if name == "__igloo_value" {
					return n
				}
				if k, ok := strings.CutPrefix(name, "__igloo_key_"); ok {
					j, _ := strconv.Atoi(k)
					return clone(keys[j])
				}
				return nil
			})
		})
	}
	pl.Cost = max(1, len(noised))
	return err
}

// noiseOf is the noisy value of __igloo_value, the aggregate key names in
// a group of keys __igloo_key_0, __igloo_key_1...: Laplace noise of the
// scale from two 52-bit parts of a SHA-256 hash of the seed (the parameter
// $seed), the aggregate and the group. A count is rounded, and never below
// zero.
func noiseOf(key string, keys int, scale float64, seed int, count bool) string {
	parts := []string{"CAST($" + strconv.Itoa(seed) + " AS VARCHAR)", "'|" + key + "'"}
	for i := range keys {
		v := fmt.Sprintf("CAST(__igloo_key_%d AS VARCHAR)", i)
		parts = append(parts, "'|'", "coalesce(length("+v+") || ':' || "+v+", 'null')")
	}
	h := "sha256(concat(" + strings.Join(parts, ", ") + "))"
	draw := func(from int) string {
		return fmt.Sprintf("ln((CAST('0x' || substr(%s, %d, 13) AS UBIGINT) + 1) / 4503599627370496.0)", h, from)
	}
	noisy := fmt.Sprintf("__igloo_value + %s * (%s - %s)", strconv.FormatFloat(scale, 'g', -1, 64), draw(1), draw(14))
	if count {
		return "CAST(greatest(0, round(" + noisy + ")) AS BIGINT)"
	}
	return "(" + noisy + ")"
}

// boundAlias is the subquery a run with bounded sums reads.
const boundAlias = "__igloo_d"

// bounding is how a run of a template that sums measures across parties
// reads its datasets: the template's joins and WHERE in a subquery whose
// rows have every column the result uses, each person's total of each
// summed measure in its group, and a mark on one row of each person and
// group; the result sums those totals, clamped to the measures' bounds,
// from the marked rows. A nil bounding reads the datasets directly.
type bounding struct {
	// columns name the subquery's column for each column the result uses,
	// by lower-cased alias.column.
	columns map[string]string
	// sums are the bounded sums in the result, by their nodes' address.
	sums map[string]boundedSum
}

// boundedSum is a sum of a measure in a run with bounds.
type boundedSum struct {
	// key names the sum for its noise: the hash of the template's sum.
	key string
	// width is the bounds' width, which the sum's noise is scaled to.
	width float64
}

func address(n node) string { return fmt.Sprintf("%p", n) }

// ref is SQL text for alias.column in the result.
func (b *bounding) ref(alias, column string) string {
	if b == nil {
		return quote(alias) + "." + quote(column)
	}
	return quote(boundAlias) + "." + quote(b.columns[strings.ToLower(alias+"."+column)])
}

// rewrite is a copy of n reading the subquery's columns.
func (b *bounding) rewrite(n node) node {
	if b == nil {
		return clone(n)
	}
	return b.rewriteInPlace(clone(n))
}

func (b *bounding) rewriteAll(ns []node) []node {
	out := make([]node, len(ns))
	for i, n := range ns {
		out[i] = b.rewrite(n)
	}
	return out
}

func (b *bounding) sum(n node) (boundedSum, bool) {
	if b == nil {
		return boundedSum{}, false
	}
	s, ok := b.sums[address(n)]
	return s, ok
}

// bound rewrites root, a run's groups, to bound its sums of measures, when
// its template joins datasets of different parties (see crossPartyMeasure),
// and returns how; nil when there is nothing to bound. Each person, by
// the first identifier of the measure's dataset, adds to a group's sum
// their total of the group's rows, clamped to the measure's bounds,
// however many rows the joins give them.
func (p *Parser) bound(ctx context.Context, a *analysis, root node) (*bounding, error) {
	if !a.crossParty {
		return nil, nil
	}
	type sum struct {
		node   node
		ref    *ref
		column string
		bound  Bound
		filter node
		key    string
	}
	var sums []*sum
	walk(root, func(n node) node {
		if str(n["class"]) != "FUNCTION" || strings.ToLower(str(n["function_name"])) != "sum" || len(arr(n["children"])) != 1 {
			return nil
		}
		parts := arr(obj(arr(n["children"])[0])["column_names"])
		if len(parts) != 2 {
			return nil
		}
		i := slices.IndexFunc(a.refs, func(r *ref) bool { return r.alias == strings.ToLower(str(parts[0])) })
		if i < 0 {
			return nil
		}
		r := a.refs[i]
		if role, column := r.dataset.Policy.role(str(parts[1])); role == roleMeasure {
			bound, _ := r.dataset.Policy.bound(column)
			digest := sha256.Sum256([]byte(canonical(n)))
			sums = append(sums, &sum{node: n, ref: r, column: column, bound: bound, filter: obj(n["filter"]), key: hex.EncodeToString(digest[:])})
		}
		return nil
	})
	if len(sums) == 0 {
		return nil, nil
	}

	// The subquery's columns: every column the result names, and every
	// identifier, which thresholds count.
	b := &bounding{columns: map[string]string{}, sums: map[string]boundedSum{}}
	var items []string
	column := func(alias, name string) {
		k := strings.ToLower(alias + "." + name)
		if _, ok := b.columns[k]; !ok {
			b.columns[k] = fmt.Sprintf("__igloo_c_%d", len(b.columns))
			items = append(items, quote(alias)+"."+quote(name)+" AS "+quote(b.columns[k]))
		}
	}
	for _, part := range []string{"select_list", "group_expressions"} {
		for _, e := range arr(root[part]) {
			walk(obj(e), func(n node) node {
				if parts := arr(n["column_names"]); str(n["class"]) == "COLUMN_REF" && len(parts) == 2 {
					column(str(parts[0]), str(parts[1]))
				}
				return nil
			})
		}
	}
	for _, r := range a.refs {
		for _, id := range r.dataset.Policy.Join {
			column(r.alias, id)
		}
	}
	// Each person's total in their group, and one row of each person and
	// group to take it from.
	partition := func(r *ref) string {
		var by []string
		for i := range a.keys {
			by = append(by, fmt.Sprintf("__igloo_key_%d", i))
		}
		return "PARTITION BY " + strings.Join(append(by, quote(r.alias)+"."+quote(r.dataset.Policy.Join[0])), ", ")
	}
	var windows []node
	parse := func(text string, filter node) error {
		n, err := p.expression(ctx, text)
		if err != nil {
			return err
		}
		windows = append(windows, walk(n, func(m node) node {
			if str(m["class"]) != "COLUMN_REF" || len(arr(m["column_names"])) != 1 {
				return nil
			}
			switch name := str(arr(m["column_names"])[0]); {
			case name == "__igloo_filter":
				return clone(filter)
			case strings.HasPrefix(name, "__igloo_key_"):
				j, _ := strconv.Atoi(strings.TrimPrefix(name, "__igloo_key_"))
				return clone(a.keys[j])
			}
			return nil
		}))
		return nil
	}
	marks := map[*ref]string{}
	totals := map[string]string{}
	for _, s := range sums {
		if _, ok := marks[s.ref]; !ok {
			marks[s.ref] = fmt.Sprintf("__igloo_r_%d", len(marks))
			if err := parse("row_number() OVER ("+partition(s.ref)+") AS "+quote(marks[s.ref]), nil); err != nil {
				return nil, err
			}
		}
		if _, ok := totals[s.key]; ok {
			continue
		}
		totals[s.key] = fmt.Sprintf("__igloo_s_%d", len(totals))
		when := "true"
		if s.filter != nil {
			when = "coalesce(__igloo_filter, false)"
		}
		// Each row's value too is clamped, as a DOUBLE, so no sum of the
		// other party's values can overflow: whether a run fails must not
		// depend on them.
		value := "greatest(" + formatBound(s.bound.Lo) + ", least(" + formatBound(s.bound.Hi) + ", CAST(" + quote(s.ref.alias) + "." + quote(s.column) + " AS DOUBLE)))"
		text := "sum(CASE WHEN " + when + " THEN " + value + " END) OVER (" + partition(s.ref) + ") AS " + quote(totals[s.key])
		if err := parse(text, s.filter); err != nil {
			return nil, err
		}
	}

	// The subquery: the template's FROM and WHERE.
	inner := clone(a.stmt)
	ir := obj(inner["node"])
	list := make([]any, 0, len(items)+len(windows))
	for _, it := range items {
		n, err := p.expression(ctx, it)
		if err != nil {
			return nil, err
		}
		list = append(list, n)
	}
	for _, w := range windows {
		list = append(list, w)
	}
	ir["select_list"], ir["group_expressions"], ir["group_sets"], ir["modifiers"] = list, []any{}, []any{}, []any{}
	ir["aggregate_handling"] = "STANDARD_HANDLING"
	text, err := p.deparse(ctx, inner)
	if err != nil {
		return nil, err
	}
	outer, err := p.parse(ctx, "SELECT 1 FROM ("+text+") AS "+quote(boundAlias))
	if err != nil {
		return nil, err
	}
	root["from_table"], root["where_clause"] = obj(obj(outer["node"])["from_table"]), nil

	// The result: the sums from the marked rows, clamped, and the rest
	// reading the subquery's columns.
	var walkErr error
	list = arr(root["select_list"])
	for i, item := range list {
		list[i] = walk(obj(item), func(n node) node {
			j := slices.IndexFunc(sums, func(s *sum) bool { return address(s.node) == address(n) })
			if j < 0 || walkErr != nil {
				return nil
			}
			s := sums[j]
			mark, total := quote(boundAlias)+"."+quote(marks[s.ref]), quote(boundAlias)+"."+quote(totals[s.key])
			lo, hi := formatBound(s.bound.Lo), formatBound(s.bound.Hi)
			var c node
			c, walkErr = p.expression(ctx, "sum(CASE WHEN "+mark+" = 1 AND "+total+" IS NOT NULL THEN greatest("+lo+", least("+hi+", "+total+")) END)")
			if walkErr != nil {
				return nil
			}
			c["alias"] = n["alias"]
			b.sums[address(c)] = boundedSum{key: s.key, width: s.bound.Hi - s.bound.Lo}
			return c
		})
	}
	if walkErr != nil {
		return nil, walkErr
	}
	for _, part := range []string{"select_list", "group_expressions"} {
		list := arr(root[part])
		for i, e := range list {
			if n := obj(e); n != nil {
				list[i] = b.rewriteInPlace(n)
			}
		}
	}
	// A result column named after its column keeps that name.
	for i, item := range arr(root["select_list"]) {
		if n := obj(item); n != nil && str(n["alias"]) == "" {
			n["alias"] = a.outputs[i]
		}
	}
	return b, nil
}

// formatBound writes a bound as a DOUBLE literal.
func formatBound(v float64) string {
	return "CAST(" + strconv.FormatFloat(v, 'g', -1, 64) + " AS DOUBLE)"
}

// rewriteInPlace makes n read the subquery's columns.
func (b *bounding) rewriteInPlace(n node) node {
	return walk(n, func(m node) node {
		parts := arr(m["column_names"])
		if str(m["class"]) != "COLUMN_REF" || len(parts) != 2 || str(parts[0]) == boundAlias {
			return nil
		}
		m["column_names"] = []any{boundAlias, b.columns[strings.ToLower(str(parts[0])+"."+str(parts[1]))]}
		return m
	})
}

// and is the conditions ANDed together.
func (p *Parser) and(ctx context.Context, conds []node) (node, error) {
	if len(conds) == 1 {
		return clone(conds[0]), nil
	}
	terms := make([]string, len(conds))
	for i := range conds {
		terms[i] = fmt.Sprintf("__igloo_cond_%d", i)
	}
	n, err := p.expression(ctx, strings.Join(terms, " AND "))
	if err != nil {
		return nil, err
	}
	return walk(n, func(m node) node {
		if str(m["class"]) != "COLUMN_REF" || len(arr(m["column_names"])) != 1 {
			return nil
		}
		if i, ok := strings.CutPrefix(str(arr(m["column_names"])[0]), "__igloo_cond_"); ok {
			j, _ := strconv.Atoi(i)
			return clone(conds[j])
		}
		return nil
	}), nil
}

// atoms are the conditions of each combination of filters and their
// opposites: for f and g, f AND g, f AND NOT g, NOT f AND g, and neither.
// A row that a filter's condition gives NULL is in its opposite.
func (p *Parser) atoms(ctx context.Context, filters []node) ([]node, error) {
	if len(filters) == 0 {
		return nil, nil
	}
	var out []node
	for mask := 0; mask < 1<<len(filters); mask++ {
		var terms []string
		for i := range filters {
			t := fmt.Sprintf("coalesce(__igloo_filter_%d, false)", i)
			if mask&(1<<i) == 0 {
				t = "NOT " + t
			}
			terms = append(terms, t)
		}
		n, err := p.expression(ctx, strings.Join(terms, " AND "))
		if err != nil {
			return nil, err
		}
		out = append(out, walk(n, func(m node) node {
			if str(m["class"]) != "COLUMN_REF" || len(arr(m["column_names"])) != 1 {
				return nil
			}
			if i, ok := strings.CutPrefix(str(arr(m["column_names"])[0]), "__igloo_filter_"); ok {
				j, _ := strconv.Atoi(i)
				return clone(filters[j])
			}
			return nil
		}))
	}
	return out, nil
}

// bindValue turns a parameter's value into what a run binds: its text,
// which the run casts to the parameter's type, or nil for NULL.
func bindValue(v any) (any, error) {
	switch v := v.(type) {
	case nil:
		return nil, nil
	case string:
		return v, nil
	case bool:
		return strconv.FormatBool(v), nil
	case json.Number:
		return v.String(), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int:
		return strconv.Itoa(v), nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	}
	return nil, fmt.Errorf("unsupported value %v: give a string, number, boolean or NULL", v)
}
