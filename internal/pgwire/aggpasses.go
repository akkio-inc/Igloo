package pgwire

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Aggregates in passes. DuckDB keeps the state of a list or string
// aggregate, and of any DISTINCT one, per group, in memory, until the
// aggregate is done, and spills it badly: a dbt model that groups 36
// million rows into 16 million (date, person) groups with nine of them
// (ARRAY_UNIQUE_AGG and LISTAGG(DISTINCT ...), list(DISTINCT ...) and
// string_agg(DISTINCT ...) once translated) ran out of memory after
// spilling 307 GiB, where a Snowflake X-Small built it in two minutes.
//
// So when CREATE TABLE ... AS reads such an aggregate, and DuckDB's plan
// estimates more groups than a quarter of the query memory holds the states
// of, Igloo fills the table in passes, each over the groups whose keys hash
// to its number: the aggregate's WHERE gains hash(keys) % passes = pass. A
// group's rows all fall in one pass, so the passes together give the rows a
// single run would, as long as each row of the result comes from one group:
// the statement reads the aggregate once, through layers that only project
// and filter, and calls no function whose value can change between runs
// (see sessionFunctions). Each pass reads the aggregate's input again.

// aggPasses is a query split into passes: how many at first, the groups
// DuckDB estimated, and the query of each pass of any number of them.
type aggPasses struct {
	passes int
	groups int64
	// query is the query of pass of n: the aggregate's WHERE gains
	// hash(keys) % n = pass.
	query func(pass, n int) (string, error)
}

const (
	// aggStateBytes is what a list, string or DISTINCT aggregate's state
	// counts for per group, and aggKeyBytes a group's key and slot.
	aggStateBytes = 200
	aggKeyBytes   = 64
	// maxAggPasses bounds the passes.
	maxAggPasses = 64
)

// holisticAggregates are DuckDB's aggregates that keep a group's values.
var holisticAggregates = map[string]bool{
	"list": true, "array_agg": true, "string_agg": true, "group_concat": true, "histogram": true, "mode": true,
	"median": true, "quantile": true, "quantile_cont": true, "quantile_disc": true,
}

// errAggPasses is why a query is not split: anything that falls outside
// what passes are known to give the same rows for.
var errAggPasses = errors.New("not an aggregate to run in passes")

// aggPassesOf splits query, the query of a CREATE TABLE ... AS, into
// passes, or returns nil. at least, when positive, is the fewest passes,
// whatever DuckDB estimates.
func (s *Server) aggPassesOf(ctx context.Context, sess *session, query string, atLeast int) *aggPasses {
	if s.queryMemory <= 0 {
		return nil
	}
	ap, err := s.splitAggregate(ctx, sess, query, atLeast)
	if err != nil {
		if !errors.Is(err, errAggPasses) {
			s.log.Debug("could not split an aggregate into passes", "err", err)
		}
		return nil
	}
	return ap
}

func (s *Server) splitAggregate(ctx context.Context, sess *session, query string, atLeast int) (*aggPasses, error) {
	f, err := s.functionSets(ctx, sess)
	if err != nil {
		return nil, err
	}
	// Functions whose value can differ from one pass to the next. error(),
	// which DuckDB counts volatile and Snowflake's dialect calls to refuse
	// a bad value (ARRAY_FLATTEN of what is not an array of arrays, say),
	// gives none: the passes together call it on the rows one run does.
	for _, w := range sqllex.CodeWords(query) {
		if w := strings.ToLower(w); f.unstable[w] || volatile[w] {
			return nil, errAggPasses
		}
	}
	serialize := func(q string) (map[string]any, error) {
		text, err := queryOne(ctx, sess.conn, "SELECT system.main.json_serialize_sql($1::VARCHAR)::VARCHAR", q)
		if err != nil {
			return nil, err
		}
		// Numbers as they are: query_location is a 64-bit integer, which a
		// float64 would not keep.
		dec := json.NewDecoder(strings.NewReader(text))
		dec.UseNumber()
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			return nil, err
		}
		if doc["error"] == true {
			return nil, errAggPasses
		}
		return doc, nil
	}
	doc, err := serialize(query)
	if err != nil {
		return nil, err
	}
	_, keys, err := passAggregate(doc, f)
	if err != nil {
		return nil, err
	}
	groups, states, err := estimatedAggregate(ctx, sess, query)
	if err != nil {
		return nil, err
	}
	need := groups * int64(states*aggStateBytes+aggKeyBytes)
	budget := s.queryMemory / 4
	passes := int((need + budget - 1) / budget)
	if atLeast <= 0 && passes <= 1 {
		return nil, errAggPasses
	}
	passes = min(max(nextPowerOfTwo(passes), atLeast, 2), maxAggPasses)
	// The filter, from DuckDB's own parse: (hash(keys) % passes) = pass, and
	// the conjunction it joins the aggregate's WHERE with.
	tmpl, err := serialize("SELECT 1 WHERE (hash(x) % 2) = 1 AND y")
	if err != nil {
		return nil, err
	}
	and := whereOf(tmpl)
	eq := asMap(asList(and["children"])[0])
	if eq == nil {
		return nil, errAggPasses
	}
	out := &aggPasses{passes: passes, groups: groups, query: func(pass, n int) (string, error) {
		d := cloneTree(doc).(map[string]any)
		a, _, err := passAggregate(d, f)
		if err != nil {
			return "", err
		}
		cond := cloneTree(eq).(map[string]any)
		mod := asMap(cond["left"])
		hash := asMap(asList(mod["children"])[0])
		hash["children"] = cloneTree(keys)
		setConstant(asMap(asList(mod["children"])[1]), n)
		setConstant(asMap(cond["right"]), pass)
		if where := asMap(a["where_clause"]); where != nil {
			conj := cloneTree(and).(map[string]any)
			conj["children"] = []any{where, cond}
			cond = conj
		}
		a["where_clause"] = cond
		text, err := json.Marshal(d)
		if err != nil {
			return "", err
		}
		return queryOne(ctx, sess.conn, "SELECT system.main.json_deserialize_sql($1::VARCHAR::JSON)", string(text))
	}}
	// The passes' rows must be the query's.
	want, err := describeQuery(ctx, sess, query)
	if err != nil {
		return nil, err
	}
	first, err := out.query(0, passes)
	if err != nil {
		return nil, err
	}
	got, err := describeQuery(ctx, sess, first)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(want, got) {
		return nil, fmt.Errorf("the passes describe as %v, the query as %v", got, want)
	}
	return out, nil
}

// passAggregate finds, in a statement's parse, the grouped aggregate every
// row of the statement's result comes from, and the expressions it groups
// by (positions and aliases resolved), or fails with errAggPasses.
func passAggregate(doc map[string]any, f *functionSets) (map[string]any, []any, error) {
	stmts := asList(doc["statements"])
	if len(stmts) != 1 {
		return nil, nil, errAggPasses
	}
	root := asMap(asMap(stmts[0])["node"])
	refs := map[string]int{}
	walkTree(root, func(m map[string]any) {
		if str(m["type"]) == "BASE_TABLE" && str(m["schema_name"]) == "" && str(m["catalog_name"]) == "" {
			refs[strings.ToLower(str(m["table_name"]))]++
		}
	})
	var scopes []map[string]any
	node := root
	for range 32 {
		if str(node["type"]) != "SELECT_NODE" || len(asList(node["modifiers"])) > 0 || node["sample"] != nil ||
			node["qualify"] != nil || str(node["aggregate_handling"]) != "STANDARD_HANDLING" {
			return nil, nil, errAggPasses
		}
		if len(asList(node["group_expressions"])) > 0 {
			keys, err := passKeys(node, f)
			return node, keys, err
		}
		// A layer above the aggregate: one input, rows passed on as they
		// are, but for filters and new columns.
		if node["having"] != nil || callsAny(node, func(m map[string]any) bool {
			return str(m["class"]) == "WINDOW" || str(m["class"]) == "FUNCTION" && f.aggregates[strings.ToLower(str(m["function_name"]))]
		}) {
			return nil, nil, errAggPasses
		}
		scopes = append(scopes, asMap(node["cte_map"]))
		from := asMap(node["from_table"])
		switch str(from["type"]) {
		case "SUBQUERY":
			node = asMap(asMap(asMap(from["subquery"])["node"]))
		case "BASE_TABLE":
			name := strings.ToLower(str(from["table_name"]))
			if str(from["schema_name"]) != "" || str(from["catalog_name"]) != "" || refs[name] != 1 {
				return nil, nil, errAggPasses
			}
			cte := cteNamed(scopes, name)
			if cte == nil {
				return nil, nil, errAggPasses
			}
			node = asMap(asMap(cte["query"])["node"])
		default:
			return nil, nil, errAggPasses
		}
		if node == nil {
			return nil, nil, errAggPasses
		}
	}
	return nil, nil, errAggPasses
}

// passKeys checks an aggregate that passes may split: one set of groups,
// list, string or DISTINCT aggregates among its calls, and no window over
// its groups; and returns what it groups by.
func passKeys(node map[string]any, f *functionSets) ([]any, error) {
	if len(asList(node["group_sets"])) != 1 || callsAny(node, func(m map[string]any) bool { return str(m["class"]) == "WINDOW" }) {
		return nil, errAggPasses
	}
	holistic := false
	for _, part := range []any{node["select_list"], node["having"]} {
		walkTree(part, func(m map[string]any) {
			name := strings.ToLower(str(m["function_name"]))
			if str(m["class"]) == "FUNCTION" && (holisticAggregates[name] || m["distinct"] == true && f.aggregates[name]) {
				holistic = true
			}
		})
	}
	if !holistic {
		return nil, errAggPasses
	}
	selects := asList(node["select_list"])
	aliases := map[string]bool{}
	for _, e := range selects {
		if a := strings.ToLower(str(asMap(e)["alias"])); a != "" {
			aliases[a] = true
		}
	}
	var keys []any
	for _, g := range asList(node["group_expressions"]) {
		m := asMap(g)
		switch {
		case str(m["class"]) == "CONSTANT":
			// GROUP BY 2: the second item of the select list.
			n, ok := constantInt(m)
			if !ok || n < 1 || n > len(selects) {
				return nil, errAggPasses
			}
			e := cloneTree(selects[n-1]).(map[string]any)
			e["alias"] = ""
			if str(e["class"]) == "CONSTANT" {
				return nil, errAggPasses
			}
			keys = append(keys, e)
		case str(m["class"]) == "COLUMN_REF" && len(asList(m["column_names"])) == 1 &&
			aliases[strings.ToLower(str(asList(m["column_names"])[0]))]:
			// A name that may be an alias of the select list, which a WHERE
			// cannot see.
			return nil, errAggPasses
		default:
			keys = append(keys, cloneTree(m))
		}
	}
	return keys, nil
}

// estimatedAggregate reads, from DuckDB's plan of query, the groups of its
// largest aggregate with list, string or DISTINCT aggregates, and how many
// of those it has.
func estimatedAggregate(ctx context.Context, sess *session, query string) (groups int64, states int, err error) {
	rows, err := sess.conn.QueryContext(ctx, "EXPLAIN (FORMAT JSON) "+query, nil)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, len(rows.Columns()))
	var plan any
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, 0, err
		}
		if key, _ := dest[0].(string); key == "physical_plan" && len(dest) > 1 {
			text, _ := dest[1].(string)
			if err := json.Unmarshal([]byte(text), &plan); err != nil {
				return 0, 0, err
			}
		}
	}
	walkTree(plan, func(m map[string]any) {
		if name := str(m["name"]); name != "HASH_GROUP_BY" && name != "PERFECT_HASH_GROUP_BY" {
			return
		}
		extra := asMap(m["extra_info"])
		n := 0
		for _, a := range asList(extra["Aggregates"]) {
			text := str(a)
			fn, _, _ := strings.Cut(text, "(")
			if holisticAggregates[strings.ToLower(fn)] || strings.Contains(text, "(DISTINCT ") {
				n++
			}
		}
		g, _ := strconv.ParseInt(str(extra["Estimated Cardinality"]), 10, 64)
		if n > 0 && g*int64(n) > groups*int64(states) {
			groups, states = g, n
		}
	})
	if states == 0 {
		return 0, 0, errAggPasses
	}
	return groups, states, nil
}

// describeQuery is the names and types of query's columns.
func describeQuery(ctx context.Context, sess *session, query string) ([]string, error) {
	cols, err := describeTable(ctx, sess, "SELECT * FROM (\n"+query+"\n) AS __igloo_q")
	if err != nil {
		return nil, err
	}
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.name + " " + c.typ
	}
	return out, nil
}

// whereOf is the WHERE of a parsed SELECT.
func whereOf(doc map[string]any) map[string]any {
	return asMap(asMap(asMap(asList(doc["statements"])[0])["node"])["where_clause"])
}

// cteNamed finds the common table expression name in scopes, innermost
// first.
func cteNamed(scopes []map[string]any, name string) map[string]any {
	for i := len(scopes) - 1; i >= 0; i-- {
		for _, e := range asList(scopes[i]["map"]) {
			if m := asMap(e); strings.EqualFold(str(m["key"]), name) {
				return asMap(m["value"])
			}
		}
	}
	return nil
}

// callsAny reports whether the expressions of a SELECT node, not those of
// the subqueries and tables it reads, have a node fn picks.
func callsAny(node map[string]any, fn func(map[string]any) bool) bool {
	found := false
	for _, part := range []any{node["select_list"], node["where_clause"]} {
		walkTree(part, func(m map[string]any) {
			if str(m["class"]) != "SUBQUERY" && fn(m) {
				found = true
			}
		})
	}
	return found
}

// walkTree calls fn on every object of a parse or plan, depth first.
func walkTree(v any, fn func(map[string]any)) {
	switch v := v.(type) {
	case map[string]any:
		fn(v)
		for _, c := range v {
			walkTree(c, fn)
		}
	case []any:
		for _, c := range v {
			walkTree(c, fn)
		}
	}
}

// cloneTree copies a parse.
func cloneTree(v any) any {
	switch v := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, c := range v {
			out[k] = cloneTree(c)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, c := range v {
			out[i] = cloneTree(c)
		}
		return out
	}
	return v
}

// constantInt reads an integer constant of a parse.
func constantInt(m map[string]any) (int, bool) {
	v := asMap(m["value"])
	if v["is_null"] == true {
		return 0, false
	}
	switch n := v["value"].(type) {
	case json.Number:
		i, err := strconv.Atoi(n.String())
		return i, err == nil
	case float64:
		return int(n), n == float64(int(n))
	case string:
		i, err := strconv.Atoi(n)
		return i, err == nil
	}
	return 0, false
}

// setConstant sets an integer constant of a parse.
func setConstant(m map[string]any, n int) {
	if v := asMap(m["value"]); v != nil {
		v["value"] = n
	}
}

// nextPowerOfTwo is the least power of two at or above n.
func nextPowerOfTwo(n int) int {
	p := 1
	for p < n {
		p *= 2
	}
	return p
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// fillInPasses fills staging with ap's passes, one after another. A pass
// that runs out of memory, its groups more than DuckDB estimated, runs
// again as the two passes of twice as many that its groups hash to, and the
// passes after it are split likewise, up to maxAggPasses: a group's
// hash(keys) % 2n is pass or pass + n exactly when hash(keys) % n is pass,
// so the passes already done stay done.
func (s *Server) fillInPasses(ctx context.Context, fill func(string) (int64, error), staging []string, ap *aggPasses) (int64, error) {
	s.log.Debug("an aggregate runs in passes", "passes", ap.passes, "groups", ap.groups)
	s.metrics.aggregatePasses()
	n := ap.passes
	pending := make([]int, n)
	for i := range pending {
		pending[i] = i
	}
	var rows int64
	for len(pending) > 0 {
		q, err := ap.query(pending[0], n)
		if err != nil {
			return 0, err
		}
		m, err := fill("INSERT INTO " + quoteName(staging) + "\n" + q)
		if isOutOfMemory(err) && 2*n <= maxAggPasses && ctx.Err() == nil {
			s.log.Debug("a pass ran out of memory; the rest run in twice as many", "passes", 2*n)
			split := make([]int, 0, 2*len(pending))
			for _, p := range pending {
				split = append(split, p, p+n)
			}
			pending, n = split, 2*n
			continue
		}
		if err != nil {
			return 0, err
		}
		rows += m
		pending = pending[1:]
	}
	return rows, nil
}

// isOutOfMemory reports whether DuckDB failed for want of memory or of room
// to spill.
func isOutOfMemory(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Out of Memory Error")
}
