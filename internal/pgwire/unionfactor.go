package pgwire

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Union branches over one FROM. Reports often compare several measures of
// the same rows as a UNION ALL of SELECTs that differ only in what they
// select, and aggregate the union:
//
//	SELECT style, channel, count(...) FROM (
//	  SELECT v.id, 'loyal' AS style, 'email' AS channel, v.loyal IN (1, 2) AS strong FROM v JOIN g ON g.id = v.id
//	  UNION ALL SELECT v.id, 'loyal', 'video', v.video IN (1, 2) FROM v JOIN g ON g.id = v.id
//	  UNION ALL ...) AS s GROUP BY style, channel
//
// DuckDB computes every branch on its own, so sixteen branches scan v and
// build the join sixteen times, and workers shuffle v's rows for each.
// Igloo computes the FROM and WHERE once instead, and makes each of their
// rows once per branch, with what that branch selects:
//
//	SELECT v.id, CASE _igloo_branch WHEN 0 THEN 'loyal' WHEN 1 THEN 'loyal' ... END AS style, ...
//	FROM v JOIN g ON g.id = v.id CROSS JOIN range(16) AS _igloo_branches(_igloo_branch)
//
// On 40 million rows of a sixteen-branch report, that took 2.3 to 2.6 s
// instead of 45 to 99.
//
// A UNION ALL is factored when it gives the rows it gave, and nothing
// reading it can tell:
//
//   - It is a derived table (a parenthesized query after FROM) that a SELECT
//     reads alone and groups or aggregates, with no aggregate whose result
//     depends on the order of its input (list, string_agg, first and the
//     like) and no window function: the factored rows come in another order.
//   - Its branches are SELECTs with the same FROM and WHERE, parse for parse,
//     that select columns, constants and operators only (no function, which
//     could be an aggregate, and no subquery), and do nothing else: no
//     DISTINCT, GROUP BY, ORDER BY, LIMIT, WITH or sample.
//   - The FROM and WHERE give the same rows each time they run, as computing
//     them once assumes: no sample, LIMIT, window, volatile function (or
//     macro calling one) or parameter in them or in the common table
//     expressions they read. The statement has no parameters at all.
//   - Each column that differs between branches has a name in the first
//     branch: an alias, or a column's own name.
//   - The factored SELECT has the union's columns, names and types alike, as
//     DuckDB describes both: CASE and UNION ALL unify their branches' types
//     each in its own way.
//
// Like the other rewrites, this one runs on the statement's text: what
// stays is the client's text, and the FROM and WHERE are the first branch's,
// copied once. If DuckDB cannot prepare the result, the statement runs as
// written.

// maxUnionBranches bounds the branches a factored UNION ALL may have: each
// row of its FROM is made once per branch.
const maxUnionBranches = 256

// branchColumn names the column that numbers each row's branch.
const branchColumn = "_igloo_branch"

// orderedAggregates are the aggregates whose result depends on the order
// of their input rows, which a factored union changes.
var orderedAggregates = map[string]bool{"list": true, "array_agg": true, "string_agg": true, "group_concat": true,
	"listagg": true, "first": true, "last": true, "any_value": true, "arbitrary": true, "arg_min": true, "arg_max": true,
	"argmin": true, "argmax": true, "min_by": true, "max_by": true, "arg_min_null": true, "arg_max_null": true,
	"mode": true, "array_agg_distinct": true, "bit_string_agg": true}

// readerAggregates are aggregates whose presence makes a SELECT without
// GROUP BY an aggregation of its rows, which hides their order.
var readerAggregates = map[string]bool{"count": true, "count_star": true, "sum": true, "min": true, "max": true,
	"avg": true, "mean": true, "bool_and": true, "bool_or": true, "approx_count_distinct": true}

// unionTools are what factoring a statement's unions needs of DuckDB.
type unionTools struct {
	// serialize returns a statement's parse as json_serialize_sql writes
	// it, nulls and all.
	serialize func(text string) (string, error)
	// deparse writes a parse back as SQL (json_deserialize_sql).
	deparse func(doc string) (string, error)
	// describe returns a query's result columns, as names and types.
	describe func(query string) ([]string, error)
	// volatile returns the volatile functions and the macros that call one.
	volatile func() (map[string]bool, error)
}

// factorUnions returns text with the UNION ALLs it may factor computed as
// one SELECT each, or "" when there is none.
func (s *Server) factorUnions(ctx context.Context, sess *session, text string) string {
	if sess == nil || sess.conn == nil || !unionAll.MatchString(maskCode(text)) {
		return ""
	}
	conn := sess.conn
	tools := unionTools{
		serialize: func(q string) (string, error) {
			return queryOne(ctx, conn, "SELECT system.main.json_serialize_sql($1::VARCHAR)::VARCHAR", q)
		},
		deparse: func(doc string) (string, error) {
			return queryOne(ctx, conn, "SELECT system.main.json_deserialize_sql($1::VARCHAR::JSON)", doc)
		},
		describe: func(q string) ([]string, error) {
			rows, err := conn.QueryContext(ctx, "DESCRIBE "+q, nil)
			if err != nil {
				return nil, err
			}
			defer rows.Close() //nolint:errcheck
			var out []string
			dest := make([]driver.Value, len(rows.Columns()))
			for {
				if err := rows.Next(dest); err != nil {
					if errors.Is(err, io.EOF) {
						return out, nil
					}
					return nil, err
				}
				out = append(out, fmt.Sprint(dest[0])+" "+fmt.Sprint(dest[1]))
			}
		},
		volatile: func() (map[string]bool, error) {
			f, err := s.functionSets(ctx, sess)
			if err != nil {
				return nil, err
			}
			return f.volatile, nil
		},
	}
	out, err := factorUnionText(text, tools)
	if err != nil {
		s.log.Debug("could not factor unions", "err", err)
		return ""
	}
	return out
}

// unionAll finds UNION ALL in SQL code, before parsing.
var unionAll = regexp.MustCompile(`(?i)\bunion\s+all\b`)

// queryOne runs a query of one parameter and returns its one value as text.
func queryOne(ctx context.Context, conn *duckdb.Conn, query, arg string) (string, error) {
	rows, err := conn.QueryContext(ctx, query, []driver.NamedValue{{Ordinal: 1, Value: arg}})
	if err != nil {
		return "", err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			err = errors.New("no result")
		}
		return "", err
	}
	s, ok := dest[0].(string)
	if !ok {
		return "", fmt.Errorf("unexpected %T", dest[0])
	}
	return s, nil
}

// factoredUnion is a UNION ALL to factor: text[start:end] becomes sel.
// path leads to its parse from the statement's.
type factoredUnion struct {
	start, end int
	sel        string
	path       []any
}

// factorUnionText returns text with the UNION ALLs it may factor computed as
// one SELECT each, or "" when there is none.
func factorUnionText(text string, tools unionTools) (string, error) {
	raw, err := tools.serialize(text)
	if err != nil {
		return "", err
	}
	tree, err := decodeParse(raw)
	if err != nil {
		return "", err
	}
	stmt := mapOf(tree)
	if hasNode(stmt, func(m map[string]any) bool {
		return str(m["type"]) == "RECURSIVE_CTE_NODE" || str(m["class"]) == "PARAMETER"
	}) || strings.Contains(strings.ToLower(text), branchColumn) {
		return "", nil
	}
	ctes := map[string][]any{}
	walkMaps(stmt, func(m map[string]any) {
		if cm, ok := m["cte_map"].(map[string]any); ok {
			for _, e := range asList(cm["map"]) {
				em, _ := e.(map[string]any)
				name := strings.ToLower(str(em["key"]))
				ctes[name] = append(ctes[name], em["value"])
			}
		}
	})
	masked := maskCode(text)
	var vol map[string]bool
	var found []*factoredUnion
	var failed error
	walkPaths(stmt, nil, func(m map[string]any, path []any) {
		if failed != nil || str(m["type"]) != "SELECT_NODE" {
			return
		}
		from, _ := m["from_table"].(map[string]any)
		if str(from["type"]) != "SUBQUERY" || !hidesOrder(m) {
			return
		}
		sub, _ := from["subquery"].(map[string]any)
		node, _ := sub["node"].(map[string]any)
		if str(node["type"]) != "SET_OPERATION_NODE" {
			return
		}
		if vol == nil {
			if vol, failed = tools.volatile(); failed != nil {
				return
			}
		}
		if f := factorUnion(text, masked, node, ctes, vol); f != nil {
			f.path = append(slices.Clone(path), "from_table", "subquery", "node")
			found = append(found, f)
		}
	})
	if failed != nil {
		return "", failed
	}
	// The outermost of nested ones; those inside on another statement.
	var edits []edit
	var kept []*factoredUnion
	for _, f := range found {
		inside := false
		for _, o := range found {
			inside = inside || o != f && f.start >= o.start && f.end <= o.end
		}
		if !inside {
			kept = append(kept, f)
			edits = append(edits, edit{f.start, f.end, f.sel})
		}
	}
	if len(kept) == 0 {
		return "", nil
	}
	out, err := applyEdits(text, edits)
	if err != nil {
		return "", err
	}
	// Check each against the union it replaces: the same columns, names and
	// types; or leave the statement as written.
	rawOut, err := tools.serialize(out)
	if err != nil {
		return "", err
	}
	for _, f := range kept {
		before, err := unionColumns(tools, raw, f.path, "SET_OPERATION_NODE")
		if err != nil {
			return "", err
		}
		now, err := unionColumns(tools, rawOut, f.path, "SELECT_NODE")
		if err != nil {
			return "", err
		}
		if !slices.Equal(before, now) {
			return "", fmt.Errorf("a factored union's columns %v differ from the union's %v", now, before)
		}
	}
	return out, nil
}

// hidesOrder reports whether a SELECT reading a derived table hides the
// order of its rows: it groups or aggregates them, with no aggregate that
// depends on their order and no window function.
func hidesOrder(sel map[string]any) bool {
	if str(sel["aggregate_handling"]) != "" && str(sel["aggregate_handling"]) != "STANDARD_HANDLING" {
		return false
	}
	parts := []any{sel["select_list"], sel["having"], sel["qualify"], sel["modifiers"]}
	if hasNode(parts, func(m map[string]any) bool {
		return str(m["class"]) == "WINDOW" || orderedAggregates[strings.ToLower(str(m["function_name"]))]
	}) {
		return false
	}
	return len(asList(sel["group_expressions"])) > 0 || len(asList(sel["group_sets"])) > 0 ||
		hasNode(sel["select_list"], func(m map[string]any) bool {
			return str(m["class"]) == "FUNCTION" && readerAggregates[strings.ToLower(str(m["function_name"]))]
		})
}

// factorUnion returns how to factor the UNION ALL node, or nil if it may
// not be.
func factorUnion(text, masked string, node map[string]any, ctes map[string][]any, vol map[string]bool) *factoredUnion {
	var branches []map[string]any
	var flatten func(n map[string]any) bool
	flatten = func(n map[string]any) bool {
		switch str(n["type"]) {
		case "SELECT_NODE":
			branches = append(branches, n)
			return true
		case "SET_OPERATION_NODE":
			if str(n["setop_type"]) != "UNION" || n["setop_all"] != true || len(asList(n["modifiers"])) > 0 ||
				len(asList(mapOf(n["cte_map"])["map"])) > 0 {
				return false
			}
			for _, side := range []string{"left", "right"} {
				c, _ := n[side].(map[string]any)
				if c == nil || !flatten(c) {
					return false
				}
			}
			return true
		}
		return false
	}
	if !flatten(node) || len(branches) < 2 || len(branches) > maxUnionBranches {
		return nil
	}
	first := branches[0]
	from, _ := first["from_table"].(map[string]any)
	where := first["where_clause"]
	if from == nil || strings.HasPrefix(str(from["type"]), "EMPTY") {
		return nil
	}
	width := len(asList(first["select_list"]))
	for _, b := range branches {
		if !plainBranch(b) || len(asList(b["select_list"])) != width ||
			!sameParse(b["from_table"], from, false) || !sameParse(b["where_clause"], where, false) {
			return nil
		}
	}
	for _, part := range append([]any{from, where}, readCTEs(ctes, from, where)...) {
		if !repeatable(part) || hasNode(part, func(m map[string]any) bool {
			if f, ok := m["function"].(map[string]any); ok && vol[strings.ToLower(str(f["function_name"]))] {
				return true
			}
			return vol[strings.ToLower(str(m["function_name"]))]
		}) {
			return nil
		}
	}

	// The branches' text: each SELECT's items, FROM and WHERE, joined by
	// UNION ALL, in the derived table's parentheses.
	spans := make([]branchSpan, len(branches))
	for i, b := range branches {
		sp, ok := readBranch(masked, b)
		if !ok {
			return nil
		}
		spans[i] = sp
	}
	if j := skipSpaceBack(masked, spans[0].start); j == 0 || masked[j-1] != '(' {
		return nil
	}
	for i := range len(spans) - 1 {
		j := spans[i].end
		if !strings.EqualFold(nextWord(masked, j), "UNION") {
			return nil
		}
		j = skipSpace(masked, j) + len("UNION")
		if !strings.EqualFold(nextWord(masked, j), "ALL") {
			return nil
		}
		if skipSpace(masked, skipSpace(masked, j)+len("ALL")) != spans[i+1].start {
			return nil
		}
	}
	last := spans[len(spans)-1].end
	if last >= len(masked) || masked[last] != ')' {
		return nil
	}

	// The factored SELECT: a column as the first branch wrote it where the
	// branches agree, and otherwise each branch's in a CASE on the branch.
	var items []string
	for j := range width {
		item0, _ := asList(first["select_list"])[j].(map[string]any)
		same := true
		for _, b := range branches[1:] {
			item, _ := asList(b["select_list"])[j].(map[string]any)
			same = same && sameParse(item, item0, true)
		}
		if same {
			items = append(items, withoutComments(text[spans[0].items[j].start:spans[0].items[j].end]))
			continue
		}
		name := str(item0["alias"])
		if names := asList(item0["column_names"]); name == "" && str(item0["class"]) == "COLUMN_REF" && len(names) > 0 {
			name = str(names[len(names)-1])
		}
		if name == "" {
			return nil
		}
		var c strings.Builder
		c.WriteString("CASE " + branchColumn)
		for i, b := range branches {
			item, _ := asList(b["select_list"])[j].(map[string]any)
			sp := spans[i].items[j]
			expr, ok := withoutAlias(text[sp.start:sp.end], str(item["alias"]))
			if !ok {
				return nil
			}
			fmt.Fprintf(&c, " WHEN %d THEN (%s)", i, expr)
		}
		c.WriteString(" END AS " + quoteIdent(name))
		items = append(items, c.String())
	}
	sel := "SELECT " + strings.Join(items, ", ") + " FROM " +
		withoutComments(text[spans[0].fromBody:spans[0].fromEnd]) +
		" CROSS JOIN range(" + strconv.Itoa(len(branches)) + ") AS _igloo_branches(" + branchColumn + ")"
	if where != nil {
		sel += " WHERE " + withoutComments(text[spans[0].where:spans[0].end])
	}
	return &factoredUnion{start: spans[0].start, end: last, sel: sel}
}

// plainBranch reports whether a branch of a UNION ALL only selects
// columns, constants and operators from its FROM and WHERE.
func plainBranch(b map[string]any) bool {
	if len(asList(mapOf(b["cte_map"])["map"])) > 0 || len(asList(b["modifiers"])) > 0 ||
		len(asList(b["group_expressions"])) > 0 || len(asList(b["group_sets"])) > 0 ||
		b["having"] != nil || b["qualify"] != nil || b["sample"] != nil ||
		str(b["aggregate_handling"]) != "" && str(b["aggregate_handling"]) != "STANDARD_HANDLING" {
		return false
	}
	for _, item := range asList(b["select_list"]) {
		m, _ := item.(map[string]any)
		if m == nil || !plainItem(m) || hasNode(m, func(m map[string]any) bool { return str(m["class"]) == "STAR" }) {
			return false
		}
	}
	return true
}

// branchSpan is where a UNION ALL branch's parts are in its text.
type branchSpan struct {
	// start is SELECT's place; items are the select items; the FROM
	// clause's body is fromBody to fromEnd, the WHERE clause's, if any,
	// where to end, and the branch ends at end.
	start             int
	items             []span
	fromBody, fromEnd int
	where, end        int
}

// readBranch finds a branch's parts in masked text, checked against its
// parse.
func readBranch(masked string, b map[string]any) (branchSpan, bool) {
	list := asList(b["select_list"])
	first := minLocation(list)
	if first < 0 {
		return branchSpan{}, false
	}
	// SELECT is the word before the first item, but for the parentheses,
	// signs and NOTs it may start with.
	i := first
	for {
		j := skipSpaceBack(masked, i)
		switch {
		case j > 0 && strings.ContainsRune("(+-", rune(masked[j-1])):
			i = j - 1
			continue
		case j >= 3 && strings.EqualFold(masked[j-3:j], "NOT") && (j == 3 || !sqllex.IdentByte(masked[j-4])):
			i = j - 3
			continue
		}
		i = j
		break
	}
	if i < len("SELECT") || !strings.EqualFold(masked[i-len("SELECT"):i], "SELECT") ||
		i > len("SELECT") && sqllex.IdentByte(masked[i-len("SELECT")-1]) {
		return branchSpan{}, false
	}
	sp := branchSpan{start: i - len("SELECT"), where: -1}
	fromAt := keywordAfter(masked, i, "FROM")
	if fromAt < 0 || maxLocation(list) >= fromAt {
		return branchSpan{}, false
	}
	// The items, split at the commas between them.
	depth, start := 0, i
	for j := i; j < fromAt; j++ {
		switch masked[j] {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case ',':
			if depth == 0 {
				sp.items = append(sp.items, span{start, j})
				start = j + 1
			}
		}
	}
	sp.items = append(sp.items, span{start, fromAt})
	if len(sp.items) != len(list) {
		return branchSpan{}, false
	}
	for k, item := range list {
		m, _ := item.(map[string]any)
		if !within(m, sp.items[k].start, sp.items[k].end) {
			return branchSpan{}, false
		}
	}
	sp.fromBody = fromAt + len("FROM")
	stop, word := clauseEnd(masked, sp.fromBody)
	sp.fromEnd, sp.end = stop, stop
	if word == "WHERE" {
		sp.where = stop + len("WHERE")
		sp.end, word = clauseEnd(masked, sp.where)
	}
	from, _ := b["from_table"].(map[string]any)
	where, _ := b["where_clause"].(map[string]any)
	if word != "" && word != "UNION" || (where != nil) != (sp.where >= 0) || !within(from, sp.fromBody, sp.fromEnd) ||
		where != nil && !within(where, sp.where, sp.end) {
		return branchSpan{}, false
	}
	return sp, true
}

// withoutAlias returns a select item's expression: its text without the
// alias, if it has one, and the AS before it.
func withoutAlias(item, alias string) (string, bool) {
	code := strings.TrimSpace(withoutComments(item))
	if alias == "" {
		return code, code != ""
	}
	var rest string
	switch {
	case strings.HasSuffix(code, `"`):
		i := len(code) - 1
		for i > 0 {
			j := strings.LastIndexByte(code[:i], '"')
			if j < 0 {
				return "", false
			}
			if j > 0 && code[j-1] == '"' {
				i = j - 1 // a doubled quote inside the name
				continue
			}
			if strings.ReplaceAll(code[j+1:len(code)-1], `""`, `"`) != alias {
				return "", false
			}
			rest = code[:j]
			break
		}
	default:
		j := len(code)
		for j > 0 && sqllex.IdentByte(code[j-1]) {
			j--
		}
		if !strings.EqualFold(code[j:], alias) {
			return "", false
		}
		rest = code[:j]
	}
	rest = strings.TrimSpace(rest)
	if n := len(rest); n >= 2 && strings.EqualFold(rest[n-2:], "AS") && (n == 2 || !sqllex.IdentByte(rest[n-3])) {
		rest = strings.TrimSpace(rest[:n-2])
	}
	return rest, rest != ""
}

// withoutComments returns SQL text with its comments made spaces, so that
// it can be put on one line with other text.
func withoutComments(text string) string {
	var b strings.Builder
	for _, seg := range sqllex.Segments(text) {
		if seg.Kind == sqllex.Comment {
			b.WriteByte(' ')
		} else {
			b.WriteString(seg.Text)
		}
	}
	return strings.TrimSpace(b.String())
}

// skipSpaceBack returns where the white space before i in s starts.
func skipSpaceBack(s string, i int) int {
	for i > 0 && isSpace(s[i-1]) {
		i--
	}
	return i
}

// sameParse reports whether two parts of parses are the same but for where
// they are in their texts, and, if noAlias, the outermost expression's
// alias.
func sameParse(a, b any, noAlias bool) bool {
	return reflectEqual(stripped(a, noAlias), stripped(b, noAlias))
}

// stripped returns a part of a parse without its positions, and, if
// noAlias, without the alias of its outermost object.
func stripped(v any, noAlias bool) any {
	switch v := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, c := range v {
			if k == "query_location" || noAlias && k == "alias" {
				continue
			}
			out[k] = stripped(c, false)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, c := range v {
			out[i] = stripped(c, false)
		}
		return out
	}
	return v
}

// reflectEqual compares decoded JSON values.
func reflectEqual(a, b any) bool {
	ja, err1 := json.Marshal(a)
	jb, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && string(ja) == string(jb)
}

// mapOf returns v as an object, or nil.
func mapOf(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// walkPaths calls fn on every object of a parse with its path from the
// root.
func walkPaths(v any, path []any, fn func(m map[string]any, path []any)) {
	switch v := v.(type) {
	case []any:
		for i, c := range v {
			walkPaths(c, append(slices.Clip(path), i), fn)
		}
	case map[string]any:
		fn(v, path)
		for k, c := range v {
			walkPaths(c, append(slices.Clip(path), k), fn)
		}
	}
}

// scopeAt returns the common table expressions the part of a parse at path
// sees, outermost first: those of the queries around it, and in a WITH
// clause those before its own.
func scopeAt(v any, path []any) []any {
	var scope []any
	for i, p := range path {
		m := mapOf(v)
		if entries := asList(mapOf(m["cte_map"])["map"]); len(entries) > 0 {
			if p == "cte_map" && i+2 < len(path) {
				n, _ := path[i+2].(int)
				scope = append(scope, entries[:min(n, len(entries))]...)
			} else {
				scope = append(scope, entries...)
			}
		}
		v = at(v, []any{p})
	}
	return scope
}

// at returns the part of a parse at path.
func at(v any, path []any) any {
	for _, p := range path {
		switch p := p.(type) {
		case string:
			v = mapOf(v)[p]
		case int:
			l := asList(v)
			if p >= len(l) {
				return nil
			}
			v = l[p]
		}
	}
	return v
}

// unionColumns describes the result columns of the query node at path in
// raw, a statement's parse, which must be of type kind, as the common table
// expressions it sees see it: SELECT * FROM (it), with those expressions
// defined before.
func unionColumns(tools unionTools, raw string, path []any, kind string) ([]string, error) {
	stmt, err := decodeExact(raw)
	if err != nil {
		return nil, err
	}
	q := mapOf(at(stmt, path))
	if str(q["type"]) != kind {
		return nil, fmt.Errorf("a %s where a %s was expected", str(q["type"]), kind)
	}
	scope := scopeAt(stmt, path)
	seen := map[string]bool{}
	for _, e := range scope {
		key := strings.ToLower(str(mapOf(e)["key"]))
		if seen[key] {
			return nil, fmt.Errorf("common table expression %s is defined twice around a union", key)
		}
		seen[key] = true
	}
	template, err := tools.serialize("SELECT * FROM (SELECT 1) AS _igloo_union")
	if err != nil {
		return nil, err
	}
	probe, err := decodeExact(template)
	if err != nil {
		return nil, err
	}
	node := mapOf(probe["node"])
	mapOf(mapOf(node["from_table"])["subquery"])["node"] = q
	cm := mapOf(node["cte_map"])
	if cm == nil {
		return nil, errors.New("no cte_map in a parse")
	}
	cm["map"] = append([]any{}, scope...)
	doc, err := json.Marshal(map[string]any{"error": false, "statements": []any{probe}})
	if err != nil {
		return nil, err
	}
	sql, err := tools.deparse(string(doc))
	if err != nil {
		return nil, err
	}
	return tools.describe(sql)
}

// decodeExact reads a parse's one statement keeping its numbers as written:
// positions such as 18446744073709551615 do not survive float64.
func decodeExact(raw string) (map[string]any, error) {
	d := json.NewDecoder(strings.NewReader(raw))
	d.UseNumber()
	var doc struct {
		Error        bool             `json:"error"`
		ErrorMessage string           `json:"error_message"`
		Statements   []map[string]any `json:"statements"`
	}
	if err := d.Decode(&doc); err != nil {
		return nil, err
	}
	if doc.Error {
		return nil, errors.New(doc.ErrorMessage)
	}
	if len(doc.Statements) != 1 {
		return nil, fmt.Errorf("expected one statement, found %d", len(doc.Statements))
	}
	return doc.Statements[0], nil
}
