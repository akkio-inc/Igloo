package pgwire

import (
	"context"
	"regexp"
	"strings"

	"github.com/akkio-inc/igloo/internal/snowflake"
)

// Approximate distinct counts. DuckDB's approx_count_distinct is a
// HyperLogLog of 64 registers, which errs by about 13%: a column profile as
// data tools send it,
//
//	SELECT APPROX_COUNT_DISTINCT("c0") AS C0_D, APPROX_COUNT_DISTINCT("c1") AS C1_D FROM t
//
// counted 6 distinct values as 5, 1,000 as 1,249 and 100,000 as 96,408,
// and tools that decide from these counts which columns are categories got
// it wrong. Snowflake's APPROX_COUNT_DISTINCT errs by about 1.6%, and so does
// its translation, a sketch of 4,096 registers (see snowflake.SketchCount).
// Igloo counts DuckDB's function with that sketch too, so that a statement
// counts alike in either dialect:
//
//	SELECT CAST(coalesce(round(datasketch_hll_estimate(datasketch_hll(12, CASE WHEN "c0" IS NOT NULL THEN hash("c0") END))), 0) AS BIGINT) AS C0_D, ...
//
// As in the translation, a call in a select with GROUP BY, or as a window
// function, counts exactly, as count(DISTINCT x): a sketch per group or
// partition is slower than that with many of them. A subquery is a select
// of its own. A FILTER of a sketch becomes a condition of its CASE, which
// keeps the same rows. Rows with a NULL value do not count, the count is a
// BIGINT, and the count of no rows is 0, as with DuckDB's own.
//
// The sketch needs the datasketches extension, which every deployment loads
// (see hasSketches); without it, calls stay DuckDB's own. Only a SELECT
// statement parses: any other statement, and one with no call, is left as
// written. So is a call that is not DuckDB's function of one value
// (qualified with a schema, with an ORDER BY, EXPORT_STATE or IGNORE
// NULLS), or whose value is a parameter, volatile or a star, which the CASE
// reads twice. DuckDB names the column of a select item without an alias
// after the item as it prints it, so the calls in such an item stay as
// written, a subquery's included, unless the item is the call of a column
// alone: that one is rewritten, with the name as its alias.
//
// Statements in Snowflake's dialect are translated before this runs (see
// dialect.go), and their translations have no approx_count_distinct where
// the extension is loaded: there is nothing to count again.

// approxCall finds approx_count_distinct( in SQL code, before parsing.
var approxCall = regexp.MustCompile(`(?i)\bapprox_count_distinct\s*\(`)

const approxName = "approx_count_distinct"

// rewriteApproxCounts counts the approx_count_distinct calls of a SELECT
// statement as Snowflake's APPROX_COUNT_DISTINCT, where DuckDB has the
// datasketches extension.
func (s *Server) rewriteApproxCounts(ctx context.Context, sess *session, text string) string {
	if sess == nil || sess.conn == nil || !approxCall.MatchString(maskCode(text)) || !s.hasSketches() {
		return text
	}
	return approxCounts(text, func(q string) (any, error) { return parseStatement(ctx, sess.conn, q) },
		func(q string) (string, error) {
			return queryOne(ctx, sess.conn, "SELECT system.main.json_deserialize_sql(system.main.json_serialize_sql($1::VARCHAR))", q)
		})
}

// approxCounts rewrites the approx_count_distinct calls of text, a SELECT
// statement, which parse parses as json_serialize_sql does. deparse prints
// a statement as DuckDB does, json_deserialize_sql of its parse, which is
// how it names the columns of select items without an alias.
func approxCounts(text string, parse func(string) (any, error), deparse func(string) (string, error)) string {
	tree, err := parse(text)
	if err != nil {
		return text
	}
	stmt, _ := tree.(map[string]any)
	if stmt == nil {
		return text
	}
	r := &approxRewrite{text: text, masked: maskCode(text), pieces: pieceText{text: text, bare: maskComments(text)}}
	r.walk(stmt, false, true)
	if len(r.unnamed) > 0 {
		calls := make([]string, len(r.unnamed))
		for i, u := range r.unnamed {
			calls[i] = text[u.start:u.end]
		}
		if names, ok := printedItems(deparse, calls); ok {
			for i, u := range r.unnamed {
				u.text += " AS " + quoteIdent(names[i])
				r.edits = append(r.edits, u)
			}
		}
	}
	out, err := applyEdits(text, r.edits)
	if err != nil {
		return text
	}
	return out
}

// approxRewrite collects the edits that rewrite a statement's calls.
type approxRewrite struct {
	text, masked string
	pieces       pieceText
	edits        []edit
	// unnamed are the edits of select items that are a call alone, without
	// an alias, which still need DuckDB's name for their column.
	unnamed []edit
}

// walk finds the calls in n. grouped says n is in a select with GROUP BY;
// named, that DuckDB names no column after n's text, as an alias names any
// select item n is in.
func (r *approxRewrite) walk(n any, grouped, named bool) {
	switch n := n.(type) {
	case []any:
		for _, c := range n {
			r.walk(c, grouped, named)
		}
	case map[string]any:
		if str(n["type"]) == "SELECT_NODE" {
			// A subquery is a select of its own.
			grouped = len(asList(n["group_expressions"])) > 0 || len(asList(n["group_sets"])) > 0 ||
				str(n["aggregate_handling"]) == "FORCE_AGGREGATES"
			r.selectList(n, grouped, named)
			for k, c := range n {
				if k != "select_list" {
					r.walk(c, grouped, named)
				}
			}
			return
		}
		if e, ok := r.call(n, grouped); ok {
			if named {
				r.edits = append(r.edits, e)
			}
			return
		}
		for _, c := range n {
			r.walk(c, grouped, named)
		}
	}
}

// selectList walks the items of the SELECT node n. An item without an
// alias that is a call of a column alone is rewritten and named after the
// call; the calls of any other such item stay as written.
func (r *approxRewrite) selectList(n map[string]any, grouped, named bool) {
	items := asList(n["select_list"])
	var spans [][2]int
	for i, item := range items {
		m, _ := item.(map[string]any)
		if str(m["alias"]) != "" {
			r.walk(item, grouped, named)
			continue
		}
		args := asList(m["children"])
		if named && str(m["class"]) == "FUNCTION" && m["filter"] == nil && len(args) == 1 {
			if arg, _ := args[0].(map[string]any); str(arg["class"]) == "COLUMN_REF" {
				if e, ok := r.call(m, grouped); ok {
					if spans == nil {
						if _, spans, ok = selectItemSpans(r.masked, n, items); !ok {
							spans = [][2]int{}
						}
					}
					// The item is the call, not in parentheses.
					if i < len(spans) {
						if lo, hi := r.pieces.bounds(spans[i][0], spans[i][1]); lo == e.start && hi == e.end {
							r.unnamed = append(r.unnamed, e)
							continue
						}
					}
				}
			}
		}
		r.walk(item, grouped, false)
	}
}

// call returns the edit that rewrites m, if it is a call that the rewrite
// counts (see the comment at the top).
func (r *approxRewrite) call(m map[string]any, grouped bool) (edit, bool) {
	window := str(m["class"]) == "WINDOW"
	if !window && str(m["class"]) != "FUNCTION" || window && str(m["type"]) != "WINDOW_AGGREGATE" ||
		!strings.EqualFold(str(m["function_name"]), approxName) || str(m["schema"]) != "" || str(m["catalog"]) != "" ||
		m["export_state"] == true || m["ignore_nulls"] == true || len(asList(orderBys(m))) > 0 || len(asList(m["arg_orders"])) > 0 {
		return edit{}, false
	}
	args := asList(m["children"])
	if len(args) != 1 {
		return edit{}, false
	}
	arg, _ := args[0].(map[string]any)
	if arg == nil || !repeatable(arg) || hasNode(arg, func(e map[string]any) bool { return str(e["class"]) == "STAR" }) {
		return edit{}, false
	}
	// The name, its parentheses, and the value inside them after DISTINCT,
	// which changes nothing in a distinct count.
	masked := r.masked
	start := location(m["query_location"])
	if start < 0 || start+len(approxName) > len(masked) || !strings.EqualFold(masked[start:start+len(approxName)], approxName) {
		return edit{}, false
	}
	open := skipSpace(masked, start+len(approxName))
	if open >= len(masked) || masked[open] != '(' {
		return edit{}, false
	}
	end := matchParen(r.text, open)
	if end < 0 {
		return edit{}, false
	}
	from := open + 1
	if m["distinct"] == true {
		from = skipSpace(masked, from)
		if !strings.EqualFold(nextWord(masked, from), "DISTINCT") {
			return edit{}, false
		}
		from += len("DISTINCT")
	}
	if !within(arg, from, end) {
		return edit{}, false
	}
	value := r.pieces.trim(from, end)
	end++
	// Grouped or as a window function, the call becomes count(DISTINCT x),
	// and its FILTER and OVER stay as they are.
	if grouped || window {
		return edit{start, end, "count(DISTINCT " + value + ")"}, true
	}
	op := value
	if str(arg["class"]) != "COLUMN_REF" {
		op = "(" + value + ")"
	}
	where := ""
	if f, _ := m["filter"].(map[string]any); f != nil {
		at := skipSpace(masked, end)
		if !strings.EqualFold(nextWord(masked, at), "FILTER") {
			return edit{}, false
		}
		paren := skipSpace(masked, at+len("FILTER"))
		if paren >= len(masked) || masked[paren] != '(' {
			return edit{}, false
		}
		shut := matchParen(r.text, paren)
		if shut < 0 {
			return edit{}, false
		}
		cond := skipSpace(masked, paren+1)
		if strings.EqualFold(nextWord(masked, cond), "WHERE") {
			cond += len("WHERE")
		}
		if !within(f, cond, shut) {
			return edit{}, false
		}
		where, end = "("+r.pieces.trim(cond, shut)+")", shut+1
	}
	return edit{start, end, snowflake.SketchCount(where, op)}, true
}

// printedItems returns select items as DuckDB prints them, by deparse
// (see approxCounts), or false.
func printedItems(deparse func(string) (string, error), items []string) ([]string, bool) {
	const head = "SELECT "
	out, err := deparse(head + strings.Join(items, ", "))
	if err != nil || !strings.HasPrefix(out, head) {
		return nil, false
	}
	spans := splitAtCommas(maskCode(out), len(head), len(out))
	if len(spans) != len(items) {
		return nil, false
	}
	names := make([]string, len(spans))
	for i, sp := range spans {
		names[i] = strings.TrimSpace(out[sp[0]:sp[1]])
	}
	return names, true
}
