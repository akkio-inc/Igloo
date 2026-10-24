package pgwire

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Recognizing audience reports (IGLOO_REPORT_REWRITE): the counting CTEs of
// a report, which join a table to labelled cohorts on the member column,
// expand each row into its dimensions and count distinct people per value,
// become calls of the report API (reportapi.go) that return the same
// columns with the same types, before anything else looks at the
// statement. reportCalls then replaces each call by the SQL it stands for,
// which reads the same tables and columns as the CTE did, so grants,
// policies and errors are those of the statement as written; where bitmap
// indexes or ids answer the call, it is faster.
//
// Recognition works on DuckDB's parse (json_serialize_sql) and is exact or
// nothing: a CTE it cannot map with certainty stays as written, and so
// does the rest of the statement, which DuckDB prints again from its parse
// (json_deserialize_sql). It reads the shapes reports take in DuckDB's
// dialect, and those the Snowflake dialect's translation gives them:
//
//   - a source table, or a CTE that selects its columns, joined to a
//     cohort (a CTE of members of a table under literal conditions, with an
//     optional latest version, or any other SELECT) or to a UNION ALL that
//     labels several, or filtered by IN (SELECT member FROM cohort);
//   - dimensions: a column grouped by; an UNNEST of a list of struct
//     literals (or the translation's CASE over range(n) of them), each
//     naming its dimension with constants and giving a value (a column, a
//     map of a boolean that keeps NULL, a constant or the elements of a
//     list, which the translation reads as JSON text where they look like
//     JSON, as the API's element_text "json" does) and optionally a metric;
//   - filters: literal conditions on the source, TRIM(value) compared
//     unequal to the empty string (with or without value IS NOT NULL),
//     comparisons of a dimension's metric;
//   - measures: count(DISTINCT member), count(*), sum and avg of a source
//     column (or of its coalesce with a literal), count(DISTINCT member,
//     column), sum, avg and count of the metric, and expressions of them;
//   - cohort sizes: count(DISTINCT member) per cohort of the cohorts alone.
//
// Measures that count rows (count(*), sums, metrics) are mapped only where
// each member appears once per cohort in the SQL; the API's cohorts are
// distinct.

// jnode is a node of DuckDB's parse.
type jnode = map[string]any

// errNotReport is a CTE that recognition leaves as written, and why.
type errNotReport struct{ why string }

func (e errNotReport) Error() string { return e.why }

func notReport(format string, args ...any) error { return errNotReport{fmt.Sprintf(format, args...)} }

// reportEnv is what recognition asks DuckDB.
type reportEnv interface {
	// parse returns the one statement of json_serialize_sql's result.
	parse(text string) (jnode, error)
	// deparse prints a statement again (json_deserialize_sql).
	deparse(stmt jnode) (string, error)
	// describe returns the names and types of the columns a query returns.
	describe(query string) ([][2]string, error)
}

// reportOutcome is what recognition did with one CTE.
type reportOutcome struct {
	CTE  string
	Spec string // the spec of its call, when recognized
	Why  string // why it stayed as written, when not
}

// recognizeReports returns text with the counting CTEs recognition maps
// replaced by calls of the report API, or text as it is.
func (s *Server) recognizeReports(ctx context.Context, sess *session, text string) string {
	if !s.reportRewrite.Load() || sess.reportRewriteOff || !mayBeReport(text) {
		return text
	}
	var accept func(spec string) error
	if !s.reportRewriteAnyPath {
		accept = func(spec string) error { return s.reportCallPays(ctx, sess, spec) }
	}
	out, outcomes, err := recognizeReportText(connReportEnv{ctx: ctx, sess: sess}, text, accept)
	if s.reportOutcomes != nil {
		s.reportOutcomes(outcomes)
	}
	if err != nil {
		s.log.Debug("report recognition left a statement as written", "err", err)
		return text
	}
	n := 0
	for _, o := range outcomes {
		if o.Spec != "" {
			n++
		} else {
			s.log.Debug("report recognition left a CTE as written", "cte", o.CTE, "why", o.Why)
		}
	}
	s.reportRecognized.Add(int64(n))
	s.metrics.reportsRecognized(n)
	return out
}

// reportCallPays reports, as an error, why a recognized CTE's call would not
// pay: one that would count the tables' values (path scan) reads what the
// CTE as written reads, and computes its cohorts again besides, where the
// CTEs as written share theirs; one that counts ids, or reads bitmaps,
// does far less (see reportPlannedPath).
func (s *Server) reportCallPays(ctx context.Context, sess *session, text string) error {
	spec, err := parseReportSpec(ctx, sess, text)
	if err != nil {
		return notReport("the call's spec: %v", err)
	}
	if path := s.reportPlannedPath(ctx, sess, spec); path == "scan" {
		return notReport("its call would count the tables' values (path scan), as the CTE does")
	}
	return nil
}

// mayBeReport is a cheap test for text that may have a counting CTE.
func mayBeReport(text string) bool {
	lower := strings.ToLower(text)
	return strings.Contains(lower, "with") && strings.Contains(lower, "count") && strings.Contains(lower, "distinct")
}

// recognizeReportText is recognition on text, with env asking DuckDB; a CTE
// whose call accept, if set, refuses stays as written.
func recognizeReportText(env reportEnv, text string, accept func(spec string) error) (string, []reportOutcome, error) {
	stmt, err := env.parse(text)
	if err != nil {
		return text, nil, err
	}
	node, _ := stmt["node"].(jnode)
	if node == nil {
		return text, nil, errors.New("no statement node")
	}
	t, err := loadReportTemplates(env)
	if err != nil {
		return text, nil, err
	}
	var outcomes []reportOutcome
	replaced := 0
	var walk func(n any)
	walk = func(n any) {
		switch v := n.(type) {
		case jnode:
			if cm, ok := v["cte_map"].(jnode); ok {
				r := &reportRecognizer{env: env, t: t, cteMap: cm, types: map[string]map[string]string{}, accept: accept}
				for _, o := range r.run() {
					if o.Spec != "" {
						replaced++
					}
					outcomes = append(outcomes, o)
				}
			}
			for k, c := range v {
				if k == "cte_map" {
					// CTE bodies may hold WITHs of their own.
					for _, e := range listOf(v["cte_map"].(jnode)["map"]) {
						if q := nodeOf(e, "value", "query", "node"); q != nil {
							walk(q)
						}
					}
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
	walk(node)
	if replaced == 0 {
		return text, outcomes, nil
	}
	out, err := env.deparse(stmt)
	if err != nil {
		return text, outcomes, err
	}
	// DuckDB prints the whole statement again; what recognition left as
	// written must parse as it did.
	again, err := env.parse(out)
	if err != nil {
		return text, outcomes, err
	}
	if outsideCalls(again) != outsideCalls(stmt) {
		return text, outcomes, errors.New("printed again, the statement parses differently")
	}
	return out, outcomes, nil
}

// outsideCalls is a statement's parse without positions and the bodies of
// the CTEs that select from the report API, as JSON, to compare.
func outsideCalls(stmt jnode) string {
	var strip func(v any) any
	strip = func(v any) any {
		switch x := v.(type) {
		case jnode:
			out := make(jnode, len(x))
			for k, c := range x {
				if k == "query_location" {
					continue
				}
				out[k] = strip(c)
			}
			if callsReportAPI(nodeOf(x, "value", "query", "node")) {
				out["value"] = nil
			}
			return out
		case []any:
			out := make([]any, len(x))
			for i, c := range x {
				out[i] = strip(c)
			}
			return out
		}
		return v
	}
	b, _ := json.Marshal(strip(stmt))
	return string(b)
}

// callsReportAPI reports whether a query node selects from
// igloo.audience_profile.
func callsReportAPI(q jnode) bool {
	f := nodeOf(q, "from_table", "function")
	return f != nil && strings.EqualFold(strOf(f, "function_name"), "audience_profile") && strings.EqualFold(strOf(f, "schema"), "igloo")
}

// connReportEnv asks the session's DuckDB connection.
type connReportEnv struct {
	ctx  context.Context
	sess *session
}

func (e connReportEnv) parse(text string) (jnode, error) {
	raw, err := e.scalar("SELECT system.main.json_serialize_sql($1::VARCHAR)::VARCHAR", text)
	if err != nil {
		return nil, err
	}
	return decodeReportParse(raw)
}

func (e connReportEnv) deparse(stmt jnode) (string, error) {
	doc, err := json.Marshal(jnode{"error": false, "statements": []any{stmt}})
	if err != nil {
		return "", err
	}
	return e.scalar("SELECT system.main.json_deserialize_sql($1::JSON)::VARCHAR", string(doc))
}

func (e connReportEnv) describe(query string) ([][2]string, error) {
	rows, err := e.sess.conn.QueryContext(e.ctx, "DESCRIBE "+query, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	cols := rows.Columns()
	dest := make([]driver.Value, len(cols))
	var out [][2]string
	for {
		if err := rows.Next(dest); err != nil {
			break
		}
		name, _ := dest[0].(string)
		typ, _ := dest[1].(string)
		out = append(out, [2]string{name, typ})
	}
	return out, nil
}

func (e connReportEnv) scalar(q, arg string) (string, error) {
	rows, err := e.sess.conn.QueryContext(e.ctx, q, []driver.NamedValue{{Ordinal: 1, Value: arg}})
	if err != nil {
		return "", err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return "", err
	}
	s, _ := dest[0].(string)
	return s, nil
}

// decodeReportParse reads json_serialize_sql's result, keeping numbers as
// written.
func decodeReportParse(raw string) (jnode, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var doc struct {
		Error        bool    `json:"error"`
		ErrorMessage string  `json:"error_message"`
		Statements   []jnode `json:"statements"`
	}
	if err := dec.Decode(&doc); err != nil {
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

// --- JSON helpers ---------------------------------------------------------------------

func strOf(n jnode, k string) string {
	s, _ := n[k].(string)
	return s
}

func listOf(v any) []any {
	l, _ := v.([]any)
	return l
}

func nodeOf(v any, path ...string) jnode {
	n, _ := v.(jnode)
	for _, p := range path {
		if n == nil {
			return nil
		}
		n, _ = n[p].(jnode)
	}
	return n
}

func childrenOf(n jnode) []jnode {
	var out []jnode
	for _, c := range listOf(n["children"]) {
		if cn, ok := c.(jnode); ok {
			out = append(out, cn)
		}
	}
	return out
}

// cloneNode deep-copies a node of the parse.
func cloneNode(v any) any {
	switch x := v.(type) {
	case jnode:
		out := make(jnode, len(x))
		for k, c := range x {
			out[k] = cloneNode(c)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, c := range x {
			out[i] = cloneNode(c)
		}
		return out
	default:
		return x
	}
}

func cloneJ(n jnode) jnode {
	c, _ := cloneNode(n).(jnode)
	return c
}

// exprKey identifies an expression up to its alias and positions.
func exprKey(n any) string {
	var strip func(v any) any
	strip = func(v any) any {
		switch x := v.(type) {
		case jnode:
			out := make(jnode, len(x))
			for k, c := range x {
				if k == "query_location" || k == "alias" {
					continue
				}
				out[k] = strip(c)
			}
			return out
		case []any:
			out := make([]any, len(x))
			for i, c := range x {
				out[i] = strip(c)
			}
			return out
		default:
			return x
		}
	}
	b, _ := json.Marshal(strip(n))
	return string(b)
}

func classOf(n jnode) string { return strOf(n, "class") }
func typeOf(n jnode) string  { return strOf(n, "type") }

func isFunc(n jnode, names ...string) bool {
	if classOf(n) != "FUNCTION" {
		return false
	}
	f := strings.ToLower(strOf(n, "function_name"))
	for _, name := range names {
		if f == name {
			return true
		}
	}
	return false
}

// columnParts returns a COLUMN_REF's names, upper-cased, or nil.
func columnParts(n jnode) []string {
	if classOf(n) != "COLUMN_REF" {
		return nil
	}
	var out []string
	for _, p := range listOf(n["column_names"]) {
		s, _ := p.(string)
		out = append(out, strings.ToUpper(s))
	}
	return out
}

// --- constants ------------------------------------------------------------------------

func isConstant(n jnode) bool { return classOf(n) == "CONSTANT" }

// constValue returns a constant's value, nil for NULL.
func constValue(n jnode) any {
	v := nodeOf(n, "value")
	if v == nil {
		return nil
	}
	if null, _ := v["is_null"].(bool); null {
		return nil
	}
	id := strings.ToUpper(strOf(nodeOf(v, "type"), "id"))
	switch id {
	case "NULL":
		return nil
	case "VARCHAR":
		s, _ := v["value"].(string)
		return s
	case "BOOLEAN":
		b, _ := v["value"].(bool)
		return b
	}
	switch x := v["value"].(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		if f, err := x.Float64(); err == nil {
			return f
		}
		return x.String()
	case nil:
		// json_serialize_sql leaves zero out with skip_null; not here.
		if strings.Contains(id, "INT") || id == "DOUBLE" || id == "FLOAT" || strings.HasPrefix(id, "DECIMAL") {
			return int64(0)
		}
		return nil
	default:
		return x
	}
}

func isNullConst(n jnode) bool { return isConstant(n) && constValue(n) == nil }

func boolConst(b bool) jnode {
	return jnode{"class": "CONSTANT", "type": "VALUE_CONSTANT", "alias": "", "query_location": json.Number("18446744073709551615"),
		"value": jnode{"type": jnode{"id": "BOOLEAN", "type_info": nil}, "is_null": false, "value": b}}
}

func nullConst() jnode {
	return jnode{"class": "CONSTANT", "type": "VALUE_CONSTANT", "alias": "", "query_location": json.Number("18446744073709551615"),
		"value": jnode{"type": jnode{"id": "NULL", "type_info": nil}, "is_null": true}}
}

func stringConst(s string) jnode {
	return jnode{"class": "CONSTANT", "type": "VALUE_CONSTANT", "alias": "", "query_location": json.Number("18446744073709551615"),
		"value": jnode{"type": jnode{"id": "VARCHAR", "type_info": nil}, "is_null": false, "value": s}}
}

func constBool(n jnode) (value, ok bool) {
	if !isConstant(n) {
		return false, false
	}
	b, isBool := constValue(n).(bool)
	return b, isBool
}

func compareConst(op string, a, b any) (bool, bool) {
	cmp := 0
	switch x := a.(type) {
	case string:
		y, ok := b.(string)
		if !ok {
			return false, false
		}
		cmp = strings.Compare(x, y)
	case bool:
		y, ok := b.(bool)
		if !ok || op != "COMPARE_EQUAL" && op != "COMPARE_NOTEQUAL" {
			return false, false
		}
		if x != y {
			cmp = 1
		}
	default:
		fx, ok1 := toFloat(a)
		fy, ok2 := toFloat(b)
		if !ok1 || !ok2 {
			return false, false
		}
		switch {
		case fx < fy:
			cmp = -1
		case fx > fy:
			cmp = 1
		}
	}
	switch op {
	case "COMPARE_EQUAL":
		return cmp == 0, true
	case "COMPARE_NOTEQUAL":
		return cmp != 0, true
	case "COMPARE_LESSTHAN":
		return cmp < 0, true
	case "COMPARE_GREATERTHAN":
		return cmp > 0, true
	case "COMPARE_LESSTHANOREQUALTO":
		return cmp <= 0, true
	case "COMPARE_GREATERTHANOREQUALTO":
		return cmp >= 0, true
	}
	return false, false
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

// foldReport folds what is constant in an expression once a dimension's
// fields are in it. Beyond SQL's own, the Snowflake translation's helpers
// fold where their result is known: sf_text_read(x) is x, and
// sf_fields_string(l, s, j) is s when s is a constant that is not NULL.
func foldReport(n jnode) jnode {
	n = cloneJ(n)
	return foldNode(n)
}

func foldNode(n jnode) jnode {
	for _, k := range []string{"children", "case_checks"} {
		if l := listOf(n[k]); l != nil {
			for i, c := range l {
				cn, ok := c.(jnode)
				if !ok {
					continue
				}
				if k == "case_checks" {
					if w := nodeOf(cn, "when_expr"); w != nil {
						cn["when_expr"] = foldNode(w)
					}
					if t := nodeOf(cn, "then_expr"); t != nil {
						cn["then_expr"] = foldNode(t)
					}
					continue
				}
				l[i] = foldNode(cn)
			}
		}
	}
	for _, k := range []string{"left", "right", "child", "else_expr", "input", "lower", "upper"} {
		if c := nodeOf(n, k); c != nil {
			n[k] = foldNode(c)
		}
	}
	return foldOne(n)
}

func foldOne(n jnode) jnode {
	switch classOf(n) {
	case "COMPARISON":
		l, r := nodeOf(n, "left"), nodeOf(n, "right")
		if l != nil && r != nil && isConstant(l) && isConstant(r) {
			a, b := constValue(l), constValue(r)
			if a == nil || b == nil {
				return nullConst()
			}
			if v, ok := compareConst(typeOf(n), a, b); ok {
				return boolConst(v)
			}
		}
	case "CONJUNCTION":
		and := typeOf(n) == "CONJUNCTION_AND"
		var kept []any
		for _, c := range childrenOf(n) {
			if b, ok := constBool(c); ok {
				if and && !b {
					return boolConst(false)
				}
				if !and && b {
					return boolConst(true)
				}
				continue
			}
			kept = append(kept, c)
		}
		switch len(kept) {
		case 0:
			return boolConst(and)
		case 1:
			return kept[0].(jnode)
		}
		n["children"] = kept
	case "OPERATOR":
		ch := childrenOf(n)
		switch typeOf(n) {
		case "OPERATOR_COALESCE":
			return foldCoalesce(n, ch)
		case "OPERATOR_NOT":
			if len(ch) == 1 {
				if b, ok := constBool(ch[0]); ok {
					return boolConst(!b)
				}
				if isNullConst(ch[0]) {
					return nullConst()
				}
			}
		case "OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL":
			if len(ch) == 1 && isConstant(ch[0]) {
				null := constValue(ch[0]) == nil
				return boolConst(null == (typeOf(n) == "OPERATOR_IS_NULL"))
			}
		}
	case "CASE":
		var kept []any
		for _, c := range listOf(n["case_checks"]) {
			cn := c.(jnode)
			w := nodeOf(cn, "when_expr")
			if b, ok := constBool(w); ok || isNullConst(w) {
				if ok && b {
					if len(kept) == 0 {
						return nodeOf(cn, "then_expr")
					}
					kept = append(kept, cn)
					n["case_checks"] = kept
					n["else_expr"] = nullConst()
					return n
				}
				continue
			}
			kept = append(kept, cn)
		}
		if len(kept) == 0 {
			if e := nodeOf(n, "else_expr"); e != nil {
				return e
			}
			return nullConst()
		}
		n["case_checks"] = kept
	case "CAST":
		c := nodeOf(n, "child")
		if c != nil && isNullConst(c) {
			return nullConst()
		}
		if c != nil && isConstant(c) && textType(nodeOf(n, "cast_type")) {
			if s, ok := constValue(c).(string); ok {
				return stringConst(s)
			}
		}
	case "FUNCTION":
		ch := childrenOf(n)
		switch strings.ToLower(strOf(n, "function_name")) {
		case "sf_text_read":
			if len(ch) == 1 {
				return ch[0]
			}
		case "sf_fields_string":
			if len(ch) == 3 {
				if isConstant(ch[1]) && constValue(ch[1]) != nil {
					return ch[1]
				}
				if isNullConst(ch[0]) && isNullConst(ch[1]) && isNullConst(ch[2]) {
					return nullConst()
				}
			}
		case "upper", "lower", "trim":
			if len(ch) == 1 && isConstant(ch[0]) {
				s, ok := constValue(ch[0]).(string)
				if !ok {
					if constValue(ch[0]) == nil {
						return nullConst()
					}
					break
				}
				switch strings.ToLower(strOf(n, "function_name")) {
				case "upper":
					s = strings.ToUpper(s)
				case "lower":
					s = strings.ToLower(s)
				default:
					s = strings.Trim(s, " ")
				}
				return stringConst(s)
			}
		case "coalesce":
			return foldCoalesce(n, ch)
		}
	}
	return n
}

// isCoalesce reports whether n is COALESCE, which DuckDB parses as an
// operator, or coalesce as a function.
func isCoalesce(n jnode) bool {
	return classOf(n) == "OPERATOR" && typeOf(n) == "OPERATOR_COALESCE" || isFunc(n, "coalesce")
}

func foldCoalesce(n jnode, ch []jnode) jnode {
	var kept []any
	for _, c := range ch {
		if isNullConst(c) {
			continue
		}
		kept = append(kept, c)
		if isConstant(c) {
			break
		}
	}
	switch len(kept) {
	case 0:
		return nullConst()
	case 1:
		return kept[0].(jnode)
	}
	n["children"] = kept
	return n
}

func textType(t jnode) bool {
	if t == nil {
		return false
	}
	return strings.ToUpper(strOf(t, "id")) == "VARCHAR"
}

func doubleType(t jnode) bool {
	return t != nil && strings.ToUpper(strOf(t, "id")) == "DOUBLE"
}

// stripText strips casts to VARCHAR and sf_text_read, which change no text.
func stripText(n jnode) jnode {
	for {
		switch {
		case classOf(n) == "CAST" && textType(nodeOf(n, "cast_type")) && !boolTrue(n["try_cast"]):
			n = nodeOf(n, "child")
		case isFunc(n, "sf_text_read") && len(childrenOf(n)) == 1:
			n = childrenOf(n)[0]
		default:
			return n
		}
	}
}

func boolTrue(v any) bool { b, _ := v.(bool); return b }

func conjuncts(n jnode) []jnode {
	if n == nil {
		return nil
	}
	if classOf(n) == "CONJUNCTION" && typeOf(n) == "CONJUNCTION_AND" {
		var out []jnode
		for _, c := range childrenOf(n) {
			out = append(out, conjuncts(c)...)
		}
		return out
	}
	return []jnode{n}
}

// --- templates ------------------------------------------------------------------------

// reportTemplates are nodes of the shapes recognition writes, from DuckDB's
// own parse.
type reportTemplates struct {
	call   jnode // SELECT ... FROM igloo.audience_profile('') AS igloo_api WHERE NOT igloo_api.is_total
	column jnode // igloo_api.people
	caseOn jnode // CASE igloo_api.dimension WHEN '' THEN NULL END
	star   jnode // SELECT * FROM x

	mu    sync.Mutex
	casts map[string]jnode // DESCRIBE's type names -> cast_type
}

// reportTemplatesLoaded keeps the templates, which are the same for every
// statement, once parsed.
var reportTemplatesLoaded atomic.Pointer[reportTemplates]

func loadReportTemplates(env reportEnv) (*reportTemplates, error) {
	if t := reportTemplatesLoaded.Load(); t != nil {
		return t, nil
	}
	t, err := newReportTemplates(env)
	if err != nil {
		return nil, err
	}
	reportTemplatesLoaded.CompareAndSwap(nil, t)
	return reportTemplatesLoaded.Load(), nil
}

func newReportTemplates(env reportEnv) (*reportTemplates, error) {
	stmt, err := env.parse("SELECT igloo_api.people, CASE igloo_api.dimension WHEN 'x' THEN NULL END FROM igloo.audience_profile('{}') AS igloo_api WHERE NOT igloo_api.is_total")
	if err != nil {
		return nil, err
	}
	call := nodeOf(stmt, "node")
	sel := listOf(call["select_list"])
	if len(sel) != 2 {
		return nil, errors.New("unexpected template parse")
	}
	star, err := env.parse("SELECT * FROM x")
	if err != nil {
		return nil, err
	}
	return &reportTemplates{call: call, column: sel[0].(jnode), caseOn: sel[1].(jnode), star: star, casts: map[string]jnode{}}, nil
}

func (t *reportTemplates) apiColumn(name string) jnode {
	c := cloneJ(t.column)
	c["column_names"] = []any{reportAlias, name}
	c["alias"] = ""
	return c
}

// castTo returns a cast of n to a DuckDB type as DESCRIBE writes it.
func (t *reportTemplates) castTo(env reportEnv, n jnode, duckdbType string) (jnode, error) {
	t.mu.Lock()
	ct, ok := t.casts[duckdbType]
	t.mu.Unlock()
	if !ok {
		stmt, err := env.parse("SELECT CAST(NULL AS " + duckdbType + ")")
		if err != nil {
			return nil, err
		}
		sel := listOf(nodeOf(stmt, "node")["select_list"])
		if len(sel) != 1 || classOf(sel[0].(jnode)) != "CAST" {
			return nil, fmt.Errorf("type %s", duckdbType)
		}
		ct = nodeOf(sel[0], "cast_type")
		t.mu.Lock()
		t.casts[duckdbType] = ct
		t.mu.Unlock()
	}
	c := jnode{"class": "CAST", "type": "OPERATOR_CAST", "alias": "", "query_location": json.Number("18446744073709551615"),
		"child": n, "cast_type": cloneNode(ct), "try_cast": false}
	return c, nil
}

const reportAlias = "igloo_api"

// --- recognition ----------------------------------------------------------------------

// reportRecognizer reads the CTEs of one WITH.
type reportRecognizer struct {
	env    reportEnv
	t      *reportTemplates
	cteMap jnode
	ctes   map[string]jnode // upper-cased name -> query node
	order  []string
	cohort map[string]*recCohort
	types  map[string]map[string]string // table -> column -> type
	// original is the WITH as written, for the types of its CTEs, and
	// described those types, by CTE.
	original  jnode
	described map[string][][2]string
	// accept, if set, refuses calls that would not pay.
	accept func(spec string) error
}

func (r *reportRecognizer) run() []reportOutcome {
	r.ctes = map[string]jnode{}
	r.cohort = map[string]*recCohort{}
	entries := listOf(r.cteMap["map"])
	for _, e := range entries {
		name := strings.ToUpper(strOf(e.(jnode), "key"))
		if _, dup := r.ctes[name]; !dup {
			r.ctes[name] = nodeOf(e, "value", "query", "node")
			r.order = append(r.order, name)
		}
	}
	var out []reportOutcome
	for _, e := range entries {
		en := e.(jnode)
		name := strOf(en, "key")
		q := nodeOf(en, "value", "query", "node")
		if q == nil || typeOf(q) != "SELECT_NODE" || !hasAggregate(listOf(q["select_list"])) {
			continue
		}
		repl, spec, err := r.replacement(name, q)
		if err == nil && r.accept != nil {
			err = r.accept(spec)
		}
		if err != nil {
			out = append(out, reportOutcome{CTE: name, Why: err.Error()})
			continue
		}
		nodeOf(en, "value", "query")["node"] = repl
		out = append(out, reportOutcome{CTE: name, Spec: spec})
	}
	return out
}

// reportAggregates are DuckDB's aggregate functions. A function missing
// here is taken for a scalar one, which recognition refuses wherever it
// would matter: such a call is neither a column nor a constant.
var reportAggregates = map[string]bool{
	"count": true, "count_star": true, "sum": true, "avg": true, "mean": true, "min": true, "max": true,
	"any_value": true, "arbitrary": true, "first": true, "last": true, "list": true, "array_agg": true,
	"string_agg": true, "group_concat": true, "listagg": true, "median": true, "mode": true, "quantile": true,
	"quantile_cont": true, "quantile_disc": true, "approx_count_distinct": true, "approx_quantile": true,
	"reservoir_quantile": true, "stddev": true, "stddev_pop": true, "stddev_samp": true, "variance": true,
	"var_pop": true, "var_samp": true, "bit_and": true, "bit_or": true, "bit_xor": true, "bool_and": true,
	"bool_or": true, "every": true, "arg_max": true, "arg_min": true, "argmax": true, "argmin": true,
	"max_by": true, "min_by": true, "arg_max_null": true, "arg_min_null": true, "product": true, "fsum": true,
	"sumkahan": true, "kahan_sum": true, "favg": true, "histogram": true, "histogram_exact": true,
	"entropy": true, "kurtosis": true, "kurtosis_pop": true, "skewness": true, "corr": true, "covar_pop": true,
	"covar_samp": true, "count_if": true, "countif": true, "sum_no_overflow": true, "bitstring_agg": true,
	"mad": true, "regr_avgx": true, "regr_avgy": true, "regr_count": true, "regr_intercept": true,
	"regr_r2": true, "regr_slope": true, "regr_sxx": true, "regr_sxy": true, "regr_syy": true,
	"approx_top_k": true, "weighted_avg": true, "wavg": true, "geomean": true, "geometric_mean": true,
	"rb_agg": true, "rb_or_agg": true, "rb_and_agg": true, "rb_count_distinct": true,
}

func reportAggregate(n jnode) bool {
	return classOf(n) == "FUNCTION" && reportAggregates[strings.ToLower(strOf(n, "function_name"))]
}

// hasAggregate reports whether expressions aggregate, outside subqueries.
func hasAggregate(exprs []any) bool {
	found := false
	for _, e := range exprs {
		walkOwnExpr(e, func(n jnode) {
			if reportAggregate(n) {
				found = true
			}
		})
	}
	return found
}

// walkOwnExpr visits an expression's nodes, not entering subqueries.
func walkOwnExpr(v any, fn func(jnode)) {
	switch x := v.(type) {
	case jnode:
		fn(x)
		if classOf(x) == "SUBQUERY" {
			if c := nodeOf(x, "child"); c != nil {
				walkOwnExpr(c, fn)
			}
			return
		}
		for k, c := range x {
			if k == "subquery" {
				continue
			}
			walkOwnExpr(c, fn)
		}
	case []any:
		for _, c := range x {
			walkOwnExpr(c, fn)
		}
	}
}

func hasWindow(exprs ...any) bool {
	found := false
	for _, e := range exprs {
		walkOwnExpr(e, func(n jnode) {
			if classOf(n) == "WINDOW" {
				found = true
			}
		})
	}
	return found
}

func (r *reportRecognizer) cteBody(table jnode) jnode {
	if typeOf(table) != "BASE_TABLE" || strOf(table, "schema_name") != "" || strOf(table, "catalog_name") != "" {
		return nil
	}
	return r.ctes[strings.ToUpper(strOf(table, "table_name"))]
}

func recTableName(table jnode) string {
	var parts []string
	for _, k := range []string{"catalog_name", "schema_name", "table_name"} {
		if s := strOf(table, k); s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, ".")
}

func tableAlias(table jnode) string {
	if a := strOf(table, "alias"); a != "" {
		return strings.ToUpper(a)
	}
	return strings.ToUpper(strOf(table, "table_name"))
}

// simpleSelect checks a SELECT with no grouping, aggregates, windows,
// joins, modifiers or CTEs of its own.
func simpleSelect(q jnode) error {
	if q == nil || typeOf(q) != "SELECT_NODE" {
		return notReport("not a plain SELECT")
	}
	for _, k := range []string{"group_expressions", "having", "qualify", "sample"} {
		if v, ok := q[k]; ok && v != nil && !emptyValue(v) {
			return notReport("a SELECT with %s", k)
		}
	}
	if len(listOf(q["modifiers"])) > 0 {
		for _, m := range listOf(q["modifiers"]) {
			if typeOf(m.(jnode)) != "DISTINCT_MODIFIER" {
				return notReport("a SELECT with %s", typeOf(m.(jnode)))
			}
		}
	}
	if cm := nodeOf(q, "cte_map"); cm != nil && len(listOf(cm["map"])) > 0 {
		return notReport("a SELECT with CTEs")
	}
	if hasAggregate(listOf(q["select_list"])) || hasWindow(q["select_list"]) {
		return notReport("a SELECT with aggregates")
	}
	return nil
}

func emptyValue(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case []any:
		return len(x) == 0
	case jnode:
		return len(x) == 0
	case string:
		return x == ""
	}
	return false
}

func isDistinct(q jnode) bool {
	for _, m := range listOf(q["modifiers"]) {
		if typeOf(m.(jnode)) == "DISTINCT_MODIFIER" && len(listOf(m.(jnode)["distinct_on_targets"])) == 0 {
			return true
		}
	}
	return false
}

// --- cohorts --------------------------------------------------------------------------

type recCohort struct {
	label    string
	spec     map[string]any
	distinct bool
}

type recCohorts struct {
	cohorts     []*recCohort
	member      string // the cohorts' member column, upper-cased
	labelColumn string // upper-cased; "" for one cohort
}

func (c *recCohorts) distinct() bool {
	for _, x := range c.cohorts {
		if !x.distinct {
			return false
		}
	}
	return true
}

// projected returns the table column a SELECT projects as name, "" if none.
func projected(q jnode, name string) string {
	for _, p := range listOf(q["select_list"]) {
		pn := p.(jnode)
		if classOf(pn) == "STAR" && strOf(pn, "relation_name") == "" {
			return name
		}
		out := strings.ToUpper(strOf(pn, "alias"))
		parts := columnParts(pn)
		if out == "" && len(parts) > 0 {
			out = parts[len(parts)-1]
		}
		if out != name {
			continue
		}
		if len(parts) > 0 {
			return parts[len(parts)-1]
		}
		return ""
	}
	return ""
}

func (r *reportRecognizer) cohortOf(table jnode, member string) (*recCohort, error) {
	label := strings.ToUpper(strOf(table, "table_name"))
	body := r.cteBody(table)
	if body == nil {
		return &recCohort{label: label, spec: map[string]any{"table": recTableName(table), "member": member}}, nil
	}
	key := label + "." + member
	if c, ok := r.cohort[key]; ok {
		return c, nil
	}
	c, err := r.cohortOfBody(label, body, member)
	if err != nil {
		return nil, err
	}
	r.cohort[key] = c
	return c, nil
}

func (r *reportRecognizer) cohortOfBody(label string, body jnode, member string) (*recCohort, error) {
	c, err := r.tableCohort(label, body, member)
	if err == nil {
		return c, nil
	}
	return r.sqlCohort(label, body, member)
}

func (r *reportRecognizer) tableCohort(label string, body jnode, member string) (*recCohort, error) {
	if err := simpleSelect(body); err != nil {
		return nil, err
	}
	from := nodeOf(body, "from_table")
	if from == nil || typeOf(from) != "BASE_TABLE" {
		return nil, notReport("a cohort not read from a table")
	}
	column := projected(body, member)
	if column == "" {
		return nil, notReport("a cohort without its %s", member)
	}
	conds := conjuncts(nodeOf(body, "where_clause"))
	if inner := r.cteBody(from); inner != nil {
		for _, c := range conds {
			if !isMemberNotNull(c, column) {
				return nil, notReport("a cohort filtering another")
			}
		}
		base, err := r.cohortOf(from, column)
		if err != nil {
			return nil, err
		}
		return &recCohort{label: label, spec: base.spec, distinct: base.distinct || isDistinct(body)}, nil
	}
	spec := map[string]any{"table": recTableName(from), "member": column}
	var filters []reportFilter
	for _, c := range conds {
		if v, ok, err := r.version(c); err != nil {
			return nil, err
		} else if ok {
			if _, dup := spec["version"]; dup {
				return nil, notReport("two versions")
			}
			spec["version"] = v
			continue
		}
		if isMemberNotNull(c, column) {
			continue
		}
		f, err := literalFilter(c, func(parts []string) (string, error) { return parts[len(parts)-1], nil })
		if err != nil {
			return nil, err
		}
		filters = append(filters, f)
	}
	if len(filters) > 0 {
		w, err := whereSpec(filters)
		if err != nil {
			return nil, err
		}
		spec["where"] = w
	}
	return &recCohort{label: label, spec: spec, distinct: isDistinct(body)}, nil
}

func (r *reportRecognizer) sqlCohort(label string, body jnode, member string) (*recCohort, error) {
	refsCTE := false
	walkAll(body, func(n jnode) {
		if typeOf(n) == "BASE_TABLE" && r.cteBody(n) != nil {
			refsCTE = true
		}
		if classOf(n) == "PARAMETER" {
			refsCTE = true
		}
	})
	if refsCTE {
		return nil, notReport("a cohort reading other CTEs")
	}
	text, err := r.deparseQuery(body)
	if err != nil {
		return nil, err
	}
	sql := "SELECT " + quoteIdent(strings.ToLower(member)) + " FROM (" + text + ") AS igloo_cohort"
	return &recCohort{label: label, spec: map[string]any{"sql": sql}}, nil
}

// deparseQuery prints a query node.
func (r *reportRecognizer) deparseQuery(q jnode) (string, error) {
	stmt := cloneJ(r.t.star)
	stmt["node"] = cloneJ(q)
	s, err := r.env.deparse(stmt)
	return strings.TrimSuffix(strings.TrimSpace(s), ";"), err
}

func walkAll(v any, fn func(jnode)) {
	switch x := v.(type) {
	case jnode:
		fn(x)
		for _, c := range x {
			walkAll(c, fn)
		}
	case []any:
		for _, c := range x {
			walkAll(c, fn)
		}
	}
}

func isMemberNotNull(c jnode, member string) bool {
	var inner jnode
	switch {
	case classOf(c) == "OPERATOR" && typeOf(c) == "OPERATOR_IS_NOT_NULL":
		inner = c
	case classOf(c) == "OPERATOR" && typeOf(c) == "OPERATOR_NOT":
		ch := childrenOf(c)
		if len(ch) == 1 && typeOf(ch[0]) == "OPERATOR_IS_NULL" {
			inner = ch[0]
		}
	}
	if inner == nil {
		return false
	}
	if classOf(c) == "OPERATOR" && typeOf(c) == "OPERATOR_NOT" {
		ch := childrenOf(inner)
		return len(ch) == 1 && lastColumn(ch[0]) == member
	}
	ch := childrenOf(inner)
	return len(ch) == 1 && lastColumn(ch[0]) == member
}

func lastColumn(n jnode) string {
	p := columnParts(n)
	if len(p) == 0 {
		return ""
	}
	return p[len(p)-1]
}

// version reads `ver = (SELECT max(ver) FROM t WHERE ...)`.
func (r *reportRecognizer) version(c jnode) (map[string]any, bool, error) {
	if classOf(c) != "COMPARISON" || typeOf(c) != "COMPARE_EQUAL" {
		return nil, false, nil
	}
	l, rt := nodeOf(c, "left"), nodeOf(c, "right")
	if classOf(l) == "SUBQUERY" {
		l, rt = rt, l
	}
	if classOf(l) != "COLUMN_REF" || classOf(rt) != "SUBQUERY" || strOf(rt, "subquery_type") != "SCALAR" {
		return nil, false, nil
	}
	q := nodeOf(rt, "subquery", "node")
	if q == nil || typeOf(q) != "SELECT_NODE" || len(listOf(q["select_list"])) != 1 {
		return nil, false, nil
	}
	m := listOf(q["select_list"])[0].(jnode)
	if !isFunc(m, "max") || len(childrenOf(m)) != 1 || classOf(childrenOf(m)[0]) != "COLUMN_REF" {
		return nil, false, nil
	}
	column := lastColumn(l)
	if lastColumn(childrenOf(m)[0]) != column {
		return nil, false, notReport("a version from another column")
	}
	from := nodeOf(q, "from_table")
	if from == nil || typeOf(from) != "BASE_TABLE" || r.cteBody(from) != nil {
		return nil, false, notReport("a version not read from a table")
	}
	for _, k := range []string{"group_expressions", "having"} {
		if !emptyValue(q[k]) {
			return nil, false, notReport("a version subquery with %s", k)
		}
	}
	latest := map[string]any{"table": recTableName(from)}
	var filters []reportFilter
	for _, cc := range conjuncts(nodeOf(q, "where_clause")) {
		f, err := literalFilter(cc, func(parts []string) (string, error) { return parts[len(parts)-1], nil })
		if err != nil {
			return nil, false, err
		}
		filters = append(filters, f)
	}
	if len(filters) > 0 {
		w, err := whereSpec(filters)
		if err != nil {
			return nil, false, err
		}
		latest["where"] = w
	}
	return map[string]any{"column": column, "latest_from": latest}, true, nil
}

// cohortsOf reads a labelled UNION ALL of cohorts, or one cohort.
func (r *reportRecognizer) cohortsOf(table jnode, member string) (*recCohorts, error) {
	body := r.cteBody(table)
	if body != nil && typeOf(body) == "SET_OPERATION_NODE" {
		return r.labelled(body, member)
	}
	c, err := r.cohortOf(table, member)
	if err != nil {
		return nil, err
	}
	return &recCohorts{cohorts: []*recCohort{c}, member: member}, nil
}

func (r *reportRecognizer) labelled(body jnode, member string) (*recCohorts, error) {
	var branches []jnode
	var collect func(n jnode) error
	collect = func(n jnode) error {
		if typeOf(n) == "SET_OPERATION_NODE" {
			if strOf(n, "setop_type") != "UNION" || !boolTrue(n["setop_all"]) {
				return notReport("a set operation of cohorts other than UNION ALL")
			}
			if len(listOf(n["modifiers"])) > 0 {
				return notReport("a UNION of cohorts with modifiers")
			}
			if err := collect(nodeOf(n, "left")); err != nil {
				return err
			}
			return collect(nodeOf(n, "right"))
		}
		branches = append(branches, n)
		return nil
	}
	if err := collect(body); err != nil {
		return nil, err
	}
	// The UNION's columns are named by its first branch: one the member
	// the join reads, the other the label, a string in each branch.
	out := &recCohorts{member: member}
	labelAt, memberAt := -1, -1
	seen := map[string]bool{}
	for bi, b := range branches {
		if err := simpleSelect(b); err != nil {
			return nil, err
		}
		if !emptyValue(b["where_clause"]) || isDistinct(b) {
			return nil, notReport("a filtered cohort branch")
		}
		sel := listOf(b["select_list"])
		if len(sel) != 2 {
			return nil, notReport("a cohort branch with other columns than its label and member")
		}
		if bi == 0 {
			for i, p := range sel {
				pn := p.(jnode)
				name := strings.ToUpper(strOf(pn, "alias"))
				if name == "" && classOf(pn) == "COLUMN_REF" {
					name = lastColumn(pn)
				}
				switch {
				case isConstant(pn) && name != "":
					labelAt, out.labelColumn = i, name
				case classOf(pn) == "COLUMN_REF" && name == member:
					memberAt = i
				}
			}
			if labelAt < 0 || memberAt < 0 {
				return nil, notReport("a UNION of cohorts without a label and member")
			}
		}
		label, ok := constValue(sel[labelAt].(jnode)).(string)
		if !ok || !isConstant(sel[labelAt].(jnode)) {
			return nil, notReport("a cohort branch label that is not a string")
		}
		column := lastColumn(sel[memberAt].(jnode))
		if column == "" {
			return nil, notReport("a cohort branch member that is not a column")
		}
		if seen[label] {
			return nil, notReport("a label used twice")
		}
		seen[label] = true
		var c *recCohort
		var err error
		switch from := nodeOf(b, "from_table"); {
		case from != nil && typeOf(from) == "BASE_TABLE":
			c, err = r.cohortOf(from, column)
		case from != nil && typeOf(from) == "SUBQUERY":
			c, err = r.cohortOfBody(label, nodeOf(from, "subquery", "node"), column)
		default:
			return nil, notReport("a cohort branch not read from a table")
		}
		if err != nil {
			return nil, err
		}
		out.cohorts = append(out.cohorts, &recCohort{label: label, spec: c.spec, distinct: c.distinct})
	}
	return out, nil
}

// --- literal filters ------------------------------------------------------------------

type reportFilter struct {
	column string
	op     string
	value  any
}

var reportComparisons = map[string]string{
	"COMPARE_EQUAL": "eq", "COMPARE_NOTEQUAL": "ne", "COMPARE_LESSTHAN": "lt", "COMPARE_GREATERTHAN": "gt",
	"COMPARE_LESSTHANOREQUALTO": "le", "COMPARE_GREATERTHANOREQUALTO": "ge",
}

var reportFlipped = map[string]string{"eq": "eq", "ne": "ne", "lt": "gt", "gt": "lt", "le": "ge", "ge": "le"}

// literalValue returns a literal the spec can carry: constants, and
// strings cast to a type, as DATE '...' is, which the column compares with
// as with the typed literal.
func recLiteral(n jnode) (any, error) {
	if isConstant(n) {
		v := constValue(n)
		if v == nil {
			return nil, notReport("a NULL literal")
		}
		if id := strings.ToUpper(strOf(nodeOf(n, "value", "type"), "id")); id != "VARCHAR" && id != "BOOLEAN" &&
			!strings.Contains(id, "INT") && id != "DOUBLE" {
			// A typed constant (DATE '...', a DECIMAL) as its text.
			if s, ok := nodeOf(n, "value")["value"].(string); ok {
				return s, nil
			}
			if num, ok := nodeOf(n, "value")["value"].(json.Number); ok {
				return num, nil
			}
			return nil, notReport("a literal of type %s", id)
		}
		return v, nil
	}
	if classOf(n) == "CAST" {
		c := nodeOf(n, "child")
		if c != nil && isConstant(c) {
			if s, ok := constValue(c).(string); ok {
				return s, nil
			}
		}
	}
	return nil, notReport("not a literal")
}

func literalFilter(c jnode, column func([]string) (string, error)) (reportFilter, error) {
	switch classOf(c) {
	case "COMPARISON":
		op, ok := reportComparisons[typeOf(c)]
		if !ok {
			break
		}
		l, r := nodeOf(c, "left"), nodeOf(c, "right")
		if p := columnParts(l); p != nil {
			v, err := recLiteral(r)
			if err != nil {
				return reportFilter{}, err
			}
			col, err := column(p)
			return reportFilter{col, op, v}, err
		}
		if p := columnParts(r); p != nil {
			v, err := recLiteral(l)
			if err != nil {
				return reportFilter{}, err
			}
			col, err := column(p)
			return reportFilter{col, reportFlipped[op], v}, err
		}
	case "OPERATOR":
		ch := childrenOf(c)
		switch typeOf(c) {
		case "COMPARE_IN":
			if len(ch) >= 2 {
				if p := columnParts(ch[0]); p != nil {
					var values []any
					for _, x := range ch[1:] {
						v, err := recLiteral(x)
						if err != nil {
							return reportFilter{}, err
						}
						values = append(values, v)
					}
					col, err := column(p)
					return reportFilter{col, "in", values}, err
				}
			}
		case "OPERATOR_IS_NULL":
			if len(ch) == 1 {
				if p := columnParts(ch[0]); p != nil {
					col, err := column(p)
					return reportFilter{col, "is_null", true}, err
				}
			}
		case "OPERATOR_IS_NOT_NULL":
			if len(ch) == 1 {
				if p := columnParts(ch[0]); p != nil {
					col, err := column(p)
					return reportFilter{col, "is_null", false}, err
				}
			}
		case "OPERATOR_NOT":
			if len(ch) == 1 && typeOf(ch[0]) == "OPERATOR_IS_NULL" {
				if cc := childrenOf(ch[0]); len(cc) == 1 {
					if p := columnParts(cc[0]); p != nil {
						col, err := column(p)
						return reportFilter{col, "is_null", false}, err
					}
				}
			}
		}
	case "FUNCTION":
		if isFunc(c, "~~") {
			ch := childrenOf(c)
			if len(ch) == 2 {
				if p := columnParts(ch[0]); p != nil {
					v, err := recLiteral(ch[1])
					if s, ok := v.(string); err == nil && ok {
						col, err := column(p)
						return reportFilter{col, "like", s}, err
					}
				}
			}
		}
	case "BETWEEN":
		if p := columnParts(nodeOf(c, "input")); p != nil {
			lo, err := recLiteral(nodeOf(c, "lower"))
			if err != nil {
				return reportFilter{}, err
			}
			hi, err := recLiteral(nodeOf(c, "upper"))
			if err != nil {
				return reportFilter{}, err
			}
			col, err := column(p)
			return reportFilter{col, "between", []any{lo, hi}}, err
		}
	}
	return reportFilter{}, notReport("a filter that is not a literal condition on a column")
}

func whereSpec(filters []reportFilter) (map[string]any, error) {
	where := map[string]any{}
	for _, f := range filters {
		cur, has := where[f.column]
		if !has && f.op == "eq" {
			where[f.column] = f.value
			continue
		}
		if !has && f.op == "in" {
			where[f.column] = f.value
			continue
		}
		var ops map[string]any
		switch x := cur.(type) {
		case nil:
			ops = map[string]any{}
		case map[string]any:
			ops = x
		case []any:
			return nil, notReport("two filters on %s", f.column)
		default:
			ops = map[string]any{"eq": x}
		}
		if _, dup := ops[f.op]; dup {
			return nil, notReport("two %s filters on %s", f.op, f.column)
		}
		ops[f.op] = f.value
		where[f.column] = ops
	}
	return where, nil
}

// --- the counting SELECT ----------------------------------------------------------------

type recSource struct {
	table   string
	alias   string
	columns map[string]string // the relation's names -> the table's; nil for the table itself
	filters []reportFilter
}

func (s *recSource) column(name string) (string, error) {
	if s.columns == nil {
		return name, nil
	}
	c, ok := s.columns[name]
	if !ok {
		return "", notReport("column %s is not one of the source's", name)
	}
	return c, nil
}

// reportEntry is one dimension entry: its struct's fields, or none.
type reportEntry struct {
	fields  map[string]jnode
	element jnode // the element marker's input, for elements
}

type countingSelect struct {
	r     *reportRecognizer
	name  string
	q     jnode
	src   *recSource
	coh   *recCohorts
	cohT  jnode
	cohA  string
	where []jnode

	// struct expansion: the struct's column and the relation it is a column of
	structCol   string // upper-cased: D natively, _SF_T in the translation
	structRel   string // the relation alias the struct is a column of ("" natively)
	structTable string // the UNNEST's own alias, natively
	entries     []map[string]jnode
	elemCol     string // the element column's name (natively)
	elemTable   string
	elemField   string // the struct field the elements come from
	elemInput   jnode  // the list the elements come from, when not a struct field
	// elemText is the API's element_text for the elements, "json" where the
	// Snowflake translation reads elements that look like JSON as JSON, and
	// elemJSON the struct field of its other part of the list.
	elemText string
	elemJSON string

	member   string
	measures map[string]map[string]any
	order    []string
}

func (r *reportRecognizer) replacement(name string, q jnode) (jnode, string, error) {
	c := &countingSelect{r: r, name: name, q: q, measures: map[string]map[string]any{}}
	return c.replacement()
}

func (c *countingSelect) replacement() (jnode, string, error) {
	q := c.q
	for _, k := range []string{"having", "qualify", "sample"} {
		if !emptyValue(q[k]) {
			return nil, "", notReport("a counting SELECT with %s", k)
		}
	}
	if len(listOf(q["modifiers"])) > 0 {
		return nil, "", notReport("a counting SELECT with ORDER BY, LIMIT or DISTINCT")
	}
	if cm := nodeOf(q, "cte_map"); cm != nil && len(listOf(cm["map"])) > 0 {
		return nil, "", notReport("a counting SELECT with CTEs of its own")
	}
	if hasWindow(q["select_list"]) {
		return nil, "", notReport("a window function")
	}
	for _, p := range listOf(q["select_list"]) {
		if classOf(p.(jnode)) == "STAR" {
			return nil, "", notReport("SELECT *")
		}
	}
	joinConds, err := c.readFrom()
	if err != nil {
		return nil, "", err
	}
	if err := c.readCohorts(joinConds); err != nil {
		return nil, "", err
	}
	if c.src == nil {
		return c.cohortSizes()
	}
	return c.counts()
}

type recRelation struct {
	node jnode
	cond jnode
}

func flattenFrom(n jnode) ([]recRelation, error) {
	if n == nil {
		return nil, notReport("no FROM")
	}
	if typeOf(n) == "JOIN" {
		jt, rt := strOf(n, "join_type"), strOf(n, "ref_type")
		if jt != "INNER" || rt != "REGULAR" && rt != "CROSS" {
			return nil, notReport("a %s %s join", jt, rt)
		}
		if len(listOf(n["using_columns"])) > 0 {
			return nil, notReport("JOIN ... USING")
		}
		left, err := flattenFrom(nodeOf(n, "left"))
		if err != nil {
			return nil, err
		}
		right, err := flattenFrom(nodeOf(n, "right"))
		if err != nil {
			return nil, err
		}
		if len(right) == 1 {
			right[0].cond = nodeOf(n, "condition")
		} else if nodeOf(n, "condition") != nil {
			return nil, notReport("a join condition over a nested join")
		}
		return append(left, right...), nil
	}
	return []recRelation{{node: n}}, nil
}

func (c *countingSelect) readFrom() ([]jnode, error) {
	rels, err := flattenFrom(nodeOf(c.q, "from_table"))
	if err != nil {
		return nil, err
	}
	var conds []jnode
	for _, rel := range rels {
		if rel.cond != nil {
			conds = append(conds, conjuncts(rel.cond)...)
		}
		switch typeOf(rel.node) {
		case "BASE_TABLE":
			body := c.r.cteBody(rel.node)
			if body != nil && looksLikeCohorts(c.r, body) {
				if c.cohT != nil {
					return nil, notReport("more than one cohort relation")
				}
				c.cohT, c.cohA = rel.node, tableAlias(rel.node)
				continue
			}
			if c.src != nil {
				return nil, notReport("more than one source relation")
			}
			src, err := c.r.sourceOf(rel.node)
			if err != nil {
				return nil, err
			}
			c.src = src
		case "SUBQUERY":
			if c.src != nil {
				return nil, notReport("more than one source relation")
			}
			if err := c.translatedSource(rel.node); err != nil {
				return nil, err
			}
		case "TABLE_FUNCTION":
			if err := c.readUnnest(rel.node); err != nil {
				return nil, err
			}
		default:
			return nil, notReport("a %s relation", typeOf(rel.node))
		}
	}
	return conds, nil
}

func looksLikeCohorts(r *reportRecognizer, body jnode) bool {
	if typeOf(body) == "SET_OPERATION_NODE" {
		return true
	}
	if typeOf(body) != "SELECT_NODE" || hasAggregate(listOf(body["select_list"])) || typeOf(nodeOf(body, "from_table")) == "JOIN" {
		return false
	}
	if len(listOf(body["select_list"])) == 1 || isDistinct(body) {
		return true
	}
	from := nodeOf(body, "from_table")
	return from != nil && typeOf(from) == "BASE_TABLE" && r.cteBody(from) != nil
}

func (r *reportRecognizer) sourceOf(table jnode) (*recSource, error) {
	body := r.cteBody(table)
	if body == nil {
		return &recSource{table: recTableName(table), alias: tableAlias(table)}, nil
	}
	if err := simpleSelect(body); err != nil {
		return nil, err
	}
	if isDistinct(body) {
		return nil, notReport("a DISTINCT source")
	}
	from := nodeOf(body, "from_table")
	if from == nil || typeOf(from) != "BASE_TABLE" || r.cteBody(from) != nil {
		return nil, notReport("a source not read from a table")
	}
	cols := map[string]string{}
	for _, p := range listOf(body["select_list"]) {
		pn := p.(jnode)
		if classOf(pn) != "COLUMN_REF" {
			return nil, notReport("a source CTE computing columns")
		}
		out := strings.ToUpper(strOf(pn, "alias"))
		if out == "" {
			out = lastColumn(pn)
		}
		cols[out] = lastColumn(pn)
	}
	var filters []reportFilter
	for _, cc := range conjuncts(nodeOf(body, "where_clause")) {
		f, err := literalFilter(cc, func(parts []string) (string, error) { return parts[len(parts)-1], nil })
		if err != nil {
			return nil, err
		}
		filters = append(filters, f)
	}
	return &recSource{table: recTableName(from), alias: tableAlias(table), columns: cols, filters: filters}, nil
}

// translatedSource reads the Snowflake translation's struct FLATTEN:
// (SELECT x.*, CASE k.i WHEN 0 THEN {...} ... END AS s FROM t AS x CROSS
// JOIN range(n) AS k(i)) AS x.
func (c *countingSelect) translatedSource(sub jnode) error {
	q := nodeOf(sub, "subquery", "node")
	if q == nil || typeOf(q) != "SELECT_NODE" {
		return notReport("a subquery relation")
	}
	for _, k := range []string{"where_clause", "group_expressions", "having", "qualify"} {
		if !emptyValue(q[k]) {
			return notReport("a subquery relation with %s", k)
		}
	}
	sel := listOf(q["select_list"])
	if len(sel) != 2 {
		return notReport("a subquery relation that is not a struct expansion")
	}
	star, cs := sel[0].(jnode), sel[1].(jnode)
	if classOf(star) == "STAR" && isFunc(cs, "unnest") && c.src == nil {
		return c.translatedElements(sub, q, star, cs)
	}
	if classOf(star) != "STAR" || classOf(cs) != "CASE" || strOf(cs, "alias") == "" {
		return notReport("a subquery relation that is not a struct expansion")
	}
	rels, err := flattenFrom(nodeOf(q, "from_table"))
	if err != nil || len(rels) != 2 || rels[1].cond != nil {
		return notReport("a struct expansion not over one table")
	}
	base, rng := rels[0].node, rels[1].node
	if typeOf(base) != "BASE_TABLE" || typeOf(rng) != "TABLE_FUNCTION" || !isFunc(nodeOf(rng, "function"), "range") {
		return notReport("a struct expansion not over one table and range(n)")
	}
	if strings.ToUpper(strOf(star, "relation_name")) != tableAlias(base) {
		return notReport("a struct expansion's star of another relation")
	}
	args := childrenOf(nodeOf(rng, "function"))
	if len(args) != 1 || !isConstant(args[0]) {
		return notReport("range of something other than a constant")
	}
	n, ok := constValue(args[0]).(int64)
	idx := listOf(rng["column_name_alias"])
	if !ok || len(idx) != 1 {
		return notReport("range without one named column")
	}
	k := strings.ToUpper(strOf(rng, "alias"))
	i := strings.ToUpper(fmt.Sprint(idx[0]))
	checks := listOf(cs["case_checks"])
	if int64(len(checks)) != n || !isNullConst(nodeOf(cs, "else_expr")) {
		return notReport("a struct expansion whose cases are not one a row of range(n)")
	}
	var entries []map[string]jnode
	for want, ch := range checks {
		w := nodeOf(ch, "when_expr")
		if classOf(w) != "COMPARISON" || typeOf(w) != "COMPARE_EQUAL" {
			return notReport("a struct expansion case that is not an index")
		}
		p := columnParts(nodeOf(w, "left"))
		v := constValue(nodeOf(w, "right"))
		if len(p) == 0 || p[len(p)-1] != i || len(p) == 2 && p[0] != k || v != int64(want) {
			return notReport("a struct expansion case that is not its index")
		}
		fields, err := structFields(nodeOf(ch, "then_expr"))
		if err != nil {
			return err
		}
		entries = append(entries, fields)
	}
	if c.r.cteBody(base) != nil {
		return notReport("a struct expansion over a CTE")
	}
	c.src = &recSource{table: recTableName(base), alias: strings.ToUpper(strOf(sub, "alias"))}
	if c.src.alias == "" {
		return notReport("a struct expansion without an alias")
	}
	c.structCol, c.structRel = strings.ToUpper(strOf(cs, "alias")), c.src.alias
	c.entries = entries
	// The base's own alias names its columns inside; outside, the
	// subquery's alias does, with the same names.
	return nil
}

// translatedElements reads the Snowflake translation's FLATTEN of a list
// in each entry of a struct FLATTEN: (SELECT x.*, unnest(sf_parts_strings(
// x.s.l, x.s.j, 'BOTH')) AS e FROM (<the struct FLATTEN>) AS x) AS x, whose
// elements are read as JSON text where they look like JSON (sf_elem_string),
// as the API's element_text "json" reads them.
func (c *countingSelect) translatedElements(sub, q, star, un jnode) error {
	inner := nodeOf(q, "from_table")
	alias := strings.ToUpper(strOf(sub, "alias"))
	if inner == nil || typeOf(inner) != "SUBQUERY" || strings.ToUpper(strOf(inner, "alias")) != alias ||
		strings.ToUpper(strOf(star, "relation_name")) != alias || strOf(un, "alias") == "" {
		return notReport("elements of a subquery relation that is not a struct expansion")
	}
	if err := c.translatedSource(inner); err != nil {
		return err
	}
	args := childrenOf(un)
	if len(args) != 1 || !isFunc(args[0], "sf_parts_strings") || len(childrenOf(args[0])) != 3 {
		return notReport("elements other than the translation's sf_parts_strings")
	}
	parts := childrenOf(args[0])
	l, j := c.structField(parts[0]), c.structField(parts[1])
	mode, _ := constValue(parts[2]).(string)
	if l == "" || j == "" || !strings.HasPrefix(l, "_SF_L") || j != "_SF_J"+strings.TrimPrefix(l, "_SF_L") ||
		mode != "BOTH" && mode != "ARRAY" {
		return notReport("elements of other parts of a struct")
	}
	c.elemCol, c.elemTable, c.elemField, c.elemJSON, c.elemText = strings.ToUpper(strOf(un, "alias")), alias, l, j, "json"
	return nil
}

// translatedItems returns the list a translated entry's elements come
// from: a source column of strings, a list literal, or nil for none.
func (c *countingSelect) translatedItems(fields map[string]jnode) (jnode, error) {
	l, j := foldReport(fields[c.elemField]), foldReport(fields[c.elemJSON])
	if l == nil || isNullConst(l) {
		if j == nil || isNullConst(j) {
			return nil, nil // no elements
		}
		return nil, notReport("elements of something other than a list")
	}
	if classOf(l) == "CAST" {
		l = nodeOf(l, "child")
	}
	if classOf(j) == "CAST" {
		j = nodeOf(j, "child")
	}
	if isFunc(l, "list_value") && (j == nil || isNullConst(j)) {
		for _, x := range childrenOf(l) {
			v, ok := constValue(x).(string)
			if !isConstant(x) || !ok || strings.HasPrefix(v, "{") || strings.HasPrefix(v, "[") {
				return nil, notReport("a list literal of other than plain strings")
			}
		}
		return l, nil
	}
	if isFunc(l, "sf_list_part") && isFunc(j, "sf_other_part") && len(childrenOf(l)) == 1 && len(childrenOf(j)) == 1 &&
		exprKey(childrenOf(l)[0]) == exprKey(childrenOf(j)[0]) {
		col := childrenOf(l)[0]
		name := c.sourceColumn(col)
		if name == "" {
			return nil, notReport("elements of something other than a source column")
		}
		t, err := c.columnType(name)
		if err != nil {
			return nil, err
		}
		if t != "VARCHAR[]" {
			return nil, notReport("elements of %s, of type %s, as the translation reads them", name, t)
		}
		return col, nil
	}
	return nil, notReport("elements the translation reads otherwise")
}

func structFields(n jnode) (map[string]jnode, error) {
	if !isFunc(n, "struct_pack", "row") {
		return nil, notReport("a struct that is not a literal")
	}
	fields := map[string]jnode{}
	for _, f := range childrenOf(n) {
		a := strings.ToUpper(strOf(f, "alias"))
		if a == "" {
			return nil, notReport("a struct field without a name")
		}
		fields[a] = f
	}
	return fields, nil
}

func (c *countingSelect) readUnnest(tf jnode) error {
	fn := nodeOf(tf, "function")
	if !isFunc(fn, "unnest") || len(childrenOf(fn)) != 1 {
		return notReport("a table function other than UNNEST")
	}
	cols := listOf(tf["column_name_alias"])
	if len(cols) != 1 {
		return notReport("an UNNEST without one named column")
	}
	col := strings.ToUpper(fmt.Sprint(cols[0]))
	table := strings.ToUpper(strOf(tf, "alias"))
	arg := childrenOf(fn)[0]
	if isFunc(arg, "list_value") && c.entries == nil {
		var entries []map[string]jnode
		for _, e := range childrenOf(arg) {
			fields, err := structFields(e)
			if err != nil {
				return notReport("an UNNEST of a list that is not of structs")
			}
			entries = append(entries, fields)
		}
		if len(entries) == 0 {
			return notReport("an empty list")
		}
		c.entries, c.structCol, c.structTable = entries, col, table
		return nil
	}
	if c.elemCol != "" {
		return notReport("a third UNNEST")
	}
	if f := c.structField(arg); f != "" {
		c.elemField = f
	} else {
		c.elemInput = arg
	}
	c.elemCol, c.elemTable = col, table
	return nil
}

// structField returns the struct field a node reads, "" if none.
func (c *countingSelect) structField(n jnode) string {
	if c.structCol == "" {
		return ""
	}
	if p := columnParts(n); p != nil {
		switch {
		case c.structRel != "" && len(p) == 3 && p[0] == c.structRel && p[1] == c.structCol:
			return p[2]
		case c.structRel != "" && len(p) == 2 && p[0] == c.structCol:
			return p[1]
		case c.structRel == "" && len(p) == 2 && p[0] == c.structCol:
			return p[1]
		case c.structRel == "" && len(p) == 3 && p[0] == c.structTable && p[1] == c.structCol:
			return p[2]
		}
		return ""
	}
	if (classOf(n) == "OPERATOR" && typeOf(n) == "ARRAY_EXTRACT") || isFunc(n, "struct_extract", "array_extract") {
		ch := childrenOf(n)
		if len(ch) == 2 && isConstant(ch[1]) {
			p := columnParts(ch[0])
			key, ok := constValue(ch[1]).(string)
			if !ok {
				return ""
			}
			if len(p) == 1 && p[0] == c.structCol && c.structRel == "" || len(p) == 2 && p[1] == c.structCol &&
				(p[0] == c.structTable || p[0] == c.structRel) {
				return strings.ToUpper(key)
			}
		}
	}
	return ""
}

// relationOf says what a column reference reads: "source", "cohort",
// "struct", "element", or "".
func (c *countingSelect) relationOf(p []string) string {
	switch len(p) {
	case 1:
		switch {
		case c.structCol != "" && c.structRel == "" && p[0] == c.structCol:
			return "struct"
		case c.elemCol != "" && p[0] == c.elemCol:
			return "element"
		case c.coh != nil && c.coh.labelColumn == p[0] && c.src != nil:
			return "cohort"
		case c.src != nil:
			return "source"
		case c.coh != nil:
			return "cohort"
		}
	case 2:
		switch {
		case c.elemCol != "" && p[0] == c.elemTable && p[1] == c.elemCol:
			return "element"
		case c.src != nil && p[0] == c.src.alias:
			if c.structRel != "" && p[1] == c.structCol {
				return "struct"
			}
			return "source"
		case c.cohA != "" && p[0] == c.cohA:
			return "cohort"
		case c.structCol != "" && c.structRel == "" && (p[0] == c.structCol || p[0] == c.structTable):
			return "struct"
		case c.elemCol != "" && (p[0] == c.elemTable && p[1] == c.elemCol):
			return "element"
		}
	case 3:
		if c.structRel != "" && p[0] == c.structRel && p[1] == c.structCol {
			return "struct"
		}
		if c.structRel == "" && p[0] == c.structTable && p[1] == c.structCol {
			return "struct"
		}
	}
	return ""
}

func (c *countingSelect) sourceColumn(n jnode) string {
	p := columnParts(n)
	if p == nil || c.relationOf(p) != "source" || c.structField(n) != "" {
		return ""
	}
	col, err := c.src.column(p[len(p)-1])
	if err != nil {
		return ""
	}
	return col
}

func (c *countingSelect) readCohorts(joinConds []jnode) error {
	if c.cohT != nil && c.src != nil {
		if len(joinConds) != 1 || classOf(joinConds[0]) != "COMPARISON" || typeOf(joinConds[0]) != "COMPARE_EQUAL" {
			return notReport("a cohort join that is not one equality")
		}
		l, rt := columnParts(nodeOf(joinConds[0], "left")), columnParts(nodeOf(joinConds[0], "right"))
		if l == nil || rt == nil {
			return notReport("a cohort join on expressions")
		}
		if c.relationOf(l) == "cohort" {
			l, rt = rt, l
		}
		if c.relationOf(l) != "source" || c.relationOf(rt) != "cohort" {
			return notReport("a cohort join on other relations")
		}
		col, err := c.src.column(l[len(l)-1])
		if err != nil {
			return err
		}
		c.member = col
		coh, err := c.r.cohortsOf(c.cohT, rt[len(rt)-1])
		if err != nil {
			return err
		}
		c.coh = coh
	} else if c.cohT != nil {
		if len(joinConds) > 0 {
			return notReport("a join without a source")
		}
		member := ""
		walkOwnExpr(c.q["select_list"], func(n jnode) {
			if isFunc(n, "count") && boolTrue(n["distinct"]) && len(childrenOf(n)) == 1 && member == "" {
				member = lastColumn(childrenOf(n)[0])
			}
		})
		if member == "" {
			return notReport("no member count")
		}
		coh, err := c.r.cohortsOf(c.cohT, member)
		if err != nil {
			return err
		}
		c.coh = coh
	} else if len(joinConds) > 0 {
		return notReport("a join that is not to cohorts")
	}
	for _, w := range conjuncts(nodeOf(c.q, "where_clause")) {
		if c.coh == nil && c.src != nil {
			ok, err := c.inCohort(w)
			if err != nil {
				return err
			}
			if ok {
				continue
			}
		}
		c.where = append(c.where, w)
	}
	if c.coh == nil {
		return notReport("no cohort")
	}
	return nil
}

// inCohort reads source.member IN (SELECT member FROM cohort).
func (c *countingSelect) inCohort(w jnode) (bool, error) {
	if classOf(w) != "SUBQUERY" || strOf(w, "subquery_type") != "ANY" || strOf(w, "comparison_type") != "COMPARE_EQUAL" {
		return false, nil
	}
	p := columnParts(nodeOf(w, "child"))
	if p == nil || c.relationOf(p) != "source" {
		return false, nil
	}
	q := nodeOf(w, "subquery", "node")
	if q == nil || typeOf(q) != "SELECT_NODE" || len(listOf(q["select_list"])) != 1 {
		return false, nil
	}
	m := listOf(q["select_list"])[0].(jnode)
	if classOf(m) != "COLUMN_REF" {
		return false, nil
	}
	col, err := c.src.column(p[len(p)-1])
	if err != nil {
		return false, err
	}
	member := lastColumn(m)
	var coh *recCohort
	from := nodeOf(q, "from_table")
	if emptyValue(q["where_clause"]) && from != nil && typeOf(from) == "BASE_TABLE" && len(listOf(q["modifiers"])) == 0 {
		coh, err = c.r.cohortOf(from, member)
	} else {
		coh, err = c.r.sqlCohort("COHORT", q, member)
	}
	if err != nil {
		return false, err
	}
	c.member = col
	// A semi-join keeps each source row once, whatever the cohort repeats.
	c.coh = &recCohorts{cohorts: []*recCohort{{label: coh.label, spec: coh.spec, distinct: true}}, member: member}
	return true, nil
}

func (c *countingSelect) isLabel(n jnode) bool {
	p := columnParts(n)
	return p != nil && c.coh != nil && c.coh.labelColumn != "" && c.relationOf(p) == "cohort" && p[len(p)-1] == c.coh.labelColumn
}

func (c *countingSelect) isMember(n jnode) bool {
	if col := c.sourceColumn(n); col != "" {
		return col == c.member
	}
	p := columnParts(n)
	if p != nil && c.src == nil && c.relationOf(p) == "cohort" {
		return p[len(p)-1] == c.coh.member
	}
	return false
}

// groups returns the GROUP BY expressions, positions and output names
// resolved; for GROUP BY ALL, the projections that do not aggregate.
func (c *countingSelect) groups() ([]jnode, error) {
	sel := listOf(c.q["select_list"])
	if strOf(c.q, "aggregate_handling") == "FORCE_AGGREGATES" {
		var out []jnode
		for _, p := range sel {
			if !hasAggregate([]any{p}) {
				out = append(out, p.(jnode))
			}
		}
		return out, nil
	}
	if len(listOf(c.q["group_sets"])) > 1 {
		return nil, notReport("GROUPING SETS, ROLLUP or CUBE")
	}
	var out []jnode
	for _, g := range listOf(c.q["group_expressions"]) {
		gn := g.(jnode)
		if isConstant(gn) {
			i, ok := constValue(gn).(int64)
			if !ok || i < 1 || int(i) > len(sel) {
				return nil, notReport("a GROUP BY position out of range")
			}
			out = append(out, sel[i-1].(jnode))
			continue
		}
		if p := columnParts(gn); len(p) == 1 {
			for _, s := range sel {
				sn := s.(jnode)
				if strings.ToUpper(strOf(sn, "alias")) == p[0] && !(classOf(sn) == "COLUMN_REF" && lastColumn(sn) == p[0]) {
					// A name of an output and of an input both is the input's
					// in GROUP BY; recognition leaves that to DuckDB.
					if c.inputName(p[0]) {
						return nil, notReport("GROUP BY %s, the name of a column and of an output", p[0])
					}
					gn = sn
					break
				}
			}
		}
		out = append(out, gn)
	}
	return out, nil
}

// inputName says whether a relation of the FROM may have a column name.
func (c *countingSelect) inputName(name string) bool {
	if c.coh != nil && (name == c.coh.member || name == c.coh.labelColumn) || name == c.structCol || name == c.elemCol {
		return true
	}
	if c.src == nil {
		return false
	}
	if c.src.columns != nil {
		_, ok := c.src.columns[name]
		return ok
	}
	_, err := c.columnType(name)
	return err == nil || !strings.HasPrefix(err.Error(), "no column ")
}

func (c *countingSelect) subst(n jnode, e reportEntry) (jnode, error) {
	var err error
	var rec func(v any) any
	rec = func(v any) any {
		switch x := v.(type) {
		case jnode:
			if f := c.structField(x); f != "" {
				if e.fields == nil {
					err = notReport("a struct field outside the struct")
					return x
				}
				val, ok := e.fields[f]
				if !ok {
					return nullConst()
				}
				return cloneJ(val)
			}
			if p := columnParts(x); p != nil {
				switch c.relationOf(p) {
				case "element":
					if e.element == nil {
						err = notReport("an element outside its UNNEST")
						return x
					}
					if classOf(e.element) == "__IGLOO_LITERAL" {
						// An element of a list literal is the element.
						lit, _ := e.element["value"].(jnode)
						return cloneJ(lit)
					}
					return elementMarker(e.element)
				case "struct":
					err = notReport("a whole struct")
					return x
				}
				return x
			}
			out := make(jnode, len(x))
			for k, cv := range x {
				out[k] = rec(cv)
			}
			return out
		case []any:
			out := make([]any, len(x))
			for i, cv := range x {
				out[i] = rec(cv)
			}
			return out
		}
		return v
	}
	out, _ := rec(n).(jnode)
	if err != nil {
		return nil, err
	}
	return foldReport(out), nil
}

const elementMarkerClass = "__IGLOO_ELEMENT"

func elementMarker(of jnode) jnode {
	return jnode{"class": elementMarkerClass, "type": elementMarkerClass, "of": cloneJ(of)}
}

func (c *countingSelect) entryList() ([]reportEntry, error) {
	if c.entries == nil {
		if c.elemCol == "" {
			return []reportEntry{{}}, nil
		}
		if c.elemInput == nil {
			return nil, notReport("elements of a struct field outside a struct")
		}
		return []reportEntry{{element: c.elemInput}}, nil
	}
	var out []reportEntry
	for _, fields := range c.entries {
		if c.elemCol == "" {
			out = append(out, reportEntry{fields: fields})
			continue
		}
		items, ok := fields[c.elemField]
		if c.elemText != "" {
			var err error
			if items, err = c.translatedItems(fields); err != nil {
				return nil, err
			}
			ok = items != nil
		}
		if c.elemField == "" || !ok || isNullConst(items) {
			continue // UNNEST(NULL) gives no rows
		}
		if isFunc(items, "list_value") {
			for _, it := range childrenOf(items) {
				out = append(out, reportEntry{fields: fields, element: jnode{"class": "__IGLOO_CONSTANT_ELEMENT", "value": it}})
			}
			continue
		}
		out = append(out, reportEntry{fields: fields, element: items})
	}
	if len(out) == 0 {
		return nil, notReport("no entries")
	}
	return out, nil
}

type mappedEntry struct {
	keys        []any
	value       map[string]any
	metric      string
	skipBlank   bool
	metricWhere []reportFilter
	entry       reportEntry
	groups      []jnode
}

func (c *countingSelect) counts() (jnode, string, error) {
	if c.member == "" {
		return nil, "", notReport("no member column")
	}
	groups, err := c.groups()
	if err != nil {
		return nil, "", err
	}
	var labelSlots []int
	for i, g := range groups {
		if c.isLabel(g) {
			labelSlots = append(labelSlots, i)
		}
	}
	if len(c.coh.cohorts) > 1 && len(labelSlots) != 1 || len(labelSlots) > 1 {
		return nil, "", notReport("several cohorts not grouped by their label once")
	}
	isLabelSlot := func(i int) bool { return len(labelSlots) == 1 && labelSlots[0] == i }
	if len(groups) == 0 {
		return c.total()
	}
	entries, err := c.entryList()
	if err != nil {
		return nil, "", err
	}
	valueSlot := -1
	var subs [][]jnode
	for _, e := range entries {
		e = c.constantElement(e)
		var row []jnode
		varying := -1
		for i, g := range groups {
			if isLabelSlot(i) {
				row = append(row, g)
				continue
			}
			s, err := c.subst(g, e)
			if err != nil {
				return nil, "", err
			}
			row = append(row, s)
			if !isConstant(s) {
				if varying >= 0 {
					return nil, "", notReport("more than one varying GROUP BY expression")
				}
				varying = i
			}
		}
		if varying >= 0 {
			if valueSlot >= 0 && valueSlot != varying {
				return nil, "", notReport("the value in different GROUP BY expressions")
			}
			valueSlot = varying
		}
		subs = append(subs, row)
	}
	if valueSlot < 0 {
		for i := range groups {
			if !isLabelSlot(i) {
				valueSlot = i
			}
		}
		if valueSlot < 0 {
			return nil, "", notReport("nothing to group by")
		}
	}
	var mapped []mappedEntry
	var sourceFilters []string
	var filterSpec []reportFilter
	first := true
	for ei, e := range entries {
		e = c.constantElement(e)
		row := subs[ei]
		value := row[valueSlot]
		metric, err := c.metricNode(e)
		if err != nil {
			return nil, "", err
		}
		var entryFilters []jnode
		skipBlank, notNull, empty := false, false, false
		var mw []reportFilter
		for _, w := range c.where {
			fw, err := c.subst(w, e)
			if err != nil {
				return nil, "", err
			}
			for _, part := range conjuncts(fw) {
				if b, ok := constBool(part); ok {
					if !b {
						empty = true
					}
					continue
				}
				if isNullConst(part) {
					empty = true
					continue
				}
				kind, f, err := c.classify(part, value, metric)
				if err != nil {
					return nil, "", err
				}
				switch kind {
				case "skip_blank":
					skipBlank = true
				case "not_null":
					notNull = true
				case "source":
					entryFilters = append(entryFilters, part)
				case "metric":
					mw = append(mw, f)
				}
			}
		}
		if empty {
			continue
		}
		if notNull && !skipBlank {
			return nil, "", notReport("value IS NOT NULL without TRIM(value) <> ''")
		}
		var keysSQL []string
		for _, f := range entryFilters {
			keysSQL = append(keysSQL, exprKey(f))
		}
		sort.Strings(keysSQL)
		if first {
			sourceFilters = keysSQL
			for _, f := range entryFilters {
				rf, err := literalFilter(f, c.filterColumn)
				if err != nil {
					return nil, "", err
				}
				filterSpec = append(filterSpec, rf)
			}
			first = false
		} else if strings.Join(keysSQL, "\x00") != strings.Join(sourceFilters, "\x00") {
			return nil, "", notReport("source filters that differ by dimension")
		}
		var keys []any
		for i, g := range row {
			if isLabelSlot(i) || i == valueSlot {
				continue
			}
			keys = append(keys, constValue(g))
		}
		vs, err := c.valueSpec(value)
		if err != nil {
			return nil, "", err
		}
		if vs == nil {
			// A NULL value: its rows count in the NULL value unless the
			// blank check drops them.
			if !skipBlank {
				return nil, "", notReport("a dimension whose value is NULL, kept")
			}
			continue
		}
		mcol := ""
		if metric != nil && !isNullConst(metric) {
			if mcol, err = c.metricColumn(metric); err != nil {
				return nil, "", err
			}
		}
		if len(mw) > 0 && mcol == "" {
			return nil, "", notReport("a metric filter without a metric")
		}
		mapped = append(mapped, mappedEntry{keys: keys, value: vs, metric: mcol, skipBlank: skipBlank, metricWhere: mw, entry: e, groups: row})
	}
	if len(mapped) == 0 {
		return nil, "", notReport("every entry filtered out")
	}
	if err := c.uniformFields(groups); err != nil {
		return nil, "", err
	}
	names := c.dimensionNames(mapped)
	types, err := c.r.cteTypes(c.name)
	if err != nil {
		return nil, "", err
	}
	var proj []any
	for i, p := range listOf(c.q["select_list"]) {
		pn := p.(jnode)
		m, err := c.mapProjection(pn, groups, isLabelSlot, valueSlot, mapped, names)
		if err != nil {
			return nil, "", err
		}
		if isAPIColumn(m, "value") && i < len(types) && !strings.EqualFold(types[i][1], "VARCHAR") {
			// The API's value is text; the CTE's, grouped by as it is, comes
			// back from that text only for types whose text is one a value.
			for _, me := range mapped {
				t, err := c.valueType(me.value)
				if err != nil {
					return nil, "", err
				}
				if !textRoundTrips(t) {
					return nil, "", notReport("a value of type %s grouped by as it is", t)
				}
			}
		}
		proj = append(proj, withAlias(m, outputName(pn)))
	}
	var dims []any
	for i, m := range mapped {
		d := map[string]any{"name": names[i]}
		for k, v := range m.value {
			d[k] = v
		}
		if m.skipBlank {
			d["skip_blank"] = true
		}
		if m.metric != "" {
			d["metric"] = m.metric
			if len(m.metricWhere) > 0 {
				w, err := whereSpec(m.metricWhere)
				if err != nil {
					return nil, "", err
				}
				d["metric_where"] = w["metric"]
			}
		}
		dims = append(dims, d)
	}
	spec, err := c.spec(dims, filterSpec, true)
	if err != nil {
		return nil, "", err
	}
	return c.apiSelect(spec, proj, false)
}

func isAPIColumn(n jnode, name string) bool {
	p := listOf(n["column_names"])
	return classOf(n) == "COLUMN_REF" && len(p) == 2 && p[0] == reportAlias && p[1] == name
}

// textRoundTrips says whether a type's values have one text each, which
// reads back as the value.
func textRoundTrips(t string) bool {
	switch {
	case t == "VARCHAR" || t == "DATE" || t == "BOOLEAN" || strings.HasPrefix(t, "DECIMAL"):
		return true
	}
	return integerType(t)
}

func integerType(t string) bool {
	switch t {
	case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT":
		return true
	}
	return false
}

// valueType returns the type of a dimension's value before the API's cast
// to text.
func (c *countingSelect) valueType(v map[string]any) (string, error) {
	if _, ok := v["map"]; ok {
		return "VARCHAR", nil
	}
	if _, ok := v["constant"]; ok {
		return "VARCHAR", nil
	}
	if col, ok := v["column"].(string); ok {
		return c.columnType(col)
	}
	if col, ok := v["elements"].(string); ok {
		t, err := c.columnType(col)
		if err != nil {
			return "", err
		}
		if !strings.HasSuffix(t, "[]") {
			return "", notReport("elements of %s, of type %s", col, t)
		}
		return strings.TrimSuffix(t, "[]"), nil
	}
	return "", notReport("a value of no known type")
}

// uniformFields checks that DuckDB's list of struct literals, which gives
// each field one type, changes none of the values recognition reads from
// it: the fields grouped by have one type in every entry, and the metric
// field types that convert to DOUBLE as the API converts the columns.
func (c *countingSelect) uniformFields(groups []jnode) error {
	if c.entries == nil {
		return nil
	}
	used := map[string]bool{}
	for _, g := range groups {
		walkOwnExpr(g, func(n jnode) {
			if f := c.structField(n); f != "" {
				used[f] = false
			}
		})
	}
	walkOwnExpr(c.q["select_list"], func(n jnode) {
		if !reportAggregate(n) {
			return
		}
		walkOwnExpr(n, func(m jnode) {
			if f := c.structField(m); f != "" {
				if _, grouped := used[f]; !grouped {
					used[f] = true
				}
			}
		})
	})
	if c.elemField != "" {
		used[c.elemField] = false
	}
	for f, metric := range used {
		types := map[string]bool{}
		for _, fields := range c.entries {
			v, ok := fields[f]
			if !ok {
				continue
			}
			t, err := c.exprType(v)
			if err != nil {
				return err
			}
			if t != "" {
				types[t] = true
			}
		}
		if len(types) <= 1 && !types["?"] {
			continue
		}
		if metric && numericUnion(types) {
			continue
		}
		return notReport("struct field %s of several types", f)
	}
	return nil
}

// exprType returns the type of an entry's field value: "" for NULL, "?"
// when recognition cannot tell.
func (c *countingSelect) exprType(n jnode) (string, error) {
	switch {
	case isNullConst(n):
		return "", nil
	case isConstant(n):
		return strings.ToUpper(strOf(nodeOf(n, "value", "type"), "id")), nil
	case classOf(n) == "CAST":
		t := nodeOf(n, "cast_type")
		id := strings.ToUpper(strOf(t, "id"))
		if id == "DECIMAL" {
			info := nodeOf(t, "type_info")
			return fmt.Sprintf("DECIMAL(%v,%v)", info["width"], info["scale"]), nil
		}
		if id == "LIST" {
			child := strings.ToUpper(strOf(nodeOf(t, "type_info", "child_type"), "id"))
			if child == "" || child == "LIST" || child == "DECIMAL" || child == "STRUCT" || child == "MAP" {
				return "?", nil
			}
			return child + "[]", nil
		}
		return id, nil
	case isFunc(n, "sf_fields_string", "upper", "lower", "trim", "concat"):
		return "VARCHAR", nil
	case isFunc(n, "sf_text_read") && len(childrenOf(n)) == 1:
		return c.exprType(childrenOf(n)[0])
	case isFunc(n, "list_value"):
		var elem string
		for _, x := range childrenOf(n) {
			t, err := c.exprType(x)
			if err != nil {
				return "", err
			}
			if t != "" && elem != "" && t != elem {
				return "?", nil
			}
			if t != "" {
				elem = t
			}
		}
		if elem == "" {
			return "?", nil
		}
		return elem + "[]", nil
	case classOf(n) == "CASE":
		var out string
		branches := []jnode{nodeOf(n, "else_expr")}
		for _, ch := range listOf(n["case_checks"]) {
			branches = append(branches, nodeOf(ch, "then_expr"))
		}
		for _, b := range branches {
			if b == nil {
				continue
			}
			t, err := c.exprType(b)
			if err != nil {
				return "", err
			}
			if t != "" && out != "" && t != out {
				return "?", nil
			}
			if t != "" {
				out = t
			}
		}
		return out, nil
	}
	if col := c.sourceColumn(n); col != "" {
		return c.columnType(col)
	}
	return "?", nil
}

// constantElement turns an element of a literal list into a constant.
func (c *countingSelect) constantElement(e reportEntry) reportEntry {
	if e.element != nil && classOf(e.element) == "__IGLOO_CONSTANT_ELEMENT" {
		v, _ := e.element["value"].(jnode)
		return reportEntry{fields: e.fields, element: jnode{"class": "__IGLOO_LITERAL", "value": v}}
	}
	return e
}

func (c *countingSelect) filterColumn(p []string) (string, error) {
	if c.relationOf(p) != "source" {
		return "", notReport("a filter on %s", strings.Join(p, "."))
	}
	return c.src.column(p[len(p)-1])
}

// metricNode returns an entry's metric: the struct field that aggregates
// read, as its expression in the entry.
func (c *countingSelect) metricNode(e reportEntry) (jnode, error) {
	if e.fields == nil {
		return nil, nil
	}
	fields := map[string]bool{}
	walkOwnExpr(c.q["select_list"], func(n jnode) {
		if !reportAggregate(n) {
			return
		}
		walkOwnExpr(n, func(m jnode) {
			if f := c.structField(m); f != "" {
				fields[f] = true
			}
		})
	})
	if len(fields) > 1 {
		return nil, notReport("aggregates over several struct fields")
	}
	for f := range fields {
		v, ok := e.fields[f]
		if !ok {
			return nullConst(), nil
		}
		return foldReport(v), nil
	}
	return nil, nil
}

// metricColumn returns the source column of a metric, as the API casts it
// to DOUBLE: a column, or the Snowflake translation's variant text of a
// column whose type makes that text read back as the same number.
func (c *countingSelect) metricColumn(n jnode) (string, error) {
	n = stripText(n)
	if col := c.sourceColumn(n); col != "" {
		return col, nil
	}
	if x := sfColumn(n); x != nil {
		if col := c.sourceColumn(x); col != "" {
			t, err := c.columnType(col)
			if err != nil {
				return "", err
			}
			if numericExactAsText(t) || t == "VARCHAR" {
				return col, nil
			}
			return "", notReport("a metric of type %s, whose text does not read back as the same number", t)
		}
	}
	return "", notReport("a metric that is not a column")
}

// sfColumn reads sf_fields_string(sf_list_part(c), sf_string_part(c),
// sf_other_part(c)): the Snowflake translation's text of a column.
func sfColumn(n jnode) jnode {
	if !isFunc(n, "sf_fields_string") {
		return nil
	}
	ch := childrenOf(n)
	if len(ch) != 3 || !isFunc(ch[0], "sf_list_part") || !isFunc(ch[1], "sf_string_part") || !isFunc(ch[2], "sf_other_part") {
		return nil
	}
	var x jnode
	for _, part := range ch {
		pc := childrenOf(part)
		if len(pc) != 1 || classOf(pc[0]) != "COLUMN_REF" {
			return nil
		}
		if x != nil && exprKey(x) != exprKey(pc[0]) {
			return nil
		}
		x = pc[0]
	}
	return x
}

// numericExactAsText says whether a column's text, as the Snowflake
// translation writes it, reads back as the DOUBLE the API casts the column
// to: that of a DOUBLE, an integer or a DECIMAL whose unscaled values a
// DOUBLE holds exactly, or that has no fraction.
func numericExactAsText(t string) bool {
	if t == "DOUBLE" || integerType(t) {
		return true
	}
	w, s, ok := decimalType(t)
	return ok && (s == 0 || w <= 15)
}

// decimalType reads DECIMAL(width,scale).
func decimalType(t string) (width, scale int, ok bool) {
	if _, err := fmt.Sscanf(t, "DECIMAL(%d,%d)", &width, &scale); err != nil {
		return 0, 0, false
	}
	return width, scale, true
}

// numericUnion says whether DuckDB's one type for values of these types,
// cast to DOUBLE, gives what casting each to DOUBLE does: with a DOUBLE
// among them, every number converts to DOUBLE as it would alone; without,
// integers and DECIMALs without a fraction widen exactly.
func numericUnion(types map[string]bool) bool {
	double := types["DOUBLE"]
	for t := range types {
		_, s, isDecimal := decimalType(t)
		switch {
		case t == "DOUBLE" || integerType(t):
		case isDecimal && (double || s == 0):
		default:
			return false
		}
	}
	return true
}

func (c *countingSelect) columnType(col string) (string, error) {
	types, ok := c.r.types[c.src.table]
	if !ok {
		cols, err := c.r.env.describe("SELECT * FROM " + quoteTableName(c.src.table))
		if err != nil {
			return "", notReport("the source's column types could not be read")
		}
		types = map[string]string{}
		for _, ct := range cols {
			types[strings.ToUpper(ct[0])] = strings.ToUpper(ct[1])
		}
		c.r.types[c.src.table] = types
	}
	t, ok := types[col]
	if !ok {
		return "", notReport("no column %s in %s", col, c.src.table)
	}
	return t, nil
}

func quoteTableName(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = quoteIdent(p)
	}
	return strings.Join(parts, ".")
}

func (c *countingSelect) classify(part, value, metric jnode) (string, reportFilter, error) {
	vkey := exprKey(stripText(value))
	if classOf(part) == "COMPARISON" && typeOf(part) == "COMPARE_NOTEQUAL" {
		l, r := nodeOf(part, "left"), nodeOf(part, "right")
		if isFunc(l, "trim") && len(childrenOf(l)) == 1 && isConstant(r) {
			if s, ok := constValue(r).(string); ok && s == "" && exprKey(stripText(childrenOf(l)[0])) == vkey {
				return "skip_blank", reportFilter{}, nil
			}
		}
	}
	if classOf(part) == "OPERATOR" {
		ch := childrenOf(part)
		if typeOf(part) == "OPERATOR_IS_NOT_NULL" && len(ch) == 1 && exprKey(stripText(ch[0])) == vkey {
			return "not_null", reportFilter{}, nil
		}
		if typeOf(part) == "OPERATOR_NOT" && len(ch) == 1 && typeOf(ch[0]) == "OPERATOR_IS_NULL" {
			if cc := childrenOf(ch[0]); len(cc) == 1 && exprKey(stripText(cc[0])) == vkey {
				return "not_null", reportFilter{}, nil
			}
		}
	}
	if metric != nil && !isNullConst(metric) {
		mkey := exprKey(stripText(metric))
		isMetric := func(n jnode) bool {
			if classOf(n) != "CAST" || !doubleType(nodeOf(n, "cast_type")) {
				return false
			}
			return exprKey(stripText(nodeOf(n, "child"))) == mkey
		}
		if classOf(part) == "COMPARISON" {
			op, ok := reportComparisons[typeOf(part)]
			l, r := nodeOf(part, "left"), nodeOf(part, "right")
			if ok {
				if isMetric(l) {
					v, err := recLiteral(r)
					return "metric", reportFilter{"metric", op, v}, err
				}
				if isMetric(r) {
					v, err := recLiteral(l)
					return "metric", reportFilter{"metric", reportFlipped[op], v}, err
				}
			}
		}
		if classOf(part) == "BETWEEN" && isMetric(nodeOf(part, "input")) {
			lo, err := recLiteral(nodeOf(part, "lower"))
			if err != nil {
				return "", reportFilter{}, err
			}
			hi, err := recLiteral(nodeOf(part, "upper"))
			return "metric", reportFilter{"metric", "between", []any{lo, hi}}, err
		}
	}
	sourceOnly := true
	walkAll(part, func(n jnode) {
		if p := columnParts(n); p != nil && (c.relationOf(p) != "source" || c.structField(n) != "") {
			sourceOnly = false
		}
		if strings.HasPrefix(classOf(n), "__IGLOO") {
			sourceOnly = false
		}
	})
	if sourceOnly {
		if _, err := literalFilter(part, c.filterColumn); err == nil {
			return "source", reportFilter{}, nil
		}
	}
	return "", reportFilter{}, notReport("a filter recognition cannot map")
}

// valueSpec returns the dimension form of a value; nil for NULL.
func (c *countingSelect) valueSpec(n jnode) (map[string]any, error) {
	s := stripText(n)
	switch {
	case isNullConst(s):
		return nil, nil
	case isConstant(s):
		v := constValue(s)
		str, ok := v.(string)
		if !ok {
			str = fmt.Sprint(v)
		}
		return map[string]any{"constant": str}, nil
	case classOf(s) == "__IGLOO_LITERAL":
		lit, _ := s["value"].(jnode)
		lit = foldReport(lit)
		if v, ok := constValue(lit).(string); ok && isConstant(lit) {
			return map[string]any{"constant": v}, nil
		}
		return nil, notReport("a list element that is not a string")
	case classOf(s) == elementMarkerClass:
		of, _ := s["of"].(jnode)
		if col := c.sourceColumn(of); col != "" {
			if c.elemText != "" {
				return map[string]any{"elements": col, "element_text": c.elemText}, nil
			}
			return map[string]any{"elements": col}, nil
		}
		return nil, notReport("elements of something other than a source column")
	}
	if col := c.sourceColumn(s); col != "" {
		return map[string]any{"column": col}, nil
	}
	if x := sfColumn(s); x != nil {
		if col := c.sourceColumn(x); col != "" {
			t, err := c.columnType(col)
			if err != nil {
				return nil, err
			}
			if t != "VARCHAR" {
				return nil, notReport("a value of type %s as Snowflake's text of it", t)
			}
			return map[string]any{"column": col}, nil
		}
	}
	if isFunc(s, "sf_fields_string") {
		ch := childrenOf(s)
		if len(ch) == 3 && isNullConst(ch[0]) && isFunc(ch[1], "sf_string_part") && isFunc(ch[2], "sf_expr_part") {
			e1, e2 := childrenOf(ch[1]), childrenOf(ch[2])
			if len(e1) == 1 && len(e2) == 1 && exprKey(e1[0]) == exprKey(e2[0]) {
				if m := c.booleanMap(e1[0]); m != nil {
					return m, nil
				}
			}
		}
	}
	if m := c.booleanMap(s); m != nil {
		return m, nil
	}
	return nil, notReport("a value recognition cannot map")
}

// booleanMap reads CASE WHEN c IS NULL THEN NULL ELSE (CASE WHEN c THEN 'a'
// ELSE 'b' END) END: a map that keeps NULL as NULL.
func (c *countingSelect) booleanMap(n jnode) map[string]any {
	if classOf(n) != "CASE" {
		return nil
	}
	checks := listOf(n["case_checks"])
	if len(checks) != 1 {
		return nil
	}
	w, t := nodeOf(checks[0], "when_expr"), nodeOf(checks[0], "then_expr")
	if typeOf(w) != "OPERATOR_IS_NULL" || !isNullConst(t) {
		return nil
	}
	wc := childrenOf(w)
	if len(wc) != 1 {
		return nil
	}
	col := c.sourceColumn(wc[0])
	if col == "" {
		return nil
	}
	inner := nodeOf(n, "else_expr")
	if classOf(inner) != "CASE" || len(listOf(inner["case_checks"])) != 1 {
		return nil
	}
	ic := listOf(inner["case_checks"])[0]
	cond, a, b := nodeOf(ic, "when_expr"), nodeOf(ic, "then_expr"), nodeOf(inner, "else_expr")
	if c.sourceColumn(cond) != col || !isConstant(a) || !isConstant(b) {
		return nil
	}
	as, ok1 := constValue(a).(string)
	bs, ok2 := constValue(b).(string)
	if !ok1 || !ok2 {
		return nil
	}
	return map[string]any{"column": col, "map": map[string]any{"true": as, "false": bs}}
}

func (c *countingSelect) dimensionNames(m []mappedEntry) []string {
	names := map[string]string{}
	used := map[string]bool{}
	var out []string
	for _, e := range m {
		var parts []string
		for _, k := range e.keys {
			if k == nil {
				parts = append(parts, "NULL")
			} else {
				parts = append(parts, fmt.Sprint(k))
			}
		}
		key := strings.Join(parts, "\x00")
		if _, ok := names[key]; !ok {
			base := strings.Join(parts, "|")
			if len(parts) == 0 {
				base = strings.ToUpper(c.name)
			}
			name := base
			for i := 2; used[name]; i++ {
				name = base + "#" + strconv.Itoa(i)
			}
			used[name] = true
			names[key] = name
		}
		out = append(out, names[key])
	}
	return out
}

func outputName(p jnode) string {
	if a := strOf(p, "alias"); a != "" {
		return a
	}
	if parts := listOf(p["column_names"]); len(parts) > 0 {
		s, _ := parts[len(parts)-1].(string)
		return s
	}
	return ""
}

func withAlias(n jnode, alias string) jnode {
	n["alias"] = alias
	return n
}

func (c *countingSelect) mapProjection(p jnode, groups []jnode, isLabelSlot func(int) bool, valueSlot int, mapped []mappedEntry, names []string) (jnode, error) {
	if hasAggregate([]any{p}) {
		return c.mapAggregates(p, true)
	}
	if c.isLabel(p) {
		return c.r.t.apiColumn("cohort"), nil
	}
	key := exprKey(p)
	for i, g := range groups {
		if exprKey(g) != key {
			continue
		}
		if isLabelSlot(i) {
			return c.r.t.apiColumn("cohort"), nil
		}
		if i == valueSlot {
			return c.r.t.apiColumn("value"), nil
		}
	}
	byName := map[string]jnode{}
	var order []string
	allSame := true
	var first string
	for i, m := range mapped {
		s, err := c.subst(p, m.entry)
		if err != nil {
			return nil, err
		}
		if !isConstant(s) {
			return nil, notReport("projection %s is neither grouped by nor constant per dimension", outputName(p))
		}
		k := exprKey(s)
		if prev, ok := byName[names[i]]; ok && exprKey(prev) != k {
			return nil, notReport("projection %s differs within a dimension", outputName(p))
		}
		if _, ok := byName[names[i]]; !ok {
			order = append(order, names[i])
		}
		byName[names[i]] = s
		if first == "" {
			first = k
		} else if k != first {
			allSame = false
		}
	}
	if allSame {
		return cloneJ(byName[order[0]]), nil
	}
	isName := true
	for name, v := range byName {
		if sv, ok := constValue(v).(string); !ok || sv != name {
			isName = false
		}
	}
	if isName {
		return c.r.t.apiColumn("dimension"), nil
	}
	cs := cloneJ(c.r.t.caseOn)
	tmpl := listOf(cs["case_checks"])[0].(jnode)
	var checks []any
	for _, name := range order {
		ch := cloneJ(tmpl)
		w := nodeOf(ch, "when_expr")
		w["left"] = c.r.t.apiColumn("dimension")
		w["right"] = stringConst(name)
		ch["then_expr"] = cloneJ(byName[name])
		checks = append(checks, ch)
	}
	cs["case_checks"] = checks
	cs["else_expr"] = nullConst()
	cs["alias"] = ""
	return cs, nil
}

func (c *countingSelect) mapAggregates(p jnode, grouped bool) (jnode, error) {
	rowsOK := c.coh.distinct()
	var err error
	wrap := func(col string, countLike bool) jnode {
		ref := c.r.t.apiColumn(col)
		if grouped {
			return ref
		}
		m := aggregateOf("max", ref)
		if countLike {
			return coalesceOf(m, intConst(0))
		}
		return m
	}
	var rec func(v any) any
	rec = func(v any) any {
		x, ok := v.(jnode)
		if !ok {
			if l, isList := v.([]any); isList {
				out := make([]any, len(l))
				for i, cv := range l {
					out[i] = rec(cv)
				}
				return out
			}
			return v
		}
		if !reportAggregate(x) {
			if columnParts(x) != nil {
				err = notReport("column %s outside an aggregate", strings.Join(columnParts(x), "."))
				return x
			}
			out := make(jnode, len(x))
			for k, cv := range x {
				out[k] = rec(cv)
			}
			return out
		}
		ch := childrenOf(x)
		fn := strings.ToLower(strOf(x, "function_name"))
		if x["filter"] != nil || len(listOf(nodeOf(x, "order_bys")["orders"])) > 0 {
			err = notReport("an aggregate with FILTER or ORDER BY")
			return x
		}
		switch {
		case fn == "count_star" || fn == "count" && len(ch) == 0:
			if !rowsOK {
				err = notReport("count(*) over a cohort that may repeat members")
				return x
			}
			return wrap("rows", true)
		case fn == "count" && boolTrue(x["distinct"]):
			if len(ch) == 1 && c.isMember(ch[0]) {
				return wrap("people", true)
			}
			if col := c.distinctPair(ch); col != "" {
				return wrap(c.measure(map[string]any{"distinct_with": col})+"_distinct", true)
			}
			err = notReport("a distinct count recognition cannot map")
			return x
		case fn == "count" && len(ch) == 1 && isConstant(ch[0]) && constValue(ch[0]) != nil:
			if !rowsOK {
				err = notReport("count(1) over a cohort that may repeat members")
				return x
			}
			return wrap("rows", true)
		case fn == "count" && len(ch) == 1 && c.isMetricArg(ch[0]):
			if !rowsOK {
				err = notReport("a metric over a cohort that may repeat members")
				return x
			}
			return coalesceOf(wrap("metric_count", true), intConst(0))
		case (fn == "sum" || fn == "avg") && len(ch) == 1 && !boolTrue(x["distinct"]):
			if !rowsOK {
				err = notReport("a %s over a cohort that may repeat members", fn)
				return x
			}
			if c.isMetricArg(ch[0]) {
				return wrap("metric_"+fn, false)
			}
			spec := c.globalMetric(ch[0])
			if spec == nil {
				err = notReport("a %s recognition cannot map", fn)
				return x
			}
			return wrap(c.measure(spec)+"_"+fn, false)
		}
		err = notReport("an aggregate recognition cannot map: %s", fn)
		return x
	}
	out, _ := rec(cloneJ(p)).(jnode)
	if err != nil {
		return nil, err
	}
	out["alias"] = ""
	return out, nil
}

func aggregateOf(name string, args ...jnode) jnode {
	var ch []any
	for _, a := range args {
		ch = append(ch, a)
	}
	return jnode{"class": "FUNCTION", "type": "FUNCTION", "alias": "", "query_location": json.Number("18446744073709551615"),
		"function_name": name, "schema": "", "catalog": "", "children": ch, "filter": nil,
		"order_bys": jnode{"type": "ORDER_MODIFIER", "orders": []any{}}, "distinct": false, "is_operator": false, "export_state": false}
}

// coalesceOf is COALESCE(a, b), which DuckDB has as an operator.
func coalesceOf(a, b jnode) jnode {
	return jnode{"class": "OPERATOR", "type": "OPERATOR_COALESCE", "alias": "", "query_location": json.Number("18446744073709551615"),
		"children": []any{a, b}}
}

func intConst(i int64) jnode {
	return jnode{"class": "CONSTANT", "type": "VALUE_CONSTANT", "alias": "", "query_location": json.Number("18446744073709551615"),
		"value": jnode{"type": jnode{"id": "INTEGER", "type_info": nil}, "is_null": false, "value": json.Number(strconv.FormatInt(i, 10))}}
}

func (c *countingSelect) measure(spec map[string]any) string {
	b, _ := json.Marshal(spec)
	key := string(b)
	if _, ok := c.measures[key]; !ok {
		name := "m" + strconv.Itoa(len(c.order))
		m := map[string]any{"name": name}
		for k, v := range spec {
			m[k] = v
		}
		c.measures[key] = m
		c.order = append(c.order, key)
	}
	return c.measures[key]["name"].(string)
}

// distinctPair reads count(DISTINCT member, column), as
// count(DISTINCT CASE WHEN member IS NOT NULL AND column IS NOT NULL THEN
// (member, column) END) or its CASE WHEN ... IS NULL THEN NULL form.
func (c *countingSelect) distinctPair(ch []jnode) string {
	if len(ch) == 2 && c.isMember(ch[0]) {
		return c.sourceColumn(ch[1])
	}
	if len(ch) != 1 || classOf(ch[0]) != "CASE" {
		return ""
	}
	cs := ch[0]
	checks := listOf(cs["case_checks"])
	var tuple jnode
	guarded := map[string]bool{}
	switch {
	case len(checks) == 1 && isNullConst(nodeOf(cs, "else_expr")):
		// CASE WHEN a IS NOT NULL AND b IS NOT NULL THEN (a, b) END
		w := nodeOf(checks[0], "when_expr")
		tuple = nodeOf(checks[0], "then_expr")
		for _, part := range conjuncts(w) {
			if typeOf(part) != "OPERATOR_IS_NOT_NULL" || len(childrenOf(part)) != 1 {
				return ""
			}
			guarded[exprKey(childrenOf(part)[0])] = true
		}
	case len(checks) == 2:
		// CASE WHEN a IS NULL THEN NULL WHEN b IS NULL THEN NULL ELSE (a, b) END
		tuple = nodeOf(cs, "else_expr")
		for _, chk := range checks {
			w, t := nodeOf(chk, "when_expr"), nodeOf(chk, "then_expr")
			if typeOf(w) != "OPERATOR_IS_NULL" || !isNullConst(t) || len(childrenOf(w)) != 1 {
				return ""
			}
			guarded[exprKey(childrenOf(w)[0])] = true
		}
	default:
		return ""
	}
	if !isFunc(tuple, "row") || len(childrenOf(tuple)) != 2 {
		return ""
	}
	a, b := childrenOf(tuple)[0], childrenOf(tuple)[1]
	if len(guarded) != 2 || !guarded[exprKey(a)] || !guarded[exprKey(b)] || !c.isMember(a) {
		return ""
	}
	return c.sourceColumn(b)
}

func (c *countingSelect) isMetricArg(a jnode) bool {
	if classOf(a) != "CAST" || !doubleType(nodeOf(a, "cast_type")) {
		return false
	}
	return c.structField(stripText(nodeOf(a, "child"))) != ""
}

func (c *countingSelect) globalMetric(a jnode) map[string]any {
	if col := c.sourceColumn(a); col != "" {
		return map[string]any{"column": col}
	}
	if isCoalesce(a) && len(childrenOf(a)) == 2 {
		ch := childrenOf(a)
		if col := c.sourceColumn(ch[0]); col != "" {
			if v, err := recLiteral(ch[1]); err == nil {
				return map[string]any{"column": col, "coalesce": v}
			}
		}
	}
	return nil
}

// total maps a count without GROUP BY over one cohort: one row, 0 if none.
func (c *countingSelect) total() (jnode, string, error) {
	if len(c.coh.cohorts) != 1 || c.entries != nil || c.elemCol != "" {
		return nil, "", notReport("a total over several cohorts or an expansion")
	}
	var filters []reportFilter
	for _, w := range c.where {
		f, err := literalFilter(w, c.filterColumn)
		if err != nil {
			return nil, "", err
		}
		filters = append(filters, f)
	}
	var proj []any
	for _, p := range listOf(c.q["select_list"]) {
		pn := p.(jnode)
		m, err := c.mapAggregates(pn, false)
		if err != nil {
			return nil, "", err
		}
		proj = append(proj, withAlias(m, outputName(pn)))
	}
	spec, err := c.spec([]any{map[string]any{"name": "ALL_ROWS", "constant": "ALL_ROWS"}}, filters, true)
	if err != nil {
		return nil, "", err
	}
	return c.apiSelect(spec, proj, false)
}

// cohortSizes maps count(DISTINCT member) per cohort of the cohorts alone.
func (c *countingSelect) cohortSizes() (jnode, string, error) {
	if len(c.where) > 0 || c.entries != nil || c.elemCol != "" {
		return nil, "", notReport("cohort sizes with filters or expansions")
	}
	groups, err := c.groups()
	if err != nil {
		return nil, "", err
	}
	if len(groups) > 1 || len(groups) == 1 && !c.isLabel(groups[0]) {
		return nil, "", notReport("cohort sizes grouped by more than the cohort")
	}
	if len(groups) == 0 && len(c.coh.cohorts) != 1 {
		return nil, "", notReport("one size of several cohorts")
	}
	grouped := len(groups) == 1
	var proj []any
	for _, p := range listOf(c.q["select_list"]) {
		pn := p.(jnode)
		var m jnode
		switch {
		case c.isLabel(pn):
			m = c.r.t.apiColumn("cohort")
		case isFunc(pn, "count") && boolTrue(pn["distinct"]) && len(childrenOf(pn)) == 1 && c.isMember(childrenOf(pn)[0]):
			m = c.r.t.apiColumn("people")
			if !grouped {
				m = coalesceOf(aggregateOf("max", m), intConst(0))
			}
		default:
			return nil, "", notReport("cohort sizes with column %s", outputName(pn))
		}
		proj = append(proj, withAlias(m, outputName(pn)))
	}
	spec, err := c.spec([]any{}, nil, false)
	if err != nil {
		return nil, "", err
	}
	return c.apiSelect(spec, proj, true)
}

func (c *countingSelect) spec(dims []any, filters []reportFilter, source bool) (map[string]any, error) {
	spec := map[string]any{"version": reportAPIVersion}
	if source {
		src := map[string]any{"table": c.src.table, "member": c.member}
		all := append(append([]reportFilter{}, c.src.filters...), filters...)
		if len(all) > 0 {
			w, err := whereSpec(all)
			if err != nil {
				return nil, err
			}
			src["where"] = w
		}
		spec["source"] = src
	}
	var cohorts []any
	for _, x := range c.coh.cohorts {
		m := map[string]any{"label": x.label}
		for k, v := range x.spec {
			m[k] = v
		}
		cohorts = append(cohorts, m)
	}
	spec["cohorts"] = cohorts
	spec["dimensions"] = dims
	if len(c.order) > 0 {
		var ms []any
		for _, k := range c.order {
			ms = append(ms, c.measures[k])
		}
		spec["metrics"] = ms
	}
	return spec, nil
}

// apiSelect builds the SELECT over the call, its columns cast to the types
// the CTE returned.
func (c *countingSelect) apiSelect(spec map[string]any, proj []any, totalsOnly bool) (jnode, string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(spec); err != nil {
		return nil, "", err
	}
	text := strings.TrimSpace(buf.String())
	types, err := c.r.cteTypes(c.name)
	if err != nil {
		return nil, "", err
	}
	if len(types) != len(proj) {
		return nil, "", notReport("the CTE's columns could not be read")
	}
	for i, p := range proj {
		pn := p.(jnode)
		if !strings.EqualFold(types[i][0], strOf(pn, "alias")) {
			return nil, "", notReport("the CTE's columns differ from its SELECT list")
		}
		if !simpleReportType(types[i][1]) {
			return nil, "", notReport("column %s of type %s", types[i][0], types[i][1])
		}
		alias := strOf(pn, "alias")
		pn["alias"] = ""
		cast, err := c.r.t.castTo(c.r.env, pn, types[i][1])
		if err != nil {
			return nil, "", err
		}
		proj[i] = withAlias(cast, alias)
	}
	sel := cloneJ(c.r.t.call)
	sel["select_list"] = proj
	fn := nodeOf(sel, "from_table", "function")
	arg := childrenOf(fn)[0]
	nodeOf(arg, "value")["value"] = text
	if totalsOnly {
		sel["where_clause"] = nil
	}
	return sel, text, nil
}

func simpleReportType(t string) bool {
	u := strings.ToUpper(t)
	return !strings.Contains(u, "[") && !strings.Contains(u, "STRUCT") && !strings.Contains(u, "MAP") && !strings.Contains(u, "UNION")
}

// cteTypes returns the columns and types a CTE of the WITH returns.
func (r *reportRecognizer) cteTypes(name string) ([][2]string, error) {
	if t, ok := r.described[name]; ok {
		return t, nil
	}
	t, err := r.describeCTE(name)
	if err != nil {
		return nil, err
	}
	if r.described == nil {
		r.described = map[string][][2]string{}
	}
	r.described[name] = t
	return t, nil
}

func (r *reportRecognizer) describeCTE(name string) ([][2]string, error) {
	stmt := cloneJ(r.t.star)
	node := nodeOf(stmt, "node")
	from := nodeOf(node, "from_table")
	from["table_name"] = name
	node["cte_map"] = cloneJ(r.cteMapOriginal())
	text, err := r.env.deparse(stmt)
	if err != nil {
		return nil, notReport("the CTE could not be printed")
	}
	types, err := r.env.describe(strings.TrimSuffix(strings.TrimSpace(text), ";"))
	if err != nil {
		return nil, notReport("the CTE's types could not be read")
	}
	return types, nil
}

// cteMapOriginal returns the WITH as written: the CTEs replaced so far
// read the API, which the description of another must not depend on.
func (r *reportRecognizer) cteMapOriginal() jnode {
	if r.original == nil {
		r.original = cloneJ(r.cteMap)
	}
	return r.original
}
