package distributed

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/duckdb/duckdb-go/v2"
)

// Statements are planned on DuckDB's own parse tree, as json_serialize_sql
// writes it and json_deserialize_sql turns back into SQL. Working on
// DuckDB's parse rather than on SQL text means the fragments and the final
// query Igloo generates name, quote and nest everything exactly as the
// statement did; only the parts Igloo replaces change.

// node is a node of the parse tree: a query node (SELECT_NODE,
// SET_OPERATION_NODE, ...), a table reference (BASE_TABLE, JOIN, SUBQUERY,
// ...) or an expression (class COLUMN_REF, FUNCTION, ...).
type node = map[string]any

// parser parses statements and prints parse trees as SQL with a DuckDB of
// its own: parsing needs no catalog and runs nothing.
type parser struct {
	db *sql.DB

	mu        sync.Mutex
	templates map[string]node
}

func newParser() (*parser, error) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, fmt.Errorf("open parser: %w", err)
	}
	db := sql.OpenDB(connector)
	// Parsing reads no files and needs no settings beyond these.
	for _, stmt := range []string{"SET enable_external_access = false", "SET threads = 1"} {
		if _, err := db.Exec(stmt); err != nil {
			return nil, errors.Join(fmt.Errorf("set up parser: %w", err), db.Close())
		}
	}
	return &parser{db: db, templates: map[string]node{}}, nil
}

func (p *parser) close() error { return p.db.Close() }

// parse returns the parse tree of query, which must hold one SELECT
// statement: the statement's "node".
func (p *parser) parse(query string) (node, error) {
	var raw string
	// No skip_empty: it drops empty grouping sets, as in ROLLUP.
	if err := p.db.QueryRow("SELECT json_serialize_sql($1::VARCHAR)::VARCHAR", query).Scan(&raw); err != nil {
		return nil, err
	}
	v, err := decodeJSON(raw)
	if err != nil {
		return nil, err
	}
	doc, _ := v.(node)
	if doc == nil {
		return nil, errors.New("unexpected parse result")
	}
	if doc["error"] == true {
		return nil, fmt.Errorf("parse: %v", doc["error_message"])
	}
	stmts, _ := doc["statements"].([]any)
	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected one statement, found %d", len(stmts))
	}
	stmt, _ := stmts[0].(node)
	// $1 and ? are listed by number; $name parameters by name.
	for _, e := range list(stmt, "named_param_map") {
		if _, err := strconv.Atoi(str(e.(node)["key"])); err != nil {
			return nil, unsupported("named parameters")
		}
	}
	n, _ := stmt["node"].(node)
	if n == nil {
		return nil, errors.New("statement has no query node")
	}
	return n, nil
}

// print turns a query node back into SQL, and checks that DuckDB parses
// that SQL into the same tree, so a construct DuckDB cannot print
// faithfully is caught before anything runs.
func (p *parser) print(n node) (string, error) {
	doc := node{"error": false, "statements": []any{node{"node": n, "named_param_map": []any{}}}}
	raw, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	var text string
	if err := p.db.QueryRow("SELECT json_deserialize_sql($1::JSON)", string(raw)).Scan(&text); err != nil {
		return "", fmt.Errorf("print query: %w", err)
	}
	back, err := p.parse(text)
	if err != nil {
		return "", fmt.Errorf("reparse generated query: %w", err)
	}
	if canonical(normalize(back)) != canonical(normalize(n)) {
		return "", errors.New("the generated query does not parse back to the planned one")
	}
	return text, nil
}

// normalize undoes the one change DuckDB 1.5 makes to a statement it prints
// and parses again: a VALUES list in FROM comes back wrapped in
// SELECT * FROM (VALUES ...) AS valueslist, once more each time, which
// reads the same rows.
func normalize(v any) any {
	c := clone(v)
	return rewrite(c, func(n node) (node, bool) {
		if str(n["type"]) != "SUBQUERY" || str(n["alias"]) != "valueslist" || len(list(n, "column_name_alias")) > 0 {
			return nil, false
		}
		sel := subqueryNode(n)
		from := child(sel, "from_table")
		items := list(sel, "select_list")
		if str(sel["type"]) != "SELECT_NODE" || str(from["type"]) != "EXPRESSION_LIST" || len(items) != 1 ||
			str(items[0].(node)["class"]) != "STAR" || child(sel, "where_clause") != nil ||
			len(list(sel, "modifiers")) > 0 || len(list(sel, "group_expressions")) > 0 {
			return nil, false
		}
		return normalize(from).(node), true
	})
}

// expr returns a fresh copy of the expression that SQL text parses to, for
// building new nodes exactly as DuckDB's parser would.
func (p *parser) expr(text string) (node, error) {
	n, err := p.template("SELECT " + text)
	if err != nil {
		return nil, err
	}
	items := list(n, "select_list")
	if len(items) != 1 {
		return nil, fmt.Errorf("%q is not one expression", text)
	}
	e, _ := items[0].(node)
	return e, nil
}

// fromRef returns a fresh copy of the table reference in FROM of text,
// which is a whole SELECT statement.
func (p *parser) fromRef(text string) (node, error) {
	n, err := p.template(text)
	if err != nil {
		return nil, err
	}
	ref, _ := n["from_table"].(node)
	if ref == nil {
		return nil, fmt.Errorf("%q has no FROM", text)
	}
	return ref, nil
}

// template parses text once and returns a fresh copy each time.
func (p *parser) template(text string) (node, error) {
	p.mu.Lock()
	n, ok := p.templates[text]
	p.mu.Unlock()
	if !ok {
		var err error
		if n, err = p.parse(text); err != nil {
			return nil, err
		}
		p.mu.Lock()
		if len(p.templates) < 1024 {
			p.templates[text] = n
		}
		p.mu.Unlock()
	}
	return clone(n).(node), nil
}

func decodeJSON(s string) (any, error) {
	d := json.NewDecoder(strings.NewReader(s))
	// Keep numbers as written: positions such as 18446744073709551615 and
	// large constants must survive the round trip exactly.
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// clone deep-copies a parse tree.
func clone(v any) any {
	switch v := v.(type) {
	case node:
		out := make(node, len(v))
		for k, c := range v {
			out[k] = clone(c)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, c := range v {
			out[i] = clone(c)
		}
		return out
	}
	return v
}

// canonical renders a tree without source positions, for comparing trees.
// Map keys come out sorted, so equal trees render equally.
func canonical(v any) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(stripPositions(v))
	return b.String()
}

func stripPositions(v any) any {
	switch v := v.(type) {
	case node:
		out := make(node, len(v))
		for k, c := range v {
			if k != "query_location" {
				out[k] = stripPositions(c)
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, c := range v {
			out[i] = stripPositions(c)
		}
		return out
	}
	return v
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func list(n node, key string) []any {
	if n == nil {
		return nil
	}
	l, _ := n[key].([]any)
	return l
}

func child(n node, key string) node {
	if n == nil {
		return nil
	}
	c, _ := n[key].(node)
	return c
}

// isExpr reports whether n is an expression node.
func isExpr(n node) bool { return n != nil && str(n["class"]) != "" }

// columnNames returns a COLUMN_REF's name parts.
func columnNames(e node) []string {
	var parts []string
	for _, p := range list(e, "column_names") {
		parts = append(parts, str(p))
	}
	return parts
}

// subqueryNode returns the query node of a SUBQUERY expression or table
// reference.
func subqueryNode(n node) node {
	return child(child(n, "subquery"), "node")
}

// intConstant returns the value of an integer constant expression.
func intConstant(e node) (int64, bool) {
	if str(e["class"]) != "CONSTANT" {
		return 0, false
	}
	v := child(e, "value")
	if v == nil || v["is_null"] == true {
		return 0, false
	}
	switch str(child(v, "type")["id"]) {
	case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "HUGEINT":
	default:
		return 0, false
	}
	num, ok := v["value"].(json.Number)
	if !ok {
		return 0, false
	}
	i, err := num.Int64()
	return i, err == nil
}

// walk calls fn for every node under v, depth first, parents first. fn
// returns false to skip a node's children.
func walk(v any, fn func(n node) bool) {
	switch v := v.(type) {
	case node:
		if !fn(v) {
			return
		}
		for _, c := range v {
			walk(c, fn)
		}
	case []any:
		for _, c := range v {
			walk(c, fn)
		}
	}
}

// rewrite replaces every node under v for which fn returns a replacement,
// in place, and returns v (or its replacement). fn sees parents before
// children; a node it replaces is not descended into.
func rewrite(v any, fn func(n node) (node, bool)) any {
	switch t := v.(type) {
	case node:
		if r, ok := fn(t); ok {
			return r
		}
		for k, c := range t {
			t[k] = rewrite(c, fn)
		}
		return t
	case []any:
		for i, c := range t {
			t[i] = rewrite(c, fn)
		}
		return t
	}
	return v
}

// quoteIdent quotes a SQL identifier.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// quoteLiteral quotes a SQL string literal.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// errUnsupported marks a statement that cannot run distributed, which then
// runs on one machine as usual.
type errUnsupported struct{ reason string }

func (e *errUnsupported) Error() string { return "cannot distribute: " + e.reason }

func unsupported(format string, args ...any) error {
	return &errUnsupported{reason: fmt.Sprintf(format, args...)}
}

// IsUnsupported reports whether err only says that a statement cannot run
// distributed.
func IsUnsupported(err error) bool {
	var u *errUnsupported
	return errors.As(err, &u)
}
