package cleanroom

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Templates are checked on DuckDB's own parse of them
// (json_serialize_sql), never on their text, and the SQL a run executes is
// written by DuckDB from a parse Igloo changed (json_deserialize_sql).
// What Igloo adds to a template it parses first too, from text of its own
// in which every name is quoted, so nothing a collaborator writes is ever
// pasted into SQL.

// node is a node of a parse, decoded with numbers kept as written.
type node = map[string]any

// Queryer runs a query on a DuckDB connection; *duckdb.Conn is one.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)
}

// Parser parses SQL with DuckDB's parser, on a connection of its own.
type Parser struct {
	mu   sync.Mutex
	conn Queryer
}

// NewParser parses on conn, which it uses for nothing else.
func NewParser(conn Queryer) *Parser { return &Parser{conn: conn} }

func (p *Parser) one(ctx context.Context, query, arg string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	rows, err := p.conn.QueryContext(ctx, query, []driver.NamedValue{{Ordinal: 1, Value: arg}})
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

// parse returns the parse of one statement: its node and parameters.
func (p *Parser) parse(ctx context.Context, sql string) (node, error) {
	raw, err := p.one(ctx, "SELECT system.main.json_serialize_sql($1::VARCHAR)::VARCHAR", sql)
	if err != nil {
		return nil, err
	}
	var doc struct {
		Error        bool   `json:"error"`
		ErrorMessage string `json:"error_message"`
		Statements   []any  `json:"statements"`
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	if doc.Error {
		return nil, invalid("the template does not parse: %s", doc.ErrorMessage)
	}
	if len(doc.Statements) != 1 {
		return nil, invalid("a template is one SELECT statement, not %d statements", len(doc.Statements))
	}
	stmt, _ := doc.Statements[0].(node)
	if stmt == nil {
		return nil, errors.New("unexpected parse")
	}
	return stmt, nil
}

// deparse writes a statement's parse back as SQL.
func (p *Parser) deparse(ctx context.Context, stmt node) (string, error) {
	doc, err := json.Marshal(map[string]any{"error": false, "statements": []any{stmt}})
	if err != nil {
		return "", err
	}
	sql, err := p.one(ctx, "SELECT system.main.json_deserialize_sql($1::JSON)", string(doc))
	if err != nil {
		return "", fmt.Errorf("write the run's SQL: %w", err)
	}
	return sql, nil
}

// expression parses SQL of Igloo's own, SELECT expr, and returns expr's
// node.
func (p *Parser) expression(ctx context.Context, expr string) (node, error) {
	stmt, err := p.parse(ctx, "SELECT "+expr)
	if err != nil {
		return nil, err
	}
	items := arr(obj(stmt["node"])["select_list"])
	if len(items) != 1 {
		return nil, fmt.Errorf("expected one expression in %q", expr)
	}
	return obj(items[0]), nil
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func arr(v any) []any {
	a, _ := v.([]any)
	return a
}

func obj(v any) node {
	n, _ := v.(node)
	return n
}

// integer reads a constant node holding an integer.
func integer(n node) (int64, bool) {
	if str(n["class"]) != "CONSTANT" {
		return 0, false
	}
	v := obj(n["value"])
	switch str(obj(v["type"])["id"]) {
	case "INTEGER", "BIGINT", "HUGEINT", "SMALLINT", "TINYINT", "UBIGINT", "UINTEGER":
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

// clone copies a parse deeply.
func clone(n node) node {
	raw, err := json.Marshal(n)
	if err != nil {
		panic(err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var out node
	if err := dec.Decode(&out); err != nil {
		panic(err)
	}
	return out
}

// canonical is a node's parse without its positions in the text or its
// aliases, and with the names of columns and functions in lower case
// (SQL's, unquoted), so the same expression written twice compares equal.
// Constants keep their case.
func canonical(n node) string {
	var strip func(v any) any
	strip = func(v any) any {
		switch v := v.(type) {
		case node:
			out := make(node, len(v))
			for k, x := range v {
				switch k {
				case "query_location", "alias":
				case "function_name":
					out[k] = strings.ToLower(str(x))
				case "column_names":
					var names []any
					for _, c := range arr(x) {
						names = append(names, strings.ToLower(str(c)))
					}
					out[k] = names
				default:
					out[k] = strip(x)
				}
			}
			return out
		case []any:
			out := make([]any, len(v))
			for i, x := range v {
				out[i] = strip(x)
			}
			return out
		}
		return v
	}
	raw, _ := json.Marshal(strip(n))
	return string(raw)
}

// walk calls f on n and every node under it, parents first. When f returns
// a node, that node replaces the one f was called on, and walk does not go
// under it.
func walk(n node, f func(node) node) node {
	if r := f(n); r != nil {
		return r
	}
	for k, v := range n {
		switch v := v.(type) {
		case node:
			n[k] = walk(v, f)
		case []any:
			for i, x := range v {
				if c, ok := x.(node); ok {
					v[i] = walk(c, f)
				}
			}
		}
	}
	return n
}

// quote quotes a name for SQL.
func quote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
