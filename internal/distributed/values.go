package distributed

import (
	"context"
	"database/sql/driver"
	"fmt"
	"maps"
	"regexp"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
)

// Values the coordinator computes for the stages. A statement Igloo writes
// to answer from its bitmap indexes may filter a large table by a cohort's
// bitmap, which a scalar subquery reads from an index's table at the
// snapshot the statement pinned:
//
//	SELECT ... FROM events AS e WHERE rb_contains(
//	  (SELECT rb_or_agg(bm) FROM igloo_bitmaps.x AT (VERSION => 7) WHERE ...), CAST(e.person AS UINTEGER))
//
// Workers may not read that table: a row access policy of Igloo's own
// guards it, and workers read data files, which no policy guards. So the
// coordinator computes such a subquery once, in the session, before the
// stages run, and the stages read its value from a DuckDB variable,
// CAST(getvariable('__igloo_value_0') AS BLOB), which each task sets
// before it runs. Its own part of the statement keeps the subquery, which
// reads the same snapshot.
//
// A subquery qualifies only when the session vouches for every table it
// reads (ValueEnv) and it reads each at a pinned snapshot, so that the
// value workers get is the one the coordinator's own reads compute; when
// it stands on its own, which binding it alone checks; and when its value
// is of a type that crosses unchanged (valueTypes). The others are planned
// as before, which keeps a statement that reads a table at a snapshot on
// one machine.

// ValueEnv is an Env that can compute a statement's scalar subqueries over
// tables workers may not read, for the stages to read their values.
type ValueEnv interface {
	Env
	// Computes reports whether the coordinator may compute, for workers, a
	// scalar subquery that reads the table a name means (as a statement
	// writes it, one to three parts) at a pinned snapshot: Igloo's own read
	// of a table that workers may not read.
	Computes(ctx context.Context, parts []string) (bool, error)
	// Value runs query, a SELECT of one value, in the session and returns
	// the value as DuckDB's driver gives it.
	Value(ctx context.Context, query string) (any, error)
}

// value is a scalar subquery the coordinator computes for the stages.
type value struct {
	// name is the variable the stages read it from; typ is its DuckDB type.
	name, typ string
	// text is the subquery's SQL; query selects its value as the
	// coordinator computes it: cast to VARCHAR unless it is a BLOB (see
	// variable).
	text, query string
	// sub is the subquery, which the coordinator's own part of the
	// statement reads in place of the variable.
	sub node
}

// variable is a value a task sets as a DuckDB variable before it runs: a
// BLOB's bytes from File, which the coordinator wrote to object storage
// once rather than send them with every task; any other type's value from
// Text, as DuckDB casts it to VARCHAR and back; or NULL.
type variable struct {
	Name string `json:"name"`
	Type string `json:"type"`
	File string `json:"file,omitempty"`
	Text string `json:"text,omitempty"`
	Null bool   `json:"null,omitempty"`
}

// maxValueBytes bounds a computed BLOB: a larger one keeps the statement on
// one machine. A BLOB is written to object storage once, and each task
// reads it once and holds it once: rb_contains folds a constant bitmap
// when it binds. An audience's bitmap at ten times the data took about
// 50 MB. A variable, for tests.
var maxValueBytes = 256 << 20

// maxValueText bounds a value that crosses as text, in every task.
const maxValueText = 1 << 20

// valueTypes are the types a computed value may have: BLOB, and types
// whose values DuckDB casts to VARCHAR and back unchanged.
var valueTypes = map[string]bool{
	"BLOB": true, "BOOLEAN": true, "VARCHAR": true, "DATE": true,
	"TINYINT": true, "SMALLINT": true, "INTEGER": true, "BIGINT": true, "HUGEINT": true,
	"UTINYINT": true, "USMALLINT": true, "UINTEGER": true, "UBIGINT": true,
}

// valuePrefix starts the names of the variables of computed values.
const valuePrefix = "__igloo_value_"

// bitmapVariable matches the names of the session variables that hold the
// cohorts' bitmaps of a report Igloo answers from bitmap indexes and a
// scan (see pgwire's report API): set in the session before the statement
// runs, from index tables at pinned snapshots, each named by a hash of its
// SELECT, so a name always means the same bitmap. No user's statement may
// name the prefix (pgwire's indexReferences), so a call with such a name is
// Igloo's own. The coordinator computes them for the stages as it does
// subqueries' values.
var bitmapVariable = regexp.MustCompile(`^__igloo_bitmap_[0-9a-f]{24}$`)

var valueName = regexp.MustCompile(`^` + valuePrefix + `[0-9]+$`)

// computedValues replaces the scalar subqueries of stmt that the
// coordinator computes (see ValueEnv), and its reads of the session's
// cohort bitmaps (bitmapVariable), with the variables the stages read them
// from, and returns them. A statement that calls getvariable() for any
// other variable keeps them all, so that every variable a stage reads is
// one of these.
func (co *Coordinator) computedValues(ctx context.Context, env Env, stmt node) ([]*value, error) {
	ve, ok := env.(ValueEnv)
	if !ok {
		return nil, nil
	}
	var bitmaps []node
	other := false
	walk(stmt, func(n node) bool {
		if isFunction(n, "getvariable") {
			if bitmapVariable.MatchString(variableName(n)) {
				bitmaps = append(bitmaps, n)
			} else {
				other = true
			}
			return false
		}
		return true
	})
	if other {
		return nil, nil
	}
	var subs []node
	walk(stmt, func(n node) bool {
		if str(n["class"]) == "SUBQUERY" && str(n["subquery_type"]) == "SCALAR" {
			subs = append(subs, n)
			return false
		}
		return true
	})
	var values []*value
	for _, sub := range subs {
		q := subqueryNode(sub)
		ok, err := computable(ctx, ve, q)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		text, err := co.parser.print(q)
		if err != nil {
			continue
		}
		// A subquery that refers to the statement around it does not bind
		// alone.
		cols, err := env.Describe(ctx, text)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil || len(cols) != 1 || !valueTypes[cols[0].Type] {
			continue
		}
		v := &value{name: fmt.Sprintf("%s%d", valuePrefix, len(values)), typ: cols[0].Type, text: text, query: "SELECT (" + text + ")"}
		if v.typ != "BLOB" {
			v.query = "SELECT CAST((" + text + ") AS VARCHAR)"
		}
		repl, err := co.parser.expr(fmt.Sprintf("CAST(getvariable(%s) AS %s)", quoteLiteral(v.name), v.typ))
		if err != nil {
			return nil, err
		}
		if a := str(sub["alias"]); a != "" {
			repl["alias"] = a
		}
		v.sub = clone(sub).(node)
		clear(sub)
		maps.Copy(sub, repl)
		values = append(values, v)
	}
	// The session's cohort bitmaps, a value a name however often read.
	byName := map[string]*value{}
	for _, call := range bitmaps {
		name := variableName(call)
		v := byName[name]
		if v == nil {
			text := "getvariable(" + quoteLiteral(name) + ")"
			v = &value{name: fmt.Sprintf("%s%d", valuePrefix, len(values)), typ: "BLOB", text: text, query: "SELECT " + text,
				sub: clone(call).(node)}
			byName[name] = v
			values = append(values, v)
		}
		repl, err := co.parser.expr(fmt.Sprintf("CAST(getvariable(%s) AS BLOB)", quoteLiteral(v.name)))
		if err != nil {
			return nil, err
		}
		repl["alias"] = call["alias"]
		clear(call)
		maps.Copy(call, repl)
	}
	return values, nil
}

// computable reports whether query node q reads only tables at pinned
// snapshots that env says the coordinator may compute for workers, at
// least one, and takes no parameters, which only the statement binds.
func computable(ctx context.Context, env ValueEnv, q node) (bool, error) {
	ok, found := true, false
	var err error
	walk(q, func(n node) bool {
		if !ok || err != nil {
			return false
		}
		switch str(n["type"]) {
		case "BASE_TABLE":
			if child(n, "at_clause") == nil || child(n, "sample") != nil {
				ok = false
				return false
			}
			ok, err = env.Computes(ctx, tableParts(n))
			found = true
			return false
		case "TABLE_FUNCTION", "PIVOT", "SHOW_REF", "COLUMN_DATA":
			ok = false
			return false
		}
		if str(n["class"]) == "PARAMETER" {
			ok = false
		}
		return ok
	})
	return ok && found && err == nil, err
}

// restoreValues puts the subqueries of values back in place of the
// variables in v, and returns v.
func restoreValues(v any, values []*value) any {
	if len(values) == 0 {
		return v
	}
	subs := map[string]node{}
	for _, val := range values {
		subs[val.name] = val.sub
	}
	return rewrite(v, func(n node) (node, bool) {
		if str(n["class"]) != "CAST" {
			return nil, false
		}
		call, _ := n["child"].(node)
		if !isFunction(call, "getvariable") {
			return nil, false
		}
		sub, ok := subs[variableName(call)]
		if !ok {
			return nil, false
		}
		// Under the name the variable has where planning put it.
		r := clone(sub).(node)
		r["alias"] = n["alias"]
		return r, true
	})
}

// valueNames returns the names of values' variables, for planning.
func valueNames(values []*value) map[string]bool {
	if len(values) == 0 {
		return nil
	}
	names := map[string]bool{}
	for _, v := range values {
		names[v.name] = true
	}
	return names
}

// compute computes the plan's values in the session. A BLOB's bytes are
// written to dir, under the warehouse, for tasks to read.
func (p *Plan) compute(ctx context.Context, store Storage, dir string) ([]variable, error) {
	out := make([]variable, 0, len(p.values))
	for _, v := range p.values {
		got, err := p.valueEnv.Value(ctx, v.query)
		if err != nil {
			return nil, err
		}
		vr := variable{Name: v.name, Type: v.typ}
		switch x := got.(type) {
		case nil:
			vr.Null = true
		case []byte:
			if len(x) > maxValueBytes {
				return nil, unsupported("a subquery's value takes %d bytes, more than workers are given", len(x))
			}
			vr.File = dir + "/" + v.name
			if err := store.Write(ctx, vr.File, x); err != nil {
				return nil, err
			}
		case string:
			if len(x) > maxValueText {
				return nil, unsupported("a subquery's value takes %d bytes as text, more than workers are given", len(x))
			}
			vr.Text = x
		default:
			return nil, fmt.Errorf("the value of %s came back as %T", v.name, got)
		}
		out = append(out, vr)
	}
	return out, nil
}

// computeLater says what Execute must compute before the stages run, for
// EXPLAIN DISTRIBUTED.
func (p *Plan) computeLater() [][2]string {
	var rows [][2]string
	for _, v := range p.values {
		rows = append(rows, [2]string{"computed first", v.name + " " + v.typ + ": (" + v.text + ")"})
	}
	return rows
}

// variablesOf returns the variables a task's SQL reads.
func variablesOf(vars []variable, sql string) []variable {
	var out []variable
	for _, v := range vars {
		if strings.Contains(sql, quoteLiteral(v.Name)) {
			out = append(out, v)
		}
	}
	return out
}

// setVariables sets a task's variables on its connection. Only the
// variables of computed values may be set, of the types those may have.
func (w *Worker) setVariables(ctx context.Context, conn *duckdb.Conn, vars []variable) error {
	for _, v := range vars {
		if !valueName.MatchString(v.Name) || !valueTypes[v.Type] {
			return fmt.Errorf("variable %s of type %s is not one tasks may set", v.Name, v.Type)
		}
		var expr string
		switch {
		case v.Null:
			expr = "CAST(NULL AS " + v.Type + ")"
		case v.File != "":
			if v.Type != "BLOB" || !w.allowed(v.File) {
				return fmt.Errorf("variable %s may not be read from %s", v.Name, v.File)
			}
			expr = "(SELECT content FROM read_blob(" + quoteLiteral(v.File) + "))"
		default:
			expr = "CAST(" + quoteLiteral(v.Text) + " AS " + v.Type + ")"
		}
		if _, err := conn.ExecContext(ctx, "SET VARIABLE "+quoteIdent(v.Name)+" = "+expr, []driver.NamedValue{}); err != nil {
			return fmt.Errorf("set variable %s: %w", v.Name, err)
		}
	}
	return nil
}

// callsFunction reports whether v calls the function name.
func callsFunction(v any, name string) bool {
	found := false
	walk(v, func(n node) bool {
		if isFunction(n, name) {
			found = true
		}
		return !found
	})
	return found
}

// isFunction reports whether n is a call of the function name.
func isFunction(n node, name string) bool {
	return n != nil && str(n["class"]) == "FUNCTION" && n["is_operator"] != true && strings.EqualFold(str(n["function_name"]), name)
}

// variableName returns the name that a call of getvariable() reads, when
// that is a constant.
func variableName(call node) string {
	args := list(call, "children")
	if len(args) != 1 {
		return ""
	}
	a, _ := args[0].(node)
	if str(a["class"]) != "CONSTANT" {
		return ""
	}
	v := child(a, "value")
	if v == nil || v["is_null"] == true || str(child(v, "type")["id"]) != "VARCHAR" {
		return ""
	}
	return str(v["value"])
}
