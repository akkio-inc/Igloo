package pgwire

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Policy bodies. A body is an expression of the policy's arguments that
// may also call these context functions, which Igloo replaces with
// constants for the session it runs in:
//
//	current_user, session_user, current_role   the user's name
//	is_role_in_session(role)                    whether the user has the custom
//	                                            or built-in role role
//	current_available_roles()                   those roles, as a VARCHAR[]
//	tag_value('tag')                            the masked column's value of a tag
//
// Igloo stores bodies with every table name fully qualified, so a session
// cannot make a body read another table (a temporary one, say) by changing
// its search path.

// sessionStateFunctions read state that a session controls, so a policy
// that called them could be steered by the user it is meant to restrict.
var sessionStateFunctions = map[string]bool{
	"getvariable": true, "current_setting": true, "current_query": true, "current_schema": true,
	"current_schemas": true, "current_database": true, "current_catalog": true, "txid_current": true,
}

// contextUserNames are the SQL functions naming the user.
var contextUserNames = map[string]bool{"current_user": true, "session_user": true, "current_role": true}

type holeKind int

const (
	holeArg   holeKind = iota // an argument
	holeUser                  // current_user and the like
	holeRole                  // is_role_in_session(...)
	holeTag                   // tag_value('...')
	holeTable                 // a table the body reads
	holeRoles                 // current_available_roles()
)

// hole is a place in a body that depends on where and for whom it runs.
type hole struct {
	kind       holeKind
	start, end int
	arg        int
	// inner is is_role_in_session's argument.
	inner span
	tag   string
	// table is the table's name as written, alias says whether one
	// follows it.
	table string
	alias bool
}

// bodyTemplate is a policy body split at its holes.
type bodyTemplate struct {
	text  string
	holes []hole
	// functions are the names of the functions the body calls, operators
	// included, lower-cased.
	functions []string
}

// policyContext is who a policy runs for and, for masking policies, the
// masked column's tags.
type policyContext struct {
	user  string
	roles []string
	tags  map[string]string
}

// tableMarker makes the reference to a table a body reads: one that the
// statement's checks can tell apart from the user's own.
type tableMarker func(table string) string

// render fills the template's holes.
func (t *bodyTemplate) render(args []string, pc policyContext, mark tableMarker) string {
	// The text is the body parsed as a query, bodyPrefix + body + ")".
	return t.renderSpan(span{len(bodyPrefix), len(t.text) - 1}, args, pc, mark)
}

func (t *bodyTemplate) renderSpan(s span, args []string, pc policyContext, mark tableMarker) string {
	var b strings.Builder
	pos := s.start
	for _, h := range t.holes {
		if h.start < pos || h.start < s.start || h.end > s.end {
			continue
		}
		b.WriteString(t.text[pos:h.start])
		switch h.kind {
		case holeArg:
			b.WriteString("(" + args[h.arg] + ")")
		case holeUser:
			b.WriteString(quoteLiteral(pc.user))
		case holeRole:
			roles := make([]string, len(pc.roles))
			for i, r := range pc.roles {
				roles[i] = quoteLiteral(r)
			}
			b.WriteString("((" + t.renderSpan(h.inner, args, pc, mark) + ") IN (" + strings.Join(roles, ", ") + "))")
		case holeRoles:
			// A list of strings, written without a function DuckDB would
			// look up; role names need no quoting inside it.
			b.WriteString("CAST(" + quoteLiteral("["+strings.Join(pc.roles, ", ")+"]") + " AS VARCHAR[])")
		case holeTag:
			if v, ok := pc.tags[h.tag]; ok {
				b.WriteString(quoteLiteral(v))
			} else {
				b.WriteString("CAST(NULL AS VARCHAR)")
			}
		case holeTable:
			b.WriteString(mark(h.table))
			if !h.alias {
				b.WriteString(" AS " + quoteIdent(lastPart(h.table)))
			}
		}
		pos = h.end
	}
	b.WriteString(t.text[pos:s.end])
	return b.String()
}

// lastPart returns the last part of a quoted three-part name.
func lastPart(name string) string {
	parts := splitQualified(name)
	if len(parts) == 0 {
		return name
	}
	return parts[len(parts)-1]
}

// bodyPrefix is put before a body to parse it as a query.
const bodyPrefix = "SELECT ("

// parseBody parses a policy body into a template. The body's tables must
// be written fully qualified, as Igloo stores them.
func (r *resolver) parseBody(p policy.Policy) (*bodyTemplate, error) {
	text := bodyPrefix + p.Body + ")"
	ast, err := r.serialize(text)
	if err != nil {
		return nil, fmt.Errorf("policy %s: %w", p.Name, err)
	}
	// One expression and nothing else, so it stays one wherever it is
	// put.
	top, _ := ast.(map[string]any)
	node, _ := top["node"].(map[string]any)
	from, _ := node["from_table"].(map[string]any)
	for k := range node {
		switch k {
		case "type", "select_list", "from_table", "aggregate_handling":
		default:
			return nil, fmt.Errorf("policy %s: the body must be one expression", p.Name)
		}
	}
	if str(node["type"]) != "SELECT_NODE" || len(asList(node["select_list"])) != 1 || str(from["type"]) != "EMPTY" ||
		slices.ContainsFunc(sqllex.Segments(p.Body), func(s sqllex.Segment) bool { return s.Kind == sqllex.Comment }) {
		return nil, fmt.Errorf("policy %s: the body must be one expression, without comments", p.Name)
	}
	t := &bodyTemplate{text: text}
	var walkErr error
	fail := func(format string, args ...any) {
		if walkErr == nil {
			walkErr = fmt.Errorf("policy %s: "+format, append([]any{p.Name}, args...)...)
		}
	}
	var visit func(n any)
	visit = func(n any) {
		switch n := n.(type) {
		case []any:
			for _, c := range n {
				visit(c)
			}
		case map[string]any:
			loc := location(n["query_location"])
			switch str(n["class"]) {
			case "COLUMN_REF":
				names := asList(n["column_names"])
				if len(names) != 1 {
					break
				}
				name := strings.ToLower(str(names[0]))
				if i := slices.IndexFunc(p.Args, func(a policy.Arg) bool { return strings.EqualFold(a.Name, name) }); i >= 0 {
					sp, ok := scanName(text, loc)
					if !ok || len(sp.parts) != 1 {
						fail("cannot place argument %s", name)
						return
					}
					t.holes = append(t.holes, hole{kind: holeArg, start: loc, end: sp.end, arg: i})
					return
				}
				if contextUserNames[name] && loc >= 0 && loc < len(text) && text[loc] != '"' {
					sp, ok := scanName(text, loc)
					if !ok {
						fail("cannot place %s", name)
						return
					}
					t.holes = append(t.holes, hole{kind: holeUser, start: loc, end: sp.end})
					return
				}
			case "FUNCTION":
				name := strings.ToLower(str(n["function_name"]))
				children := asList(n["children"])
				callEnd := func() (int, int) {
					sc := &scanner{s: text, i: loc}
					if parts, ok := sc.name(); !ok || len(parts) != 1 || !strings.EqualFold(parts[0], name) {
						return -1, -1
					}
					sc.skip()
					if sc.i >= len(text) || text[sc.i] != '(' {
						return -1, -1
					}
					return sc.i, matchParen(text, sc.i)
				}
				switch {
				case name == "is_role_in_session" || name == "tag_value" ||
					(contextUserNames[name] || name == "current_available_roles") && len(children) == 0:
					if str(n["schema"]) != "" {
						fail("%s cannot be qualified", name)
						return
					}
					open, end := callEnd()
					if end < 0 {
						fail("cannot place %s", name)
						return
					}
					h := hole{start: loc, end: end + 1}
					switch name {
					case "is_role_in_session":
						if len(children) != 1 {
							fail("is_role_in_session takes one role name")
							return
						}
						h.kind, h.inner = holeRole, span{open + 1, end}
						t.holes = append(t.holes, h)
						visit(children)
					case "tag_value":
						var c, v map[string]any
						if len(children) == 1 {
							c, _ = children[0].(map[string]any)
							v, _ = c["value"].(map[string]any)
						}
						tag, ok := v["value"].(string)
						if len(children) != 1 || str(c["class"]) != "CONSTANT" || !ok {
							fail("tag_value takes a tag's name as a string, as in tag_value('pii')")
							return
						}
						if p.Kind != policy.Masking {
							fail("tag_value is for masking policies")
							return
						}
						h.kind, h.tag = holeTag, tag
						t.holes = append(t.holes, h)
					case "current_available_roles":
						h.kind = holeRoles
						t.holes = append(t.holes, h)
					default:
						h.kind = holeUser
						t.holes = append(t.holes, h)
					}
					return
				case sessionStateFunctions[name]:
					fail("%s depends on the session, which the user controls", name)
					return
				}
				if !slices.Contains(t.functions, name) {
					t.functions = append(t.functions, name)
				}
			case "PARAMETER":
				fail("parameters are not allowed")
				return
			}
			switch str(n["type"]) {
			case "BASE_TABLE":
				sp, ok := scanName(text, loc)
				if !ok || len(sp.parts) != 3 {
					fail("cannot place table %s", str(n["table_name"]))
					return
				}
				if n["at_clause"] != nil {
					fail("AT clauses are not allowed")
					return
				}
				t.holes = append(t.holes, hole{kind: holeTable, start: loc, end: sp.end, table: quoteName(sp.parts),
					alias: str(n["alias"]) != ""})
				return
			case "TABLE_FUNCTION":
				f, _ := n["function"].(map[string]any)
				name := strings.ToLower(str(f["function_name"]))
				if !allowedFunctions[name] {
					fail("%s reads data that policies cannot check", name)
					return
				}
			case "SHOW_REF":
				fail("DESCRIBE, SHOW and SUMMARIZE are not allowed")
				return
			}
			for _, v := range n {
				visit(v)
			}
		}
	}
	visit(ast)
	if walkErr != nil {
		return nil, walkErr
	}
	slices.SortFunc(t.holes, func(a, b hole) int { return a.start - b.start })
	return t, nil
}

// --- the probe ----------------------------------------------------------------

// policyProbe works out, for a policy as it runs for one user, whether it
// is a no-op: a row access policy whose expression is always true, or a
// masking policy that returns its argument unchanged, as DuckDB's
// optimizer folds them. Statements of users a policy exempts that way run
// as written. The probe binds the body on a connection of its own, which
// no user's temporary objects or settings reach, against a relation with
// one column per argument whose values DuckDB cannot know.
type policyProbe struct {
	mu      sync.Mutex
	conn    *duckdb.Conn
	connect func(context.Context) (*duckdb.Conn, error)
	results map[string]bool
	// defaults are the values of policySettings that sessions start with.
	defaults []string
}

// open connects the probe, if it is not connected. pp.mu must be held.
func (pp *policyProbe) open(ctx context.Context) error {
	if pp.conn != nil {
		return nil
	}
	conn, err := pp.connect(ctx)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION explain_output = 'optimized_only'", nil); err != nil {
		_ = conn.Close()
		return err
	}
	pp.conn = conn
	return nil
}

// settings returns the values of policySettings that sessions start
// with.
func (pp *policyProbe) settings(ctx context.Context) ([]string, error) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if pp.defaults != nil {
		return pp.defaults, nil
	}
	if err := pp.open(ctx); err != nil {
		return nil, err
	}
	vals, err := currentSettings(ctx, pp.conn, policySettings)
	if err != nil {
		return nil, err
	}
	pp.defaults = vals
	return vals, nil
}

// currentSettings returns the values of settings on conn, upper-cased,
// with DuckDB's long names of orders shortened.
func currentSettings(ctx context.Context, conn *duckdb.Conn, settings []string) ([]string, error) {
	exprs := make([]string, len(settings))
	for i, name := range settings {
		exprs[i] = "system.main.current_setting(" + quoteLiteral(name) + ")::VARCHAR"
	}
	rows, err := conn.QueryContext(ctx, "SELECT "+strings.Join(exprs, ", "), nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, len(settings))
	if err := rows.Next(dest); err != nil {
		return nil, err
	}
	vals := make([]string, len(settings))
	for i, d := range dest {
		v, _ := d.(string)
		v = strings.ReplaceAll(strings.ToUpper(strings.TrimSpace(v)), " ", "_")
		if short, ok := map[string]string{"ASCENDING": "ASC", "DESCENDING": "DESC"}[v]; ok {
			v = short
		}
		vals[i] = v
	}
	return vals, nil
}

// probeColumns are the columns of the probe relation, one per argument.
var probeColumns = []string{"name", "value", "description", "input_type", "scope"}

// maxPolicyArgs bounds a row access policy's arguments.
var maxPolicyArgs = len(probeColumns)

// probeArgs are the expressions the probe binds a policy's arguments to.
func probeArgs(p policy.Policy) []string {
	args := make([]string, len(p.Args))
	for i, a := range p.Args {
		args[i] = "CAST(" + quoteIdent("__igloo_args") + "." + quoteIdent(fmt.Sprintf("a%d", i)) + " AS " + a.Type + ")"
	}
	return args
}

const probeRelation = `(SELECT name AS a0, value AS a1, description AS a2, input_type AS a3, scope AS a4
	FROM system.main.duckdb_settings()) AS "__igloo_args"`

// noop reports whether body, a policy of kind rendered with probeArgs,
// does nothing: it is always true, or returns its first argument. It also
// checks that the body binds by itself, so it names nothing that the
// statement it is put into could supply.
func (pp *policyProbe) noop(ctx context.Context, kind policy.Kind, arg0, body string) (bool, error) {
	text := "SELECT " + arg0 + ", (" + body + ") FROM " + probeRelation
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if v, ok := pp.results[text]; ok {
		return v, nil
	}
	if err := pp.open(ctx); err != nil {
		return false, err
	}
	exprs, err := optimizedProjection(ctx, pp.conn, text)
	if err != nil {
		return false, err
	}
	noop := false
	if len(exprs) == 2 {
		switch kind {
		case policy.RowAccess:
			noop = exprs[1] == "true"
		case policy.Masking:
			noop = exprs[1] == exprs[0]
		}
	}
	if len(pp.results) > 4096 {
		pp.results = nil
	}
	if pp.results == nil {
		pp.results = map[string]bool{}
	}
	pp.results[text] = noop
	return noop, nil
}

// describe returns the type of a policy body, rendered with probeArgs.
func (pp *policyProbe) describe(ctx context.Context, body string) (string, error) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if err := pp.open(ctx); err != nil {
		return "", err
	}
	rows, err := pp.conn.QueryContext(ctx, "SELECT column_type FROM (DESCRIBE SELECT ("+body+") AS x FROM "+probeRelation+")", nil)
	if err != nil {
		return "", err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return "", err
	}
	typ, _ := dest[0].(string)
	return typ, nil
}

func (pp *policyProbe) close() {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if pp.conn != nil {
		_ = pp.conn.Close()
		pp.conn = nil
	}
}

// optimizedProjection returns the expressions of the top projection of
// text's optimized plan, as DuckDB prints them. The connection's
// explain_output must be optimized_only.
func optimizedProjection(ctx context.Context, conn *duckdb.Conn, text string) ([]string, error) {
	rows, err := conn.QueryContext(ctx, "EXPLAIN (FORMAT JSON) "+text, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, errors.New("no optimized plan")
			}
			return nil, err
		}
		if key, _ := dest[0].(string); key != "logical_opt" {
			continue
		}
		var plan []struct {
			Name      string `json:"name"`
			ExtraInfo struct {
				Expressions any `json:"Expressions"`
			} `json:"extra_info"`
		}
		value, _ := dest[1].(string)
		if err := json.Unmarshal([]byte(value), &plan); err != nil {
			return nil, fmt.Errorf("read plan: %w", err)
		}
		if len(plan) != 1 || plan[0].Name != "PROJECTION" {
			return nil, nil
		}
		var out []string
		switch e := plan[0].ExtraInfo.Expressions.(type) {
		case string:
			out = []string{e}
		case []any:
			for _, x := range e {
				s, _ := x.(string)
				out = append(out, s)
			}
		}
		return out, nil
	}
}
