package snowflake

import (
	"fmt"
	"strconv"
	"strings"
)

// Snowflake's ML classes (SNOWFLAKE.ML.FORECAST and the like) are objects
// Igloo keeps itself, with statements and methods it carries out itself
// (see docs/ml-functions.md). The translator passes their statements on as
// written, for ParseModelStatement, and turns their methods in queries into
// calls Igloo resolves before DuckDB sees the statement:
//
//	TABLE(m!METHOD(...)), ai_predict_class(...)  → igloo_ml_method('<the call as written>')
//	m!PREDICT(INPUT_DATA => {*})                  → CAST(igloo_ml_predict_ref('m', '*') AS JSON)
//
// igloo_ml_predict_ref's arguments after the model's name say what the
// input object holds: '*' for every column, then names left out
// ('*', 'a' for {* EXCLUDE a}); '*:t' for t's columns ({t.*}); or '=' then
// key and value pairs ({'a': x}, OBJECT_CONSTRUCT('a', x)).

// ModelClasses are the classes Igloo has, by their names after
// SNOWFLAKE.ML.
var ModelClasses = []string{"FORECAST", "ANOMALY_DETECTION", "CLASSIFICATION", "TOP_INSIGHTS"}

// ModelStatement is a statement on Snowflake's ML classes or their
// instances, models.
type ModelStatement struct {
	// Verb is CREATE, DROP, SHOW, ALTER, DESCRIBE, GRANT, REVOKE or CALL.
	Verb string
	// Class is the class after SNOWFLAKE.ML., upper-cased; a CALL has none.
	Class string
	// Name is the model's name, its parts as written: unquoted ones
	// lower-cased, quoted ones as they are.
	Name []string
	// Replace, IfNotExists and IfExists are the statement's clauses.
	Replace, IfNotExists, IfExists bool
	// Call is CALL's call, or CREATE's arguments as a call of the class.
	Call *ModelCall
	// Comment is CREATE's COMMENT, Tags whether it had a TAG clause.
	Comment *string
	Tags    bool
	// Like and In are SHOW's LIKE pattern and IN clause: ACCOUNT,
	// DATABASE or SCHEMA, with a name or not.
	Like   *string
	In     string
	InName []string
	// Privilege is what GRANT and REVOKE give: USAGE, MLADMIN, MLCONSUMER
	// (an instance role) or CREATE; Grantee who to, a role unless
	// GranteeUser.
	Privilege   string
	Grantee     string
	GranteeUser bool
}

// ModelCall is a call of a model's method, m!METHOD(...), or of a
// function such as ai_predict_class(...) (Name empty).
type ModelCall struct {
	Name   []string
	Method string
	Args   []ModelArg
	// Text is the call as written.
	Text string
}

// ModelArg is a named argument; Name is upper-cased, or empty for one
// passed by position.
type ModelArg struct {
	Name  string
	Value ModelValue
}

// Arg returns the argument named name (upper case), or nil.
func (c *ModelCall) Arg(name string) *ModelValue {
	for i := range c.Args {
		if c.Args[i].Name == name {
			return &c.Args[i].Value
		}
	}
	return nil
}

// ValueKind is what a ModelValue holds.
type ValueKind int

const (
	ValueNull ValueKind = iota
	ValueString
	ValueNumber
	ValueBool
	ValueObject
	ValueArray
	// ValueTable names a table or view: TABLE(name) or
	// SYSTEM$REFERENCE('TABLE', 'name').
	ValueTable
	// ValueQuery is a query: SYSTEM$QUERY_REFERENCE('...') or
	// TABLE(SELECT ...), in Snowflake's SQL.
	ValueQuery
)

// ModelValue is an argument's value: a literal, an object or array of
// them, or a reference to the rows of a table, view or query.
type ModelValue struct {
	Kind ValueKind
	// Str is a string, or a query's text; Num a number.
	Str  string
	Num  float64
	Bool bool
	// Keys and Elems are an object's keys and values, in order, or an
	// array's elements (Elems alone).
	Keys  []string
	Elems []ModelValue
	// Table is a table's name, its parts as for ModelStatement.Name.
	Table []string
}

// Field returns an object's value at key (case-insensitively), or nil.
func (v *ModelValue) Field(key string) *ModelValue {
	if v == nil || v.Kind != ValueObject {
		return nil
	}
	for i, k := range v.Keys {
		if strings.EqualFold(k, key) {
			return &v.Elems[i]
		}
	}
	return nil
}

// ParseModelStatement recognizes a statement on Snowflake's ML classes:
// CREATE, DROP, SHOW, ALTER, DESCRIBE, GRANT and REVOKE of SNOWFLAKE.ML.x
// and CALL of a model's method. ok is false for any other statement; err
// reports one that cannot be read.
func ParseModelStatement(sql string) (st *ModelStatement, ok bool, err error) {
	toks, err := lex(sql)
	if err != nil {
		return nil, false, nil
	}
	nodes, err := build(toks)
	if err != nil {
		return nil, false, nil
	}
	if !isModelStatement(nodes) {
		return nil, false, nil
	}
	p := &modelParser{src: sql, nodes: nodes, idx: sig(nodes)}
	st, err = p.statement()
	return st, true, err
}

// ParseModelCall reads a call of a model's method or of ai_predict_class
// or ai_predict_value, as igloo_ml_method's argument holds it.
func ParseModelCall(text string) (*ModelCall, error) {
	toks, err := lex(text)
	if err != nil {
		return nil, err
	}
	nodes, err := build(toks)
	if err != nil {
		return nil, err
	}
	p := &modelParser{src: text, nodes: nodes, idx: sig(nodes)}
	c, err := p.call()
	if err != nil {
		return nil, err
	}
	if !p.done() {
		return nil, p.errorf("unexpected text after %s", c.Text)
	}
	return c, nil
}

// isModelStatement reports whether nodes are a statement ParseModelStatement
// reads.
func isModelStatement(nodes []*node) bool {
	idx := sig(nodes)
	w := func(k int) *node {
		if k < len(idx) {
			return nodes[idx[k]]
		}
		return nil
	}
	mlAt := func(k int) bool {
		return w(k).isWord("SNOWFLAKE") && w(k+1).isOp(".") && w(k+2).isWord("ML") && w(k+3).isOp(".")
	}
	switch w(0).word() {
	case "CREATE":
		if w(1).isWord("OR") && w(2).isWord("REPLACE") {
			return mlAt(3)
		}
		return mlAt(1)
	case "DROP", "SHOW", "ALTER", "DESC", "DESCRIBE":
		return mlAt(1)
	case "GRANT", "REVOKE":
		for k := 1; k < len(idx); k++ {
			if mlAt(k) {
				return true
			}
		}
	case "CALL":
		k := 1
		for {
			if _, _, ok := w(k).name(); !ok {
				return false
			}
			if !w(k + 1).isOp(".") {
				return w(k + 1).isOp("!")
			}
			k += 2
		}
	}
	return false
}

// modelParser reads a model statement's significant nodes in order.
type modelParser struct {
	src   string
	nodes []*node
	idx   []int
	at    int
}

func (p *modelParser) peek(k int) *node {
	if p.at+k < len(p.idx) {
		return p.nodes[p.idx[p.at+k]]
	}
	return nil
}

func (p *modelParser) done() bool {
	if n := p.peek(0); n != nil && n.isOp(";") {
		p.at++
	}
	return p.at >= len(p.idx)
}

func (p *modelParser) errorf(format string, args ...any) *Error {
	return &Error{Code: CodeSyntax, Message: fmt.Sprintf(format, args...)}
}

// words consumes the words given if they come next.
func (p *modelParser) words(ws ...string) bool {
	for k, w := range ws {
		if !p.peek(k).isWord(w) {
			return false
		}
	}
	p.at += len(ws)
	return true
}

// class reads SNOWFLAKE.ML.<class>.
func (p *modelParser) class() (string, error) {
	if !(p.peek(0).isWord("SNOWFLAKE") && p.peek(1).isOp(".") && p.peek(2).isWord("ML") && p.peek(3).isOp(".")) {
		return "", p.errorf("expected SNOWFLAKE.ML.<class>")
	}
	c := p.peek(4).word()
	if c == "" {
		return "", p.errorf("expected a class after SNOWFLAKE.ML.")
	}
	p.at += 5
	return c, nil
}

// name reads an identifier chain.
func (p *modelParser) name() ([]string, error) {
	var parts []string
	for {
		n := p.peek(0)
		s, quoted, ok := n.name()
		if !ok {
			return nil, p.errorf("expected a name")
		}
		if !quoted {
			s = strings.ToLower(s)
		}
		parts = append(parts, s)
		p.at++
		if !p.peek(0).isOp(".") {
			return parts, nil
		}
		p.at++
	}
}

func (p *modelParser) statement() (*ModelStatement, error) {
	st := &ModelStatement{Verb: p.peek(0).word()}
	if st.Verb == "DESC" {
		st.Verb = "DESCRIBE"
	}
	p.at++
	var err error
	switch st.Verb {
	case "CREATE":
		st.Replace = p.words("OR", "REPLACE")
		if st.Class, err = p.class(); err != nil {
			return nil, err
		}
		st.IfNotExists = p.words("IF", "NOT", "EXISTS")
		if st.Name, err = p.name(); err != nil {
			return nil, err
		}
		g := p.peek(0)
		if !g.isGroup('(') {
			return nil, p.errorf("expected the arguments of CREATE SNOWFLAKE.ML.%s %s(...)", st.Class, strings.Join(st.Name, "."))
		}
		p.at++
		args, err := p.args(g)
		if err != nil {
			return nil, err
		}
		st.Call = &ModelCall{Name: st.Name, Method: st.Class, Args: args, Text: source(p.src, p.nodes[p.idx[0]:p.idx[p.at-1]+1])}
		for !p.done() {
			switch {
			case p.words("WITH", "TAG"), p.words("TAG"):
				if !p.peek(0).isGroup('(') {
					return nil, p.errorf("expected TAG (name = 'value', ...)")
				}
				p.at++
				st.Tags = true
			case p.words("COMMENT"):
				if p.peek(0).isOp("=") {
					p.at++
				}
				s, ok := p.peek(0).stringValue()
				if !ok {
					return nil, p.errorf("COMMENT takes a string")
				}
				p.at++
				st.Comment = &s
			default:
				return nil, p.errorf("unexpected text after CREATE SNOWFLAKE.ML.%s %s(...)", st.Class, strings.Join(st.Name, "."))
			}
		}
		return st, nil
	case "DROP", "ALTER", "DESCRIBE":
		if st.Class, err = p.class(); err != nil {
			return nil, err
		}
		if st.Verb == "DROP" {
			st.IfExists = p.words("IF", "EXISTS")
		}
		if st.Name, err = p.name(); err != nil {
			return nil, err
		}
		if st.Verb == "ALTER" {
			return st, nil // refused by the caller, whatever follows
		}
		if !p.done() {
			return nil, p.errorf("unexpected text after %s SNOWFLAKE.ML.%s %s", st.Verb, st.Class, strings.Join(st.Name, "."))
		}
		return st, nil
	case "SHOW":
		if st.Class, err = p.class(); err != nil {
			return nil, err
		}
		p.words("INSTANCES")
		if p.words("LIKE") {
			s, ok := p.peek(0).stringValue()
			if !ok {
				return nil, p.errorf("LIKE takes a string")
			}
			p.at++
			st.Like = &s
		}
		if p.words("IN") {
			switch {
			case p.words("ACCOUNT"):
				st.In = "ACCOUNT"
			case p.words("DATABASE"):
				st.In = "DATABASE"
			case p.words("SCHEMA"):
				st.In = "SCHEMA"
			default:
				st.In = "SCHEMA"
			}
			if st.In != "ACCOUNT" && !p.done() {
				if st.InName, err = p.name(); err != nil {
					return nil, err
				}
			}
		}
		if !p.done() {
			return nil, p.errorf("unexpected text after SHOW SNOWFLAKE.ML.%s", st.Class)
		}
		return st, nil
	case "GRANT", "REVOKE":
		return p.grant(st)
	case "CALL":
		c, err := p.call()
		if err != nil {
			return nil, err
		}
		if c.Name == nil {
			return nil, p.errorf("expected CALL model!METHOD(...)")
		}
		st.Call, st.Name = c, c.Name
		if !p.done() {
			return nil, p.errorf("unexpected text after CALL %s", c.Text)
		}
		return st, nil
	}
	return nil, p.errorf("unexpected %s", st.Verb)
}

// grant reads the rest of
//
//	GRANT USAGE ON SNOWFLAKE.ML.<class> name TO [ROLE | USER] grantee
//	GRANT SNOWFLAKE.ML.<class> ROLE name!MLADMIN | MLCONSUMER TO [ROLE | USER] grantee
//	GRANT CREATE SNOWFLAKE.ML.<class> ON SCHEMA name TO ...
//
// and REVOKE's with FROM.
func (p *modelParser) grant(st *ModelStatement) (*ModelStatement, error) {
	var err error
	to := "TO"
	if st.Verb == "REVOKE" {
		to = "FROM"
	}
	switch {
	case p.words("USAGE", "ON"):
		st.Privilege = "USAGE"
		if st.Class, err = p.class(); err != nil {
			return nil, err
		}
		if st.Name, err = p.name(); err != nil {
			return nil, err
		}
	case p.words("CREATE"):
		st.Privilege = "CREATE"
		if st.Class, err = p.class(); err != nil {
			return nil, err
		}
		if !p.words("ON", "SCHEMA") {
			return nil, p.errorf("expected ON SCHEMA after %s CREATE SNOWFLAKE.ML.%s", st.Verb, st.Class)
		}
		if st.Name, err = p.name(); err != nil {
			return nil, err
		}
	default:
		if st.Class, err = p.class(); err != nil {
			return nil, err
		}
		if !p.words("ROLE") {
			return nil, p.errorf("expected %s USAGE ON SNOWFLAKE.ML.%s name or %s SNOWFLAKE.ML.%s ROLE name!MLCONSUMER",
				st.Verb, st.Class, st.Verb, st.Class)
		}
		if st.Name, err = p.name(); err != nil {
			return nil, err
		}
		if !p.peek(0).isOp("!") || p.peek(1).word() == "" {
			return nil, p.errorf("expected the instance role, as in %s!MLCONSUMER", strings.Join(st.Name, "."))
		}
		st.Privilege = p.peek(1).word()
		p.at += 2
	}
	if !p.words(to) {
		return nil, p.errorf("expected %s ROLE role or %s USER user", to, to)
	}
	switch {
	case p.words("USER"):
		st.GranteeUser = true
	case p.words("ROLE"):
	}
	g, err := p.name()
	if err != nil || len(g) != 1 {
		return nil, p.errorf("expected the role or user to %s", strings.ToLower(st.Verb))
	}
	st.Grantee = g[0]
	if !p.done() {
		return nil, p.errorf("unexpected text after %s ... %s", st.Verb, st.Grantee)
	}
	return st, nil
}

// call reads name!METHOD(args), or function(args).
func (p *modelParser) call() (*ModelCall, error) {
	start := p.at
	name, err := p.name()
	if err != nil {
		return nil, err
	}
	c := &ModelCall{}
	if p.peek(0).isOp("!") {
		c.Name = name
		c.Method = p.peek(1).word()
		if c.Method == "" {
			return nil, p.errorf("expected a method after %s!", strings.Join(name, "."))
		}
		p.at += 2
	} else if len(name) == 1 {
		c.Method = strings.ToUpper(name[0])
	} else {
		return nil, p.errorf("expected model!METHOD(...)")
	}
	g := p.peek(0)
	if !g.isGroup('(') {
		return nil, p.errorf("expected the arguments of %s", source(p.src, p.nodes[p.idx[start]:p.idx[p.at-1]+1]))
	}
	p.at++
	if c.Args, err = p.args(g); err != nil {
		return nil, err
	}
	c.Text = source(p.src, p.nodes[p.idx[start]:p.idx[p.at-1]+1])
	return c, nil
}

// args reads a group's arguments.
func (p *modelParser) args(g *node) ([]ModelArg, error) {
	var out []ModelArg
	for _, a := range splitArgs(g.kids) {
		a = trim(a)
		ix := sig(a)
		if len(ix) == 0 {
			return nil, p.errorf("an argument is empty")
		}
		var arg ModelArg
		if len(ix) >= 2 && a[ix[0]].kind == kWord && a[ix[1]].isOp("=>") {
			arg.Name = strings.ToUpper(a[ix[0]].tok.text)
			a = a[ix[1]+1:]
		}
		v, err := p.value(trim(a))
		if err != nil {
			if arg.Name != "" {
				err.Message = arg.Name + ": " + err.Message
			}
			return nil, err
		}
		arg.Value = v
		out = append(out, arg)
	}
	return out, nil
}

// value reads a literal argument.
func (p *modelParser) value(a []*node) (ModelValue, *Error) {
	ix := sig(a)
	if len(ix) == 0 {
		return ModelValue{}, p.errorf("expected a value")
	}
	// A cast, as in 'Series A'::VARIANT, changes nothing here.
	for k := 1; k < len(ix); k++ {
		if a[ix[k]].isOp("::") {
			return p.value(a[:ix[k]])
		}
	}
	first := a[ix[0]]
	if len(ix) == 2 && first.isOp("-") && a[ix[1]].kind == kNumber {
		v, err := p.value(a[ix[1]:])
		v.Num = -v.Num
		return v, err
	}
	if len(ix) == 1 {
		switch n := first; {
		case n.kind == kString:
			s, ok := n.stringValue()
			if !ok {
				return ModelValue{}, p.errorf("expected a string")
			}
			return ModelValue{Kind: ValueString, Str: s}, nil
		case n.kind == kNumber:
			f, err := strconv.ParseFloat(n.tok.text, 64)
			if err != nil {
				return ModelValue{}, p.errorf("bad number %s", n.tok.text)
			}
			return ModelValue{Kind: ValueNumber, Num: f}, nil
		case n.isWord("TRUE"), n.isWord("FALSE"):
			return ModelValue{Kind: ValueBool, Bool: n.isWord("TRUE")}, nil
		case n.isWord("NULL"):
			return ModelValue{}, nil
		case n.isGroup('{') && isStar(trim(n.kids)):
			return ModelValue{}, p.errorf("{*} is a row's columns, for PREDICT in a query: SELECT m!PREDICT(INPUT_DATA => {*}) FROM t")
		case n.isGroup('{'):
			obj := ModelValue{Kind: ValueObject}
			for _, pair := range splitArgs(n.kids) {
				pair = trim(pair)
				colon := -1
				for k, m := range pair {
					if m.isOp(":") {
						colon = k
						break
					}
				}
				if colon < 0 {
					return ModelValue{}, p.errorf("expected 'key': value in an object")
				}
				key, ok := operand(pair[:colon]).stringValue()
				if !ok {
					return ModelValue{}, p.errorf("an object's keys are strings")
				}
				v, err := p.value(trim(pair[colon+1:]))
				if err != nil {
					return ModelValue{}, err
				}
				obj.Keys, obj.Elems = append(obj.Keys, key), append(obj.Elems, v)
			}
			return obj, nil
		case n.isGroup('['):
			return p.array(n.kids)
		}
	}
	if len(ix) == 2 && a[ix[1]].isGroup('(') {
		g := a[ix[1]]
		switch first.word() {
		case "ARRAY_CONSTRUCT", "ARRAY":
			return p.array(g.kids)
		case "OBJECT_CONSTRUCT", "OBJECT_CONSTRUCT_KEEP_NULL":
			args := splitArgs(g.kids)
			if len(args)%2 != 0 {
				return ModelValue{}, p.errorf("OBJECT_CONSTRUCT takes keys and values")
			}
			obj := ModelValue{Kind: ValueObject}
			for k := 0; k < len(args); k += 2 {
				key, ok := operand(args[k]).stringValue()
				if !ok {
					return ModelValue{}, p.errorf("an object's keys are strings")
				}
				v, err := p.value(trim(args[k+1]))
				if err != nil {
					return ModelValue{}, err
				}
				obj.Keys, obj.Elems = append(obj.Keys, key), append(obj.Elems, v)
			}
			return obj, nil
		case "TABLE":
			return p.tableRef(g)
		case "SYSTEM$REFERENCE":
			args := splitArgs(g.kids)
			if len(args) < 2 {
				return ModelValue{}, p.errorf("SYSTEM$REFERENCE takes the object's kind and name")
			}
			kind, ok1 := operand(args[0]).stringValue()
			name, ok2 := operand(args[1]).stringValue()
			if !ok1 || !ok2 {
				return ModelValue{}, p.errorf("SYSTEM$REFERENCE's kind and name are strings")
			}
			switch strings.ToUpper(kind) {
			case "TABLE", "VIEW", "MATERIALIZED_VIEW", "MATERIALIZED VIEW", "EXTERNAL_TABLE", "EXTERNAL TABLE", "DYNAMIC_TABLE":
			default:
				return ModelValue{}, p.errorf("SYSTEM$REFERENCE of a %s is not supported; reference a table or view", kind)
			}
			parts, err := identifierParts(name)
			if err != nil {
				return ModelValue{}, p.errorf("SYSTEM$REFERENCE's name %q: %s", name, err.Message)
			}
			return ModelValue{Kind: ValueTable, Table: parts}, nil
		case "SYSTEM$QUERY_REFERENCE":
			args := splitArgs(g.kids)
			if len(args) < 1 {
				return ModelValue{}, p.errorf("SYSTEM$QUERY_REFERENCE takes a query")
			}
			q, ok := operand(args[0]).stringValue()
			if !ok {
				return ModelValue{}, p.errorf("SYSTEM$QUERY_REFERENCE's query is a string")
			}
			return ModelValue{Kind: ValueQuery, Str: q}, nil
		}
	}
	return ModelValue{}, p.errorf("expected a literal, an object or a reference to a table, view or query, not %s", source(p.src, a))
}

func (p *modelParser) array(kids []*node) (ModelValue, *Error) {
	arr := ModelValue{Kind: ValueArray}
	for _, e := range splitArgs(kids) {
		v, err := p.value(trim(e))
		if err != nil {
			return ModelValue{}, err
		}
		arr.Elems = append(arr.Elems, v)
	}
	return arr, nil
}

// tableRef reads TABLE(name) or TABLE(query).
func (p *modelParser) tableRef(g *node) (ModelValue, *Error) {
	kids := trim(g.kids)
	ix := sig(kids)
	if len(ix) == 0 {
		return ModelValue{}, p.errorf("TABLE() needs a table, view or query")
	}
	if len(ix) == 1 && kids[ix[0]].isGroup('(') {
		return p.tableRef(kids[ix[0]])
	}
	if w := kids[ix[0]].word(); w == "SELECT" || w == "WITH" || w == "FROM" {
		return ModelValue{Kind: ValueQuery, Str: source(p.src, kids)}, nil
	}
	if len(ix) == 1 && kids[ix[0]].kind == kString {
		// TABLE('name'), as Snowflake's TABLE takes a name in a string.
		s, _ := kids[ix[0]].stringValue()
		parts, err := identifierParts(s)
		if err != nil {
			return ModelValue{}, err
		}
		return ModelValue{Kind: ValueTable, Table: parts}, nil
	}
	sub := &modelParser{src: p.src, nodes: kids, idx: ix}
	parts, err := sub.name()
	if err != nil || !sub.done() {
		return ModelValue{}, p.errorf("TABLE() needs a table or view's name, or a query")
	}
	return ModelValue{Kind: ValueTable, Table: parts}, nil
}

// identifierParts splits a name written in a string, as SYSTEM$REFERENCE
// takes it, into its parts.
func identifierParts(s string) ([]string, *Error) {
	toks, err := lex(s)
	if err != nil {
		return nil, &Error{Code: CodeSyntax, Message: err.Error()}
	}
	nodes, err := build(toks)
	if err != nil {
		return nil, &Error{Code: CodeSyntax, Message: err.Error()}
	}
	p := &modelParser{src: s, nodes: nodes, idx: sig(nodes)}
	parts, perr := p.name()
	if perr != nil || !p.done() {
		return nil, &Error{Code: CodeSyntax, Message: "not a name"}
	}
	return parts, nil
}

// --- translation -----------------------------------------------------------

// modelFunctions are the table functions Igloo computes like a model's
// methods: Databricks' train-and-score calls.
var modelFunctions = map[string]bool{"AI_PREDICT_CLASS": true, "AI_PREDICT_VALUE": true}

// modelMethods turns the model methods and model functions among nodes
// into the calls Igloo resolves (see the top of this file). It runs before
// a list's groups are translated, since their arguments are not SQL
// DuckDB could read.
func (t *translator) modelMethods(nodes []*node) ([]*node, error) {
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]
		next := nextSig(nodes, i)
		switch {
		case n.isWord("TABLE") && next >= 0 && nodes[next].isGroup('(') && t.methodCall(nodes[next].kids):
			call := source(t.src, trim(nodes[next].kids))
			nodes = replace(nodes, i, next+1, seq(nodes[i:next+1], raw("igloo_ml_method("+quoteString(call)+")")))
		case modelFunctions[n.word()] && next >= 0 && nodes[next].isGroup('(') && !isQualified(nodes, i):
			call := source(t.src, nodes[i:next+1])
			nodes = replace(nodes, i, next+1, seq(nodes[i:next+1], raw("igloo_ml_method("+quoteString(call)+")")))
		case n.isOp("!") && next >= 0 && nodes[next].kind == kWord:
			p := prevSig(nodes, i)
			if p < 0 {
				continue
			}
			if _, _, ok := nodes[p].name(); !ok {
				continue
			}
			start := qualifiedStart(nodes, p)
			args := nextSig(nodes, next)
			if args < 0 || !nodes[args].isGroup('(') {
				return nil, unsupported("model methods without arguments", "")
			}
			method := nodes[next].word()
			if method != "PREDICT" {
				return nil, unsupported("calling "+method+" in an expression",
					"call it with CALL model!"+method+"(...) or SELECT * FROM TABLE(model!"+method+"(...))")
			}
			repl, err := t.predictRef(nodes[start:i], nodes[args])
			if err != nil {
				return nil, err
			}
			nodes = replace(nodes, start, args+1, seq(nodes[start:args+1], repl))
			i = start
		}
	}
	return nodes, nil
}

// isQualified reports whether the name at nodes[i] follows a dot.
func isQualified(nodes []*node, i int) bool {
	p := prevSig(nodes, i)
	return p >= 0 && nodes[p].isOp(".")
}

// methodCall reports whether nodes are just name!METHOD(...).
func (t *translator) methodCall(nodes []*node) bool {
	ix := sig(nodes)
	k := 0
	for {
		if k >= len(ix) {
			return false
		}
		if _, _, ok := nodes[ix[k]].name(); !ok {
			return false
		}
		if k+1 < len(ix) && nodes[ix[k+1]].isOp(".") {
			k += 2
			continue
		}
		break
	}
	return k+3 == len(ix)-1 && nodes[ix[k+1]].isOp("!") && nodes[ix[k+2]].kind == kWord && nodes[ix[k+3]].isGroup('(')
}

// predictRef translates model!PREDICT(INPUT_DATA => object [, CONFIG_OBJECT
// => ...]) to igloo_ml_predict_ref, whose result is the prediction as
// JSON.
func (t *translator) predictRef(name []*node, args *node) (*node, error) {
	modelName := source(t.src, name)
	var input []*node
	for k, a := range splitArgs(args.kids) {
		a = trim(a)
		ix := sig(a)
		if len(ix) >= 2 && a[ix[0]].kind == kWord && a[ix[1]].isOp("=>") {
			switch a[ix[0]].word() {
			case "INPUT_DATA":
				input = trim(a[ix[1]+1:])
			case "CONFIG_OBJECT":
				// on_error is all it holds; errors always abort.
			default:
				return nil, &Error{Code: CodeSyntax, Message: "PREDICT takes INPUT_DATA and CONFIG_OBJECT, not " + a[ix[0]].word()}
			}
			continue
		}
		if k == 0 {
			input = a
			continue
		}
		return nil, &Error{Code: CodeSyntax, Message: "name PREDICT's arguments: INPUT_DATA => {*}"}
	}
	if input == nil {
		return nil, &Error{Code: CodeSyntax, Message: "PREDICT needs INPUT_DATA, such as INPUT_DATA => {*}"}
	}
	out := []*node{raw("CAST(igloo_ml_predict_ref(" + quoteString(modelName))}
	ix := sig(input)
	switch {
	case len(ix) == 1 && input[ix[0]].isGroup('{'):
		spec, err := t.objectSpec(input[ix[0]].kids)
		if err != nil {
			return nil, err
		}
		out = append(out, spec...)
	case len(ix) == 2 && input[ix[0]].word() == "OBJECT_CONSTRUCT" && input[ix[1]].isGroup('('):
		g := input[ix[1]]
		if isStar(trim(g.kids)) {
			out = append(out, raw(", '*'"))
			break
		}
		kv := splitArgs(g.kids)
		if len(kv)%2 != 0 {
			return nil, &Error{Code: CodeSyntax, Message: "OBJECT_CONSTRUCT takes keys and values"}
		}
		out = append(out, raw(", '='"))
		for k := 0; k < len(kv); k += 2 {
			key, ok := operand(kv[k]).stringValue()
			if !ok {
				return nil, unsupported("PREDICT's INPUT_DATA with keys that are not strings", "")
			}
			v, err := t.seq(trim(kv[k+1]), false)
			if err != nil {
				return nil, err
			}
			out = append(out, raw(", "+quoteString(key)+", "), operand(v))
		}
	default:
		return nil, unsupported("PREDICT's INPUT_DATA other than an object of the row's columns",
			"pass INPUT_DATA => {*}, or an object such as {'feature': column, ...}")
	}
	out = append(out, raw(") AS JSON)"))
	n := seq(name, out...)
	n.json = true
	return n, nil
}

// objectSpec translates the inside of an object constant given as
// PREDICT's input.
func (t *translator) objectSpec(kids []*node) ([]*node, error) {
	ix := sig(kids)
	// {*}, {* EXCLUDE a}, {* EXCLUDE (a, b)}, {t.*}
	if len(ix) >= 1 && kids[ix[0]].isOp("*") {
		out := []*node{raw(", '*'")}
		rest := kids[ix[0]+1:]
		rix := sig(rest)
		if len(rix) == 0 {
			return out, nil
		}
		if !rest[rix[0]].isWord("EXCLUDE") || len(rix) < 2 {
			return nil, unsupported("{* "+source(t.src, rest)+"}", "use {*} or {* EXCLUDE (column, ...)}")
		}
		names := rest[rix[1]:]
		if g := trim(names); len(g) == 1 && g[0].isGroup('(') {
			names = g[0].kids
		}
		for _, a := range splitArgs(names) {
			s, _, ok := operand(a).name()
			if !ok {
				return nil, unsupported("{* EXCLUDE "+source(t.src, names)+"}", "list the columns to leave out")
			}
			out = append(out, raw(", "+quoteString(s)))
		}
		return out, nil
	}
	if len(ix) >= 3 && kids[ix[len(ix)-1]].isOp("*") && kids[ix[len(ix)-2]].isOp(".") {
		qual := source(t.src, kids[:ix[len(ix)-2]])
		return []*node{raw(", " + quoteString("*:"+qual))}, nil
	}
	out := []*node{raw(", '='")}
	for _, pair := range splitArgs(kids) {
		pair = trim(pair)
		colon := -1
		for k, m := range pair {
			if m.isOp(":") {
				colon = k
				break
			}
		}
		if colon < 0 {
			return nil, &Error{Code: CodeSyntax, Message: "expected 'key': value in an object constant"}
		}
		key, ok := operand(pair[:colon]).stringValue()
		if !ok {
			return nil, unsupported("PREDICT's INPUT_DATA with keys that are not strings", "")
		}
		v, err := t.seq(trim(pair[colon+1:]), false)
		if err != nil {
			return nil, err
		}
		out = append(out, raw(", "+quoteString(key)+", "), operand(v))
	}
	return out, nil
}
