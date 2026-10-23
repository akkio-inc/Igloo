package snowflake

import (
	"regexp"
	"strconv"
	"strings"
)

// seq translates a list of nodes: the groups in it first, then the
// constructs at its own level. top says the list is a whole statement.
func (t *translator) seq(nodes []*node, top bool) ([]*node, error) {
	for _, n := range nodes {
		if n.kind == kGroup {
			kids, err := t.seq(n.kids, false)
			if err != nil {
				return nil, err
			}
			n.kids = kids
		}
	}
	var err error
	for _, pass := range []func([]*node) ([]*node, error){
		t.clauses, t.literals, t.calls, t.tableFunctions, t.sample, t.values, t.paths, t.casts, t.variantModes,
		t.operators, t.pivots, t.orderBy, t.top, t.recursiveCTE, t.structWhere,
	} {
		if nodes, err = pass(nodes); err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

// replace returns nodes with nodes[from:to] replaced by n.
func replace(nodes []*node, from, to int, n ...*node) []*node {
	out := make([]*node, 0, len(nodes)-(to-from)+len(n))
	out = append(out, nodes[:from]...)
	out = append(out, n...)
	return append(out, nodes[to:]...)
}

// qualifiedStart returns the index where the dotted name ending at
// nodes[j] starts.
func qualifiedStart(nodes []*node, j int) int {
	for {
		p := prevSig(nodes, j)
		if p < 0 || !nodes[p].isOp(".") {
			return j
		}
		q := prevSig(nodes, p)
		if q < 0 || nodes[q].kind != kWord && nodes[q].kind != kQuoted {
			return j
		}
		j = q
	}
}

// notCallWords are words a parenthesized group can follow without being
// a function's arguments.
var notCallWords = map[string]bool{
	"FILTER": true, "OVER": true, "AS": true, "IN": true, "VALUES": true, "EXISTS": true, "ANY": true,
	"ALL": true, "SOME": true, "USING": true, "ON": true, "FROM": true, "JOIN": true, "WHERE": true, "AND": true,
	"OR": true, "NOT": true, "WHEN": true, "THEN": true, "ELSE": true, "SELECT": true, "BY": true,
	"INTO": true, "TABLE": true, "VIEW": true, "LATERAL": true, "WITH": true, "RECURSIVE": true,
	"UNION": true, "EXCEPT": true, "MINUS": true, "INTERSECT": true, "RETURNS": true, "SAMPLE": true,
	"TABLESAMPLE": true, "SEED": true, "REPEATABLE": true, "BERNOULLI": true, "SYSTEM": true, "BLOCK": true,
	"ROW": true, "QUALIFY": true, "HAVING": true, "LIMIT": true, "PIVOT": true, "UNPIVOT": true, "FOR": true,
	"GROUP": true, "CUBE": true, "ROLLUP": true, "SETS": true, "KEY": true, "REFERENCES": true, "UNIQUE": true,
	"CHECK": true, "INTERVAL": true, "CASE": true, "IS": true, "LIKE": true, "ILIKE": true, "BETWEEN": true,
	"MATCH_CONDITION": true, "WITHIN": true, "PARTITION": true, "MATERIALIZED": true, "CLUSTER": true,
}

// operandStart returns the index where the operand ending at nodes[end]
// (significant) starts: a dotted name, a function call, a parenthesized
// expression, an element access or a single node.
func operandStart(nodes []*node, end int) int {
	n := nodes[end]
	switch {
	case n.isGroup('('):
		p := prevSig(nodes, end)
		if p >= 0 && (nodes[p].kind == kWord && !notCallWords[nodes[p].word()] || nodes[p].kind == kQuoted) {
			return qualifiedStart(nodes, p)
		}
	case n.isGroup('['):
		if p := prevSig(nodes, end); p >= 0 && endsOperand(nodes[p]) {
			return operandStart(nodes, p)
		}
	case n.kind == kWord || n.kind == kQuoted:
		return qualifiedStart(nodes, end)
	}
	return end
}

// literals translates $variables (at render time, see node.write),
// object and array constants and Snowflake's typed literals.
func (t *translator) literals(nodes []*node) ([]*node, error) {
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]
		switch {
		case n.isGroup('{'):
			obj, err := t.objectConstant(n)
			if err != nil {
				return nil, err
			}
			obj.semi = 'O'
			nodes[i] = obj
		case n.isGroup('['):
			if p := prevSig(nodes, i); p >= 0 && endsOperand(nodes[p]) {
				continue // an element access, see paths
			}
			elems := splitArgs(n.kids)
			kids := []*node{raw("json_array(")}
			var args []*node
			for k, e := range elems {
				if k > 0 {
					kids = append(kids, raw(", "))
				}
				a := jsonArg(e)
				kids = append(kids, a)
				args = append(args, a)
			}
			kids = append(kids, raw(")"))
			nodes[i] = seq([]*node{n}, kids...)
			nodes[i].json = true
			nodes[i].elems = args
			nodes[i].semi = 'A'
		case n.kind == kWord:
			// TIMESTAMP_LTZ '2024-01-01 10:00' and the like.
			j := nextSig(nodes, i)
			if j < 0 || nodes[j].kind != kString || nodes[j].tok.raw {
				continue
			}
			switch n.word() {
			case "TIMESTAMP_NTZ", "TIMESTAMPNTZ", "DATETIME", "TIMESTAMP_LTZ", "TIMESTAMPLTZ", "TIMESTAMP_TZ", "TIMESTAMPTZ":
				typ, _, _, _ := parseType(nodes, i)
				nodes = replace(nodes, i, j+1, seq(nodes[i:j+1], raw("CAST("), nodes[j], raw(" AS "+typ.duck+")")))
			}
		}
	}
	return nodes, nil
}

// objectConstant translates {'key': value, ...}, an OBJECT constant.
func (t *translator) objectConstant(g *node) (*node, error) {
	var keys, values [][]*node
	for _, pair := range splitArgs(g.kids) {
		colon := -1
		for k, n := range pair {
			if n.isOp(":") {
				colon = k
				break
			}
		}
		if colon < 0 && isStar(pair) {
			return nil, unsupported("{*}", "name the keys and values: {'a': a, 'b': b}")
		}
		if colon < 0 {
			return nil, &Error{Code: CodeSyntax, Message: "expected 'key': value in an object constant"}
		}
		keys, values = append(keys, pair[:colon]), append(values, pair[colon+1:])
	}
	n := objectConstruct([]*node{g}, keys, values, false)
	return n, nil
}

// jsonArg makes an argument a JSON value, for functions that build JSON:
// a variant as JSON, a column through sf_to_json (see semiArg), anything
// else as itself.
func jsonArg(arg []*node) *node {
	arg = trim(arg)
	if len(arg) == 1 {
		if arg[0].kind == kVariant {
			arg[0].v.mode = vJSON
		}
	}
	return semiArg(arg, "sf_to_json(")
}

// readArg makes an argument a semi-structured value to read, for
// functions that take an ARRAY or OBJECT: a column through sf_json, which
// reads text as JSON text and Igloo's ARRAY and OBJECT columns as theirs,
// anything else as itself, for CAST(... AS JSON).
func readArg(arg []*node) *node {
	arg = trim(arg)
	if len(arg) == 1 && arg[0].kind == kVariant {
		arg[0].v.mode = vJSON
	}
	return semiArg(arg, "sf_json(")
}

// semiArg is x through macro fn when x is a column, which may be one of
// Igloo's ARRAY or OBJECT columns, a list or map of strings (see sf_json and
// sf_to_json); any other expression as itself. Only columns can be such
// lists: the translations of Snowflake's functions return JSON.
func semiArg(x []*node, fn string) *node {
	x = trim(x)
	if isColumnRef(x) {
		n := seq(x, raw(fn), seq(x, x...), raw(")"))
		n.column = true
		return n
	}
	if len(x) == 1 {
		return x[0]
	}
	return seq(x, x...)
}

// isColumnRef reports whether nodes are a column's name: name, or names
// joined by dots.
func isColumnRef(nodes []*node) bool {
	idx := sig(nodes)
	if len(idx) == 0 || len(idx)%2 == 0 || len(idx) > 7 {
		return false
	}
	for k, i := range idx {
		n := nodes[i]
		if k%2 == 1 {
			if !n.isOp(".") {
				return false
			}
			continue
		}
		switch n.kind {
		case kQuoted:
		case kWord:
			switch n.word() {
			case "NULL", "TRUE", "FALSE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "LOCALTIME",
				"LOCALTIMESTAMP", "SYSDATE", "DEFAULT":
				return false
			}
		default:
			return false
		}
	}
	return true
}

// paths translates access to semi-structured values: v:a.b[0], v['a'],
// GET-like element access, and the VALUE and THIS columns of FLATTEN.
func (t *translator) paths(nodes []*node) ([]*node, error) {
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]
		var base []*node
		var start int
		var v *variant
		switch {
		case n.isOp(":") || n.isGroup('['):
			p := prevSig(nodes, i)
			// After a string, a colon is an object constant's ('key': value).
			if p < 0 || !endsOperand(nodes[p]) || nodes[p].kind == kString {
				continue
			}
			if n.isOp(":") {
				q := nextSig(nodes, i)
				if q < 0 || nodes[q].kind != kWord && nodes[q].kind != kQuoted {
					continue
				}
			}
			if nodes[p].kind == kVariant {
				// A path continued after a cast-less variant, as in v:a[0]
				// when the [0] came after a translated node.
				v, start = nodes[p].v, p
			} else {
				start = operandStart(nodes, p)
				// x::VARIANT[0] reads the cast's result.
				var s int
				var ok bool
				var err error
				if nodes, s, ok, err = castBase(nodes, start, p); err != nil {
					return nil, err
				} else if ok {
					i -= p - s
					p, start = s, s
				}
				base = nodes[start : p+1]
				v = &variant{base: seq(base, base...), column: isColumnRef(base)}
			}
		case t.isFlattenColumn(nodes, i):
			start = qualifiedStart(nodes, i)
			base = nodes[start : i+1]
			v = &variant{base: seq(base, base...), json: true, obj: t.structReads[nodes[i]], elem: t.elemReads[nodes[i]],
				text: t.elemTexts[nodes[i]], guard: t.guarded[nodes[i]]}
			i++
		default:
			continue
		}
		// Path elements: :name, .name (once a path started) and [key].
		j := i
		started := len(v.segs) > 0
		for j < len(nodes) {
			k := j
			if !nodes[k].significant() {
				k = nextSig(nodes, j-1)
				if k < 0 || !(nodes[k].isOp(":") || nodes[k].isOp(".") && started) {
					break
				}
			}
			m := nodes[k]
			if m.isOp(":") || m.isOp(".") && started {
				q := nextSig(nodes, k)
				if q < 0 {
					break
				}
				name, _, ok := nodes[q].name()
				if !ok {
					break
				}
				if m.isOp(".") && !started {
					break
				}
				v.segs = append(v.segs, pathSeg{key: name})
				started = true
				j = q + 1
				continue
			}
			if m.isGroup('[') && k == j {
				seg, err := t.elementSeg(m)
				if err != nil {
					return nil, err
				}
				v.segs = append(v.segs, seg)
				started = true
				j = k + 1
				continue
			}
			break
		}
		if len(v.segs) == 0 && !v.json {
			continue
		}
		vn := &node{kind: kVariant, v: v, pos: nodes[start].pos, end: nodes[j-1].end}
		nodes = replace(nodes, start, j, vn)
		i = start
	}
	return nodes, nil
}

// isFlattenColumn reports whether nodes[i] is FLATTEN's VALUE or THIS
// column: alias.value for a FLATTEN with that alias, or value alone when
// the statement has a FLATTEN without one.
func (t *translator) isFlattenColumn(nodes []*node, i int) bool {
	w := nodes[i].word()
	if w != "VALUE" && w != "THIS" {
		return false
	}
	if next := nextSig(nodes, i); next >= 0 && (nodes[next].isGroup('(') || nodes[next].isOp(".") || nodes[next].isOp("=>")) {
		return false
	}
	p := prevSig(nodes, i)
	if p >= 0 && nodes[p].isOp(".") {
		q := prevSig(nodes, p)
		if q < 0 {
			return false
		}
		alias, _, ok := nodes[q].name()
		if !ok || qualifiedStart(nodes, q) != q {
			return false
		}
		return t.flatten[strings.ToLower(alias)]
	}
	if p >= 0 && nodes[p].word() == "AS" {
		return false
	}
	return t.anon > 0
}

// elementSeg reads [key] or [index] after a semi-structured value.
func (t *translator) elementSeg(g *node) (pathSeg, error) {
	kids := trim(g.kids)
	if len(kids) == 1 {
		if s, ok := kids[0].stringValue(); ok {
			return pathSeg{key: s}, nil
		}
		if kids[0].kind == kNumber {
			n, err := strconv.Atoi(kids[0].tok.text)
			if err != nil || n < 0 {
				return pathSeg{}, &Error{Code: CodeInvalidValue, Message: "invalid array index " + kids[0].tok.text,
					Hint: "array indexes start at 0"}
			}
			return pathSeg{index: n, isIndex: true}, nil
		}
	}
	if len(kids) == 0 {
		return pathSeg{}, &Error{Code: CodeSyntax, Message: "empty [] after a value"}
	}
	return pathSeg{dyn: seq(kids, kids...)}, nil
}

// casts translates value::type.
func (t *translator) casts(nodes []*node) ([]*node, error) {
	for i := 0; i < len(nodes); i++ {
		if !nodes[i].isOp("::") {
			continue
		}
		p, q := prevSig(nodes, i), nextSig(nodes, i)
		if p < 0 || q < 0 {
			return nil, &Error{Code: CodeSyntax, Message: "expected value::type"}
		}
		typ, end, ok, err := parseType(nodes, q)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		start := operandStart(nodes, p)
		cast := castExpr(nodes[start:p+1], typ, false)
		cast.pos, cast.end = nodes[start].pos, nodes[end-1].end
		nodes = replace(nodes, start, end, cast)
		i = start
	}
	return nodes, nil
}

// castBase translates the cast x::type when nodes[start:p+1] is its type
// and a path follows, as in x::VARIANT[0] or x::OBJECT:a, so that the path
// reads the cast's result. It returns the cast's index.
func castBase(nodes []*node, start, p int) ([]*node, int, bool, error) {
	c := prevSig(nodes, start)
	if c < 0 || !nodes[c].isOp("::") {
		return nodes, 0, false, nil
	}
	o := prevSig(nodes, c)
	if o < 0 || !endsOperand(nodes[o]) {
		return nodes, 0, false, nil
	}
	typ, end, ok, err := parseType(nodes, start)
	if err != nil || !ok || end != p+1 {
		return nodes, 0, false, err
	}
	s := operandStart(nodes, o)
	cast := castExpr(nodes[s:c], typ, false)
	cast.pos, cast.end = nodes[s].pos, nodes[p].end
	return replace(nodes, s, p+1, cast), s, true, nil
}

// castExpr casts x to typ as Snowflake does.
func castExpr(x []*node, typ sqlType, try bool) *node {
	x = trim(x)
	fn := "CAST("
	if try {
		fn = "TRY_CAST("
	}
	if len(x) == 1 && x[0].kind == kVariant {
		v := x[0].v
		switch typ.class {
		case classJSON:
			v.mode = vJSON
			return seq(x, x[0])
		case classString:
			v.mode = vString
			return seq(x, x[0])
		case classNested:
			// A structured type converts the value's elements as
			// Snowflake does: DuckDB's casts of JSON to lists, structs
			// and maps.
			v.mode = vJSON
			return seq(x, raw(fn), x[0], raw(" AS "+typ.duck+")"))
		}
		v.mode = vString
		if d := decimalCast(x[0], fn, typ.duck); d != nil {
			return seq(x, d...)
		}
		return seq(x, raw(fn), x[0], raw(" AS "+typ.duck+")"))
	}
	switch typ.class {
	case classJSON:
		n := seq(x, raw("to_json("), seq(x, x...), raw(")"))
		if isColumnRef(x) {
			n = semiArg(x, "sf_to_json(")
		}
		n.json = true
		n.semi = typ.semi()
		return n
	case classString:
		return varcharCast(x)
	case classNested:
		// Text is JSON text, as for paths; see sf_json.
		return seq(x, raw(fn+"sf_json("), seq(x, x...), raw(") AS "+typ.duck+")"))
	}
	if s, ok := x[0].stringValue(); ok && len(x) == 1 && typ.duck == "TIMESTAMPTZ" {
		// Snowflake reads '2024-01-01 10:00:00 +05:00', with a space
		// before the offset, which DuckDB would take for a zone's name.
		x = []*node{{kind: kRaw, text: quoteString(tzOffsetSpace.ReplaceAllString(s, "$1")), pos: x[0].pos, end: x[0].end}}
	}
	if d := decimalCast(operand(x), fn, typ.duck); d != nil {
		return seq(x, d...)
	}
	return seq(x, raw(fn), seq(x, x...), raw(" AS "+typ.duck+")"))
}

var wideDecimal = regexp.MustCompile(`^DECIMAL\((\d+),(\d+)\)$`)

// decimalCast casts x to typ with fn ("CAST(" or "TRY_CAST(") when typ is
// a DECIMAL of more than 18 digits, such as NUMBER's DECIMAL(38,0), and
// returns nil for other types. DuckDB 1.5 reads text as such a DECIMAL,
// which it keeps as a 128-bit integer, some 40 times slower than as one of
// up to 18 digits (61 s against 1.5 s for 40 million ten-digit strings):
// text goes through DECIMAL(18,s) first, and through typ only where that
// fails, too large or not a number, to give typ's value or error. Both read
// a string alike, so the value is the same. typeof(x) is a constant DuckDB
// folds, so other types cast as before.
func decimalCast(x *node, fn, typ string) []*node {
	m := wideDecimal.FindStringSubmatch(typ)
	if m == nil {
		return nil
	}
	p, _ := strconv.Atoi(m[1])
	s, _ := strconv.Atoi(m[2])
	if p <= 18 || s > 18 {
		return nil
	}
	narrow := "DECIMAL(18," + strconv.Itoa(s) + ")"
	return fill("CASE WHEN typeof({x}) = 'VARCHAR' THEN CAST(coalesce(TRY_CAST({x} AS "+narrow+"), "+fn+"{x} AS "+typ+")) AS "+typ+")"+
		" ELSE "+fn+"{x} AS "+typ+") END", x)
}

var tzOffsetSpace = regexp.MustCompile(`\s+([+-]\d{1,2}(:?\d{2})?)$`)

// varcharCast casts x to VARCHAR as Snowflake does: a semi-structured
// value without quotes around a string, and floating-point numbers, times,
// timestamps and binary values in Snowflake's default output formats
// (NUMBER, DATE and BOOLEAN print the same in both). typeof(x) is a
// constant DuckDB folds, so only one branch remains.
func varcharCast(x []*node) *node {
	x = trim(x)
	if len(x) == 1 {
		if x[0].kind == kString || x[0].str {
			return x[0]
		}
		// A number without an exponent is a NUMBER; 1e3 is a FLOAT.
		if x[0].kind == kNumber && !strings.ContainsAny(x[0].tok.text, "eE") {
			return seq(x, raw("CAST("), x[0], raw(" AS VARCHAR)"))
		}
	}
	return seq(x, fill(defaultText, operand(x))...)
}

// defaultText is TO_VARCHAR of {x} without a format, by x's type:
//   - FLOAT: up to 12 significant digits without trailing zeros, in
//     scientific notation from 1e12 or below 1e-4, as Snowflake prints
//     them (999.889980465, 9244, 1e+20, NaN, inf);
//   - TIMESTAMP_NTZ: TIMESTAMP_NTZ_OUTPUT_FORMAT's default, YYYY-MM-DD
//     HH24:MI:SS.FF3;
//   - TIMESTAMP_LTZ and _TZ: TIMESTAMP_OUTPUT_FORMAT's, YYYY-MM-DD
//     HH24:MI:SS.FF3 TZHTZM, in the session's time zone;
//   - TIME: TIME_OUTPUT_FORMAT's, HH24:MI:SS;
//   - BINARY: BINARY_OUTPUT_FORMAT's, HEX;
//   - ARRAY and OBJECT (lists, maps and structs): their JSON text.
var defaultText = "CASE WHEN typeof({x}) = 'JSON' THEN json_extract_string(CAST({x} AS JSON), '$')" +
	" WHEN typeof({x}) IN ('DOUBLE', 'FLOAT') THEN " + floatText("CAST({x} AS DOUBLE)") +
	" WHEN typeof({x}) IN ('TIMESTAMP', 'TIMESTAMP_S', 'TIMESTAMP_MS', 'TIMESTAMP_NS') THEN strftime(CAST({x} AS TIMESTAMP), '%Y-%m-%d %H:%M:%S.%g')" +
	" WHEN typeof({x}) = 'TIMESTAMP WITH TIME ZONE' THEN " + tzhtzm("strftime(CAST({x} AS TIMESTAMPTZ), '%Y-%m-%d %H:%M:%S.%g %z')") +
	" WHEN typeof({x}) = 'TIME' THEN strftime(DATE '1970-01-01' + CAST({x} AS TIME), '%H:%M:%S')" +
	" WHEN typeof({x}) = 'BLOB' THEN hex(CAST({x} AS BLOB))" +
	" WHEN typeof({x}) LIKE '%[]' OR typeof({x}) LIKE 'MAP(%' OR typeof({x}) LIKE 'STRUCT(%' THEN CAST(sf_json({x}) AS VARCHAR)" +
	" ELSE CAST({x} AS VARCHAR) END"

// floatText formats v, a DOUBLE, as Snowflake prints a FLOAT.
func floatText(v string) string {
	return "CASE WHEN isnan(" + v + ") THEN 'NaN' ELSE format('{:.12g}', " + v + ") END"
}

// The offset from UTC of a timestamp in the session's time zone, which
// strftime's %z writes as +00, -07 or +05:30 (and +HH:MM:SS for the local
// mean times of the 19th century), as Snowflake's TZHTZM (+0000, -0700,
// +0530), TZH (+05) and TZM (30). Asking strftime once is several times
// faster than date_part('timezone', ...).

// tzhtzm turns s, text that ends with %z, into text that ends with TZHTZM.
func tzhtzm(s string) string {
	return "regexp_replace(" + s + ` || '00', ':(\d\d)(:\d\d)?00$', '\1')`
}

// tzHours and tzMinutes are TZH and TZM of ts.
func tzHours(ts string) string { return "left(strftime(" + ts + ", '%z'), 3)" }

func tzMinutes(ts string) string {
	return "rpad(substring(strftime(" + ts + ", '%z'), 5, 2), 2, '0')"
}

// fill returns the nodes of tmpl, DuckDB text, with x for each {x}.
func fill(tmpl string, x *node) []*node {
	parts := strings.Split(tmpl, "{x}")
	out := make([]*node, 0, 2*len(parts))
	for i, p := range parts {
		if i > 0 {
			out = append(out, x)
		}
		if p != "" {
			out = append(out, raw(p))
		}
	}
	return out
}

// comparisonOps are the operators after which a semi-structured value is
// compared as text, or as a number when the other side is one.
var comparisonOps = map[string]bool{"=": true, "==": true, "!=": true, "<>": true, "<": true, ">": true, "<=": true, ">=": true}

var arithmeticOps = map[string]bool{"+": true, "-": true, "*": true, "/": true, "%": true}

// variantModes decides how each semi-structured value at this level is
// read, from what is around it, as Snowflake converts a VARIANT for the
// operator it meets: compared with text, matched with LIKE or
// concatenated it is text; compared with a number or in arithmetic it is a
// number. Anywhere else it stays JSON.
func (t *translator) variantModes(nodes []*node) ([]*node, error) {
	for i, n := range nodes {
		if n.kind != kVariant {
			continue
		}
		p, q := prevSig(nodes, i), nextSig(nodes, i)
		var before, after *node
		if p >= 0 {
			before = nodes[p]
		}
		if q >= 0 {
			after = nodes[q]
		}
		mode := n.v.mode
		switch {
		case after != nil && after.kind == kOp && comparisonOps[after.tok.text]:
			mode = otherSide(nodes, nextSig(nodes, q))
		case before != nil && before.kind == kOp && comparisonOps[before.tok.text]:
			mode = otherSide(nodes, prevSig(nodes, p))
		case after.isOp("||") || before.isOp("||"):
			mode = vString
		case after != nil && after.kind == kOp && arithmeticOps[after.tok.text],
			before != nil && before.kind == kOp && arithmeticOps[before.tok.text]:
			mode = vNumber
		case after != nil && (after.word() == "LIKE" || after.word() == "ILIKE" || after.word() == "SIMILAR" ||
			after.word() == "RLIKE" || after.word() == "REGEXP"),
			before != nil && (before.word() == "LIKE" || before.word() == "ILIKE" || before.word() == "ESCAPE"):
			mode = vString
		case after != nil && (after.word() == "IN" || after.word() == "BETWEEN" || after.word() == "NOT"):
			k := q
			if after.word() == "NOT" {
				k = nextSig(nodes, q)
			}
			if k >= 0 && (nodes[k].word() == "LIKE" || nodes[k].word() == "ILIKE" || nodes[k].word() == "RLIKE") {
				mode = vString
				break
			}
			if k >= 0 && nodes[k].word() == "IN" {
				if g := nextSig(nodes, k); g >= 0 && nodes[g].isGroup('(') {
					mode = vString
					if args := splitArgs(nodes[g].kids); len(args) > 0 {
						mode = otherSideNodes(trim(args[0]))
					}
				}
			} else if k >= 0 && nodes[k].word() == "BETWEEN" {
				mode = otherSide(nodes, nextSig(nodes, k))
			}
		case before != nil && before.word() == "CASE":
			mode = vString
		}
		n.v.mode = mode
	}
	return nodes, nil
}

// otherSide is how a value compared with nodes[i] is read: as a number
// when nodes[i] is a numeric literal, else as text.
func otherSide(nodes []*node, i int) vmode {
	if i < 0 {
		return vString
	}
	n := nodes[i]
	if n.isOp("-") || n.isOp("+") {
		if j := nextSig(nodes, i); j >= 0 {
			n = nodes[j]
		}
	}
	if n.kind == kNumber || n.num {
		return vNumber
	}
	if n.kind == kVariant && n.v.mode == vNumber {
		return vNumber
	}
	return vString
}

func otherSideNodes(nodes []*node) vmode {
	if len(nodes) == 0 {
		return vString
	}
	return otherSide(nodes, 0)
}

// operators translates Snowflake's operators and keywords: MINUS, RLIKE
// and REGEXP, and LIKE ANY and LIKE ALL.
func (t *translator) operators(nodes []*node) ([]*node, error) {
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]
		switch n.word() {
		case "MINUS":
			nodes[i] = &node{kind: kRaw, text: "EXCEPT", pos: n.pos, end: n.end}
		case "RLIKE", "REGEXP":
			p := prevSig(nodes, i)
			if p >= 0 && (endsOperand(nodes[p]) || nodes[p].word() == "NOT") {
				nodes[i] = &node{kind: kRaw, text: "SIMILAR TO", pos: n.pos, end: n.end}
			}
		case "LIKE", "ILIKE":
			q := nextSig(nodes, i)
			if q < 0 || nodes[q].word() != "ANY" && nodes[q].word() != "ALL" && nodes[q].word() != "SOME" {
				continue
			}
			g := nextSig(nodes, q)
			if g < 0 || !nodes[g].isGroup('(') {
				continue
			}
			p := prevSig(nodes, i)
			if p >= 0 && nodes[p].word() == "NOT" {
				return nil, unsupported("NOT "+n.word()+" "+nodes[q].word(), "write NOT (x "+n.word()+" "+nodes[q].word()+" (...))")
			}
			start := likeOperandStart(nodes, i)
			if start < 0 {
				return nil, &Error{Code: CodeSyntax, Message: n.word() + " " + nodes[q].word() + " needs a value before it"}
			}
			left := trim(nodes[start:i])
			end := g + 1
			var escape []*node
			if e := nextSig(nodes, g); e >= 0 && nodes[e].word() == "ESCAPE" {
				if s := nextSig(nodes, e); s >= 0 {
					escape = []*node{raw(" ESCAPE "), nodes[s]}
					end = s + 1
				}
			}
			out, err := likeAny(left, n.word(), nodes[q].word() == "ALL", nodes[g], escape)
			if err != nil {
				return nil, err
			}
			out.pos, out.end = nodes[start].pos, nodes[end-1].end
			nodes = replace(nodes, start, end, out)
			i = start
		}
	}
	return nodes, nil
}

// likeOperandStart finds where the value before LIKE at nodes[i] starts:
// after the nearest keyword or comma that cannot be part of it.
func likeOperandStart(nodes []*node, i int) int {
	start := -1
	for j := i - 1; j >= 0; j-- {
		n := nodes[j]
		if !n.significant() {
			continue
		}
		w := n.word()
		if n.isOp(",") || n.isOp("=") || w != "" && (nonOperandWords[w] || w == "WHEN" || w == "THEN" || w == "ELSE") {
			break
		}
		start = j
	}
	return start
}

// likeAny expands x LIKE ANY (p1, p2) to (x LIKE p1 OR x LIKE p2), and a
// subquery's patterns to EXISTS.
func likeAny(left []*node, op string, all bool, g *node, escape []*node) (*node, error) {
	x := operand(left)
	if idx := sig(g.kids); len(idx) > 0 && (g.kids[idx[0]].word() == "SELECT" || g.kids[idx[0]].word() == "WITH") {
		if all {
			return nil, unsupported(op+" ALL (subquery)", "list the patterns")
		}
		kids := []*node{raw("EXISTS (SELECT 1 FROM ("), seq(g.kids, g.kids...), raw(") AS _sf_patterns(_sf_pattern) WHERE "),
			x, raw(" " + op + " _sf_patterns._sf_pattern")}
		kids = append(kids, escape...)
		return seq(left, append(kids, raw(")"))...), nil
	}
	pats := splitArgs(g.kids)
	if len(pats) == 0 {
		return nil, &Error{Code: CodeSyntax, Message: op + " ANY needs at least one pattern"}
	}
	join := " OR "
	if all {
		join = " AND "
	}
	kids := []*node{raw("(")}
	for k, p := range pats {
		if k > 0 {
			kids = append(kids, raw(join))
		}
		kids = append(kids, x, raw(" "+op+" "), operand(p))
		kids = append(kids, escape...)
	}
	return seq(left, append(kids, raw(")"))...), nil
}

// orderBy makes the place of NULLs in each ORDER BY explicit, as
// Snowflake orders them (last ascending, first descending), and orders a
// semi-structured value numbers first, then text, as Snowflake does.
func (t *translator) orderBy(nodes []*node) ([]*node, error) {
	for i := 0; i < len(nodes); i++ {
		if nodes[i].word() != "ORDER" {
			continue
		}
		by := nextSig(nodes, i)
		if by < 0 || nodes[by].word() != "BY" {
			continue
		}
		// The clause runs to the end of the list or a keyword that ends it.
		end := len(nodes)
		for j := by + 1; j < len(nodes); j++ {
			switch nodes[j].word() {
			case "LIMIT", "OFFSET", "FETCH", "ROWS", "RANGE", "GROUPS", "UNION", "EXCEPT", "INTERSECT", "MINUS",
				"QUALIFY", "WINDOW", "FOR", "EXCLUDE":
				end = j
			}
			if nodes[j].isOp(";") {
				end = j
			}
			if end != len(nodes) {
				break
			}
		}
		var out []*node
		items := splitArgs(nodes[by+1 : end])
		for k, item := range items {
			if k > 0 {
				out = append(out, raw(","))
			}
			out = append(out, orderItem(item)...)
		}
		// Keep the white space before the clause's end.
		tail := nodes[end:]
		if end < len(nodes) && end > by+1 && !nodes[end-1].significant() {
			out = append(out, raw(" "))
		}
		nodes = append(append(append([]*node{}, nodes[:by+1]...), out...), tail...)
		i = by + len(out)
	}
	return nodes, nil
}

// orderItem gives one ORDER BY item an explicit NULLS FIRST or LAST.
func orderItem(item []*node) []*node {
	idx := sig(item)
	if len(idx) == 0 {
		return item
	}
	last := item[idx[len(idx)-1]].word()
	if len(idx) >= 2 && item[idx[len(idx)-2]].word() == "NULLS" {
		return item
	}
	desc := last == "DESC"
	exprEnd := len(item)
	if last == "ASC" || last == "DESC" {
		exprEnd = idx[len(idx)-1]
	}
	dir, nulls := " ASC NULLS LAST", " NULLS LAST"
	if desc {
		dir, nulls = " DESC NULLS FIRST", " NULLS FIRST"
	} else if last != "ASC" {
		dir = ""
	}
	expr := trim(item[:exprEnd])
	if len(expr) == 1 && expr[0].kind == kVariant {
		v := expr[0].v
		v.mode = vString
		num := &variant{base: v.base, segs: v.segs, json: v.json, column: v.column, obj: v.obj, elem: v.elem, text: v.text,
			guard: v.guard, mode: vNumber, try: true}
		d := " ASC"
		if desc {
			d = " DESC"
		}
		lead := item[:idx[0]]
		return append(append([]*node{}, lead...), &node{kind: kVariant, v: num}, raw(d+nulls+", "), expr[0], raw(d+nulls))
	}
	if dir == "" {
		return append(append([]*node{}, trimRight(item)...), raw(nulls))
	}
	return append(append([]*node{}, item[:exprEnd]...), raw(strings.TrimPrefix(dir, " ")))
}

// trimRight drops white space and comments from the end of nodes.
func trimRight(nodes []*node) []*node {
	for len(nodes) > 0 && !nodes[len(nodes)-1].significant() {
		nodes = nodes[:len(nodes)-1]
	}
	return nodes
}

// recursiveCTE adds RECURSIVE to a WITH whose common table expressions
// refer to themselves, which Snowflake allows without it.
func (t *translator) recursiveCTE(nodes []*node) ([]*node, error) {
	idx := sig(nodes)
	for k, i := range idx {
		if nodes[i].word() != "WITH" || k+1 >= len(idx) || nodes[idx[k+1]].word() == "RECURSIVE" {
			continue
		}
		// name [(columns)] AS [NOT] [MATERIALIZED] (body), ...
		recursive := false
		for m := k + 1; m < len(idx); {
			name, _, ok := nodes[idx[m]].name()
			if !ok {
				break
			}
			m++
			if m < len(idx) && nodes[idx[m]].isGroup('(') {
				m++
			}
			if m >= len(idx) || nodes[idx[m]].word() != "AS" {
				break
			}
			m++
			for m < len(idx) && (nodes[idx[m]].word() == "NOT" || nodes[idx[m]].word() == "MATERIALIZED") {
				m++
			}
			if m >= len(idx) || !nodes[idx[m]].isGroup('(') {
				break
			}
			if mentions(nodes[idx[m]].kids, name) {
				recursive = true
			}
			m++
			if m >= len(idx) || !nodes[idx[m]].isOp(",") {
				break
			}
			m++
		}
		if recursive {
			nodes = replace(nodes, i, i+1, &node{kind: kRaw, text: nodes[i].tok.text + " RECURSIVE", pos: nodes[i].pos, end: nodes[i].end})
		}
		break
	}
	return nodes, nil
}

// mentions reports whether nodes name name anywhere, as a word or quoted.
func mentions(nodes []*node, name string) bool {
	for _, n := range nodes {
		if got, _, ok := n.name(); ok && strings.EqualFold(got, name) {
			return true
		}
		if (n.kind == kGroup || n.kind == kSeq) && mentions(n.kids, name) {
			return true
		}
	}
	return false
}

// selectEnd are the keywords that end a select list.
var selectEnd = map[string]bool{
	"FROM": true, "WHERE": true, "GROUP": true, "HAVING": true, "QUALIFY": true, "ORDER": true, "LIMIT": true,
	"UNION": true, "EXCEPT": true, "MINUS": true, "INTERSECT": true, "WINDOW": true, "INTO": true, "FETCH": true,
	"OFFSET": true,
}

// expressionEndWords end an expression, so an item ending in one has no
// alias.
var expressionEndWords = map[string]bool{
	"END": true, "NULL": true, "TRUE": true, "FALSE": true, "ASC": true, "DESC": true, "LAST": true, "FIRST": true,
}

// resultNames names the result columns of a statement's outermost select
// list as Snowflake does: an expression without an alias after its text in
// upper case, as in "COUNT(*)". (Other names are folded to upper case when
// the result is described; see ColumnName.)
func (t *translator) resultNames(nodes []*node) ([]*node, error) {
	idx := sig(nodes)
	if len(idx) == 0 || !namesResult(nodes, idx) {
		return nodes, nil
	}
	sel := -1
	for _, i := range idx {
		if nodes[i].word() == "SELECT" {
			sel = i
			break
		}
	}
	if sel < 0 {
		return nodes, nil
	}
	start := sel + 1
	if q := nextSig(nodes, sel); q >= 0 && (nodes[q].word() == "DISTINCT" || nodes[q].word() == "ALL") {
		start = q + 1
	}
	end := len(nodes)
	for j := start; j < len(nodes); j++ {
		w := nodes[j].word()
		if (w == "GROUP" || w == "ORDER") && !nodes[nextSigOr(nodes, j)].isWord("BY") {
			continue // WITHIN GROUP (...)
		}
		if selectEnd[w] || nodes[j].isOp(";") {
			end = j
			break
		}
	}
	// In a query, an unquoted alias names its column in upper case, as in
	// Snowflake. (In CREATE TABLE AS and views it is kept as written, the
	// column's name in the catalog, and shown in upper case when read.)
	query := false
	if len(idx) > 0 {
		switch first := nodes[idx[0]]; {
		case first.word() == "SELECT", first.word() == "WITH", first.isGroup('('):
			query = true
		}
	}
	items := splitArgs(nodes[start:end])
	var out []*node
	for k, item := range items {
		if k > 0 {
			out = append(out, raw(","))
		}
		if query {
			upperAlias(item)
		}
		out = append(out, item...)
		if name := t.expressionName(item); name != "" {
			// Before trailing white space.
			trailing := len(item) - len(trimRight(item))
			out = out[:len(out)-trailing]
			out = append(out, raw(" AS "+quoteIdent(name)))
			out = append(out, item[len(item)-trailing:]...)
		}
	}
	return append(append(append([]*node{}, nodes[:start]...), out...), nodes[end:]...), nil
}

// namesResult reports whether a statement's select list names columns a
// client or the catalog sees: a query's, or that of CREATE TABLE ... AS,
// CREATE VIEW or CREATE MATERIALIZED VIEW.
func namesResult(nodes []*node, idx []int) bool {
	switch first := nodes[idx[0]]; {
	case first.word() == "SELECT", first.word() == "WITH", first.isGroup('('):
		return true
	case first.word() != "CREATE":
		return false
	}
	for _, i := range idx[1:] {
		switch nodes[i].word() {
		case "TABLE", "VIEW":
			return true
		case "OR", "REPLACE", "TEMP", "TEMPORARY", "MATERIALIZED", "TRANSIENT", "SECURE", "LOCAL", "GLOBAL", "VOLATILE":
		default:
			return false
		}
	}
	return false
}

// upperAlias writes a select item's unquoted alias, if it has one, as a
// quoted name in upper case.
func upperAlias(item []*node) {
	idx := sig(item)
	if len(idx) < 2 {
		return
	}
	last := item[idx[len(idx)-1]]
	before := item[idx[len(idx)-2]]
	if last.kind != kWord || expressionEndWords[last.word()] && before.word() != "AS" {
		return
	}
	if before.word() == "AS" || endsOperand(before) && !before.isOp(".") {
		upper := strings.ToUpper(last.tok.text)
		item[idx[len(idx)-1]] = &node{kind: kQuoted, tok: token{kind: kQuoted, text: quoteIdent(upper), val: upper, pos: last.pos, end: last.end},
			pos: last.pos, end: last.end}
	}
}

// expressionName returns the name Snowflake gives a select item without
// an alias that is not a column, or "".
func (t *translator) expressionName(item []*node) string {
	idx := sig(item)
	if len(idx) == 0 {
		return ""
	}
	last := item[idx[len(idx)-1]]
	// A star, perhaps with EXCLUDE, RENAME, REPLACE or ILIKE.
	for _, i := range idx {
		if item[i].isOp("*") && (i == idx[0] || item[prevSig(item, i)].isOp(".")) {
			return ""
		}
	}
	// A column, perhaps qualified; FLATTEN's VALUE and THIS are columns too.
	if _, _, ok := last.name(); ok && qualifiedStart(item, idx[len(idx)-1]) == idx[0] && last.kind != kVariant {
		return ""
	}
	if len(idx) == 1 && last.kind == kVariant && last.v.json && len(last.v.segs) == 0 && last.v.mode == vJSON {
		return ""
	}
	// An alias, with or without AS.
	if len(idx) >= 2 {
		before := item[idx[len(idx)-2]]
		if _, _, ok := last.name(); ok && (before.word() == "AS" ||
			!expressionEndWords[last.word()] && endsOperand(before) && !before.isOp(".")) {
			return ""
		}
	}
	text := source(t.src, item)
	if text == "" {
		return ""
	}
	return strings.ToUpper(stripComments(text))
}

// stripComments drops the comments from Snowflake SQL text.
func stripComments(s string) string {
	toks, err := lex(s)
	if err != nil {
		return s
	}
	var b strings.Builder
	for _, tok := range toks {
		if tok.kind != kComment {
			b.WriteString(s[tok.pos:tok.end])
		}
	}
	return strings.TrimSpace(b.String())
}

// collectGrouped notes, before anything is translated, the
// APPROX_COUNT_DISTINCT and HLL calls of selects with GROUP BY, which count
// exactly (see approxCountDistinct): a call is translated before the select
// around it is seen. grouped says nodes are in such a select.
func (t *translator) collectGrouped(nodes []*node, grouped bool) {
	for i, n := range nodes {
		switch w := n.word(); {
		case w == "SELECT":
			// Its clauses run to the next SELECT at this level, after UNION
			// and the like.
			grouped = false
			for j := i + 1; j < len(nodes) && nodes[j].word() != "SELECT"; j++ {
				if nodes[j].word() == "GROUP" && nodes[nextSigOr(nodes, j)].isWord("BY") {
					grouped = true
					break
				}
			}
		case w == "APPROX_COUNT_DISTINCT" || w == "HLL":
			if grouped {
				t.grouped[n] = true
			}
		case n.kind == kGroup:
			// A subquery is a select of its own.
			idx := sig(n.kids)
			sub := len(idx) > 0 && (n.kids[idx[0]].word() == "SELECT" || n.kids[idx[0]].word() == "WITH")
			t.collectGrouped(n.kids, grouped && !sub)
		}
	}
}

// collectFlattens notes the aliases of the statement's FLATTENs before
// anything is translated, since the select list that reads their columns
// comes before them.
func (t *translator) collectFlattens(nodes []*node) {
	for i, n := range nodes {
		if n.kind == kGroup {
			t.collectFlattens(n.kids)
		}
		_, after, ok := flattenAt(nodes, i)
		if !ok {
			continue
		}
		alias, _, cols := tableAlias(nodes, after)
		if alias != "" {
			t.flatten[strings.ToLower(alias)] = true
		} else {
			t.anon++
		}
		// The names of its columns, which are not reads of them.
		if cols != nil {
			for _, c := range splitArgs(cols.kids) {
				if c = trim(c); len(c) == 1 {
					if name, _, ok := c[0].name(); ok {
						t.columnNames[strings.ToUpper(name)]++
					}
				}
			}
		}
	}
}

// flattenAt reports whether nodes[i] starts a FLATTEN, FLATTEN(...) or
// TABLE(FLATTEN(...)), and returns its argument group (nil if TABLE's has
// none) and the index of the group its alias follows.
func flattenAt(nodes []*node, i int) (args *node, after int, ok bool) {
	g := nextSig(nodes, i)
	if g < 0 || !nodes[g].isGroup('(') {
		return nil, 0, false
	}
	switch nodes[i].word() {
	case "FLATTEN":
		// Inside TABLE(...), the alias follows TABLE's group, which the
		// parent list holds; see tableAlias.
		if p := prevSig(nodes, i); p < 0 && nextSig(nodes, g) < 0 {
			return nil, 0, false
		}
		return nodes[g], g, true
	case "TABLE":
		inner := sig(nodes[g].kids)
		if len(inner) == 0 || nodes[g].kids[inner[0]].word() != "FLATTEN" {
			return nil, 0, false
		}
		if len(inner) == 2 && nodes[g].kids[inner[1]].isGroup('(') {
			args = nodes[g].kids[inner[1]]
		}
		return args, g, true
	}
	return nil, 0, false
}

// notAliasWords may follow a table reference in its place of an alias.
var notAliasWords = map[string]bool{
	"WHERE": true, "ON": true, "JOIN": true, "LEFT": true, "RIGHT": true, "INNER": true, "OUTER": true,
	"FULL": true, "CROSS": true, "NATURAL": true, "GROUP": true, "ORDER": true, "LIMIT": true, "QUALIFY": true,
	"HAVING": true, "UNION": true, "EXCEPT": true, "MINUS": true, "INTERSECT": true, "WINDOW": true,
	"USING": true, "LATERAL": true, "SAMPLE": true, "TABLESAMPLE": true, "ASOF": true, "POSITIONAL": true,
	"OFFSET": true, "FETCH": true, "PIVOT": true, "UNPIVOT": true, "MATCH_CONDITION": true, "AT": true,
	"BEFORE": true, "CHANGES": true,
}

// tableAlias reads the alias after the table reference ending at
// nodes[i]: [AS] name [(columns)]. It returns the alias, the index just
// past it (and its columns), and the column list group if any.
func tableAlias(nodes []*node, i int) (alias string, end int, cols *node) {
	j := aliasIndex(nodes, i)
	if j < 0 {
		return "", i + 1, nil
	}
	name, _, _ := nodes[j].name()
	end = j + 1
	if k := nextSig(nodes, j); k >= 0 && nodes[k].isGroup('(') {
		cols, end = nodes[k], k+1
	}
	return name, end, cols
}

// aliasIndex returns the index of the alias after the table reference
// ending at nodes[i], [AS] name, or -1.
func aliasIndex(nodes []*node, i int) int {
	j := nextSig(nodes, i)
	if j >= 0 && nodes[j].word() == "AS" {
		j = nextSig(nodes, j)
	}
	if j < 0 {
		return -1
	}
	if _, _, ok := nodes[j].name(); !ok || notAliasWords[nodes[j].word()] {
		return -1
	}
	return j
}
