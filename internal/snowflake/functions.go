package snowflake

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// call is a function call being translated.
type call struct {
	t    *translator
	name string
	// args are the arguments, trimmed and translated; named holds those
	// written name => value, by upper-cased name.
	args     [][]*node
	named    map[string][]*node
	distinct bool
	// span is the call as written, from its name to the last clause it
	// took (WITHIN GROUP, IGNORE NULLS, OVER).
	span []*node
	// within is the ORDER BY of WITHIN GROUP (ORDER BY ...), without the
	// words ORDER BY.
	within []*node
	// over is the window: OVER and what follows it.
	over []*node
	// nulls is IGNORE NULLS or RESPECT NULLS.
	nulls string
}

// rewrite translates calls of one function.
type rewrite struct {
	fn func(c *call) (*node, error)
	// within, nulls and over say the rewrite takes the call's WITHIN
	// GROUP, IGNORE/RESPECT NULLS and OVER clauses.
	within, nulls, over bool
}

// arg returns argument i as one node, or nil.
func (c *call) arg(i int) *node {
	if i >= len(c.args) {
		return nil
	}
	return seq(c.args[i], c.args[i]...)
}

// op returns argument i ready to sit next to an operator.
func (c *call) op(i int) *node {
	if i >= len(c.args) {
		return nil
	}
	return operand(c.args[i])
}

// out builds the translation from DuckDB text and nodes.
func (c *call) out(parts ...any) *node {
	kids := make([]*node, 0, len(parts))
	for _, p := range parts {
		switch p := p.(type) {
		case string:
			kids = append(kids, raw(p))
		case *node:
			kids = append(kids, p)
		case []*node:
			kids = append(kids, p...)
		default:
			panic(fmt.Sprintf("snowflake: cannot build from %T", p))
		}
	}
	return seq(c.span, kids...)
}

// argc checks the number of arguments.
func (c *call) argc(min, max int) error {
	if n := len(c.args); n < min || max >= 0 && n > max {
		want := strconv.Itoa(min)
		switch {
		case max < 0:
			want = "at least " + want
		case max != min:
			want = fmt.Sprintf("%d to %d", min, max)
		}
		return &Error{Code: CodeSyntax, Message: fmt.Sprintf("%s takes %s arguments, got %d", c.name, want, n)}
	}
	return nil
}

// list renders the arguments from i on, separated by commas.
func (c *call) list(from int) []*node {
	if from >= len(c.args) {
		return nil
	}
	return joinArgs(c.args[from:])
}

// literal returns argument i's value if it is a string literal.
func (c *call) literal(i int) (string, bool) {
	if i >= len(c.args) || len(c.args[i]) != 1 {
		return "", false
	}
	return c.args[i][0].stringValue()
}

// number returns argument i's value if it is an integer literal, perhaps
// signed.
func (c *call) number(i int) (int, bool) {
	if i >= len(c.args) {
		return 0, false
	}
	a := c.args[i]
	sign := 1
	if len(a) == 2 && (a[0].isOp("-") || a[0].isOp("+")) {
		if a[0].isOp("-") {
			sign = -1
		}
		a = a[1:]
	}
	if len(a) != 1 || a[0].kind != kNumber {
		return 0, false
	}
	n, err := strconv.Atoi(a[0].tok.text)
	if err != nil {
		return 0, false
	}
	return sign * n, true
}

// isLiteral reports whether argument i is a literal: a string, a number
// or NULL.
func (c *call) isLiteral(i int) bool {
	if i >= len(c.args) {
		return false
	}
	a := c.args[i]
	if len(a) == 2 && (a[0].isOp("-") || a[0].isOp("+")) {
		a = a[1:]
	}
	return len(a) == 1 && (a[0].kind == kString || a[0].kind == kNumber || a[0].isWord("NULL"))
}

// tsArg returns argument i for a function that reads dates and times: a
// string literal is read as a TIMESTAMP, as Snowflake reads it.
func (c *call) tsArg(i int) *node {
	if i >= len(c.args) {
		return nil
	}
	if _, ok := c.literal(i); ok {
		return c.out("CAST(", c.arg(i), " AS TIMESTAMP)")
	}
	return c.op(i)
}

// newCall reads the call of the function named at nodes[i] with arguments
// in the group at nodes[g].
func (t *translator) newCall(nodes []*node, i, g int) *call {
	c := &call{t: t, name: nodes[i].word(), named: map[string][]*node{}, span: nodes[i : g+1]}
	for _, a := range splitArgs(nodes[g].kids) {
		a = trim(a)
		if idx := sig(a); len(idx) >= 2 && a[idx[1]].isOp("=>") && a[idx[0]].kind == kWord {
			c.named[a[idx[0]].word()] = trim(a[idx[1]+1:])
			continue
		}
		c.args = append(c.args, a)
	}
	if len(c.args) > 0 {
		if idx := sig(c.args[0]); len(idx) > 1 && c.args[0][idx[0]].word() == "DISTINCT" {
			c.distinct = true
			c.args[0] = trim(c.args[0][idx[0]+1:])
		}
	}
	return c
}

// definesName are words after which a name followed by a parenthesized
// list is not a call: a table's or view's columns.
var definesName = map[string]bool{
	"TABLE": true, "INTO": true, "VIEW": true, "FUNCTION": true, "PROCEDURE": true, "MACRO": true,
	"EXISTS": true, "UPDATE": true, "TYPE": true, "INDEX": true, "SCHEMA": true,
}

// calls translates the function calls at this level.
func (t *translator) calls(nodes []*node) ([]*node, error) {
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]
		if n.kind != kWord {
			continue
		}
		p := prevSig(nodes, i)
		if p >= 0 && (nodes[p].isOp(".") || definesName[nodes[p].word()]) {
			continue
		}
		name := n.word()
		if name == "REPLACE" && afterStar(nodes, i) {
			continue // SELECT * REPLACE (x AS y), which DuckDB has too
		}
		g := nextSig(nodes, i)
		if g < 0 || !nodes[g].isGroup('(') {
			if fn, ok := noParens[name]; ok && (g < 0 || !nodes[g].isOp(".")) {
				nodes[i] = &node{kind: kRaw, text: fn, pos: n.pos, end: n.end}
			}
			continue
		}
		rw, known := rewrites[name]
		if !known && !windowNulls[name] && argModes[name] == 0 {
			continue
		}
		c := t.newCall(nodes, i, g)
		c.setModes()
		end := g + 1
		// WITHIN GROUP (ORDER BY ...)
		if rw.within {
			if w := nextSig(nodes, g); w >= 0 && nodes[w].word() == "WITHIN" {
				gr := nextSig(nodes, w)
				og := nextSig(nodes, gr)
				if gr < 0 || nodes[gr].word() != "GROUP" || og < 0 || !nodes[og].isGroup('(') {
					return nil, &Error{Code: CodeSyntax, Message: "expected WITHIN GROUP (ORDER BY ...)"}
				}
				ob := sig(nodes[og].kids)
				if len(ob) < 2 || nodes[og].kids[ob[0]].word() != "ORDER" || nodes[og].kids[ob[1]].word() != "BY" {
					return nil, &Error{Code: CodeSyntax, Message: "expected ORDER BY in WITHIN GROUP"}
				}
				c.within = trim(nodes[og].kids[ob[1]+1:])
				end = og + 1
			}
		}
		// IGNORE NULLS or RESPECT NULLS after the arguments.
		if rw.nulls || windowNulls[name] {
			if w := nextSig(nodes, end-1); w >= 0 && (nodes[w].word() == "IGNORE" || nodes[w].word() == "RESPECT") {
				if nn := nextSig(nodes, w); nn >= 0 && nodes[nn].word() == "NULLS" {
					c.nulls = nodes[w].word() + " NULLS"
					end = nn + 1
				}
			}
		}
		// OVER (window) or OVER name.
		if rw.over {
			if w := nextSig(nodes, end-1); w >= 0 && nodes[w].word() == "OVER" {
				if s := nextSig(nodes, w); s >= 0 {
					c.over = []*node{raw(" OVER "), nodes[s]}
					end = s + 1
				}
			}
		}
		c.span = nodes[i:end]
		var out *node
		var err error
		switch {
		case known:
			out, err = rw.fn(c)
		case c.nulls != "":
			// lag(x) IGNORE NULLS OVER (...) is lag(x IGNORE NULLS) in DuckDB.
			out = c.out(n.tok.text, "(", c.list(0), " "+c.nulls+")")
		default:
			out = c.out(n.tok.text, "(", distinctPrefix(c), c.list(0), ")")
			if selectors[name] && len(c.args) == 1 {
				out.sel = &selector{name: n.tok.text, arg: c.args[0], distinct: c.distinct}
			}
		}
		if err != nil {
			return nil, err
		}
		if out == nil {
			continue
		}
		nodes = replace(nodes, i, end, out)
	}
	return nodes, nil
}

func distinctPrefix(c *call) string {
	if c.distinct {
		return "DISTINCT "
	}
	return ""
}

// noParens are Snowflake functions written without parentheses.
var noParens = map[string]string{
	"SYSDATE": "CAST(timezone('UTC', current_timestamp) AS TIMESTAMP)",
}

// windowNulls are the window functions that take IGNORE NULLS, which
// Snowflake writes after the arguments and DuckDB inside them.
var windowNulls = map[string]bool{"LAG": true, "LEAD": true, "FIRST_VALUE": true, "LAST_VALUE": true, "NTH_VALUE": true}

// argMode is how a function reads semi-structured arguments.
type argMode int

const (
	modeJSON argMode = iota
	modeString
	modeNumber
	modeLiterals // like the literal arguments beside them (COALESCE and the like)
)

// argModes says how functions that Snowflake gives a VARIANT convert it.
var argModes = func() map[string]argMode {
	m := map[string]argMode{}
	for _, f := range strings.Fields(`UPPER LOWER TRIM LTRIM RTRIM SUBSTR SUBSTRING LEFT RIGHT LENGTH LEN CONCAT
		CONCAT_WS REPLACE SPLIT SPLIT_PART STRTOK STRTOK_TO_ARRAY REGEXP_LIKE REGEXP_SUBSTR REGEXP_REPLACE
		REGEXP_COUNT REGEXP_INSTR REGEXP_SUBSTR_ALL RLIKE CONTAINS STARTSWITH ENDSWITH CHARINDEX POSITION LPAD
		RPAD INITCAP REVERSE TRANSLATE MD5 MD5_HEX SHA1 SHA1_HEX SHA2 SHA2_HEX EDITDISTANCE TO_DATE DATE
		TRY_TO_DATE TO_TIMESTAMP TO_TIMESTAMP_NTZ TO_TIMESTAMP_LTZ TO_TIMESTAMP_TZ TRY_TO_TIMESTAMP
		TRY_TO_TIMESTAMP_NTZ TRY_TO_TIMESTAMP_LTZ TRY_TO_TIMESTAMP_TZ TO_TIME TIME TRY_TO_TIME TO_NUMBER
		TO_DECIMAL TO_NUMERIC TRY_TO_NUMBER TRY_TO_DECIMAL TRY_TO_NUMERIC TO_DOUBLE TRY_TO_DOUBLE TO_BOOLEAN
		TRY_TO_BOOLEAN ASCII SEARCH BASE64_ENCODE HEX_ENCODE LISTAGG ILIKE LIKE SOUNDEX SPACE REPEAT UNICODE
		JAROWINKLER_SIMILARITY DAYNAME MONTHNAME`) {
		m[f] = modeString
	}
	for _, f := range strings.Fields(`ABS ROUND CEIL CEILING FLOOR TRUNC TRUNCATE SQRT POWER POW EXP LN LOG MOD
		SIGN SUM AVG STDDEV STDDEV_POP STDDEV_SAMP VARIANCE VARIANCE_POP VARIANCE_SAMP VAR_POP VAR_SAMP MEDIAN
		APPROX_PERCENTILE CORR COVAR_POP COVAR_SAMP KURTOSIS SKEW DIV0 DIV0NULL SQUARE ZEROIFNULL NULLIFZERO
		RATIO_TO_REPORT WIDTH_BUCKET CBRT SIN COS TAN ASIN ACOS ATAN ATAN2 DEGREES RADIANS FACTORIAL BITAND
		BITOR BITXOR BITNOT BITSHIFTLEFT BITSHIFTRIGHT REGR_SLOPE REGR_INTERCEPT REGR_R2 REGR_COUNT REGR_AVGX
		REGR_AVGY REGR_SXX REGR_SYY REGR_SXY`) {
		m[f] = modeNumber
	}
	for _, f := range strings.Fields(`COALESCE NVL IFNULL NVL2 IFF DECODE GREATEST LEAST NULLIF EQUAL_NULL`) {
		m[f] = modeLiterals
	}
	return m
}()

// setModes decides how the call reads arguments that are semi-structured
// values.
func (c *call) setModes() {
	mode := argModes[c.name]
	litMode := vJSON
	if mode == modeLiterals {
		for _, a := range c.args {
			if len(a) == 1 && a[0].kind == kString {
				litMode = vString
				break
			}
			if len(a) == 1 && a[0].kind == kNumber || len(a) == 2 && a[0].isOp("-") && a[1].kind == kNumber {
				litMode = vNumber
			}
		}
	}
	for k, a := range c.args {
		if len(a) != 1 || a[0].kind != kVariant {
			continue
		}
		v := a[0].v
		switch mode {
		case modeString:
			v.mode = vString
		case modeNumber:
			v.mode = vNumber
		case modeLiterals:
			// IFF's condition, NVL2's test and DECODE's arguments keep their
			// own reading.
			if c.name == "IFF" && k == 0 || c.name == "NVL2" && k == 0 {
				continue
			}
			if litMode != vJSON {
				v.mode = litMode
			}
		}
	}
}

// rewrites are the functions Igloo translates, by upper-cased name.
var rewrites map[string]rewrite

func init() {
	rewrites = map[string]rewrite{}
	simple := func(fn func(c *call) (*node, error)) rewrite { return rewrite{fn: fn} }
	rename := func(to string, min, max int) rewrite {
		return simple(func(c *call) (*node, error) {
			if err := c.argc(min, max); err != nil {
				return nil, err
			}
			return c.out(to+"(", distinctPrefix(c), c.list(0), ")"), nil
		})
	}
	tmpl := func(n int, parts ...any) rewrite {
		return simple(func(c *call) (*node, error) {
			if err := c.argc(n, n); err != nil {
				return nil, err
			}
			out := make([]any, len(parts))
			for k, p := range parts {
				switch p := p.(type) {
				case int:
					out[k] = c.op(p)
				default:
					out[k] = p
				}
			}
			return c.out(out...), nil
		})
	}
	add := func(rw rewrite, names ...string) {
		for _, n := range names {
			rewrites[n] = rw
		}
	}

	// Conditional expressions.
	add(tmpl(3, "CASE WHEN ", 0, " THEN ", 1, " ELSE ", 2, " END"), "IFF")
	add(tmpl(2, "coalesce(", 0, ", ", 1, ")"), "NVL", "IFNULL")
	add(tmpl(3, "CASE WHEN ", 0, " IS NOT NULL THEN ", 1, " ELSE ", 2, " END"), "NVL2")
	add(tmpl(1, "coalesce(", 0, ", 0)"), "ZEROIFNULL")
	add(tmpl(1, "nullif(", 0, ", 0)"), "NULLIFZERO")
	add(tmpl(2, "(", 0, " IS NOT DISTINCT FROM ", 1, ")"), "EQUAL_NULL")
	add(tmpl(2, "CASE WHEN ", 1, " = 0 THEN 0 ELSE ", 0, " / ", 1, " END"), "DIV0")
	add(tmpl(2, "CASE WHEN ", 1, " = 0 OR ", 1, " IS NULL THEN 0 ELSE ", 0, " / ", 1, " END"), "DIV0NULL")
	add(simple(decode), "DECODE")
	add(simple(greatestLeast), "GREATEST", "LEAST")
	add(rename("greatest", 1, -1), "GREATEST_IGNORE_NULLS")
	add(rename("least", 1, -1), "LEAST_IGNORE_NULLS")
	add(rename("bool_and", 1, 1), "BOOLAND_AGG")
	add(rename("bool_or", 1, 1), "BOOLOR_AGG")
	add(rename("bit_and", 1, 1), "BITAND_AGG")
	add(rename("bit_or", 1, 1), "BITOR_AGG")
	add(rename("bit_xor", 1, 1), "BITXOR_AGG")
	add(tmpl(2, "(", 0, " & ", 1, ")"), "BITAND")
	add(tmpl(2, "(", 0, " | ", 1, ")"), "BITOR")
	add(tmpl(2, "xor(", 0, ", ", 1, ")"), "BITXOR")
	add(tmpl(1, "(~", 0, ")"), "BITNOT")
	add(tmpl(2, "(", 0, " << ", 1, ")"), "BITSHIFTLEFT")
	add(tmpl(2, "(", 0, " >> ", 1, ")"), "BITSHIFTRIGHT")

	// Dates and times.
	add(simple(dateAdd), "DATEADD", "TIMEADD", "TIMESTAMPADD")
	add(simple(dateDiff), "DATEDIFF", "TIMEDIFF", "TIMESTAMPDIFF")
	add(simple(dateTrunc), "DATE_TRUNC")
	add(simple(trunc), "TRUNC", "TRUNCATE")
	add(simple(datePartCall), "DATE_PART")
	add(simple(extract), "EXTRACT")
	add(simple(lastDay), "LAST_DAY")
	add(simple(func(c *call) (*node, error) { return dayName(c, "%a") }), "DAYNAME")
	add(simple(func(c *call) (*node, error) { return dayName(c, "%b") }), "MONTHNAME")
	add(simple(func(c *call) (*node, error) { return nextDay(c, "sf_next_day") }), "NEXT_DAY")
	add(simple(func(c *call) (*node, error) { return nextDay(c, "sf_previous_day") }), "PREVIOUS_DAY")
	add(rename("isodow", 1, 1), "DAYOFWEEKISO")
	add(rename("weekofyear", 1, 1), "WEEKISO")
	add(rename("isoyear", 1, 1), "YEAROFWEEK", "YEAROFWEEKISO")
	add(simple(timeSlice), "TIME_SLICE")
	add(simple(addMonths), "ADD_MONTHS")
	add(tmpl(2, "sf_months_between(", 0, ", ", 1, ")"), "MONTHS_BETWEEN")
	add(simple(dateFromParts), "DATE_FROM_PARTS", "DATEFROMPARTS")
	add(simple(func(c *call) (*node, error) { return timestampFromParts(c, "TIMESTAMP") }),
		"TIMESTAMP_FROM_PARTS", "TIMESTAMPFROMPARTS", "TIMESTAMP_NTZ_FROM_PARTS", "TIMESTAMPNTZFROMPARTS")
	add(simple(func(c *call) (*node, error) { return timestampFromParts(c, "TIMESTAMPTZ") }),
		"TIMESTAMP_LTZ_FROM_PARTS", "TIMESTAMPLTZFROMPARTS", "TIMESTAMP_TZ_FROM_PARTS", "TIMESTAMPTZFROMPARTS")
	add(simple(timeFromParts), "TIME_FROM_PARTS", "TIMEFROMPARTS")
	add(simple(convertTimezone), "CONVERT_TIMEZONE")
	add(simple(func(c *call) (*node, error) { return toDate(c, false) }), "TO_DATE", "DATE")
	add(simple(func(c *call) (*node, error) { return toDate(c, true) }), "TRY_TO_DATE")
	for _, kind := range []string{"", "_NTZ", "_LTZ", "_TZ"} {
		tz := kind == "_LTZ" || kind == "_TZ"
		add(simple(func(c *call) (*node, error) { return toTimestamp(c, tz, false) }), "TO_TIMESTAMP"+kind)
		add(simple(func(c *call) (*node, error) { return toTimestamp(c, tz, true) }), "TRY_TO_TIMESTAMP"+kind)
	}
	add(simple(func(c *call) (*node, error) { return toTime(c, false) }), "TO_TIME", "TIME")
	add(simple(func(c *call) (*node, error) { return toTime(c, true) }), "TRY_TO_TIME")
	add(simple(toChar), "TO_CHAR", "TO_VARCHAR")
	for _, n := range []string{"TO_NUMBER", "TO_DECIMAL", "TO_NUMERIC"} {
		add(simple(func(c *call) (*node, error) { return toNumber(c, false) }), n)
		add(simple(func(c *call) (*node, error) { return toNumber(c, true) }), "TRY_"+n)
	}
	add(simple(func(c *call) (*node, error) { return castTo(c, "DOUBLE", false) }), "TO_DOUBLE")
	add(simple(func(c *call) (*node, error) { return castTo(c, "DOUBLE", true) }), "TRY_TO_DOUBLE")
	add(simple(func(c *call) (*node, error) { return typedCast(c, "sf_to_boolean", "CAST", "BOOLEAN") }), "TO_BOOLEAN")
	add(simple(func(c *call) (*node, error) { return typedCast(c, "sf_try_to_boolean", "TRY_CAST", "BOOLEAN") }), "TRY_TO_BOOLEAN")
	add(simple(func(c *call) (*node, error) { return castCall(c, false) }), "CAST")
	add(simple(func(c *call) (*node, error) { return castCall(c, true) }), "TRY_CAST")
	add(simple(func(c *call) (*node, error) { return c.out("current_timestamp"), nil }),
		"CURRENT_TIMESTAMP", "GETDATE", "SYSTIMESTAMP", "LOCALTIMESTAMP")
	add(simple(func(c *call) (*node, error) {
		return c.out("CAST(timezone('UTC', current_timestamp) AS TIMESTAMP)"), nil
	}), "SYSDATE")
	add(simple(func(c *call) (*node, error) { return c.out("CAST(current_timestamp AS TIME)"), nil }),
		"CURRENT_TIME", "LOCALTIME")
	add(simple(func(c *call) (*node, error) { return c.out("current_date"), nil }), "CURRENT_DATE")
	for _, n := range []string{"CURRENT_WAREHOUSE", "CURRENT_ACCOUNT", "CURRENT_ACCOUNT_NAME", "CURRENT_REGION",
		"CURRENT_ORGANIZATION_NAME", "CURRENT_SESSION", "CURRENT_STATEMENT", "CURRENT_CLIENT",
		"CURRENT_SECONDARY_ROLES", "CURRENT_IP_ADDRESS"} {
		add(simple(func(c *call) (*node, error) { return c.out("CAST(NULL AS VARCHAR)"), nil }), n)
	}
	add(simple(func(c *call) (*node, error) {
		return c.out("(SELECT library_version FROM pragma_version())"), nil
	}), "CURRENT_VERSION")
	add(simple(func(c *call) (*node, error) {
		return nil, unsupported("LAST_QUERY_ID and RESULT_SCAN", "keep the result in a temporary table with CREATE TEMP TABLE ... AS")
	}), "LAST_QUERY_ID")

	// Strings.
	add(rename("length", 1, 1), "LEN")
	add(simple(charIndex), "CHARINDEX")
	add(simple(position), "POSITION")
	add(simple(splitPart), "SPLIT_PART")
	add(simple(substr), "SUBSTR", "SUBSTRING")
	add(tmpl(2, "left(", 0, ", CAST(greatest(", 1, ", 0) AS INTEGER))"), "LEFT")
	add(tmpl(2, "right(", 0, ", CAST(greatest(", 1, ", 0) AS INTEGER))"), "RIGHT")
	add(simple(strtok), "STRTOK")
	add(simple(strtokToArray), "STRTOK_TO_ARRAY")
	add(simple(split), "SPLIT")
	add(simple(concat), "CONCAT")
	add(simple(concatWS), "CONCAT_WS")
	add(simple(func(c *call) (*node, error) { return pad(c, "lpad") }), "LPAD")
	add(simple(func(c *call) (*node, error) { return pad(c, "rpad") }), "RPAD")
	add(simple(replaceFn), "REPLACE")
	add(tmpl(2, "starts_with(", 0, ", ", 1, ")"), "STARTSWITH")
	add(tmpl(2, "ends_with(", 0, ", ", 1, ")"), "ENDSWITH")
	add(simple(initcap), "INITCAP")
	add(tmpl(1, "repeat(' ', CAST(", 0, " AS INTEGER))"), "SPACE")
	add(simple(editDistance), "EDITDISTANCE")
	add(tmpl(2, "CAST(round(jaro_winkler_similarity(", 0, ", ", 1, ") * 100) AS BIGINT)"), "JAROWINKLER_SIMILARITY")
	add(tmpl(1, "chr(CAST(", 0, " AS INTEGER))"), "CHR", "CHAR")
	add(rename("md5", 1, 1), "MD5_HEX")
	add(rename("sha1", 1, 1), "SHA1_HEX")
	add(simple(sha2), "SHA2", "SHA2_HEX")
	add(tmpl(1, "to_base64(CAST(", 0, " AS BLOB))"), "BASE64_ENCODE")
	add(tmpl(1, "decode(from_base64(", 0, "))"), "BASE64_DECODE_STRING")
	add(simple(hexEncode), "HEX_ENCODE")
	add(tmpl(1, "from_base64(", 0, ")"), "BASE64_DECODE_BINARY")
	add(simple(func(c *call) (*node, error) {
		if err := c.argc(1, 1); err != nil {
			return nil, err
		}
		return toBinary(c, false)
	}), "HEX_DECODE_BINARY")
	add(tmpl(2, "((", 0, " <> 0) AND (", 1, " <> 0))"), "BOOLAND")
	add(tmpl(2, "((", 0, " <> 0) OR (", 1, " <> 0))"), "BOOLOR")
	add(tmpl(2, "((", 0, " <> 0) <> (", 1, " <> 0))"), "BOOLXOR")
	add(tmpl(1, "(NOT (", 0, " <> 0))"), "BOOLNOT")
	add(tmpl(2, "((", 0, " >> CAST(", 1, " AS INTEGER)) & 1)"), "GETBIT")
	add(simple(func(c *call) (*node, error) { return toBinary(c, false) }), "TO_BINARY")
	add(simple(func(c *call) (*node, error) { return toBinary(c, true) }), "TRY_TO_BINARY")
	add(tmpl(1, "decode(unhex(", 0, "))"), "HEX_DECODE_STRING")
	add(simple(uuidString), "UUID_STRING")
	add(simple(hashFn), "HASH")
	add(simple(regexpLike), "REGEXP_LIKE", "RLIKE")
	add(simple(func(c *call) (*node, error) { return regexpSubstr(c, false) }), "REGEXP_SUBSTR")
	add(simple(func(c *call) (*node, error) { return regexpSubstr(c, true) }), "REGEXP_SUBSTR_ALL", "REGEXP_EXTRACT_ALL")
	add(simple(regexpReplace), "REGEXP_REPLACE")
	add(simple(regexpCount), "REGEXP_COUNT")
	add(simple(regexpInstr), "REGEXP_INSTR")
	add(simple(search), "SEARCH")

	// Numbers.
	add(tmpl(1, "power(", 0, ", 2)"), "SQUARE")
	add(simple(func(c *call) (*node, error) { return scaled(c, "ceil") }), "CEIL", "CEILING")
	add(simple(func(c *call) (*node, error) { return scaled(c, "floor") }), "FLOOR")
	add(simple(randomFn), "RANDOM")
	add(simple(uniform), "UNIFORM")
	add(simple(normal), "NORMAL")
	add(simple(widthBucket), "WIDTH_BUCKET")
	add(simple(func(c *call) (*node, error) {
		return c.out("(row_number() OVER () - 1)"), nil
	}), "SEQ1", "SEQ2", "SEQ4", "SEQ8")

	// Aggregates and window functions.
	add(rewrite{fn: listagg, within: true, over: true}, "LISTAGG")
	add(rewrite{fn: arrayAgg, within: true, over: true}, "ARRAY_AGG", "ARRAYAGG")
	add(rewrite{fn: arrayUniqueAgg, over: true}, "ARRAY_UNIQUE_AGG")
	add(rewrite{fn: objectAgg, over: true}, "OBJECT_AGG")
	add(rewrite{fn: ratioToReport, over: true}, "RATIO_TO_REPORT")
	add(rewrite{fn: conditionalTrueEvent, over: true}, "CONDITIONAL_TRUE_EVENT")
	add(simple(func(c *call) (*node, error) {
		return nil, unsupported("CONDITIONAL_CHANGE_EVENT", "compare with lag() in a subquery and sum the changes")
	}), "CONDITIONAL_CHANGE_EVENT")
	add(rewrite{fn: minMax, over: true}, "MIN", "MAX")
	add(simple(countFn), "COUNT")
	add(rewrite{fn: approxCountDistinct, over: true}, "HLL", "APPROX_COUNT_DISTINCT")
	add(tmpl(1, "datasketch_hll(12, ", 0, ")"), "HLL_ACCUMULATE")
	add(tmpl(1, "datasketch_hll_union(12, ", 0, ")"), "HLL_COMBINE")
	add(tmpl(1, "CAST(round(datasketch_hll_estimate(", 0, ")) AS BIGINT)"), "HLL_ESTIMATE")
	add(rename("approx_quantile", 2, 2), "APPROX_PERCENTILE")
	add(simple(approxTopK), "APPROX_TOP_K")
	add(rename("skewness", 1, 1), "SKEW")
	add(rename("var_pop", 1, 1), "VARIANCE_POP")
	add(rename("var_samp", 1, 1), "VARIANCE_SAMP")

	// Semi-structured data.
	add(tmpl(1, "json(", 0, ")"), "PARSE_JSON")
	add(tmpl(1, "CASE WHEN json_valid(", 0, ") THEN json(", 0, ") END"), "TRY_PARSE_JSON")
	add(tmpl(1, "CASE WHEN ", 0, " IS NULL OR json_valid(", 0, ") THEN NULL ELSE 'invalid JSON' END"), "CHECK_JSON")
	add(simple(toJSON), "TO_JSON")
	add(simple(func(c *call) (*node, error) { return toVariant(c, "") }), "TO_VARIANT", "TO_OBJECT")
	add(simple(func(c *call) (*node, error) { return toVariant(c, "ARRAY") }), "TO_ARRAY")
	add(simple(func(c *call) (*node, error) { return objectConstructCall(c, false) }), "OBJECT_CONSTRUCT")
	add(simple(func(c *call) (*node, error) { return objectConstructCall(c, true) }), "OBJECT_CONSTRUCT_KEEP_NULL")
	add(simple(arrayConstruct), "ARRAY_CONSTRUCT")
	add(simple(arrayConstructCompact), "ARRAY_CONSTRUCT_COMPACT")
	add(simple(arraySize), "ARRAY_SIZE")
	add(simple(arrayContains), "ARRAY_CONTAINS")
	add(simple(arrayPosition), "ARRAY_POSITION")
	add(simple(arrayToString), "ARRAY_TO_STRING")
	add(simple(arraySlice), "ARRAY_SLICE")
	add(simple(func(c *call) (*node, error) { return arrayList(c, "list_append", 2, true) }), "ARRAY_APPEND")
	add(simple(func(c *call) (*node, error) { return arrayList(c, "list_prepend", 2, true) }), "ARRAY_PREPEND")
	add(simple(func(c *call) (*node, error) { return arrayList(c, "list_concat", 2, false) }), "ARRAY_CAT")
	add(simple(func(c *call) (*node, error) { return arrayList(c, "list_intersect", 2, false) }), "ARRAY_INTERSECTION")
	add(simple(func(c *call) (*node, error) { return arrayList(c, "list_distinct", 1, false) }), "ARRAY_DISTINCT")
	add(simple(arraysOverlap), "ARRAYS_OVERLAP")
	add(simple(arrayCompact), "ARRAY_COMPACT")
	add(simple(arrayGenerateRange), "ARRAY_GENERATE_RANGE")
	add(tmpl(1, "to_json(list_sort(json_keys(CAST(", 0, " AS JSON))))"), "OBJECT_KEYS")
	add(simple(objectInsert), "OBJECT_INSERT")
	add(simple(get), "GET")
	add(simple(getPath), "GET_PATH")
	add(simple(typeOf), "TYPEOF")
	for name, types := range map[string]string{
		"IS_ARRAY": "'ARRAY'", "IS_OBJECT": "'OBJECT'", "IS_BOOLEAN": "'BOOLEAN'", "IS_VARCHAR": "'VARCHAR'",
		"IS_INTEGER": "'BIGINT', 'UBIGINT'", "IS_DECIMAL": "'BIGINT', 'UBIGINT', 'DOUBLE'",
		"IS_DOUBLE": "'BIGINT', 'UBIGINT', 'DOUBLE'", "IS_REAL": "'BIGINT', 'UBIGINT', 'DOUBLE'", "IS_NULL_VALUE": "'NULL'",
	} {
		add(tmpl(1, "(json_type(CAST(", 0, " AS JSON)) IN ("+types+"))"), name)
	}
	add(tmpl(1, "CASE WHEN json_type(CAST(", 0, " AS JSON)) = 'NULL' THEN NULL ELSE CAST(", 0, " AS JSON) END"), "STRIP_NULL_VALUE")
	add(tmpl(1, "CASE WHEN json_type(CAST(", 0, " AS JSON)) = 'VARCHAR' THEN json_extract_string(CAST(", 0,
		" AS JSON), '$') END"), "AS_VARCHAR", "AS_CHAR")
	add(tmpl(1, "CASE WHEN json_type(CAST(", 0, " AS JSON)) IN ('BIGINT', 'UBIGINT', 'DOUBLE') THEN CAST(json_extract_string(CAST(", 0,
		" AS JSON), '$') AS DOUBLE) END"), "AS_DOUBLE", "AS_REAL")
	add(tmpl(1, "CASE WHEN json_type(CAST(", 0, " AS JSON)) IN ('BIGINT', 'UBIGINT') THEN CAST(json_extract_string(CAST(", 0,
		" AS JSON), '$') AS BIGINT) END"), "AS_INTEGER")
	add(tmpl(1, "CASE WHEN json_type(CAST(", 0, " AS JSON)) = 'BOOLEAN' THEN CAST(json_extract_string(CAST(", 0,
		" AS JSON), '$') AS BOOLEAN) END"), "AS_BOOLEAN")
	add(simple(func(c *call) (*node, error) {
		if err := c.argc(1, 1); err != nil {
			return nil, err
		}
		text, err := c.t.nameOf("IDENTIFIER", c.args[0])
		if err != nil {
			return nil, err
		}
		return c.out(text), nil
	}), "IDENTIFIER")
	for _, n := range []string{"OBJECT_DELETE", "OBJECT_PICK", "GET_IGNORE_CASE", "ARRAY_FLATTEN", "ARRAY_SORT",
		"ARRAY_MAX", "ARRAY_MIN", "ARRAY_EXCEPT", "ARRAY_REMOVE", "ARRAY_INSERT", "PARSE_XML", "XMLGET",
		"SOUNDEX", "SOUNDEX_P123", "ZIPF", "RANDSTR", "SYSTEM$TYPEOF", "STRTOK_SPLIT_TO_TABLE", "MATCH_RECOGNIZE",
		"TO_GEOGRAPHY", "ST_MAKEPOINT", "ENCRYPT", "DECRYPT", "COMPRESS", "DECOMPRESS_STRING"} {
		name := n
		add(simple(func(c *call) (*node, error) {
			return nil, unsupported(name+"()", "")
		}), name)
	}
}

// decode translates DECODE(expr, search, result, ..., default), which
// matches NULL with NULL.
func decode(c *call) (*node, error) {
	if err := c.argc(3, -1); err != nil {
		return nil, err
	}
	parts := []any{"CASE"}
	k := 1
	for ; k+1 < len(c.args); k += 2 {
		parts = append(parts, " WHEN ", c.op(0), " IS NOT DISTINCT FROM ", c.op(k), " THEN ", c.arg(k+1))
	}
	if k < len(c.args) {
		parts = append(parts, " ELSE ", c.arg(k))
	}
	return c.out(append(parts, " END")...), nil
}

// greatestLeast translates GREATEST and LEAST, which are NULL when any
// argument is.
func greatestLeast(c *call) (*node, error) {
	if err := c.argc(1, -1); err != nil {
		return nil, err
	}
	parts := []any{"CASE WHEN "}
	for k := range c.args {
		if k > 0 {
			parts = append(parts, " OR ")
		}
		parts = append(parts, c.op(k), " IS NULL")
	}
	parts = append(parts, " THEN NULL ELSE "+strings.ToLower(c.name)+"(", c.list(0), ") END")
	return c.out(parts...), nil
}

// datePartAliases maps Snowflake's names of date and time parts to
// Igloo's canonical ones.
var datePartAliases = func() map[string]string {
	m := map[string]string{}
	for canon, names := range map[string]string{
		"year":              "YEAR Y YY YYY YYYY YR YEARS YRS",
		"month":             "MONTH MM MON MONS MONTHS",
		"day":               "DAY D DD DAYS DAYOFMONTH",
		"week":              "WEEK W WK WEEKOFYEAR WOY WY WEEKS",
		"quarter":           "QUARTER Q QTR QTRS QUARTERS",
		"hour":              "HOUR H HH HR HOURS HRS",
		"minute":            "MINUTE M MI MIN MINUTES MINS",
		"second":            "SECOND S SEC SECONDS SECS",
		"millisecond":       "MILLISECOND MS MSEC MILLISECONDS",
		"microsecond":       "MICROSECOND US USEC MICROSECONDS",
		"nanosecond":        "NANOSECOND NS NSEC NANOSEC NSECOND NANOSECONDS NANOSECS NSECONDS",
		"dayofweek":         "DAYOFWEEK WEEKDAY DOW DW",
		"dayofweekiso":      "DAYOFWEEKISO WEEKDAY_ISO DOW_ISO DW_ISO DAYOFWEEK_ISO",
		"dayofyear":         "DAYOFYEAR YEARDAY DOY DY",
		"weekiso":           "WEEKISO WEEK_ISO WEEKOFYEARISO WEEKOFYEAR_ISO",
		"yearofweek":        "YEAROFWEEK",
		"yearofweekiso":     "YEAROFWEEKISO",
		"epoch_second":      "EPOCH_SECOND EPOCH EPOCH_SECONDS",
		"epoch_millisecond": "EPOCH_MILLISECOND EPOCH_MILLISECONDS",
		"epoch_microsecond": "EPOCH_MICROSECOND EPOCH_MICROSECONDS",
		"epoch_nanosecond":  "EPOCH_NANOSECOND EPOCH_NANOSECONDS",
		"timezone_hour":     "TIMEZONE_HOUR TZH",
		"timezone_minute":   "TIMEZONE_MINUTE TZM",
	} {
		for _, n := range strings.Fields(names) {
			m[n] = canon
		}
	}
	return m
}()

// datePart reads a date or time part written as a word or a string.
func datePart(arg []*node) (string, error) {
	arg = trim(arg)
	if len(arg) == 1 {
		w := arg[0].word()
		if s, ok := arg[0].stringValue(); ok {
			w = strings.ToUpper(strings.TrimSpace(s))
		}
		if p, ok := datePartAliases[w]; ok {
			return p, nil
		}
	}
	return "", &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("%s is not a date or time part", render(arg)),
		Hint: "use a part such as year, quarter, month, week, day, hour, minute or second"}
}

// dateAdd translates DATEADD(part, n, x), which keeps x's type for parts
// of a day or more, as Snowflake does.
func dateAdd(c *call) (*node, error) {
	if err := c.argc(3, 3); err != nil {
		return nil, err
	}
	part, err := datePart(c.args[0])
	if err != nil {
		return nil, err
	}
	if c.args[1][0].kind == kVariant {
		c.args[1][0].v.mode = vNumber
	}
	return addParts(c, part, c.args[2], c.arg(1))
}

// addParts adds n parts to x, keeping x's type where Snowflake does.
func addParts(c *call, part string, x []*node, n *node) (*node, error) {
	macro, factor := "", ""
	switch part {
	case "year":
		macro, factor = "sf_add_months", "12"
	case "quarter":
		macro, factor = "sf_add_months", "3"
	case "month":
		macro, factor = "sf_add_months", "1"
	case "week":
		macro, factor = "sf_add_days", "7"
	case "day":
		macro, factor = "sf_add_days", "1"
	case "hour":
		macro, factor = "sf_add_micros", "3600000000"
	case "minute":
		macro, factor = "sf_add_micros", "60000000"
	case "second":
		macro, factor = "sf_add_micros", "1000000"
	case "millisecond":
		macro, factor = "sf_add_micros", "1000"
	case "microsecond":
		macro, factor = "sf_add_micros", "1"
	case "nanosecond":
		macro, factor = "sf_add_micros", "0.001"
	default:
		return nil, &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("%s cannot be added to a date", part)}
	}
	return typed(x, func(x *node) *node {
		return c.out(macro+"(", x, ", ", n, ", "+factor+")")
	}, func(x *node) *node {
		switch macro {
		case "sf_add_months":
			return c.out("(", x, " + to_months(CAST(", n, " AS INTEGER) * "+factor+"))")
		case "sf_add_days":
			return c.out("(", x, " + to_days(CAST(CAST(", n, " AS INTEGER) * "+factor+" AS INTEGER)))")
		}
		return c.out("(", x, " + to_microseconds(CAST(CAST(", n, " AS BIGINT) * "+factor+" AS BIGINT)))")
	}), nil
}

// dateDiff translates DATEDIFF(part, a, b): how many boundaries of part
// lie between a and b.
func dateDiff(c *call) (*node, error) {
	if err := c.argc(3, 3); err != nil {
		return nil, err
	}
	part, err := datePart(c.args[0])
	if err != nil {
		return nil, err
	}
	a, b := c.tsArg(1), c.tsArg(2)
	switch part {
	case "year", "quarter", "month", "day", "hour", "minute", "second", "millisecond", "microsecond":
		return c.out("date_diff('"+part+"', ", a, ", ", b, ")"), nil
	case "week":
		// Weeks start on Monday; DuckDB's date_diff counts 7-day spans.
		return c.out("(date_diff('day', date_trunc('week', ", a, "), date_trunc('week', ", b, ")) // 7)"), nil
	case "nanosecond":
		return c.out("(date_diff('microsecond', ", a, ", ", b, ") * 1000)"), nil
	}
	return nil, &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("%s is not a valid part for %s", part, c.name)}
}

// truncParts are the parts DATE_TRUNC takes, by canonical name.
var truncParts = map[string]string{
	"year": "year", "quarter": "quarter", "month": "month", "week": "week", "day": "day", "hour": "hour",
	"minute": "minute", "second": "second", "millisecond": "millisecond", "microsecond": "microsecond",
	"nanosecond": "microsecond",
}

// dateTrunc translates DATE_TRUNC(part, x), which keeps x's type.
func dateTrunc(c *call) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	part, err := datePart(c.args[0])
	if err != nil {
		return nil, err
	}
	p, ok := truncParts[part]
	if !ok {
		return nil, &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("%s is not a valid part for DATE_TRUNC", part)}
	}
	return truncTyped(c, p, c.args[1]), nil
}

// trunc translates TRUNC(x, part) of a date and TRUNC(n [, scale]) of a
// number.
func trunc(c *call) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	if len(c.args) == 2 {
		if s, ok := c.literal(1); ok {
			tp, ok := truncParts[datePartAliases[strings.ToUpper(strings.TrimSpace(s))]]
			if !ok {
				return nil, &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("%s is not a valid part for TRUNC", s)}
			}
			return truncTyped(c, tp, c.args[0]), nil
		}
		if s, ok := c.number(1); ok {
			return c.out(scaledExpr("trunc", c.op(0), s)), nil
		}
		return nil, unsupported("TRUNC with a computed scale", "give the scale as a number")
	}
	return c.out("trunc(", c.arg(0), ")"), nil
}

// scaled translates CEIL(x [, scale]) and FLOOR(x [, scale]).
func scaled(c *call, fn string) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	if len(c.args) == 1 {
		return c.out(fn+"(", c.arg(0), ")"), nil
	}
	s, ok := c.number(1)
	if !ok {
		return nil, unsupported(c.name+" with a computed scale", "give the scale as a number")
	}
	return c.out(scaledExpr(fn, c.op(0), s)), nil
}

// scaledExpr rounds x with fn (trunc, ceil, floor) to scale decimal
// places, as a DECIMAL with that scale when it is not negative.
func scaledExpr(fn string, x *node, scale int) *node {
	f := strconv.FormatFloat(math.Pow(10, float64(scale)), 'f', -1, 64)
	if scale < 0 {
		return seq(nil, raw(fn+"("), x, raw(" * "+f+") / "+f))
	}
	return seq(nil, raw("CAST("+fn+"("), x, raw(" * "+f+") / "+f+" AS DECIMAL(38,"+strconv.Itoa(scale)+"))"))
}

// datePartCall translates DATE_PART(part, x).
func datePartCall(c *call) (*node, error) {
	if len(c.args) == 1 {
		return extract(c) // DATE_PART(part FROM x)
	}
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	part, err := datePart(c.args[0])
	if err != nil {
		return nil, err
	}
	return datePartExpr(c, part, c.tsArg(1))
}

// extract translates EXTRACT(part FROM x).
func extract(c *call) (*node, error) {
	if len(c.args) == 2 {
		return datePartCall(c) // EXTRACT(part, x)
	}
	if len(c.args) != 1 {
		return nil, &Error{Code: CodeSyntax, Message: "expected EXTRACT(part FROM value)"}
	}
	a := c.args[0]
	idx := sig(a)
	if len(idx) < 3 || a[idx[1]].word() != "FROM" {
		return nil, &Error{Code: CodeSyntax, Message: "expected EXTRACT(part FROM value)"}
	}
	part, err := datePart(a[idx[0] : idx[0]+1])
	if err != nil {
		return nil, err
	}
	x := trim(a[idx[1]+1:])
	if len(x) == 1 && x[0].kind == kString {
		return datePartExpr(c, part, c.out("CAST(", x[0], " AS TIMESTAMP)"))
	}
	return datePartExpr(c, part, operand(x))
}

// datePartExpr extracts part from x as Snowflake does.
func datePartExpr(c *call, part string, x *node) (*node, error) {
	duck := map[string]string{
		"year": "year", "quarter": "quarter", "month": "month", "week": "week", "day": "day", "hour": "hour",
		"minute": "minute", "second": "second", "dayofweek": "dow", "dayofweekiso": "isodow",
		"dayofyear": "doy", "weekiso": "week", "yearofweek": "isoyear", "yearofweekiso": "isoyear",
		"timezone_hour": "timezone_hour", "timezone_minute": "timezone_minute",
	}
	if p, ok := duck[part]; ok {
		return c.out("date_part('"+p+"', ", x, ")"), nil
	}
	switch part {
	case "nanosecond":
		return c.out("(date_part('microseconds', ", x, ") % 1000000 * 1000)"), nil
	case "epoch_second":
		return c.out("CAST(floor(date_part('epoch', ", x, ")) AS BIGINT)"), nil
	case "epoch_millisecond":
		return c.out("epoch_ms(", x, ")"), nil
	case "epoch_microsecond":
		return c.out("epoch_us(", x, ")"), nil
	case "epoch_nanosecond":
		return c.out("epoch_ns(", x, ")"), nil
	}
	return nil, &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("%s is not a valid part for %s", part, c.name)}
}

// lastDay translates LAST_DAY(x [, part]).
func lastDay(c *call) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	part := "month"
	if len(c.args) == 2 {
		p, err := datePart(c.args[1])
		if err != nil {
			return nil, err
		}
		part = p
	}
	x := c.tsArg(0)
	switch part {
	case "month":
		return c.out("last_day(", x, ")"), nil
	case "year":
		return c.out("CAST(date_trunc('year', ", x, ") + INTERVAL 1 YEAR - INTERVAL 1 DAY AS DATE)"), nil
	case "quarter":
		return c.out("CAST(date_trunc('quarter', ", x, ") + INTERVAL 3 MONTH - INTERVAL 1 DAY AS DATE)"), nil
	case "week":
		return c.out("(CAST(date_trunc('week', ", x, ") AS DATE) + 6)"), nil
	}
	return nil, &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("%s is not a valid part for LAST_DAY", part)}
}

// dayName translates DAYNAME and MONTHNAME, which give three letters.
func dayName(c *call, f string) (*node, error) {
	if err := c.argc(1, 1); err != nil {
		return nil, err
	}
	return c.out("strftime(", c.tsArg(0), ", '"+f+"')"), nil
}

// nextDay translates NEXT_DAY and PREVIOUS_DAY.
func nextDay(c *call, macro string) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	return c.out(macro+"(", c.tsArg(0), ", ", c.arg(1), ")"), nil
}

// timeSlice translates TIME_SLICE(x, n, part [, 'START' | 'END']).
// Snowflake's slices start at 1970-01-01, weeks on the Monday before it.
func timeSlice(c *call) (*node, error) {
	if err := c.argc(3, 4); err != nil {
		return nil, err
	}
	part, err := datePart(c.args[2])
	if err != nil {
		return nil, err
	}
	n := c.op(1)
	var width []any
	origin := "'1970-01-01'"
	switch part {
	case "year":
		width = []any{"to_years(CAST(", n, " AS INTEGER))"}
	case "quarter":
		width = []any{"to_months(CAST(", n, " AS INTEGER) * 3)"}
	case "month":
		width = []any{"to_months(CAST(", n, " AS INTEGER))"}
	case "week":
		width = []any{"to_days(CAST(", n, " AS INTEGER) * 7)"}
		origin = "'1969-12-29'"
	case "day":
		width = []any{"to_days(CAST(", n, " AS INTEGER))"}
	case "hour":
		width = []any{"to_hours(CAST(", n, " AS BIGINT))"}
	case "minute":
		width = []any{"to_minutes(CAST(", n, " AS BIGINT))"}
	case "second":
		width = []any{"to_seconds(CAST(", n, " AS BIGINT))"}
	default:
		return nil, &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("%s is not a valid part for TIME_SLICE", part)}
	}
	slice := typed(c.args[0], func(x *node) *node {
		parts := append([]any{"sf_time_slice(", x, ", "}, width...)
		return c.out(append(parts, ", "+origin+")")...)
	}, func(x *node) *node {
		parts := append([]any{"time_bucket("}, width...)
		return c.out(append(parts, ", CAST(", x, " AS TIMESTAMP), CAST("+origin+" AS TIMESTAMP))")...)
	})
	if len(c.args) == 4 {
		s, ok := c.literal(3)
		switch {
		case !ok:
			return nil, unsupported("TIME_SLICE with a computed START or END", "write 'START' or 'END'")
		case strings.EqualFold(s, "END"):
			return addParts(c, part, []*node{slice}, n)
		case !strings.EqualFold(s, "START"):
			return nil, &Error{Code: CodeInvalidValue, Message: "TIME_SLICE takes 'START' or 'END', got " + quoteString(s)}
		}
	}
	return slice, nil
}

// dateFromParts translates DATE_FROM_PARTS, which carries months and days
// beyond their range into the next, as Snowflake does.
func dateFromParts(c *call) (*node, error) {
	if err := c.argc(3, 3); err != nil {
		return nil, err
	}
	return c.out("CAST(make_date(CAST(", c.arg(0), " AS BIGINT), 1, 1) + to_months(CAST(", c.op(1),
		" AS INTEGER) - 1) + to_days(CAST(", c.op(2), " AS INTEGER) - 1) AS DATE)"), nil
}

// timestampFromParts translates TIMESTAMP_FROM_PARTS and its kinds.
func timestampFromParts(c *call, typ string) (*node, error) {
	if len(c.args) == 2 {
		return c.out("CAST(CAST(", c.arg(0), " AS DATE) + CAST(", c.arg(1), " AS TIME) AS "+typ+")"), nil
	}
	if err := c.argc(6, 8); err != nil {
		return nil, err
	}
	if len(c.args) == 8 {
		return nil, unsupported(c.name+" with a time zone", "convert the result with CONVERT_TIMEZONE")
	}
	parts := []any{"CAST(CAST(make_date(CAST(", c.arg(0), " AS BIGINT), 1, 1) AS TIMESTAMP) + to_months(CAST(", c.op(1),
		" AS INTEGER) - 1) + to_days(CAST(", c.op(2), " AS INTEGER) - 1) + to_hours(CAST(", c.op(3),
		" AS BIGINT)) + to_minutes(CAST(", c.op(4), " AS BIGINT)) + to_microseconds(CAST(", c.op(5), " * 1000000"}
	if len(c.args) == 7 {
		parts = append(parts, " + ", c.op(6), " / 1000")
	}
	parts = append(parts, " AS BIGINT)) AS "+typ+")")
	return c.out(parts...), nil
}

// convertTimezone translates CONVERT_TIMEZONE. The two-argument form
// gives the wall-clock time in the target zone, as TIMESTAMP_NTZ.
func convertTimezone(c *call) (*node, error) {
	if err := c.argc(2, 3); err != nil {
		return nil, err
	}
	if len(c.args) == 3 {
		return c.out("timezone(", c.arg(1), ", timezone(", c.arg(0), ", CAST(", c.arg(2), " AS TIMESTAMP)))"), nil
	}
	return c.out("timezone(", c.arg(0), ", CAST(", c.arg(1), " AS TIMESTAMPTZ))"), nil
}

// formatArg reads the format argument i: a string literal, converted for
// strftime and strptime. ok is false when the call has no such argument.
func (c *call) formatArg(i int) (f string, ok bool, err error) {
	if i >= len(c.args) {
		return "", false, nil
	}
	s, lit := c.literal(i)
	if !lit {
		return "", false, unsupported(c.name+" with a computed format", "give the format as a string literal")
	}
	if strings.EqualFold(strings.TrimSpace(s), "AUTO") {
		return "", false, nil
	}
	f, err = dateFormat(s)
	return f, true, err
}

// toDate translates TO_DATE and DATE, and TRY_TO_DATE.
func toDate(c *call, try bool) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	f, hasFormat, err := c.formatArg(1)
	if err != nil {
		return nil, err
	}
	if hasFormat {
		fn := "strptime"
		if try {
			fn = "try_strptime"
		}
		return c.out("CAST("+fn+"(", c.arg(0), ", "+quoteString(f)+") AS DATE)"), nil
	}
	if try {
		return typedCast(c, "sf_try_to_date", "TRY_CAST", "DATE")
	}
	return typedCast(c, "sf_to_date", "CAST", "DATE")
}

// toTimestamp translates TO_TIMESTAMP and its kinds: tz says the result
// has a time zone (TO_TIMESTAMP_LTZ and _TZ).
func toTimestamp(c *call, tz, try bool) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	typ := "TIMESTAMP"
	if tz {
		typ = "TIMESTAMPTZ"
	}
	if scale, ok := c.number(1); ok {
		if scale < 0 || scale > 9 {
			return nil, &Error{Code: CodeInvalidValue, Message: "the scale of " + c.name + " must be from 0 to 9"}
		}
		x := c.op(0)
		if tz {
			return c.out("to_timestamp(", x, " / 1e"+strconv.Itoa(scale)+")"), nil
		}
		if scale <= 6 {
			return c.out("make_timestamp(CAST(", x, " * 1e"+strconv.Itoa(6-scale)+" AS BIGINT))"), nil
		}
		return c.out("make_timestamp(CAST(", x, " // 1e"+strconv.Itoa(scale-6)+" AS BIGINT))"), nil
	}
	f, hasFormat, err := c.formatArg(1)
	if err != nil {
		return nil, err
	}
	if hasFormat {
		fn := "strptime"
		if try {
			fn = "try_strptime"
		}
		return c.out("CAST("+fn+"(", c.arg(0), ", "+quoteString(f)+") AS "+typ+")"), nil
	}
	if try {
		return c.out("TRY_CAST(", c.arg(0), " AS "+typ+")"), nil
	}
	if tz {
		return typedCast(c, "sf_to_timestamp_tz", "CAST", "TIMESTAMPTZ")
	}
	return typedCast(c, "sf_to_timestamp", "CAST", "TIMESTAMP")
}

// toTime translates TO_TIME and TIME, and TRY_TO_TIME.
func toTime(c *call, try bool) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	f, hasFormat, err := c.formatArg(1)
	if err != nil {
		return nil, err
	}
	if hasFormat {
		fn := "strptime"
		if try {
			fn = "try_strptime"
		}
		return c.out("CAST("+fn+"(", c.arg(0), ", "+quoteString(f)+") AS TIME)"), nil
	}
	cast := "CAST("
	if try {
		cast = "TRY_CAST("
	}
	return c.out(cast, c.arg(0), " AS TIME)"), nil
}

// toChar translates TO_CHAR and TO_VARCHAR.
func toChar(c *call) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	if len(c.args) == 1 {
		n := varcharCast(c.args[0])
		n.str = true
		return c.out(n), nil
	}
	s, ok := c.literal(1)
	if !ok {
		return nil, unsupported(c.name+" with a computed format", "give the format as a string literal")
	}
	if looksLikeDateFormat(s) {
		// strftime has no offset as Snowflake writes one (+05:30, +0530,
		// -08), so TZH and TZM are formatted apart.
		var parts []any
		x := c.tsArg(0)
		pieces := splitOffsets(s)
		for i, p := range pieces {
			if i > 0 {
				parts = append(parts, " || ")
			}
			switch strings.ToUpper(p) {
			case "TZH":
				parts = append(parts, fill(tzHours("{x}"), x))
			case "TZM":
				parts = append(parts, fill(tzMinutes("{x}"), x))
			default:
				f, err := dateFormat(p)
				if err != nil {
					return nil, err
				}
				parts = append(parts, "strftime(", x, ", "+quoteString(f)+")")
			}
		}
		n := c.out(parts...)
		if len(pieces) > 1 {
			n = c.out("(", n, ")")
		}
		n.str = true
		return n, nil
	}
	nf, ok := parseNumberFormat(s)
	if !ok {
		return nil, unsupported(fmt.Sprintf("The format %s", quoteString(s)),
			"Igloo formats numbers with 9, 0, a comma, a point, $ and FM")
	}
	n := c.out(nf.sql(render([]*node{c.op(0)})))
	n.str = true
	return n, nil
}

// toNumber translates TO_NUMBER, TO_DECIMAL and TO_NUMERIC:
// (x [, format] [, precision [, scale]]).
func toNumber(c *call, try bool) (*node, error) {
	if err := c.argc(1, 4); err != nil {
		return nil, err
	}
	rest := c.args[1:]
	x := c.arg(0)
	if len(rest) > 0 {
		if _, ok := c.literal(1); ok {
			// A format: Igloo drops group separators, currency and spaces.
			x = c.out("replace(replace(replace(trim(CAST(", c.arg(0), " AS VARCHAR)), ',', ''), '$', ''), ' ', '')")
			rest = rest[1:]
		}
	}
	p, s := 38, 0
	for k, a := range rest {
		cc := &call{args: [][]*node{a}}
		v, ok := cc.number(0)
		if !ok {
			return nil, unsupported(c.name+" with a computed precision or scale", "give them as numbers")
		}
		if k == 0 {
			p = v
		} else {
			s = v
		}
	}
	if p < 1 || p > 38 || s < 0 || s > p {
		return nil, &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("invalid precision %d or scale %d for %s", p, s, c.name)}
	}
	cast := "CAST("
	if try {
		cast = "TRY_CAST("
	}
	n := c.out(cast, x, fmt.Sprintf(" AS DECIMAL(%d,%d))", p, s))
	n.num = true
	return n, nil
}

// castTo translates TO_DOUBLE, TO_BOOLEAN and their TRY_ kinds.
func castTo(c *call, typ string, try bool) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	return castExpr(c.args[0], sqlType{duck: typ}, try), nil
}

// castCall translates CAST(x AS type) and TRY_CAST(x AS type).
func castCall(c *call, try bool) (*node, error) {
	if len(c.args) != 1 {
		return nil, &Error{Code: CodeSyntax, Message: "expected " + c.name + "(value AS type)"}
	}
	a := c.args[0]
	as := -1
	for k := len(a) - 1; k >= 0; k-- {
		if a[k].word() == "AS" {
			as = k
			break
		}
	}
	if as < 0 {
		return nil, &Error{Code: CodeSyntax, Message: "expected " + c.name + "(value AS type)"}
	}
	q := nextSig(a, as)
	if q < 0 {
		return nil, &Error{Code: CodeSyntax, Message: "expected a type after AS"}
	}
	typ, end, ok, err := parseType(a, q)
	if err != nil {
		return nil, err
	}
	if !ok {
		fn := "CAST("
		if try {
			fn = "TRY_CAST("
		}
		return c.out(fn, a, ")"), nil
	}
	if nextSig(a, end-1) >= 0 {
		return nil, &Error{Code: CodeSyntax, Message: "unexpected text after the type in " + c.name}
	}
	n := castExpr(a[:as], typ, try)
	n.pos, n.end = c.span[0].pos, c.span[len(c.span)-1].end
	return n, nil
}

// charIndex translates CHARINDEX(sub, s [, start]).
func charIndex(c *call) (*node, error) {
	if err := c.argc(2, 3); err != nil {
		return nil, err
	}
	if len(c.args) == 2 {
		return c.out("strpos(", c.arg(1), ", ", c.arg(0), ")"), nil
	}
	return c.out("sf_charindex(", c.arg(0), ", ", c.arg(1), ", ", c.arg(2), ")"), nil
}

// position translates POSITION(sub, s [, start]); POSITION(sub IN s) is
// DuckDB's too.
func position(c *call) (*node, error) {
	if len(c.args) == 1 {
		return c.out("position(", c.arg(0), ")"), nil
	}
	return charIndex(c)
}

// splitPart translates SPLIT_PART, where part 0 means the first.
func splitPart(c *call) (*node, error) {
	if err := c.argc(3, 3); err != nil {
		return nil, err
	}
	if n, ok := c.number(2); ok {
		if n == 0 {
			n = 1
		}
		return c.out("split_part(", c.arg(0), ", ", c.arg(1), ", "+strconv.Itoa(n)+")"), nil
	}
	return c.out("split_part(", c.arg(0), ", ", c.arg(1), ", CAST(CASE WHEN ", c.op(2), " = 0 THEN 1 ELSE ", c.op(2), " END AS BIGINT))"), nil
}

// substr translates SUBSTR and SUBSTRING, where position 0 means 1.
func substr(c *call) (*node, error) {
	if len(c.args) == 1 {
		// SUBSTRING(s FROM n FOR m)
		return c.out("substring(", c.arg(0), ")"), nil
	}
	if err := c.argc(2, 3); err != nil {
		return nil, err
	}
	start := c.out("CAST(CASE WHEN ", c.op(1), " = 0 THEN 1 ELSE ", c.op(1), " END AS BIGINT)")
	if n, ok := c.number(1); ok {
		if n == 0 {
			n = 1
		}
		start = c.out(strconv.Itoa(n))
	}
	if len(c.args) == 2 {
		return c.out("substring(", c.arg(0), ", ", start, ")"), nil
	}
	return c.out("substring(", c.arg(0), ", ", start, ", CAST(greatest(", c.op(2), ", 0) AS BIGINT))"), nil
}

// strtok translates STRTOK(s [, delimiters [, n]]): the nth token between
// any of the delimiter characters, skipping empty ones.
func strtok(c *call) (*node, error) {
	if err := c.argc(1, 3); err != nil {
		return nil, err
	}
	delims := c.out("' '")
	if len(c.args) > 1 {
		delims = c.arg(1)
	}
	n := c.out("1")
	if len(c.args) > 2 {
		n = c.arg(2)
	}
	return c.out("sf_tokens(", c.arg(0), ", ", delims, ")[CAST(", n, " AS BIGINT)]"), nil
}

// strtokToArray translates STRTOK_TO_ARRAY.
func strtokToArray(c *call) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	delims := c.out("' '")
	if len(c.args) > 1 {
		delims = c.arg(1)
	}
	n := c.out("to_json(sf_tokens(", c.arg(0), ", ", delims, "))")
	n.json = true
	return n, nil
}

// split translates SPLIT, which returns an ARRAY.
func split(c *call) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	n := c.out("to_json(string_split(", c.arg(0), ", ", c.arg(1), "))")
	n.json = true
	return n, nil
}

// concat translates CONCAT, which is NULL when any argument is.
func concat(c *call) (*node, error) {
	if err := c.argc(1, -1); err != nil {
		return nil, err
	}
	parts := []any{"("}
	for k := range c.args {
		if k > 0 {
			parts = append(parts, " || ")
		}
		parts = append(parts, stringOperand(c, k))
	}
	n := c.out(append(parts, ")")...)
	n.str = true
	return n, nil
}

// stringOperand is argument k as a string.
func stringOperand(c *call, k int) *node {
	a := c.args[k]
	if len(a) == 1 && (a[0].kind == kString || a[0].str || a[0].kind == kVariant) {
		return a[0]
	}
	return c.out("CAST(", c.arg(k), " AS VARCHAR)")
}

// concatWS translates CONCAT_WS, which is NULL when any argument is.
func concatWS(c *call) (*node, error) {
	if err := c.argc(2, -1); err != nil {
		return nil, err
	}
	parts := []any{"("}
	for k := 1; k < len(c.args); k++ {
		if k > 1 {
			parts = append(parts, " || ", stringOperand(c, 0), " || ")
		}
		parts = append(parts, stringOperand(c, k))
	}
	n := c.out(append(parts, ")")...)
	n.str = true
	return n, nil
}

// pad translates LPAD and RPAD, whose padding defaults to a space.
func pad(c *call, fn string) (*node, error) {
	if err := c.argc(2, 3); err != nil {
		return nil, err
	}
	with := c.out("' '")
	if len(c.args) == 3 {
		with = c.arg(2)
	}
	return c.out(fn+"(", c.arg(0), ", CAST(", c.arg(1), " AS INTEGER), ", with, ")"), nil
}

// replaceFn translates REPLACE, whose replacement defaults to ”.
func replaceFn(c *call) (*node, error) {
	if err := c.argc(2, 3); err != nil {
		return nil, err
	}
	with := c.out("''")
	if len(c.args) == 3 {
		with = c.arg(2)
	}
	return c.out("replace(", c.arg(0), ", ", c.arg(1), ", ", with, ")"), nil
}

// initcapDelimiters are the characters after which INITCAP capitalizes, by
// default: white space and punctuation other than the apostrophe.
const initcapDelimiters = `' ' || chr(9) || chr(10) || chr(11) || chr(12) || chr(13) || '!?@"^#$&~_,.:;+-*%/|\[](){}<>'`

// initcap translates INITCAP(s [, delimiters]).
func initcap(c *call) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	delims := c.out(initcapDelimiters)
	if len(c.args) == 2 {
		delims = c.arg(1)
	}
	return c.out("sf_initcap(", c.arg(0), ", ", delims, ")"), nil
}

// editDistance translates EDITDISTANCE(a, b [, max]).
func editDistance(c *call) (*node, error) {
	if err := c.argc(2, 3); err != nil {
		return nil, err
	}
	if len(c.args) == 3 {
		return c.out("least(levenshtein(", c.arg(0), ", ", c.arg(1), "), ", c.arg(2), ")"), nil
	}
	return c.out("levenshtein(", c.arg(0), ", ", c.arg(1), ")"), nil
}

// sha2 translates SHA2(x [, 256]).
func sha2(c *call) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	if len(c.args) == 2 {
		if n, ok := c.number(1); !ok || n != 256 {
			return nil, unsupported(c.name+" with a digest other than 256 bits", "use SHA2(x) or SHA2(x, 256)")
		}
	}
	return c.out("sha256(", c.arg(0), ")"), nil
}

// uuidString translates UUID_STRING(); the name-based form is refused.
func uuidString(c *call) (*node, error) {
	if len(c.args) > 0 {
		return nil, unsupported("UUID_STRING(namespace, name)", "use UUID_STRING() for a random UUID")
	}
	return c.out("CAST(uuid() AS VARCHAR)"), nil
}

// hashFn translates HASH into DuckDB's hash, whose values differ from
// Snowflake's: they are as well distributed, as a signed 64-bit integer,
// but no value matches. The client is told so.
func hashFn(c *call) (*node, error) {
	if err := c.argc(1, -1); err != nil {
		return nil, err
	}
	c.t.notice("HASH computes DuckDB's hash, not Snowflake's: the values differ from Snowflake's, though they are as well distributed")
	args := c.list(0)
	if len(c.args) == 1 && len(c.args[0]) == 1 && c.args[0][0].isOp("*") {
		args = []*node{raw("*COLUMNS(*)")}
	}
	return c.out("CAST(CAST(hash(", args, ") AS HUGEINT) - 9223372036854775808 AS BIGINT)"), nil
}

// regexFlags turns Snowflake's regular expression parameters into RE2's
// inline flags. extract says 'e' was given.
func regexFlags(params string) (flags string, extract bool, err error) {
	set := map[byte]bool{}
	for i := 0; i < len(params); i++ {
		switch p := params[i]; p {
		case 'c':
			delete(set, 'i')
		case 'i', 'm', 's':
			set[p] = true
		case 'e':
			extract = true
		default:
			return "", false, &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("unknown regular expression parameter %q", string(p))}
		}
	}
	for _, f := range []byte{'i', 'm', 's'} {
		if set[f] {
			flags += string(f)
		}
	}
	if flags != "" {
		flags = "(?" + flags + ")"
	}
	return flags, extract, nil
}

// pattern returns argument i as a regular expression with the call's
// parameters (argument p, if given) as inline flags.
func (c *call) pattern(i, p int) (*node, bool, error) {
	params := ""
	if p < len(c.args) {
		s, ok := c.literal(p)
		if !ok {
			return nil, false, unsupported(c.name+" with computed parameters", "give the parameters as a string literal")
		}
		params = s
	}
	flags, extract, err := regexFlags(params)
	if err != nil {
		return nil, false, err
	}
	if flags == "" {
		return c.arg(i), extract, nil
	}
	if s, ok := c.literal(i); ok {
		return c.out(quoteString(flags + s)), extract, nil
	}
	return c.out("'"+flags+"' || ", c.op(i)), extract, nil
}

// regexpLike translates REGEXP_LIKE and RLIKE, which match the whole
// string.
func regexpLike(c *call) (*node, error) {
	if err := c.argc(2, 3); err != nil {
		return nil, err
	}
	p, _, err := c.pattern(1, 2)
	if err != nil {
		return nil, err
	}
	return c.out("regexp_full_match(", c.arg(0), ", ", p, ")"), nil
}

// fromPosition returns s from position argument i on, if given.
func (c *call) fromPosition(s *node, i int) *node {
	if i >= len(c.args) {
		return s
	}
	if n, ok := c.number(i); ok && n == 1 {
		return s
	}
	return c.out("substring(", s, ", ", c.arg(i), ")")
}

// regexpSubstr translates REGEXP_SUBSTR(s, p [, position [, occurrence [,
// parameters [, group]]]]), and REGEXP_SUBSTR_ALL when all.
func regexpSubstr(c *call, all bool) (*node, error) {
	if err := c.argc(2, 6); err != nil {
		return nil, err
	}
	p, extract, err := c.pattern(1, 4)
	if err != nil {
		return nil, err
	}
	group := 0
	if extract {
		group = 1
	}
	if len(c.args) == 6 {
		g, ok := c.number(5)
		if !ok {
			return nil, unsupported(c.name+" with a computed group", "give the group as a number")
		}
		group = g
	}
	s := c.fromPosition(c.arg(0), 2)
	matches := c.out("regexp_extract_all(", s, ", ", p, ", "+strconv.Itoa(group)+")")
	if all {
		n := c.out("to_json(", matches, ")")
		n.json = true
		return n, nil
	}
	occ := c.out("1")
	if len(c.args) > 3 {
		occ = c.arg(3)
	}
	return c.out(matches, "[CAST(", occ, " AS BIGINT)]"), nil
}

// regexpReplace translates REGEXP_REPLACE(s, p [, replacement [, position
// [, occurrence [, parameters]]]]), which replaces every match by default.
func regexpReplace(c *call) (*node, error) {
	if err := c.argc(2, 6); err != nil {
		return nil, err
	}
	p, _, err := c.pattern(1, 5)
	if err != nil {
		return nil, err
	}
	with := c.out("''")
	if len(c.args) > 2 {
		with = c.arg(2)
	}
	opt := ", 'g'"
	if len(c.args) > 4 {
		occ, ok := c.number(4)
		switch {
		case !ok || occ > 1 || occ < 0:
			return nil, unsupported("REGEXP_REPLACE of one occurrence other than the first", "replace every match (occurrence 0) or the first (1)")
		case occ == 1:
			opt = ""
		}
	}
	if len(c.args) > 3 {
		if n, ok := c.number(3); !ok || n != 1 {
			return c.out("(left(", c.arg(0), ", CAST(", c.op(3), " AS INTEGER) - 1) || regexp_replace(substring(", c.arg(0), ", ",
				c.arg(3), "), ", p, ", ", with, opt+"))"), nil
		}
	}
	return c.out("regexp_replace(", c.arg(0), ", ", p, ", ", with, opt+")"), nil
}

// regexpCount translates REGEXP_COUNT(s, p [, position [, parameters]]).
func regexpCount(c *call) (*node, error) {
	if err := c.argc(2, 4); err != nil {
		return nil, err
	}
	p, _, err := c.pattern(1, 3)
	if err != nil {
		return nil, err
	}
	return c.out("len(regexp_extract_all(", c.fromPosition(c.arg(0), 2), ", ", p, "))"), nil
}

// regexpInstr translates REGEXP_INSTR(s, p [, position [, occurrence [,
// option [, parameters [, group]]]]]): the position of a match, or 0.
func regexpInstr(c *call) (*node, error) {
	if err := c.argc(2, 6); err != nil {
		return nil, err
	}
	p, _, err := c.pattern(1, 5)
	if err != nil {
		return nil, err
	}
	pos, occ, opt := c.out("1"), c.out("1"), c.out("0")
	if len(c.args) > 2 {
		pos = c.arg(2)
	}
	if len(c.args) > 3 {
		occ = c.arg(3)
	}
	if len(c.args) > 4 {
		opt = c.arg(4)
	}
	return c.out("sf_regexp_instr(", c.arg(0), ", ", p, ", ", pos, ", ", occ, ", ", opt, ")"), nil
}

// search translates SEARCH(columns, terms [, SEARCH_MODE => mode]), with
// the default analyzer's tokens: runs of letters and digits, in lower
// case.
func search(c *call) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	if _, ok := c.named["ANALYZER"]; ok {
		return nil, unsupported("SEARCH with an ANALYZER", "")
	}
	mode := "OR"
	if m, ok := c.named["SEARCH_MODE"]; ok {
		s, lit := (&call{args: [][]*node{m}}).literal(0)
		if !lit {
			return nil, unsupported("SEARCH with a computed SEARCH_MODE", "write 'OR', 'AND' or 'PHRASE'")
		}
		mode = strings.ToUpper(s)
	}
	// The searched text: one column, or several in parentheses.
	target := c.args[0]
	var text *node
	if len(target) == 1 && target[0].isGroup('(') {
		var parts []any
		for k, a := range splitArgs(target[0].kids) {
			if k > 0 {
				parts = append(parts, " || ' ' || ")
			}
			parts = append(parts, "coalesce(CAST(", operand(a), " AS VARCHAR), '')")
		}
		text = c.out(parts...)
	} else if idx := sig(target); len(idx) > 0 && target[idx[len(idx)-1]].isOp("*") {
		return nil, unsupported("SEARCH of every column (*)", "name the columns to search")
	} else {
		text = c.out("CAST(", c.arg(0), " AS VARCHAR)")
	}
	terms := c.arg(1)
	switch mode {
	case "OR":
		return c.out("list_has_any(sf_search_tokens(", text, "), sf_search_tokens(", terms, "))"), nil
	case "AND":
		return c.out("list_has_all(sf_search_tokens(", text, "), sf_search_tokens(", terms, "))"), nil
	case "PHRASE":
		return c.out("contains(' ' || array_to_string(sf_search_tokens(", text, "), ' ') || ' ', ' ' || array_to_string(sf_search_tokens(",
			terms, "), ' ') || ' ')"), nil
	}
	return nil, unsupported("SEARCH_MODE "+quoteString(mode), "use 'OR', 'AND' or 'PHRASE'")
}

// randomFn translates RANDOM(), a random signed 64-bit integer.
func randomFn(c *call) (*node, error) {
	if len(c.args) > 0 {
		c.t.notice("RANDOM's seed is ignored: Igloo's random numbers cannot be seeded per call")
	}
	return c.out("CAST((random() - 0.5) * 18446744073709549568.0 AS BIGINT)"), nil
}

// isFloatLiteral reports whether a literal argument is a FLOAT in
// Snowflake: written with an exponent or cast to a float type.
func isFloatLiteral(a []*node) bool {
	return len(a) == 1 && a[0].kind == kNumber && strings.ContainsAny(a[0].tok.text, "eE")
}

// uniform translates UNIFORM(min, max, generator): an integer when min and
// max are integers, else a float.
func uniform(c *call) (*node, error) {
	if err := c.argc(3, 3); err != nil {
		return nil, err
	}
	c.generatorNotice(2)
	lo, hi := c.op(0), c.op(1)
	if isFloatLiteral(c.args[0]) || isFloatLiteral(c.args[1]) {
		return c.out("(", lo, " + random() * (", hi, " - ", lo, "))"), nil
	}
	return c.out("CAST(floor(", lo, " + random() * (", hi, " - ", lo, " + 1)) AS BIGINT)"), nil
}

// generatorNotice tells the client that a seeded generator argument is
// not honoured.
func (c *call) generatorNotice(i int) {
	if i >= len(c.args) {
		return
	}
	a := c.args[i]
	if len(a) == 1 && a[0].kind == kNumber {
		c.t.notice("%s's generator %s is ignored: Igloo's random numbers cannot be seeded", c.name, a[0].tok.text)
	}
}

// normal translates NORMAL(mean, stddev, generator) with the Box-Muller
// transform.
func normal(c *call) (*node, error) {
	if err := c.argc(3, 3); err != nil {
		return nil, err
	}
	c.generatorNotice(2)
	return c.out("(", c.op(0), " + ", c.op(1), " * sqrt(-2 * ln(1 - random())) * cos(2 * pi() * random()))"), nil
}

// widthBucket translates WIDTH_BUCKET(x, low, high, n).
func widthBucket(c *call) (*node, error) {
	if err := c.argc(4, 4); err != nil {
		return nil, err
	}
	x, lo, hi, n := c.op(0), c.op(1), c.op(2), c.op(3)
	return c.out("CASE WHEN ", x, " < ", lo, " THEN 0 WHEN ", x, " >= ", hi, " THEN ", n, " + 1 ELSE CAST(floor((",
		x, " - ", lo, ") / (", hi, " - ", lo, ") * ", n, ") AS BIGINT) + 1 END"), nil
}

// orderBy is ORDER BY and the WITHIN GROUP items, for an aggregate's
// arguments, or nothing.
func (c *call) orderBy() []*node {
	if len(c.within) == 0 {
		return nil
	}
	// The group's items have their NULLS FIRST or LAST already (see
	// translator.orderBy).
	return append([]*node{raw(" ORDER BY ")}, c.within...)
}

// listagg translates LISTAGG([DISTINCT] x [, separator]) [WITHIN GROUP
// (ORDER BY ...)], whose separator defaults to ” and whose result for no
// rows is ”.
func listagg(c *call) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	sep := c.out("''")
	if len(c.args) == 2 {
		sep = c.arg(1)
	}
	n := c.out("coalesce(string_agg(", distinctPrefix(c), c.arg(0), ", ", sep, c.orderBy(), ")", c.over, ", '')")
	n.str = true
	return n, nil
}

// arrayAgg translates ARRAY_AGG([DISTINCT] x) [WITHIN GROUP (ORDER BY
// ...)], an ARRAY without NULLs, empty for no rows.
func arrayAgg(c *call) (*node, error) {
	if err := c.argc(1, 1); err != nil {
		return nil, err
	}
	x := jsonArg(c.args[0])
	filter := []any{" FILTER (WHERE ", operand([]*node{x}), " IS NOT NULL)"}
	if x.notNull {
		filter = nil
	}
	parts := append([]any{"coalesce(to_json(list(", distinctPrefix(c), x, c.orderBy(), ")"}, filter...)
	n := c.out(append(parts, c.over, "), CAST('[]' AS JSON))")...)
	n.json = true
	return n, nil
}

// arrayUniqueAgg translates ARRAY_UNIQUE_AGG(x).
func arrayUniqueAgg(c *call) (*node, error) {
	if err := c.argc(1, 1); err != nil {
		return nil, err
	}
	x := jsonArg(c.args[0])
	n := c.out("coalesce(to_json(list(DISTINCT ", x, ") FILTER (WHERE ", operand([]*node{x}), " IS NOT NULL)", c.over,
		"), CAST('[]' AS JSON))")
	n.json = true
	return n, nil
}

// objectAgg translates OBJECT_AGG(key, value), which skips NULL keys and
// values.
func objectAgg(c *call) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	v := jsonArg(c.args[1])
	// DuckDB's json_group_object is a macro, which takes no FILTER; a map
	// of the pairs is the same object, its keys in order.
	n := c.out("coalesce(to_json(map_from_entries(list_sort(list({'k': CAST(", c.arg(0), " AS VARCHAR), 'v': to_json(", v,
		")}) FILTER (WHERE ", c.op(0), " IS NOT NULL AND ", operand([]*node{v}), " IS NOT NULL)", c.over,
		"))), CAST('{}' AS JSON))")
	n.json = true
	return n, nil
}

// ratioToReport translates RATIO_TO_REPORT(x) OVER (...).
func ratioToReport(c *call) (*node, error) {
	if err := c.argc(1, 1); err != nil {
		return nil, err
	}
	if c.over == nil {
		return nil, &Error{Code: CodeSyntax, Message: "RATIO_TO_REPORT needs OVER (...)"}
	}
	return c.out("(", c.op(0), " / nullif(sum(", c.arg(0), ")", c.over, ", 0))"), nil
}

// conditionalTrueEvent translates CONDITIONAL_TRUE_EVENT(condition) OVER
// (...): how many rows so far met the condition.
func conditionalTrueEvent(c *call) (*node, error) {
	if err := c.argc(1, 1); err != nil {
		return nil, err
	}
	if c.over == nil || !c.over[1].isGroup('(') {
		return nil, &Error{Code: CodeSyntax, Message: "CONDITIONAL_TRUE_EVENT needs OVER (ORDER BY ...)"}
	}
	w := c.over[1]
	frame := []*node{raw("(")}
	frame = append(frame, w.kids...)
	hasFrame := false
	for _, n := range w.kids {
		if n.word() == "ROWS" || n.word() == "RANGE" {
			hasFrame = true
		}
	}
	if !hasFrame {
		frame = append(frame, raw(" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"))
	}
	frame = append(frame, raw(")"))
	return c.out("sum(CASE WHEN ", c.arg(0), " THEN 1 ELSE 0 END) OVER ", frame), nil
}

// minMax translates MIN and MAX of a semi-structured value, which compare
// numbers as numbers and text as text, as Snowflake does.
func minMax(c *call) (*node, error) {
	if len(c.args) != 1 || len(c.args[0]) != 1 || c.args[0][0].kind != kVariant || c.distinct {
		n := c.out(strings.ToLower(c.name)+"(", distinctPrefix(c), c.list(0), ")", c.over)
		if len(c.args) == 1 {
			n.sel = &selector{name: strings.ToLower(c.name), arg: c.args[0], distinct: c.distinct, tail: c.over}
		}
		return n, nil
	}
	v := c.args[0][0].v
	str := &node{kind: kVariant, v: &variant{base: v.base, segs: v.segs, json: v.json, mode: vString}}
	num := &node{kind: kVariant, v: &variant{base: v.base, segs: v.segs, json: v.json, mode: vNumber, try: true}}
	v.mode = vJSON
	fn := "arg_min"
	if c.name == "MAX" {
		fn = "arg_max"
	}
	return c.out(fn+"(", c.args[0][0], ", {'n': ", num, ", 's': ", str, "}) FILTER (WHERE ", str, " IS NOT NULL)", c.over), nil
}

// isStar reports whether an argument is a star: *, t.* or either with
// ILIKE or EXCLUDE.
func isStar(arg []*node) bool {
	for _, i := range sig(arg) {
		if arg[i].isOp("*") {
			p := prevSig(arg, i)
			return p < 0 || arg[p].isOp(".")
		}
	}
	return false
}

// nameOf resolves the name that IDENTIFIER(arg) or TABLE(arg) stands for:
// a string literal, or a session variable that holds one.
func (t *translator) nameOf(fn string, arg []*node) (string, error) {
	arg = trim(arg)
	if len(arg) == 1 {
		if s, ok := arg[0].stringValue(); ok {
			return identifierText(s)
		}
		if arg[0].kind == kVar {
			name := arg[0].tok.val
			if t.opts.Variable == nil {
				return "", unsupported(fn+"($variable)", "write the name, or "+fn+"('name')")
			}
			value, ok, err := t.opts.Variable(name)
			if err != nil {
				return "", err
			}
			if !ok {
				return "", &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("session variable $%s is not set", name)}
			}
			return identifierText(value)
		}
	}
	return "", unsupported(fn+"() of an expression", "give "+fn+" a string literal or a session variable")
}

// afterStar reports whether nodes[i] follows a star and its modifiers, as
// in * EXCLUDE (a) REPLACE (...).
func afterStar(nodes []*node, i int) bool {
	p := prevSig(nodes, i)
	for p >= 0 {
		switch {
		case nodes[p].isOp("*"):
			return true
		case nodes[p].isGroup('('):
			p = prevSig(nodes, p)
			if p < 0 || nodes[p].word() != "EXCLUDE" && nodes[p].word() != "RENAME" && nodes[p].word() != "REPLACE" {
				return false
			}
		case nodes[p].kind == kString:
			p = prevSig(nodes, p)
			if p < 0 || nodes[p].word() != "ILIKE" && nodes[p].word() != "LIKE" {
				return false
			}
		default:
			return false
		}
		p = prevSig(nodes, p)
	}
	return false
}

// countFn translates COUNT(DISTINCT a, b), which counts the rows where no
// value is NULL.
func countFn(c *call) (*node, error) {
	if !c.distinct || len(c.args) < 2 {
		return c.out("count(", distinctPrefix(c), c.list(0), ")"), nil
	}
	parts := []any{"count(DISTINCT CASE WHEN "}
	for k := range c.args {
		if k > 0 {
			parts = append(parts, " AND ")
		}
		parts = append(parts, c.op(k), " IS NOT NULL")
	}
	parts = append(parts, " THEN (", c.list(0), ") END)")
	return c.out(parts...), nil
}

// approxCountDistinct translates APPROX_COUNT_DISTINCT and HLL, which take
// several values as COUNT(DISTINCT a, b) does. With the datasketches
// extension they count with its HLL sketch of 4,096 registers, as many as
// Snowflake's, whose error is Snowflake's (about 1.6%); the hash of the
// values makes it count values of any type. DuckDB's approx_count_distinct
// has 64 registers, and errs by about 13%. In a select with GROUP BY (see
// collectGrouped) and as window functions they count exactly: a sketch per
// group or partition is slower than COUNT(DISTINCT), by 25 times with
// millions of groups, and up to twice as fast with a few hundred.
func approxCountDistinct(c *call) (*node, error) {
	if err := c.argc(1, -1); err != nil {
		return nil, err
	}
	fn := "approx_count_distinct("
	switch {
	case c.t.grouped[c.span[0]] || len(c.over) > 0:
		fn = "count(DISTINCT "
	case c.t.opts.Sketches:
		parts := []any{"CAST(coalesce(round(datasketch_hll_estimate(datasketch_hll(12, CASE WHEN "}
		for k := range c.args {
			if k > 0 {
				parts = append(parts, " AND ")
			}
			parts = append(parts, c.op(k), " IS NOT NULL")
		}
		parts = append(parts, " THEN hash(", c.list(0), ") END))), 0) AS BIGINT)")
		return c.out(parts...), nil
	}
	if len(c.args) == 1 {
		return c.out(fn, c.arg(0), ")", c.over), nil
	}
	parts := []any{fn + "CASE WHEN "}
	for k := range c.args {
		if k > 0 {
			parts = append(parts, " AND ")
		}
		parts = append(parts, c.op(k), " IS NOT NULL")
	}
	parts = append(parts, " THEN (", c.list(0), ") END)", c.over)
	return c.out(parts...), nil
}

// timeFromParts translates TIME_FROM_PARTS(h, m, s [, ns]); the parts may
// overflow into the next, as in Snowflake.
func timeFromParts(c *call) (*node, error) {
	if err := c.argc(3, 4); err != nil {
		return nil, err
	}
	ns := c.out("0")
	if len(c.args) == 4 {
		ns = c.op(3)
	}
	return c.out("CAST(TIME '00:00:00' + to_microseconds(CAST(", c.op(0), " AS BIGINT) * 3600000000 + CAST(",
		c.op(1), " AS BIGINT) * 60000000 + CAST(", c.op(2), " AS BIGINT) * 1000000 + CAST(", ns, " AS BIGINT) // 1000) AS TIME)"), nil
}

// toBinary translates TO_BINARY(s [, format]) and TRY_TO_BINARY, whose
// format is HEX (the default), BASE64 or UTF-8.
func toBinary(c *call, try bool) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	format := "HEX"
	if len(c.args) == 2 {
		f, ok := c.literal(1)
		if !ok {
			return nil, unsupported(c.name+" with a format that is not a string literal", "write the format as 'HEX', 'BASE64' or 'UTF-8'")
		}
		format = strings.ToUpper(f)
	}
	var n *node
	switch format {
	case "HEX":
		// DuckDB's unhex would take an odd number of digits.
		if try {
			return c.out("CASE WHEN length(", c.arg(0), ") % 2 = 0 THEN try(unhex(", c.arg(0), ")) END"), nil
		}
		n = c.out("CASE WHEN length(", c.arg(0), ") % 2 = 0 THEN unhex(", c.arg(0), ") ELSE error(",
			quoteString(c.name+": the hexadecimal string has an odd number of digits"), ") END")
	case "BASE64":
		n = c.out("from_base64(", c.arg(0), ")")
	case "UTF-8", "UTF8":
		n = c.out("encode(", c.arg(0), ")")
	default:
		return nil, &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("%s does not know the format %q", c.name, format)}
	}
	if try {
		return c.out("try(", n, ")"), nil
	}
	return n, nil
}

// hexEncode translates HEX_ENCODE(x [, case]): case 0 writes lower case.
func hexEncode(c *call) (*node, error) {
	if err := c.argc(1, 2); err != nil {
		return nil, err
	}
	if len(c.args) == 2 {
		return c.out("CASE WHEN ", c.op(1), " = 0 THEN lower(hex(", c.arg(0), ")) ELSE hex(", c.arg(0), ") END"), nil
	}
	return c.out("hex(", c.arg(0), ")"), nil
}

// approxTopK translates APPROX_TOP_K(x [, k]) into an exact count of the k
// most frequent values, as [[value, count], ...].
func approxTopK(c *call) (*node, error) {
	if err := c.argc(1, 3); err != nil {
		return nil, err
	}
	k := c.out("1")
	if len(c.args) > 1 {
		k = c.arg(1)
	}
	n := c.out("to_json(list_transform(list_slice(list_reverse_sort(list_transform(map_entries(histogram(", c.arg(0),
		")), lambda e: {'c': e.value, 'k': e.key})), 1, CAST(", k, " AS BIGINT)), lambda e: json_array(e.k, e.c)))")
	n.json = true
	return n, nil
}

// toJSON translates TO_JSON, which writes a value as JSON text.
func toJSON(c *call) (*node, error) {
	if err := c.argc(1, 1); err != nil {
		return nil, err
	}
	n := c.out("CAST(to_json(", jsonArg(c.args[0]), ") AS VARCHAR)")
	n.str = true
	return n, nil
}

// toVariant translates TO_VARIANT, TO_OBJECT and TO_ARRAY.
func toVariant(c *call, kind string) (*node, error) {
	if err := c.argc(1, 1); err != nil {
		return nil, err
	}
	x := jsonArg(c.args[0])
	var n *node
	if kind == "ARRAY" {
		n = c.out("CASE WHEN json_type(to_json(", x, ")) = 'ARRAY' THEN to_json(", x, ") WHEN ", operand([]*node{x}),
			" IS NOT NULL THEN json_array(", x, ") END")
	} else {
		n = c.out("to_json(", x, ")")
	}
	n.json = true
	return n, nil
}

// objectConstructCall translates OBJECT_CONSTRUCT and
// OBJECT_CONSTRUCT_KEEP_NULL.
func objectConstructCall(c *call, keepNull bool) (*node, error) {
	for _, a := range c.args {
		if isStar(a) {
			return nil, unsupported(c.name+"(*)", "name the keys and values")
		}
	}
	if len(c.args)%2 != 0 {
		return nil, &Error{Code: CodeSyntax, Message: c.name + " takes keys and values in pairs"}
	}
	var keys, values [][]*node
	for k := 0; k < len(c.args); k += 2 {
		keys, values = append(keys, c.args[k]), append(values, c.args[k+1])
	}
	return objectConstruct(c.span, keys, values, keepNull), nil
}

// objectConstruct builds an OBJECT of keys and values, with its keys in
// order as Snowflake keeps them; without keepNull, keys whose value is
// NULL are left out.
func objectConstruct(span []*node, keys, values [][]*node, keepNull bool) *node {
	if len(keys) == 0 {
		n := seq(span, raw("CAST('{}' AS JSON)"))
		n.json, n.notNull = true, true
		return n
	}
	kids := []*node{raw("to_json(map_from_entries(list_sort(")}
	if !keepNull {
		kids = append(kids, raw("list_filter("))
	}
	kids = append(kids, raw("["))
	for k := range keys {
		if k > 0 {
			kids = append(kids, raw(", "))
		}
		kids = append(kids, raw("{'k': CAST("), seq(keys[k], trim(keys[k])...), raw(" AS VARCHAR), 'v': to_json("),
			jsonArg(values[k]), raw(")}"))
	}
	kids = append(kids, raw("]"))
	if !keepNull {
		kids = append(kids, raw(", lambda e: e.v IS NOT NULL)"))
	}
	kids = append(kids, raw(")))"))
	n := seq(span, kids...)
	n.json, n.notNull = true, true
	return n
}

// arrayConstruct translates ARRAY_CONSTRUCT.
func arrayConstruct(c *call) (*node, error) {
	if len(c.args) == 0 {
		n := c.out("CAST('[]' AS JSON)")
		n.json = true
		return n, nil
	}
	parts := []any{"json_array("}
	for k, a := range c.args {
		if k > 0 {
			parts = append(parts, ", ")
		}
		parts = append(parts, jsonArg(a))
	}
	n := c.out(append(parts, ")")...)
	n.json, n.notNull = true, true
	return n, nil
}

// arrayConstructCompact translates ARRAY_CONSTRUCT_COMPACT, without NULLs.
func arrayConstructCompact(c *call) (*node, error) {
	parts := []any{"to_json(list_filter(["}
	for k, a := range c.args {
		if k > 0 {
			parts = append(parts, ", ")
		}
		parts = append(parts, "to_json(", jsonArg(a), ")")
	}
	n := c.out(append(parts, "], lambda e: e IS NOT NULL AND CAST(e AS VARCHAR) <> 'null'))")...)
	n.json = true
	return n, nil
}

// jsonList is argument i, an ARRAY, as a DuckDB list of JSON values.
func (c *call) jsonList(i int) *node {
	return c.out("CAST(CAST(", jsonArg(c.args[i]), " AS JSON) AS JSON[])")
}

// arraySize translates ARRAY_SIZE, NULL for what is not an ARRAY.
func arraySize(c *call) (*node, error) {
	if err := c.argc(1, 1); err != nil {
		return nil, err
	}
	x := jsonArg(c.args[0])
	return c.out("CASE WHEN json_type(CAST(", x, " AS JSON)) = 'ARRAY' THEN CAST(json_array_length(CAST(", x,
		" AS JSON)) AS BIGINT) END"), nil
}

// arrayContains translates ARRAY_CONTAINS(value, array).
func arrayContains(c *call) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	return c.out("list_contains(", c.jsonList(1), ", to_json(", jsonArg(c.args[0]), "))"), nil
}

// arrayPosition translates ARRAY_POSITION(value, array), counting from 0.
func arrayPosition(c *call) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	return c.out("(list_position(", c.jsonList(1), ", to_json(", jsonArg(c.args[0]), ")) - 1)"), nil
}

// arrayToString translates ARRAY_TO_STRING, which writes NULL elements as
// empty strings.
func arrayToString(c *call) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	n := c.out("array_to_string(list_transform(", c.jsonList(0), ", lambda e: coalesce(json_extract_string(e, '$'), '')), ",
		c.arg(1), ")")
	n.str = true
	return n, nil
}

// arraySlice translates ARRAY_SLICE(array, from, to), counting from 0 and
// excluding to.
func arraySlice(c *call) (*node, error) {
	if err := c.argc(3, 3); err != nil {
		return nil, err
	}
	from, to := c.op(1), c.op(2)
	n := c.out("to_json(list_slice(", c.jsonList(0), ", CASE WHEN ", from, " < 0 THEN ", from, " ELSE ", from,
		" + 1 END, CASE WHEN ", to, " < 0 THEN ", to, " - 1 ELSE ", to, " END))")
	n.json = true
	return n, nil
}

// arrayList translates the ARRAY functions DuckDB's list functions do:
// value says the second argument is an element.
func arrayList(c *call, fn string, args int, value bool) (*node, error) {
	if err := c.argc(args, args); err != nil {
		return nil, err
	}
	parts := []any{"to_json(" + fn + "("}
	switch {
	case args == 1:
		parts = append(parts, c.jsonList(0))
	case fn == "list_prepend":
		parts = append(parts, "to_json(", jsonArg(c.args[1]), "), ", c.jsonList(0))
	case value:
		parts = append(parts, c.jsonList(0), ", to_json(", jsonArg(c.args[1]), ")")
	default:
		parts = append(parts, c.jsonList(0), ", ", c.jsonList(1))
	}
	n := c.out(append(parts, "))")...)
	n.json = true
	return n, nil
}

// arraysOverlap translates ARRAYS_OVERLAP.
func arraysOverlap(c *call) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	return c.out("list_has_any(", c.jsonList(0), ", ", c.jsonList(1), ")"), nil
}

// arrayCompact translates ARRAY_COMPACT, which drops NULLs.
func arrayCompact(c *call) (*node, error) {
	if err := c.argc(1, 1); err != nil {
		return nil, err
	}
	n := c.out("to_json(list_filter(", c.jsonList(0), ", lambda e: CAST(e AS VARCHAR) <> 'null'))")
	n.json = true
	return n, nil
}

// arrayGenerateRange translates ARRAY_GENERATE_RANGE(start, stop [, step]).
func arrayGenerateRange(c *call) (*node, error) {
	if err := c.argc(2, 3); err != nil {
		return nil, err
	}
	n := c.out("to_json(range(", c.list(0), "))")
	n.json = true
	return n, nil
}

// objectInsert translates OBJECT_INSERT(object, key, value).
func objectInsert(c *call) (*node, error) {
	if err := c.argc(3, 4); err != nil {
		return nil, err
	}
	n := c.out("json_merge_patch(CAST(", jsonArg(c.args[0]), " AS JSON), json_object(", c.arg(1), ", ", jsonArg(c.args[2]), "))")
	n.json = true
	return n, nil
}

// get translates GET(value, key or index).
func get(c *call) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	var seg pathSeg
	if s, ok := c.literal(1); ok {
		seg = pathSeg{key: s}
	} else if n, ok := c.number(1); ok && n >= 0 {
		seg = pathSeg{index: n, isIndex: true}
	} else {
		seg = pathSeg{dyn: c.arg(1)}
	}
	return variantOf(c, []pathSeg{seg}), nil
}

// variantOf makes a semi-structured access of argument 0.
func variantOf(c *call, segs []pathSeg) *node {
	base := c.args[0]
	if len(base) == 1 && base[0].kind == kVariant {
		v := base[0].v
		return &node{kind: kVariant, v: &variant{base: v.base, segs: append(append([]pathSeg{}, v.segs...), segs...), json: v.json},
			pos: c.span[0].pos, end: c.span[len(c.span)-1].end}
	}
	return &node{kind: kVariant, v: &variant{base: c.arg(0), segs: segs}, pos: c.span[0].pos, end: c.span[len(c.span)-1].end}
}

// getPath translates GET_PATH(value, 'path').
func getPath(c *call) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	s, ok := c.literal(1)
	if !ok {
		return nil, unsupported("GET_PATH with a computed path", "give the path as a string literal")
	}
	segs, err := parsePath(s)
	if err != nil {
		return nil, err
	}
	return variantOf(c, segs), nil
}

// parsePath reads a Snowflake path such as a.b[0]."c d".
func parsePath(s string) ([]pathSeg, error) {
	toks, err := lex(s)
	if err != nil {
		return nil, err
	}
	nodes, err := build(toks)
	if err != nil {
		return nil, err
	}
	var segs []pathSeg
	expectName := true
	for _, n := range nodes {
		switch {
		case !n.significant():
		case n.isOp(".") && !expectName:
			expectName = true
		case n.isOp(":") && !expectName:
			expectName = true
		case expectName && (n.kind == kWord || n.kind == kQuoted):
			name, _, _ := n.name()
			segs = append(segs, pathSeg{key: name})
			expectName = false
		case n.isGroup('['):
			t := &translator{}
			seg, err := t.elementSeg(n)
			if err != nil || seg.dyn != nil {
				return nil, &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("invalid path %q", s)}
			}
			segs = append(segs, seg)
			expectName = false
		default:
			return nil, &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("invalid path %q", s)}
		}
	}
	return segs, nil
}

// typeOf translates TYPEOF of a semi-structured value.
func typeOf(c *call) (*node, error) {
	if err := c.argc(1, 1); err != nil {
		return nil, err
	}
	x := jsonArg(c.args[0])
	n := c.out("CASE json_type(CAST(", x, " AS JSON)) WHEN 'UBIGINT' THEN 'INTEGER' WHEN 'BIGINT' THEN 'INTEGER' "+
		"WHEN 'DOUBLE' THEN 'DECIMAL' WHEN 'NULL' THEN 'NULL_VALUE' ELSE json_type(CAST(", x, " AS JSON)) END")
	n.str = true
	return n, nil
}

// truncTyped truncates x to part p with sf_date_trunc, which keeps x's
// type, or with date_trunc where DuckDB cannot choose its overload.
func truncTyped(c *call, p string, x []*node) *node {
	return typed(x, func(x *node) *node {
		return c.out("sf_date_trunc('"+p+"', ", x, ")")
	}, func(x *node) *node {
		return c.out("date_trunc('"+p+"', ", x, ")")
	})
}

// typedCast calls macro, a conversion with overloads by type, on argument
// 0, or casts it with cast (CAST or TRY_CAST) to typ where DuckDB cannot
// choose the macro's overload.
func typedCast(c *call, macro, cast, typ string) (*node, error) {
	return typed(c.args[0], func(x *node) *node {
		return c.out(macro+"(", x, ")")
	}, func(x *node) *node {
		return c.out(cast+"(", x, " AS "+typ+")")
	}), nil
}

// addMonths translates ADD_MONTHS(x, n), which keeps the last day of a
// month the last day of the month it moves to.
func addMonths(c *call) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	n := c.op(1)
	return typed(c.args[0], func(x *node) *node {
		return c.out("sf_add_months_eom(", x, ", ", n, ")")
	}, func(x *node) *node {
		return c.out("CASE WHEN CAST(", x, " AS DATE) = last_day(", x, ") THEN CAST(last_day(", x, " + to_months(CAST(", n,
			" AS INTEGER))) AS TIMESTAMP) + (CAST(", x, " AS TIMESTAMP) - date_trunc('day', CAST(", x,
			" AS TIMESTAMP))) ELSE ", x, " + to_months(CAST(", n, " AS INTEGER)) END")
	}), nil
}
