package snowflake

import "strings"

// Snowflake converts values where DuckDB asks for a cast: it reads any
// value as text in a string function or LIKE, and casts text compared
// with a date, a time or a number to that type. Which conversion a
// statement needs depends on its columns' types, which the translation
// does not know, so Options.Implicit has the translation make them all
// through the macros sf_str and sf_as, which keep a value of the type
// already wanted as it is. Igloo translates with it only to retry a
// statement that DuckDB could not bind without it (see pgwire), so
// statements that bind as written stay exactly as they were.

// textArgs are the arguments, by position, that string functions read as
// text.
var textArgs = map[string][]int{
	"UPPER": {0}, "LOWER": {0}, "TRIM": {0, 1}, "LTRIM": {0, 1}, "RTRIM": {0, 1}, "SUBSTR": {0}, "SUBSTRING": {0},
	"LEFT": {0}, "RIGHT": {0}, "LENGTH": {0}, "LEN": {0}, "REPLACE": {0, 1, 2}, "SPLIT": {0, 1}, "SPLIT_PART": {0, 1},
	"STRTOK": {0, 1}, "STRTOK_TO_ARRAY": {0, 1}, "REGEXP_LIKE": {0}, "RLIKE": {0}, "REGEXP_SUBSTR": {0},
	"REGEXP_REPLACE": {0, 2}, "REGEXP_COUNT": {0}, "REGEXP_INSTR": {0}, "REGEXP_SUBSTR_ALL": {0}, "CONTAINS": {0, 1},
	"STARTSWITH": {0, 1}, "ENDSWITH": {0, 1}, "CHARINDEX": {0, 1}, "LPAD": {0, 2}, "RPAD": {0, 2}, "INITCAP": {0},
	"REVERSE": {0}, "TRANSLATE": {0, 1, 2}, "MD5": {0}, "MD5_HEX": {0}, "SHA1": {0}, "SHA1_HEX": {0}, "SHA2": {0},
	"SHA2_HEX": {0}, "EDITDISTANCE": {0, 1}, "JAROWINKLER_SIMILARITY": {0, 1}, "ASCII": {0}, "REPEAT": {0},
	"UNICODE": {0}, "BASE64_ENCODE": {0}, "HEX_ENCODE": {0},
}

// implicitText has the call read its text arguments through sf_str.
func (c *call) implicitText() {
	for _, k := range textArgs[c.name] {
		if k < len(c.args) && !isTextOperand(c.args[k]) {
			n := strOf(c.args[k])
			c.args[k] = []*node{n}
		}
	}
}

// strOf is x read as text.
func strOf(x []*node) *node {
	x = trim(x)
	kids := append(append([]*node{raw("sf_str(")}, x...), raw(")"))
	n := seq(x, kids...)
	n.str = true
	return n
}

// isTextOperand reports whether x is known to be text already, or is a
// semi-structured path, which variantModes reads as the call needs.
func isTextOperand(x []*node) bool {
	x = trim(x)
	return len(x) == 1 && (x[0].kind == kString || x[0].str || x[0].kind == kVariant)
}

// implicitCasts converts the operands of comparisons, BETWEEN and LIKE at
// this level as Snowflake does, when Options.Implicit is set. Only
// operands that stand alone between lower-precedence words or the ends of
// the list are converted, so that the conversion cannot change what the
// operator applies to; literals are left to DuckDB, which casts them as
// Snowflake would.
func (t *translator) implicitCasts(nodes []*node) ([]*node, error) {
	if !t.opts.Implicit {
		return nodes, nil
	}
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]
		switch {
		case n.kind == kOp && comparisonOps[n.tok.text] && !assignment(nodes, i):
			ls, le, ok := leftOperand(nodes, i)
			if !ok {
				continue
			}
			rs, re, ok := rightOperand(nodes, i)
			if !ok || literalOperand(nodes[ls:le+1]) || literalOperand(nodes[rs:re+1]) {
				continue
			}
			l, r := trim(nodes[ls:le+1]), trim(nodes[rs:re+1])
			out := seq(nodes[ls:re+1], asOf(l, r), raw(" "+n.tok.text+" "), asOf(r, l))
			nodes = replace(nodes, ls, re+1, out)
			i = ls
		case n.word() == "BETWEEN":
			x := i
			if p := prevSig(nodes, i); p >= 0 && nodes[p].word() == "NOT" {
				x = p
			}
			xs, xe, ok := leftOperand(nodes, x)
			if !ok || literalOperand(nodes[xs:xe+1]) {
				continue
			}
			ls, le, ok := rightOperand(nodes, i)
			if !ok {
				continue
			}
			and := nextSig(nodes, le)
			if and < 0 || nodes[and].word() != "AND" {
				continue
			}
			hs, he, ok := rightOperand(nodes, and)
			if !ok {
				continue
			}
			v, lo, hi := trim(nodes[xs:xe+1]), trim(nodes[ls:le+1]), trim(nodes[hs:he+1])
			loLit, hiLit := literalOperand(lo), literalOperand(hi)
			if loLit && hiLit {
				continue
			}
			other := lo
			if loLit {
				other = hi
			}
			parts := []*node{asOf(v, other), raw(" " + render(trim(nodes[xe+1:i+1])) + " ")}
			for k, b := range [][]*node{lo, hi} {
				if k == 1 {
					parts = append(parts, raw(" AND "))
				}
				if literalOperand(b) {
					parts = append(parts, operand(b))
				} else {
					parts = append(parts, asOf(b, v))
				}
			}
			nodes = replace(nodes, xs, he+1, seq(nodes[xs:he+1], parts...))
			i = xs
		case n.word() == "LIKE" || n.word() == "ILIKE":
			x := i
			if p := prevSig(nodes, i); p >= 0 && nodes[p].word() == "NOT" {
				x = p
			}
			if q := nextSig(nodes, i); q >= 0 && (nodes[q].word() == "ANY" || nodes[q].word() == "ALL" || nodes[q].word() == "SOME") {
				continue
			}
			if ps, pe, ok := rightOperand(nodes, i); ok && !literalOperand(nodes[ps:pe+1]) && !isTextOperand(nodes[ps:pe+1]) {
				nodes = replace(nodes, ps, pe+1, strOf(nodes[ps:pe+1]))
			}
			if xs, xe, ok := leftOperand(nodes, x); ok && !literalOperand(nodes[xs:xe+1]) && !isTextOperand(nodes[xs:xe+1]) {
				nodes = replace(nodes, xs, xe+1, strOf(nodes[xs:xe+1]))
				i = xs
			}
		}
	}
	return nodes, nil
}

// assignment reports whether the = at nodes[i] assigns, as in UPDATE's
// and MERGE's SET a = b, c = d: the nearest clause word before it is SET.
func assignment(nodes []*node, i int) bool {
	for j := prevSig(nodes, i); j >= 0; j = prevSig(nodes, j) {
		switch nodes[j].word() {
		case "SET":
			return true
		case "WHERE", "ON", "WHEN", "AND", "OR", "SELECT", "HAVING", "QUALIFY", "THEN", "ELSE", "CASE":
			return false
		}
	}
	return false
}

// asOf is a compared with b, through sf_as.
func asOf(a, b []*node) *node {
	kids := append(append(append(append([]*node{raw("sf_as(")}, a...), raw(", ")), b...), raw(")"))
	return seq(a, kids...)
}

// opBoundary are the words that bind more loosely than a comparison, on
// either side of one.
var opBoundary = map[string]bool{
	"AND": true, "OR": true, "NOT": true, "WHERE": true, "ON": true, "WHEN": true, "THEN": true, "ELSE": true,
	"END": true, "HAVING": true, "QUALIFY": true, "SELECT": true, "DISTINCT": true, "ORDER": true, "GROUP": true,
	"LIMIT": true, "OFFSET": true, "WINDOW": true, "UNION": true, "EXCEPT": true, "INTERSECT": true, "MINUS": true,
	"FROM": true, "AS": true, "ASC": true, "DESC": true, "NULLS": true, "ESCAPE": true, "RETURN": true,
}

// bound reports whether nodes[i], the significant node beside an operand,
// or the list's end (i < 0), separates it from any other operator: as do
// the words of opBoundary, and the text the translation writes to open or
// close a parenthesis, such as the ON ( and ) of a WHERE moved into a
// FLATTEN's join (see whereOn).
func bound(nodes []*node, i int) bool {
	if i < 0 || nodes[i].isOp(",") || nodes[i].isOp(";") || opBoundary[nodes[i].word()] {
		return true
	}
	text := strings.TrimSpace(nodes[i].text)
	return nodes[i].kind == kRaw && (strings.HasSuffix(text, "(") || strings.HasPrefix(text, ")"))
}

// leftOperand is the operand before the operator at nodes[i], when it
// stands alone: its first and last indexes.
func leftOperand(nodes []*node, i int) (start, end int, ok bool) {
	end = prevSig(nodes, i)
	if end < 0 || !endsOperand(nodes[end]) {
		return 0, 0, false
	}
	start = operandStart(nodes, end)
	if !endsOperand(nodes[start]) && !nodes[start].isGroup('(') || !bound(nodes, prevSig(nodes, start)) {
		return 0, 0, false
	}
	return start, end, true
}

// rightOperand is the operand after the operator at nodes[i], when it
// stands alone: a dotted name with perhaps a call and element accesses
// after it, or a single node.
func rightOperand(nodes []*node, i int) (start, end int, ok bool) {
	start = nextSig(nodes, i)
	if start < 0 || !endsOperand(nodes[start]) {
		return 0, 0, false
	}
	end = start
	if n := nodes[start]; n.kind == kWord || n.kind == kQuoted {
		for {
			d := nextSig(nodes, end)
			if d < 0 || !nodes[d].isOp(".") {
				break
			}
			e := nextSig(nodes, d)
			if e < 0 || nodes[e].kind != kWord && nodes[e].kind != kQuoted {
				return 0, 0, false
			}
			end = e
		}
		if g := nextSig(nodes, end); g >= 0 && nodes[g].isGroup('(') {
			end = g
		}
	}
	for {
		g := nextSig(nodes, end)
		if g < 0 || !nodes[g].isGroup('[') {
			break
		}
		end = g
	}
	if !bound(nodes, nextSig(nodes, end)) {
		return 0, 0, false
	}
	return start, end, true
}

// literalOperand reports whether x is a constant DuckDB casts as Snowflake
// would: a string, a number, NULL, TRUE or FALSE, or a typed literal.
func literalOperand(x []*node) bool {
	x = trim(x)
	if len(x) != 1 {
		return false
	}
	switch n := x[0]; {
	case n.kind == kString || n.kind == kNumber:
		return true
	case n.kind == kWord:
		w := n.word()
		return w == "NULL" || w == "TRUE" || w == "FALSE"
	}
	return false
}
