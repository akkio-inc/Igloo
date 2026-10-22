package snowflake

import (
	"strings"
)

// node is a token, a bracketed group of nodes, or text the translation
// produced. A statement is a list of nodes; groups nest.
type node struct {
	kind kind
	tok  token
	// open is a group's opening bracket: '(', '[' or '{'.
	open byte
	// kids are a group's or a seq's nodes.
	kids []*node
	// text is a raw node's DuckDB text.
	text string
	// v is a variant node's access.
	v *variant
	// pos and end are the node's span in the statement as written.
	pos, end int
	// json, str and num say a translated expression is known to be JSON,
	// VARCHAR or a number, and notNull that it is never NULL.
	json, str, num, notNull bool
	// sel, when set, says the node is a call of MIN or MAX (see typed).
	sel *selector
	// elems, when set, are the elements of the array constant the node
	// builds, [a, b] or ARRAY_CONSTRUCT(a, b), each as the JSON functions
	// take it (see jsonArg), which FLATTEN unnests without building the
	// array.
	elems []*node
	// column says the node reads a column (see semiArg).
	column bool
	// semi is 'A' for a node known to be an ARRAY and 'O' for one known to
	// be an OBJECT, which CREATE TABLE ... AS stores as such columns (see
	// semiColumns).
	semi byte
}

// closer is the bracket that closes open.
func closer(open byte) byte {
	switch open {
	case '(':
		return ')'
	case '[':
		return ']'
	}
	return '}'
}

// build nests tokens into groups by their brackets.
func build(toks []token) ([]*node, error) {
	type frame struct {
		n     *node
		nodes []*node
	}
	stack := []frame{{}}
	for _, t := range toks {
		top := &stack[len(stack)-1]
		if t.kind == kOp && len(t.text) == 1 {
			switch c := t.text[0]; c {
			case '(', '[', '{':
				stack = append(stack, frame{n: &node{kind: kGroup, open: c, pos: t.pos}})
				continue
			case ')', ']', '}':
				if len(stack) == 1 || closer(top.n.open) != c {
					return nil, syntaxError(t.pos, "unbalanced "+string(c))
				}
				g := top.n
				g.kids, g.end = top.nodes, t.end
				stack = stack[:len(stack)-1]
				parent := &stack[len(stack)-1]
				parent.nodes = append(parent.nodes, g)
				continue
			}
		}
		n := &node{kind: t.kind, tok: t, pos: t.pos, end: t.end}
		if t.kind == kRaw {
			n.text = t.text // a literal the lexer translated, as X'...'
		}
		top.nodes = append(top.nodes, n)
	}
	if len(stack) != 1 {
		g := stack[len(stack)-1].n
		return nil, syntaxError(g.pos, "unclosed "+string(g.open))
	}
	return stack[0].nodes, nil
}

// String renders the node as DuckDB SQL.
func (n *node) String() string {
	var b strings.Builder
	n.write(&b)
	return b.String()
}

func (n *node) write(b *strings.Builder) {
	switch n.kind {
	case kGroup:
		b.WriteByte(n.open)
		writeNodes(b, n.kids)
		b.WriteByte(closer(n.open))
	case kSeq:
		writeNodes(b, n.kids)
	case kRaw:
		b.WriteString(n.text)
	case kVariant:
		b.WriteString(n.v.render())
	case kComment:
		b.WriteString(commentText(n.tok.text))
	case kVar:
		// $name, a session variable.
		b.WriteString("getvariable(" + quoteString(n.tok.val) + ")")
	case kString:
		if n.tok.raw {
			b.WriteString(n.tok.text)
		} else {
			b.WriteString(quoteString(n.tok.val))
		}
	default:
		b.WriteString(n.tok.text)
	}
}

func writeNodes(b *strings.Builder, nodes []*node) {
	for _, n := range nodes {
		n.write(b)
	}
}

// render renders nodes as DuckDB SQL.
func render(nodes []*node) string {
	var b strings.Builder
	writeNodes(&b, nodes)
	return b.String()
}

// raw makes a node of DuckDB text.
func raw(text string) *node { return &node{kind: kRaw, text: text} }

// seq makes a node of nodes, spanning from the first to the last of span
// in the statement as written.
func seq(span []*node, kids ...*node) *node {
	n := &node{kind: kSeq, kids: kids}
	if len(span) > 0 {
		n.pos, n.end = span[0].pos, span[len(span)-1].end
	}
	return n
}

// group makes a parenthesized group of nodes.
func group(kids ...*node) *node { return &node{kind: kGroup, open: '(', kids: kids} }

// significant reports whether the node is more than white space or a
// comment.
func (n *node) significant() bool { return n.kind != kSpace && n.kind != kComment }

// sig returns the indexes of the significant nodes.
func sig(nodes []*node) []int {
	idx := make([]int, 0, len(nodes))
	for i, n := range nodes {
		if n.significant() {
			idx = append(idx, i)
		}
	}
	return idx
}

// trim drops white space and comments from both ends of nodes.
func trim(nodes []*node) []*node {
	for len(nodes) > 0 && !nodes[0].significant() {
		nodes = nodes[1:]
	}
	for len(nodes) > 0 && !nodes[len(nodes)-1].significant() {
		nodes = nodes[:len(nodes)-1]
	}
	return nodes
}

// isWord reports whether n is the unquoted word w, in any case.
func (n *node) isWord(w string) bool {
	return n != nil && n.kind == kWord && strings.EqualFold(n.tok.text, w)
}

// word returns n's text in upper case if it is an unquoted word, else "".
func (n *node) word() string {
	if n == nil || n.kind != kWord {
		return ""
	}
	return strings.ToUpper(n.tok.text)
}

// isOp reports whether n is the operator op.
func (n *node) isOp(op string) bool { return n != nil && n.kind == kOp && n.tok.text == op }

// isGroup reports whether n is a group opened by open.
func (n *node) isGroup(open byte) bool { return n != nil && n.kind == kGroup && n.open == open }

// name returns the identifier n is: an unquoted word as written, or a
// quoted identifier's name. ok is false for anything else.
func (n *node) name() (name string, quoted, ok bool) {
	switch {
	case n == nil:
		return "", false, false
	case n.kind == kWord:
		return n.tok.text, false, true
	case n.kind == kQuoted:
		return n.tok.val, true, true
	}
	return "", false, false
}

// isString reports whether n is a string literal, and returns its value.
func (n *node) stringValue() (string, bool) {
	if n == nil || n.kind != kString || n.tok.raw {
		return "", false
	}
	return n.tok.val, true
}

// splitArgs splits a group's nodes at its top-level commas.
func splitArgs(nodes []*node) [][]*node {
	var out [][]*node
	start := 0
	for i, n := range nodes {
		if n.isOp(",") {
			out = append(out, nodes[start:i])
			start = i + 1
		}
	}
	if len(trim(nodes)) == 0 && len(out) == 0 {
		return nil
	}
	return append(out, nodes[start:])
}

// joinArgs joins arguments with commas.
func joinArgs(args [][]*node) []*node {
	var out []*node
	for i, a := range args {
		if i > 0 {
			out = append(out, raw(", "))
		}
		out = append(out, trim(a)...)
	}
	return out
}

// operand makes nodes, one argument, safe to place next to an operator:
// in parentheses unless it is a single node.
func operand(nodes []*node) *node {
	nodes = trim(nodes)
	if len(nodes) == 1 {
		return nodes[0]
	}
	return group(nodes...)
}

// text renders nodes, trimmed.
func text(nodes []*node) string { return render(trim(nodes)) }

// source is the text of nodes as the statement wrote them.
func source(src string, nodes []*node) string {
	start, end := -1, -1
	for _, n := range trim(nodes) {
		if n.end <= n.pos {
			continue
		}
		if start < 0 || n.pos < start {
			start = n.pos
		}
		if n.end > end {
			end = n.end
		}
	}
	if start < 0 || end > len(src) {
		return ""
	}
	return src[start:end]
}

// prevSig returns the index of the significant node before i, or -1.
func prevSig(nodes []*node, i int) int {
	for j := i - 1; j >= 0; j-- {
		if nodes[j].significant() {
			return j
		}
	}
	return -1
}

// nextSigOr returns the index of the significant node after i, or i
// itself when there is none.
func nextSigOr(nodes []*node, i int) int {
	if j := nextSig(nodes, i); j >= 0 {
		return j
	}
	return i
}

// nextSig returns the index of the significant node after i, or -1.
func nextSig(nodes []*node, i int) int {
	for j := i + 1; j < len(nodes); j++ {
		if nodes[j].significant() {
			return j
		}
	}
	return -1
}

// endsOperand reports whether n can end an operand: a name, a literal, a
// group or a translated expression. An operator or keyword after it then
// applies to it.
func endsOperand(n *node) bool {
	switch n.kind {
	case kQuoted, kString, kNumber, kVar, kParam, kRaw, kSeq, kVariant:
		return true
	case kGroup:
		return n.open == '(' || n.open == '['
	case kWord:
		return !nonOperandWords[strings.ToUpper(n.tok.text)]
	}
	return false
}

// nonOperandWords are keywords that cannot end an operand.
var nonOperandWords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "AND": true, "OR": true, "NOT": true, "ON": true,
	"WHEN": true, "THEN": true, "ELSE": true, "CASE": true, "IN": true, "IS": true, "LIKE": true,
	"ILIKE": true, "RLIKE": true, "REGEXP": true, "BETWEEN": true, "AS": true, "BY": true, "HAVING": true,
	"QUALIFY": true, "JOIN": true, "UNION": true, "EXCEPT": true, "MINUS": true, "INTERSECT": true,
	"DISTINCT": true, "ALL": true, "ANY": true, "SOME": true, "EXISTS": true, "WITH": true, "SET": true,
	"VALUES": true, "INTO": true, "RETURN": true, "LIMIT": true, "OFFSET": true, "USING": true,
	"OVER": true, "PARTITION": true, "WITHIN": true, "LATERAL": true, "TABLE": true, "INTERVAL": true,
}
