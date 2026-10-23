package snowflake

import "regexp"

// onceOnly matches what sf_div must not compute more than once: a
// subquery or a volatile function.
var onceOnly = regexp.MustCompile(`(?i)\bselect\b|\b(random|uuid|gen_random_uuid|nextval|setseed)\s*\(`)

// divisions translates a / b to sf_div(a, b), which divides two NUMBERs
// as Snowflake does (see the macro). The dividend is everything the
// division applies to: the operands of the multiplications and modulos
// before it, which bind as tightly and from the left. A division whose
// operands it cannot tell stays DuckDB's.
func (t *translator) divisions(nodes []*node) ([]*node, error) {
	for i := 0; i < len(nodes); i++ {
		if !nodes[i].isOp("/") {
			continue
		}
		p, q := prevSig(nodes, i), nextSig(nodes, i)
		if p < 0 || q < 0 || !endsOperand(nodes[p]) {
			continue
		}
		start, end := dividendStart(nodes, p), divisorEnd(nodes, q)
		if start < 0 || end < 0 {
			continue
		}
		left, right := trim(nodes[start:i]), trim(nodes[i+1:end+1])
		fn := "sf_div("
		if onceOnly.MatchString(render(left)) || onceOnly.MatchString(render(right)) {
			fn = "sf_div_once("
		}
		kids := append([]*node{raw(fn)}, left...)
		kids = append(kids, raw(", "))
		kids = append(kids, right...)
		out := seq(nodes[start:end+1], append(kids, raw(")"))...)
		out.num = true
		nodes = replace(nodes, start, end+1, out)
		i = start
	}
	return nodes, nil
}

// dividendStart returns where the dividend of a division starts whose
// operator follows nodes[p]: the first of the operands, each with its
// unary signs, that the multiplications and modulos before it join. -1
// when one is not an operand termStart knows, or a division is among
// them that divisions left as it was.
func dividendStart(nodes []*node, p int) int {
	for {
		start := termStart(nodes, p)
		if start < 0 {
			return -1
		}
		for {
			s := prevSig(nodes, start)
			if s < 0 || !nodes[s].isOp("-") && !nodes[s].isOp("+") {
				break
			}
			if b := prevSig(nodes, s); b >= 0 && endsOperand(nodes[b]) {
				break // a subtraction or an addition
			}
			start = s
		}
		o := prevSig(nodes, start)
		if o >= 0 && nodes[o].isOp("/") {
			return -1
		}
		if o < 0 || !nodes[o].isOp("*") && !nodes[o].isOp("%") {
			return start
		}
		if p = prevSig(nodes, o); p < 0 || !endsOperand(nodes[p]) {
			return -1
		}
	}
}

// termStart returns where the operand ending at nodes[p] starts: a name,
// a literal, a call (with its WITHIN GROUP, FILTER, IGNORE or RESPECT
// NULLS and OVER), a parenthesized expression, CASE ... END or a
// translated expression, with its element accesses. -1 when it is none of
// these.
func termStart(nodes []*node, p int) int {
	n := nodes[p]
	switch {
	case n.word() == "END":
		return caseStart(nodes, p)
	case n.isGroup('('), n.kind == kWord && !nonOperandWords[n.word()], n.kind == kQuoted:
	case n.isGroup('['), n.kind == kNumber, n.kind == kString, n.kind == kVar, n.kind == kParam, n.kind == kRaw,
		n.kind == kSeq, n.kind == kVariant:
		return operandStart(nodes, p)
	default:
		return -1
	}
	k := p
	if b := prevSig(nodes, k); b >= 0 && nodes[b].word() == "OVER" {
		if k = prevSig(nodes, b); k >= 0 && nodes[k].word() == "NULLS" {
			r := prevSig(nodes, k)
			if r < 0 || nodes[r].word() != "IGNORE" && nodes[r].word() != "RESPECT" {
				return -1
			}
			k = prevSig(nodes, r)
		}
		if k < 0 {
			return -1
		}
	}
	if nodes[k].isGroup('(') {
		if b := prevSig(nodes, k); b >= 0 && nodes[b].word() == "FILTER" {
			if k = prevSig(nodes, b); k < 0 {
				return -1
			}
		}
	}
	if nodes[k].isGroup('(') {
		if b := prevSig(nodes, k); b >= 0 && nodes[b].word() == "GROUP" {
			if w := prevSig(nodes, b); w >= 0 && nodes[w].word() == "WITHIN" {
				if k = prevSig(nodes, w); k < 0 {
					return -1
				}
			}
		}
	}
	if k != p && !nodes[k].isGroup('(') && nodes[k].kind != kRaw && nodes[k].kind != kSeq {
		return -1 // what OVER, FILTER or WITHIN GROUP follows is not a call
	}
	return operandStart(nodes, k)
}

// caseStart returns the index of the CASE that the END at nodes[p] closes,
// or -1.
func caseStart(nodes []*node, p int) int {
	depth := 0
	for j := p; j >= 0; j-- {
		switch nodes[j].word() {
		case "END":
			depth++
		case "CASE":
			if depth--; depth == 0 {
				return j
			}
		}
	}
	return -1
}

// caseEnd returns the index of the END that closes the CASE at nodes[q],
// or -1.
func caseEnd(nodes []*node, q int) int {
	depth := 0
	for j := q; j < len(nodes); j++ {
		switch nodes[j].word() {
		case "CASE":
			depth++
		case "END":
			if depth--; depth == 0 {
				return j
			}
		}
	}
	return -1
}

// divisorEnd returns the index of the last node of a division's divisor,
// which starts at nodes[q]: unary signs, then a name, a literal, a call
// with what may follow it, a parenthesized expression, CASE ... END or a
// translated expression, then element accesses. -1 when it is none of
// these, or something follows it that would bind to it first.
func divisorEnd(nodes []*node, q int) int {
	for nodes[q].isOp("-") || nodes[q].isOp("+") {
		if q = nextSig(nodes, q); q < 0 {
			return -1
		}
	}
	n, end := nodes[q], q
	switch {
	case n.word() == "CASE":
		if end = caseEnd(nodes, q); end < 0 {
			return -1
		}
	case n.kind == kWord && !nonOperandWords[n.word()], n.kind == kQuoted:
		for {
			d := nextSig(nodes, end)
			if d < 0 || !nodes[d].isOp(".") {
				break
			}
			w := nextSig(nodes, d)
			if w < 0 || nodes[w].kind != kWord && nodes[w].kind != kQuoted {
				return -1
			}
			end = w
		}
		if g := nextSig(nodes, end); g >= 0 && nodes[g].isGroup('(') && (nodes[end].kind == kQuoted || !notCallWords[nodes[end].word()]) {
			end = callEnd(nodes, g)
		}
	case n.kind == kRaw, n.kind == kSeq:
		end = callEnd(nodes, q)
	case n.isGroup('('), n.kind == kNumber, n.kind == kString, n.kind == kVar, n.kind == kParam, n.kind == kVariant:
	default:
		return -1
	}
	for {
		g := nextSig(nodes, end)
		if g < 0 || !nodes[g].isGroup('[') {
			break
		}
		end = g
	}
	if a := nextSig(nodes, end); a >= 0 {
		switch b := nodes[a]; {
		case b.isOp("."), b.isOp(":"), b.isOp("::"), b.isGroup('('), b.word() == "OVER", b.word() == "FILTER",
			b.word() == "WITHIN", b.word() == "IGNORE", b.word() == "RESPECT", b.word() == "COLLATE":
			return -1
		}
	}
	return end
}

// callEnd returns the index of the last node of a call whose arguments
// (or translation) are nodes[g]: its WITHIN GROUP, FILTER, IGNORE or
// RESPECT NULLS and OVER, those it has.
func callEnd(nodes []*node, g int) int {
	end := g
	if w := nextSig(nodes, end); w >= 0 && nodes[w].word() == "WITHIN" {
		if r := nextSig(nodes, w); r >= 0 && nodes[r].word() == "GROUP" {
			if o := nextSig(nodes, r); o >= 0 && nodes[o].isGroup('(') {
				end = o
			}
		}
	}
	if f := nextSig(nodes, end); f >= 0 && nodes[f].word() == "FILTER" {
		if o := nextSig(nodes, f); o >= 0 && nodes[o].isGroup('(') {
			end = o
		}
	}
	if r := nextSig(nodes, end); r >= 0 && (nodes[r].word() == "IGNORE" || nodes[r].word() == "RESPECT") {
		if s := nextSig(nodes, r); s >= 0 && nodes[s].word() == "NULLS" {
			end = s
		}
	}
	if o := nextSig(nodes, end); o >= 0 && nodes[o].word() == "OVER" {
		if w := nextSig(nodes, o); w >= 0 && (nodes[w].isGroup('(') || nodes[w].kind == kWord || nodes[w].kind == kQuoted) {
			end = w
		}
	}
	return end
}
