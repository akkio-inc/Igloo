package snowflake

import (
	"slices"
	"strconv"
	"strings"
)

// structFlatten is a FLATTEN of objects built from each row, as reports
// build them to flatten one of their members in turn:
//
//	FLATTEN(INPUT => [OBJECT_CONSTRUCT('kind', 'all', 'items', ['every']),
//		OBJECT_CONSTRUCT_KEEP_NULL('kind', 'genre', 'items', m.genres)]) AS t,
//	FLATTEN(INPUT => GET_PATH(t.value, 'items')) AS v
//
// whose VALUE the statement reads only by a key (see collectStructs). Its
// elements are DuckDB structs with fields for each key, rather than JSON
// objects. The JSON translation writes each object's lists as JSON text and
// parses it back to flatten them, and DuckDB joins a FLATTEN that reads an
// earlier FLATTEN's JSON VALUE to its rows with a dependent join, where it
// unnests a list in a struct where it reads it: over 2 million rows of five
// lists of strings, 29 million elements, 1.6 s becomes 0.4 s. A key whose
// values are all string literals has one field, a VARCHAR; any other has a
// list part, a VARCHAR[] that holds a value that is a list of strings, a
// string part, a VARCHAR that holds a string, and a JSON part that holds any
// other value (see sf_list_part), so that a string reads as itself. A key an
// object lacks is NULL in all. Each struct has its index in the array too,
// _sf_i, which INDEX reads, so that a WHERE may filter the list (see
// structWhere). The FLATTEN's struct is a seventh column, _sf_fields,
// beside its VALUE as JSON, which DuckDB computes only if the statement
// reads it.
type structFlatten struct {
	// keys are the objects' keys, in the order first met, and general says
	// which of them have a value other than a string literal.
	keys    []string
	general []bool
	// text says which general keys every read casts to a type that is not
	// semi-structured, in a FLATTEN hoisted into its FROM item's select
	// list: their field is their text alone, _sf_tN (see keysText).
	textKeys []bool
	// base, when set, is the struct that reads of its VALUE read in place
	// of alias._sf_fields: the column of the select list it is hoisted into
	// (see hoistOf).
	base string
}

// objectParts are the keys and values of an OBJECT_CONSTRUCT whose keys are
// string literals, the values as jsonArg made them.
type objectParts struct {
	keys     []string
	values   []*node
	keepNull bool
}

// key returns the index of a key, or -1.
func (s *structFlatten) key(k string) int {
	for i, have := range s.keys {
		if have == k {
			return i
		}
	}
	return -1
}

// fieldNames are the names of key i's fields: one for a key of string
// literals or one read only as text, a list, a string and a JSON part for
// any other.
func (s *structFlatten) fieldNames(i int) (str, list, string_, json string) {
	n := strconv.Itoa(i + 1)
	switch {
	case !s.general[i]:
		return "_sf_k" + n, "", "", ""
	case s.textOnly(i):
		return "_sf_t" + n, "", "", ""
	}
	return "", "_sf_l" + n, "_sf_s" + n, "_sf_j" + n
}

// textOnly reports whether key i's field is its text (see text).
func (s *structFlatten) textOnly(i int) bool { return i >= 0 && i < len(s.textKeys) && s.textKeys[i] }

// parts are the list, string and JSON parts of key i in the struct x, as
// SQL: for a key of string literals, its field as the string; for a key the
// objects lack, NULL.
func (s *structFlatten) parts(x string, i int) (list, str, json string) {
	if i < 0 {
		return "CAST(NULL AS VARCHAR[])", "CAST(NULL AS VARCHAR)", "CAST(NULL AS JSON)"
	}
	k, l, st, j := s.fieldNames(i)
	if k != "" {
		return "CAST(NULL AS VARCHAR[])", x + "." + k, "CAST(NULL AS JSON)"
	}
	return x + "." + l, x + "." + st, x + "." + j
}

// json is key i of the struct x as JSON, and text as text. A key read only
// as text has no JSON a read takes.
func (s *structFlatten) json(x string, i int) string {
	l, st, j := s.parts(x, i)
	switch {
	case i < 0:
		return j
	case !s.general[i] || s.textOnly(i):
		return "to_json(" + st + ")"
	}
	return "sf_fields_json(" + l + ", " + st + ", " + j + ")"
}

func (s *structFlatten) text(x string, i int) string {
	l, st, j := s.parts(x, i)
	if i < 0 || !s.general[i] || s.textOnly(i) {
		return st
	}
	return "sf_fields_string(" + l + ", " + st + ", " + j + ")"
}

// collectStructs finds, before anything is translated, the FLATTENs that
// flattenStructs translates: those of an array constant, [...] or
// ARRAY_CONSTRUCT(...), of OBJECT_CONSTRUCT or OBJECT_CONSTRUCT_KEEP_NULL
// calls with string literals for keys, without PATH, OUTER or RECURSIVE or
// with a MODE other than 'OBJECT', and with an alias that their select, and
// what it nests, names nothing else by and uses only to qualify columns.
// Their VALUE, not renamed, must be read only by a key there:
// alias.VALUE:key or alias.VALUE['key'], GET_PATH(alias.VALUE, 'key...')
// or GET(alias.VALUE, 'key'), whatever path or cast follows, a later
// FLATTEN's INPUT included. A FLATTEN whose select selects a star, names
// VALUE unqualified or filters its rows other than by a WHERE that
// structWhere keeps out of the joins (see filters) stays JSON. Selects
// apart, such as the CTEs of reports that each flatten objects AS t, are
// apart: t.structs and t.structReads hold the FLATTENs found, by their
// argument groups, and the reads of their VALUE, by VALUE's nodes, and
// t.onlyValues every FLATTEN whose select reads only its VALUE and SEQ.
func (t *translator) collectStructs(nodes []*node) {
	type flat struct {
		alias, args, cols *node
		up                string
		level             []*node
		i                 int
	}
	var flats []flat
	colLists := map[*node]bool{}
	var find func(nodes []*node)
	find = func(nodes []*node) {
		for i, n := range nodes {
			if n.kind == kGroup {
				find(n.kids)
			}
			args, after, ok := flattenAt(nodes, i)
			if !ok {
				continue
			}
			a := aliasIndex(nodes, after)
			if a < 0 || args == nil {
				continue
			}
			var cols *node
			if k := nextSig(nodes, a); k >= 0 && nodes[k].isGroup('(') {
				cols = nodes[k]
				colLists[cols] = true
			}
			name, _, _ := nodes[a].name()
			flats = append(flats, flat{alias: nodes[a], args: args, cols: cols, up: strings.ToUpper(name), level: nodes, i: i})
		}
	}
	find(nodes)
	for _, f := range flats {
		if !otherColumnsRead(f.level, f.up, f.cols, colLists) {
			t.onlyValues[f.args] = true
		}
	}
	hoisted := map[*node]bool{} // the FROM items' alias nodes
	for _, f := range flats {
		s := structInput(f.args)
		if s == nil || f.up == "VALUE" || selectsStar(f.level) || !valueKept(f.cols) || !readInPlace(f.level, f.i, f.up) {
			continue
		}
		reads, ok := readsOnlyByKey(f.level, f.alias, f.up, colLists)
		if !ok {
			continue
		}
		h := t.hoistOf(f.level, f.i, f.up, f.args, reads, s, colLists)
		if h != nil && hoisted[h.item] {
			h = nil // one unnest a select list can zip, not two
		}
		if h == nil && t.filters(f.level, f.i, f.up) {
			continue
		}
		t.structs[f.args] = s
		for r := range reads {
			t.structReads[r] = s
		}
		if h == nil {
			continue
		}
		// Its reads in the WHERE, whose text DuckDB is to filter above the
		// select list (see sf_text_read).
		if _, _, where, end, _ := whereClause(f.level); where >= 0 {
			for _, n := range f.level[where+1 : end] {
				walk(n, func(n *node) { t.guarded[n] = true })
			}
		}
		hoisted[h.item] = true
		s.base = h.alias + "._sf_t"
		text := keysText(f.level, reads, colLists)
		s.textKeys = make([]bool, len(s.keys))
		for i, k := range s.keys {
			s.textKeys[i] = s.general[i] && text[k] && i != h.key
		}
		t.hoists[f.args] = h
		if h.reader != nil {
			t.hoists[h.reader] = h
			for _, r := range h.elems {
				t.elemReads[r] = h.alias + "._sf_e"
				t.elemTexts[r] = h.text
			}
		}
	}
	for _, f := range flats {
		if structInput(f.args) != nil {
			continue
		}
		h := t.listHoistOf(f.level, f.i, f.up, f.alias, f.args, f.cols, colLists)
		if h != nil && h.column {
			// A column: its FROM item's select list may compute the list,
			// which the lateral subquery unnests (see listColumns).
			t.flattenLists++
			if t.opts.FlattenLists {
				t.listCols[f.args] = h
			}
			h = nil
		}
		if h == nil || hoisted[h.item] {
			if t.textValuesOf(f.level, f.i, f.up, f.alias, f.args, f.cols, colLists) {
				t.textValues[f.args] = true
				name, _, _ := f.alias.name()
				for _, r := range valueReads(f.level, f.up, colLists) {
					t.elemReads[r] = quoteIdent(name) + ".\"VALUE\""
					t.elemTexts[r] = true
				}
			}
			if lc := t.listCols[f.args]; lc != nil {
				lc.text = t.textValues[f.args]
			}
			continue
		}
		hoisted[h.item] = true
		t.hoists[f.args] = h
		for _, r := range valueReads(f.level, f.up, colLists) {
			t.elemReads[r] = h.alias + "._sf_v"
			t.elemTexts[r] = h.text
		}
	}
}

// hoist is a FLATTEN of structs that its FROM item's select list unnests
// (see hoistStructs): the item's alias as written and upper-cased, and the
// alias's node; the FLATTEN's keys and its list of structs; and the index
// of the key that a second FLATTEN unnests, a reader, with its arguments,
// MODE and the nodes of its VALUE's reads, and whether those all read text,
// which the select list then unnests rather than JSON (see readsText).
type hoist struct {
	alias  string
	up     string
	item   *node
	s      *structFlatten
	key    int
	reader *node
	mode   string
	elems  []*node
	text   bool
	// list is the objects' struct for each row and index (see hoistList),
	// and objects their number. A hoisted FLATTEN of a list (see
	// listHoistOf) has no s, and list is the list that the select list
	// unnests, as text when text is set.
	list    *node
	objects int
	// column says the FLATTEN unnests a column, whose list the select list
	// computes as name and the FLATTEN's lateral subquery unnests (see
	// listColumns), rather than the select list unnesting it.
	column bool
	name   string
}

// listHoistOf decides whether the FLATTEN at nodes[i], aliased by the node
// alias (upper-cased up), with arguments args and column list cols (or
// nil), flattens a list that its FROM item's select list unnests instead
// (see hoistStructs): a list computed from the columns of one FROM item
// before it, such as SPLIT(t.tags, ','). flattenUnnest computes such an
// input in a subquery of its own, which DuckDB joins to the item's rows
// with a dependent join that holds them all in a hash table: 98 s against
// 41 s for a report that flattened a list column of tens of millions
// of rows joined to an audience. A column flattenUnnest unnests where it
// reads it, unless its select's WHERE stays a WHERE (see laterallyFiltered),
// and then DuckDB takes a dependent join for it too. INPUT must be its only
// argument, and not an array constant; it must be joined plainly at a level
// with no outer join, its column list must keep FLATTEN's names, no later
// FLATTEN may read it, and its select must read only its VALUE, qualified
// and outside subqueries, and name neither VALUE nor SEQ unqualified. A
// SPLIT is unnested as text, as are values whose every read is cast to text
// (see readsText), which reads of VALUE read in place of the elements' JSON
// (see variant.render).
func (t *translator) listHoistOf(nodes []*node, i int, up string, alias, args, cols *node, colLists map[*node]bool) *hoist {
	if _, _, _, _, ok := whereClause(nodes); !ok || up == "VALUE" || up == "SEQ" || !t.onlyValues[args] || !onlyValue(nodes, up) ||
		!plainlyJoined(nodes, i) || selectsStar(nodes) || !defaultColumns(cols) || len(readers(nodes, i, up)) > 0 ||
		!readsPlainly(nodes, alias, up, colLists) {
		return nil
	}
	input := onlyInput(args)
	idx := sig(input)
	if len(idx) == 0 || input[idx[0]].isGroup('[') || input[idx[0]].word() == "ARRAY_CONSTRUCT" {
		return nil
	}
	column := isColumnRef(input) && !laterallyFiltered(nodes, i)
	q := qualifiers(input)
	if len(q) != 1 || q[up] || unqualified(input) {
		return nil
	}
	var item string
	for name := range q {
		item = name
	}
	_, end, as, ok := fromItem(nodes, item)
	if !ok || end > i {
		return nil
	}
	text := len(idx) == 2 && input[idx[0]].word() == "SPLIT" && input[idx[1]].isGroup('(') || readsText(nodes, up, colLists)
	return &hoist{alias: as, up: item, item: nodes[end-1], key: -1, text: text, column: column}
}

// laterallyFiltered reports whether the select at nodes, with a FLATTEN at
// nodes[i], has a WHERE that whereOn leaves where it is: one after a FROM
// item after the FLATTEN, after an ON that follows a comma before it, or
// that cannot be an ON (see movable).
func laterallyFiltered(nodes []*node, i int) bool {
	sel, from, where, end, ok := whereClause(nodes)
	if !ok || where < 0 {
		return false
	}
	_, after, _ := flattenAt(nodes, i)
	last := aliasIndex(nodes, after)
	if k := nextSig(nodes, last); last >= 0 && k >= 0 && nodes[k].isGroup('(') {
		last = k
	}
	if last < 0 || prevSig(nodes, where) != last {
		return true
	}
	comma := false
	for k := from + 1; k < i; k++ {
		switch {
		case nodes[k].isOp(","):
			comma = true
		case nodes[k].word() == "ON" && comma:
			return true
		}
	}
	return !movable(nodes[where+1:end], selectAliases(nodes, sel, from))
}

// onlyInput returns a FLATTEN's INPUT, as written, when that is its only
// argument, or nil.
func onlyInput(args *node) []*node {
	var input []*node
	for _, a := range splitArgs(args.kids) {
		a = trim(a)
		idx := sig(a)
		named := len(idx) >= 2 && a[idx[1]].isOp("=>")
		switch {
		case input != nil || len(idx) == 0:
			return nil
		case named && a[idx[0]].word() == "INPUT":
			input = trim(a[idx[1]+1:])
		case named:
			return nil
		default:
			input = a
		}
	}
	return input
}

// textValuesOf reports whether the FLATTEN at nodes[i], aliased by the node
// alias (upper-cased up), with arguments args and column list cols (or
// nil), unnests its values as text, as listHoistOf's lists of text: a
// FLATTEN of a column, its only argument, whose select reads only its VALUE,
// qualified and outside subqueries, and only cast to text or tested for NULL
// (see readsText), which no later FLATTEN reads. The JSON of each element,
// which every read made text again, is not made: a report's categories of
// pages visited, ...
func (t *translator) textValuesOf(nodes []*node, i int, up string, alias, args, cols *node, colLists map[*node]bool) bool {
	input := onlyInput(args)
	return isColumnRef(input) && up != "VALUE" && up != "SEQ" && t.onlyValues[args] && onlyValue(nodes, up) && valueKept(cols) &&
		len(readers(nodes, i, up)) == 0 && readsPlainly(nodes, alias, up, colLists) && readsText(nodes, up, colLists)
}

// defaultColumns reports whether a FLATTEN's column list, cols (or nil),
// names its columns as FLATTEN does.
func defaultColumns(cols *node) bool {
	if cols == nil {
		return true
	}
	names := splitArgs(cols.kids)
	if len(names) != 6 {
		return false
	}
	for k, want := range []string{"SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"} {
		n := trim(names[k])
		if len(n) != 1 {
			return false
		}
		if name, _, ok := n[0].name(); !ok || strings.ToUpper(name) != want {
			return false
		}
	}
	return true
}

// readsPlainly reports whether a select, nodes, and what it nests name the
// FLATTEN aliased by the node alias (upper-cased up) only to qualify
// columns, outside subqueries, and name no VALUE or SEQ unqualified,
// outside FLATTENs' column lists (colLists).
func readsPlainly(nodes []*node, alias *node, up string, colLists map[*node]bool) bool {
	ok := true
	var check func(nodes []*node, nested bool)
	check = func(nodes []*node, nested bool) {
		for i, n := range nodes {
			if !ok {
				return
			}
			if n.kind == kGroup && !colLists[n] {
				k := sig(n.kids)
				check(n.kids, nested || len(k) > 0 && (n.kids[k[0]].word() == "SELECT" || n.kids[k[0]].word() == "WITH"))
			}
			name, _, isName := n.name()
			p := prevSig(nodes, i)
			if !isName || n == alias || p >= 0 && nodes[p].isOp(":") {
				continue
			}
			switch strings.ToUpper(name) {
			case "VALUE", "SEQ":
				if p < 0 || !nodes[p].isOp(".") && nodes[p].word() != "AS" {
					ok = false
				}
			case up:
				q := nextSig(nodes, i)
				if nested || p >= 0 && nodes[p].isOp(".") || q < 0 || !nodes[q].isOp(".") {
					ok = false
				}
			}
		}
	}
	check(nodes, false)
	return ok
}

// hoistOf decides whether the FLATTEN of structs at nodes[i], aliased up
// (upper case), with arguments args and its VALUE's reads (and their keys)
// reads, is hoisted into its FROM item's select list. DuckDB plans a FLATTEN
// as a dependent join when a filter is pushed onto its output, as reports'
// outer queries filter by the keys they group by (2.3 s against 1.8 s as
// JSON for 3 million rows of three lists), and when it reads a string or
// number column; a select list that builds the objects is neither (0.48 s
// for an unnest of them there), and Igloo distributes it. The FLATTEN must
// read the columns of one FROM item before
// it at that level, qualified, and be joined plainly; its select must name
// it only by its VALUE's keys; and at most one later FLATTEN, which must
// flatten one key of it and be read only by its VALUE, may read it, which
// a second select list around the first unnests: as fast as DuckDB's
// unnests of laterals where nothing is pushed onto them (0.24 s), where an
// unnest of (object, element) pairs built in one select list took 0.28 s.
func (t *translator) hoistOf(nodes []*node, i int, up string, args *node, reads map[*node]string, s *structFlatten,
	colLists map[*node]bool) *hoist {
	if _, _, _, _, ok := whereClause(nodes); !ok || !t.onlyValues[args] || !onlyValue(nodes, up) || !plainlyJoined(nodes, i) {
		return nil
	}
	q := qualifiers(args.kids)
	if len(q) != 1 || q[up] || unqualified(args.kids) {
		return nil
	}
	var item string
	for name := range q {
		item = name
	}
	_, end, alias, ok := fromItem(nodes, item)
	if !ok || end > i {
		return nil
	}
	h := &hoist{alias: alias, up: item, item: nodes[end-1], s: s, key: -1}
	rs := readers(nodes, i, up)
	switch len(rs) {
	case 0:
		return h
	case 1:
	default:
		return nil
	}
	vArgs, after, _ := flattenAt(nodes, rs[0])
	a := aliasIndex(nodes, after)
	if a < 0 || !t.onlyValues[vArgs] || !plainlyJoined(nodes, rs[0]) {
		return nil
	}
	name, _, _ := nodes[a].name()
	v := strings.ToUpper(name)
	if !onlyValue(nodes, v) {
		return nil
	}
	// The key it flattens.
	key := ""
	for r, k := range reads {
		walk(vArgs, func(n *node) {
			if n == r {
				key = k
			}
		})
	}
	h.key = s.key(key)
	if h.key < 0 {
		return nil
	}
	h.reader, h.mode = vArgs, "BOTH"
	for _, a := range splitArgs(vArgs.kids) {
		if idx := sig(a); len(idx) >= 3 && a[idx[0]].word() == "MODE" && a[idx[1]].isOp("=>") {
			m, _ := a[idx[2]].stringValue()
			h.mode = strings.ToUpper(m)
		}
	}
	h.elems = valueReads(nodes, v, colLists)
	h.text = readsText(nodes, v, colLists)
	return h
}

// plainlyJoined reports whether the FROM item at nodes[i] is joined by a
// comma or a JOIN without ON or USING.
func plainlyJoined(nodes []*node, i int) bool {
	p := prevSig(nodes, i)
	if p >= 0 && nodes[p].word() == "LATERAL" {
		p = prevSig(nodes, p)
	}
	if p < 0 || !nodes[p].isOp(",") && nodes[p].word() != "JOIN" {
		return false
	}
	if p := prevSig(nodes, p); p >= 0 && (nodes[p].word() == "LEFT" || nodes[p].word() == "RIGHT" || nodes[p].word() == "FULL" ||
		nodes[p].word() == "OUTER" || nodes[p].word() == "NATURAL") {
		return false
	}
	_, after, _ := flattenAt(nodes, i)
	a := aliasIndex(nodes, after)
	if a < 0 {
		return false
	}
	end := a
	if k := nextSig(nodes, a); k >= 0 && nodes[k].isGroup('(') {
		end = k
	}
	q := nextSig(nodes, end)
	return q < 0 || nodes[q].word() != "ON" && nodes[q].word() != "USING"
}

// unqualified reports whether nodes, an expression as written, may name a
// column unqualified: a word that is not a keyword, a call, a qualifier, a
// type after AS or ::, or a named argument.
func unqualified(nodes []*node) bool {
	found := false
	var walk func(nodes []*node)
	walk = func(nodes []*node) {
		for i, n := range nodes {
			if n.kind == kGroup {
				walk(n.kids)
			}
			if n.kind != kWord && n.kind != kQuoted {
				continue
			}
			p, q := prevSig(nodes, i), nextSig(nodes, i)
			switch w := n.word(); {
			case q >= 0 && (nodes[q].isOp(".") || nodes[q].isGroup('(') || nodes[q].isOp("=>")):
			case p >= 0 && (nodes[p].isOp(".") || nodes[p].isOp("::") || nodes[p].isOp(":") || nodes[p].word() == "AS"):
			case nonOperandWords[w], w == "NULL", w == "TRUE", w == "FALSE", w == "END":
			default:
				found = true
			}
		}
	}
	walk(nodes)
	return found
}

// valueReads returns the nodes of the reads of alias.VALUE (alias upper
// case) in nodes and what they nest, outside FLATTENs' column lists.
func valueReads(nodes []*node, alias string, colLists map[*node]bool) []*node {
	var out []*node
	var walk func(nodes []*node)
	walk = func(nodes []*node) {
		for i, n := range nodes {
			if n.kind == kGroup && !colLists[n] {
				walk(n.kids)
			}
			if n.word() != "VALUE" {
				continue
			}
			p := prevSig(nodes, i)
			if p < 0 || !nodes[p].isOp(".") {
				continue
			}
			if q := prevSig(nodes, p); q >= 0 {
				if name, _, ok := nodes[q].name(); ok && strings.ToUpper(name) == alias {
					out = append(out, n)
				}
			}
		}
	}
	walk(nodes)
	return out
}

// readsText reports whether every read of alias.VALUE (alias upper case)
// in nodes and what they nest, outside FLATTENs' column lists, is cast to
// a type that is not semi-structured, as CAST(alias.VALUE AS type),
// TRY_CAST(alias.VALUE AS type) or alias.VALUE::type, which read the
// value's text (see castExpr), or tested for NULL, which a FLATTEN's VALUE
// never is (see variant.render). Reports read a FLATTEN's elements so, and
// unnesting their text spares making JSON of each string and reading it
// back (0.65 s against 1.05 s for 3 million rows of four lists).
func readsText(nodes []*node, alias string, colLists map[*node]bool) bool {
	text := true
	var walk func(nodes []*node, cast bool)
	walk = func(nodes []*node, cast bool) {
		for i, n := range nodes {
			if n.kind == kGroup && !colLists[n] {
				p := prevSig(nodes, i)
				walk(n.kids, n.isGroup('(') && p >= 0 && (nodes[p].word() == "CAST" || nodes[p].word() == "TRY_CAST"))
			}
			if n.word() != "VALUE" {
				continue
			}
			p := prevSig(nodes, i)
			if p < 0 || !nodes[p].isOp(".") {
				continue
			}
			q := prevSig(nodes, p)
			if q < 0 {
				continue
			}
			if name, _, ok := nodes[q].name(); !ok || strings.ToUpper(name) != alias {
				continue
			}
			if !castsText(nodes, q, i, cast) && !nullTest(nodes, i) {
				text = false
			}
		}
	}
	walk(nodes, false)
	return text
}

// nullTest reports whether the read that ends at nodes[i] is tested for
// NULL: IS NULL or IS NOT NULL after it.
func nullTest(nodes []*node, i int) bool {
	k := nextSig(nodes, i)
	if k < 0 || nodes[k].word() != "IS" {
		return false
	}
	if k = nextSig(nodes, k); k >= 0 && nodes[k].word() == "NOT" {
		k = nextSig(nodes, k)
	}
	return k >= 0 && nodes[k].word() == "NULL"
}

// keysText reports, of the keys that reads (VALUE's nodes in a select,
// nodes, and what it nests, outside FLATTENs' column lists) read, those that
// every read casts to a type that is not semi-structured, by the key alone:
// alias.VALUE:key or alias.VALUE['key'], or GET_PATH(alias.VALUE, 'key') or
// GET(alias.VALUE, 'key'), with ::type after or as the whole of CAST's or
// TRY_CAST's value, as reports read their objects' names and metrics. Such a
// key's field holds its text alone (0.17 s against 0.19 s for 5 million rows'
// objects of a dimension and a string, read three times).
func keysText(nodes []*node, reads map[*node]string, colLists map[*node]bool) map[string]bool {
	text := map[string]bool{}
	for _, k := range reads {
		text[k] = true
	}
	// A group's place: the nodes it is in, its index there, and whether it
	// is CAST's or TRY_CAST's.
	type place struct {
		nodes []*node
		at    int
		cast  bool
		call  string
	}
	var walk func(nodes []*node, up []place)
	walk = func(nodes []*node, up []place) {
		for i, n := range nodes {
			if n.kind == kGroup && !colLists[n] {
				call := ""
				if p := prevSig(nodes, i); p >= 0 && n.isGroup('(') {
					call = nodes[p].word()
				}
				walk(n.kids, append(up, place{nodes: nodes, at: i, cast: call == "CAST" || call == "TRY_CAST", call: call}))
			}
			k, ok := reads[n]
			if !ok || !text[k] {
				continue
			}
			var in place
			if len(up) > 0 {
				in = up[len(up)-1]
			}
			a := prevSig(nodes, prevSig(nodes, i)) // the alias
			end := -1
			if i+1 < len(nodes) && nodes[i+1].isGroup('[') {
				end = i + 1
			} else if q := nextSig(nodes, i); q >= 0 && nodes[q].isOp(":") {
				end = nextSig(nodes, q)
			}
			switch {
			case end >= 0:
				text[k] = castsText(nodes, a, end, in.cast)
			case (in.call == "GET_PATH" || in.call == "GET") && singleKey(nodes, in.call):
				// The call, from its name to its arguments, in the nodes
				// around it.
				var out place
				if len(up) > 1 {
					out = up[len(up)-2]
				}
				text[k] = castsText(in.nodes, prevSig(in.nodes, in.at), in.at, out.cast)
			default:
				text[k] = false
			}
		}
	}
	walk(nodes, nil)
	return text
}

// singleKey reports whether the arguments of a GET_PATH or GET call, nodes,
// read one key, as readKey reads it: GET's key, or a path of one key.
func singleKey(nodes []*node, call string) bool {
	args := splitArgs(nodes)
	if len(args) != 2 {
		return false
	}
	lit := trim(args[1])
	if len(lit) != 1 {
		return false
	}
	s, ok := lit[0].stringValue()
	if !ok || call == "GET" {
		return ok
	}
	segs, err := parsePath(s)
	return err == nil && len(segs) == 1 && !segs[0].isIndex && segs[0].dyn == nil
}

// castsText reports whether the expression nodes[start:end+1] is cast to a
// type that is not semi-structured: followed by ::type, with no path after,
// or the whole value of the CAST or TRY_CAST whose group nodes is (cast),
// followed by AS type.
func castsText(nodes []*node, start, end int, cast bool) bool {
	k := nextSig(nodes, end)
	switch {
	case k >= 0 && nodes[k].isOp("::"):
		e, ok := scalarType(nodes, nextSig(nodes, k))
		r := nextSig(nodes, e-1)
		return ok && (r < 0 || !nodes[r].isOp(":") && !nodes[r].isOp(".") && !nodes[r].isGroup('['))
	case cast && prevSig(nodes, start) < 0 && k >= 0 && nodes[k].word() == "AS":
		e, ok := scalarType(nodes, nextSig(nodes, k))
		return ok && nextSig(nodes, e-1) < 0
	}
	return false
}

// scalarType reports whether a type that is not semi-structured starts at
// nodes[i], and the index just past it.
func scalarType(nodes []*node, i int) (int, bool) {
	if i < 0 {
		return 0, false
	}
	typ, end, ok, err := parseType(nodes, i)
	return end, err == nil && ok && typ.class != classJSON && typ.class != classNested
}

// otherColumnsRead reports whether a select, nodes, or what it nests may
// read the KEY, PATH, INDEX or THIS of the FLATTEN aliased alias (upper
// case) whose columns are named cols (or nil): with a star at its level, or
// by their names, qualified by the alias or not, but not as an alias, a
// path's key, a qualifier, a call or a named argument, nor in FLATTENs'
// column lists (colLists). A column of that name in another select, such as
// a report's INDEX, is another's.
func otherColumnsRead(nodes []*node, alias string, cols *node, colLists map[*node]bool) bool {
	if selectsStar(nodes) {
		return true
	}
	names := map[string]bool{}
	defaults := []string{"SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"}
	var list [][]*node
	if cols != nil {
		list = splitArgs(cols.kids)
	}
	for _, pos := range []int{1, 2, 3, 5} {
		name := defaults[pos]
		if pos < len(list) {
			n := trim(list[pos])
			if len(n) != 1 {
				return true
			}
			var ok bool
			if name, _, ok = n[0].name(); !ok {
				return true
			}
		}
		names[strings.ToUpper(name)] = true
	}
	read := false
	var walk func(nodes []*node)
	walk = func(nodes []*node) {
		for i, n := range nodes {
			if n.kind == kGroup && !colLists[n] {
				walk(n.kids)
			}
			name, _, ok := n.name()
			if !ok || !names[strings.ToUpper(name)] {
				continue
			}
			p, q := prevSig(nodes, i), nextSig(nodes, i)
			switch {
			case q >= 0 && (nodes[q].isOp(".") || nodes[q].isGroup('(') || nodes[q].isOp("=>")):
			case p >= 0 && (nodes[p].isOp(":") || nodes[p].word() == "AS"):
			case p >= 0 && nodes[p].isOp("."):
				if r := prevSig(nodes, p); r >= 0 {
					if qual, _, ok := nodes[r].name(); ok && strings.ToUpper(qual) == alias {
						read = true
					}
				}
			default:
				read = true
			}
		}
	}
	walk(nodes)
	return read
}

// readsOnlyByKey reports whether a select, nodes, and what it nests read
// the VALUE of the FLATTEN aliased by the node alias (upper-cased up) only
// by a key (see readsByKey), name the alias only to qualify columns, and
// name no VALUE unqualified, outside FLATTENs' column lists (colLists), nor
// the alias in a subquery; it returns the reads' VALUE nodes and their keys.
func readsOnlyByKey(nodes []*node, alias *node, up string, colLists map[*node]bool) (reads map[*node]string, ok bool) {
	reads = map[*node]string{}
	ok = true
	var check func(nodes []*node, call string, nested bool)
	check = func(nodes []*node, call string, nested bool) {
		for i, n := range nodes {
			if !ok {
				return
			}
			if n.kind == kGroup && !colLists[n] {
				fn := ""
				if p := prevSig(nodes, i); p >= 0 {
					fn = nodes[p].word()
				}
				k := sig(n.kids)
				sub := len(k) > 0 && (n.kids[k[0]].word() == "SELECT" || n.kids[k[0]].word() == "WITH")
				check(n.kids, fn, nested || sub)
			}
			name, _, isName := n.name()
			p := prevSig(nodes, i)
			if !isName || n == alias || p >= 0 && nodes[p].isOp(":") {
				continue // a path's key, v:value, is neither
			}
			switch strings.ToUpper(name) {
			case "VALUE":
				if p < 0 || !nodes[p].isOp(".") && nodes[p].word() != "AS" {
					ok = false
				}
			case up:
				// A qualifier, alias.column, not schema.alias.column, and not
				// in a subquery, which DuckDB would join with a dependent
				// join holding each row's structs.
				q := nextSig(nodes, i)
				r := nextSig(nodes, q)
				if nested || p >= 0 && nodes[p].isOp(".") || q < 0 || !nodes[q].isOp(".") || r < 0 {
					ok = false
					continue
				}
				col, _, isCol := nodes[r].name()
				switch {
				case !isCol:
					ok = false
				case strings.ToUpper(col) == "VALUE":
					key, byKey := readKey(nodes, i, r, call)
					if !byKey {
						ok = false
					}
					reads[nodes[r]] = key
				}
			}
		}
	}
	check(nodes, "", false)
	return reads, ok
}

// filters reports whether the level of a FLATTEN at nodes[i], with the
// upper-cased alias alias, filters the rows it joins so that DuckDB would
// join a later FLATTEN of its keys with a dependent join, whose hash table
// holds each row's structs of lists at a higher cost than JSON text (2.9 s
// against 1.8 s for 2 million rows of five lists with a WHERE on the
// elements, 2.3 s against 2 s with a HAVING on the keys): a WHERE that
// structWhere cannot keep out of the joins (see whereFits), an ON or USING
// after the FLATTEN, or a HAVING with a condition on the groups' keys,
// which DuckDB pushes below the grouping. Without a later FLATTEN of its
// keys, a dependent join holds the tables' columns, as it does for JSON,
// and the structs still read faster (1.5 s against 5.2 s for 5 million rows
// of five strings each with a WHERE on them).
func (t *translator) filters(nodes []*node, i int, alias string) bool {
	if len(readers(nodes, i, alias)) == 0 {
		return false
	}
	for k, n := range nodes {
		switch n.word() {
		case "WHERE":
			if !t.whereFits(nodes, i, alias) {
				return true
			}
		case "ON", "USING":
			if k > i {
				return true
			}
		case "HAVING":
			end := len(nodes)
			for j := k + 1; j < len(nodes); j++ {
				if havingEnds[nodes[j].word()] || nodes[j].isOp(";") {
					end = j
					break
				}
			}
			start := k + 1
			for j := start; j <= end; j++ {
				if j < end && nodes[j].word() != "AND" {
					continue
				}
				if !callsAggregate(nodes[start:j]) {
					return true
				}
				start = j + 1
			}
		}
	}
	return false
}

// readInPlace reports whether every later FLATTEN at the level of the one
// at nodes[i], aliased alias (upper case), that reads its VALUE flattens one
// key of it (see readsOneKey), which DuckDB unnests where it reads the
// struct; another, with OUTER or PATH or a longer path, takes a dependent
// join, which would hold each row's structs.
func readInPlace(nodes []*node, i int, alias string) bool {
	for _, k := range readers(nodes, i, alias) {
		if args, _, _ := flattenAt(nodes, k); !readsOneKey(args, alias) {
			return false
		}
	}
	return true
}

// readers are the indexes of the FLATTENs after nodes[i] at its level whose
// arguments name alias (upper case), and so may read its VALUE.
func readers(nodes []*node, i int, alias string) []int {
	var out []int
	for k := i + 1; k < len(nodes); k++ {
		if args, _, ok := flattenAt(nodes, k); ok && args != nil && qualifiers(args.kids)[alias] {
			out = append(out, k)
		}
	}
	return out
}

// whereFits reports whether structWhere can keep the WHERE at the level of
// the FLATTEN at nodes[i], aliased alias (upper case), out of the joins: the
// FLATTEN is the first FROM item that is one, not the first item, and every
// item after it is one; the WHERE could move into an ON (see whereOn); and
// each of its conditions (see whereTerms) names, of the FLATTENs, only the
// FLATTEN's VALUE, which filters its objects, and the last FLATTEN's VALUE,
// which filters that one's elements, when that one flattens one key of the
// FLATTEN's VALUE (see readsOneKey) and its select reads none of its
// columns but VALUE and SEQ (see otherColumnsRead).
func (t *translator) whereFits(nodes []*node, i int, alias string) bool {
	sel, from, where, end, ok := whereClause(nodes)
	if !ok || where < 0 || i < from || i > where {
		return false
	}
	var flats []int
	for k := from + 1; k < where; k++ {
		if _, _, ok := flattenAt(nodes, k); ok {
			flats = append(flats, k)
		}
	}
	// An item before it, whose join takes the ON.
	p := prevSig(nodes, i)
	if p >= 0 && nodes[p].word() == "LATERAL" {
		p = prevSig(nodes, p)
	}
	if flats[0] != i || p < 0 || !nodes[p].isOp(",") && nodes[p].word() != "JOIN" {
		return false
	}
	later := map[string]bool{}
	for k := i + 1; k < where; k++ {
		if nodes[k].isOp(",") || nodes[k].word() == "JOIN" {
			q := nextSig(nodes, k)
			if q >= 0 && nodes[q].word() == "LATERAL" {
				q = nextSig(nodes, q)
			}
			if q < 0 || !slices.Contains(flats, q) {
				return false
			}
			_, after, _ := flattenAt(nodes, q)
			if a := aliasIndex(nodes, after); a >= 0 {
				name, _, _ := nodes[a].name()
				later[strings.ToUpper(name)] = true
			}
		}
	}
	cond := nodes[where+1 : end]
	if !movable(cond, selectAliases(nodes, sel, from)) {
		return false
	}
	// The last FLATTEN, whose elements a condition may filter.
	lastArgs, after, _ := flattenAt(nodes, flats[len(flats)-1])
	last := ""
	if a := aliasIndex(nodes, after); a >= 0 && len(flats) > 1 {
		name, _, _ := nodes[a].name()
		last = strings.ToUpper(name)
	}
	for _, term := range whereTerms(cond) {
		q := qualifiers(term)
		for name := range q {
			if later[name] && name != last {
				return false
			}
		}
		switch {
		case last != "" && q[last]:
			if lastArgs == nil || !readsOneKey(lastArgs, alias) || !t.onlyValues[lastArgs] ||
				!onlyValue(term, last) || q[alias] && !onlyValue(term, alias) {
				return false
			}
		case q[alias]:
			if !onlyValue(term, alias) {
				return false
			}
		}
	}
	return true
}

// onlyValue reports whether nodes name columns of alias (upper case) only as
// alias.VALUE.
func onlyValue(nodes []*node, alias string) bool {
	ok := true
	var walk func(nodes []*node)
	walk = func(nodes []*node) {
		for i, n := range nodes {
			if n.kind == kGroup {
				walk(n.kids)
			}
			name, _, isName := n.name()
			q := nextSig(nodes, i)
			if !isName || strings.ToUpper(name) != alias || q < 0 || !nodes[q].isOp(".") {
				continue
			}
			if p := prevSig(nodes, i); p >= 0 && (nodes[p].isOp(".") || nodes[p].isOp(":")) {
				continue
			}
			if r := nextSig(nodes, q); r < 0 || nodes[r].word() != "VALUE" {
				ok = false
			}
		}
	}
	walk(nodes)
	return ok
}

// whereTerms splits a WHERE's condition into the conditions ANDed at its
// top, unless an OR is there too, which binds less tightly. The AND of a
// BETWEEN, and those inside a CASE, are not.
func whereTerms(cond []*node) [][]*node {
	var terms [][]*node
	start, between, cases := 0, false, 0
	for k, n := range cond {
		switch n.word() {
		case "OR":
			if cases == 0 {
				return [][]*node{cond}
			}
		case "CASE":
			cases++
		case "END":
			cases--
		case "BETWEEN":
			between = true
		case "AND":
			switch {
			case cases > 0:
			case between:
				between = false
			default:
				terms = append(terms, cond[start:k])
				start = k + 1
			}
		}
	}
	return append(terms, cond[start:])
}

// qualifiers are the upper-cased names that nodes qualify columns with: the
// first of a dotted name.
func qualifiers(nodes []*node) map[string]bool {
	out := map[string]bool{}
	var walk func(nodes []*node)
	walk = func(nodes []*node) {
		for i, n := range nodes {
			if n.kind == kGroup {
				walk(n.kids)
			}
			name, _, ok := n.name()
			if !ok {
				continue
			}
			p, q := prevSig(nodes, i), nextSig(nodes, i)
			if (p < 0 || !nodes[p].isOp(".")) && q >= 0 && nodes[q].isOp(".") {
				out[strings.ToUpper(name)] = true
			}
		}
	}
	walk(nodes)
	return out
}

// readsOneKey reports whether a FLATTEN's arguments, g as written, flatten
// one key of alias.VALUE (alias upper case), as flattenUnnest unnests a
// struct's field: INPUT alias.VALUE:key, alias.VALUE['key'],
// GET_PATH(alias.VALUE, 'key') or GET(alias.VALUE, 'key'), and no PATH,
// OUTER or RECURSIVE.
func readsOneKey(g *node, alias string) bool {
	var input []*node
	for _, a := range splitArgs(g.kids) {
		a = trim(a)
		idx := sig(a)
		if len(idx) < 2 || !a[idx[1]].isOp("=>") {
			input = a
			continue
		}
		switch a[idx[0]].word() {
		case "INPUT":
			input = trim(a[idx[1]+1:])
		case "MODE":
		default:
			return false
		}
	}
	idx := sig(input)
	isValue := func(k int) bool {
		name, _, ok := input[idx[k]].name()
		return ok && strings.ToUpper(name) == alias && input[idx[k+1]].isOp(".") && input[idx[k+2]].word() == "VALUE"
	}
	switch {
	case len(idx) == 5 && isValue(0) && input[idx[3]].isOp(":"):
		return input[idx[4]].kind == kWord || input[idx[4]].kind == kQuoted
	case len(idx) == 4 && isValue(0) && input[idx[3]].isGroup('[') && idx[3] == idx[2]+1:
		k := trim(input[idx[3]].kids)
		if len(k) != 1 {
			return false
		}
		_, ok := k[0].stringValue()
		return ok
	case len(idx) == 2 && (input[idx[0]].word() == "GET_PATH" || input[idx[0]].word() == "GET") && input[idx[1]].isGroup('('):
		args := splitArgs(input[idx[1]].kids)
		if len(args) != 2 {
			return false
		}
		first, lit := trim(args[0]), trim(args[1])
		fi := sig(first)
		if len(fi) != 3 || len(lit) != 1 {
			return false
		}
		name, _, ok := first[fi[0]].name()
		if !ok || strings.ToUpper(name) != alias || !first[fi[1]].isOp(".") || first[fi[2]].word() != "VALUE" {
			return false
		}
		s, ok := lit[0].stringValue()
		if !ok {
			return false
		}
		if input[idx[0]].word() == "GET" {
			return true
		}
		segs, err := parsePath(s)
		return err == nil && len(segs) == 1 && !segs[0].isIndex
	}
	return false
}

// havingEnds are the words that end a HAVING clause.
var havingEnds = map[string]bool{
	"QUALIFY": true, "ORDER": true, "LIMIT": true, "WINDOW": true, "OFFSET": true, "FETCH": true,
	"UNION": true, "EXCEPT": true, "MINUS": true, "INTERSECT": true,
}

// aggregates are aggregate functions that a condition of HAVING may call,
// which keep it above the grouping.
var aggregates = map[string]bool{
	"COUNT": true, "COUNT_IF": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true, "MEDIAN": true,
	"APPROX_COUNT_DISTINCT": true, "HLL": true, "STDDEV": true, "STDDEV_SAMP": true, "STDDEV_POP": true,
	"VARIANCE": true, "VAR_SAMP": true, "VAR_POP": true, "ANY_VALUE": true, "LISTAGG": true, "ARRAY_AGG": true,
}

// callsAggregate reports whether nodes call one of aggregates.
func callsAggregate(nodes []*node) bool {
	for i, n := range nodes {
		if n.kind == kGroup && callsAggregate(n.kids) {
			return true
		}
		if g := nextSig(nodes, i); aggregates[n.word()] && g >= 0 && nodes[g].isGroup('(') {
			return true
		}
	}
	return false
}

// valueKept reports whether a FLATTEN's column list, cols (or nil), keeps
// VALUE's name: it names fewer than five columns or names the fifth VALUE,
// and names at most FLATTEN's six.
func valueKept(cols *node) bool {
	if cols == nil {
		return true
	}
	names := splitArgs(cols.kids)
	if len(names) > 6 {
		return false
	}
	if len(names) < 5 {
		return true
	}
	n := trim(names[4])
	return len(n) == 1 && n[0].word() == "VALUE"
}

// readsByKey reports whether alias.VALUE, nodes[a] to nodes[r], is read by
// a key (see readKey).
func readsByKey(nodes []*node, a, r int, call string) bool {
	_, ok := readKey(nodes, a, r, call)
	return ok
}

// readKey returns the key that alias.VALUE, nodes[a] to nodes[r], is read
// by: the key after it, :key or ['key'], or the first of the path of
// GET_PATH of it, or the key of GET of it, where call is the function whose
// arguments nodes are.
func readKey(nodes []*node, a, r int, call string) (string, bool) {
	if nodes[r].kind != kWord {
		return "", false
	}
	if r+1 < len(nodes) && nodes[r+1].isGroup('[') {
		k := trim(nodes[r+1].kids)
		if len(k) != 1 {
			return "", false
		}
		return k[0].stringValue()
	}
	if q := nextSig(nodes, r); q >= 0 && nodes[q].isOp(":") {
		k := nextSig(nodes, q)
		if k < 0 || nodes[k].kind != kWord && nodes[k].kind != kQuoted {
			return "", false
		}
		name, _, _ := nodes[k].name()
		return name, true
	}
	if call != "GET_PATH" && call != "GET" {
		return "", false
	}
	args := splitArgs(nodes)
	if len(args) != 2 || len(sig(args[0])) != 3 || trim(args[0])[0] != nodes[a] {
		return "", false
	}
	lit := trim(args[1])
	if len(lit) != 1 {
		return "", false
	}
	s, ok := lit[0].stringValue()
	if !ok || call == "GET" {
		return s, ok
	}
	segs, err := parsePath(s)
	if err != nil || len(segs) == 0 || segs[0].isIndex {
		return "", false
	}
	return segs[0].key, true
}

// structInput reads a FLATTEN's arguments, as written, for flattenStructs:
// the keys of its INPUT's objects, and which of them have a value other than
// a string literal; nil when the FLATTEN is not one of objects of literal
// keys or has other arguments.
func structInput(g *node) *structFlatten {
	if g == nil {
		return nil
	}
	var input []*node
	for _, a := range splitArgs(g.kids) {
		a = trim(a)
		idx := sig(a)
		if len(idx) < 2 || !a[idx[1]].isOp("=>") {
			if input != nil {
				return nil
			}
			input = a
			continue
		}
		v := trim(a[idx[1]+1:])
		switch a[idx[0]].word() {
		case "INPUT":
			if input != nil {
				return nil
			}
			input = v
		case "MODE":
			if len(v) != 1 {
				return nil
			}
			if m, ok := v[0].stringValue(); !ok || !strings.EqualFold(m, "ARRAY") && !strings.EqualFold(m, "BOTH") {
				return nil
			}
		default:
			return nil
		}
	}
	var elems [][]*node
	switch idx := sig(input); {
	case len(idx) == 1 && input[idx[0]].isGroup('['):
		elems = splitArgs(input[idx[0]].kids)
	case len(idx) == 2 && input[idx[0]].word() == "ARRAY_CONSTRUCT" && input[idx[1]].isGroup('('):
		elems = splitArgs(input[idx[1]].kids)
	default:
		return nil
	}
	s := &structFlatten{}
	for _, e := range elems {
		idx := sig(e)
		if len(idx) != 2 || !e[idx[1]].isGroup('(') {
			return nil
		}
		keep := false
		switch e[idx[0]].word() {
		case "OBJECT_CONSTRUCT":
		case "OBJECT_CONSTRUCT_KEEP_NULL":
			keep = true
		default:
			return nil
		}
		args := splitArgs(e[idx[1]].kids)
		if len(args)%2 != 0 {
			return nil
		}
		seen := map[string]bool{}
		for k := 0; k < len(args); k += 2 {
			key, value := trim(args[k]), trim(args[k+1])
			if len(key) != 1 || len(value) == 0 {
				return nil
			}
			name, ok := key[0].stringValue()
			if !ok || seen[name] {
				return nil
			}
			seen[name] = true
			i := s.key(name)
			if i < 0 {
				i = len(s.keys)
				s.keys, s.general = append(s.keys, name), append(s.general, false)
			}
			if len(value) == 1 && value[0].word() == "NULL" && !keep {
				continue // left out
			}
			if _, str := value[0].stringValue(); len(value) != 1 || !str {
				s.general[i] = true
			}
		}
	}
	if len(s.keys) == 0 {
		return nil
	}
	return s
}

// flattenStructs translates a FLATTEN that collectStructs found, of the
// array constant in, as flattenUnnest does one of JSON objects, with each
// element a struct (see structFlatten), its index in its field _sf_i, in the
// column _sf_fields as well. The list of the structs is a node of its own,
// which structWhere may filter.
func (t *translator) flattenStructs(s *structFlatten, in *node, alias string, cols *node, wantThis bool) (*node, error) {
	list := seq(nil, raw("["))
	for k, e := range in.elems {
		if e.obj == nil {
			return nil, &Error{Code: CodeSyntax, Message: "expected OBJECT_CONSTRUCT in FLATTEN's INPUT"}
		}
		if k > 0 {
			list.kids = append(list.kids, raw(", "))
		}
		list.kids = append(list.kids, structOf(s, e.obj, k))
	}
	list.kids = append(list.kids, raw("]"))
	x := "_sf_flatten._sf_value"
	entries := make([]string, len(s.keys))
	for i, k := range s.keys {
		entries[i] = "{'k': CAST(" + quoteString(k) + " AS VARCHAR), 'v': CAST(" + s.json(x, i) + " AS JSON)}"
	}
	value := "to_json(map_from_entries(list_sort(list_filter([" + strings.Join(entries, ", ") + "], lambda e: e.v IS NOT NULL))))"
	this := "CAST(NULL AS JSON)"
	lists := []*node{raw("unnest("), list, raw(") AS _sf_value")}
	if wantThis {
		this = "_sf_flatten._sf_this"
		lists = append(lists, raw(", unnest(repeat(["), in, raw("], len("), list, raw("))) AS _sf_this"))
	}
	kids := []*node{raw("LATERAL (SELECT CAST(NULL AS BIGINT) AS \"SEQ\", CAST(NULL AS VARCHAR) AS \"KEY\", " +
		"coalesce('[' || " + x + "._sf_i || ']', '') AS \"PATH\", " + x + "._sf_i AS \"INDEX\", " + value + " AS \"VALUE\", " +
		this + " AS \"THIS\", " + x + " AS _sf_fields FROM (SELECT ")}
	kids = append(kids, lists...)
	kids = append(kids, raw(") AS _sf_flatten)"+aliasClause(alias, cols)))
	out := seq(nil, kids...)
	t.structOuts[out] = &structOut{alias: alias, s: s, list: list}
	return out, nil
}

// structOut is what flattenStructs made of a FLATTEN: its alias, its keys,
// and the node of its list of structs.
type structOut struct {
	alias string
	s     *structFlatten
	list  *node
}

// structOf is the struct of an object's keys and values, the k-th of the
// array. Each field is cast to its type, which also keeps DuckDB 1.5 from
// losing the name of a field whose value is a macro's call inside a lateral
// subquery.
func structOf(s *structFlatten, o *objectParts, k int) *node {
	kids := []*node{raw("{'_sf_i': CAST(" + itoa(k) + " AS BIGINT)")}
	for i, key := range s.keys {
		var x *node
		for j, have := range o.keys {
			if have == key {
				x = o.values[j]
			}
		}
		str, l, st, j := s.fieldNames(i)
		if s.textOnly(i) {
			// Its text, as a read of its parts reads it.
			list, text, json := structParts(x, o.keepNull)
			kids = append(kids, raw(", '"+str+"': CAST(sf_fields_string("), list, raw(", "), text, raw(", "), json, raw(") AS VARCHAR)"))
			continue
		}
		if str != "" {
			if x == nil || x.word() == "NULL" {
				x = raw("NULL")
			}
			kids = append(kids, raw(", '"+str+"': CAST("), x, raw(" AS VARCHAR)"))
			continue
		}
		list, text, json := structParts(x, o.keepNull)
		kids = append(kids, raw(", '"+l+"': CAST("), list, raw(" AS VARCHAR[]), '"+st+"': CAST("), text,
			raw(" AS VARCHAR), '"+j+"': CAST("), json, raw(" AS JSON)"))
	}
	return seq(nil, append(kids, raw("}"))...)
}

// hoistList is the struct that the select list of a hoisted FLATTEN's FROM
// item builds for each of its rows and of the objects' indexes, _sf_k.i
// (see hoistStructs), from the FLATTEN's argument group g: a CASE of the
// index, or the one object's struct; it sets h.objects.
func (t *translator) hoistList(g *node, h *hoist) (*node, error) {
	pos, named := t.tableArgs(g)
	input := named["INPUT"]
	if input == nil && len(pos) > 0 {
		input = pos[0]
	}
	in := flattenInput(input)
	h.objects = len(in.elems)
	var kids []*node
	for k, e := range in.elems {
		if e.obj == nil {
			return nil, &Error{Code: CodeSyntax, Message: "expected OBJECT_CONSTRUCT in FLATTEN's INPUT"}
		}
		if len(in.elems) == 1 {
			return structOf(h.s, e.obj, k), nil
		}
		kids = append(kids, raw(" WHEN "+itoa(k)+" THEN "), structOf(h.s, e.obj, k))
	}
	return seq(nil, append(append([]*node{raw("CASE _sf_k.i")}, kids...), raw(" END"))...), nil
}

// listOf is the list that the select list of a hoisted FLATTEN's FROM item
// unnests (see listHoistOf), from the FLATTEN's argument group g: a SPLIT's
// list of strings, or FLATTEN's values of its input, as text for text.
func (t *translator) listOf(g *node, h *hoist) (*node, error) {
	pos, named := t.tableArgs(g)
	input := named["INPUT"]
	if input == nil && len(pos) > 0 {
		input = pos[0]
	}
	in := flattenInput(input)
	switch {
	case !h.text:
		return seq(nil, raw("sf_flatten_values("), in, raw(", 'BOTH')")), nil
	case in.strs != nil:
		return in.strs, nil
	}
	return seq(nil, raw("sf_flatten_strings("), in, raw(", 'BOTH')")), nil
}

// hoistStructs takes out of a level of a statement its hoisted FLATTENs,
// what tableFunctions left of them (removed), and the comma or JOIN before
// each, and makes each FROM item they read a subquery that joins it to the
// range of the objects' indexes and builds each row's object as _sf_t: FROM
// m, LATERAL FLATTEN(...) t becomes FROM (SELECT m.*, CASE _sf_k.i WHEN 0
// THEN {...} ... END AS _sf_t FROM m CROSS JOIN range(n) AS _sf_k(i)) AS m.
// DuckDB estimates that at n times m's rows, where it takes an unnest of a
// list of the n objects for m's rows alone, and a join of the objects with
// a smaller table would build its hash table on the objects (in a signals
// report, 90.7 million browsing elements against 1.0 million cohort
// members, 94 s of 465 s thread time). With a reader, another select list
// around that one unnests the key it flattens as _sf_e, as text when its
// reads all read text (see readsText). A FLATTEN of a list is unnested in
// the one select list, as _sf_v: (SELECT m.*, unnest(list) AS _sf_v FROM
// m) AS m.
func hoistStructs(nodes []*node, removed map[*node]bool, hoists []*hoist) ([]*node, error) {
	for k := 0; k < len(nodes); k++ {
		if !removed[nodes[k]] {
			continue
		}
		start := k
		if p := prevSig(nodes, k); p >= 0 && nodes[p].isOp(",") {
			start = p
		} else if p >= 0 && nodes[p].word() == "JOIN" {
			start = p
			if c := prevSig(nodes, p); c >= 0 && (nodes[c].word() == "CROSS" || nodes[c].word() == "INNER") {
				start = c
			}
		}
		for start > 0 && nodes[start-1].kind == kSpace {
			start--
		}
		nodes = replace(nodes, start, k+1)
		k = start - 1
	}
	for _, h := range hoists {
		start, end, alias, ok := fromItem(nodes, h.up)
		if !ok {
			return nil, &Error{Code: CodeSyntax, Message: "FLATTEN reads a FROM item that is not there: " + h.alias}
		}
		item := nodes[start:end]
		if h.s == nil {
			out := seq(item, append(append([]*node{raw("(SELECT " + alias + ".*, unnest("), h.list, raw(") AS _sf_v FROM ")}, item...),
				raw(") AS "+alias))...)
			nodes = replace(nodes, start, end, out)
			continue
		}
		join := ""
		if h.objects != 1 {
			join = " CROSS JOIN range(" + itoa(h.objects) + ") AS _sf_k(i)"
		}
		out := seq(item, append(append([]*node{raw("(SELECT " + alias + ".*, "), h.list, raw(" AS _sf_t FROM ")}, item...),
			raw(join+") AS "+alias))...)
		if h.reader != nil {
			l, _, j := h.s.parts(alias+"._sf_t", h.key)
			values := "sf_parts_values("
			if h.text {
				values = "sf_parts_strings("
			}
			out = seq(item, raw("(SELECT "+alias+".*, unnest("+values+l+", "+j+", "+quoteString(h.mode)+")) AS _sf_e FROM "), out,
				raw(") AS "+alias))
		}
		nodes = replace(nodes, start, end, out)
	}
	return nodes, nil
}

// structParts are the list, string and JSON parts of an object's value x
// (nil for none): a string literal as the string, a constant list of
// strings as the list, and anything else as the one of the three that its
// type makes it, when DuckDB binds the statement (see sf_list_part): a list
// of strings only for a column, as Igloo's ARRAY columns are, and JSON as
// OBJECT_CONSTRUCT takes it. NULL in all three for a value OBJECT_CONSTRUCT
// leaves out; JSON null for a NULL that OBJECT_CONSTRUCT_KEEP_NULL keeps.
func structParts(x *node, keep bool) (list, str, json *node) {
	null := raw("NULL")
	if keep {
		null = raw("CAST('null' AS JSON)")
	}
	suffix := "("
	if keep {
		suffix = "_keep_null("
	}
	switch {
	case x == nil:
		return raw("NULL"), raw("NULL"), raw("NULL")
	case x.word() == "NULL":
		return raw("NULL"), raw("NULL"), null
	case x.kind == kString && !x.tok.raw:
		return raw("NULL"), x, raw("NULL")
	case stringList(x):
		kids := []*node{raw("[")}
		for k, e := range x.elems {
			if k > 0 {
				kids = append(kids, raw(", "))
			}
			kids = append(kids, e)
		}
		return seq(nil, append(kids, raw("]"))...), raw("NULL"), raw("NULL")
	}
	if c := jsonArgColumn(x); c != nil {
		return seq(nil, raw("sf_list_part("), c, raw(")")), seq(nil, raw("sf_string_part("), c, raw(")")),
			seq(nil, raw("sf_other_part"+suffix), c, raw(")"))
	}
	return raw("NULL"), seq(nil, raw("sf_string_part("), x, raw(")")), seq(nil, raw("sf_expr_part"+suffix), x, raw(")"))
}

// stringList reports whether x is an array constant of string literals,
// none of which a list part would read as an object or an array's JSON
// text (see sf_elem).
func stringList(x *node) bool {
	if len(x.elems) == 0 {
		return false
	}
	for _, e := range x.elems {
		if s, ok := e.stringValue(); !ok || strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") {
			return false
		}
	}
	return true
}

// structRead renders a read of a structFlatten's VALUE by a key, for the
// variant's mode: the key's field, and the rest of the path, if any, into
// that field as JSON. It reports false for any other read, which reads
// VALUE as JSON. The struct is alias._sf_fields, or the element of a
// lambda's when structWhere filters the structs.
func (v *variant) structRead() (string, bool) {
	if v.obj == nil || len(v.segs) == 0 || v.segs[0].dyn != nil || v.segs[0].isIndex {
		return "", false
	}
	x := structColumn(v.base)
	if v.obj.base != "" {
		x = v.obj.base
	}
	if v.elem != "" {
		x = v.elem
	}
	i := v.obj.key(v.segs[0].key)
	rest := &variant{segs: v.segs[1:]}
	json := v.obj.json(x, i)
	str := v.obj.text(x, i)
	if v.guard && i >= 0 && (!v.obj.general[i] || v.obj.textOnly(i)) {
		// A field of text, a key of string literals' too: else DuckDB
		// derives from a WHERE's ORs on it a condition that it pushes below
		// the select list, as a join of the FROM item and the range of the
		// objects' indexes, which it takes for a fraction of their rows.
		_, st, _ := v.obj.parts(x, i)
		str = "sf_text_read(" + st + ")"
		json = "to_json(" + str + ")"
	}
	if len(rest.segs) > 0 {
		str = "json_extract_string(" + json + ", " + rest.path() + ")"
	}
	switch v.mode {
	case vString:
		return str, true
	case vNumber:
		if v.try {
			return "TRY_CAST(" + str + " AS DOUBLE)", true
		}
		return "CAST(" + str + " AS DOUBLE)", true
	}
	if len(rest.segs) == 0 {
		return json, true
	}
	return "json_extract(" + json + ", " + rest.path() + ")", true
}

// structColumn is the struct a read of a structFlatten's VALUE, base
// (alias.VALUE), reads: alias._sf_fields.
func structColumn(base *node) string {
	for k := len(base.kids) - 1; k >= 0; k-- {
		if base.kids[k].isOp(".") {
			return render(trim(base.kids[:k])) + "._sf_fields"
		}
	}
	return "_sf_fields"
}

// structFold is what a FLATTEN of a key of a structFlatten's objects unnests
// when the statement reads only its VALUE (see flattenUnnest): its alias,
// and the node of the list of its elements, which structWhere may filter.
type structFold struct {
	alias  string
	values *node
	// column says the list is a column's elements as text, which
	// valueWhere filters rather than structWhere.
	column bool
}

// structWhere keeps the WHERE of a select whose first FLATTEN flattenStructs
// made out of its joins, which DuckDB would otherwise plan as dependent
// joins (see whereFits): conditions that name none of the FLATTENs go into
// the ON of the first FLATTEN's join, which DuckDB pushes down to the
// tables; conditions that name the first FLATTEN's VALUE filter its list of
// structs, and conditions that name the last FLATTEN's VALUE, when that one
// unnests only its elements (see structFold), filter those, lambdas whose
// element the conditions' reads of VALUE read. Over 2 million rows of five
// lists, a WHERE on the table took 1.5 s as JSON and takes 0.4 s, one on
// the keys 1.9 s and 0.6 s; over 3 million rows of three lists, one on the
// elements 6.1 s and 1.5 s. The WHERE stays where it is when a condition
// names another FLATTEN or another column of these.
func (t *translator) structWhere(nodes []*node) ([]*node, error) {
	_, from, where, end, ok := whereClause(nodes)
	if !ok || where < 0 {
		return nodes, nil
	}
	first := -1
	var fold *structFold
	var others []string
	for k := from + 1; k < where; k++ {
		alias, isFlat := t.flattenOuts[nodes[k]]
		if !isFlat {
			continue
		}
		switch {
		case first < 0 && t.structOuts[nodes[k]] != nil:
			first = k
		case t.folds[nodes[k]] != nil && t.folds[nodes[k]].values != nil && k == prevSig(nodes, where):
			fold = t.folds[nodes[k]]
		default:
			others = append(others, alias)
		}
	}
	if first < 0 {
		return nodes, nil
	}
	so := t.structOuts[nodes[first]]
	var table, own, elems []*node
	var marked []*variant
	undo := func() ([]*node, error) {
		for _, v := range marked {
			v.elem = ""
		}
		return nodes, nil
	}
	// mark makes the reads of a FLATTEN's VALUE in a condition read a
	// lambda's element instead.
	mark := func(term []*node, reads func(v *variant) bool) {
		for _, n := range term {
			walk(n, func(n *node) {
				if n.kind == kVariant && reads(n.v) {
					n.v.elem = "_sf_e"
					marked = append(marked, n.v)
				}
			})
		}
	}
	and := func(to []*node, term []*node) []*node {
		if len(to) > 0 {
			to = append(to, raw(" AND "))
		}
		return append(append(append(to, raw("(")), trim(term)...), raw(")"))
	}
	for _, term := range whereTerms(nodes[where+1 : end]) {
		named := func(alias string) bool { return mentionsQualifier(render(term), alias) }
		switch {
		case fold != nil && named(fold.alias):
			mark(term, func(v *variant) bool { return v.json && v.obj == nil && isValueOf(v.base, fold.alias) })
			if named(fold.alias) || slices.ContainsFunc(others, named) {
				return undo() // another column of the last FLATTEN, or another FLATTEN
			}
			elems = and(elems, term)
		case named(so.alias):
			mark(term, func(v *variant) bool { return v.obj == so.s && isValueOf(v.base, so.alias) })
			if named(so.alias) || fold != nil && named(fold.alias) || slices.ContainsFunc(others, named) {
				return undo()
			}
			own = and(own, term)
		default:
			if fold != nil && named(fold.alias) || slices.ContainsFunc(others, named) {
				return undo()
			}
			table = and(table, term)
		}
	}
	out := append([]*node{}, trimRight(nodes[:where])...)
	if len(table) > 0 {
		j := prevSig(out, first)
		switch {
		case out[j].isOp(","):
			out[j] = raw(" JOIN")
		case out[j].word() != "JOIN":
			return undo()
		default:
			if c := prevSig(out, j); c >= 0 && out[c].word() == "CROSS" {
				out[c] = raw("")
			}
		}
		on := append(append([]*node{raw(" ON (")}, table...), raw(")"))
		out = append(out[:first+1], append(on, out[first+1:]...)...)
	}
	filter := func(list *node, cond []*node) {
		inner := seq(nil, list.kids...)
		list.kids = append(append([]*node{raw("list_filter("), inner, raw(", lambda _sf_e: ")}, cond...), raw(")"))
	}
	if len(own) > 0 {
		filter(so.list, own)
	}
	if len(elems) > 0 {
		filter(fold.values, elems)
	}
	return append(append(out, raw(" ")), nodes[end:]...), nil
}

// valueWhere moves the conditions of the ON that whereOn made of a WHERE,
// for a FLATTEN of a column whose elements its select reads only as text
// (see flattenUnnest), that read nothing of the FLATTEN but its VALUE, into
// a list_filter of the elements it unnests, lambdas whose element the
// conditions' reads of VALUE read. DuckDB 1.5.6 unnests a column in a
// lateral subquery where it reads it, after the joins before it, only when
// nothing filters the unnested values; a filter on them, as dashboards
// write WHERE f.value <> 'unknown', made it a dependent join, which held
// the rows aside and joined them back on their lists: 85 s for a query
// with six such FLATTENs of 40 million rows each. The conditions left stay
// in the ON, which DuckDB pushes down to the tables, and an ON left with
// none is ON (true). Conditions may read other columns, which the lambdas
// capture.
func (t *translator) valueWhere(nodes []*node) ([]*node, error) {
	for k := 0; k < len(nodes); k++ {
		on, ok := t.valueOns[nodes[k]]
		fold := t.valueFolds[nodes[k]]
		if !ok || fold == nil {
			continue
		}
		open, close := slices.Index(nodes, on[0]), slices.Index(nodes, on[1])
		if open < k || close < open {
			continue
		}
		and := func(to []*node, term []*node) []*node {
			if len(to) > 0 {
				to = append(to, raw(" AND "))
			}
			return append(append(append(to, raw("(")), trim(term)...), raw(")"))
		}
		var keep, elems []*node
		for _, term := range whereTerms(nodes[open+1 : close]) {
			type read struct {
				v    *variant
				elem string
			}
			var marked []read
			for _, n := range term {
				walk(n, func(n *node) {
					if n.kind == kVariant && n.v.text && n.v.elem != "" && isValueOf(n.v.base, fold.alias) {
						marked = append(marked, read{n.v, n.v.elem})
						n.v.elem = "_sf_e"
					}
				})
			}
			if len(marked) == 0 || mentionsQualifier(render(term), fold.alias) {
				// No read of its VALUE, or a read of another column of it.
				for _, r := range marked {
					r.v.elem = r.elem
				}
				keep = and(keep, term)
				continue
			}
			elems = and(elems, term)
		}
		if len(elems) == 0 {
			continue
		}
		inner := seq(nil, fold.values.kids...)
		fold.values.kids = append(append([]*node{raw("list_filter("), inner, raw(", lambda _sf_e: ")}, elems...), raw(")"))
		if len(keep) == 0 {
			keep = []*node{raw("true")}
		}
		out := append(append([]*node{}, nodes[:open+1]...), keep...)
		nodes = append(out, nodes[close:]...)
	}
	return nodes, nil
}

// isValueOf reports whether base, a variant's base, is alias.VALUE.
func isValueOf(base *node, alias string) bool {
	q, c, _ := strings.Cut(strings.TrimSpace(base.String()), ".")
	return strings.EqualFold(strings.Trim(q, `"`), alias) && strings.EqualFold(strings.Trim(c, `"`), "VALUE")
}

// structField is FLATTEN's input in when it reads a structFlatten's VALUE by
// one key: the key's list and JSON parts, as SQL.
func structField(in *node) (list, json string, ok bool) {
	if in.kind != kVariant || in.v.obj == nil || len(in.v.segs) != 1 || in.v.segs[0].dyn != nil || in.v.segs[0].isIndex {
		return "", "", false
	}
	list, _, json = in.v.obj.parts(structColumn(in.v.base), in.v.obj.key(in.v.segs[0].key))
	return list, json, true
}
