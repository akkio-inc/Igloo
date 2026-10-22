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
// literals, a list, a string and a JSON part for any other.
func (s *structFlatten) fieldNames(i int) (str, list, string_, json string) {
	n := strconv.Itoa(i + 1)
	if !s.general[i] {
		return "_sf_k" + n, "", "", ""
	}
	return "", "_sf_l" + n, "_sf_s" + n, "_sf_j" + n
}

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

// json is key i of the struct x as JSON, and text as text.
func (s *structFlatten) json(x string, i int) string {
	l, st, j := s.parts(x, i)
	switch {
	case i < 0:
		return j
	case !s.general[i]:
		return "to_json(" + st + ")"
	}
	return "sf_fields_json(" + l + ", " + st + ", " + j + ")"
}

func (s *structFlatten) text(x string, i int) string {
	l, st, j := s.parts(x, i)
	if i < 0 || !s.general[i] {
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
// argument groups, and the reads of their VALUE, by VALUE's nodes.
func (t *translator) collectStructs(nodes []*node) {
	type candidate struct {
		alias, args *node
		up          string
		level       []*node
		s           *structFlatten
	}
	var found []candidate
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
			if a < 0 {
				continue
			}
			var cols *node
			if k := nextSig(nodes, a); k >= 0 && nodes[k].isGroup('(') {
				cols = nodes[k]
				colLists[cols] = true
			}
			name, _, _ := nodes[a].name()
			up := strings.ToUpper(name)
			if s := structInput(args); s != nil && up != "VALUE" && !selectsStar(nodes) && valueKept(cols) && readInPlace(nodes, i, up) &&
				!t.filters(nodes, i, up) {
				found = append(found, candidate{alias: nodes[a], args: args, up: up, level: nodes, s: s})
			}
		}
	}
	find(nodes)
	for _, c := range found {
		if reads, ok := readsOnlyByKey(c.level, c.alias, c.up, colLists); ok {
			t.structs[c.args] = c.s
			for _, r := range reads {
				t.structReads[r] = c.s
			}
		}
	}
}

// readsOnlyByKey reports whether a select, nodes, and what it nests read
// the VALUE of the FLATTEN aliased by the node alias (upper-cased up) only
// by a key (see readsByKey), name the alias only to qualify columns, and
// name no VALUE unqualified, outside FLATTENs' column lists (colLists), nor
// the alias in a subquery; it returns the reads' VALUE nodes.
func readsOnlyByKey(nodes []*node, alias *node, up string, colLists map[*node]bool) (reads []*node, ok bool) {
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
					if !readsByKey(nodes, i, r, call) {
						ok = false
					}
					reads = append(reads, nodes[r])
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
// FLATTEN's VALUE (see readsOneKey) and the statement reads none of its
// columns but VALUE and SEQ.
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
	var cols *node
	if a := aliasIndex(nodes, after); a >= 0 {
		if k := nextSig(nodes, a); k >= 0 && nodes[k].isGroup('(') {
			cols = nodes[k]
		}
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
			if lastArgs == nil || !readsOneKey(lastArgs, alias) || t.listsWanted(nodes, cols) || t.thisWanted(nodes, cols) ||
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
// a key: followed by :key or ['key'], or the first argument of GET_PATH of
// a path that starts with a key, or of GET of a key, where call is the
// function whose arguments nodes are.
func readsByKey(nodes []*node, a, r int, call string) bool {
	if nodes[r].kind != kWord {
		return false
	}
	if r+1 < len(nodes) && nodes[r+1].isGroup('[') {
		k := trim(nodes[r+1].kids)
		if len(k) != 1 {
			return false
		}
		_, ok := k[0].stringValue()
		return ok
	}
	if q := nextSig(nodes, r); q >= 0 && nodes[q].isOp(":") {
		k := nextSig(nodes, q)
		return k >= 0 && (nodes[k].kind == kWord || nodes[k].kind == kQuoted)
	}
	if call != "GET_PATH" && call != "GET" {
		return false
	}
	args := splitArgs(nodes)
	if len(args) != 2 || len(sig(args[0])) != 3 || trim(args[0])[0] != nodes[a] {
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
	return err == nil && len(segs) > 0 && !segs[0].isIndex
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

// stringList reports whether x is an array constant of string literals.
func stringList(x *node) bool {
	if len(x.elems) == 0 {
		return false
	}
	for _, e := range x.elems {
		if _, ok := e.stringValue(); !ok {
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
	if v.elem != "" {
		x = v.elem
	}
	i := v.obj.key(v.segs[0].key)
	rest := &variant{segs: v.segs[1:]}
	json := v.obj.json(x, i)
	str := v.obj.text(x, i)
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
