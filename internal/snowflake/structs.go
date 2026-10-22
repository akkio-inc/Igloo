package snowflake

import (
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
// list part, a VARCHAR[] that holds a value that is a list of strings, and
// a JSON part that holds any other value (see sf_list_part). A key an
// object lacks is NULL in both. The FLATTEN's struct is a seventh column,
// _sf_fields, beside its VALUE as JSON, which DuckDB computes only if the
// statement reads it.
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
// literals, a list part and a JSON part for any other.
func (s *structFlatten) fieldNames(i int) (str, list, json string) {
	n := strconv.Itoa(i + 1)
	if !s.general[i] {
		return "_sf_k" + n, "", ""
	}
	return "", "_sf_l" + n, "_sf_j" + n
}

// parts are the list and JSON parts of key i in the struct x, as SQL: for a
// key of string literals, no list and its string as JSON; for a key the
// objects lack, NULL.
func (s *structFlatten) parts(x string, i int) (list, json string) {
	if i < 0 {
		return "CAST(NULL AS VARCHAR[])", "CAST(NULL AS JSON)"
	}
	str, l, j := s.fieldNames(i)
	if str != "" {
		return "CAST(NULL AS VARCHAR[])", "to_json(" + x + "." + str + ")"
	}
	return x + "." + l, x + "." + j
}

// json is key i of the struct x as JSON.
func (s *structFlatten) json(x string, i int) string {
	l, j := s.parts(x, i)
	if i < 0 || !s.general[i] {
		return j
	}
	return "sf_parts_json(" + l + ", " + j + ")"
}

// collectStructs finds, before anything is translated, the FLATTENs that
// flattenStructs translates: those of an array constant, [...] or
// ARRAY_CONSTRUCT(...), of OBJECT_CONSTRUCT or OBJECT_CONSTRUCT_KEEP_NULL
// calls with string literals for keys, without PATH, OUTER or RECURSIVE or
// with a MODE other than 'OBJECT', and with an alias that the statement
// names nothing else by and uses only to qualify columns. Their VALUE, not
// renamed, must be read only by a key: alias.VALUE:key or
// alias.VALUE['key'], GET_PATH(alias.VALUE, 'key...') or GET(alias.VALUE,
// 'key'), whatever path or cast follows, a later FLATTEN's INPUT included.
// A FLATTEN at a level that selects a star or filters its rows (see
// filters), or in a statement that names VALUE unqualified, stays JSON.
func (t *translator) collectStructs(nodes []*node) {
	type candidate struct {
		alias    *node
		s        *structFlatten
		rejected bool
	}
	byAlias := map[string]*candidate{}
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
			if have := byAlias[up]; have != nil {
				have.rejected = true
				continue
			}
			c := &candidate{alias: nodes[a], s: structInput(args)}
			byAlias[up] = c
			if c.s == nil || up == "VALUE" || selectsStar(nodes) || !valueKept(cols) || filters(nodes, i) {
				c.rejected = true
			}
		}
	}
	find(nodes)
	if len(byAlias) == 0 {
		return
	}
	unqualified := false
	var check func(nodes []*node, call string)
	check = func(nodes []*node, call string) {
		for i, n := range nodes {
			if n.kind == kGroup && !colLists[n] {
				fn := ""
				if p := prevSig(nodes, i); p >= 0 {
					fn = nodes[p].word()
				}
				check(n.kids, fn)
			}
			name, _, ok := n.name()
			if !ok {
				continue
			}
			up := strings.ToUpper(name)
			p := prevSig(nodes, i)
			if up == "VALUE" && (p < 0 || !nodes[p].isOp(".") && !nodes[p].isOp(":") && nodes[p].word() != "AS") {
				unqualified = true
			}
			c := byAlias[up]
			if c == nil || n == c.alias {
				continue
			}
			// A qualifier: alias.column, but not schema.alias.column.
			q := nextSig(nodes, i)
			if p >= 0 && nodes[p].isOp(".") || q < 0 || !nodes[q].isOp(".") {
				c.rejected = true
				continue
			}
			r := nextSig(nodes, q)
			if r < 0 {
				c.rejected = true
				continue
			}
			if col, _, ok := nodes[r].name(); !ok || strings.ToUpper(col) == "VALUE" && !readsByKey(nodes, i, r, call) {
				c.rejected = true
			}
		}
	}
	check(nodes, "")
	if unqualified {
		return
	}
	for _, c := range byAlias {
		if !c.rejected {
			name, _, _ := c.alias.name()
			t.structs[strings.ToLower(name)] = c.s
		}
	}
}

// filters reports whether the level of a FLATTEN at nodes[i] filters the
// rows it joins: a WHERE, an ON or USING after the FLATTEN, or a HAVING with
// a condition on the groups' keys, which DuckDB pushes below the grouping.
// DuckDB then joins a FLATTEN to its rows with a dependent join, whose hash
// table holds each row's structs of lists at a higher cost than JSON text:
// 2.9 s against 1.8 s for 2 million rows of five lists with a WHERE on the
// elements, 2.3 s against 2 s with a HAVING on the keys.
func filters(nodes []*node, i int) bool {
	for k, n := range nodes {
		switch n.word() {
		case "WHERE":
			return true
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
// element a struct (see structFlatten) in the column _sf_fields as well.
func flattenStructs(s *structFlatten, in *node, alias string, cols *node, wantThis bool) (*node, error) {
	var elems []*node
	for k, e := range in.elems {
		if e.obj == nil {
			return nil, &Error{Code: CodeSyntax, Message: "expected OBJECT_CONSTRUCT in FLATTEN's INPUT"}
		}
		if k > 0 {
			elems = append(elems, raw(", "))
		}
		elems = append(elems, structOf(s, e.obj))
	}
	x := "_sf_flatten._sf_value"
	entries := make([]string, len(s.keys))
	for i, k := range s.keys {
		entries[i] = "{'k': CAST(" + quoteString(k) + " AS VARCHAR), 'v': CAST(" + s.json(x, i) + " AS JSON)}"
	}
	value := "to_json(map_from_entries(list_sort(list_filter([" + strings.Join(entries, ", ") + "], lambda e: e.v IS NOT NULL))))"
	this := "CAST(NULL AS JSON)"
	n := itoa(len(in.elems))
	lists := []*node{raw("unnest([")}
	lists = append(lists, elems...)
	lists = append(lists, raw("]) AS _sf_value, unnest(range("+n+")) AS _sf_index"))
	if wantThis {
		this = "_sf_flatten._sf_this"
		lists = append(lists, raw(", unnest(repeat(["), in, raw("], "+n+")) AS _sf_this"))
	}
	kids := []*node{raw("LATERAL (SELECT CAST(NULL AS BIGINT) AS \"SEQ\", CAST(NULL AS VARCHAR) AS \"KEY\", " +
		"coalesce('[' || _sf_flatten._sf_index || ']', '') AS \"PATH\", _sf_flatten._sf_index AS \"INDEX\", " + value + " AS \"VALUE\", " +
		this + " AS \"THIS\", " + x + " AS _sf_fields FROM (SELECT ")}
	kids = append(kids, lists...)
	kids = append(kids, raw(") AS _sf_flatten)"+aliasClause(alias, cols)))
	return seq(nil, kids...), nil
}

// structOf is the struct of an object's keys and values. Each field is
// cast to its type, which also keeps DuckDB 1.5 from losing the name of a
// field whose value is a macro's call inside a lateral subquery.
func structOf(s *structFlatten, o *objectParts) *node {
	kids := []*node{raw("{")}
	for i, k := range s.keys {
		if i > 0 {
			kids = append(kids, raw(", "))
		}
		var x *node
		for j, have := range o.keys {
			if have == k {
				x = o.values[j]
			}
		}
		str, l, j := s.fieldNames(i)
		if str != "" {
			if x == nil || x.word() == "NULL" {
				x = raw("NULL")
			}
			kids = append(kids, raw("'"+str+"': CAST("), x, raw(" AS VARCHAR)"))
			continue
		}
		list, json := structParts(x, o.keepNull)
		kids = append(kids, raw("'"+l+"': CAST("), list, raw(" AS VARCHAR[]), '"+j+"': CAST("), json, raw(" AS JSON)"))
	}
	return seq(nil, append(kids, raw("}"))...)
}

// structParts are the list and JSON parts of an object's value x (nil for
// none): a constant list of strings as the list, a column as its list when
// it is a list of strings and as JSON otherwise (see sf_list_part), and
// anything else as JSON, as OBJECT_CONSTRUCT takes it; NULL in both for a
// value OBJECT_CONSTRUCT leaves out, and JSON null for a NULL that
// OBJECT_CONSTRUCT_KEEP_NULL keeps.
func structParts(x *node, keep bool) (list, json *node) {
	null := raw("NULL")
	if keep {
		null = raw("CAST('null' AS JSON)")
	}
	switch {
	case x == nil:
		return raw("NULL"), raw("NULL")
	case x.word() == "NULL":
		return raw("NULL"), null
	case stringList(x):
		kids := []*node{raw("[")}
		for k, e := range x.elems {
			if k > 0 {
				kids = append(kids, raw(", "))
			}
			kids = append(kids, e)
		}
		return seq(nil, append(kids, raw("]"))...), raw("NULL")
	}
	if c := jsonArgColumn(x); c != nil {
		fn := "sf_json_part("
		if keep {
			fn = "sf_json_part_keep_null("
		}
		return seq(nil, raw("sf_list_part("), c, raw(")")), seq(nil, raw(fn), c, raw(")"))
	}
	if keep {
		return raw("NULL"), seq(nil, raw("coalesce(to_json("), x, raw("), CAST('null' AS JSON))"))
	}
	return raw("NULL"), seq(nil, raw("to_json("), x, raw(")"))
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
// VALUE as JSON.
func (v *variant) structRead() (string, bool) {
	if v.obj == nil || len(v.segs) == 0 || v.segs[0].dyn != nil || v.segs[0].isIndex {
		return "", false
	}
	x := structColumn(v.base)
	i := v.obj.key(v.segs[0].key)
	rest := &variant{segs: v.segs[1:]}
	json := v.obj.json(x, i)
	str := "json_extract_string(" + json + ", " + rest.path() + ")"
	if i >= 0 && !v.obj.general[i] && len(rest.segs) == 0 {
		f, _, _ := v.obj.fieldNames(i)
		str = x + "." + f // a string as itself
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

// structField is FLATTEN's input in when it reads a structFlatten's VALUE by
// one key: the key's list and JSON parts, as SQL.
func structField(in *node) (list, json string, ok bool) {
	if in.kind != kVariant || in.v.obj == nil || len(in.v.segs) != 1 || in.v.segs[0].dyn != nil || in.v.segs[0].isIndex {
		return "", "", false
	}
	list, json = in.v.obj.parts(structColumn(in.v.base), in.v.obj.key(in.v.segs[0].key))
	return list, json, true
}
