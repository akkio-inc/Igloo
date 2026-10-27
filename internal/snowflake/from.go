package snowflake

import (
	"slices"
	"strings"
)

// clauses translates or refuses Snowflake's clauses of FROM and of joins
// that DuckDB lacks or reads otherwise: ASOF JOIN ... MATCH_CONDITION,
// DIRECTED joins, time travel by OFFSET, BEFORE and CHANGES, stages,
// CONNECT BY and the (+) outer join. Models' methods are translated before
// (see modelMethods).
func (t *translator) clauses(nodes []*node) ([]*node, error) {
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]
		next := nextSig(nodes, i)
		switch {
		case n.isOp("@"):
			return nil, unsupported("stages (@stage)", "load files with COPY INTO from a location in IGLOO_ALLOWED_LOCATIONS")
		case n.isGroup('(') && len(sig(n.kids)) == 1 && n.kids[sig(n.kids)[0]].isOp("+"):
			return nil, unsupported("the (+) outer join operator", "write the join as LEFT JOIN ... ON")
		case n.word() == "CONNECT" && next >= 0 && nodes[next].word() == "BY",
			n.word() == "CONNECT_BY_ROOT", n.word() == "SYS_CONNECT_BY_PATH":
			return nil, unsupported("CONNECT BY", "write the hierarchy as WITH RECURSIVE")
		case n.word() == "SEMANTIC_VIEW" && next >= 0 && nodes[next].isGroup('('):
			return nil, unsupported("SEMANTIC_VIEW", "")
		case n.word() == "MATCH_RECOGNIZE" && next >= 0 && nodes[next].isGroup('('):
			return nil, unsupported("MATCH_RECOGNIZE", "")
		case n.word() == "DIRECTED" && next >= 0 && nodes[next].word() == "JOIN":
			// DIRECTED fixes the join order; the result is the same.
			nodes = replace(nodes, i, next)
			i--
		case (n.word() == "AT" || n.word() == "BEFORE" || n.word() == "CHANGES" || n.word() == "END") && next >= 0 && isNamedArgs(nodes[next]):
			var err error
			if nodes, err = timeTravel(nodes, i, next); err != nil {
				return nil, err
			}
		case n.word() == "ASOF" && next >= 0 && nodes[next].word() == "JOIN":
			var err error
			if nodes, err = asofJoin(nodes, next); err != nil {
				return nil, err
			}
		}
	}
	return nodes, nil
}

// isNamedArgs reports whether n is a group of named arguments, (NAME => ...).
func isNamedArgs(n *node) bool {
	if !n.isGroup('(') {
		return false
	}
	idx := sig(n.kids)
	return len(idx) >= 2 && n.kids[idx[0]].kind == kWord && n.kids[idx[1]].isOp("=>")
}

// timeTravel translates AT (OFFSET => seconds), which Igloo's time travel
// lacks, and refuses Snowflake's clauses that it has no counterpart for.
// AT (TIMESTAMP => ...) is Igloo's own and passes as written.
func timeTravel(nodes []*node, i, g int) ([]*node, error) {
	kids := nodes[g].kids
	idx := sig(kids)
	switch nodes[i].word() {
	case "BEFORE":
		return nil, unsupported("BEFORE (...)", "read a snapshot with AT (TIMESTAMP => ...) or AT (VERSION => ...)")
	case "CHANGES", "END":
		return nil, unsupported("CHANGES (...)", "")
	}
	switch kids[idx[0]].word() {
	case "OFFSET":
		value := trim(kids[idx[1]+1:])
		nodes[g] = &node{kind: kGroup, open: '(', pos: nodes[g].pos, end: nodes[g].end, kids: []*node{
			raw("TIMESTAMP => current_timestamp + to_microseconds(CAST(("), seq(value, value...), raw(") * 1000000 AS BIGINT))"),
		}}
	case "STATEMENT", "STREAM":
		return nil, unsupported("AT ("+kids[idx[0]].word()+" => ...)", "read a snapshot with AT (TIMESTAMP => ...) or AT (VERSION => ...)")
	}
	// Snowflake writes the table's alias after AT (...), DuckDB before.
	if alias, end, _ := tableAlias(nodes, g); alias != "" && nodes[i].word() == "AT" {
		moved := append([]*node{raw("AS ")}, trim(nodes[nextSig(nodes, g):end])...)
		if moved[1].word() == "AS" {
			moved = moved[1:]
		}
		rest := append(append([]*node{}, nodes[i:g+1]...), nodes[end:]...)
		nodes = append(append(append(nodes[:i:i], moved...), raw(" ")), rest...)
	}
	return nodes, nil
}

// asofJoin translates ASOF JOIN t MATCH_CONDITION (c) [ON e], whose JOIN
// is nodes[join]. Snowflake keeps the left rows without a match, as
// DuckDB's ASOF LEFT JOIN does, and DuckDB takes the match condition as
// part of ON.
func asofJoin(nodes []*node, join int) ([]*node, error) {
	m := -1
	for k := join + 1; k < len(nodes); k++ {
		if nodes[k].word() == "MATCH_CONDITION" {
			m = k
			break
		}
		if nodes[k].word() == "JOIN" || nodes[k].word() == "ON" || nodes[k].word() == "WHERE" {
			break
		}
	}
	g := -1
	if m >= 0 {
		g = nextSig(nodes, m)
	}
	if g < 0 || !nodes[g].isGroup('(') {
		return nil, &Error{Code: CodeSyntax, Message: "expected MATCH_CONDITION (...) after ASOF JOIN"}
	}
	cond := nodes[g]
	on := nextSig(nodes, g)
	switch {
	case on >= 0 && nodes[on].word() == "USING":
		return nil, unsupported("ASOF JOIN ... USING", "write the join condition with ON")
	case on < 0 || nodes[on].word() != "ON":
		// MATCH_CONDITION (c) becomes ON (c).
		nodes = replace(nodes, m, g+1, raw("ON "), cond)
	default:
		// MATCH_CONDITION (c) ON e becomes ON (c) AND (e).
		end := len(nodes)
		for k := on + 1; k < len(nodes); k++ {
			w := nodes[k].word()
			if nodes[k].isOp(",") || joinEndWords[w] && !((w == "LEFT" || w == "RIGHT") && nextSigOr(nodes, k) != k && nodes[nextSig(nodes, k)].isGroup('(')) {
				end = k
				break
			}
		}
		e := trim(nodes[on+1 : end])
		var after []*node // the white space before what ends the condition
		for k := end - 1; k > on && !nodes[k].significant(); k-- {
			after = append([]*node{nodes[k]}, after...)
		}
		nodes = replace(nodes, m, end, append([]*node{raw("ON "), cond, raw(" AND "), group(e...)}, after...)...)
	}
	return replace(nodes, join, join+1, raw("LEFT JOIN")), nil
}

// joinEndWords end a join's ON condition.
var joinEndWords = map[string]bool{
	"WHERE": true, "GROUP": true, "HAVING": true, "QUALIFY": true, "ORDER": true, "LIMIT": true, "UNION": true,
	"EXCEPT": true, "MINUS": true, "INTERSECT": true, "JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true,
	"FULL": true, "CROSS": true, "NATURAL": true, "ASOF": true, "WINDOW": true, "LATERAL": true, "OFFSET": true,
	"FETCH": true, "SAMPLE": true, "TABLESAMPLE": true, "PIVOT": true, "UNPIVOT": true,
}

// tableFunctions translates Snowflake's table functions in FROM:
// [LATERAL] FLATTEN(...), TABLE(FLATTEN(...)), SPLIT_TO_TABLE and
// GENERATOR, and TABLE(f(...)) of other table functions.
func (t *translator) tableFunctions(nodes []*node) ([]*node, error) {
	var flats []flattened
	removed := map[*node]bool{}
	var hoists []*hoist
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]
		w := n.word()
		var fn string
		var call *node // the function's argument group
		start, end := i, 0
		switch w {
		case "TABLE":
			g := nextSig(nodes, i)
			if g < 0 || !nodes[g].isGroup('(') {
				continue
			}
			inner := sig(nodes[g].kids)
			if len(inner) == 1 && (nodes[g].kids[inner[0]].kind == kString || nodes[g].kids[inner[0]].kind == kVar) {
				// TABLE('name') and TABLE($name) name a table, as IDENTIFIER does.
				name, err := t.nameOf("TABLE", nodes[g].kids)
				if err != nil {
					return nil, err
				}
				nodes = replace(nodes, i, g+1, &node{kind: kRaw, text: name, pos: n.pos, end: nodes[g].end})
				continue
			}
			if len(inner) != 2 || nodes[g].kids[inner[0]].kind != kWord || !nodes[g].kids[inner[1]].isGroup('(') {
				continue
			}
			fn = nodes[g].kids[inner[0]].word()
			call = nodes[g].kids[inner[1]]
			end = g + 1
			switch fn {
			case "FLATTEN", "SPLIT_TO_TABLE", "GENERATOR":
			case "RESULT_SCAN":
				return nil, unsupported("RESULT_SCAN", "keep the result in a temporary table with CREATE TEMP TABLE ... AS")
			default:
				// TABLE(f(...)) is f(...) in DuckDB.
				nodes = replace(nodes, i, end, seq(nodes[i:end], nodes[g].kids...))
				continue
			}
		case "FLATTEN", "SPLIT_TO_TABLE":
			g := nextSig(nodes, i)
			if g < 0 || !nodes[g].isGroup('(') {
				continue
			}
			p := prevSig(nodes, i)
			if p >= 0 && nodes[p].isOp(".") {
				continue
			}
			// The whole of a group: TABLE(FLATTEN(...)), which the level
			// above translates.
			if p < 0 && nextSig(nodes, g) < 0 {
				continue
			}
			fn, call, end = w, nodes[g], g+1
		default:
			continue
		}
		// LATERAL before it is part of what it becomes.
		if p := prevSig(nodes, start); p >= 0 && nodes[p].word() == "LATERAL" {
			start = p
		}
		alias, aliasEnd, cols := tableAlias(nodes, end-1)
		var out *node
		var err error
		switch fn {
		case "FLATTEN":
			st := t.structs[call]
			if h := t.hoists[call]; h != nil {
				// Hoisted into its FROM item's select list (see hoistOf).
				if call != h.reader {
					if h.s == nil {
						h.list, err = t.listOf(call, h)
					} else {
						h.list, err = t.hoistList(call, h)
					}
					if err != nil {
						return nil, err
					}
					hoists = append(hoists, h)
				}
				out = raw("")
				removed[out] = true
				flats = append(flats, flattened{alias: alias, out: out, structs: true})
				break
			}
			if alias == "" {
				t.gen++
				alias = "flatten"
				if t.gen > 1 {
					alias = "flatten_" + itoa(t.gen)
				}
			}
			out, err = t.flattenTable(call, alias, cols, t.thisWanted(nodes, cols), t.onlyValues[call], st)
			flats = append(flats, flattened{alias: alias, out: out, structs: st != nil})
			t.flattenOuts[out] = alias
		case "SPLIT_TO_TABLE":
			if alias == "" {
				alias = "split_to_table"
			}
			out, err = t.splitToTable(call, alias, cols)
		case "GENERATOR":
			if alias == "" {
				alias = "generator"
			}
			out, err = t.generator(call, alias)
		}
		if err != nil {
			return nil, err
		}
		if alias != "" && aliasEnd > end {
			end = aliasEnd
		}
		out.pos, out.end = nodes[start].pos, nodes[end-1].end
		nodes = replace(nodes, start, end, out)
		i = start
	}
	if len(hoists) > 0 {
		var err error
		if nodes, err = hoistStructs(nodes, removed, hoists); err != nil {
			return nil, err
		}
	}
	return whereOn(jsonAhead(nodes, flats), flats), nil
}

// whereOn moves the WHERE of a select whose last FROM item is a FLATTEN
// into the ON of that FLATTEN's join: FROM t, LATERAL (...) AS f WHERE p
// becomes FROM t JOIN LATERAL (...) AS f ON (p), the same rows when every
// join is an inner one. DuckDB unnests a list or map column of the rows a
// lateral subquery is joined to where it reads it, but with a WHERE in the
// select it plans a dependent join instead, which holds those rows' lists
// or maps in a hash table: flattening a map of seven strings in 30 million
// rows ran out of 18 GiB that way, and 5 million took 3.6 s rather than
// 0.6 s. Commas before that FLATTEN become CROSS JOINs, so that the joins
// nest from the left and the ON sees every FROM item before it: FROM a, b,
// LATERAL f would become a, (b JOIN LATERAL f ON (p)), whose p cannot read
// a, which DuckDB refuses when f reads b ("Non-inner join on correlated
// columns not supported"). It is left as it is when the select has another
// FROM or WHERE, a set operation, an outer, natural, ASOF, positional, semi
// or anti join, an ON after such a comma (whose unqualified names would see
// more items), a subquery in its WHERE, or a WHERE that names a select
// item's alias, which an ON cannot see. The WHERE of a select with FLATTENs
// of structs is structWhere's.
func whereOn(nodes []*node, flats []flattened) []*node {
	if len(flats) == 0 || slices.ContainsFunc(flats, func(f flattened) bool { return f.structs }) {
		return nodes // structWhere's
	}
	sel, from, where, end, ok := whereClause(nodes)
	if !ok || where < 0 {
		return nodes
	}
	last := prevSig(nodes, where)
	if last < 0 || !slices.ContainsFunc(flats, func(f flattened) bool { return f.out == nodes[last] }) {
		return nodes
	}
	join := prevSig(nodes, last)
	if join < 0 || !nodes[join].isOp(",") && nodes[join].word() != "JOIN" {
		return nodes
	}
	var commas []int
	for k := from + 1; k < join; k++ {
		switch {
		case nodes[k].isOp(","):
			commas = append(commas, k)
		case nodes[k].word() == "ON" && len(commas) > 0:
			return nodes
		}
	}
	cond := nodes[where+1 : end]
	if !movable(cond, selectAliases(nodes, sel, from)) {
		return nodes
	}
	out := append([]*node{}, nodes[:last+1]...)
	for _, k := range commas {
		out[k] = raw(" CROSS JOIN")
	}
	if out[join].isOp(",") {
		out[join] = raw(" JOIN")
	} else if c := prevSig(out, join); c >= 0 && out[c].word() == "CROSS" {
		out[c] = raw("")
	}
	out = append(out, raw(" ON ("))
	out = append(out, trim(cond)...)
	out = append(out, raw(") "))
	return append(out, nodes[end:]...)
}

// whereClause finds, at one level of a statement, a select's SELECT, FROM
// and WHERE, and the end of the WHERE's condition (where is -1 for a select
// without one); ok is false when the level has another FROM or WHERE, a set
// operation, or an outer, natural, ASOF, positional, semi or anti join or
// USING, where a WHERE cannot move into the joins.
func whereClause(nodes []*node) (sel, from, where, end int, ok bool) {
	sel, from, where = -1, -1, -1
	for i, n := range nodes {
		switch n.word() {
		case "SELECT":
			if from < 0 {
				sel = i
			}
		case "FROM":
			if from >= 0 {
				return 0, 0, 0, 0, false
			}
			from = i
		case "WHERE":
			if where >= 0 || from < 0 {
				return 0, 0, 0, 0, false
			}
			where = i
		case "UNION", "EXCEPT", "INTERSECT", "MINUS":
			return 0, 0, 0, 0, false
		case "LEFT", "RIGHT", "FULL", "OUTER", "NATURAL", "ASOF", "POSITIONAL", "ANTI", "SEMI", "USING":
			if from >= 0 && where < 0 {
				return 0, 0, 0, 0, false
			}
		}
	}
	if sel < 0 || from < 0 {
		return 0, 0, 0, 0, false
	}
	end = len(nodes)
	if where >= 0 {
		for j := where + 1; j < len(nodes); j++ {
			if whereEnds[nodes[j].word()] || nodes[j].isOp(";") {
				end = j
				break
			}
		}
	}
	return sel, from, where, end, true
}

// selectAliases are the upper-cased aliases of a select's items, from its
// SELECT at nodes[sel] to its FROM at nodes[from].
func selectAliases(nodes []*node, sel, from int) map[string]bool {
	aliases := map[string]bool{}
	start := sel + 1
	if q := nextSig(nodes, sel); q >= 0 && (nodes[q].word() == "DISTINCT" || nodes[q].word() == "ALL") {
		start = q + 1
	}
	for _, item := range splitArgs(nodes[start:from]) {
		idx := sig(item)
		if len(idx) < 2 {
			continue
		}
		alias, before := item[idx[len(idx)-1]], item[idx[len(idx)-2]]
		if name, _, ok := alias.name(); ok && (before.word() == "AS" || endsOperand(before) && !before.isOp(".")) {
			aliases[strings.ToUpper(name)] = true
		}
	}
	return aliases
}

// whereEnds are the words that end a WHERE clause.
var whereEnds = map[string]bool{
	"GROUP": true, "HAVING": true, "QUALIFY": true, "ORDER": true, "LIMIT": true, "WINDOW": true, "OFFSET": true,
	"FETCH": true,
}

// movable reports whether a WHERE condition can be an ON condition: it has
// no subquery and names none of aliases (upper case) unqualified.
func movable(nodes []*node, aliases map[string]bool) bool {
	for i, n := range nodes {
		if n.kind == kGroup || n.kind == kSeq {
			if k := sig(n.kids); n.kind == kGroup && len(k) > 0 && (n.kids[k[0]].word() == "SELECT" || n.kids[k[0]].word() == "WITH") {
				return false
			}
			if !movable(n.kids, aliases) {
				return false
			}
			continue
		}
		name, _, ok := n.name()
		if !ok || !aliases[strings.ToUpper(name)] {
			continue
		}
		p, q := prevSig(nodes, i), nextSig(nodes, i)
		if (p < 0 || !nodes[p].isOp(".")) && (q < 0 || !nodes[q].isOp(".") && !nodes[q].isGroup('(')) {
			return false
		}
	}
	return true
}

// flattened is a FLATTEN that tableFunctions translated: its alias, what
// it became, and whether its elements are structs (see flattenStructs).
type flattened struct {
	alias   string
	out     *node
	structs bool
}

// jsonAhead turns, ahead of the FLATTENs of one level that a later FLATTEN
// there reads (FLATTEN(INPUT => [OBJECT_CONSTRUCT('k', m.list), ...]) AS f,
// then FLATTEN(INPUT => f.value:k)), the columns that their inputs give to
// the functions that build JSON (sf_to_json(m.c), see semiArg) into that
// JSON, in a subquery over their table: (SELECT * REPLACE (sf_to_json(m.c)
// AS c) FROM t AS m) AS m. DuckDB joins such a FLATTEN to its rows with a
// dependent join, which holds each row's columns that the FLATTEN reads in
// a hash table; lists of strings, Igloo's ARRAY columns, cost several times
// as much there as JSON text (6.3 s against 2.6 s for 2 million rows of five
// lists; 480 s against 265 s for a report that flattened objects built from
// a table's ARRAY columns). sf_to_json of JSON is that JSON, so the inputs
// read the same. A column is turned only when every mention of its name at
// that level is such an argument, so nothing else there sees the change,
// and only in a table, view or subquery that is the one FROM item with that
// alias, written plainly ([schema.]name [AS] alias or (SELECT ...) [AS]
// alias). A FLATTEN whose elements are structs keeps its columns, which a
// later FLATTEN reads without a dependent join.
func jsonAhead(nodes []*node, flats []flattened) []*node {
	if len(flats) < 2 {
		return nodes
	}
	// The columns to turn, by table alias, in the order first met.
	turn := map[string][]*node{}
	var aliases []string
	for i, f := range flats[:len(flats)-1] {
		if f.structs {
			continue
		}
		read := false
		for _, g := range flats[i+1:] {
			if mentionsQualifier(g.out.String(), f.alias) {
				read = true
				break
			}
		}
		if !read {
			continue
		}
		walk(f.out, func(n *node) {
			q, c, ok := builtColumn(n)
			if !ok {
				return
			}
			key := strings.ToUpper(q)
			for _, have := range turn[key] {
				if strings.EqualFold(have.String(), c.String()) {
					return
				}
			}
			if turn[key] == nil {
				aliases = append(aliases, key)
			}
			turn[key] = append(turn[key], c)
		})
	}
	if len(aliases) == 0 {
		return nodes
	}
	text := render(nodes)
	built := map[string]int{}
	for _, n := range nodes {
		walk(n, func(n *node) {
			if _, c, ok := builtColumn(n); ok {
				name, _, _ := c.name()
				built[strings.ToUpper(name)]++
			}
		})
	}
	for _, a := range aliases {
		var cols []string
		for _, c := range turn[a] {
			name, _, _ := c.name()
			if wordCount(text, name) == built[strings.ToUpper(name)] {
				cols = append(cols, c.String())
			}
		}
		if len(cols) == 0 {
			continue
		}
		start, end, alias, ok := fromItem(nodes, a)
		if !ok {
			continue
		}
		list := make([]string, len(cols))
		for k, c := range cols {
			list[k] = "sf_to_json(" + alias + "." + c + ") AS " + c
		}
		item := nodes[start:end]
		out := seq(item, append(append([]*node{raw("(SELECT * REPLACE (" + strings.Join(list, ", ") + ") FROM ")},
			item...), raw(") AS "+alias))...)
		nodes = replace(nodes, start, end, out)
	}
	return nodes
}

// walk calls fn for n and every node inside it.
func walk(n *node, fn func(*node)) {
	fn(n)
	for _, k := range n.kids {
		walk(k, fn)
	}
}

// builtColumn reports whether n is sf_to_json(q.c), a column given to a
// function that builds JSON (see semiArg), and returns its qualifier's name
// and the column's node.
func builtColumn(n *node) (q string, c *node, ok bool) {
	col := jsonArgColumn(n)
	if col == nil {
		return "", nil, false
	}
	ref := col.kids
	idx := sig(ref)
	if len(idx) != 3 || !ref[idx[1]].isOp(".") {
		return "", nil, false
	}
	q, _, ok = ref[idx[0]].name()
	if !ok {
		return "", nil, false
	}
	if _, _, ok = ref[idx[2]].name(); !ok {
		return "", nil, false
	}
	return q, ref[idx[2]], true
}

// jsonArgColumn returns the column that n reads when n is a column given to
// a function that builds JSON, sf_to_json(c) (see jsonArg), or nil.
func jsonArgColumn(n *node) *node {
	if !n.column || n.kind != kSeq || len(n.kids) != 3 || n.kids[0].kind != kRaw || n.kids[0].text != "sf_to_json(" {
		return nil
	}
	return n.kids[1]
}

// wordCount counts the words in DuckDB text that are name, in any case,
// quoted or not; string literals and names inside others count too, which
// errs toward more.
func wordCount(text, name string) int {
	n := 0
	up, w := strings.ToUpper(text), strings.ToUpper(name)
	for i := strings.Index(up, w); i >= 0; {
		before := i == 0 || !identByte(up[i-1])
		after := i+len(w) == len(up) || !identByte(up[i+len(w)])
		if before && after {
			n++
		}
		next := strings.Index(up[i+1:], w)
		if next < 0 {
			break
		}
		i += 1 + next
	}
	return n
}

// mentionsQualifier reports whether DuckDB text names a column of alias:
// alias. or "alias". in any case.
func mentionsQualifier(text, alias string) bool {
	up, a := strings.ToUpper(text), strings.ToUpper(alias)
	for _, w := range []string{a, `"` + a + `"`} {
		for i := strings.Index(up, w); i >= 0; {
			end := i + len(w)
			if (i == 0 || !identByte(up[i-1]) && up[i-1] != '.') && strings.HasPrefix(strings.TrimLeft(up[end:], " \t\r\n"), ".") &&
				(w[0] == '"' || end == len(up) || !identByte(up[end])) {
				return true
			}
			next := strings.Index(up[i+1:], w)
			if next < 0 {
				break
			}
			i += 1 + next
		}
	}
	return false
}

// fromItem finds, at one level of a statement, the FROM item that alias
// (upper case) names, written [schema.]name [AS] alias, (SELECT ...) [AS]
// alias or a bare name: the one name there that is alias and not part of a
// dotted name, after FROM, JOIN or a comma and before what may end an item.
// It returns the item's span and the alias as written.
func fromItem(nodes []*node, alias string) (start, end int, as string, ok bool) {
	found := -1
	for i, n := range nodes {
		name, _, isName := n.name()
		if !isName || strings.ToUpper(name) != alias {
			continue
		}
		if p := prevSig(nodes, i); p >= 0 && nodes[p].isOp(".") {
			continue
		}
		if q := nextSig(nodes, i); q >= 0 && nodes[q].isOp(".") {
			continue
		}
		if sameNamedTable(nodes, i, alias) {
			continue // FROM t AS t: the alias names the item, not the table's name
		}
		if found >= 0 {
			return 0, 0, "", false
		}
		found = i
	}
	if found < 0 {
		return 0, 0, "", false
	}
	if q := nextSig(nodes, found); q >= 0 && !nodes[q].isOp(",") && !itemEnds[nodes[q].word()] {
		return 0, 0, "", false
	}
	startsItem := func(i int) bool {
		p := prevSig(nodes, i)
		if p < 0 || nodes[p].word() != "FROM" && nodes[p].word() != "JOIN" && !nodes[p].isOp(",") {
			return false
		}
		// In a FROM clause, not a select list.
		for j := p; j >= 0; j-- {
			switch nodes[j].word() {
			case "FROM":
				return true
			case "SELECT", "WHERE", "GROUP", "HAVING", "QUALIFY", "ORDER", "WINDOW", "LIMIT":
				return false
			}
		}
		return false
	}
	if startsItem(found) && nodes[found].kind == kWord && !notAliasWords[nodes[found].word()] {
		return found, found + 1, nodes[found].String(), true // a bare name
	}
	r := prevSig(nodes, found)
	if r >= 0 && nodes[r].word() == "AS" {
		r = prevSig(nodes, r)
	}
	if r < 0 {
		return 0, 0, "", false
	}
	switch {
	case nodes[r].isGroup('('):
		k := sig(nodes[r].kids)
		if len(k) == 0 || nodes[r].kids[k[0]].word() != "SELECT" && nodes[r].kids[k[0]].word() != "WITH" {
			return 0, 0, "", false
		}
		start = r
	case nodes[r].kind == kWord && !notAliasWords[nodes[r].word()] && nodes[r].word() != "FROM" && nodes[r].word() != "JOIN",
		nodes[r].kind == kQuoted:
		start = qualifiedStart(nodes, r)
	default:
		return 0, 0, "", false
	}
	if !startsItem(start) {
		return 0, 0, "", false
	}
	return start, found + 1, nodes[found].String(), true
}

// sameNamedTable reports whether the name at nodes[i] is a table's name
// that an alias of the same name, alias (upper case), follows: t in t AS t
// or t t.
func sameNamedTable(nodes []*node, i int, alias string) bool {
	q := nextSig(nodes, i)
	if q >= 0 && nodes[q].word() == "AS" {
		q = nextSig(nodes, q)
	}
	if q < 0 {
		return false
	}
	name, _, ok := nodes[q].name()
	return ok && strings.ToUpper(name) == alias
}

// itemEnds are the words that may follow a FROM item.
var itemEnds = map[string]bool{
	"INNER": true, "LEFT": true, "RIGHT": true, "FULL": true, "CROSS": true, "NATURAL": true, "JOIN": true,
	"ON": true, "USING": true, "WHERE": true, "GROUP": true, "HAVING": true, "QUALIFY": true, "ORDER": true,
	"LIMIT": true, "WINDOW": true, "UNION": true, "EXCEPT": true, "MINUS": true, "INTERSECT": true,
	"ASOF": true, "POSITIONAL": true, "ANTI": true, "SEMI": true,
}

// tableArgs reads a table function's arguments: positional ones, and
// name => value ones by upper-cased name.
func (t *translator) tableArgs(g *node) ([][]*node, map[string][]*node) {
	named := map[string][]*node{}
	var pos [][]*node
	for _, a := range splitArgs(g.kids) {
		a = trim(a)
		if idx := sig(a); len(idx) >= 2 && a[idx[1]].isOp("=>") {
			named[a[idx[0]].word()] = trim(a[idx[1]+1:])
			continue
		}
		pos = append(pos, a)
	}
	return pos, named
}

// boolArg reads TRUE or FALSE.
func boolArg(a []*node) (bool, error) {
	if len(a) == 1 {
		switch a[0].word() {
		case "TRUE":
			return true, nil
		case "FALSE":
			return false, nil
		}
	}
	return false, &Error{Code: CodeInvalidValue, Message: "expected TRUE or FALSE, got " + render(a)}
}

// aliasClause renders AS alias(columns).
func aliasClause(alias string, cols *node) string {
	s := " AS " + quoteIdent(alias)
	if cols != nil {
		s += cols.String()
	}
	return s
}

// flattenTable translates FLATTEN(input => ..., path => ..., outer => ...,
// recursive => ..., mode => ...) into a lateral subquery, with FLATTEN's
// columns SEQ, KEY, PATH, INDEX, VALUE and THIS: over unnested lists of the
// elements (see flattenUnnest), or with OUTER or RECURSIVE over DuckDB's
// json_each or json_tree. SEQ is NULL: a lateral subquery cannot number the
// rows it is joined to. st, when set, says its elements are structs (see
// flattenStructs); onlyValues, that its select reads only its VALUE and
// SEQ (see otherColumnsRead).
func (t *translator) flattenTable(g *node, alias string, cols *node, wantThis, onlyValues bool, st *structFlatten) (*node, error) {
	pos, named := t.tableArgs(g)
	input := named["INPUT"]
	if input == nil && len(pos) > 0 {
		input = pos[0]
	}
	if input == nil {
		return nil, &Error{Code: CodeSyntax, Message: "FLATTEN needs an INPUT"}
	}
	for name := range named {
		switch name {
		case "INPUT", "PATH", "OUTER", "RECURSIVE", "MODE":
		default:
			return nil, &Error{Code: CodeSyntax, Message: "FLATTEN has no argument " + name}
		}
	}
	outer, recursive := false, false
	var err error
	if a, ok := named["OUTER"]; ok {
		if outer, err = boolArg(a); err != nil {
			return nil, err
		}
	}
	if a, ok := named["RECURSIVE"]; ok {
		if recursive, err = boolArg(a); err != nil {
			return nil, err
		}
	}
	mode := "BOTH"
	if a, ok := named["MODE"]; ok {
		s, lit := (&call{args: [][]*node{a}}).literal(0)
		if !lit {
			return nil, unsupported("FLATTEN with a computed MODE", "write 'OBJECT', 'ARRAY' or 'BOTH'")
		}
		mode = strings.ToUpper(s)
		if mode != "OBJECT" && mode != "ARRAY" && mode != "BOTH" {
			return nil, &Error{Code: CodeInvalidValue, Message: "FLATTEN's MODE is 'OBJECT', 'ARRAY' or 'BOTH'"}
		}
	}
	prefix := ""
	in := flattenInput(input)
	if a, ok := named["PATH"]; ok {
		s, lit := (&call{args: [][]*node{a}}).literal(0)
		if !lit {
			return nil, unsupported("FLATTEN with a computed PATH", "give the path as a string literal")
		}
		segs, err := parsePath(s)
		if err != nil {
			return nil, err
		}
		v := &variant{segs: segs}
		prefix = s
		in = seq(nil, raw("json_extract(sf_json("), in, raw("), "+v.path()+")"))
	}
	if st != nil {
		return t.flattenStructs(st, in, alias, cols, wantThis)
	}
	if !recursive && !outer {
		out, fold := flattenUnnest(in, prefix, mode, alias, cols, wantThis, onlyValues, t.textValues[g])
		if fold != nil {
			fold.alias = alias
			t.folds[out] = fold
		}
		return out, nil
	}
	parent := "_sf_flatten_input._sf_this"
	tableFn := "json_each"
	cond := "json_type(_sf_flatten_input._sf_this) IN ('ARRAY', 'OBJECT')"
	if recursive {
		parent = "json_extract(_sf_flatten_input._sf_this, _sf_flatten.path)"
		tableFn = "json_tree"
		cond += " AND _sf_flatten.fullkey <> '$'"
	}
	if mode != "BOTH" {
		cond += " AND json_type(" + parent + ") = '" + mode + "'"
	}
	rel := "regexp_replace(substring(_sf_flatten.fullkey, 2), '^\\.', '')"
	path := rel
	if prefix != "" {
		path = quoteString(prefix) + " || CASE WHEN starts_with(" + rel + ", '[') THEN '' ELSE '.' END || " + rel
	}
	// The elements, in their order in the document; with OUTER, a row of
	// NULLs when there are none.
	elements := "(SELECT * FROM " + tableFn + "(_sf_flatten_input._sf_this) AS _sf_flatten WHERE " + cond + ") AS _sf_flatten"
	if outer {
		elements = " LEFT JOIN LATERAL " + elements + " ON true"
	} else {
		elements = ", LATERAL " + elements
	}
	sql := "LATERAL (SELECT CAST(NULL AS BIGINT) AS \"SEQ\", " +
		"CASE WHEN json_type(" + parent + ") = 'OBJECT' THEN _sf_flatten.key END AS \"KEY\", " +
		"coalesce(" + path + ", '') AS \"PATH\", " +
		"CASE WHEN json_type(" + parent + ") = 'ARRAY' THEN CAST(_sf_flatten.key AS BIGINT) END AS \"INDEX\", " +
		"_sf_flatten.value AS \"VALUE\", " + parent + " AS \"THIS\" FROM (SELECT sf_json("
	tail := ") AS _sf_this) AS _sf_flatten_input" + elements + " ORDER BY _sf_flatten.id)"
	return seq(nil, raw(sql), in, raw(tail+aliasClause(alias, cols))), nil
}

// flattenUnnest is FLATTEN without OUTER or RECURSIVE: the elements of an
// array or the members of an object, unnested from lists of their values,
// keys and indexes, in their order in the document. DuckDB unnests lists a
// vector at a time, where json_each, a table function, takes one document
// at a time, and a lateral json_each inside the lateral subquery needed a
// second dependent join and a sort of every element to keep the order: a
// query that flattened arrays of objects built from a million rows' columns,
// and their arrays in turn, ran out of 18 GiB that way, and finishes this
// way. Unnests in one select list are zipped, the shorter padded with NULL,
// so an array's elements have no key and an object's members no index;
// MODE leaves out the others' lists. JSON nulls stay JSON, as in json_each.
//
// The lists come from the sf_flatten_ macros, which take the input as it
// is: JSON or JSON text, or a list or map of strings, Igloo's ARRAY and
// OBJECT columns, whose elements they unnest without going through JSON
// text. An input that is a column is unnested where it is read: DuckDB
// plans an unnest of a lateral subquery without FROM as an unnest of the
// rows it is joined to, where one with FROM takes a dependent join, which
// costs as much again as unnesting (0.3 s against 1 s for 15 million
// elements of 3 million lists). Any other input is computed once, in a
// subquery of its own, since the unnests would each compute it. An array
// constant, [a, b] or ARRAY_CONSTRUCT(a, b), is unnested from a list of its
// elements, without building the array and reading it back: three times
// faster where each row builds objects to flatten. A key of objects that
// flattenStructs made structs is unnested from the struct's field, which
// DuckDB too unnests where it reads it, and when its select reads none of
// KEY, PATH, INDEX and THIS (onlyValues), only its elements, which a lone
// unnest gives faster than three together (0.9 s against 1.4 s for 3
// million rows of three lists), in a node of their own, fold, that
// structWhere may filter. A column whose elements alone its select reads
// is unnested alone too, as text when every read reads text (text, see
// textValuesOf).
func flattenUnnest(in *node, prefix, mode, alias string, cols *node, wantThis, onlyValues, text bool) (out *node, fold *structFold) {
	rel := "CASE WHEN _sf_flatten._sf_key IS NULL THEN '[' || _sf_flatten._sf_index || ']' ELSE _sf_flatten._sf_key END"
	path := rel
	if prefix != "" {
		path = quoteString(prefix) + " || CASE WHEN _sf_flatten._sf_key IS NULL THEN '' ELSE '.' END || " + rel
	}
	key := "_sf_flatten._sf_key"
	this := "CAST(NULL AS JSON)"
	m := quoteString(mode)
	var lists []*node
	l, j, field := structField(in)
	switch {
	case in.elems != nil && prefix == "" && mode != "OBJECT":
		key, path = "CAST(NULL AS VARCHAR)", "'[' || _sf_flatten._sf_index || ']'"
		lists = append(lists, raw("unnest(["))
		for k, e := range in.elems {
			if k > 0 {
				lists = append(lists, raw(", "))
			}
			lists = append(lists, raw("coalesce(to_json("), e, raw("), CAST('null' AS JSON))"))
		}
		n := itoa(len(in.elems))
		lists = append(lists, raw("]) AS _sf_value, unnest(range("+n+")) AS _sf_index"))
		if wantThis {
			this = "_sf_flatten._sf_this"
			lists = append(lists, raw(", unnest(repeat(["), in, raw("], "+n+")) AS _sf_this"))
		}
	case field && prefix == "" && onlyValues:
		// A key of objects flattened as structs (see flattenStructs), of
		// which the statement reads only the elements.
		fold = &structFold{values: seq(nil, raw("sf_parts_values("+l+", "+j+", "+m+")"))}
		lists = append(lists, raw("unnest("), fold.values, raw(") AS _sf_value, CAST(NULL AS VARCHAR) AS _sf_key, CAST(NULL AS BIGINT) AS _sf_index"))
	case field && prefix == "":
		// A key of objects flattened as structs: its list or JSON, which
		// DuckDB unnests where it reads the struct.
		args := l + ", " + j + ", " + m
		lists = append(lists, raw("unnest(sf_parts_values("+args+")) AS _sf_value, unnest(sf_parts_keys("+args+
			")) AS _sf_key, unnest(sf_parts_indexes("+args+")) AS _sf_index"))
		if wantThis {
			this = "_sf_flatten._sf_this"
			lists = append(lists, raw(", unnest(sf_parts_this("+args+")) AS _sf_this"))
		}
	case isColumnNode(in) && prefix == "" && onlyValues:
		// A column of which the statement reads only the elements.
		values := "sf_flatten_values("
		if text {
			values = "sf_flatten_strings("
		}
		lists = append(lists, raw("unnest("+values+in.String()+", "+m+
			")) AS _sf_value, CAST(NULL AS VARCHAR) AS _sf_key, CAST(NULL AS BIGINT) AS _sf_index"))
	case isColumnNode(in):
		x := in.String()
		lists = append(lists, raw("unnest(sf_flatten_values("+x+", "+m+")) AS _sf_value, unnest(sf_flatten_keys("+x+", "+m+
			")) AS _sf_key, unnest(sf_flatten_indexes("+x+", "+m+")) AS _sf_index"))
		if wantThis {
			this = "_sf_flatten._sf_this"
			lists = append(lists, raw(", unnest(sf_flatten_this("+x+", "+m+")) AS _sf_this"))
		}
	default:
		if wantThis {
			this = "sf_json(_sf_flatten._sf_this)"
		}
		lists = append(lists, raw("_sf_this, unnest(sf_flatten_values(_sf_this, "+m+")) AS _sf_value, unnest(sf_flatten_keys(_sf_this, "+m+
			")) AS _sf_key, unnest(sf_flatten_indexes(_sf_this, "+m+")) AS _sf_index FROM (SELECT "), in,
			raw(" AS _sf_this) AS _sf_flatten_input"))
	}
	kids := []*node{raw("LATERAL (SELECT CAST(NULL AS BIGINT) AS \"SEQ\", " + key + " AS \"KEY\", " +
		"coalesce(" + path + ", '') AS \"PATH\", _sf_flatten._sf_index AS \"INDEX\", _sf_flatten._sf_value AS \"VALUE\", " +
		this + " AS \"THIS\" FROM (SELECT ")}
	kids = append(kids, lists...)
	kids = append(kids, raw(") AS _sf_flatten)"+aliasClause(alias, cols)))
	return seq(nil, kids...), fold
}

// thisWanted reports whether a statement may read the THIS of a FLATTEN
// whose columns are named cols (or nil), at level nodes: by name, THIS or
// the sixth of cols, anywhere but in FLATTEN's column lists (which reports
// write in full, naming THIS without reading it), or with a star at that
// level. THIS, the whole input once for each element, is computed only
// then: computed for every FLATTEN of a report that flattened arrays of
// objects built from each row, it ran out of 75 GiB of disk.
func (t *translator) thisWanted(nodes []*node, cols *node) bool {
	if selectsStar(nodes) {
		return true
	}
	name := "THIS"
	if cols != nil {
		names := splitArgs(cols.kids)
		if len(names) < 6 {
			return false
		}
		n := trim(names[5])
		if len(n) != 1 {
			return true
		}
		var ok bool
		if name, _, ok = n[0].name(); !ok {
			return true
		}
		name = strings.ToUpper(name)
	}
	return t.names[name] > t.columnNames[name]
}

// selectsStar reports whether nodes, one level of a statement, select
// columns with a star (*, t.*), which would take a FLATTEN's THIS.
func selectsStar(nodes []*node) bool {
	for i, n := range nodes {
		if !n.isOp("*") {
			continue
		}
		p := prevSig(nodes, i)
		if p < 0 {
			continue
		}
		switch w := nodes[p].word(); {
		case nodes[p].isOp(","), nodes[p].isOp("."), w == "SELECT", w == "DISTINCT", w == "ALL", nodes[p].kind == kNumber:
			return true
		}
	}
	return false
}

// flattenInput is FLATTEN's input as the sf_ macros take it: a column as
// it is, a list or map included, rather than through sf_semi.
func flattenInput(input []*node) *node {
	x := trim(input)
	if len(x) == 1 && x[0].kind == kVariant {
		x[0].v.mode = vJSON
	}
	if len(x) == 1 && x[0].word() == "NULL" {
		return seq(x, raw("CAST(NULL AS JSON)"))
	}
	if isColumnRef(x) {
		n := seq(x, x...)
		n.column = true
		return n
	}
	if len(x) == 1 {
		return x[0]
	}
	return seq(x, x...)
}

// isColumnNode reports whether n, an argument as jsonArg made it, reads a
// column: a table's (see semiArg), or a FLATTEN's VALUE or THIS.
func isColumnNode(n *node) bool {
	if n.kind == kVariant {
		return n.v.json && len(n.v.segs) == 0
	}
	return n.column
}

// splitToTable translates SPLIT_TO_TABLE(s, delimiter): a row for each
// part, with SEQ (NULL, as for FLATTEN), INDEX from 1 and VALUE.
func (t *translator) splitToTable(g *node, alias string, cols *node) (*node, error) {
	pos, named := t.tableArgs(g)
	if len(pos) != 2 || len(named) > 0 {
		return nil, &Error{Code: CodeSyntax, Message: "SPLIT_TO_TABLE takes a string and a delimiter"}
	}
	kids := []*node{raw("LATERAL (SELECT CAST(NULL AS BIGINT) AS \"SEQ\", _sf_i AS \"INDEX\", _sf_split._sf_parts[_sf_i] AS \"VALUE\" " +
		"FROM (SELECT string_split("), seq(pos[0], pos[0]...), raw(", "), seq(pos[1], pos[1]...),
		raw(") AS _sf_parts) AS _sf_split, range(1, len(_sf_split._sf_parts) + 1) AS _sf_range(_sf_i) ORDER BY _sf_i)" +
			aliasClause(alias, cols))}
	return seq(nil, kids...), nil
}

// generator translates GENERATOR(ROWCOUNT => n), n rows of no interest
// but their number.
func (t *translator) generator(g *node, alias string) (*node, error) {
	_, named := t.tableArgs(g)
	n, ok := named["ROWCOUNT"]
	if !ok {
		return nil, unsupported("GENERATOR without ROWCOUNT", "give ROWCOUNT => n")
	}
	if _, ok := named["TIMELIMIT"]; ok {
		return nil, unsupported("GENERATOR's TIMELIMIT", "give ROWCOUNT => n only")
	}
	return seq(nil, raw("range(CAST("), seq(n, n...), raw(" AS BIGINT)) AS "+quoteIdent(alias)+"(_sf_row)")), nil
}

// sample translates SAMPLE and TABLESAMPLE after a table: [BERNOULLI | ROW
// | SYSTEM | BLOCK] (p | n ROWS) [SEED | REPEATABLE (s)].
func (t *translator) sample(nodes []*node) ([]*node, error) {
	for i := 0; i < len(nodes); i++ {
		w := nodes[i].word()
		if w != "SAMPLE" && w != "TABLESAMPLE" {
			continue
		}
		p := prevSig(nodes, i)
		if p < 0 || !endsOperand(nodes[p]) {
			continue
		}
		j := nextSig(nodes, i)
		method := "bernoulli"
		if j >= 0 {
			switch nodes[j].word() {
			case "BERNOULLI", "ROW":
				j = nextSig(nodes, j)
			case "SYSTEM", "BLOCK":
				method = "system"
				j = nextSig(nodes, j)
			}
		}
		if j < 0 || !nodes[j].isGroup('(') {
			continue
		}
		g := nodes[j]
		idx := sig(g.kids)
		size := ""
		switch {
		case len(idx) == 1 && g.kids[idx[0]].kind == kNumber:
			size = g.kids[idx[0]].tok.text + "%"
		case len(idx) == 2 && g.kids[idx[0]].kind == kNumber && g.kids[idx[1]].word() == "ROWS":
			size = g.kids[idx[0]].tok.text + " ROWS"
			method = "reservoir"
		default:
			return nil, unsupported("SAMPLE with a computed size", "give the percentage or number of rows as a number")
		}
		end := j + 1
		seed := ""
		if s := nextSig(nodes, j); s >= 0 && (nodes[s].word() == "SEED" || nodes[s].word() == "REPEATABLE") {
			sg := nextSig(nodes, s)
			if sg < 0 || !nodes[sg].isGroup('(') {
				return nil, &Error{Code: CodeSyntax, Message: "expected SEED (number)"}
			}
			seed = " REPEATABLE " + nodes[sg].String()
			end = sg + 1
		}
		out := &node{kind: kRaw, text: "TABLESAMPLE " + method + "(" + size + ")" + seed, pos: nodes[i].pos, end: nodes[end-1].end}
		nodes = replace(nodes, i, end, out)
	}
	return nodes, nil
}

// fromWords are the words after which a table reference follows.
var fromWords = map[string]bool{"FROM": true, "JOIN": true}

// values names the columns of VALUES in FROM as Snowflake does, COLUMN1,
// COLUMN2 and so on, and puts parentheses around a FROM VALUES list.
func (t *translator) values(nodes []*node) ([]*node, error) {
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]
		p := prevSig(nodes, i)
		if p < 0 || !fromWords[nodes[p].word()] {
			continue
		}
		switch {
		case n.word() == "VALUES":
			// FROM VALUES (...), (...) [alias]
			end := i + 1
			for {
				g := nextSig(nodes, end-1)
				if g < 0 || !nodes[g].isGroup('(') {
					break
				}
				end = g + 1
				c := nextSig(nodes, end-1)
				if c < 0 || !nodes[c].isOp(",") {
					break
				}
				if g2 := nextSig(nodes, c); g2 < 0 || !nodes[g2].isGroup('(') {
					break
				}
				end = c + 1
			}
			grp := group(nodes[i:end]...)
			grp.pos, grp.end = nodes[i].pos, nodes[end-1].end
			nodes = replace(nodes, i, end, grp)
			fallthrough
		case nodes[i].isGroup('('):
			g := nodes[i]
			idx := sig(g.kids)
			if len(idx) < 2 || g.kids[idx[0]].word() != "VALUES" || !g.kids[idx[1]].isGroup('(') {
				continue
			}
			ncols := len(splitArgs(g.kids[idx[1]].kids))
			names := make([]string, ncols)
			for k := range names {
				names[k] = "column" + itoa(k+1)
			}
			colList := "(" + strings.Join(names, ", ") + ")"
			alias, aliasEnd, cols := tableAlias(nodes, i)
			switch {
			case alias == "":
				nodes = replace(nodes, i+1, i+1, raw(" AS _sf_values"+colList))
			case cols == nil:
				nodes = replace(nodes, aliasEnd, aliasEnd, raw(colList))
			}
		}
	}
	return nodes, nil
}

// top translates SELECT TOP n into a LIMIT at the end of the select.
func (t *translator) top(nodes []*node) ([]*node, error) {
	idx := sig(nodes)
	for k, i := range idx {
		if nodes[i].word() != "SELECT" {
			continue
		}
		m := k + 1
		if m < len(idx) && (nodes[idx[m]].word() == "DISTINCT" || nodes[idx[m]].word() == "ALL") {
			m++
		}
		if m+1 >= len(idx) || nodes[idx[m]].word() != "TOP" {
			continue
		}
		count := nodes[idx[m+1]]
		if count.kind != kNumber {
			return nil, unsupported("TOP with a computed count", "use LIMIT")
		}
		// The select must end this list: no set operation or LIMIT after it.
		for _, j := range idx[m+2:] {
			switch nodes[j].word() {
			case "UNION", "EXCEPT", "MINUS", "INTERSECT", "LIMIT", "FETCH":
				return nil, unsupported("TOP with "+nodes[j].word(), "use LIMIT")
			}
		}
		end := len(nodes)
		if last := idx[len(idx)-1]; nodes[last].isOp(";") {
			end = last
		}
		out := append([]*node{}, nodes[:idx[m]]...)
		rest := idx[m+1] + 1
		for rest < end && nodes[rest].kind == kSpace {
			rest++
		}
		out = append(out, nodes[rest:end]...)
		out = append(trimRight(out), raw(" LIMIT "+count.tok.text))
		return append(out, nodes[end:]...), nil
	}
	return nodes, nil
}

// pivots names columns as Snowflake does: PIVOT's after each value of
// the IN list, a string with its quotes ('a'); UNPIVOT's names of columns
// in upper case unless quoted.
func (t *translator) pivots(nodes []*node) ([]*node, error) {
	for i, n := range nodes {
		w := n.word()
		if w != "PIVOT" && w != "UNPIVOT" {
			continue
		}
		g := nextSig(nodes, i)
		for g >= 0 && (nodes[g].word() == "INCLUDE" || nodes[g].word() == "EXCLUDE" || nodes[g].word() == "NULLS") {
			g = nextSig(nodes, g)
		}
		if g < 0 || !nodes[g].isGroup('(') {
			continue
		}
		kids := nodes[g].kids
		idx := sig(kids)
		for k := range idx {
			if kids[idx[k]].word() != "FOR" {
				continue
			}
			in := -1
			for m := k + 1; m < len(idx); m++ {
				if kids[idx[m]].word() == "IN" {
					in = m
					break
				}
			}
			if in < 0 || in+1 >= len(idx) || !kids[idx[in+1]].isGroup('(') {
				break
			}
			list := kids[idx[in+1]]
			if w == "PIVOT" {
				if first := sig(list.kids); len(first) > 0 && (list.kids[first[0]].word() == "ANY" || list.kids[first[0]].word() == "SELECT") {
					return nil, unsupported("PIVOT ... IN (ANY) and IN (SELECT ...)", "list the values: IN ('a', 'b')")
				}
				if in+2 < len(idx) && kids[idx[in+2]].word() == "DEFAULT" {
					return nil, unsupported("PIVOT ... DEFAULT ON NULL", "replace NULLs with COALESCE in the select list")
				}
			}
			var out []*node
			for a, item := range splitArgs(list.kids) {
				if a > 0 {
					out = append(out, raw(","))
				}
				out = append(out, trimRight(item)...)
				ii := sig(item)
				if len(ii) != 1 {
					continue
				}
				x := item[ii[0]]
				if w == "PIVOT" {
					if s, ok := x.stringValue(); ok {
						name := "'" + s + "'"
						t.quoted[name] = true
						out = append(out, raw(" AS "+quoteIdent(name)))
					}
				} else if name, quoted, ok := x.name(); ok {
					if !quoted {
						name = strings.ToUpper(name)
					}
					out = append(out, raw(" AS "+quoteString(name)))
				}
			}
			list.kids = out
			break
		}
	}
	return nodes, nil
}
