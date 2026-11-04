package pgwire

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Sibling aggregations. Generated reports often count the same rows several
// ways, each in a common table expression of its own over the same join, the
// same filter and the same groups:
//
//	WITH seen AS (SELECT DISTINCT m.id, m.x FROM m JOIN c ON c.id = m.id WHERE p),
//	     days AS (SELECT m.id, m.x, count(DISTINCT m.day) AS days FROM m JOIN c ON c.id = m.id WHERE p GROUP BY m.id, m.x),
//	     views AS (SELECT m.id, m.x, count(*) AS views FROM m JOIN c ON c.id = m.id WHERE p GROUP BY m.id, m.x)
//
// DuckDB scans, joins and groups the rows once for each: a dashboard's report
// at ten times its data built three hash tables of 35 million groups each, for
// each of its two cohorts. Igloo computes the groups once, in a common table
// expression marked MATERIALIZED with every aggregate the siblings ask for, and
// each sibling reads its columns from it:
//
//	WITH _igloo_siblings_1 AS MATERIALIZED (SELECT m.id AS _k1, m.x AS _k2,
//	         count(DISTINCT m.day) AS _a1, count(*) AS _a2 FROM m JOIN c ON c.id = m.id WHERE p GROUP BY m.id, m.x),
//	     seen AS (SELECT _k1 AS id, _k2 AS x FROM _igloo_siblings_1), ...
//
// That report took 121 to 128 CPU-seconds rather than 224, and 12 to 20 s rather
// than 40 to 44 on 16 cores. (Counting the distinct values as the groups of a
// grouping by them as well, under one by the siblings' groups, took twice as long:
// DuckDB finished the inner grouping on one thread.)
//
// Siblings are common table expressions of one WITH list whose queries are each
// SELECT [DISTINCT] ... FROM ... [WHERE ...] [GROUP BY ...] and nothing else, with
// the same FROM and WHERE text (compared as normalizedQuery compares), and the
// same groups: a DISTINCT one's are its columns, and a grouped one's are its
// GROUP BY expressions, each of which it selects as written or not at all. A
// grouped one selects only those and calls of count, sum, min, max and avg with
// nothing after them (no FILTER or OVER). Every column is named: by AS, or as the
// column it reads. None is marked MATERIALIZED or NOT MATERIALIZED, none has a
// column list, the list is not RECURSIVE, nothing they read is a common table
// expression the list defines after the first of them, they call no volatile
// function, and the statement has no ? parameters. Where the rewritten statement
// does not prepare, it runs as written.

// siblingPrefix starts the names of the common table expressions sibling
// aggregations become.
const siblingPrefix = "_igloo_siblings_"

// minSiblings is how many siblings are merged at the least. Merging saves each
// sibling but one its scan, join and grouping, and costs keeping the groups, which
// the siblings then read: about one grouping's work, and more where the memory is
// short. Of a dashboard's reports at ten times its data on 8 cores and 22 GB, those
// of three siblings took 453 to 490 CPU-seconds rather than 809 to 908, and those
// of two (a sum and a DISTINCT of the same groups, of 53 million) 20 to 40% more.
var minSiblings = 3

// siblingAggregates are the aggregates siblings may compute: each depends on its
// group's rows alone, not on their order.
var siblingAggregates = map[string]bool{"COUNT": true, "COUNT_STAR": true, "SUM": true, "MIN": true, "MAX": true, "AVG": true}

// mergeSiblings returns query with its sibling aggregations computed once, or ""
// when it has none.
func (s *Server) mergeSiblings(ctx context.Context, sess *session, query string) string {
	if sess == nil || sess.conn == nil || !strings.Contains(strings.ToUpper(query), "WITH") {
		return ""
	}
	volatileNames := func() (map[string]bool, error) {
		f, err := s.functionSets(ctx, sess)
		if err != nil {
			return nil, err
		}
		return f.volatile, nil
	}
	out, err := mergeSiblingText(query, volatileNames)
	if err != nil {
		s.log.Debug("could not merge sibling aggregations", "err", err)
		return ""
	}
	return out
}

// siblingCTE is a common table expression of a WITH list as mergeSiblingText reads
// it: its name and the token that starts it, where its query is (text[open+1:close]),
// and, when it may be a sibling, its parts.
type siblingCTE struct {
	name             string
	nameTok          int
	open, close      int
	ok               bool
	distinct         bool
	from, where      string
	keys             []string // the groups' expressions as written
	norm             []string // and as they compare, sorted
	items            []siblingItem
	fromTok, fromEnd int // the tokens of its FROM and WHERE
}

// siblingItem is a column a sibling selects: its expression as written and as
// it compares, its name, and whether it is an aggregate (else one of the keys).
type siblingItem struct {
	expr, norm, name string
	aggregate        bool
}

// mergeSiblingText rewrites text so that each group of sibling aggregations is
// computed once. volatileNames returns the functions whose calls keep siblings as
// written; it is called only when there are siblings. It returns "" when there are
// none.
func mergeSiblingText(text string, volatileNames func() (map[string]bool, error)) (string, error) {
	toks := codeTokens(text)
	for _, t := range toks {
		if t.punct == '?' {
			return "", nil
		}
	}
	at := make(map[int]int, len(toks))
	for i, t := range toks {
		at[t.start] = i
	}
	var vol map[string]bool
	var edits []edit
	taken := strings.ToLower(text)
	n := 0
	for i, t := range toks {
		if t.word != "WITH" || i+1 >= len(toks) || toks[i+1].word == "RECURSIVE" {
			continue
		}
		list := withList(text, toks, at, i+1)
		if len(list) < 2 {
			continue
		}
		groups := map[string][]int{}
		var order []string
		for j := range list {
			c := &list[j]
			readSibling(text, toks, at, c)
			if !c.ok {
				continue
			}
			key := c.from + "\x00" + c.where + "\x00" + strings.Join(c.norm, "\x00")
			if _, ok := groups[key]; !ok {
				order = append(order, key)
			}
			groups[key] = append(groups[key], j)
		}
		for _, key := range order {
			members := groups[key]
			if len(members) < minSiblings || !siblingNamesResolve(list, members, toks) {
				continue
			}
			if vol == nil {
				var err error
				if vol, err = volatileNames(); err != nil {
					return "", err
				}
			}
			if callsVolatile(list, members, toks, vol) {
				continue
			}
			var name string
			for {
				n++
				name = fmt.Sprintf("%s%d", siblingPrefix, n)
				if !strings.Contains(taken, name) {
					break
				}
			}
			edits = append(edits, siblingEdits(text, toks, list, members, name)...)
		}
	}
	if len(edits) == 0 {
		return "", nil
	}
	out, err := applyEdits(text, edits)
	if err != nil {
		// Siblings inside siblings: leave the statement as written.
		return "", nil
	}
	return out, nil
}

// withList reads the common table expressions of the WITH list whose first name is
// toks[i]; it stops at the first it cannot read.
func withList(text string, toks []codeTok, at map[int]int, i int) []siblingCTE {
	var list []siblingCTE
	for i+2 < len(toks) && toks[i].word != "" {
		c := siblingCTE{name: toks[i].word, nameTok: i, ok: true}
		j := i + 1
		if toks[j].punct == '(' || toks[j].word != "AS" {
			// A column list, or a name DuckDB reads otherwise.
			return list
		}
		j++
		if toks[j].word == "MATERIALIZED" || toks[j].word == "NOT" {
			c.ok = false
			for j < len(toks) && toks[j].punct != '(' {
				j++
			}
		}
		if j >= len(toks) || toks[j].punct != '(' {
			return list
		}
		end := matchParen(text, toks[j].start)
		k, found := at[end]
		if end < 0 || !found {
			return list
		}
		c.open, c.close = toks[j].start, end
		list = append(list, c)
		if k+1 >= len(toks) || toks[k+1].punct != ',' {
			return list
		}
		i = k + 2
	}
	return list
}

// readSibling reads c's query as a sibling's, setting c.ok to whether it may be one.
func readSibling(text string, toks []codeTok, at map[int]int, c *siblingCTE) {
	if !c.ok {
		return
	}
	c.ok = false
	first, last := at[c.open]+1, at[c.close]-1
	if first > last || toks[first].word != "SELECT" {
		return
	}
	// The clauses at the query's own depth.
	depth := 0
	var from, where, group = -1, -1, -1
	for k := first; k <= last; k++ {
		t := toks[k]
		switch t.punct {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			depth--
			continue
		}
		if depth != 0 {
			continue
		}
		switch t.word {
		case "FROM":
			if from < 0 {
				from = k
			}
		case "WHERE":
			if from < 0 || where >= 0 || group >= 0 {
				return
			}
			where = k
		case "GROUP":
			if from < 0 || group >= 0 || k+1 > last || toks[k+1].word != "BY" {
				return
			}
			group = k
		case "HAVING", "QUALIFY", "WINDOW", "ORDER", "LIMIT", "OFFSET", "FETCH", "UNION", "EXCEPT", "INTERSECT",
			"ROLLUP", "CUBE", "GROUPING", "SETS", "SAMPLE", "TABLESAMPLE", "PIVOT", "UNPIVOT":
			return
		}
	}
	if from < 0 || depth != 0 {
		return
	}
	sel := first + 1
	if toks[sel].word == "ALL" {
		return
	}
	if toks[sel].word == "DISTINCT" {
		if toks[sel+1].word == "ON" {
			return
		}
		c.distinct = true
		sel++
	}
	if c.distinct == (group >= 0) {
		// A DISTINCT that groups too, or a SELECT that does neither.
		return
	}
	// The clauses' text, from the end of the word that starts each to the start of
	// the next (quoted names are not tokens, so tokens do not bound them).
	clauseEnd := c.close
	switch {
	case where >= 0:
		clauseEnd = toks[where].start
	case group >= 0:
		clauseEnd = toks[group].start
	}
	if fromText := strings.TrimSpace(text[toks[from].end:clauseEnd]); fromText == "" {
		return
	} else {
		c.from = normalizedQuery(fromText)
	}
	c.fromTok, c.fromEnd = from+1, last
	if where >= 0 {
		end := c.close
		if group >= 0 {
			end = toks[group].start
		}
		w := strings.TrimSpace(text[toks[where].end:end])
		if w == "" {
			return
		}
		c.where = normalizedQuery(w)
	}
	items := splitTop(text, toks, sel, from-1, toks[sel-1].end, toks[from].start)
	if items == nil {
		return
	}
	for _, it := range items {
		si, ok := selectItem(text, toks, it[0], it[1])
		if !ok {
			return
		}
		c.items = append(c.items, si)
	}
	if c.distinct {
		for _, it := range c.items {
			c.keys = append(c.keys, it.expr)
			c.norm = append(c.norm, it.norm)
		}
	} else {
		groups := splitTop(text, toks, group+2, last, toks[group+1].end, c.close)
		if groups == nil {
			return
		}
		for _, g := range groups {
			k := strings.TrimSpace(text[g[0]:g[1]])
			c.keys = append(c.keys, k)
			c.norm = append(c.norm, normalizedQuery(k))
		}
		for k := range c.items {
			it := &c.items[k]
			if slices.Contains(c.norm, it.norm) {
				continue
			}
			if !aggregateCall(it) {
				return
			}
			it.aggregate = true
		}
	}
	// Keys compare in sorted order, and none twice.
	sorted := slices.Clone(c.norm)
	slices.Sort(sorted)
	if len(slices.Compact(slices.Clone(sorted))) != len(sorted) {
		return
	}
	c.norm = sorted
	c.ok = true
}

// splitTop splits text[from:to], whose tokens are first..last, at the commas of
// its own depth, giving each part's start and end in text; nil if a part is empty.
func splitTop(text string, toks []codeTok, first, last, from, to int) [][2]int {
	var parts [][2]int
	depth, start := 0, from
	for k := first; k <= last; k++ {
		switch toks[k].punct {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case ',':
			if depth == 0 {
				if strings.TrimSpace(text[start:toks[k].start]) == "" {
					return nil
				}
				parts = append(parts, [2]int{start, toks[k].start})
				start = toks[k].end
			}
		}
	}
	if strings.TrimSpace(text[start:to]) == "" {
		return nil
	}
	return append(parts, [2]int{start, to})
}

// plainColumn is a column read by name, name or table.name, written plainly.
var plainColumn = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)*$`)

// selectItem reads the select-list item text[start:end]: an expression named by
// AS and a plain word, or a column read by a plain name, which names it; ok is
// false for others.
func selectItem(text string, toks []codeTok, start, end int) (siblingItem, bool) {
	var in []int
	for k, t := range toks {
		if t.start >= start && t.end <= end {
			in = append(in, k)
		}
	}
	depth, as := 0, -1
	for _, k := range in {
		switch toks[k].punct {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		}
		if depth == 0 && toks[k].word == "AS" {
			as = k
		}
	}
	if as >= 0 {
		// The name is one plain word after AS, and the item ends with it.
		if len(in) < 2 || as != in[len(in)-1]-1 {
			return siblingItem{}, false
		}
		alias := toks[as+1]
		if alias.word == "" || strings.TrimSpace(text[alias.end:end]) != "" {
			return siblingItem{}, false
		}
		name := text[alias.start:alias.end]
		if !plainColumn.MatchString(name) {
			return siblingItem{}, false
		}
		expr := strings.TrimSpace(text[start:toks[as].start])
		if expr == "" {
			return siblingItem{}, false
		}
		return siblingItem{expr: expr, norm: normalizedQuery(expr), name: name}, true
	}
	expr := strings.TrimSpace(text[start:end])
	if !plainColumn.MatchString(expr) {
		return siblingItem{}, false
	}
	return siblingItem{expr: expr, norm: normalizedQuery(expr), name: expr[strings.LastIndexByte(expr, '.')+1:]}, true
}

// aggregateCall reports whether it is a call of one of siblingAggregates that is
// the whole expression, with nothing after its parentheses.
func aggregateCall(it *siblingItem) bool {
	sub := codeTokens(it.expr)
	if len(sub) < 3 || !siblingAggregates[sub[0].word] || sub[1].punct != '(' {
		return false
	}
	end := matchParen(it.expr, sub[1].start)
	return end == len(strings.TrimRight(it.expr, " \t\r\n"))-1
}

// siblingNamesResolve reports whether nothing the members read is a common table
// expression the list defines after the first of them, which their merged query,
// defined before it, would not see.
func siblingNamesResolve(list []siblingCTE, members []int, toks []codeTok) bool {
	later := map[string]bool{}
	for j := members[0]; j < len(list); j++ {
		later[list[j].name] = true
	}
	c := list[members[0]]
	for k := c.fromTok; k <= c.fromEnd; k++ {
		if later[toks[k].word] {
			return false
		}
	}
	return true
}

// callsVolatile reports whether the members call a volatile function.
func callsVolatile(list []siblingCTE, members []int, toks []codeTok, vol map[string]bool) bool {
	for _, j := range members {
		c := list[j]
		for k := c.nameTok; k < len(toks) && toks[k].start < c.close; k++ {
			if toks[k].word != "" && vol[strings.ToLower(toks[k].word)] && k+1 < len(toks) && toks[k+1].punct == '(' {
				return true
			}
		}
	}
	return false
}

// siblingEdits defines name, the members' merged query, before the first of them,
// and has each read its columns from it.
func siblingEdits(text string, toks []codeTok, list []siblingCTE, members []int, name string) []edit {
	first := list[members[0]]
	keys := first.keys
	if first.distinct {
		// A grouped member's keys, as written, where there is one.
		for _, j := range members {
			if !list[j].distinct {
				keys = list[j].keys
				break
			}
		}
	}
	keyCol := map[string]string{}
	var cols []string
	for k, e := range keys {
		col := fmt.Sprintf("_k%d", k+1)
		keyCol[normalizedQuery(e)] = col
		cols = append(cols, e+" AS "+col)
	}
	aggCol := map[string]string{}
	var aggs []siblingItem
	for _, j := range members {
		for _, it := range list[j].items {
			if it.aggregate {
				if _, ok := aggCol[it.norm]; !ok {
					aggCol[it.norm] = fmt.Sprintf("_a%d", len(aggCol)+1)
					aggs = append(aggs, it)
				}
			}
		}
	}
	// FROM to the end of the WHERE, as the first member has it.
	f := first
	end := f.close
	for k := f.fromTok; k <= f.fromEnd; k++ {
		if toks[k].word == "GROUP" && depthAt(toks, f.fromTok, k) == 0 {
			end = toks[k].start
			break
		}
	}
	fromWhere := strings.TrimRight(text[toks[f.fromTok-1].start:end], " \t\r\n")
	for _, a := range aggs {
		cols = append(cols, a.expr+" AS "+aggCol[a.norm])
	}
	body := "SELECT " + strings.Join(cols, ", ") + " " + fromWhere + "\nGROUP BY " + strings.Join(keys, ", ")
	def := name + " AS MATERIALIZED (" + body + "\n), "
	edits := []edit{{toks[first.nameTok].start, toks[first.nameTok].start, def}}
	for _, j := range members {
		c := list[j]
		var sel []string
		for _, it := range c.items {
			col := keyCol[it.norm]
			if it.aggregate {
				col = aggCol[it.norm]
			}
			sel = append(sel, col+" AS "+it.name)
		}
		edits = append(edits, edit{c.open + 1, c.close, "SELECT " + strings.Join(sel, ", ") + " FROM " + name})
	}
	return edits
}

// depthAt is the parenthesis depth at toks[k], counted from toks[from].
func depthAt(toks []codeTok, from, k int) int {
	depth := 0
	for i := from; i < k; i++ {
		switch toks[i].punct {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		}
	}
	return depth
}
