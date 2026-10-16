package pgwire

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Text surgery for policies (see protect.go): statements are changed by
// replacing the exact stretches of their text that DuckDB's parser says are
// table references, so everything else the client wrote stays as written.
// DuckDB's parse (json_serialize_sql) gives each table reference's byte
// offset in the text; the reference's extent, its alias and its AT clause
// are read from there and checked against the parse.

// edit replaces text[start:end] with text.
type edit struct {
	start, end int
	text       string
}

// applyEdits applies non-overlapping edits to text.
func applyEdits(text string, edits []edit) (string, error) {
	if len(edits) == 0 {
		return text, nil
	}
	sorted := slices.Clone(edits)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].start < sorted[j].start })
	var b strings.Builder
	pos := 0
	for _, e := range sorted {
		if e.start < pos || e.end < e.start || e.end > len(text) {
			return "", errors.New("overlapping edits")
		}
		b.WriteString(text[pos:e.start])
		b.WriteString(e.text)
		pos = e.end
	}
	b.WriteString(text[pos:])
	return b.String(), nil
}

// shiftEdits moves edits made to a part of a text that starts at offset.
func shiftEdits(edits []edit, offset int) []edit {
	out := make([]edit, len(edits))
	for i, e := range edits {
		out[i] = edit{e.start + offset, e.end + offset, e.text}
	}
	return out
}

// nameSpan is where a possibly qualified name, a string used as a name, or
// a quoted identifier is written in a text.
type nameSpan struct {
	start, end int
	parts      []string
}

// scanName reads the name at text[at:]: identifiers joined by dots (with
// white space and comments allowed around the dots), or one 'string'.
func scanName(text string, at int) (nameSpan, bool) {
	sp := nameSpan{start: at}
	sc := &scanner{s: text, i: at}
	if at < len(text) && text[at] == '\'' {
		v, ok := sc.literal()
		if !ok || v == nil {
			return sp, false
		}
		sp.parts, sp.end = []string{*v}, sc.i
		return sp, true
	}
	parts, ok := sc.name()
	if !ok {
		return sp, false
	}
	sp.parts, sp.end = parts, sc.i
	return sp, true
}

// sameParts reports whether a name as written matches the parts DuckDB's
// parse gave for it.
func sameParts(written, parsed []string) bool {
	return slices.EqualFunc(written, parsed, strings.EqualFold)
}

// tableRefSpan is a table reference's extent in a text: its name, then its
// alias with any column names, then its AT clause.
type tableRefSpan struct {
	name nameSpan
	// alias is the alias and its column list as written, without AS, or
	// "" when there is none.
	alias string
	// at is the AT (...) clause as written, or "".
	at string
	// end is where the reference ends: after the alias and AT clause.
	end int
}

// scanTableRef reads the table reference ref at its offset in text,
// checking it against what the parse says it is.
func scanTableRef(text string, ref *refNode) (tableRefSpan, error) {
	var sp tableRefSpan
	bad := func(what string) (tableRefSpan, error) {
		return sp, fmt.Errorf("cannot find the %s of %s in the statement", what, strings.Join(ref.parts, "."))
	}
	name, ok := scanName(text, ref.loc)
	if !ok || !sameParts(name.parts, ref.parts) {
		return bad("name")
	}
	sp.name, sp.end = name, name.end
	sc := &scanner{s: text, i: name.end}
	if ref.alias != "" {
		sc.keywords("AS")
		sc.skip()
		start := sc.i
		alias, ok := sc.name()
		if !ok || len(alias) != 1 || !strings.EqualFold(alias[0], ref.alias) {
			return bad("alias")
		}
		if len(ref.columnAliases) > 0 {
			sc.skip()
			if sc.i >= len(sc.s) || sc.s[sc.i] != '(' {
				return bad("column aliases")
			}
			end := matchParen(sc.s, sc.i)
			if end < 0 {
				return bad("column aliases")
			}
			sc.i = end + 1
		}
		sp.alias, sp.end = text[start:sc.i], sc.i
	}
	if ref.at {
		if !sc.keywords("AT") {
			return bad("AT clause")
		}
		sc.skip()
		if sc.i >= len(sc.s) || sc.s[sc.i] != '(' {
			return bad("AT clause")
		}
		end := matchParen(sc.s, sc.i)
		if end < 0 {
			return bad("AT clause")
		}
		sp.at, sp.end = "AT "+text[sc.i:end+1], end+1
	}
	return sp, nil
}

// prevWord returns the last word of SQL code before at, upper-cased, and
// where it starts.
func prevWord(text string, at int) (string, int) {
	word, start, pos := "", -1, 0
	for _, seg := range sqllex.Segments(text) {
		segStart := pos
		pos += len(seg.Text)
		if segStart >= at {
			break
		}
		if seg.Kind != sqllex.Code {
			if seg.Kind != sqllex.Comment {
				word, start = "", -1
			}
			continue
		}
		s := seg.Text
		if pos > at {
			s = s[:at-segStart]
		}
		for i := 0; i < len(s); {
			c := s[i]
			switch {
			case sqllex.IdentByte(c):
				j := i
				for j < len(s) && sqllex.IdentByte(s[j]) {
					j++
				}
				word, start = strings.ToUpper(s[i:j]), segStart+i
				i = j
			case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
				i++
			default:
				word, start = "", -1
				i++
			}
		}
	}
	return word, start
}

// --- parse trees ----------------------------------------------------------

// refNode is a table reference in a parse.
type refNode struct {
	loc           int
	parts         []string
	alias         string
	columnAliases []string
	at            bool
	// cte says the reference names a common table expression in scope.
	cte bool
	// summarized says it is the bare name of SUMMARIZE name.
	summarized bool
}

// fnNode is a function call in a parse.
type fnNode struct {
	loc        int
	schema     string
	name       string
	operator   bool
	args       []any
	tableValue bool
}

// parseScan is what a parse holds, for policies: every table reference
// and function call, wherever it is, and whether it has parameters.
type parseScan struct {
	refs      []*refNode
	functions []*fnNode
	params    bool
}

// scanParse walks a statement's parse. Unlike astWalk, which follows what
// DuckDB plans, it visits every node once, the bodies of unused common
// table expressions included. References inside DESCRIBE are left out:
// they read no rows.
func scanParse(n any) *parseScan {
	ps := &parseScan{}
	ps.walk(n, nil)
	return ps
}

func (ps *parseScan) walk(n any, sc *cteScope) {
	switch n := n.(type) {
	case []any:
		for _, c := range n {
			ps.walk(c, sc)
		}
	case map[string]any:
		if cm, ok := n["cte_map"].(map[string]any); ok {
			entries, _ := cm["map"].([]any)
			for _, e := range entries {
				em, _ := e.(map[string]any)
				key, _ := em["key"].(string)
				value, _ := em["value"].(map[string]any)
				def := &cteDef{body: value["query"], scope: sc}
				next := &cteScope{defs: map[string]*cteDef{strings.ToLower(key): def}, parent: sc}
				q, _ := value["query"].(map[string]any)
				if node, _ := q["node"].(map[string]any); str(node["type"]) == "RECURSIVE_CTE_NODE" {
					// The recursive term sees the CTE itself. The first does
					// not: DuckDB binds the name there to a table.
					ps.walk(node["left"], sc)
					ps.walk(node["right"], next)
					for k, v := range node {
						if k != "left" && k != "right" {
							ps.walk(v, sc)
						}
					}
				} else {
					ps.walk(def.body, def.scope)
				}
				sc = next
			}
		}
		switch str(n["type"]) {
		case "BASE_TABLE":
			ref := &refNode{loc: location(n["query_location"]), alias: str(n["alias"])}
			for _, p := range []string{str(n["catalog_name"]), str(n["schema_name"]), str(n["table_name"])} {
				if p != "" {
					ref.parts = append(ref.parts, p)
				}
			}
			for _, a := range asList(n["column_name_alias"]) {
				ref.columnAliases = append(ref.columnAliases, str(a))
			}
			ref.at = n["at_clause"] != nil
			if len(ref.parts) == 1 && sc.find(ref.parts[0]) != nil {
				ref.cte = true
			}
			ps.refs = append(ps.refs, ref)
			ps.walk(n["at_clause"], sc)
			return
		case "SHOW_REF":
			switch str(n["show_type"]) {
			case "DESCRIBE":
				return
			case "SUMMARY":
				// SUMMARIZE name parses as SUMMARIZE SELECT * FROM name,
				// with a select list that is not in the text.
				if q, _ := n["query"].(map[string]any); q != nil && syntheticStar(q) {
					if from, _ := q["from_table"].(map[string]any); str(from["type"]) == "BASE_TABLE" {
						before := len(ps.refs)
						ps.walk(from, sc)
						if len(ps.refs) > before {
							ps.refs[before].summarized = true
						}
						return
					}
				}
			}
		case "TABLE_FUNCTION":
			if f, ok := n["function"].(map[string]any); ok {
				ps.functions = append(ps.functions, &fnNode{loc: location(f["query_location"]), schema: str(f["schema"]),
					name: str(f["function_name"]), args: asList(f["children"]), tableValue: true})
				ps.walk(f["children"], sc)
			}
			for k, v := range n {
				if k != "function" {
					ps.walk(v, sc)
				}
			}
			return
		}
		switch str(n["class"]) {
		case "FUNCTION":
			ps.functions = append(ps.functions, &fnNode{loc: location(n["query_location"]), schema: str(n["schema"]),
				name: str(n["function_name"]), operator: n["is_operator"] == true, args: asList(n["children"])})
		case "PARAMETER":
			ps.params = true
		}
		for k, v := range n {
			if k != "cte_map" {
				ps.walk(v, sc)
			}
		}
	}
}

// syntheticStar reports whether a SELECT node's select list is a lone *
// that the text does not have.
func syntheticStar(q map[string]any) bool {
	list := asList(q["select_list"])
	if len(list) != 1 {
		return false
	}
	star, _ := list[0].(map[string]any)
	return str(star["class"]) == "STAR" && location(star["query_location"]) < 0
}

// location reads a query_location, which is -1 (as an unsigned maximum)
// when the node is not in the text.
func location(v any) int {
	f, ok := v.(float64)
	if !ok || f < 0 || f > 1<<53 {
		return -1
	}
	return int(f)
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

// --- composed texts ---------------------------------------------------------

// composer builds a text from pieces of another, remembering where each
// piece came from, so positions in the parse of the new text map back.
// UPDATE, DELETE and MERGE are parsed this way: DuckDB serializes only
// queries, so their parts are put together as one.
type composer struct {
	b    strings.Builder
	segs []composedSeg
}

type composedSeg struct{ at, from, n int }

// lit adds text of the composer's own.
func (c *composer) lit(s string) { c.b.WriteString(s) }

// orig adds src[start:end] of the original text.
func (c *composer) orig(src string, start, end int) {
	c.segs = append(c.segs, composedSeg{c.b.Len(), start, end - start})
	c.b.WriteString(src[start:end])
}

func (c *composer) String() string { return c.b.String() }

// toOrig maps a position in the composed text to the original, if it is
// in a piece that came from there.
func (c *composer) toOrig(pos int) (int, bool) {
	for _, s := range c.segs {
		if pos >= s.at && pos < s.at+s.n {
			return s.from + pos - s.at, true
		}
	}
	return 0, false
}

// span is a stretch of a text.
type span struct{ start, end int }

func (s span) empty() bool { return s.end <= s.start }

func trimSpan(text string, s span) span {
	for s.start < s.end && isSpace(text[s.start]) {
		s.start++
	}
	for s.end > s.start && (isSpace(text[s.end-1]) || text[s.end-1] == ';') {
		s.end--
	}
	return s
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' }

// keywordAt returns where words follow each other at the top level of
// text[s.start:s.end], as a position in text, or -1.
func keywordAt(text string, s span, words ...string) int {
	i := topLevelKeyword(text[s.start:s.end], words...)
	if i < 0 {
		return -1
	}
	return s.start + i
}

// afterKeywords returns the position just after words that start at pos.
func afterKeywords(text string, pos int, words ...string) int {
	sc := &scanner{s: text, i: pos}
	sc.keywords(words...)
	return sc.i
}

// blank returns text with the given spans replaced by spaces.
func blank(text string, spans ...span) string {
	b := []byte(text)
	for _, s := range spans {
		for i := max(s.start, 0); i < min(s.end, len(b)); i++ {
			b[i] = ' '
		}
	}
	return string(b)
}
