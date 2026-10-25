package snowflake

import "strings"

// Snowflake keeps a temporary table or view in the schema its name says,
// for the session that made it: dbt's incremental models create
// db.schema.model__dbt_tmp, merge it into db.schema.model and drop it, by
// that name. DuckDB keeps temporary objects in its temp catalog only, and
// refuses a temporary table named in any other. So the dialect creates such
// an object in temp.main, under its own name, and the session records the
// schema it was named in (Result.Temporary); its later statements that name
// it in that schema, with or without the database, read temp.main's
// (Options.Temporary), as Snowflake finds the session's temporary object
// before a table of the same name. Dropping it ends that (Result.Dropped).

// TempName is a temporary table's or view's schema and name, lower-cased,
// as a statement in Snowflake's dialect named it.
type TempName struct{ Schema, Name string }

// temporaryNames rewrites the names in toks of the temporary objects the
// statement creates in a schema, or that known says the session has, to
// their names in temp.main (see the top of this file). It returns the
// tokens and what of them the statement creates or drops.
func temporaryNames(toks []token, known func(schema, name string) bool) (out []token, created, dropped *TempName) {
	var s []int
	for i, tok := range toks {
		if tok.kind != kSpace && tok.kind != kComment {
			s = append(s, i)
		}
	}
	w := func(k int) string {
		if k < len(s) && toks[s[k]].kind == kWord {
			return strings.ToUpper(toks[s[k]].text)
		}
		return ""
	}
	op := func(k int, o string) bool {
		return k >= 0 && k < len(s) && toks[s[k]].kind == kOp && toks[s[k]].text == o
	}
	part := func(k int) (string, bool) {
		if k >= len(s) {
			return "", false
		}
		switch tok := toks[s[k]]; tok.kind {
		case kWord:
			return strings.ToLower(tok.text), true
		case kQuoted:
			return strings.ToLower(tok.val), true
		}
		return "", false
	}

	// Where the statement's own name starts: of CREATE TEMPORARY TABLE or
	// VIEW, or DROP TABLE or VIEW.
	head, creates := -1, false
	switch w(0) {
	case "CREATE":
		k := 1
		if w(k) == "OR" && w(k+1) == "REPLACE" {
			k += 2
		}
		for ; ; k++ {
			switch w(k) {
			case "LOCAL", "GLOBAL", "VOLATILE":
				continue
			case "TEMP", "TEMPORARY":
				creates = true
				continue
			}
			break
		}
		if creates && (w(k) == "TABLE" || w(k) == "VIEW") {
			k++
			if w(k) == "IF" && w(k+1) == "NOT" && w(k+2) == "EXISTS" {
				k += 3
			}
			head = k
		}
		creates = head >= 0
	case "DROP":
		if w(1) == "TABLE" || w(1) == "VIEW" {
			head = 2
			if w(2) == "IF" && w(3) == "EXISTS" {
				head = 4
			}
		}
	}

	// Each qualified name of two or three parts (not a function's, nor a
	// path into semi-structured data) that names one of them becomes
	// temp.main.name.
	rewrite := map[int][2]int{} // first part's sig index -> last part's
	for k := 0; k < len(s); k++ {
		if _, ok := part(k); !ok || op(k-1, ".") || op(k-1, ":") {
			continue
		}
		end := k
		for op(end+1, ".") {
			if _, ok := part(end + 2); !ok {
				break
			}
			end += 2
		}
		// A column list may follow the statement's own name; elsewhere a
		// name before ( is a function's.
		n := (end-k)/2 + 1
		if n < 2 || n > 3 || op(end+1, "(") && k != head {
			k = end
			continue
		}
		schema, _ := part(end - 2)
		name, _ := part(end)
		tn := TempName{Schema: schema, Name: name}
		switch {
		case k == head && creates:
			created = &tn
		case known != nil && known(schema, name):
			if k == head {
				dropped = &tn
			}
		default:
			k = end
			continue
		}
		rewrite[k] = [2]int{k, end}
		k = end
	}
	if len(rewrite) == 0 {
		return toks, created, dropped
	}
	out = make([]token, 0, len(toks)+2)
	for k, i := 0, 0; i < len(toks); i++ {
		for k < len(s) && s[k] < i {
			k++
		}
		r, ok := rewrite[k]
		if !ok || k >= len(s) || s[k] != i {
			out = append(out, toks[i])
			continue
		}
		last := toks[s[r[1]]]
		out = append(out, token{kind: kWord, text: "temp", pos: last.pos, end: last.pos}, token{kind: kOp, text: ".", pos: last.pos, end: last.pos},
			token{kind: kWord, text: "main", pos: last.pos, end: last.pos}, token{kind: kOp, text: ".", pos: last.pos, end: last.pos}, last)
		i = s[r[1]]
	}
	return out, created, dropped
}
