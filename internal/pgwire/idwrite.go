package pgwire

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Writes to tables with columns in ID domains (see iddomains.go). Each
// member column has a column beside it that holds its values' ids, which
// statements read as if the table did not have it. Writes must keep it
// right, so before a write to such a table is prepared Igloo rewrites it
// (memberWrites):
//
//   - An INSERT that names no columns, which DuckDB would match by
//     position against every column, the ids' included, names the
//     table's other columns: INSERT INTO t VALUES (...) becomes INSERT
//     INTO t (a, b) VALUES (...). The ids are left NULL, as in any INSERT
//     that names its columns. The same goes for MERGE's INSERT VALUES.
//   - An UPDATE, or a MERGE's UPDATE SET, that sets a member column sets
//     its ids to NULL too, so no row keeps the id of a value it no longer
//     holds.
//   - A MERGE's UPDATE BY NAME or BY POSITION, which could change a member
//     column and keep its ids, is refused; naming the columns works.
//
// Ids left NULL are filled later (they are taken as not known meanwhile);
// an id is never wrong.

// memberWrites rewrites query, as described above, when it writes a table
// with member columns. For an INSERT it also returns the table and its
// members, whose ids it then writes (see idassign.go). The caller holds the
// catalog lock exclusively.
func (s *Server) memberWrites(ctx context.Context, sess *session, query string) (string, *idWrite, error) {
	if s.catalogName() == "" {
		return query, nil, nil
	}
	_, names := s.memberTables(ctx)
	if len(names) == 0 {
		return query, nil, nil
	}
	kind, name, after, ok := writeTarget(query)
	if !ok || !names[strings.ToLower(name[len(name)-1])] {
		return query, nil, nil
	}
	cat, schema, table, cols, err := s.policyTable(ctx, sess, name, true)
	if err != nil || cat != "" {
		// Not a table of the catalog; DuckDB reports one that does not
		// exist.
		return query, nil, nil
	}
	snap := s.memberSnapshot(ctx)
	if snap == nil {
		return query, nil, nil
	}
	members := snap.TableMembers(schema, table)
	if len(members) == 0 {
		return query, nil, nil
	}
	var visible []string
	for _, c := range cols {
		if !iddomain.IsHidden(c.name) {
			visible = append(visible, c.name)
		}
	}
	rewritten, err := rewriteMemberWrite(query, kind, after, schema+"."+table, members, visible)
	if err != nil || kind != "INSERT" {
		return rewritten, nil, err
	}
	return rewritten, &idWrite{schema: schema, table: table, members: idMembers(snap, schema, table)}, nil
}

// writeTarget reads the statement kind (INSERT, UPDATE or MERGE) and target
// table of a write, which may follow common table expressions, and where
// the target's name ends.
func writeTarget(query string) (kind string, name []string, after int, ok bool) {
	sc := &scanner{s: query}
	if sc.keywords("WITH") {
		// The bodies of the expressions are in parentheses.
		at := -1
		for _, t := range topWords(query) {
			if t.word == "INSERT" || t.word == "UPDATE" || t.word == "MERGE" {
				at = t.at
				break
			}
		}
		if at < 0 {
			return "", nil, 0, false
		}
		sc.i = at
	}
	switch {
	case sc.keywords("INSERT", "INTO"):
		kind = "INSERT"
	case sc.keywords("UPDATE"):
		kind = "UPDATE"
	case sc.keywords("MERGE", "INTO"):
		kind = "MERGE"
	default:
		return "", nil, 0, false
	}
	if name, ok = sc.name(); !ok {
		return "", nil, 0, false
	}
	return kind, name, sc.i, true
}

// rewriteMemberWrite rewrites a write of kind to table, whose name ends at
// after in query: members are its member columns and visible its other
// columns, in order.
func rewriteMemberWrite(query, kind string, after int, table string, members []iddomain.Member, visible []string) (string, error) {
	w := memberWrite{query: query, members: members}
	for _, c := range visible {
		w.visible = append(w.visible, quoteIdent(c))
	}
	switch kind {
	case "INSERT":
		w.insertColumns(after)
	case "UPDATE":
		w.update(&scanner{s: query, i: after})
	case "MERGE":
		if err := w.merge(after, table); err != nil {
			return "", err
		}
	}
	return w.apply(), nil
}

// memberWrite collects the insertions memberWrites makes into one
// statement.
type memberWrite struct {
	query   string
	members []iddomain.Member
	// visible are the table's columns other than the ids', quoted.
	visible []string
	inserts []insertion
}

type insertion struct {
	at   int
	text string
}

// apply makes the insertions, from the last to the first.
func (w *memberWrite) apply() string {
	out := w.query
	slices.SortStableFunc(w.inserts, func(a, b insertion) int { return b.at - a.at })
	for _, in := range w.inserts {
		out = out[:in.at] + in.text + out[in.at:]
	}
	return out
}

// insertColumns adds the column list to an INSERT whose target ends at i,
// when it names no columns.
func (w *memberWrite) insertColumns(i int) {
	sc := &scanner{s: w.query, i: i}
	sc.skip()
	at := sc.i
	switch {
	case sc.punct('('), sc.keywords("BY", "NAME"), sc.keywords("DEFAULT", "VALUES"), sc.keywords("AS"):
		// Columns matched by name or named, no values, or an alias, which
		// only INSERT ... ON CONFLICT takes.
		return
	case sc.keywords("BY", "POSITION"):
		// Named columns are matched by position anyway.
		w.query = w.query[:at] + strings.TrimLeft(w.query[sc.i:], " \t")
	}
	w.inserts = append(w.inserts, insertion{at: at, text: "(" + strings.Join(w.visible, ", ") + ") "})
}

// update handles UPDATE t [[AS] alias] SET ..., with sc after t.
func (w *memberWrite) update(sc *scanner) {
	if !sc.keywords("SET") {
		if sc.keywords("AS") || sc.word() != "SET" {
			if _, ok := sc.name(); !ok {
				return
			}
		}
		if !sc.keywords("SET") {
			return
		}
	}
	end := len(w.query)
	for _, t := range topWords(w.query[sc.i:]) {
		if t.word == "FROM" || t.word == "WHERE" || t.word == "RETURNING" {
			end = sc.i + t.at
			break
		}
	}
	w.nullIDs(sc.i, end)
}

// nullIDs sets the ids of the member columns the assignments in
// query[start:end] set to NULL too.
func (w *memberWrite) nullIDs(start, end int) {
	set := assignedColumns(w.query[start:end])
	var extra []string
	for _, m := range w.members {
		if slices.ContainsFunc(set, func(c string) bool { return strings.EqualFold(c, m.Column) }) {
			extra = append(extra, quoteIdent(iddomain.HiddenColumn(m.Column))+" = NULL")
		}
	}
	if len(extra) == 0 {
		return
	}
	at := end
	if at == len(w.query) {
		at = len(strings.TrimRight(w.query, "; \t\r\n"))
	}
	// On a line of its own, after assignments that may end in a comment.
	w.inserts = append(w.inserts, insertion{at: at, text: "\n, " + strings.Join(extra, ", ") + " "})
}

// merge handles the actions of MERGE INTO t ..., with i after t: each
// runs from a THEN to the next WHEN.
func (w *memberWrite) merge(i int, table string) error {
	words := topWords(w.query[i:])
	for k, t := range words {
		if t.word != "THEN" {
			continue
		}
		end := len(w.query)
		for _, next := range words[k+1:] {
			if next.word == "WHEN" {
				end = i + next.at
				break
			}
		}
		sc := &scanner{s: w.query, i: i + t.at}
		sc.keywords("THEN")
		switch {
		case sc.keywords("UPDATE", "SET"):
			w.nullIDs(sc.i, end)
		case sc.keywords("UPDATE", "BY"):
			return pgerror.WithCode(fmt.Errorf("MERGE ... UPDATE BY %s cannot write %s, which has columns in ID domains: Igloo keeps their ids "+
				"beside them and must know which columns change; name them, as in UPDATE SET a = s.a, b = s.b", sc.word(), table),
				codes.FeatureNotSupported)
		case sc.keywords("INSERT"):
			at := sc.i
			if sc.keywords("VALUES") {
				w.inserts = append(w.inserts, insertion{at: at, text: " (" + strings.Join(w.visible, ", ") + ")"})
			}
		}
	}
	return nil
}

// wordAt is a word of a statement's code and where it starts.
type wordAt struct {
	word string // upper case
	at   int
}

// topWords returns the words of query's code outside parentheses and
// outside CASE ... END, whose WHEN and THEN are not a MERGE's.
func topWords(query string) []wordAt {
	var out []wordAt
	depth, cases, pos := 0, 0, 0
	for _, seg := range sqllex.Segments(query) {
		start := pos
		pos += len(seg.Text)
		if seg.Kind != sqllex.Code {
			continue
		}
		t := seg.Text
		for i := 0; i < len(t); i++ {
			switch c := t[i]; {
			case c == '(':
				depth++
			case c == ')':
				depth--
			case sqllex.IdentByte(c) && (i == 0 || !sqllex.IdentByte(t[i-1])):
				j := i
				for j < len(t) && sqllex.IdentByte(t[j]) {
					j++
				}
				word := strings.ToUpper(t[i:j])
				if depth == 0 {
					switch {
					case word == "CASE":
						cases++
					case word == "END" && cases > 0:
						cases--
					case cases == 0:
						out = append(out, wordAt{word: word, at: start + i})
					}
				}
				i = j - 1
			}
		}
	}
	return out
}

// assignedColumns returns the columns a SET list assigns: a = ..., t.a =
// ... and (a, b) = (...), the last part of qualified names.
func assignedColumns(list string) []string {
	var out []string
	for _, a := range splitTopLevel(list, ',') {
		eq := topLevelIndex(a, '=')
		if eq < 0 {
			continue
		}
		left := strings.TrimSpace(a[:eq])
		left = strings.TrimSuffix(strings.TrimPrefix(left, "("), ")")
		for _, part := range splitTopLevel(left, ',') {
			sc := &scanner{s: part}
			if n, ok := sc.name(); ok {
				out = append(out, n[len(n)-1])
			}
		}
	}
	return out
}

// prepareWithoutIDs prepares an INSERT that reads member tables as
// policies rewrite it, reading them without their columns of ids, or
// returns nil. DuckDB expands a member table's * with those columns, so
// INSERT INTO t SELECT * FROM t, which names t's other columns (see
// memberWrites), does not prepare as written, nor does an INSERT of a
// member table's * into a table with its other columns.
func (s *Server) prepareWithoutIDs(ctx context.Context, sess *session, user auth.User, query string, oids []uint32) *statement {
	if _, names := s.memberTables(ctx); len(names) == 0 {
		return nil
	}
	if kind, _, _, ok := writeTarget(query); !ok || kind != "INSERT" {
		return nil
	}
	protected, _, _, err := s.policyRewrite(ctx, sess, user, duckdb.STATEMENT_TYPE_INSERT, query, oids, nil)
	if err != nil || protected == query {
		return nil
	}
	st, err := prepare(ctx, sess.conn, protected, oids)
	if err != nil {
		return nil
	}
	return st
}
