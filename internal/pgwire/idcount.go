package pgwire

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/policy"
)

// Counts through ID domains' ids (package iddomain). A count of the
// distinct values of a member column,
//
//	SELECT s.segment, count(DISTINCT s.person_id) FROM segments AS s ... GROUP BY s.segment
//
// counts the distinct ids beside them instead, as 32-bit integers, which
// DuckDB hashes and compares far faster than text and the rb extension
// counts in bitmaps (see bitmaps.go):
//
//	SELECT s.segment, count(DISTINCT CAST(s."_igloo_id_person_id" AS UINTEGER))
//	FROM segments AS s AT (VERSION => 4242) ... GROUP BY s.segment
//
// Values and ids correspond one to one, so the counts agree as long as
// every row with a value has its id. So the rewrite (rewriteIDCounts)
// counts through ids only at a snapshot of the table where every row has,
// and reads the table at that snapshot; before each run, a statement whose
// table has changed since is prepared again (refreshIDCounts), which pins
// the new snapshot, or counts the values while the new snapshot lacks ids.
//
// It counts through ids only where all of these hold, and otherwise leaves
// the statement as it is:
//   - IGLOO_ID_DOMAINS is on, and the statement is a query.
//   - The count is count(DISTINCT x) of a member column x, qualified with
//     the alias (or name) of a member table in the FROM of the same SELECT,
//     or unqualified with that table alone there; or of a column of a CTE,
//     subquery or view there that the member column reaches unchanged (see
//     idlineage.go). Connections' tables are not member tables.
//   - No row access policy or clean room confinement is on the table, nor a
//     masking policy, direct or through a tag, on the column, whoever the
//     user: ids stand for the values and rows they would hide.
//   - Nothing in the statement could show the column of ids, which the
//     policy rewrite keeps in the table's subquery for the count (see
//     policyrewrite.go): no * or COLUMNS(...) in a SELECT that reads the
//     table, and no reference to a whole row of it by its alias.
//   - No reference to the table travels in time or samples of its own.

// countDistinctCall finds count(DISTINCT in SQL code, before parsing.
var countDistinctCall = regexp.MustCompile(`(?i)\bcount\s*\(\s*distinct\b`)

// idCounts is what a statement counts through ids, at which snapshots.
type idCounts struct {
	pins []idPin
}

// idPin is a member table whose column a statement counts, or would count,
// through ids, and the snapshot it read the table at: complete says that
// snapshot had every row's id, so the statement counted through them.
type idPin struct {
	schema, table, column string
	snapshot              int64
	complete              bool
}

// idRef is a reference to a table in a SELECT's FROM.
type idRef struct {
	node  map[string]any
	alias string // lower case, the table's name when it has none
	// schema and table name a member table of the catalog; key is
	// schema.table in lower case, empty for any other table.
	schema, table, key string
}

// rewriteIDCounts rewrites text to count member columns through ids where
// it may (see above), and returns what it pinned. A view on a counted
// column's lineage is expanded and the statement analyzed again (see
// idlineage.go); a statement that then counts nothing through ids stays
// as written.
func (s *Server) rewriteIDCounts(ctx context.Context, sess *session, text string) (string, *idCounts) {
	current := text
	for range maxViewDepth {
		out, counts, inlined := s.rewriteIDCountsOnce(ctx, sess, current)
		if inlined == "" {
			if out == current && current != text {
				return text, counts
			}
			return out, counts
		}
		current = inlined
	}
	return text, nil
}

// rewriteIDCountsOnce is rewriteIDCounts of text as it is: the statement
// rewritten, or inlined, text with a view on a counted column's lineage
// expanded, to analyze again.
func (s *Server) rewriteIDCountsOnce(ctx context.Context, sess *session, text string) (_ string, _ *idCounts, inlined string) {
	if !s.idDomainsOn || s.ddl == nil || s.catalogName() == "" || sess == nil || sess.conn == nil || sess.idCountsOff {
		return text, nil, ""
	}
	if !countDistinctCall.MatchString(maskCode(text)) {
		return text, nil, ""
	}
	members := s.memberSnapshot(ctx)
	if members == nil || len(members.Members) == 0 {
		return text, nil, ""
	}
	tree, err := parseStatement(ctx, sess.conn, text)
	if err != nil {
		return text, nil, ""
	}
	root, _ := tree.(map[string]any)
	if root == nil || hasNode(root, func(m map[string]any) bool { return strings.Contains(str(m["type"]), "PIVOT") }) {
		return text, nil, ""
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return text, nil, ""
	}
	ctes := map[string]bool{}
	walkMaps(root, func(m map[string]any) {
		if cm, ok := m["cte_map"].(map[string]any); ok {
			for _, e := range asList(cm["map"]) {
				if em, _ := e.(map[string]any); em != nil {
					ctes[strings.ToLower(str(em["key"]))] = true
				}
			}
		}
	})

	// Every SELECT and the tables of its FROM, and the CTEs and subqueries
	// it reads, which a counted column may come through (see
	// idlineage.go).
	type selectRefs struct {
		node   map[string]any
		refs   []*idRef
		others int
	}
	var selects []*selectRefs
	byKey := map[string][]*idRef{}
	walkMaps(root, func(m map[string]any) {
		if str(m["type"]) != "SELECT_NODE" {
			return
		}
		sr := &selectRefs{node: m}
		fromRefs(m["from_table"], func(t map[string]any) {
			if str(t["type"]) == "SUBQUERY" && str(t["alias"]) != "" {
				sr.refs = append(sr.refs, &idRef{node: t, alias: strings.ToLower(str(t["alias"]))})
				return
			}
			if str(t["type"]) != "BASE_TABLE" {
				sr.others++
				return
			}
			ref := &idRef{node: t, alias: strings.ToLower(str(t["alias"]))}
			if ref.alias == "" {
				ref.alias = strings.ToLower(str(t["table_name"]))
			}
			var parts []string
			for _, k := range []string{"catalog_name", "schema_name", "table_name"} {
				if v := str(t[k]); v != "" {
					parts = append(parts, v)
				}
			}
			if len(parts) == 1 && ctes[strings.ToLower(parts[0])] {
				sr.refs = append(sr.refs, ref)
				return
			}
			if res, err := r.resolve(tableRef{parts: parts}, nil); err == nil && len(res.all) == 1 && len(res.first) == 1 {
				o := res.first[0]
				if !o.view && strings.EqualFold(o.db, s.catalogName()) && len(members.TableMembers(o.schema, o.name)) > 0 {
					ref.schema, ref.table, ref.key = o.schema, o.name, strings.ToLower(o.schema+"."+o.name)
					byKey[ref.key] = append(byKey[ref.key], ref)
				}
			}
			sr.refs = append(sr.refs, ref)
		})
		selects = append(selects, sr)
	})

	// Tables whose column of ids the statement could show, or that it
	// reads at a time or sample of its own.
	unsafe := map[string]bool{}
	aliases := map[string][]string{} // member alias -> keys
	for key, refs := range byKey {
		for _, ref := range refs {
			aliases[ref.alias] = append(aliases[ref.alias], key)
			if ref.node["at_clause"] != nil || ref.node["sample"] != nil {
				unsafe[key] = true
			}
		}
	}
	for _, sr := range selects {
		walkOwn(sr.node, func(m map[string]any) {
			switch str(m["class"]) {
			case "STAR":
				rel := strings.ToLower(str(m["relation_name"]))
				for _, ref := range sr.refs {
					if ref.key != "" && (rel == "" || rel == ref.alias) {
						unsafe[ref.key] = true
					}
				}
				for _, key := range aliases[rel] {
					unsafe[key] = true
				}
			case "COLUMN_REF":
				if names := asList(m["column_names"]); len(names) == 1 {
					for _, key := range aliases[strings.ToLower(str(names[0]))] {
						unsafe[key] = true
					}
				}
			}
		})
	}

	gov := s.gov.snapshot(ctx)
	var (
		edits []edit
		pins  []idPin
		used  = map[string]int64{}
	)
	pinned := func(schema, table, column string) (int64, bool) {
		for _, p := range pins {
			if strings.EqualFold(p.schema, schema) && strings.EqualFold(p.table, table) && strings.EqualFold(p.column, column) {
				return p.snapshot, p.complete
			}
		}
		snapshot, complete, err := s.idSnapshot(ctx, sess, schema, table, column)
		if err != nil {
			s.log.Debug("cannot tell whether a member table has every row's ids", "table", schema+"."+table, "err", err)
			return 0, false
		}
		pins = append(pins, idPin{schema: schema, table: table, column: column, snapshot: snapshot, complete: complete})
		return snapshot, complete
	}
	lt := newLineage(s, ctx, sess, text, root, r, members, gov, pinned)
	counting := map[string]map[string]bool{} // SELECT -> relations whose lineage it counts
	for _, sr := range selects {
		// through substitutes a reference to a member column, counted in a
		// count of distinct values or of distinct tuples, reporting false
		// when it may not count that column through ids.
		through := func(arg map[string]any, apply bool) bool {
			names := asList(arg["column_names"])
			var ref *idRef
			switch len(names) {
			case 2:
				for _, rf := range sr.refs {
					if rf.alias == strings.ToLower(str(names[0])) {
						ref = rf
					}
				}
			case 1:
				if len(sr.refs) == 1 && sr.others == 0 {
					ref = sr.refs[0]
				}
			}
			if ref != nil && ref.key == "" {
				return lt.through(sr.node, ref, arg, names, apply, counting)
			}
			if ref == nil || unsafe[ref.key] {
				return false
			}
			member := members.MemberOf(ref.schema, ref.table, str(names[len(names)-1]))
			if member == nil || !countable(gov.Rules(ref.schema, ref.table), member.Column) {
				return false
			}
			snapshot, complete := pinned(ref.schema, ref.table, member.Column)
			if !complete {
				return false
			}
			loc, ok := nodeAt(arg)
			if !ok {
				return false
			}
			sp, ok := scanName(text, loc)
			if !ok || len(sp.parts) != len(names) {
				return false
			}
			if apply {
				// Always qualified, so that the policy rewrite keeps the
				// column in this reference's subquery alone (see keptIDs).
				qualifier := quoteIdent(ref.alias) + "."
				if len(names) == 2 {
					qualifier = quoteIdent(sp.parts[0]) + "."
				}
				edits = append(edits, edit{sp.start, sp.end,
					"CAST(" + qualifier + quoteIdent(iddomain.HiddenColumn(member.Column)) + " AS UINTEGER)"})
				used[ref.key] = snapshot
			}
			return true
		}
		walkOwn(sr.node, func(m map[string]any) {
			if str(m["class"]) != "FUNCTION" || !strings.EqualFold(str(m["function_name"]), "count") || m["distinct"] != true {
				return
			}
			args := asList(m["children"])
			if len(args) != 1 {
				return
			}
			arg, _ := args[0].(map[string]any)
			if str(arg["class"]) == "COLUMN_REF" {
				through(arg, true)
				return
			}
			// A count of distinct tuples, as Snowflake's COUNT(DISTINCT a,
			// b) translates (see paircount.go): its member columns are
			// counted through ids wherever the tuple has them, in its test
			// for NULLs too, so that it stays a tuple of the values the
			// pair-count rewrite moves into its inner select.
			vals, ok := distinctTuple(m)
			if !ok {
				return
			}
			for _, v := range vals {
				vm, _ := v.(map[string]any)
				if str(vm["class"]) != "COLUMN_REF" || !through(vm, false) {
					continue
				}
				walkMaps(arg, func(c map[string]any) {
					if str(c["class"]) == "COLUMN_REF" && sameExpr(c, vm) {
						through(c, true)
					}
				})
			}
		})
	}
	if lt.inlined != "" {
		return text, nil, lt.inlined
	}
	if len(lt.edits) > 0 {
		// Unless the ids could show, when every count along a lineage
		// stays as written.
		if !lt.leaks(counting) {
			edits = append(append(edits, lt.edits...), lt.counts...)
			for key, snapshot := range lt.used {
				used[key] = snapshot
			}
		}
	}
	if len(edits) == 0 {
		if len(pins) == 0 {
			return text, nil, ""
		}
		// Counted values for want of ids: prepared again once they are
		// complete.
		return text, &idCounts{pins: pins}, ""
	}
	// Every reference to a table counted through ids reads the snapshot
	// that has them, after its alias.
	for key, snapshot := range used {
		for _, ref := range byKey[key] {
			if lt.wrapped[nodeKey(ref.node)] {
				// Read at the snapshot inside its subquery.
				continue
			}
			pos, ok := afterTableRef(text, ref.node)
			if !ok {
				return text, nil, ""
			}
			edits = append(edits, edit{pos, pos, " AT (VERSION => " + strconv.FormatInt(snapshot, 10) + ")"})
		}
	}
	out, err := applyEdits(text, edits)
	if err != nil {
		return text, nil, ""
	}
	return out, &idCounts{pins: pins}, ""
}

// countable reports whether rules allow counting column of their table
// through ids: no row access policy or confinement on the table, and no
// masking policy on the column.
func countable(rules *policy.TableRules, column string) bool {
	return rules == nil || rules.Filter == nil && len(rules.Confined) == 0 && !rules.Column(column).Masked()
}

// fromRefs calls fn with each table a FROM reads, through joins.
func fromRefs(from any, fn func(map[string]any)) {
	t, _ := from.(map[string]any)
	if t == nil {
		return
	}
	switch str(t["type"]) {
	case "JOIN":
		fromRefs(t["left"], fn)
		fromRefs(t["right"], fn)
	case "EMPTY", "EMPTY_FROM", "":
	default:
		fn(t)
	}
}

// walkOwn calls fn on each object of a SELECT's parse, but not those of
// the SELECTs inside it, which have FROMs of their own.
func walkOwn(sel map[string]any, fn func(map[string]any)) {
	var walk func(n any)
	walk = func(n any) {
		switch n := n.(type) {
		case []any:
			for _, c := range n {
				walk(c)
			}
		case map[string]any:
			if str(n["type"]) == "SELECT_NODE" {
				return
			}
			fn(n)
			for _, c := range n {
				walk(c)
			}
		}
	}
	fn(sel)
	for _, c := range sel {
		walk(c)
	}
}

// nodeAt is where a parse object is in the text (see location).
func nodeAt(m map[string]any) (int, bool) {
	l := location(m["query_location"])
	return l, l >= 0
}

// afterTableRef returns where an AT clause goes in the text of a FROM's
// table reference: after its name, alias and column aliases.
func afterTableRef(text string, t map[string]any) (int, bool) {
	loc, ok := nodeAt(t)
	if !ok {
		return 0, false
	}
	sp, ok := scanName(text, loc)
	if !ok || !strings.EqualFold(sp.parts[len(sp.parts)-1], str(t["table_name"])) {
		return 0, false
	}
	pos := sp.end
	if str(t["alias"]) == "" {
		return pos, true
	}
	sc := &scanner{s: text, i: pos}
	sc.keywords("AS")
	if n, ok := sc.name(); !ok || len(n) != 1 || !strings.EqualFold(n[0], str(t["alias"])) {
		return 0, false
	}
	pos = sc.i
	sc.skip()
	if sc.i < len(text) && text[sc.i] == '(' {
		end := matchParen(text, sc.i)
		if end < 0 {
			return 0, false
		}
		pos = end + 1
	}
	return pos, true
}

// idSnapshot returns schema.table's current snapshot and whether every row
// of it with a value in column has its id (see idsComplete).
func (s *Server) idSnapshot(ctx context.Context, sess *session, schema, table, column string) (int64, bool, error) {
	tbl, err := s.ddl.LoadTable(ctx, schema, table)
	if err != nil {
		return 0, false, err
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		// An empty table counts nothing, either way.
		return 0, false, nil
	}
	complete, err := s.idsComplete(ctx, sess, tbl, snap.SnapshotID, column)
	return snap.SnapshotID, complete, err
}

// idComplete caches idsComplete's answers, which never change for a
// snapshot.
type idComplete struct {
	mu   sync.Mutex
	seen map[string]bool
}

// idsComplete reports whether every row of tbl's snapshot that has a value
// in column has its id. The manifests' null counts tell, for each data file
// without deletes: the column of ids has no more NULLs than column. Files
// with deletes, whose counts include deleted rows, as the rows of ids
// filled in after a write are, and files without the counts, are looked at
// in DuckDB.
func (s *Server) idsComplete(ctx context.Context, sess *session, tbl *table.Table, snapshot int64, column string) (bool, error) {
	key := tbl.Metadata().TableUUID().String() + "/" + strconv.FormatInt(snapshot, 10) + "/" + strings.ToLower(column)
	s.idComplete.mu.Lock()
	known, ok := s.idComplete.seen[key]
	s.idComplete.mu.Unlock()
	if ok {
		return known, nil
	}
	sc := tbl.Schema()
	col, ok := sc.FindFieldByNameCaseInsensitive(column)
	if !ok {
		return false, fmt.Errorf("no column %s", column)
	}
	hidden, ok := sc.FindFieldByNameCaseInsensitive(iddomain.HiddenColumn(col.Name))
	if !ok {
		return false, nil
	}
	tasks, err := tbl.Scan(table.WithSnapshotID(snapshot)).PlanFiles(ctx)
	if err != nil {
		return false, err
	}
	complete, unsure := true, false
	for _, task := range tasks {
		if len(task.DeleteFiles)+len(task.DeletionVectorFiles)+len(task.EqualityDeleteFiles) > 0 {
			unsure = true
			continue
		}
		nulls := task.File.NullValueCounts()
		n, okc := nulls[col.ID]
		h, okh := nulls[hidden.ID]
		switch {
		case !okc || !okh:
			unsure = true
		case h > n:
			complete = false
		}
		if !complete {
			break
		}
	}
	if complete && unsure {
		ident := tbl.Identifier()
		name := quoteName([]string{s.catalogName(), ident[len(ident)-2], ident[len(ident)-1]})
		rows, err := sess.conn.QueryContext(ctx, "SELECT count(*) FROM (SELECT 1 FROM "+name+" AT (VERSION => "+strconv.FormatInt(snapshot, 10)+
			") WHERE "+quoteIdent(hidden.Name)+" IS NULL AND "+quoteIdent(col.Name)+" IS NOT NULL LIMIT 1)", nil)
		if err != nil {
			return false, err
		}
		n, err := countOf(rows)
		if err != nil {
			return false, err
		}
		complete = n == 0
	}
	s.idComplete.mu.Lock()
	if s.idComplete.seen == nil || len(s.idComplete.seen) > 100_000 {
		s.idComplete.seen = map[string]bool{}
	}
	s.idComplete.seen[key] = complete
	s.idComplete.mu.Unlock()
	return complete, nil
}

// refreshIDCounts prepares st again, before it runs, if a table it counts
// through ids, or would, has changed since it was prepared: the new
// statement pins the table's current snapshot, or counts its values while
// that snapshot lacks ids. Preparing takes the catalog lock it needs; the
// caller holds none.
func (s *Server) refreshIDCounts(ctx context.Context, sess *session, st *statement) error {
	ic := st.idCounts
	if ic == nil || st.text == "" {
		return nil
	}
	stale := false
	for _, p := range ic.pins {
		snapshot, complete, err := s.idSnapshot(ctx, sess, p.schema, p.table, p.column)
		if err != nil || snapshot != p.snapshot || complete != p.complete {
			stale = true
			break
		}
	}
	if !stale {
		return nil
	}
	fresh, err := s.prepareChecked(ctx, sess, sess.currentUser(), st.text, st.clientOIDs)
	if err != nil {
		return err
	}
	if !sameShape(fresh, st, false) {
		_ = fresh.close()
		return errors.New("a statement counting through ids changed its result's columns when prepared again")
	}
	old := st.stmt
	*st = *fresh
	return old.Close()
}
