package pgwire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Answering counts from bitmap indexes. Before a statement is prepared,
// after Igloo's other rewrites and before policies apply, each SELECT whose
// counts of distinct members bitmap indexes answer (see bitmapmatch.go) is
// written to read the indexes' tables instead of its table:
//
//	SELECT s.segment AS segment, count(DISTINCT s.person) AS people
//	FROM segments AS s JOIN audience AS a ON a.person = s.person WHERE s.segment LIKE 'x%' GROUP BY s.segment
//
// becomes, with an index of segments ON (segment) FOR person and one of
// the table audience selects from,
//
//	SELECT s.segment AS segment, rb_and_cardinality(rb_or_agg(s.bm),
//	  (SELECT rb_or_agg(l.bm) FROM igloo_bitmaps.aud AS l WHERE l.source_snapshot_id = 7 AND l.generation = 1 AND (...))) AS people
//	FROM igloo_bitmaps.seg AS s WHERE s.source_snapshot_id = 9 AND s.generation = 1 AND (s.segment LIKE 'x%') GROUP BY s.segment
//
// It reads an index only when:
//
//   - its bitmaps are of its table's current snapshot, for the domain's
//     current generation, and every table the SELECT answers for is the
//     same domain's;
//   - no row access policy or clean room's confinement is on that table,
//     or on the view the statement reads it through, and no masking policy,
//     direct or through a tag, is on the member column or any column the
//     SELECT groups or filters by, whoever the user is;
//   - the user's role reads the whole warehouse (users with the role none,
//     whose reads grants decide, read the tables);
//   - each select item with a count has a name of its own, which the
//     rewrite keeps;
//   - and a view in between is a plain projection of one table's columns
//     under their own names.
//
// The statement then keeps the snapshots it read (indexPin): every other
// reference to those tables reads the same snapshot (AT (VERSION => ...)),
// and before each run Igloo checks that the tables are still at them,
// writing the statement again, or back, if not (refreshIndexes). No user's
// statement may name igloo_bitmaps (indexReferences), so references to it
// are Igloo's own, which the policy rewrite leaves to it.

// indexPin is a table a statement reads as of a snapshot, through index
// x's bitmaps.
type indexPin struct {
	schema, table string
	index         string
	snapshot      int64
	generation    int
}

// indexUse is an index a rewrite reads for one relation of a SELECT.
type indexUse struct {
	index      iddomain.BitmapIndex
	snapshot   int64
	generation int
	domain     string
}

// indexReferences refuses a statement a user wrote that names Igloo's
// bitmap indexes' schema: only Igloo reads them, in the statements it
// answers from them.
func indexReferences(text string) error {
	for _, w := range namesIn(text) {
		if strings.EqualFold(w, iddomain.IndexSchema) {
			return iglooDenied()
		}
	}
	return nil
}

// namesIn returns the names in SQL text: its code's words and its quoted
// identifiers.
func namesIn(text string) []string {
	names := sqllex.CodeWords(text)
	for _, v := range textValues(text) {
		if v.ident {
			names = append(names, v.value)
		}
	}
	return names
}

// rewriteIndexes answers the counts of query that bitmap indexes can, and
// returns the snapshots it read.
//
// candidate says some SELECT has counts of the shapes indexes answer, so
// that a statement that read the tables tries the indexes again before it
// runs again (see refreshIndexes).
func (s *Server) rewriteIndexes(ctx context.Context, sess *session, user auth.User, query string, counts *idCounts) (_ string, _ []indexPin, candidate bool) {
	if !s.bitmapIndexesOn || sess == nil || sess.conn == nil || rank[user.Role] < rank[auth.RoleReader] || !countsDistinct.MatchString(query) {
		return query, nil, false
	}
	snap := s.memberSnapshot(ctx)
	if snap == nil || len(snap.Indexes) == 0 {
		return query, nil, false
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return query, nil, false
	}
	r.lenient = true
	p := &indexPlanner{s: s, ctx: ctx, r: r, states: map[string]indexState{}, counts: counts}
	written := query
	rewritten := int64(0)
	var pins []indexPin
	for range 16 {
		tree, err := parseStatement(ctx, sess.conn, query)
		if err != nil {
			break
		}
		next, used, ok := p.rewriteOne(query, tree)
		if !ok {
			break
		}
		query = next
		rewritten++
		for _, u := range used {
			pin := indexPin{schema: u.index.Schema, table: u.index.Table, index: u.index.Name, snapshot: u.snapshot, generation: u.generation}
			if !slices.Contains(pins, pin) {
				pins = append(pins, pin)
			}
		}
	}
	if len(pins) == 0 {
		return query, nil, p.candidate
	}
	pinned, ok := p.pinReferences(sess, query, pins)
	if !ok {
		// Other references to the tables could not be held to the
		// snapshots the bitmaps are of: the statement as it was.
		return written, nil, true
	}
	s.indexRewrites.Add(rewritten)
	return pinned, pins, true
}

type indexPlanner struct {
	s      *Server
	ctx    context.Context
	r      *resolver
	states map[string]indexState
	// counts are the tables the statement counts through ids, which it
	// reads at their pinned snapshots (see idcount.go).
	counts *idCounts
	// candidate says a SELECT's counts are of a shape indexes answer.
	candidate bool
}

// idPinned returns the snapshot the statement's count through ids pinned
// schema.table at, if it did.
func (p *indexPlanner) idPinned(schema, table string) (int64, bool) {
	if p.counts == nil {
		return 0, false
	}
	for _, pin := range p.counts.pins {
		if pin.complete && strings.EqualFold(pin.schema, schema) && strings.EqualFold(pin.table, table) {
			return pin.snapshot, true
		}
	}
	return 0, false
}

// rewriteOne rewrites the first SELECT of text whose counts indexes answer.
func (p *indexPlanner) rewriteOne(text string, tree any) (string, []indexUse, bool) {
	masked := maskCode(text)
	pieces := pieceText{text: text, bare: maskComments(text)}
	var out string
	var used []indexUse
	found := false
	matchSelects(tree, nil, false, func(m map[string]any, sc *cteScope) {
		if found {
			return
		}
		counts := matchSelectCounts(m, sc)
		if len(counts) == 0 {
			return
		}
		for _, c := range counts {
			if c.miss != "" {
				return
			}
		}
		c := counts[0]
		if c.restrictIn {
			return
		}
		p.candidate = true
		target, ok := p.use(c.target)
		if !ok {
			return
		}
		uses := []indexUse{target}
		var set *indexUse
		if c.restrict != nil {
			u, ok := p.use(*c.restrict)
			if !ok || u.domain != target.domain {
				return
			}
			set = &u
			uses = append(uses, u)
		}
		next, ok := p.s.indexSelect(pieces, masked, m, counts, c, target, set)
		if !ok {
			return
		}
		out, used, found = next, uses, true
	})
	return out, used, found
}

// use finds the index that answers rel, if it may: see the comment at the
// top.
func (p *indexPlanner) use(rel indexRel) (indexUse, bool) {
	s := p.s
	res, err := p.r.resolve(tableRef{parts: rel.parts}, nil)
	if err != nil || len(res.first) != 1 || len(res.all) != 1 {
		return indexUse{}, false
	}
	o := res.first[0]
	gov := s.gov.snapshot(p.ctx)
	if o.view {
		// A plain projection of one table's columns under their own names.
		base, ok := p.viewBase(o, rel)
		if !ok {
			return indexUse{}, false
		}
		if rules := gov.Rules(o.schema, o.name); rules != nil {
			return indexUse{}, false
		}
		o = base
	}
	if !strings.EqualFold(o.db, s.catalogName()) || o.external || isIglooSchema(o.schema) {
		return indexUse{}, false
	}
	if rules := gov.Rules(o.schema, o.name); rules != nil {
		if rules.Filter != nil || len(rules.Confined) > 0 {
			return indexUse{}, false
		}
		for _, col := range append([]string{rel.column}, rel.groups...) {
			if c := rules.Columns[strings.ToLower(col)]; c != nil && c.Masked() {
				return indexUse{}, false
			}
		}
	}
	snap := s.memberSnapshot(p.ctx)
	if snap == nil {
		return indexUse{}, false
	}
	m := snap.MemberOf(o.schema, o.name, rel.column)
	if m == nil {
		return indexUse{}, false
	}
	d := snap.Domain(m.Domain)
	if d == nil {
		return indexUse{}, false
	}
	var best *iddomain.BitmapIndex
	for _, x := range snap.TableIndexes(o.schema, o.name) {
		if !strings.EqualFold(x.Column, m.Column) || !coversGroups(x.Groups, rel.groups) {
			continue
		}
		if best == nil || len(x.Groups) < len(best.Groups) {
			x := x
			best = &x
		}
	}
	if best == nil {
		return indexUse{}, false
	}
	st, seen := p.states[best.Name]
	if !seen {
		st, err = s.indexState(p.ctx, *best)
		if err != nil {
			return indexUse{}, false
		}
		p.states[best.Name] = st
	}
	if st.built == 0 || st.built != st.current || st.generation != d.Generation {
		return indexUse{}, false
	}
	if rel.at {
		// Read at a version: the one the count through ids pinned, which
		// the bitmaps must be of; one the statement chose is its own.
		if pinned, ok := p.idPinned(o.schema, o.name); !ok || pinned != st.built {
			return indexUse{}, false
		}
	}
	return indexUse{index: *best, snapshot: st.built, generation: d.Generation, domain: d.Name}, true
}

// viewBase returns the table a view reads when it is SELECT * or SELECT of
// columns under their own names FROM one table, with nothing else.
func (p *indexPlanner) viewBase(v object, rel indexRel) (object, bool) {
	cols, body, ok := viewParts(v.sql)
	if !ok || len(cols) > 0 {
		// A view that names its columns renames them.
		return object{}, false
	}
	tree, err := p.r.serialize(body)
	if err != nil {
		return object{}, false
	}
	stmt, _ := tree.(map[string]any)
	node, _ := stmt["node"].(map[string]any)
	if node == nil {
		node = stmt
	}
	if str(node["type"]) != "SELECT_NODE" || node["where_clause"] != nil || len(asList(node["group_expressions"])) > 0 ||
		node["having"] != nil || node["qualify"] != nil || node["sample"] != nil || len(asList(node["modifiers"])) > 0 {
		return object{}, false
	}
	if cm, ok := node["cte_map"].(map[string]any); ok && len(asList(cm["map"])) > 0 {
		return object{}, false
	}
	from, _ := node["from_table"].(map[string]any)
	if str(from["type"]) != "BASE_TABLE" || from["at_clause"] != nil || from["sample"] != nil {
		return object{}, false
	}
	star := false
	names := map[string]bool{}
	for _, it := range asList(node["select_list"]) {
		im, _ := it.(map[string]any)
		switch str(im["class"]) {
		case "STAR":
			if len(asList(im["exclude_list"])) > 0 || im["replace_list"] != nil || len(asList(im["rename_list"])) > 0 {
				return object{}, false
			}
			star = true
		case "COLUMN_REF":
			cn := asList(im["column_names"])
			name := strings.ToLower(str(cn[len(cn)-1]))
			if a := str(im["alias"]); a != "" && !strings.EqualFold(a, name) {
				return object{}, false
			}
			names[name] = true
		default:
			return object{}, false
		}
	}
	if !star {
		for _, col := range append([]string{rel.column}, rel.groups...) {
			if !names[strings.ToLower(col)] {
				return object{}, false
			}
		}
	}
	var parts []string
	for _, k := range []string{"catalog_name", "schema_name", "table_name"} {
		if p := str(from[k]); p != "" {
			parts = append(parts, p)
		}
	}
	res, err := p.r.resolve(tableRef{parts: parts}, &v)
	if err != nil || len(res.first) != 1 || len(res.all) != 1 || res.first[0].view {
		return object{}, false
	}
	return res.first[0], true
}

// coversGroups reports whether an index's groups include every one of
// need.
func coversGroups(groups, need []string) bool {
	for _, n := range need {
		if !slices.ContainsFunc(groups, func(g string) bool { return strings.EqualFold(g, n) }) {
			return false
		}
	}
	return true
}

// indexSelect writes SELECT node m of text, whose counts c and its kin
// count, to read target's bitmaps (and set's, when the members must also
// be in another index's). It needs FROM, WHERE and GROUP BY where the
// parse says they are, and each select item that counts to have a name.
func (s *Server) indexSelect(pieces pieceText, masked string, m map[string]any, counts []indexCount, c indexCount, target indexUse, set *indexUse) (string, bool) {
	text := pieces.text
	items := asList(m["select_list"])
	selectAt, itemSpans, ok := selectItemSpans(masked, m, items)
	if !ok {
		return "", false
	}
	fromAt := keywordAfter(masked, itemSpans[len(itemSpans)-1][1], "FROM")
	if fromAt < 0 || maxLocation(items) >= fromAt {
		return "", false
	}
	fromBody := fromAt + len("FROM")
	fromEnd, word := clauseEnd(masked, fromBody)
	end := fromEnd
	whereText := ""
	if word == "WHERE" {
		whereBody := fromEnd + len("WHERE")
		whereEnd, w := clauseEnd(masked, whereBody)
		where, _ := m["where_clause"].(map[string]any)
		if where == nil || !within(where, whereBody, whereEnd) {
			return "", false
		}
		whereText = pieces.trim(whereBody, whereEnd)
		end, word = whereEnd, w
	} else if m["where_clause"] != nil {
		return "", false
	}
	groupText := ""
	if word == "GROUP" {
		byAt := skipSpace(masked, end+len("GROUP"))
		if !strings.EqualFold(nextWord(masked, byAt), "BY") {
			return "", false
		}
		groupBody := byAt + len("BY")
		groupEnd, w := clauseEnd(masked, groupBody)
		for _, g := range asList(m["group_expressions"]) {
			gm, _ := g.(map[string]any)
			if !within(gm, groupBody, groupEnd) {
				return "", false
			}
		}
		switch strings.ToUpper(nextWord(masked, groupBody)) {
		case "ALL", "ROLLUP", "CUBE", "GROUPING":
			return "", false
		}
		groupText = pieces.trim(groupBody, groupEnd)
		end, word = groupEnd, w
	} else if len(asList(m["group_expressions"])) > 0 {
		return "", false
	}
	switch word {
	case "", "ORDER", "LIMIT", "OFFSET", "UNION", "EXCEPT", "INTERSECT":
	default:
		return "", false
	}

	alias := quoteIdent(c.target.alias)
	call := "rb_cardinality(rb_or_agg(" + alias + ".bm))"
	if set != nil {
		setWhere := ""
		if c.restrict.where != nil {
			t, ok := setWhereText(pieces, maskCode(text), c.restrict.node)
			if !ok {
				return "", false
			}
			setWhere = " AND (" + t + ")"
		}
		sa := quoteIdent(c.restrict.innerAlias)
		call = fmt.Sprintf("rb_and_cardinality(rb_or_agg(%s.bm), (SELECT rb_or_agg(%s.bm) FROM %s AS %s WHERE %s.source_snapshot_id = %d AND %s.generation = %d%s))",
			alias, sa, s.indexTable(set.index.Name), sa, sa, set.snapshot, sa, set.generation, setWhere)
	}
	var outItems []string
	for i, it := range items {
		im, _ := it.(map[string]any)
		start, stop := itemSpans[i][0], itemSpans[i][1]
		if !hasNode(im, isCountDistinct) {
			outItems = append(outItems, pieces.trim(start, stop))
			continue
		}
		if str(im["alias"]) == "" || aliasAt(masked, start, stop) < 0 {
			return "", false
		}
		var edits []edit
		ok := true
		walkMaps(im, func(e map[string]any) {
			if !ok || !isCountDistinct(e) {
				return
			}
			loc := location(e["query_location"])
			callEnd := callEndAt(masked, loc)
			if loc < start || callEnd < 0 || callEnd > stop {
				ok = false
				return
			}
			edits = append(edits, edit{loc, callEnd, call})
		})
		if !ok || len(edits) == 0 {
			return "", false
		}
		lo, hi := pieces.bounds(start, stop)
		for k := range edits {
			edits[k].start -= lo
			edits[k].end -= lo
		}
		item, err := applyEdits(text[lo:hi], edits)
		if err != nil {
			return "", false
		}
		outItems = append(outItems, item)
	}
	q := fmt.Sprintf("SELECT %s FROM %s AS %s\nWHERE %s.source_snapshot_id = %d AND %s.generation = %d",
		strings.Join(outItems, ", "), s.indexTable(target.index.Name), alias, alias, target.snapshot, alias, target.generation)
	if whereText != "" {
		q += " AND (" + whereText + "\n)"
	}
	if groupText != "" {
		q += "\nGROUP BY " + groupText + "\n"
	}
	if word != "" {
		q += " "
	}
	out, err := applyEdits(text, []edit{{selectAt, end, q}})
	return out, err == nil
}

// setWhereText returns the text of the WHERE of a set's SELECT node.
func setWhereText(pieces pieceText, masked string, node map[string]any) (string, bool) {
	items := asList(node["select_list"])
	if len(items) == 0 {
		return "", false
	}
	_, spans, ok := selectItemSpans(masked, node, items)
	if !ok {
		return "", false
	}
	fromAt := keywordAfter(masked, spans[len(spans)-1][1], "FROM")
	if fromAt < 0 {
		return "", false
	}
	fromEnd, word := clauseEnd(masked, fromAt+len("FROM"))
	if word != "WHERE" {
		return "", false
	}
	whereBody := fromEnd + len("WHERE")
	whereEnd, _ := clauseEnd(masked, whereBody)
	where, _ := node["where_clause"].(map[string]any)
	if where == nil || !within(where, whereBody, whereEnd) {
		return "", false
	}
	return pieces.trim(whereBody, whereEnd), true
}

// pinReferences has every reference in text to a table pins hold read the
// pinned snapshot, as the bitmaps answering for it are of. ok is false
// when it cannot place one, or a view of the table is read too.
func (p *indexPlanner) pinReferences(sess *session, text string, pins []indexPin) (string, bool) {
	tree, err := parseStatement(p.ctx, sess.conn, text)
	if err != nil {
		return text, false
	}
	var edits []edit
	ok := true
	for _, n := range liveTableRefs(tree) {
		if !ok {
			break
		}
		var parts []string
		for _, k := range []string{"catalog_name", "schema_name", "table_name"} {
			if v := str(n[k]); v != "" {
				parts = append(parts, v)
			}
		}
		if len(parts) == 2 && strings.EqualFold(parts[0], iddomain.IndexSchema) || len(parts) == 3 && strings.EqualFold(parts[1], iddomain.IndexSchema) {
			continue
		}
		res, err := p.r.resolve(tableRef{parts: parts}, nil)
		if err != nil || len(res.first) != 1 {
			continue
		}
		o := res.first[0]
		for _, pin := range pins {
			if o.view {
				// A view that may read a pinned table, which AT cannot
				// hold to the snapshot.
				if slices.ContainsFunc(namesIn(o.sql), func(w string) bool { return strings.EqualFold(w, pin.table) }) {
					ok = false
				}
				continue
			}
			if !strings.EqualFold(o.db, p.s.catalogName()) || !strings.EqualFold(o.schema, pin.schema) || !strings.EqualFold(o.name, pin.table) {
				continue
			}
			if n["at_clause"] != nil {
				// Pinned already, by the count through ids, to the same
				// snapshot, or else to another.
				if pinned, isPinned := p.idPinned(o.schema, o.name); !isPinned || pinned != pin.snapshot {
					ok = false
				}
				break
			}
			at, found := afterTableRef(text, n)
			if !found {
				ok = false
				break
			}
			edits = append(edits, edit{at, at, fmt.Sprintf(" AT (VERSION => %d)", pin.snapshot)})
		}
	}
	if !ok {
		return text, false
	}
	if len(edits) == 0 {
		return text, true
	}
	slices.SortFunc(edits, func(a, b edit) int { return a.start - b.start })
	out, err := applyEdits(text, edits)
	return out, err == nil
}

// refreshIndexes prepares st again, before it runs, if the tables it
// answers counts of from bitmap indexes are no longer at the snapshots it
// read them at, with their bitmaps, or if it reads the tables while
// bitmaps could answer it now. Preparing takes the catalog lock it needs;
// the caller holds none.
func (s *Server) refreshIndexes(ctx context.Context, sess *session, st *statement) error {
	if st.indexed == "" || st.text == "" {
		return nil
	}
	stale := false
	if len(st.indexPins) > 0 {
		for _, pin := range st.indexPins {
			snap := s.memberSnapshot(ctx)
			var x *iddomain.BitmapIndex
			if snap != nil {
				x = snap.Index(pin.index)
			}
			if x == nil {
				stale = true
				break
			}
			state, err := s.indexState(ctx, *x)
			if err != nil || state.current != pin.snapshot || state.built != pin.snapshot || state.generation != pin.generation {
				stale = true
				break
			}
		}
	} else {
		// Back to the bitmaps once they have caught up.
		_, pins, _ := s.rewriteIndexes(ctx, sess, sess.currentUser(), st.indexed, st.idCounts)
		stale = len(pins) > 0
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
		return errors.New("a statement answered from bitmap indexes changed its result's columns when prepared again")
	}
	old := st.stmt
	*st = *fresh
	return old.Close()
}

// liveTableRefs returns the references to tables (not CTEs) in a parse
// that the statement reads: outside the bodies of CTEs nothing refers to.
func liveTableRefs(tree any) []map[string]any {
	type cte struct {
		body any
		uses []*cte
		live bool
	}
	main := &cte{}
	var refs []struct {
		n   map[string]any
		def *cte
	}
	var visit func(n any, sc map[string]*cte, in *cte)
	visit = func(n any, sc map[string]*cte, in *cte) {
		switch n := n.(type) {
		case []any:
			for _, c := range n {
				visit(c, sc, in)
			}
		case map[string]any:
			if cm, ok := n["cte_map"].(map[string]any); ok {
				for _, e := range asList(cm["map"]) {
					em, _ := e.(map[string]any)
					key, _ := em["key"].(string)
					value, _ := em["value"].(map[string]any)
					def := &cte{body: value["query"]}
					inner := map[string]*cte{}
					for k, v := range sc {
						inner[k] = v
					}
					visit(value["query"], inner, def)
					next := map[string]*cte{}
					for k, v := range sc {
						next[k] = v
					}
					next[strings.ToLower(key)] = def
					sc = next
				}
			}
			if str(n["type"]) == "BASE_TABLE" {
				if str(n["schema_name"]) == "" && str(n["catalog_name"]) == "" {
					if d := sc[strings.ToLower(str(n["table_name"]))]; d != nil {
						in.uses = append(in.uses, d)
						return
					}
				}
				refs = append(refs, struct {
					n   map[string]any
					def *cte
				}{n, in})
			}
			for k, v := range n {
				if k != "cte_map" {
					visit(v, sc, in)
				}
			}
		}
	}
	visit(tree, map[string]*cte{}, main)
	var mark func(c *cte)
	mark = func(c *cte) {
		if c.live {
			return
		}
		c.live = true
		for _, u := range c.uses {
			mark(u)
		}
	}
	mark(main)
	var out []map[string]any
	for _, r := range refs {
		if r.def.live {
			out = append(out, r.n)
		}
	}
	return out
}
