package pgwire

import (
	"context"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/iddomain"
)

// The report API counts through ID domains' ids where it may, as Igloo's
// counts of distinct members do (see idcount.go): when the source and every
// cohort are tables, or views of tables (see viewBase), whose member columns
// belong to one domain, the statement joins and counts the members' ids, 32-
// bit integers that the rb extension counts in bitmaps, rather than their
// values. The result is the same: each value has one id. It does so only
// where nothing could make the two differ:
//
//   - every table's current snapshot has every row's id, and the statement
//     reads the tables at those snapshots, which it keeps as a count of
//     distinct members through ids keeps its own (idPin), so that a
//     prepared statement is written again when one changes;
//   - no row access policy, clean room or masking policy of the member
//     column applies to a table for anyone (countable);
//   - a view is read through its table only for users whose role reads the
//     whole warehouse, who may read the table themselves, and only when no
//     policy applies to the view.
//
// Cohorts given as SQL are counted by value, and so then is everything else.

// reportIDs are the tables a call reads through ids, at their snapshots.
type reportIDs struct {
	source  reportIDTable
	cohorts []reportIDTable // by cohort; all are tables
}

// reportIDTable is a member table read through its column of ids.
type reportIDTable struct {
	from   string // quoted, catalog.schema.table
	hidden string // the quoted column of ids
	at     int64
}

// reportIDs returns how spec's call reads its tables through ids, or nil
// when it reads them by value, and the member tables whose snapshots that
// depends on.
func (s *Server) reportIDs(ctx context.Context, sess *session, spec *reportSpec) (*reportIDs, []idPin) {
	if !s.idDomainsOn || s.ddl == nil || s.catalogName() == "" || sess == nil || sess.conn == nil || len(spec.Dimensions) == 0 {
		return nil, nil
	}
	members := s.memberSnapshot(ctx)
	if members == nil || len(members.Members) == 0 {
		return nil, nil
	}
	for _, c := range spec.Cohorts {
		if c.SQL != "" {
			return nil, nil
		}
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return nil, nil
	}
	r.lenient = true
	p := &indexPlanner{s: s, ctx: ctx, sess: sess, r: r}
	gov := s.gov.snapshot(ctx)
	reader := rank[sess.currentUser().Role] >= rank[auth.RoleReader]
	var pins []idPin
	domain := ""
	complete := true
	// read reads table through ids; rows says the statement counts its
	// rows and sums its columns, not just its members, so a view that may
	// repeat its table's rows is read as it is.
	read := func(table, member string, cols []string, rows bool) (reportIDTable, bool) {
		res, err := r.resolve(tableRef{parts: strings.Split(table, ".")}, nil)
		if err != nil || len(res.first) != 1 || len(res.all) != 1 {
			return reportIDTable{}, false
		}
		o := res.first[0]
		if o.view {
			if !reader || gov.Rules(o.schema, o.name) != nil {
				return reportIDTable{}, false
			}
			base, joined, ok := p.viewTable(o, indexRel{column: member, groups: cols})
			if !ok || rows && joined {
				return reportIDTable{}, false
			}
			o = base
		}
		if !strings.EqualFold(o.db, s.catalogName()) || o.external || isIglooSchema(o.schema) {
			return reportIDTable{}, false
		}
		m := members.MemberOf(o.schema, o.name, member)
		if m == nil || domain != "" && m.Domain != domain || !countable(gov.Rules(o.schema, o.name), m.Column) {
			return reportIDTable{}, false
		}
		domain = m.Domain
		var snapshot int64
		done := false
		for _, pin := range pins {
			if strings.EqualFold(pin.schema, o.schema) && strings.EqualFold(pin.table, o.name) && strings.EqualFold(pin.column, m.Column) {
				snapshot, done = pin.snapshot, true
				complete = complete && pin.complete
			}
		}
		if !done {
			snap, ok, err := s.idSnapshot(ctx, sess, o.schema, o.name, m.Column)
			if err != nil {
				return reportIDTable{}, false
			}
			pins = append(pins, idPin{schema: o.schema, table: o.name, column: m.Column, snapshot: snap, complete: ok})
			snapshot, complete = snap, complete && ok
		}
		return reportIDTable{from: quoteName([]string{o.db, o.schema, o.name}), hidden: quoteIdent(iddomain.HiddenColumn(m.Column)), at: snapshot}, true
	}
	ids := &reportIDs{}
	for _, c := range spec.Cohorts {
		cols := mapKeysOf(c.Where)
		if c.Version != nil {
			cols = append(cols, c.Version.Column)
		}
		t, ok := read(c.Table, c.Member, cols, false)
		if !ok {
			return nil, nil
		}
		ids.cohorts = append(ids.cohorts, t)
	}
	src := spec.Source
	cols := mapKeysOf(src.Where)
	for _, d := range spec.Dimensions {
		for _, c := range []string{d.Column, d.Elements, d.Metric} {
			if c != "" {
				cols = append(cols, c)
			}
		}
	}
	for _, m := range spec.Metrics {
		for _, c := range []string{m.Column, m.DistinctWith} {
			if c != "" {
				cols = append(cols, c)
			}
		}
	}
	if spec.Extent != "" {
		cols = append(cols, spec.Extent)
	}
	t, ok := read(src.Table, src.Member, cols, true)
	if !ok {
		return nil, nil
	}
	ids.source = t
	if !complete {
		// By value until mapping fills the ids in; the pins tell a
		// prepared statement when to look again.
		return nil, pins
	}
	return ids, pins
}

// mapKeysOf returns a where's columns.
func mapKeysOf(w conditions) []string {
	var out []string
	for k := range w {
		out = append(out, k)
	}
	return out
}
