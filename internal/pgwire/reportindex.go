package pgwire

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/iddomain"
)

// The report API answers a call from bitmap indexes (see bitmapindex.go)
// when they hold the people it counts, reading their bitmaps instead of
// the tables, as the rewrite of counts of distinct members does
// (bitmaprewrite.go), and under the same conditions: the caller's role
// reads the whole warehouse; no row access policy, clean room or masking
// policy applies to the tables or the member and group columns, through
// views too; the bitmaps are of the tables' current snapshots, which the
// statement keeps (indexPin) and checks again before each run. And:
//
//   - each cohort is a table, with conditions only on the columns of an
//     index of it for its member;
//   - each dimension is a column, a list's elements or a constant, of the
//     source or a view of it alone (whose rows are the table's), with an
//     index of the source for the member by that column and the columns of
//     the source's conditions (by those alone, for a constant; of the
//     list's elements, ON (ELEMENTS(list)), without conditions), and no
//     condition on its metric (metric_where), which bitmaps cannot keep.
//
// A value's rows are the index rows (groups) that have it, under the
// source's conditions: its people within a cohort are the members of the
// union of their bitmaps and the cohort's. A value most often has one
// group, whose bitmap is read as it is; one of a constant, of a column of
// an index by more columns, of a map or of entries that merge has several,
// which are OR-ed. Totals are the members of the union of a dimension's
// groups.
//
// When each group read has one row for each of its members (the index
// records each bitmap's rows and members), and the call asks for nothing
// but people and rows, the bitmaps answer it all: a value's rows are the
// sum, over its groups, of each one's members in the cohort, one each
// (path 'index'). Otherwise (rows a member repeats, metrics, the extent)
// the people come from the bitmaps and the rest from one scan of the
// source, at the snapshot the bitmaps are of, that keeps the rows of each
// cohort's members by its bitmap (rb_contains) instead of joining the
// cohorts, and counts rows and sums by value with no distinct members
// (path 'index+scan'). The planner computes the cohorts' bitmaps on the
// coordinator and gives workers the values (see distributed's computed
// values).

// reportIndexPlan is how a call reads bitmap indexes: an index for each
// cohort and each dimension, and, for a hybrid, the source to scan.
type reportIndexPlan struct {
	cohorts []indexUse
	dims    []indexUse
	pins    []indexPin
	// at is the snapshot of each index's table to read, by index name,
	// which lets the planner compute a cohort's bitmap first.
	at map[string]int64
	// hybrid scans source, at the snapshot the dimensions' bitmaps are of,
	// for the rows and metrics.
	hybrid bool
	source reportIDTable
}

// reportIndexes returns how spec's call reads bitmap indexes, or nil when
// they cannot answer it.
func (s *Server) reportIndexes(ctx context.Context, sess *session, spec *reportSpec) *reportIndexPlan {
	if !s.bitmapIndexesOn || sess == nil || sess.conn == nil || len(spec.Dimensions) == 0 || rank[sess.currentUser().Role] < rank[auth.RoleReader] {
		return nil
	}
	snap := s.memberSnapshot(ctx)
	if snap == nil || len(snap.Indexes) == 0 {
		return nil
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return nil
	}
	r.lenient = true
	p := &indexPlanner{s: s, ctx: ctx, sess: sess, r: r, states: map[string]indexState{}}
	plan := &reportIndexPlan{at: map[string]int64{}}
	domain := ""
	use := func(table, member string, groups []string, elements string) (indexUse, bool) {
		u, ok := p.use(indexRel{parts: strings.Split(table, "."), column: member, groups: groups, elements: elements})
		if !ok || domain != "" && u.domain != domain {
			return indexUse{}, false
		}
		domain = u.domain
		pin := indexPin{schema: u.index.Schema, table: u.index.Table, index: u.index.Name, snapshot: u.snapshot, generation: u.generation}
		found := false
		for _, q := range plan.pins {
			found = found || q == pin
		}
		if !found {
			plan.pins = append(plan.pins, pin)
		}
		if _, seen := plan.at[u.index.Name]; !seen {
			if tbl, err := s.ddl.LoadTable(ctx, iddomain.IndexSchema, u.index.Name); err == nil && tbl.CurrentSnapshot() != nil {
				plan.at[u.index.Name] = tbl.CurrentSnapshot().SnapshotID
			}
		}
		return u, true
	}
	for _, c := range spec.Cohorts {
		if c.SQL != "" {
			return nil
		}
		groups := mapKeysOf(c.Where)
		if c.Version != nil {
			groups = append(groups, c.Version.Column)
		}
		u, ok := use(c.Table, c.Member, groups, "")
		if !ok {
			return nil
		}
		plan.cohorts = append(plan.cohorts, u)
	}
	// The source's rows must be its table's: not a view that may repeat
	// them.
	res, err := r.resolve(tableRef{parts: strings.Split(spec.Source.Table, ".")}, nil)
	if err != nil || len(res.first) != 1 || len(res.all) != 1 {
		return nil
	}
	base := res.first[0]
	if base.view {
		b, joined, ok := p.viewTable(base, indexRel{column: spec.Source.Member})
		if !ok || joined {
			return nil
		}
		base = b
	}
	where := mapKeysOf(spec.Source.Where)
	whole := len(spec.Metrics) == 0 && spec.Extent == ""
	for _, d := range spec.Dimensions {
		if d.MetricWhere != nil {
			return nil
		}
		if d.Metric != "" {
			whole = false
		}
		groups, elements := where, ""
		switch {
		case d.Elements != "":
			// An index of the list's elements, which holds nothing else
			// to keep rows by.
			if d.Path != nil || len(where) > 0 {
				return nil
			}
			groups, elements = []string{d.Elements}, d.Elements
		case d.Column != "":
			groups = append([]string{d.Column}, where...)
		}
		u, ok := use(spec.Source.Table, spec.Source.Member, groups, elements)
		if !ok {
			return nil
		}
		plan.dims = append(plan.dims, u)
	}
	// Each group of the bitmaps read: one row for each member, for the
	// bitmaps to give the rows too. Asked on the session's connection, as
	// preparing holds the catalog lock.
	for _, u := range plan.dims {
		if !whole {
			break
		}
		if !s.indexCounted(ctx, u.index) {
			whole = false
			break
		}
		conds := indexRows("x", u)
		if w := spec.Source.Where.sql("x"); w != "" {
			conds += " AND " + w
		}
		rows, err := queryRows(ctx, sess.conn, "SELECT count(*) FILTER (WHERE row_count IS NULL OR members IS NULL OR row_count <> members) FROM "+
			s.indexTable(u.index.Name)+" AS x WHERE "+conds)
		if err != nil || len(rows) != 1 || len(rows[0]) != 1 {
			return nil
		}
		if n, _ := rows[0][0].(int64); n != 0 {
			whole = false
		}
	}
	if whole {
		return plan
	}
	// The rest from the source, through its ids, at the snapshot the
	// dimensions' bitmaps are of: all are of the source's current one.
	m := snap.MemberOf(base.schema, base.name, spec.Source.Member)
	if m == nil {
		return nil
	}
	plan.hybrid = true
	plan.source = reportIDTable{from: quoteName([]string{base.db, base.schema, base.name}), hidden: quoteIdent(iddomain.HiddenColumn(m.Column)),
		at: plan.dims[0].snapshot}
	return plan
}

// indexRows is the condition on the rows of index table alias that holds
// the bitmaps u reads.
func indexRows(alias string, u indexUse) string {
	return fmt.Sprintf("%s.source_snapshot_id = %d AND %s.generation = %d", alias, u.snapshot, alias, u.generation)
}

// indexFrom is u's index table, read at the snapshot plan holds, as alias.
func (s *Server) indexFrom(plan *reportIndexPlan, u indexUse, alias string) string {
	from := s.indexTable(u.index.Name) + " AS " + alias
	if at, ok := plan.at[u.index.Name]; ok {
		from += " AT (VERSION => " + strconv.FormatInt(at, 10) + ")"
	}
	return from
}

// cohortBitmap is the SELECT of cohort i's bitmap: the union of its index's
// bitmaps under its conditions.
func (s *Server) cohortBitmap(spec *reportSpec, plan *reportIndexPlan, i int) string {
	c, u := spec.Cohorts[i], plan.cohorts[i]
	conds := []string{indexRows("x", u)}
	if w := c.Where.sql("x"); w != "" {
		conds = append(conds, w)
	}
	if v := c.Version; v != nil {
		col := "x." + quoteIdent(v.Column)
		if v.Equals != nil {
			l, _ := sqlLiteral(v.Equals)
			conds = append(conds, "("+col+" = "+l+")")
		} else {
			sub := "SELECT max(v." + quoteIdent(v.Column) + ") FROM " + tableName(v.LatestFrom.Table) + " AS v"
			if w := v.LatestFrom.Where.sql("v"); w != "" {
				sub += " WHERE " + w
			}
			conds = append(conds, "("+col+" = ("+sub+"))")
		}
	}
	return "SELECT rb_or_agg(x.bm) AS bm FROM " + s.indexFrom(plan, u, "x") + " WHERE " + strings.Join(conds, " AND ")
}

// reportIndexSQL returns the statement spec stands for, read from the
// bitmaps plan holds, and for a hybrid from the source too.
func (s *Server) reportIndexSQL(spec *reportSpec, plan *reportIndexPlan) string {
	var b strings.Builder
	b.WriteString("WITH ")
	var labels []string
	for i, c := range spec.Cohorts {
		name := cte("cohort_" + strconv.Itoa(i+1))
		b.WriteString(name + " AS (" + s.cohortBitmap(spec, plan, i) + "),\n")
		labels = append(labels, "SELECT "+quoteLiteral(c.Label)+" AS cohort, bm FROM "+name)
	}
	b.WriteString(cte("cohorts") + " AS (" + strings.Join(labels, " UNION ALL ") + "),\n")
	b.WriteString(cte("sizes") + " AS (SELECT cohort, rb_cardinality(bm) AS cohort_size, bm FROM " + cte("cohorts") + " WHERE rb_cardinality(bm) > 0),\n")
	var groups []string
	for i, d := range spec.Dimensions {
		u := plan.dims[i]
		group := "CAST(NULL AS VARCHAR)"
		if d.Group != nil {
			group = quoteLiteral(*d.Group)
		}
		v := "CAST(" + d.valueOf("x") + " AS VARCHAR)"
		if d.Elements != "" {
			v = "CAST(x." + quoteIdent(d.Elements) + " AS VARCHAR)"
		}
		conds := []string{indexRows("x", u)}
		if w := spec.Source.Where.sql("x"); w != "" {
			conds = append(conds, w)
		}
		if d.SkipBlank {
			conds = append(conds, "("+v+") IS NOT NULL", "trim("+v+") <> ''")
		}
		groups = append(groups, "SELECT "+quoteLiteral(d.Name)+" AS dimension, "+group+" AS dim_group, "+v+" AS value, x.bm FROM "+
			s.indexFrom(plan, u, "x")+" WHERE "+strings.Join(conds, " AND "))
	}
	keys := "dimension, dim_group, value"
	gkeys := "g." + strings.ReplaceAll(keys, ", ", ", g.")
	same := func(a, b string) string {
		return a + ".dimension = " + b + ".dimension AND " + a + ".dim_group IS NOT DISTINCT FROM " + b + ".dim_group AND " + a + ".value IS NOT DISTINCT FROM " + b + ".value"
	}
	b.WriteString(cte("groups") + " AS (" + strings.Join(groups, " UNION ALL ") + "),\n")
	// Each group's members in each cohort.
	b.WriteString(cte("pairs") + " AS (SELECT g.dimension, g.dim_group, g.value, z.cohort, rb_and_cardinality(g.bm, z.bm) AS n FROM " +
		cte("groups") + " AS g CROSS JOIN " + cte("sizes") + " AS z),\n")
	b.WriteString(cte("people_counts") + " AS (SELECT " + keys + ", cohort, count(*) AS k, max(n) AS one, CAST(sum(n) AS BIGINT) AS n FROM " + cte("pairs") +
		" GROUP BY " + keys + ", cohort),\n")
	// The values of several groups, whose bitmaps are OR-ed.
	b.WriteString(cte("unions") + " AS (SELECT " + gkeys + ", rb_or_agg(g.bm) AS bm FROM " + cte("groups") +
		" AS g JOIN (SELECT " + keys + " FROM " + cte("groups") + " GROUP BY " + keys + " HAVING count(*) > 1) AS m ON " + same("g", "m") +
		" GROUP BY " + gkeys + "),\n")
	b.WriteString(cte("people") + " AS (SELECT c.dimension, c.dim_group, c.value, c.cohort, c.n, " +
		"CASE WHEN c.k = 1 THEN c.one ELSE rb_and_cardinality(u.bm, z.bm) END AS people FROM " + cte("people_counts") + " AS c JOIN " + cte("sizes") +
		" AS z ON z.cohort = c.cohort LEFT JOIN " + cte("unions") + " AS u ON " + same("u", "c") + ")")
	if spec.Totals {
		// A dimension's people are the members of the union of its groups.
		b.WriteString(",\n" + cte("total_people") + " AS (SELECT t.dimension, t.dim_group, z.cohort, rb_and_cardinality(t.bm, z.bm) AS people FROM " +
			"(SELECT dimension, dim_group, rb_or_agg(bm) AS bm FROM " + cte("groups") + " GROUP BY dimension, dim_group) AS t CROSS JOIN " + cte("sizes") + " AS z)")
	}
	b.WriteString("\n")
	if plan.hybrid {
		s.writeHybrid(&b, spec, plan, same)
		return b.String()
	}
	none := "CAST(NULL AS DOUBLE) AS metric_sum, CAST(NULL AS BIGINT) AS metric_count, CAST(NULL AS DOUBLE) AS metric_avg, 'index' AS path"
	b.WriteString("SELECT p.dimension, p.dim_group, p.value, p.cohort, false AS is_total, p.people, p.n AS \"rows\", z.cohort_size, " + none +
		" FROM " + cte("people") + " AS p JOIN " + cte("sizes") + " AS z ON z.cohort = p.cohort WHERE p.n > 0")
	if spec.Totals {
		// A member has one row in each group: a dimension's rows are the
		// sum of its values'.
		b.WriteString("\nUNION ALL SELECT t.dimension, t.dim_group, CAST(NULL AS VARCHAR) AS value, t.cohort, true AS is_total, t.people, " +
			"CAST(n.n AS BIGINT) AS \"rows\", z.cohort_size, " + none + " FROM " + cte("total_people") + " AS t JOIN " + cte("sizes") + " AS z ON z.cohort = t.cohort" +
			" JOIN (SELECT dimension, dim_group, cohort, sum(n) AS n FROM " + cte("pairs") + " GROUP BY dimension, dim_group, cohort) AS n" +
			" ON n.dimension = t.dimension AND n.dim_group IS NOT DISTINCT FROM t.dim_group AND n.cohort = t.cohort WHERE n.n > 0")
	}
	return b.String()
}

// writeHybrid ends a hybrid's statement: the source's rows of each
// cohort's members, kept by the cohort's bitmap, expanded into their
// values, counted and summed as reportSpec.sql counts them, with people
// from the bitmaps.
func (s *Server) writeHybrid(b *strings.Builder, spec *reportSpec, plan *reportIndexPlan, same func(a, b string) string) {
	src := plan.source
	member := "CAST(s." + src.hidden + " AS UINTEGER)"
	var tests []string
	for i, c := range spec.Cohorts {
		tests = append(tests, "CASE WHEN rb_contains(("+s.cohortBitmap(spec, plan, i)+"), "+member+") THEN "+quoteLiteral(c.Label)+" END")
	}
	b.WriteString(", " + cte("members") + " AS (SELECT s.*, " + member + " AS __igloo_member, unnest(list_filter([" + strings.Join(tests, ", ") +
		"], lambda c: c IS NOT NULL)) AS __igloo_cohort FROM " + src.from + " AS s AT (VERSION => " + strconv.FormatInt(src.at, 10) + ")")
	if w := spec.Source.Where.sql("s"); w != "" {
		b.WriteString(" WHERE " + w)
	}
	b.WriteString(")\n")
	counts, metricCols, extent := spec.scanCounts(cte("members")+" AS s", "s.__igloo_cohort", "s.__igloo_member", false)
	b.WriteString(counts)
	b.WriteString("SELECT n.dimension, n.dim_group, n.value, n.cohort, n.total = 1 AS is_total, ")
	if spec.Totals {
		b.WriteString("CASE WHEN n.total = 1 THEN t.people ELSE p.people END AS people, ")
	} else {
		b.WriteString("p.people, ")
	}
	b.WriteString("n.\"rows\", z.cohort_size, n.metric_sum, n.metric_count, n.metric_avg")
	for _, c := range metricCols {
		b.WriteString(", " + c)
	}
	if spec.Extent != "" {
		b.WriteString(", x.extent_min, x.extent_max")
	}
	b.WriteString(", 'index+scan' AS path FROM " + cte("counts") + " AS n JOIN " + cte("sizes") + " AS z ON z.cohort = n.cohort LEFT JOIN " + cte("people") +
		" AS p ON " + same("p", "n") + " AND p.cohort = n.cohort AND n.total = 0")
	if spec.Totals {
		b.WriteString(" LEFT JOIN " + cte("total_people") + " AS t ON t.dimension = n.dimension AND t.dim_group IS NOT DISTINCT FROM n.dim_group" +
			" AND t.cohort = n.cohort AND n.total = 1")
	}
	if extent != "" {
		b.WriteString(" CROSS JOIN (" + extent + ") AS x")
	}
	b.WriteString(" WHERE n.everything = 0 AND n.\"rows\" > 0")
}
