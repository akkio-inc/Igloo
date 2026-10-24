package pgwire

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
)

// The report API answers a call from bitmap indexes (see bitmapindex.go)
// when they hold everything it counts, reading their bitmaps instead of the
// tables, as the rewrite of counts of distinct members does
// (bitmaprewrite.go), and under the same conditions: the caller's role
// reads the whole warehouse; no row access policy, clean room or masking
// policy applies to the tables or the columns read, through views too; the
// bitmaps are of the tables' current snapshots, which the statement keeps
// (indexPin) and checks again before each run. And:
//
//   - each cohort is a table, with conditions only on the columns of an
//     index of it for its member;
//   - each dimension is a column, a list's elements or a constant, of the
//     source or a view of it alone (whose rows are the table's), with an
//     index of the source for the member by that column and the columns of
//     the source's conditions (by those alone, for a constant; of the
//     list's elements, ON (ELEMENTS(list)), without conditions), and there
//     are no metrics and no extent;
//   - each bitmap's group has one row for each of its members (the index
//     records each bitmap's rows and members).
//
// A value's rows are then the index rows (groups) that have it, under the
// source's conditions: its people within a cohort are the members of the
// union of their bitmaps and the cohort's, and its rows the sum, over the
// groups, of those of each group's members in the cohort, one each. A
// value most often has one group, whose bitmap is read as it is; one of a
// constant, of a column of an index by more columns, of a map or of
// entries that merge has several, which are OR-ed. Totals are the union of
// a dimension's groups, and their rows the sum of its values'.

// reportIndexPlan is how a call reads bitmap indexes: an index for each
// cohort and each dimension.
type reportIndexPlan struct {
	cohorts []indexUse
	dims    []indexUse
	pins    []indexPin
}

// reportIndexes returns how spec's call reads bitmap indexes, or nil when
// they cannot answer it.
func (s *Server) reportIndexes(ctx context.Context, sess *session, spec *reportSpec) *reportIndexPlan {
	if !s.bitmapIndexesOn || sess == nil || sess.conn == nil || len(spec.Dimensions) == 0 || len(spec.Metrics) > 0 || spec.Extent != "" ||
		rank[sess.currentUser().Role] < rank[auth.RoleReader] {
		return nil
	}
	if snap := s.memberSnapshot(ctx); snap == nil || len(snap.Indexes) == 0 {
		return nil
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return nil
	}
	r.lenient = true
	p := &indexPlanner{s: s, ctx: ctx, sess: sess, r: r, states: map[string]indexState{}}
	plan := &reportIndexPlan{}
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
	if res, err := r.resolve(tableRef{parts: strings.Split(spec.Source.Table, ".")}, nil); err != nil || len(res.first) != 1 {
		return nil
	} else if o := res.first[0]; o.view {
		if _, joined, ok := p.viewTable(o, indexRel{column: spec.Source.Member}); !ok || joined {
			return nil
		}
	}
	where := mapKeysOf(spec.Source.Where)
	for _, d := range spec.Dimensions {
		if d.Metric != "" {
			return nil
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
	// Each group of the bitmaps read: one row for each member. Asked on
	// the session's connection, as preparing holds the catalog lock.
	for _, u := range plan.dims {
		if !s.indexCounted(ctx, u.index) {
			return nil
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
			return nil
		}
	}
	return plan
}

// indexRows is the condition on the rows of index table alias that holds
// the bitmaps u reads.
func indexRows(alias string, u indexUse) string {
	return fmt.Sprintf("%s.source_snapshot_id = %d AND %s.generation = %d", alias, u.snapshot, alias, u.generation)
}

// indexSQL returns the statement spec stands for, read from the bitmaps
// plan holds.
func (s *Server) reportIndexSQL(spec *reportSpec, plan *reportIndexPlan) string {
	var b strings.Builder
	b.WriteString("WITH ")
	var labels []string
	for i, c := range spec.Cohorts {
		u := plan.cohorts[i]
		name := cte("cohort_" + strconv.Itoa(i+1))
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
		b.WriteString(name + " AS (SELECT rb_or_agg(x.bm) AS bm FROM " + s.indexTable(u.index.Name) + " AS x WHERE " + strings.Join(conds, " AND ") + "),\n")
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
			s.indexTable(u.index.Name)+" AS x WHERE "+strings.Join(conds, " AND "))
	}
	keys := "dimension, dim_group, value"
	same := func(a, b string) string {
		return a + ".dimension = " + b + ".dimension AND " + a + ".dim_group IS NOT DISTINCT FROM " + b + ".dim_group AND " + a + ".value IS NOT DISTINCT FROM " + b + ".value"
	}
	b.WriteString(cte("groups") + " AS (" + strings.Join(groups, " UNION ALL ") + "),\n")
	// Each group's members in each cohort, one row each.
	b.WriteString(cte("pairs") + " AS (SELECT g.dimension, g.dim_group, g.value, z.cohort, rb_and_cardinality(g.bm, z.bm) AS n FROM " +
		cte("groups") + " AS g CROSS JOIN " + cte("sizes") + " AS z),\n")
	b.WriteString(cte("counts") + " AS (SELECT " + keys + ", cohort, count(*) AS k, max(n) AS one, CAST(sum(n) AS BIGINT) AS n FROM " + cte("pairs") +
		" GROUP BY " + keys + ", cohort),\n")
	// The values of several groups, whose bitmaps are OR-ed.
	b.WriteString(cte("unions") + " AS (SELECT g." + strings.ReplaceAll(keys, ", ", ", g.") + ", rb_or_agg(g.bm) AS bm FROM " + cte("groups") +
		" AS g JOIN (SELECT " + keys + " FROM " + cte("groups") + " GROUP BY " + keys + " HAVING count(*) > 1) AS m ON " + same("g", "m") +
		" GROUP BY g." + strings.ReplaceAll(keys, ", ", ", g.") + ")\n")
	none := "CAST(NULL AS DOUBLE) AS metric_sum, CAST(NULL AS BIGINT) AS metric_count, CAST(NULL AS DOUBLE) AS metric_avg, 'index' AS path"
	b.WriteString("SELECT c.dimension, c.dim_group, c.value, c.cohort, false AS is_total, " +
		"CASE WHEN c.k = 1 THEN c.one ELSE rb_and_cardinality(u.bm, z.bm) END AS people, c.n AS \"rows\", z.cohort_size, " + none +
		" FROM " + cte("counts") + " AS c JOIN " + cte("sizes") + " AS z ON z.cohort = c.cohort LEFT JOIN " + cte("unions") + " AS u ON " + same("u", "c") +
		" WHERE c.n > 0")
	if spec.Totals {
		// A dimension's people are the members of the union of its
		// groups, its rows the sum of its values'.
		b.WriteString("\nUNION ALL SELECT t.dimension, t.dim_group, CAST(NULL AS VARCHAR) AS value, z.cohort, true AS is_total, " +
			"rb_and_cardinality(t.bm, z.bm) AS people, CAST(n.n AS BIGINT) AS \"rows\", z.cohort_size, " + none +
			" FROM (SELECT dimension, dim_group, rb_or_agg(bm) AS bm FROM " + cte("groups") + " GROUP BY dimension, dim_group) AS t" +
			" CROSS JOIN " + cte("sizes") + " AS z JOIN (SELECT dimension, dim_group, cohort, sum(n) AS n FROM " + cte("pairs") +
			" GROUP BY dimension, dim_group, cohort) AS n ON n.dimension = t.dimension AND n.dim_group IS NOT DISTINCT FROM t.dim_group AND n.cohort = z.cohort" +
			" WHERE n.n > 0")
	}
	return b.String()
}
