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
//   - each dimension is a column or a constant, of the source or a view of
//     it alone (whose rows are the table's), with an index of the source
//     for the member by that column alone (by nothing, for a constant),
//     and the source's conditions are on that column; a dimension maps its
//     values only one to one and leaving out the rest (skip_blank);
//   - no two dimensions share a name and group, and there are no metrics
//     and no extent;
//   - each bitmap's group has one row for each of its members (the index
//     records each bitmap's rows and members): the rows a cohort's members
//     have with a value are then as many as those members.
//
// People are then the members of a value's bitmap and a cohort's, rows as
// many, and totals the members of the union of a dimension's bitmaps.

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
	use := func(table, member string, groups []string) (indexUse, bool) {
		u, ok := p.use(indexRel{parts: strings.Split(table, "."), column: member, groups: groups})
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
		u, ok := use(c.Table, c.Member, groups)
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
	keys := map[string]bool{}
	for _, d := range spec.Dimensions {
		if d.Metric != "" || d.Elements != "" {
			return nil
		}
		key := d.Name + "\x00"
		if d.Group != nil {
			key += "g" + *d.Group
		}
		if keys[key] {
			return nil
		}
		keys[key] = true
		if d.Map != nil {
			if !d.SkipBlank {
				return nil
			}
			seen := map[string]bool{}
			for _, to := range d.Map {
				if to != nil && seen[*to] {
					return nil
				}
				if to != nil {
					seen[*to] = true
				}
			}
		}
		var groups []string
		if d.Column != "" {
			groups = []string{d.Column}
			for _, w := range where {
				if !strings.EqualFold(w, d.Column) {
					return nil
				}
			}
		} else if len(where) > 0 {
			return nil
		}
		u, ok := use(spec.Source.Table, spec.Source.Member, groups)
		if !ok || len(u.index.Groups) != len(groups) || len(groups) == 1 && !strings.EqualFold(u.index.Groups[0], groups[0]) {
			return nil
		}
		plan.dims = append(plan.dims, u)
	}
	// Each group of the bitmaps read: one row for each member. Asked on
	// the session's connection, as preparing holds the catalog lock.
	for i, d := range spec.Dimensions {
		u := plan.dims[i]
		if !s.indexCounted(ctx, u.index) {
			return nil
		}
		conds := indexRows("x", u)
		if d.Column != "" {
			if w := spec.Source.Where.sql("x"); w != "" {
				conds += " AND " + w
			}
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
		conds := []string{indexRows("x", u)}
		if d.Column != "" {
			if w := spec.Source.Where.sql("x"); w != "" {
				conds = append(conds, w)
			}
		}
		if d.SkipBlank {
			conds = append(conds, "("+v+") IS NOT NULL", "trim("+v+") <> ''")
		}
		groups = append(groups, "SELECT "+quoteLiteral(d.Name)+" AS dimension, "+group+" AS dim_group, "+v+" AS value, x.bm FROM "+
			s.indexTable(u.index.Name)+" AS x WHERE "+strings.Join(conds, " AND "))
	}
	b.WriteString(cte("groups") + " AS (" + strings.Join(groups, " UNION ALL ") + "),\n")
	b.WriteString(cte("pairs") + " AS (SELECT g.dimension, g.dim_group, g.value, z.cohort, z.cohort_size, rb_and_cardinality(g.bm, z.bm) AS people FROM " +
		cte("groups") + " AS g CROSS JOIN " + cte("sizes") + " AS z)\n")
	none := "CAST(NULL AS DOUBLE) AS metric_sum, CAST(NULL AS BIGINT) AS metric_count, CAST(NULL AS DOUBLE) AS metric_avg, 'index' AS path"
	b.WriteString("SELECT dimension, dim_group, value, cohort, false AS is_total, people, people AS \"rows\", cohort_size, " + none +
		" FROM " + cte("pairs") + " WHERE people > 0")
	if spec.Totals {
		// A member has one row in each group: a dimension's rows are the
		// sum of its values', its people the members of their union.
		b.WriteString("\nUNION ALL SELECT t.dimension, t.dim_group, CAST(NULL AS VARCHAR) AS value, z.cohort, true AS is_total, " +
			"rb_and_cardinality(t.bm, z.bm) AS people, CAST(n.n AS BIGINT) AS \"rows\", z.cohort_size, " + none +
			" FROM (SELECT dimension, dim_group, rb_or_agg(bm) AS bm FROM " + cte("groups") + " GROUP BY dimension, dim_group) AS t" +
			" CROSS JOIN " + cte("sizes") + " AS z JOIN (SELECT dimension, dim_group, cohort, sum(people) AS n FROM " + cte("pairs") +
			" GROUP BY dimension, dim_group, cohort) AS n ON n.dimension = t.dimension AND n.dim_group IS NOT DISTINCT FROM t.dim_group AND n.cohort = z.cohort" +
			" WHERE n.n > 0")
	}
	return b.String()
}
