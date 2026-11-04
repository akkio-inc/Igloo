package distributed

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// Random queries, run on one machine and on workers and compared, in the shapes the
// planner rewrites and their combinations: grouping with HAVING, rankings kept to
// their first rows, QUALIFY, ORDER BY ... LIMIT, windows, DISTINCT, set operations,
// inner and outer joins, scalar, IN, ANY, ALL and EXISTS subqueries, over NULLs and
// ties. A ranking's cut before a HAVING that read a subquery returned no rows where
// one machine returned 10, and one before a window over the groups changed its
// values; each rewrite was right alone.
//
// The values are such that every result is exact: v is a multiple of 0.25, whose
// sums a double holds exactly, so that a HAVING over an average does not flip with
// the order rows are added in; and an ordering that picks rows (ROW_NUMBER, LIMIT)
// orders by enough to pick the same ones.

// randomFound are queries the generator, or TestAgreement, found wrong, each run first
// in every run.
var randomFound = []string{
	// HAVING's subquery read a common table expression that the coordinator's
	// statement no longer defined, as TPC-DS q24's reads ssales.
	`WITH c AS (SELECT t.u, t.h, t.v FROM f t WHERE t.h < 7) SELECT u, count(*) AS n, sum(v) AS sv FROM c GROUP BY u
		HAVING sum(v) > (SELECT 4 * avg(v) FROM c WHERE h = 2) ORDER BY sv DESC NULLS LAST, u LIMIT 10`,
	// A subquery in a FULL JOIN, read for fewer columns than it has, kept the mark that
	// renames them after its candidate was given up on, and the statement failed.
	`SELECT count(*), count(DISTINCT r) FROM (SELECT * FROM (SELECT *, row_number() OVER (ORDER BY a0 ASC NULLS LAST, c0 DESC) AS r
		FROM (SELECT t.u AS c0, avg(t.v) AS a0 FROM f t FULL JOIN (SELECT * FROM dim WHERE w < 29) dim ON t.k = dim.k
		WHERE t.id % 2 = 0 AND t.k NOT IN (SELECT k FROM dim WHERE w = 0) GROUP BY t.u HAVING count(*) >= 25 AND avg(t.v) < 6.5) x) y
		WHERE r <= 7) z`,
	// A ranking's cut fed a window over every group, of the aggregation or of a
	// projection between them.
	`SELECT * FROM (SELECT c0, c1, a0, a1, w0, dense_rank() OVER (ORDER BY a0 ASC NULLS FIRST, c0 DESC, c1 DESC) AS r FROM (SELECT t.u AS c0,
		(t.k % 7) AS c1, max(t.d) AS a0, min(t.s) AS a1, sum(count(*)) OVER () AS w0 FROM f t JOIN dim ON t.k = dim.k GROUP BY t.u, (t.k % 7)
		HAVING count(*) >= 3 AND avg(t.v) < 8) x) y WHERE r < 3`,
	`SELECT * FROM (SELECT c0, c1, a0, pw, rank() OVER (ORDER BY a0 DESC NULLS FIRST, c0 ASC, c1 DESC) AS r FROM (SELECT c0, c1, a0,
		rank() OVER (ORDER BY c0, c1) AS pw FROM (SELECT t.u AS c0, t.g AS c1, max(t.v) FILTER (WHERE t.h > 3) AS a0 FROM f t JOIN dim ON t.k = dim.k
		GROUP BY t.u, t.g) p) x) y WHERE r <= 9`,
	// Sections left out of a union of five, three left, which nested otherwise
	// than DuckDB parses them and did not print as themselves.
	`WITH s1 AS (SELECT CAST(NULL AS INTEGER) AS tag, t.h AS c, t.v AS x FROM f t WHERE NOT (t.h = 2 OR t.g > 42) AND t.g < 5),
		items AS (SELECT CAST(4 AS BIGINT) AS sec, (t.k % 7) AS c, t.d AS x FROM f t UNION ALL SELECT CAST(4 AS BIGINT) AS sec, t.id % 5 AS c, t.v AS x FROM f2 t
		UNION ALL SELECT s1.tag AS tag, s1.c AS c, s1.x AS x FROM s1 AS s1 UNION ALL SELECT 3 AS section, (t.k % 7) AS c, t.d AS x FROM f t
		UNION ALL SELECT CAST(NULL AS INTEGER) AS sec, dim.w AS c, t.v AS x FROM f t JOIN dim ON t.k = dim.k),
		agg AS (SELECT sec, c, count(*) AS n, sum(x) AS sx, 0 AS gc FROM items GROUP BY sec, c),
		ranked AS (SELECT sec, c, n, sx, gc, row_number() OVER (PARTITION BY sec ORDER BY sx DESC NULLS LAST, gc, c NULLS LAST) AS r FROM agg)
		SELECT t0.* FROM (SELECT sec, c, n, sx, gc, r FROM ranked) AS t0 WHERE t0.sec + 1 > 2 AND t0.r <= 3`,
	`WITH s2 AS (SELECT 'ash' AS section, dim.w AS c, t.v AS x FROM f t JOIN dim ON t.k = dim.k UNION ALL SELECT 'ash' AS sec, t.g AS c, t.d AS x FROM f2 t),
		items AS (SELECT CAST(NULL AS VARCHAR) AS sec, t.g AS c, t.d AS x FROM f2 t WHERE t.v IS NOT NULL UNION ALL SELECT 'Birch' AS sec, t.g AS c, t.d AS x FROM f2 t
		UNION ALL SELECT s2.section AS section, s2.c AS c, s2.x AS x FROM s2 AS s2 UNION ALL SELECT 'ash' AS tag, t.id % 5 AS c, t.v AS x FROM f2 t
		UNION ALL SELECT 'ash' AS tag, t.h AS c, t.v AS x FROM f t UNION ALL SELECT 'ash' AS sec, t.g AS c, t.d AS x FROM f2 t),
		agg AS (SELECT sec, c, count(*) AS n, sum(x) AS sx, 0 AS gc FROM items GROUP BY sec, c),
		ranked AS (SELECT sec, c, n, sx, gc, rank() OVER (PARTITION BY sec, c ORDER BY n DESC) AS r FROM agg)
		SELECT t0.* FROM (SELECT sec, c, n, sx, gc, r FROM ranked) AS t0 WHERE t0.sec IN ('ash', 'Birch') AND t0.r <= 4`,
	// The one section left of a UNION, not ALL, kept its rows alike.
	`WITH s0 AS (SELECT CAST(NULL AS VARCHAR) AS sec, dim.w AS c, t.v AS x FROM f t JOIN dim ON t.k = dim.k WHERE t.k BETWEEN 55 AND 156),
		s1 AS (SELECT 'ash' AS sec, t.h AS c, t.v AS x FROM f t),
		items AS (SELECT s0.sec AS sec, s0.c AS c, s0.x AS x FROM s0 AS s0 UNION SELECT s1.sec AS sec, s1.c AS c, s1.x AS x FROM s1 AS s1),
		agg AS (SELECT sec, c, count(*) AS n, sum(x) AS sx, 0 AS gc FROM items GROUP BY sec, c),
		ranked AS (SELECT sec, c, n, sx, gc, row_number() OVER (PARTITION BY sec ORDER BY n DESC, gc, c NULLS FIRST) AS r FROM agg)
		SELECT t0.* FROM (SELECT sec, c, n, sx, gc, r FROM ranked ORDER BY sec) AS t0 WHERE t0.sec IS NULL AND t0.r <= 5`,
}

// randomTables are the tables random queries read: f and f2 alike, f2 smaller, and
// dim, whose keys meet some of theirs, a few twice.
var randomTables = map[string]string{
	"f": `(SELECT i AS id, CASE WHEN i % 53 = 0 THEN NULL ELSE (i * 7) % 50 END::INTEGER AS g, (i % 10)::INTEGER AS h,
		CASE WHEN i % 31 = 0 THEN NULL ELSE ((i * 37) % 41) * 0.25 END::DOUBLE AS v, (((i * 13) % 997) / 4)::DECIMAL(10, 2) AS d,
		CASE WHEN i % 17 = 0 THEN NULL ELSE ['ash', 'birch', 'cedar', 'dogwood', 'elm'][1 + (i * 3) % 5] END AS s,
		CASE WHEN i % 23 = 0 THEN NULL ELSE (i * 11) % 200 END::INTEGER AS k,
		CASE WHEN i % 61 = 0 THEN NULL ELSE (i * 7919) % 4000 END::INTEGER AS u FROM range(60000) t(i))`,
	"f2": `(SELECT i AS id, CASE WHEN i % 29 = 0 THEN NULL ELSE (i * 3) % 40 END::INTEGER AS g, (i % 7)::INTEGER AS h,
		CASE WHEN i % 19 = 0 THEN NULL ELSE ((i * 11) % 37) * 0.25 END::DOUBLE AS v, (((i * 7) % 911) / 4)::DECIMAL(10, 2) AS d,
		CASE WHEN i % 13 = 0 THEN NULL ELSE ['ash', 'birch', 'cedar', 'fir'][1 + i % 4] END AS s,
		CASE WHEN i % 11 = 0 THEN NULL ELSE (i * 17) % 180 END::INTEGER AS k,
		CASE WHEN i % 37 = 0 THEN NULL ELSE (i * 7907) % 3000 END::INTEGER AS u FROM range(20000) t(i))`,
	"dim": `(SELECT i AS k, 'n' || (i % 13) AS name, CASE WHEN i % 9 = 0 THEN NULL ELSE i % 30 END::INTEGER AS w FROM range(150) t(i)
		UNION ALL SELECT i AS k, 'm' || (i % 5) AS name, (i % 4)::INTEGER AS w FROM range(140, 160) t(i))`,
}

// gen writes random queries.
type gen struct{ r *rand.Rand }

func (g *gen) pick(options ...string) string { return options[g.r.IntN(len(options))] }
func (g *gen) pick1(options ...int) int      { return options[g.r.IntN(len(options))] }
func (g *gen) chance(p float64) bool         { return g.r.Float64() < p }
func (g *gen) n(lo, hi int) int              { return lo + g.r.IntN(hi-lo+1) }

// pred is a condition on the columns of f or f2, as t names them.
func (g *gen) pred(t string) string {
	c := func(col string) string { return t + "." + col }
	ps := []string{
		fmt.Sprintf("%s < %d", c("g"), g.n(5, 45)),
		fmt.Sprintf("%s = %d", c("h"), g.n(0, 9)),
		fmt.Sprintf("%s > %g", c("v"), float64(g.n(0, 40))/4),
		fmt.Sprintf("%s = '%s'", c("s"), g.pick("ash", "birch", "cedar", "elm")),
		c("s") + " IS NULL",
		c("v") + " IS NOT NULL",
		fmt.Sprintf("%s BETWEEN %d AND %d", c("k"), g.n(0, 80), g.n(80, 199)),
		fmt.Sprintf("(%s = %d OR %s > %d)", c("h"), g.n(0, 9), c("g"), g.n(20, 49)),
		fmt.Sprintf("%s %% %d = %d", c("id"), g.n(2, 9), g.n(0, 1)),
		fmt.Sprintf("%s > %g", c("d"), float64(g.n(0, 240))),
	}
	p := ps[g.r.IntN(len(ps))]
	if g.chance(0.15) {
		p = "NOT " + p
	}
	if g.chance(0.25) {
		p += " AND " + g.pred(t)
	}
	return p
}

// subqueryPred is a condition that reads a subquery, of f's columns as t names them.
func (g *gen) subqueryPred(t string) string {
	switch g.n(0, 8) {
	case 0:
		return fmt.Sprintf("%s.v > (SELECT avg(v) FROM f2 WHERE h = %d)", t, g.n(0, 6))
	case 1:
		return fmt.Sprintf("%s.k IN (SELECT k FROM dim WHERE w > %d)", t, g.n(0, 25))
	case 2:
		return fmt.Sprintf("%s.k NOT IN (SELECT k FROM dim WHERE w = %d)", t, g.n(0, 29))
	case 3:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM dim WHERE dim.k = %s.k AND dim.w > %d)", t, g.n(0, 25))
	case 4:
		return fmt.Sprintf("NOT EXISTS (SELECT 1 FROM f2 WHERE f2.k = %s.k AND f2.h = %d)", t, g.n(0, 6))
	case 5:
		// f2's k has NULLs: NOT IN keeps no rows unless they are left out.
		return fmt.Sprintf("%s.k NOT IN (SELECT k FROM f2 WHERE h = %d%s)", t, g.n(0, 6), g.pick("", " AND k IS NOT NULL"))
	case 6:
		return fmt.Sprintf("%s.v %s (SELECT v FROM f2 WHERE h = %d AND id < %d)", t, g.pick("> ALL", ">= ANY", "= ANY"), g.n(0, 6), g.n(20, 400))
	case 7:
		return fmt.Sprintf("%s.h = (SELECT max(w) FROM dim WHERE dim.k = %s.k)", t, t)
	default:
		return fmt.Sprintf("%s.g > (SELECT min(g) FROM f2 WHERE f2.k = %s.k)", t, t)
	}
}

// from is a FROM clause over f as t, perhaps joined to dim or f2 (as o).
func (g *gen) from() (string, string) {
	switch g.n(0, 6) {
	case 0:
		return "f t JOIN dim ON t.k = dim.k", "t"
	case 1:
		return fmt.Sprintf("f t LEFT JOIN dim ON t.k = dim.k AND dim.w > %d", g.n(0, 20)), "t"
	case 2:
		return "f t JOIN f2 o ON t.k = o.k AND t.h = o.h", "t"
	case 3:
		return fmt.Sprintf("f t FULL JOIN (SELECT * FROM dim WHERE w < %d) dim ON t.k = dim.k", g.n(5, 29)), "t"
	default:
		return "f t", "t"
	}
}

// groupCols are columns to group by: often u, of thousands of groups, which workers
// complete partition by partition, as they do the groups of a large table.
func (g *gen) groupCols(t string) []string {
	all := []string{t + ".g", t + ".h", t + ".s", "(" + t + ".k % 7)"}
	g.r.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	cols := all[:g.n(1, 2)]
	if g.chance(0.6) {
		cols = append([]string{t + ".u"}, cols[:len(cols)-1]...)
	}
	return cols
}

// agg is an aggregate of f's columns as t names them.
func (g *gen) agg(t string) string {
	return g.pick("count(*)", "count("+t+".v)", "sum("+t+".v)", "avg("+t+".v)", "min("+t+".s)", "max("+t+".d)",
		"count(DISTINCT "+t+".h)", "sum(DISTINCT "+t+".g)", "max("+t+".v) FILTER (WHERE "+t+".h > 3)", "sum("+t+".d)",
		"string_agg("+t+".s, ',' ORDER BY "+t+".s)", "bool_or("+t+".v > 8)")
}

// having is a HAVING condition over the aggregates a0, ... of n of them.
func (g *gen) having(t string, n int) string {
	a := fmt.Sprintf("a%d", g.r.IntN(n))
	switch g.n(0, 6) {
	case 0:
		return fmt.Sprintf("count(*) > %d", g.pick1(g.n(1, 30), g.n(1, 400)))
	case 1:
		return fmt.Sprintf("sum(%s.v) > %d", t, g.pick1(g.n(10, 150), g.n(10, 2000)))
	case 2:
		return fmt.Sprintf("avg(%s.v) > (SELECT avg(v) FROM f2 WHERE h = %d)", t, g.n(0, 6))
	case 3:
		return fmt.Sprintf("count(*) > (SELECT count(*) / %d FROM f2)", g.n(50, 2000))
	case 4:
		return fmt.Sprintf("max(%s.d) < %d", t, g.n(20, 250))
	case 5:
		return a + " IS NOT NULL"
	default:
		return fmt.Sprintf("count(*) >= %d AND avg(%s.v) < %g", g.n(1, 200), t, float64(g.n(10, 40))/4)
	}
}

// grouped is an aggregation: its text, its group columns' names, its aggregates' and
// those of its windows over the groups, if any.
func (g *gen) grouped() (string, []string, []string, []string) {
	from, t := g.from()
	groups := g.groupCols(t)
	var items, names, aggs []string
	for i, c := range groups {
		name := fmt.Sprintf("c%d", i)
		items, names = append(items, c+" AS "+name), append(names, name)
	}
	for i := range g.n(1, 3) {
		name := fmt.Sprintf("a%d", i)
		items, aggs = append(items, g.agg(t)+" AS "+name), append(aggs, name)
	}
	var windows []string
	if g.chance(0.2) {
		w := g.pick("sum(count(*)) OVER ()", "count(*) OVER (PARTITION BY "+groups[0]+")", "rank() OVER (ORDER BY "+strings.Join(groups, ", ")+")")
		items, windows = append(items, w+" AS w0"), append(windows, "w0")
	}
	q := "SELECT " + strings.Join(items, ", ") + " FROM " + from
	var where []string
	if g.chance(0.5) {
		where = append(where, g.pred(t))
	}
	if g.chance(0.15) {
		where = append(where, g.subqueryPred(t))
	}
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " GROUP BY " + strings.Join(groups, ", ")
	if g.chance(0.5) {
		q += " HAVING " + g.having(t, len(aggs))
	}
	return q, names, aggs, windows
}

// rankFn is a ranking window over cols, ordered by one of aggs and then every col, so
// that ROW_NUMBER picks the same rows on one machine and on workers.
func (g *gen) rankFn(cols, aggs []string) string {
	fn := g.pick("rank()", "dense_rank()", "row_number()")
	order := aggs[g.r.IntN(len(aggs))] + " " + g.pick("ASC", "DESC") + " " + g.pick("NULLS FIRST", "NULLS LAST")
	for _, c := range cols {
		order += ", " + c + " " + g.pick("ASC", "DESC")
	}
	part := ""
	if len(cols) > 1 && g.chance(0.5) {
		part = "PARTITION BY " + cols[0] + " "
	}
	return fn + " OVER (" + part + "ORDER BY " + order + ")"
}

// orderAll orders by every one of cols, so that LIMIT keeps the same rows.
func (g *gen) orderAll(cols []string) string {
	var out []string
	for _, c := range cols {
		out = append(out, c+" "+g.pick("ASC", "DESC")+" "+g.pick("NULLS FIRST", "NULLS LAST"))
	}
	g.r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return strings.Join(out, ", ")
}

// sections is a report of sections, as a profile report computes them: a union whose
// branches each name their section by a constant, perhaps in common table expressions
// of their own, grouped by section, ranked within each, and filtered on the section
// from outside, which leaves out of the plan the branches the filter rejects (see
// pruneBranches). Read twice, ranked over every section, grouped in more than one
// grouping set or not by section, filtered together with another column, under a
// LIMIT, DISTINCT ON, QUALIFY, SAMPLE, a volatile function, INTERSECT or EXCEPT, they
// stay: each is a place a filter must stop, and a change that lets it pass one of
// them returns other rows than one machine.
func (g *gen) sections() string {
	// The sections' tags, of one type, a few alike, one NULL; and filters on them.
	tags, typ := []string{"'ash'", "'Birch'", "'CEDAR'", "CAST(NULL AS VARCHAR)", "'elm'", "'ash'"}, "VARCHAR"
	filters := []string{
		"upper(%[1]s) IN ('ASH', 'CEDAR')", "%[1]s IN ('ash', 'Birch')", "%[1]s NOT IN ('ash', 'elm')",
		"%[1]s NOT IN ('ash', NULL)", "%[1]s IS NULL", "%[1]s IS NOT NULL", "CAST(%[1]s AS VARCHAR(10)) = 'Birch'",
		"lower(%[1]s) LIKE 'c%%'", "coalesce(%[1]s, 'none') IN ('none', 'elm')", "(%[1]s = 'ash' OR %[1]s IS NULL)",
		"NOT (upper(%[1]s) = 'ASH')", "length(%[1]s) > 3", "%[1]s > 'B'", "upper(%[1]s) = upper('birch')",
	}
	if g.chance(0.3) {
		tags, typ = []string{"1", "2", "3", "CAST(NULL AS INTEGER)", "CAST(4 AS BIGINT)", "2"}, "BIGINT"
		filters = []string{"%[1]s IN (1, 3)", "%[1]s NOT IN (2)", "%[1]s IS NULL", "CAST(%[1]s AS VARCHAR) IN ('1', '4')",
			"%[1]s + 1 > 2", "%[1]s BETWEEN 2 AND 3", "coalesce(%[1]s, 0) = 0"}
	}
	// A branch reads f, f2 or f joined to dim, for an integer column c and a
	// numeric one x, whose types differ between branches.
	sources := []struct{ from, c, x string }{
		{"f t", "t.h", "t.v"}, {"f2 t", "t.g", "t.d"}, {"f t JOIN dim ON t.k = dim.k", "dim.w", "t.v"},
		{"f2 t", "t.id % 5", "t.v"}, {"f t", "(t.k % 7)", "t.d"},
	}
	// branch reads a source for a section; the union's first names the columns.
	branch := func(tag, name string) string {
		src := sources[g.r.IntN(len(sources))]
		where := ""
		if g.chance(0.5) {
			where = " WHERE " + g.pred("t")
		}
		// Or sections of each row by a CASE over a key, as Snowflake's FLATTEN
		// translated makes them: of the section itself, or of a struct's field
		// through stars and an unnest, with an arm of NULL.
		var arms []string
		for i := range g.n(2, 3) {
			arms = append(arms, fmt.Sprintf("WHEN k.i = %d THEN %%s", i))
		}
		armed := func(value func(tag string) string) string {
			var b strings.Builder
			b.WriteString("CASE")
			for _, a := range arms {
				b.WriteString(" " + fmt.Sprintf(a, value(tags[g.r.IntN(len(tags))])))
			}
			b.WriteString(" ELSE " + value("NULL") + " END")
			return b.String()
		}
		switch g.n(0, 5) {
		case 0:
			return fmt.Sprintf("SELECT %s AS %s, %s AS c, %s AS x FROM %s CROSS JOIN range(%d) AS k(i)%s",
				armed(func(tag string) string { return tag }), name, src.c, src.x, src.from, len(arms)+1, where)
		case 1:
			kind := armed(func(tag string) string {
				if tag == "NULL" {
					return g.pick("NULL", "struct_pack(sec := CAST(NULL AS "+typ+"), w := B.c)")
				}
				return "struct_pack(sec := CAST(" + tag + " AS " + typ + "), w := " + g.pick("1", "B.c") + ")"
			})
			inner := fmt.Sprintf("SELECT B.*, %s AS s FROM (SELECT %s AS c, %s AS x FROM %s%s) AS B CROSS JOIN range(%d) AS k(i)",
				kind, src.c, src.x, src.from, where, len(arms)+1)
			if g.chance(0.5) {
				inner = "SELECT B.*, unnest([1, 2]) AS e FROM (" + inner + ") AS B"
			}
			return fmt.Sprintf("SELECT q.s.sec AS %s, q.c AS c, q.x AS x FROM (%s) AS q", name, inner)
		}
		return fmt.Sprintf("SELECT %s AS %s, %s AS c, %s AS x FROM %s%s", tag, name, src.c, src.x, src.from, where)
	}
	op := g.pick("UNION ALL", "UNION ALL", "UNION ALL", "UNION", "INTERSECT", "EXCEPT ALL")
	var ctes, branches []string
	for i := range g.n(2, 5) {
		tag, name := tags[g.r.IntN(len(tags))], "sec"
		if i > 0 {
			name = g.pick("sec", "section", "tag")
		}
		b := branch(tag, name)
		if g.chance(0.2) {
			// A section of its own two branches.
			b += " UNION ALL " + branch(tag, "sec")
		}
		if g.chance(0.5) {
			// Its own common table expression, which the union reads, by name
			// or by a star, which keeps it if it comes first.
			cte := fmt.Sprintf("s%d", i)
			ctes = append(ctes, cte+" AS ("+b+")")
			b = fmt.Sprintf("SELECT %[1]s.%[2]s AS %[2]s, %[1]s.c AS c, %[1]s.x AS x FROM %[1]s AS %[1]s", cte, name)
			if g.chance(0.3) {
				b = fmt.Sprintf("SELECT %[1]s.* FROM %[1]s AS %[1]s", cte)
			}
		}
		branches = append(branches, b)
	}
	ctes = append(ctes, "items AS ("+strings.Join(branches, " "+op+" ")+")")
	// Grouped by section, in one grouping set or more; gc tells a rollup's rows
	// from groups whose c is NULL, so that the ranking's order is total.
	agg := "SELECT sec, c, count(*) AS n, sum(x) AS sx, 0 AS gc FROM items GROUP BY sec, c"
	switch g.n(0, 9) {
	case 0, 1:
		agg = "SELECT sec, c, count(*) AS n, sum(x) AS sx, grouping(c) AS gc FROM items GROUP BY GROUPING SETS ((sec, c), (sec))"
	case 2:
		// A section of groups not of sections.
		agg = "SELECT min(sec) AS sec, c, count(*) AS n, sum(x) AS sx, 0 AS gc FROM items GROUP BY c"
	case 3:
		// Groups of every section, whose section is NULL.
		agg = "SELECT sec, c, count(*) AS n, sum(x) AS sx, grouping(sec) AS gc FROM items GROUP BY GROUPING SETS ((sec, c), (c))"
	}
	ctes = append(ctes, "agg AS ("+agg+")")
	win := g.pick("row_number() OVER (PARTITION BY sec ORDER BY n DESC, gc, c NULLS FIRST)", "rank() OVER (PARTITION BY sec, c ORDER BY n DESC)",
		"row_number() OVER (PARTITION BY sec ORDER BY sx DESC NULLS LAST, gc, c NULLS LAST)", "row_number() OVER (ORDER BY sec NULLS FIRST, gc, c NULLS FIRST)",
		"dense_rank() OVER (PARTITION BY c ORDER BY sec NULLS LAST)")
	ctes = append(ctes, "ranked AS (SELECT sec, c, n, sx, gc, "+win+" AS r FROM agg)")
	q := "WITH " + strings.Join(ctes, ", ") + " "
	// filter takes the section's column, %[1]s, and n's, %[2]s.
	filter := filters[g.r.IntN(len(filters))]
	cut := fmt.Sprintf("r <= %d", g.n(1, 6))
	if g.chance(0.2) {
		filter = "(" + filter + " OR %[1]s IS NULL AND %[2]s > 3)"
	}
	if g.chance(0.25) {
		// The filter on the table expression itself.
		return q + "SELECT * FROM ranked WHERE " + fmt.Sprintf(filter, "sec", "n") + " AND " + cut
	}
	inner := "SELECT sec, c, n, sx, gc, r FROM ranked"
	switch g.n(0, 10) {
	case 0:
		// Every row, so that the result is the same whichever it keeps.
		inner += " ORDER BY sec, c LIMIT 100000"
	case 1, 2, 3:
		inner += " ORDER BY sec"
	case 4:
		// Some rows, of every section: (sec, c, gc) is unique.
		inner += fmt.Sprintf(" ORDER BY sec NULLS FIRST, c NULLS FIRST, gc, n LIMIT %d", g.n(2, 30))
	case 5:
		inner = "SELECT DISTINCT ON (c) sec, c, n, sx, gc, r FROM ranked ORDER BY c NULLS FIRST, sec NULLS FIRST, gc"
	case 6:
		inner += fmt.Sprintf(" QUALIFY row_number() OVER (ORDER BY n DESC, sec NULLS FIRST, c NULLS FIRST, gc) <= %d", g.n(2, 30))
	case 7:
		inner += " USING SAMPLE 100 PERCENT (bernoulli)"
	case 8:
		inner += " WHERE gen_random_uuid() IS NOT NULL"
	}
	other := ""
	if g.chance(0.15) {
		other = ", (SELECT count(*) AS m FROM agg) AS t1"
	}
	return q + "SELECT t0.* FROM (" + inner + ") AS t0" + other + " WHERE " + fmt.Sprintf(filter, "t0.sec", "t0.n") + " AND t0." + cut
}

// query is a random query.
func (g *gen) query() string {
	switch g.n(0, 13) {
	case 0, 1:
		// Groups kept to a ranking's first, by a later SELECT, through a projection
		// that may have a window of its own.
		q, cols, aggs, windows := g.grouped()
		all := slices.Concat(cols, aggs, windows)
		if g.chance(0.25) {
			w := ""
			if g.chance(0.5) {
				w = ", rank() OVER (ORDER BY " + strings.Join(cols, ", ") + ") AS pw"
			}
			q = "SELECT " + strings.Join(all, ", ") + w + " FROM (" + q + ") p"
			if w != "" {
				all = append(all, "pw")
			}
		}
		cond := fmt.Sprintf("r <= %d", g.n(1, 12))
		switch {
		case g.chance(0.2):
			cond = fmt.Sprintf("r < %d", g.n(2, 12))
		case g.chance(0.2):
			cond += fmt.Sprintf(" AND %s IS NOT NULL", cols[0])
		case g.chance(0.15):
			cond = fmt.Sprintf("(r <= %d OR %s IS NULL)", g.n(1, 5), cols[0])
		}
		// The ranking SELECT names its columns, or reads them by a star.
		sel := strings.Join(all, ", ")
		if g.chance(0.25) {
			sel = "*"
		}
		out := "SELECT * FROM (SELECT " + sel + ", " + g.rankFn(cols, aggs) + " AS r FROM (" + q + ") x) y WHERE " + cond
		if g.chance(0.3) {
			return "SELECT count(*), count(DISTINCT r) FROM (" + out + ") z"
		}
		return out
	case 2:
		// Their own QUALIFY.
		q, cols, aggs, _ := g.grouped()
		return q + fmt.Sprintf(" QUALIFY %s <= %d", g.rankFn(cols, aggs), g.n(1, 10))
	case 3:
		// ORDER BY ... LIMIT over groups.
		q, cols, aggs, _ := g.grouped()
		q += " ORDER BY " + g.orderAll(append(slices.Clone(cols), aggs...)) + fmt.Sprintf(" LIMIT %d", g.n(1, 15))
		if g.chance(0.3) {
			q += fmt.Sprintf(" OFFSET %d", g.n(1, 5))
		}
		return q
	case 4:
		// Windows over rows.
		w := g.pick("sum(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
			"lag(v) OVER (PARTITION BY h ORDER BY id)", "rank() OVER (PARTITION BY g ORDER BY v NULLS LAST)",
			"count(*) OVER (PARTITION BY s)", "row_number() OVER (PARTITION BY h ORDER BY d, id)",
			"avg(v) OVER (PARTITION BY g)", "dense_rank() OVER (ORDER BY h, g NULLS FIRST)")
		q := "SELECT id, g, h, " + w + " AS w FROM f WHERE " + g.pred("f")
		if g.chance(0.4) {
			q += fmt.Sprintf(" QUALIFY w %s %d", g.pick("<=", ">", "="), g.n(1, 5))
		}
		if g.chance(0.4) {
			return "SELECT g, count(*), sum(w) FROM (" + q + ") x GROUP BY g"
		}
		return q
	case 5:
		// DISTINCT.
		switch g.n(0, 2) {
		case 0:
			return "SELECT DISTINCT g, s FROM f WHERE " + g.pred("f")
		case 1:
			return "SELECT h, count(DISTINCT s), count(DISTINCT g), sum(DISTINCT k) FROM f WHERE " + g.pred("f") + " GROUP BY h"
		default:
			return "SELECT DISTINCT ON (h) h, g, id FROM f WHERE " + g.pred("f") + " ORDER BY h, g NULLS LAST, id"
		}
	case 6:
		// Set operations.
		op := g.pick("UNION", "UNION ALL", "INTERSECT", "EXCEPT", "INTERSECT ALL", "EXCEPT ALL")
		q := "SELECT g, h FROM f WHERE " + g.pred("f") + " " + op + " SELECT g, h FROM f2 WHERE " + g.pred("f2")
		if g.chance(0.5) {
			return "SELECT g, count(*) AS n FROM (" + q + ") x GROUP BY g"
		}
		return q
	case 7:
		// Joins, outer ones keeping NULLs.
		from, t := g.from()
		cols := "t.id, t.g, dim.name"
		if !strings.Contains(from, "dim") {
			cols = "t.id, t.g, t.v"
		}
		q := "SELECT " + cols + " FROM " + from + " WHERE " + g.pred(t)
		if g.chance(0.5) {
			return "SELECT count(*), count(id), count(DISTINCT g) FROM (" + q + ") x"
		}
		return q
	case 10:
		// Scalar subqueries in the select list, of the groups' columns.
		from, t := g.from()
		q := fmt.Sprintf("SELECT %s.h, count(*) AS n, (SELECT count(*) FROM dim WHERE dim.w = %s.h) AS c, (SELECT max(v) FROM f2 WHERE f2.h = %s.h) AS m FROM %s",
			t, t, t, from)
		if g.chance(0.5) {
			q += " WHERE " + g.pred(t)
		}
		return q + " GROUP BY " + t + ".h"
	case 11:
		// What an outer join leaves without a match.
		from, t := g.from()
		if !strings.Contains(from, "LEFT JOIN") && !strings.Contains(from, "FULL JOIN") {
			from = "f t LEFT JOIN dim ON t.k = dim.k"
		}
		return fmt.Sprintf("SELECT %s.h, count(*), count(dim.k), sum(%s.v) FROM %s WHERE dim.k IS %sNULL GROUP BY %s.h", t, t, from, g.pick("", "NOT "), t)
	case 12:
		// A common table expression read by FROM and by a subquery of HAVING or of
		// the select list, as TPC-DS q24 reads ssales.
		q := fmt.Sprintf("WITH c AS (SELECT t.u, t.h, t.v FROM f t WHERE %s) SELECT u, count(*) AS n, sum(v) AS sv, (SELECT count(*) FROM c) AS total FROM c GROUP BY u",
			g.pred("t"))
		if g.chance(0.7) {
			q += fmt.Sprintf(" HAVING sum(v) > (SELECT %g * avg(v) FROM c WHERE h = %d)", float64(g.n(1, 40))/4, g.n(0, 9))
		}
		if g.chance(0.4) {
			q += fmt.Sprintf(" ORDER BY sv DESC NULLS LAST, u LIMIT %d", g.n(1, 20))
		}
		return q
	case 13:
		return g.sections()
	case 8:
		// Subqueries in WHERE.
		q := "SELECT f.h, count(*), sum(f.v) FROM f WHERE " + g.subqueryPred("f")
		if g.chance(0.5) {
			q += " AND " + g.pred("f")
		}
		return q + " GROUP BY f.h"
	default:
		// A ranking's first rows over a UNION ALL of groups.
		q1, cols, aggs, windows := g.grouped()
		// The same over f2, which has the same columns.
		q2 := strings.Replace(q1, " FROM f t", " FROM f2 t", 1)
		// src tells the branches' groups apart, so that the ranking's order is total.
		names := strings.Join(slices.Concat(cols, aggs, windows), ", ")
		u := "SELECT " + names + ", 1 AS src FROM (" + q1 + ") u1 UNION ALL SELECT " + names + ", 2 AS src FROM (" + q2 + ") u2"
		sel := names + ", src"
		if g.chance(0.25) {
			sel = "*"
		}
		return "SELECT * FROM (SELECT " + sel + ", " + g.rankFn(append(slices.Clone(cols), "src"), aggs) + " AS r FROM (" + u + ") x) y WHERE " +
			fmt.Sprintf("r <= %d", g.n(1, 8))
	}
}

// TestRandomAgreement runs randomFound and random queries (IGLOO_TEST_RANDOM_QUERIES,
// 150 by default) from IGLOO_TEST_RANDOM_SEED on one machine and on workers, with the planner's joins
// and with every join partitioned and partial results a row at a time, and fails on any result that
// differs. A query one machine refuses is not counted.
func TestRandomAgreement(t *testing.T) {
	queries, seed := 150, uint64(1)
	if v := os.Getenv("IGLOO_TEST_RANDOM_QUERIES"); v != "" {
		queries, _ = strconv.Atoi(v)
	}
	if v := os.Getenv("IGLOO_TEST_RANDOM_SEED"); v != "" {
		seed, _ = strconv.ParseUint(v, 10, 64)
	}
	h := newHarness(t, 0.01, 2, 3)
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for n := range randomTables {
			if !yield(n) {
				return
			}
		}
	}) {
		files := 6
		if name == "dim" {
			files = 2
		}
		h.addTable(name, randomTables[name], files)
	}
	g := &gen{r: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
	ctx := context.Background()
	broadcast := h.co.maxBroadcast
	// cut counts runs whose stages kept only some rows early, by QUALIFY or LIMIT,
	// and pruned those that left out branches of unions or arms of CASE a filter rejects.
	ran, invalid, failed, cut, pruned := 0, 0, 0, 0, 0
	for i := range len(randomFound) + queries {
		q, name := "", ""
		if i < len(randomFound) {
			q, name = randomFound[i], fmt.Sprintf("found query %d", i)
		} else {
			q, name = g.query(), fmt.Sprintf("seed %d, query %d", seed, i-len(randomFound))
		}
		want, wantCols, err := h.query(ctx, q)
		if err != nil {
			invalid++
			if testing.Verbose() {
				t.Logf("%s is not valid SQL: %v\n%s", name, err, q)
			}
			continue
		}
		distributed := false
		for _, partitioned := range []bool{false, true} {
			// Partitioned, partial results are written a row at a time wherever
			// they may be (see rowSpec).
			h.co.maxBroadcast, h.co.partialRows = broadcast, ""
			if partitioned {
				h.co.maxBroadcast, h.co.partialRows = 1, partialsByRow
			}
			got, gotCols, plan, ex, err := h.run(ctx, q, ModeOn)
			if IsUnsupported(err) {
				continue
			}
			distributed = true
			if plan != nil && (strings.Contains(plan.describe(), " QUALIFY ") || strings.Contains(plan.describe(), " LIMIT ")) {
				cut++
			}
			if plan != nil && plan.pruned > 0 {
				pruned++
			}
			msg := ""
			switch {
			case err != nil:
				msg = err.Error()
			default:
				msg = compareRows(slices.Clone(want), got, false)
				if msg == "" && !slices.Equal(wantCols, gotCols) {
					msg = fmt.Sprintf("columns %v, want %v", gotCols, wantCols)
				}
			}
			if msg != "" {
				failed++
				detail := ""
				if plan != nil {
					detail = plan.describe()
				}
				if ex != nil {
					detail += ex.exchanges
				}
				t.Errorf("%s, partitioned %v: %s\n%s\n%s", name, partitioned, msg, q, detail)
			}
		}
		if distributed {
			ran++
		}
	}
	h.co.maxBroadcast, h.co.partialRows = broadcast, ""
	total := len(randomFound) + queries
	t.Logf("seed %d: %d queries, %d not valid SQL, %d ran on workers, %d runs cut rows early, %d left out branches or arms, %d results differed",
		seed, total, invalid, ran, cut, pruned, failed)
	if valid := total - invalid; valid > 0 && ran*2 < valid {
		t.Errorf("only %d of %d valid queries ran on workers", ran, valid)
	}
}
