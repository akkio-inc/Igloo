package distributed

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
)

// Synthetic reports of the shapes that, on workers, kept parts on the
// coordinator in a real workload's heavy reports: a report's common table
// expressions aggregate a large table's unnested rows per value, rank the
// values within partitions and keep the first of each, and its last SELECT
// sorts them; the client's query reads that as a subquery, filters it and
// takes its first rows by an order of its own, sometimes through another
// subquery that only limits, or a UNION of two such tops. Their cohorts
// and metrics are joined on upper(...) of their columns.

// rankedReport ranks a lineitem report's values per kind, as a report's
// behaviors are ranked, and sorts them in its last SELECT.
const rankedReport = `WITH base AS (
  SELECT l_orderkey AS id, l_shipmode, l_returnflag, l_linestatus, l_suppkey, l_extendedprice,
         min(l_shipdate) OVER () AS period_start, max(l_shipdate) OVER () AS period_end
  FROM lineitem JOIN orders ON o_orderkey = l_orderkey WHERE o_orderstatus <> 'X'),
expanded AS (
  SELECT 'lineitem' AS source, b.period_start, b.period_end, e.kind, e.value, b.id, b.l_extendedprice AS amount
  FROM base AS b, unnest([{'kind': 'overall', 'value': 'all'}, {'kind': 'mode', 'value': b.l_shipmode},
    {'kind': 'flag', 'value': b.l_returnflag || b.l_linestatus}, {'kind': 'supplier', 'value': CAST(b.l_suppkey % 40 AS VARCHAR)}]) AS u(e)),
aggregated AS (
  SELECT source, min(period_start) AS period_start, max(period_end) AS period_end, kind, value,
         count(DISTINCT id) AS members, count(*) AS activity, sum(amount) AS total
  FROM expanded GROUP BY source, kind, value),
orders_expanded AS (
  SELECT 'orders' AS source, o_orderdate AS period_start, o_orderdate AS period_end, e.kind, e.value, o_custkey AS id, o_totalprice AS amount
  FROM orders, unnest([{'kind': 'overall', 'value': 'all'}, {'kind': 'priority', 'value': o_orderpriority},
    {'kind': 'clerk', 'value': o_clerk}]) AS u(e)),
orders_aggregated AS (
  SELECT source, min(period_start) AS period_start, max(period_end) AS period_end, kind, value,
         count(DISTINCT id) AS members, count(*) AS activity, sum(amount) AS total
  FROM orders_expanded GROUP BY source, kind, value),
all_values AS (SELECT * FROM aggregated UNION ALL SELECT * FROM orders_aggregated),
ranked AS (
  SELECT source, period_start, period_end, kind, value, members, activity, total,
         CASE WHEN kind = 'overall' THEN 0 ELSE row_number() OVER (PARTITION BY source, kind ORDER BY members DESC NULLS LAST, activity DESC NULLS LAST, value) END AS rnk
  FROM all_values)
SELECT * FROM ranked WHERE kind = 'overall' OR rnk <= 10 ORDER BY source, kind, rnk, members DESC NULLS LAST`

// signalReport compares an audience's shares of values with everyone's,
// joining its metrics on upper(...) of their columns, as a report's
// signals are compared, and sorts them in its last SELECT.
const signalReport = `WITH cohorts AS (
  SELECT 'AUDIENCE' AS cohort, c_custkey AS id FROM (SELECT DISTINCT c_custkey FROM customer WHERE c_mktsegment = 'BUILDING')
  UNION ALL SELECT 'EVERYONE', c_custkey FROM (SELECT DISTINCT c_custkey FROM customer)),
sizes AS (SELECT cohort, count(DISTINCT id) AS size FROM cohorts GROUP BY cohort),
signals AS (
  SELECT 'ORDERS' AS category, e.dimension, e.name, c.cohort, count(DISTINCT o_custkey) AS people, avg(o_totalprice) AS intensity
  FROM orders JOIN cohorts AS c ON c.id = o_custkey,
    unnest([{'dimension': 'PRIORITY', 'name': o_orderpriority}, {'dimension': 'CLERK', 'name': o_clerk},
      {'dimension': 'STATUS', 'name': o_orderstatus}]) AS u(e)
  GROUP BY e.dimension, e.name, c.cohort),
metrics AS (
  SELECT s.category, s.dimension, s.name, s.cohort, s.people, z.size, s.people / NULLIF(z.size, 0) AS share, s.intensity
  FROM signals AS s JOIN sizes AS z ON upper(z.cohort) = upper(s.cohort)),
audience AS (SELECT * FROM metrics WHERE upper(cohort) = 'AUDIENCE'),
everyone AS (SELECT * FROM metrics WHERE upper(cohort) = 'EVERYONE'),
comparisons AS (
  SELECT a.category, a.dimension, a.name, a.people AS audience_people, e.people AS everyone_people,
         a.share AS audience_share, e.share AS everyone_share, a.share / NULLIF(e.share, 0) AS idx
  FROM audience AS a JOIN everyone AS e ON upper(e.category) = upper(a.category) AND upper(e.dimension) = upper(a.dimension) AND upper(e.name) = upper(a.name)),
candidates AS (
  SELECT *, row_number() OVER (PARTITION BY category, dimension ORDER BY ln(1 + audience_people) * greatest(coalesce(idx, 0), 1) DESC NULLS LAST, name) AS dimension_rank
  FROM comparisons),
ranked AS (
  SELECT *, row_number() OVER (PARTITION BY category ORDER BY audience_share DESC NULLS LAST, dimension, name) AS rank_within_category
  FROM candidates WHERE dimension_rank <= 10),
selected AS (SELECT * FROM ranked WHERE rank_within_category <= 30)
SELECT m.category, m.dimension, m.name, m.cohort, m.people, m.size, m.share, m.intensity, s.rank_within_category,
       s.audience_share, s.everyone_share, s.idx
FROM selected AS s JOIN metrics AS m ON upper(m.category) = upper(s.category) AND upper(m.dimension) = upper(s.dimension) AND upper(m.name) = upper(s.name)
ORDER BY category, rank_within_category, dimension, name, cohort`

// profileReport profiles an audience's traits, with a median, and a row
// of its size, as a report's profile is, sorted in its last SELECT.
const profileReport = `WITH audience AS (SELECT DISTINCT o_custkey AS id FROM orders WHERE o_orderpriority = '1-URGENT'),
total AS (SELECT count(*) AS size FROM audience),
items AS (
  SELECT a.id, 'parts' AS section, e.trait, e.kind, e.value
  FROM orders JOIN audience AS a ON a.id = o_custkey,
    unnest([{'trait': 'status', 'kind': 'category', 'value': o_orderstatus}, {'trait': 'clerk', 'kind': 'category', 'value': o_clerk},
      {'trait': 'price', 'kind': 'numeric', 'value': CAST(o_totalprice AS VARCHAR)}]) AS u(e)),
aggregates AS (
  SELECT section, trait, kind, CASE WHEN kind = 'category' THEN value ELSE 'numeric summary' END AS category,
         count(DISTINCT id) AS members, avg(TRY_CAST(value AS DOUBLE)) AS average, median(TRY_CAST(value AS DOUBLE)) AS middle
  FROM items GROUP BY section, trait, kind, 4),
ranked AS (
  SELECT p.section, p.trait, p.category, p.members, 100.0 * p.members / NULLIF(t.size, 0) AS pct, p.average, p.middle,
         row_number() OVER (PARTITION BY p.section, p.trait ORDER BY p.members DESC NULLS LAST, p.category) AS rnk
  FROM aggregates AS p CROSS JOIN total AS t),
size_row AS (SELECT 'overview' AS section, 'size' AS trait, 'all' AS category, t.size AS members, 100.0 AS pct,
  CAST(NULL AS DOUBLE) AS average, CAST(NULL AS DOUBLE) AS middle, CAST(1 AS BIGINT) AS rnk FROM total AS t),
profile AS (SELECT * FROM size_row UNION ALL SELECT * FROM ranked WHERE rnk <= 10)
SELECT p.*, p.members >= 5 AS significant FROM profile AS p CROSS JOIN total AS t ORDER BY section, trait, rnk, category`

// reportShapes are the client's queries over the reports, grouped by the
// reason a real one kept a part on the coordinator.
var reportShapes = []struct{ group, query string }{
	{"ORDER BY in a relation", `SELECT * FROM (` + rankedReport + `) AS t0 WHERE t0.kind <> 'overall' AND t0.rnk <= 3 AND t0.members > 0
		ORDER BY t0.source, t0.kind, t0.rnk, t0.members DESC NULLS LAST, t0.value LIMIT 10000`},
	{"ORDER BY in a relation", `SELECT t0.section, t0.trait, t0.category, t0.pct FROM (` + profileReport + `) AS t0
		WHERE upper(t0.section) IN ('PARTS', 'OVERVIEW') AND t0.rnk = 1 AND t0.members > 0 ORDER BY t0.section, t0.pct DESC NULLS LAST, t0.trait LIMIT 10000`},
	{"LIMIT without ORDER BY", `SELECT * FROM (SELECT t0.value FROM (` + rankedReport + `) AS t0 WHERE t0.kind <> 'overall' AND t0.rnk = 1
		ORDER BY t0.members DESC NULLS LAST, t0.activity DESC NULLS LAST, t0.value LIMIT 4) AS t2 LIMIT 10000`},
	{"LIMIT without ORDER BY", `SELECT l_orderkey, l_comment FROM lineitem WHERE l_quantity > 10 LIMIT 25`},
	{"two large relations not joined on equal columns", `SELECT t2.* FROM (SELECT t0.*, t0.audience_share * 100 AS reach FROM (` + signalReport + `) AS t0
		WHERE upper(t0.cohort) = 'AUDIENCE' AND t0.audience_share IS NOT NULL ORDER BY t0.audience_share DESC NULLS LAST, t0.people DESC, t0.name LIMIT 12) AS t2 LIMIT 10000`},
	{"UNION in a relation", `SELECT t5.* FROM (
		SELECT * FROM (SELECT t0.* FROM (` + signalReport + `) AS t0 WHERE upper(t0.cohort) = 'AUDIENCE' AND t0.idx >= 1 ORDER BY t0.idx DESC NULLS LAST, t0.name LIMIT 10) AS t3
		UNION
		SELECT * FROM (SELECT t0.* FROM (` + signalReport + `) AS t0 WHERE upper(t0.cohort) = 'AUDIENCE' AND t0.idx < 1 ORDER BY t0.idx ASC, t0.name LIMIT 5) AS t4) AS t5
		ORDER BY t5.idx DESC NULLS LAST, t5.name LIMIT 10000`},
	{"UNION in a relation", `SELECT kind, count(*) FROM (SELECT l_shipmode AS kind, l_orderkey FROM lineitem UNION SELECT o_orderpriority, o_orderkey FROM orders) GROUP BY 1`},
	{"a subquery without aggregation", `SELECT x.l_orderkey, x.l_comment, count(*) OVER () AS n FROM (SELECT l_orderkey, l_comment FROM lineitem WHERE l_quantity > 45) AS x ORDER BY 1, 2`},
	{"LEFT JOIN with a large right side", `SELECT c_custkey, c_name, count(o_orderkey) AS orders, sum(o_totalprice) AS spent FROM customer
		LEFT JOIN orders ON o_custkey = c_custkey GROUP BY 1, 2 ORDER BY 3 DESC, 1 LIMIT 20`},
	{"LEFT JOIN with a large right side", `SELECT o_orderpriority, count(*) AS n, count(l_orderkey) AS shipped FROM orders
		FULL JOIN (SELECT * FROM lineitem WHERE l_shipmode = 'AIR') AS l ON l_orderkey = o_orderkey GROUP BY 1 ORDER BY 1`},
}

// TestReportShapes runs reportShapes, forced on, with the relations they
// compute too large for every worker to read in full, as a real report's
// are, checks their rows against one machine's, and logs a table, by the
// reason their real counterparts kept a part on the coordinator, of how
// many run on workers with the coordinator reading no table itself, only
// what the workers send, and how much they send; and what the coordinator
// still reads, and why.
func TestReportShapes(t *testing.T) {
	h := newHarness(t, 0.02, 4, 3)
	h.co.maxBroadcast = h.tables["customer"].Bytes - 1
	ctx := context.Background()
	type tally struct {
		clean, total int
		received     int64
	}
	groups := map[string]*tally{}
	var stays []string
	for _, s := range reportShapes {
		g := groups[s.group]
		if g == nil {
			g = &tally{}
			groups[s.group] = g
		}
		g.total++
		want, _, err := h.query(ctx, s.query)
		if err != nil {
			t.Fatalf("%s: %v", short(s.query), err)
		}
		got, _, plan, ex, err := h.run(ctx, s.query, ModeOn)
		if IsUnsupported(err) {
			stays = append(stays, fmt.Sprintf("%s: %s: on one machine: %v", s.group, short(s.query), err))
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", short(s.query), err)
		}
		if anyRows(s.query) {
			// Any rows: as many.
			if len(got) != len(want) {
				t.Errorf("%s: %d rows, want %d", short(s.query), len(got), len(want))
			}
		} else if msg := compareRows(want, got, hasOrderBy(s.query)); msg != "" {
			t.Errorf("%s\n%s\n%s", short(s.query), msg, plan.describe())
		}
		g.received += ex.Received
		if read := coordinatorReads(plan); len(read) == 0 {
			g.clean++
		} else {
			stays = append(stays, fmt.Sprintf("%s: %s: the coordinator reads %s; %s", s.group, short(s.query), strings.Join(read, ", "),
				strings.Join(plan.local, "; ")))
		}
	}
	var names []string
	for n := range groups {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString("\n| Reason a part stayed on the coordinator | On workers, the coordinator reading no table | Shapes | Sent to the coordinator |\n|---|---|---|---|\n")
	for _, n := range names {
		fmt.Fprintf(&b, "| %s | %d | %d | %d KB |\n", n, groups[n].clean, groups[n].total, groups[n].received/1000)
	}
	b.WriteString("\nWhat the coordinator still reads itself:\n")
	for _, s := range stays {
		b.WriteString("- " + s + "\n")
	}
	t.Log(b.String())
}

// anyRows says whether a query takes any rows, by LIMIT without ORDER BY
// after its last subquery.
func anyRows(query string) bool {
	q := strings.ToUpper(query)
	tail := q[strings.LastIndex(q, ")")+1:]
	return strings.Contains(tail, "LIMIT") && !strings.Contains(tail, "ORDER BY")
}

// coordinatorReads lists the tables a plan's coordinator part reads itself,
// rather than the partial results of its stages.
func coordinatorReads(p *Plan) []string {
	ctes := map[string]bool{}
	walk(p.final, func(n node) bool {
		for _, name := range cteNames(n) {
			ctes[name] = true
		}
		return true
	})
	var out []string
	walk(p.final, func(n node) bool {
		if str(n["type"]) != "BASE_TABLE" {
			return true
		}
		name := strings.ToLower(str(n["table_name"]))
		if !strings.HasPrefix(name, "__igloo_") && !(str(n["schema_name"]) == "" && ctes[name]) && !slices.Contains(out, name) {
			out = append(out, name)
		}
		return true
	})
	return out
}
