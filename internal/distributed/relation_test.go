package distributed

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/snowflake"
)

// planOf plans query distributed.
func (h *harness) planOf(query string) *Plan {
	h.t.Helper()
	plan, err := h.co.Plan(context.Background(), query, harnessEnv{h}, ModeOn)
	if err != nil {
		h.t.Fatal(err)
	}
	return plan
}

// relationExchanges counts the distinct exchanges a plan writes
// relations' rows to, and the stages that scan table.
func relationExchanges(p *Plan, table string) (exchanges, scans int) {
	seen := map[int]bool{}
	for _, s := range p.Stages {
		if s.Out != nil && s.Out.purpose == "relation" && !seen[s.Out.ID] {
			seen[s.Out.ID] = true
			exchanges++
		}
		if in := s.split(); in != nil && in.Table.Name == table {
			scans++
		}
	}
	return exchanges, scans
}

// coordinatorStages counts the stages that send rows to the coordinator.
func coordinatorStages(p *Plan) int {
	n := 0
	for _, s := range p.Stages {
		if s.Out == nil {
			n++
		}
	}
	return n
}

// audienceHarness adds tables shaped like an audience report's: people,
// their media and browsing events, and audiences' members by version.
func audienceHarness(t *testing.T) *harness {
	h := newHarness(t, 0.01, 3, 3)
	h.addTable("people", `(SELECT 'p' || lpad(i::VARCHAR, 6, '0') AS person_id, ['AZ', 'CA', 'NY', 'TX', NULL][1 + i % 5] AS state,
		(i * 37 % 200000)::DOUBLE AS income FROM range(20000) t(i))`, 5)
	h.addTable("media", `(SELECT 'p' || lpad((i * 7919 % 25000)::VARCHAR, 6, '0') AS person_id,
		['drama', 'news', 'sports', 'kids', 'comedy', NULL][1 + i % 6] AS genre, CASE WHEN i % 13 = 0 THEN NULL ELSE i % 3600 END AS seconds,
		DATE '2026-01-01' + (i % 90)::INTEGER AS day FROM range(120000) t(i))`, 6)
	h.addNestedTable("browsing", `(SELECT 'p' || lpad((i * 104729 % 26000)::VARCHAR, 6, '0') AS person_id, DATE '2026-01-01' + (i % 30)::INTEGER AS day,
		['kw' || (i % 40), 'kw' || (i % 7), 'all'] AS keywords FROM range(60000) t(i))`, 4,
		[]Column{{ID: 1}, {ID: 2}, {ID: 3, Nested: &Nesting{Kind: "list", Fields: []Field{{Name: "element", ID: 4}}}}})
	h.addTable("lookup", `(SELECT (i % 5) AS audience_id, 1 + (i % 2) AS ver, 'p' || lpad((i * 13 % 30000)::VARCHAR, 6, '0') AS person_id
		FROM range(60000) t(i))`, 4)
	h.addTable("meta", `(SELECT a AS audience_id, v AS ver FROM range(5) t(a), range(1, 3) s(v))`, 1)
	// Everything but the audiences' versions is too large for every worker
	// to read.
	h.co.maxBroadcast = h.tables["meta"].Bytes
	return h
}

// audienceReport is shaped like the behaviors and signals reports: cohorts
// of people (an audience and everyone), signals from media, browsing and
// attributes, each counted by distinct people per cohort, then compared
// and ranked over the few signal-level rows.
const audienceReport = `WITH audience AS (
	SELECT person_id FROM lookup WHERE audience_id = 3 AND ver = (SELECT max(ver) FROM meta WHERE audience_id = 3)),
population AS (SELECT person_id AS person_id, state, income FROM people),
aud_cohort AS (SELECT DISTINCT person_id AS person_id FROM audience AS audience WHERE NOT person_id IS NULL),
population_cohort AS (SELECT DISTINCT person_id AS person_id FROM population AS population WHERE NOT person_id IS NULL),
memberships AS (SELECT 'AUDIENCE' AS cohort, aud_cohort.person_id AS person_id FROM aud_cohort AS aud_cohort
	UNION ALL SELECT 'POPULATION' AS cohort, population_cohort.person_id AS person_id FROM population_cohort AS population_cohort),
sizes AS (SELECT memberships.cohort AS cohort, count(DISTINCT memberships.person_id) AS size FROM memberships AS memberships GROUP BY memberships.cohort),
media_counts AS (SELECT 'MEDIA' AS category, media.genre AS signal, cohort.cohort AS cohort, count(DISTINCT media.person_id) AS people,
	sum(coalesce(media.seconds, 0)) / nullif(count(DISTINCT media.person_id), 0) AS intensity
	FROM media AS media INNER JOIN memberships AS cohort ON cohort.person_id = media.person_id
	WHERE NOT media.genre IS NULL GROUP BY media.genre, cohort.cohort),
browsing_counts AS (SELECT 'BROWSING' AS category, b.keyword AS signal, cohort.cohort AS cohort, count(DISTINCT b.person_id) AS people,
	count(DISTINCT CASE WHEN (b.person_id) IS NOT NULL AND (b.day) IS NOT NULL THEN (b.person_id, b.day) END) / nullif(count(DISTINCT b.person_id), 0) AS intensity
	FROM (SELECT person_id, day, unnest(keywords) AS keyword FROM browsing) AS b INNER JOIN memberships AS cohort ON cohort.person_id = b.person_id
	GROUP BY b.keyword, cohort.cohort),
attribute_counts AS (SELECT 'ATTRIBUTE' AS category, p.state AS signal, cohort.cohort AS cohort, count(DISTINCT p.person_id) AS people,
	avg(p.income) AS intensity FROM people AS p INNER JOIN memberships AS cohort ON cohort.person_id = p.person_id
	WHERE NOT p.state IS NULL GROUP BY p.state, cohort.cohort),
all_counts AS (SELECT * FROM media_counts UNION ALL SELECT * FROM browsing_counts UNION ALL SELECT * FROM attribute_counts),
metrics AS (SELECT s.category, s.signal, s.cohort, s.people, z.size, s.people / nullif(z.size, 0) AS pen, s.intensity
	FROM all_counts AS s INNER JOIN sizes AS z ON upper(z.cohort) = upper(s.cohort)),
comparisons AS (SELECT a.category, a.signal, a.people AS audience_people, p.people AS population_people, a.pen / nullif(p.pen, 0) AS idx, a.intensity
	FROM metrics AS a INNER JOIN metrics AS p ON upper(p.category) = upper(a.category) AND upper(p.signal) = upper(a.signal)
	WHERE upper(a.cohort) = 'AUDIENCE' AND upper(p.cohort) = 'POPULATION'),
ranked AS (SELECT *, row_number() OVER (PARTITION BY category ORDER BY ln(1 + audience_people) * greatest(coalesce(idx, 0), 1) DESC NULLS LAST, signal) AS r
	FROM comparisons)
SELECT category, signal, audience_people, population_people, round(idx, 6) AS idx, round(intensity, 6) AS intensity, r
FROM ranked WHERE r <= 5 ORDER BY category, r`

// TestAudienceReport runs a report shaped like the behaviors and signals
// reports: the cohorts are computed once, by workers, and written to an
// exchange keyed by person, which the media, browsing, attribute and size
// aggregations read partition by partition, counting distinct people in
// place; their signal-level rows are joined, compared and ranked on
// workers too, partitioned by upper(...) of the columns they are joined
// on, and only the ranks the statement keeps reach the coordinator.
func TestAudienceReport(t *testing.T) {
	h := audienceHarness(t)
	if !h.check(audienceReport) {
		t.Fatal("did not run distributed")
	}
	p := h.planOf(audienceReport)
	// memberships, written once, through both branches of its UNION ALL,
	// the audience and the population its branches read, the browsing
	// subquery, the sizes and three kinds of signals, the distinct people
	// of each counted in place, in partitions keyed by person, and the
	// metrics, comparisons and ranks over them, joined on upper(...) of
	// their columns and partitioned so.
	if n, scans := relationExchanges(p, "people"); n != 9 || scans != 2 {
		t.Errorf("%d relation exchanges and %d scans of people, want 9 and 2:\n%s", n, scans, p.describe())
	}
	// The sizes' and signals' groups, of the cohorts' and the tables'
	// columns, which may be many, are completed by a last stage each.
	if n := p.exchanges("aggregate"); n != 4 {
		t.Errorf("%d stages wrote partial results to an exchange, want the 4 groups':\n%s", n, p.describe())
	}
	// Only the ranks' rows the statement keeps reach the coordinator.
	if n := coordinatorStages(p); n != 1 {
		t.Errorf("%d stages send rows to the coordinator, want 1:\n%s", n, p.describe())
	}
	if !slices.ContainsFunc(p.Stages, func(s *Stage) bool { return s.Out != nil && slices.Contains(s.Out.Casts, "upper(%s)") }) {
		t.Errorf("no exchange partitioned by upper(...):\n%s", p.describe())
	}
	// memberships: the exchange both branches write.
	writers := map[int]int{}
	for _, s := range p.Stages {
		if s.Out != nil {
			writers[s.Out.ID]++
		}
	}
	readers := 0
	for _, s := range p.Stages {
		for _, in := range s.Inputs {
			if in.Exchange != nil && writers[in.Exchange.ID] == 2 {
				readers++
			}
		}
	}
	if readers != 4 {
		t.Errorf("%d stages read memberships, want 4:\n%s", readers, p.describe())
	}
}

// TestCohortExample runs the example of docs/distributed-execution.md.
func TestCohortExample(t *testing.T) {
	h := audienceHarness(t)
	q := `WITH cohorts AS (
		SELECT 'audience' AS cohort, person_id FROM (SELECT DISTINCT person_id FROM lookup WHERE audience_id = 2)
		UNION ALL
		SELECT 'everyone', person_id FROM (SELECT DISTINCT person_id FROM people)),
	genres AS (
		SELECT m.genre, c.cohort, count(DISTINCT m.person_id) AS people
		FROM media m JOIN cohorts c ON c.person_id = m.person_id GROUP BY 1, 2),
	sizes AS (SELECT cohort, count(DISTINCT person_id) AS size FROM cohorts GROUP BY 1)
	SELECT g.genre, g.cohort, g.people / s.size AS reach FROM genres g JOIN sizes s USING (cohort) ORDER BY 1, 2`
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
	p := h.planOf(q)
	// The cohorts' DISTINCTs complete their groups by person; the genres'
	// and sizes' distinct people are counted in place, and their groups
	// completed by group.
	if n := coordinatorStages(p); n != 2 || p.exchanges("aggregate") != 4 {
		t.Errorf("%d stages to the coordinator and %d aggregate exchanges, want 2 and 4:\n%s", n, p.exchanges("aggregate"), p.describe())
	}
}

// TestRelations covers the shapes multi-stage plans add: a table joined to
// itself, joins of large intermediate results, UNION ALL of large branches
// feeding one aggregation, and a common table expression read twice or by
// the coordinator too.
func TestRelations(t *testing.T) {
	h := newHarness(t, 0.02, 5, 3)
	h.co.maxBroadcast = h.tables["customer"].Bytes
	for _, tc := range []struct {
		query string
		// scans is how many stages read lineitem's files, and stages how
		// many the plan has, if not 0.
		scans, stages int
	}{
		// A large table joined to itself: both sides partitioned.
		{`SELECT count(*), sum(a.l_quantity - b.l_quantity) FROM lineitem a JOIN lineitem b ON a.l_orderkey = b.l_orderkey AND a.l_linenumber < b.l_linenumber`, 2, 0},
		// Joins of large intermediate results, on the key one groups by.
		{`WITH x AS (SELECT l_orderkey, sum(l_quantity) AS q, count(*) AS n FROM lineitem GROUP BY 1),
			y AS (SELECT o_orderkey, o_custkey, o_orderpriority FROM orders WHERE o_totalprice > 1000)
			SELECT o_orderpriority, count(*), sum(q), max(n) FROM x JOIN y ON x.l_orderkey = y.o_orderkey GROUP BY 1 ORDER BY 1`, 1, 0},
		{`SELECT o_orderpriority, count(*), sum(q) FROM (SELECT l_orderkey AS k, sum(l_quantity) AS q FROM lineitem GROUP BY 1) AS x
			JOIN orders ON o_orderkey = x.k WHERE o_orderstatus = 'F' GROUP BY 1 ORDER BY 1`, 1, 0},
		// As scripts/e2e-distributed.sh runs it.
		{`WITH s AS (SELECT l_orderkey AS k, sum(l_quantity) AS q, count(DISTINCT l_suppkey) AS sups FROM lineitem GROUP BY 1),
			u AS (SELECT o_orderkey AS k FROM orders WHERE o_orderpriority = '1-URGENT'
				UNION ALL SELECT o_orderkey FROM orders WHERE o_orderpriority = '2-HIGH')
			SELECT count(*), sum(q), sum(sups) FROM s JOIN u ON s.k = u.k`, 1, 0},
		// UNION ALL of large branches feeding one aggregation.
		{`SELECT k % 10 AS b, count(*), count(DISTINCT k) FROM (SELECT l_partkey AS k FROM lineitem UNION ALL SELECT ps_partkey FROM partsupp) GROUP BY 1 ORDER BY 1`, 1, 0},
		{`WITH u AS (SELECT l_suppkey AS s, l_quantity AS q FROM lineitem WHERE l_shipmode = 'AIR'
			UNION ALL SELECT ps_suppkey, ps_availqty FROM partsupp WHERE ps_supplycost > 500)
			SELECT s % 7 AS k, count(DISTINCT s), sum(q), count(*) FROM u GROUP BY 1 ORDER BY 1`, 1, 0},
		// Aggregates that do not compute in parts, over a relation keyed by
		// the groups, whose partitions hold them whole.
		{`WITH items AS (SELECT 'lines' AS section, l_shipmode AS name, l_quantity::DOUBLE AS v FROM lineitem
				UNION ALL SELECT 'supplies', CAST(ps_suppkey % 7 AS VARCHAR), ps_supplycost::DOUBLE FROM partsupp)
			SELECT section, name, median(v) AS m, quantile_cont(v, 0.9) AS q, count(*) AS n, avg(v) AS a,
				string_agg(DISTINCT CAST(v::INTEGER % 3 AS VARCHAR), ',' ORDER BY CAST(v::INTEGER % 3 AS VARCHAR)) AS s
			FROM items GROUP BY section, name ORDER BY 1, 2`, 1, 0},
		{`WITH items AS (SELECT l_shipmode AS name, l_extendedprice AS v FROM lineitem WHERE l_quantity > 10),
				stats AS (SELECT name, median(v) AS m, count(DISTINCT v) AS d FROM items GROUP BY 1)
			SELECT name, m, d, rank() OVER (ORDER BY m DESC) AS r FROM stats ORDER BY name`, 1, 0},
		// Aggregates over all of a relation's rows, as the reports' period
		// start and end: the relation's rows joined to the aggregates' row.
		{`WITH urgent AS (SELECT o_orderkey FROM orders WHERE o_orderpriority = '1-URGENT'),
			base AS (SELECT l.l_orderkey, l.l_shipmode, l.l_quantity, min(l.l_shipdate) OVER () AS first_ship, max(l.l_shipdate) OVER () AS last_ship,
				count(*) OVER () AS n, sum(l.l_quantity) FILTER (WHERE l.l_returnflag = 'R') OVER () AS returned,
				l.l_quantity / max(l.l_quantity) OVER () AS share
				FROM lineitem AS l INNER JOIN urgent AS u ON u.o_orderkey = l.l_orderkey)
			SELECT l_shipmode, first_ship, last_ship, n, returned, count(DISTINCT l_orderkey) AS orders, sum(l_quantity), sum(share)
			FROM base GROUP BY ALL ORDER BY 1`, 1, 0},
		// Common table expressions of a set operation, read by its branches.
		{`WITH x AS (SELECT l_returnflag AS f, l_orderkey AS k, sum(l_quantity) AS q FROM lineitem GROUP BY 1, 2)
			SELECT 'q' AS m, f, sum(q) FROM x GROUP BY 1, 2 UNION ALL SELECT 'n', f, count(*) FROM x GROUP BY 1, 2 ORDER BY 1, 2`, 1, 0},
		// x IN (SELECT y ...) too large for every worker: a semi join to a
		// relation keyed by y.
		{`SELECT o_orderpriority, count(*), sum(o_totalprice) FROM orders WHERE o_orderstatus <> 'X' AND o_orderkey IN
			(SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 250) GROUP BY 1 ORDER BY 1`, 1, 0},
		{`SELECT count(*), count(DISTINCT l_suppkey) FROM lineitem WHERE l_orderkey IN (SELECT o_orderkey FROM orders WHERE o_orderpriority = '1-URGENT')
			AND l_partkey IN (SELECT p_partkey FROM part WHERE p_size < 10)`, 1, 0},
		// A filter's subquery that reads the table the part divides: a
		// relation of one row, which every task combines from the partial
		// results itself. (Groups of lineitem's columns take a stage more,
		// which completes them.)
		{`SELECT count(*) FROM lineitem WHERE l_quantity < (SELECT avg(l_quantity) FROM lineitem)`, 2, 2},
		{`SELECT l_returnflag, count(*) FROM lineitem WHERE l_suppkey IN (SELECT max(l_suppkey) FROM lineitem WHERE l_shipmode = 'RAIL')
			AND l_quantity > (SELECT avg(l_quantity) FROM lineitem WHERE l_shipmode = 'AIR') GROUP BY 1 ORDER BY 1`, 3, 4},
		// A relation of one row every task reads; lineitem is split.
		{`SELECT l_returnflag, count(*) FROM lineitem, (SELECT avg(l_quantity) AS a FROM lineitem) AS s
			WHERE l_quantity > s.a GROUP BY 1 ORDER BY 1`, 2, 3},
		// TPC-H Q18 with its subquery as a common table expression: three
		// inputs partitioned by order.
		{`WITH big AS (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 250)
			SELECT c_name, o_orderkey, o_totalprice, sum(lineitem.l_quantity) FROM customer, orders, lineitem, big
			WHERE o_orderkey = big.l_orderkey AND c_custkey = o_custkey AND o_orderkey = lineitem.l_orderkey
			GROUP BY c_name, o_orderkey, o_totalprice ORDER BY o_totalprice DESC, o_orderkey LIMIT 20`, 2, 0},
		// A common table expression read twice by one part: computed once.
		{`WITH s AS (SELECT l_orderkey AS k, sum(l_quantity) AS q FROM lineitem GROUP BY 1)
			SELECT o_orderpriority, count(*), sum(a.q), sum(b.q) FROM s a JOIN orders o ON o.o_orderkey = a.k JOIN s b ON b.k = o.o_orderkey
			GROUP BY 1 ORDER BY 1`, 1, 0},
		// Read by a distributed part and by the coordinator: computed once,
		// and the coordinator reads what workers wrote.
		{`WITH s AS (SELECT l_orderkey AS k, sum(l_quantity) AS q FROM lineitem GROUP BY 1),
			t AS (SELECT o_orderpriority, sum(q) AS total, count(*) AS n FROM s JOIN orders ON o_orderkey = k GROUP BY 1)
			SELECT (SELECT count(*) FROM s WHERE q > 100) AS heavy, o_orderpriority, total, n FROM t ORDER BY 2`, 1, 0},
	} {
		t.Run(short(tc.query), func(t *testing.T) {
			h.t = t
			if !h.check(tc.query) {
				t.Fatal("did not run distributed")
			}
			if tc.scans > 0 {
				p := h.planOf(tc.query)
				if _, scans := relationExchanges(p, "lineitem"); scans != tc.scans {
					t.Errorf("%d stages read lineitem's files, want %d:\n%s", scans, tc.scans, p.describe())
				}
				if tc.stages > 0 && len(p.Stages) != tc.stages {
					t.Errorf("%d stages, want %d:\n%s", len(p.Stages), tc.stages, p.describe())
				}
			}
		})
	}
}

// TestInlinedRelations checks that a common table expression read once,
// which only filters and projects tables, is read as its query: its reader
// divides the table itself and aggregates where it reads it, with no stage
// of the expression's own. One read twice is a relation still, computed
// once.
func TestInlinedRelations(t *testing.T) {
	h := newHarness(t, 0.02, 5, 3)
	h.co.maxBroadcast = h.tables["customer"].Bytes
	for _, tc := range []struct {
		query string
		// stages and scans, if not 0, are how many stages the plan has and
		// how many of them read lineitem's files. Groups of lineitem's
		// columns take a stage besides the one that reads it, which
		// completes them.
		stages, scans int
	}{
		{`WITH air AS (SELECT l_returnflag AS f, l_quantity AS q FROM lineitem WHERE l_shipmode = 'AIR')
			SELECT f, sum(q), count(*) FROM air GROUP BY 1 ORDER BY 1`, 2, 1},
		// Columns named by the expression, and by the reference.
		{`WITH x(f) AS (SELECT l_returnflag, l_quantity AS q FROM lineitem) SELECT f, sum(q) FROM x GROUP BY 1 ORDER BY 1`, 2, 1},
		{`WITH x(f, q) AS (SELECT l_returnflag, l_quantity FROM lineitem) SELECT y.a, sum(y.b) FROM x AS y(a, b) GROUP BY 1 ORDER BY 1`, 2, 1},
		// Distinct values of each task's rows, completed by value.
		{`WITH big AS (SELECT l_orderkey, l_suppkey FROM lineitem WHERE l_quantity > 20)
			SELECT l_orderkey % 7 AS k, count(DISTINCT l_suppkey) FROM big GROUP BY 1 ORDER BY 1`, 0, 1},
		// Read twice: computed once.
		{`WITH s AS (SELECT l_orderkey AS k, l_quantity AS q FROM lineitem WHERE l_quantity > 10)
			SELECT count(*), sum(a.q) FROM s a JOIN s b ON a.k = b.k`, 0, 1},
		// One read as its query reads another, read once there.
		{`WITH a AS (SELECT * FROM lineitem WHERE l_quantity > 5), b AS (SELECT l_returnflag AS f, l_quantity AS q FROM a WHERE l_shipmode <> 'AIR')
			SELECT f, sum(q) FROM b GROUP BY 1 ORDER BY 1`, 2, 1},
	} {
		t.Run(short(tc.query), func(t *testing.T) {
			h.t = t
			if !h.check(tc.query) {
				t.Fatal("did not run distributed")
			}
			p := h.planOf(tc.query)
			_, scans := relationExchanges(p, "lineitem")
			if tc.stages > 0 && len(p.Stages) != tc.stages || tc.scans > 0 && scans != tc.scans {
				t.Errorf("%d stages, %d reading lineitem's files; want %d and %d:\n%s", len(p.Stages), scans, tc.stages, tc.scans, p.describe())
			}
		})
	}
}

// TestPartitions checks how many partitions exchanges have: twice the
// workers, more for larger divided tables, up to eight times the workers
// and maxPartitions.
func TestPartitions(t *testing.T) {
	for _, c := range []struct {
		workers      int
		largest, per int64
		want         int
	}{{0, 0, 1 << 20, 2}, {4, 0, 1 << 20, 8}, {4, 10 << 20, 1 << 20, 10}, {4, 10<<20 + 1, 1 << 20, 11},
		{4, 1 << 50, 1 << 20, 32}, {16, 1 << 50, 1 << 20, 128}, {40, 1 << 50, 1 << 20, maxPartitions}} {
		if got := partitions(c.workers, c.largest, c.per); got != c.want {
			t.Errorf("partitions(%d, %d, %d) = %d, want %d", c.workers, c.largest, c.per, got, c.want)
		}
	}
}

// TestFusedPartitions checks that a stage that computes, partition by
// partition, rows keyed as it read them is computed by the one stage that
// reads them, and returns what one machine returns, for keys of several
// types: an aggregation by key over a relation keyed by it, and a semi
// join's rows joined to a table on its key. A key cast on its way, or an
// integer one, which exchanges hash as BIGINT, keeps its stage: read in
// place of it, the semi join's partitions would not be the table's.
func TestFusedPartitions(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	keys := `(i % 5000)::BIGINT AS kb, (i % 5000)::DECIMAL(38,0) AS kd, 'k' || (i % 5000) AS ks, DATE '2020-01-01' + (i % 5000)::INTEGER AS kt`
	h.addTable("facts", `(SELECT i AS id, `+keys+`, (i % 97)::DOUBLE AS v FROM range(60000) t(i))`, 3)
	h.addTable("dims", `(SELECT `+strings.ReplaceAll(keys, "i % 5000", "i * 3 % 6000")+` FROM range(4000) t(i))`, 2)
	h.addTable("others", `(SELECT `+strings.ReplaceAll(keys, "i % 5000", "i % 7000")+`, CAST(i % 7000 AS VARCHAR) AS kc,
		(i % 13)::BIGINT AS w FROM range(30000) t(i))`, 2)
	h.co.maxBroadcast = 1
	grouped := func(key, out, on string) string {
		return `WITH s AS (SELECT ` + key + ` AS k, v FROM facts WHERE ` + key + ` IN (SELECT ` + key + ` FROM dims)),
			per AS (SELECT ` + out + ` AS k, sum(v) AS total, count(*) AS n FROM s GROUP BY 1)
			SELECT count(*), sum(per.total), sum(per.n), sum(o.w) FROM per JOIN others AS o ON o.` + on + ` = per.k`
	}
	joined := func(key, out, on string) string {
		return `WITH s AS (SELECT ` + out + ` AS k, f.v FROM facts AS f WHERE f.` + key + ` IN (SELECT ` + key + ` FROM dims))
			SELECT count(*), sum(s.v), sum(o.w), count(DISTINCT o.w) FROM s JOIN others AS o ON o.` + on + ` = s.k`
	}
	for _, tc := range []struct {
		query string
		fused bool
	}{
		{grouped("kb", "k", "kb"), false},
		{grouped("kd", "k", "kd"), true},
		{grouped("ks", "k", "ks"), true},
		{grouped("kt", "k", "kt"), true},
		{grouped("kb", "CAST(k AS VARCHAR)", "kc"), false},
		{joined("kb", "f.kb", "kb"), false},
		{joined("kd", "f.kd", "kd"), true},
		{joined("ks", "f.ks", "ks"), true},
		{joined("kt", "f.kt", "kt"), true},
		{joined("kb", "CAST(f.kb AS VARCHAR)", "kc"), false},
		{joined("ks", "CAST(f.ks AS VARCHAR)", "ks"), false},
	} {
		t.Run(short(tc.query), func(t *testing.T) {
			h.t = t
			if !h.check(tc.query) {
				t.Fatal("did not run distributed")
			}
			p := h.planOf(tc.query)
			fused := slices.ContainsFunc(p.Stages, func(s *Stage) bool { return strings.Contains(s.SQL, "__igloo_x") })
			if fused != tc.fused {
				t.Errorf("fused: %v, want %v:\n%s", fused, tc.fused, p.describe())
			}
			t.Logf("%d stages:\n%s", len(p.Stages), p.describe())
		})
	}
}

// TestSnowflakeAudienceReport runs the audience report of
// scripts/e2e-distributed.sh, in Snowflake's dialect as BI tools send it:
// cohorts from an audience's latest version and from a view of people,
// signals from FLATTEN of array columns read through views, distinct
// people per signal and cohort, and a ranking. It runs on workers up to
// the comparisons, alone and twice over in one statement; each copy of
// the report, whose common table expressions have the names of the
// other's, is planned as the report alone is.
func TestSnowflakeAudienceReport(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	for _, stmt := range snowflake.Macros() {
		h.exec(stmt)
	}
	list := func(id int) []Column {
		return []Column{{ID: id, Nested: &Nesting{Kind: "list", Fields: []Field{{Name: "element", ID: id + 10}}}}}
	}
	h.addTable("aud_people", `(SELECT 'p' || i AS person_id, 'h' || (i // 3) AS household_id, CASE WHEN i % 2 = 0 THEN 'F' ELSE 'M' END AS gender,
		CAST(i % 7 + 1 AS INTEGER) AS age_bucket FROM range(9000) r(i))`, 4)
	h.addNestedTable("aud_media_daily", `(SELECT 'p' || (i % 9000) AS person_id, DATE '2026-09-01' + CAST(i % 30 AS INTEGER) AS day,
		list_transform(range(1 + i % 4), x -> 'net' || ((i * 7 + x * 13) % 40)) AS network_array,
		list_transform(range(1 + i % 3), x -> 'genre' || ((i * 3 + x * 5) % 12)) AS genre_array FROM range(27000) r(i))`, 4,
		append(append([]Column{{ID: 1}, {ID: 2}}, list(3)...), list(4)...))
	h.addNestedTable("aud_browsing_daily", `(SELECT 'p' || ((i * 7) % 9000) AS person_id, DATE '2026-09-01' + CAST(i % 30 AS INTEGER) AS day,
		list_transform(range(1 + i % 5), x -> 'cat' || ((i * 5 + x * 11) % 60)) AS category_array FROM range(27000) r(i))`, 4,
		append([]Column{{ID: 1}, {ID: 2}}, list(3)...))
	h.addTable("aud_members", `(SELECT 'a1' AS audience_id, 1 AS ver, 'p' || i AS person_id FROM range(9000) r(i) WHERE i % 5 = 0
		UNION ALL SELECT 'a1', 2, 'p' || i FROM range(9000) r(i) WHERE i % 6 = 0 AND i % 4 <> 2)`, 3)
	h.addView("v_aud_people", `SELECT * FROM aud_people`)
	h.addView("v_aud_media_daily", `SELECT * FROM aud_media_daily`)
	h.addView("v_aud_browsing_daily", `SELECT * FROM aud_browsing_daily`)
	h.co.maxBroadcast = 1

	const report = `WITH AUDIENCE AS (
    SELECT PERSON_ID FROM AUD_MEMBERS WHERE AUDIENCE_ID = 'a1'
      AND VER = (SELECT MAX(VER) FROM AUD_MEMBERS WHERE AUDIENCE_ID = 'a1')),
  BASE_POPULATION AS (SELECT PERSON_ID FROM V_AUD_PEOPLE),
  COHORT_MEMBERS AS (
    SELECT PERSON_ID, 'AUDIENCE' AS COHORT FROM AUDIENCE
    UNION ALL SELECT PERSON_ID, 'POPULATION' AS COHORT FROM BASE_POPULATION),
  COHORT_TOTALS AS (SELECT COHORT, COUNT(DISTINCT PERSON_ID) AS COHORT_SIZE FROM COHORT_MEMBERS GROUP BY COHORT),
  MEDIA_COUNTS AS (
    SELECT M.PERSON_ID, 'NETWORK' AS DIMENSION, F.VALUE::STRING AS SIGNAL
    FROM V_AUD_MEDIA_DAILY M, LATERAL FLATTEN(INPUT => M.NETWORK_ARRAY) F),
  GENRE_COUNTS AS (
    SELECT M.PERSON_ID, 'GENRE' AS DIMENSION, F.VALUE::STRING AS SIGNAL
    FROM V_AUD_MEDIA_DAILY M, LATERAL FLATTEN(INPUT => M.GENRE_ARRAY) F),
  BROWSING_COUNTS AS (
    SELECT B.PERSON_ID, 'CATEGORY' AS DIMENSION, F.VALUE::STRING AS SIGNAL
    FROM V_AUD_BROWSING_DAILY B, LATERAL FLATTEN(INPUT => B.CATEGORY_ARRAY) F),
  ALL_COUNTS AS (
    SELECT * FROM MEDIA_COUNTS UNION ALL SELECT * FROM GENRE_COUNTS UNION ALL SELECT * FROM BROWSING_COUNTS),
  ITEM_METRICS AS (
    SELECT C.COHORT, S.DIMENSION, S.SIGNAL, COUNT(DISTINCT S.PERSON_ID) AS PEOPLE
    FROM ALL_COUNTS S JOIN COHORT_MEMBERS C ON C.PERSON_ID = S.PERSON_ID
    GROUP BY C.COHORT, S.DIMENSION, S.SIGNAL),
  COMPARISONS AS (
    SELECT A.DIMENSION, A.SIGNAL, A.PEOPLE AS DISTINCT_PEOPLE,
      ROUND(A.PEOPLE / ASZ.COHORT_SIZE, 6) AS AUDIENCE_RATE,
      ROUND(P.PEOPLE / PSZ.COHORT_SIZE, 6) AS POPULATION_RATE,
      ROUND((A.PEOPLE / ASZ.COHORT_SIZE) / NULLIF(P.PEOPLE / PSZ.COHORT_SIZE, 0), 6) AS INDEX_RATIO
    FROM ITEM_METRICS A
    JOIN ITEM_METRICS P ON P.DIMENSION = A.DIMENSION AND P.SIGNAL = A.SIGNAL AND P.COHORT = 'POPULATION'
    JOIN COHORT_TOTALS ASZ ON ASZ.COHORT = 'AUDIENCE'
    JOIN COHORT_TOTALS PSZ ON PSZ.COHORT = 'POPULATION'
    WHERE A.COHORT = 'AUDIENCE'),
  RANKED AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY DIMENSION ORDER BY INDEX_RATIO DESC, SIGNAL) AS RN FROM COMPARISONS)
  SELECT DIMENSION, SIGNAL, DISTINCT_PEOPLE, AUDIENCE_RATE, POPULATION_RATE, INDEX_RATIO FROM RANKED`
	twice := `SELECT * FROM (SELECT * FROM (` + report + `) ORDER BY INDEX_RATIO DESC, SIGNAL LIMIT 5)
  UNION ALL SELECT * FROM (SELECT * FROM (` + report + `) ORDER BY INDEX_RATIO ASC, SIGNAL LIMIT 5)
  ORDER BY INDEX_RATIO DESC, SIGNAL`
	var stages []int
	for _, q := range []string{report, twice} {
		r, err := snowflake.Translate(`SELECT * FROM (`+q+`) ORDER BY DIMENSION, INDEX_RATIO DESC, SIGNAL`, snowflake.Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.check(r.SQL) {
				t.Fatal("did not run distributed")
			}
			p := h.planOf(r.SQL)
			stages = append(stages, len(p.Stages))
			t.Logf("%d stages:\n%s", len(p.Stages), p.describe())
			// The ranking and the comparisons stay on the coordinator, and
			// the plan says why, for the logs.
			if len(p.local) == 0 {
				t.Error("no reasons for the parts the coordinator runs")
			}
			t.Logf("on the coordinator: %s", strings.Join(p.local, "; "))
		})
	}
	if len(stages) == 2 && stages[1] != 2*stages[0] {
		t.Errorf("the report twice over runs in %d stages, the report in %d", stages[1], stages[0])
	}
}

// TestSemiJoinReport runs a report shaped like the travel-interest ones:
// a large table of people's interests filtered by membership of an
// audience, x IN (SELECT DISTINCT ...), read by two aggregations, one
// through a subquery that groups by person.
func TestSemiJoinReport(t *testing.T) {
	h := audienceHarness(t)
	q := `WITH audience AS (
		SELECT person_id, state, income FROM people
		WHERE person_id IN (SELECT DISTINCT person_id FROM ((SELECT person_id FROM lookup
			WHERE audience_id = 2 AND ver = (SELECT max(ver) FROM meta WHERE audience_id = 2))))),
	by_state AS (SELECT state, count(1) AS n, sum(income) AS w, sum(count(1)) OVER () AS total FROM audience GROUP BY state),
	fill AS (SELECT 1 - (CAST(count(CASE WHEN v = 0 THEN 1 END) AS DOUBLE) / NULLIF(count(1), 0)) AS fill_rate
		FROM (SELECT person_id, sum(income) AS v FROM audience GROUP BY person_id))
	SELECT state, n, w, total, (SELECT fill_rate FROM fill) AS fill_rate FROM by_state ORDER BY state NULLS FIRST`
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
	p := h.planOf(q)
	// people and the audience's members are scanned once each.
	if _, scans := relationExchanges(p, "people"); scans != 1 {
		t.Errorf("%d scans of people, want 1:\n%s", scans, p.describe())
	}
	if _, scans := relationExchanges(p, "lookup"); scans != 1 {
		t.Errorf("%d scans of lookup, want 1:\n%s", scans, p.describe())
	}
	t.Logf("%d stages:\n%s", len(p.Stages), p.describe())
}

// TestSmallRelation checks that a relation small enough for every worker
// to read is read in full beside the table the workers divide, rather
// than the table partitioned through object storage.
func TestSmallRelation(t *testing.T) {
	h := newHarness(t, 0.02, 5, 3)
	h.co.maxBroadcast = h.tables["customer"].Bytes
	for _, q := range []string{
		`SELECT l_shipmode, count(*), sum(c) FROM lineitem JOIN (SELECT n_regionkey AS r, count(*) AS c FROM nation GROUP BY 1) AS s
			ON s.r = l_suppkey % 5 GROUP BY 1 ORDER BY 1`,
		`WITH s AS (SELECT n_regionkey AS r, count(*) AS c FROM nation GROUP BY 1)
			SELECT l_shipmode, count(*), sum(c) FROM lineitem, s WHERE s.r = l_partkey % 5 GROUP BY 1 ORDER BY 1`,
	} {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.check(q) {
				t.Fatal("did not run distributed")
			}
			if p := h.planOf(q); p.exchanges("join") != 0 {
				t.Errorf("lineitem was partitioned:\n%s", p.describe())
			}
		})
	}
}

// TestRelationColumns checks that a subquery read as a relation, as the
// branches of a report read a view of a wide table joined on its key,
// writes only the columns its reader reads, and returns what one machine
// does.
func TestRelationColumns(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	h.co.maxBroadcast = 1
	q := `WITH g AS (SELECT l_orderkey FROM lineitem)
		SELECT x.f, count(*) FROM (
			SELECT a.l_orderkey AS k, a.l_returnflag AS f FROM (SELECT * FROM (SELECT * FROM lineitem) AS l) AS a JOIN g ON g.l_orderkey = a.l_orderkey
			UNION ALL
			SELECT a.l_orderkey, a.l_linestatus FROM (SELECT * FROM (SELECT * FROM lineitem) AS l) AS a JOIN g ON g.l_orderkey = a.l_orderkey
		) AS x GROUP BY 1 ORDER BY 1`
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
	p := h.planOf(q)
	var written []string
	for _, s := range p.Stages {
		if s.Out != nil && s.split() != nil && strings.Contains(s.SQL, "AS l(") {
			written = append(written, s.SQL[:strings.Index(s.SQL, " FROM ")])
		}
	}
	slices.Sort(written)
	if want := []string{"SELECT l_orderkey, l_linestatus", "SELECT l_orderkey, l_returnflag"}; !slices.Equal(written, want) {
		t.Errorf("the relations' stages write %q, want %q:\n%s", written, want, p.describe())
	}
}

// TestFactoredUnion checks the form pgwire gives a UNION ALL whose branches
// select from one join (see pgwire's unionfactor.go): one SELECT over the
// join, made once per branch by a series, in a subquery the aggregate
// reads. The join is partitioned where it is, inside the subquery, and its
// rows are aggregated where they are made, not written to an exchange.
func TestFactoredUnion(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	h.co.maxBroadcast = 1
	for _, q := range []string{
		`WITH g AS (SELECT l_orderkey FROM lineitem)
		SELECT x.f, count(*), count(CASE WHEN x.late THEN x.k END) FROM (
			SELECT a.l_orderkey AS k, CASE _igloo_branch WHEN 0 THEN a.l_returnflag WHEN 1 THEN a.l_linestatus END AS f,
				CASE _igloo_branch WHEN 0 THEN a.l_receiptdate > a.l_commitdate WHEN 1 THEN a.l_shipdate > a.l_commitdate END AS late
			FROM (SELECT * FROM (SELECT * FROM lineitem) AS l) AS a JOIN g ON g.l_orderkey = a.l_orderkey
			CROSS JOIN range(2) AS _igloo_branches(_igloo_branch)
		) AS x GROUP BY 1 ORDER BY 1`,
		// The same without the series: a subquery over a join.
		`WITH g AS (SELECT l_orderkey FROM lineitem)
		SELECT x.f, count(*) FROM (
			SELECT a.l_orderkey AS k, a.l_returnflag AS f
			FROM (SELECT * FROM (SELECT * FROM lineitem) AS l) AS a JOIN g ON g.l_orderkey = a.l_orderkey
		) AS x GROUP BY 1 ORDER BY 1`,
	} {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.check(q) {
				t.Fatal("did not run distributed")
			}
			p := h.planOf(q)
			// The stage that joins the partitions aggregates their rows;
			// it writes only its partial results, for the stage that
			// completes the groups of lineitem's columns.
			for _, s := range p.Stages {
				if s.Out != nil && s.split() == nil && s.Out.purpose != "aggregate" {
					t.Errorf("a stage writes rows it read by partition:\n%s", p.describe())
					break
				}
			}
			if len(p.Stages) != 4 {
				t.Errorf("%d stages, want 4:\n%s", len(p.Stages), p.describe())
			}
		})
	}
}

// TestScalarSubqueryRelation checks a relation whose select list has a
// scalar subquery that stands on its own, as pgwire writes min(x) OVER ()
// for one machine: the subquery is a relation of one row that every task
// reads, and the statement returns what one machine does.
func TestScalarSubqueryRelation(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	h.co.maxBroadcast = 1
	for _, q := range []string{
		`WITH aud AS (SELECT DISTINCT o_orderkey AS k FROM orders WHERE o_orderpriority = '1-URGENT'),
			base AS (SELECT l.l_orderkey AS k, l.l_quantity AS q,
				(SELECT min(l2.l_shipdate) FROM lineitem AS l2 JOIN aud ON aud.k = l2.l_orderkey) AS first_day,
				(SELECT max(l2.l_shipdate) FROM lineitem AS l2 JOIN aud ON aud.k = l2.l_orderkey) AS last_day
				FROM lineitem AS l JOIN aud ON aud.k = l.l_orderkey)
			SELECT first_day, last_day, count(*), sum(q) FROM base GROUP BY 1, 2`,
		// As written before pgwire's rewrite.
		`WITH aud AS (SELECT DISTINCT o_orderkey AS k FROM orders WHERE o_orderpriority = '1-URGENT'),
			base AS (SELECT l.l_orderkey AS k, l.l_quantity AS q, min(l.l_shipdate) OVER () AS first_day, max(l.l_shipdate) OVER () AS last_day
				FROM lineitem AS l JOIN aud ON aud.k = l.l_orderkey)
			SELECT first_day, last_day, count(*), sum(q) FROM base GROUP BY 1, 2`,
	} {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.check(q) {
				t.Error("did not run distributed")
			}
		})
	}
	// Correlated, it stays on one machine.
	h.t = t
	q := `WITH base AS (SELECT l.l_orderkey AS k, (SELECT max(o_totalprice) FROM orders WHERE o_orderkey = l.l_orderkey) AS price FROM lineitem AS l)
		SELECT count(*), sum(price) FROM base`
	if plan, err := h.co.Plan(t.Context(), q, harnessEnv{h}, ModeOn); err == nil && len(plan.Stages) > 0 {
		for _, s := range plan.Stages {
			if strings.Contains(s.SQL, "o_orderkey = l.l_orderkey") {
				t.Errorf("a correlated subquery went to the workers:\n%s", plan.describe())
			}
		}
	}
}
