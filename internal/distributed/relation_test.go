package distributed

import (
	"context"
	"testing"
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
// place; only the signal-level rows reach the coordinator.
func TestAudienceReport(t *testing.T) {
	h := audienceHarness(t)
	if !h.check(audienceReport) {
		t.Fatal("did not run distributed")
	}
	p := h.planOf(audienceReport)
	// audience, aud_cohort, population, population_cohort, the
	// browsing subquery, and memberships, written once by both branches of
	// its UNION ALL.
	if n, scans := relationExchanges(p, "people"); n != 6 || scans != 2 {
		t.Errorf("%d relation exchanges and %d scans of people, want 6 and 2:\n%s", n, scans, p.describe())
	}
	// The sizes and three kinds of signals, the distinct people of each
	// counted in place, in partitions keyed by person.
	if n := coordinatorStages(p); n != 4 {
		t.Errorf("%d stages send rows to the coordinator, want 4:\n%s", n, p.describe())
	}
	if n := p.exchanges("aggregate"); n != 0 {
		t.Errorf("%d stages wrote partial results to an exchange, want none:\n%s", n, p.describe())
	}
	readers := 0
	for _, s := range p.Stages {
		for _, in := range s.Inputs {
			if in.Exchange != nil && in.Exchange.ID == p.Stages[2].Out.ID {
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
	// and sizes' distinct people are counted in place.
	if n := coordinatorStages(p); n != 2 || p.exchanges("aggregate") != 2 {
		t.Errorf("%d stages to the coordinator and %d aggregate exchanges, want 2 and 2:\n%s", n, p.exchanges("aggregate"), p.describe())
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
		// scans is how many stages read lineitem's files, if not 0.
		scans int
	}{
		// A large table joined to itself: both sides partitioned.
		{`SELECT count(*), sum(a.l_quantity - b.l_quantity) FROM lineitem a JOIN lineitem b ON a.l_orderkey = b.l_orderkey AND a.l_linenumber < b.l_linenumber`, 2},
		// Joins of large intermediate results, on the key one groups by.
		{`WITH x AS (SELECT l_orderkey, sum(l_quantity) AS q, count(*) AS n FROM lineitem GROUP BY 1),
			y AS (SELECT o_orderkey, o_custkey, o_orderpriority FROM orders WHERE o_totalprice > 1000)
			SELECT o_orderpriority, count(*), sum(q), max(n) FROM x JOIN y ON x.l_orderkey = y.o_orderkey GROUP BY 1 ORDER BY 1`, 1},
		{`SELECT o_orderpriority, count(*), sum(q) FROM (SELECT l_orderkey AS k, sum(l_quantity) AS q FROM lineitem GROUP BY 1) AS x
			JOIN orders ON o_orderkey = x.k WHERE o_orderstatus = 'F' GROUP BY 1 ORDER BY 1`, 1},
		// As scripts/e2e-distributed.sh runs it.
		{`WITH s AS (SELECT l_orderkey AS k, sum(l_quantity) AS q, count(DISTINCT l_suppkey) AS sups FROM lineitem GROUP BY 1),
			u AS (SELECT o_orderkey AS k FROM orders WHERE o_orderpriority = '1-URGENT'
				UNION ALL SELECT o_orderkey FROM orders WHERE o_orderpriority = '2-HIGH')
			SELECT count(*), sum(q), sum(sups) FROM s JOIN u ON s.k = u.k`, 1},
		// UNION ALL of large branches feeding one aggregation.
		{`SELECT k % 10 AS b, count(*), count(DISTINCT k) FROM (SELECT l_partkey AS k FROM lineitem UNION ALL SELECT ps_partkey FROM partsupp) GROUP BY 1 ORDER BY 1`, 1},
		{`WITH u AS (SELECT l_suppkey AS s, l_quantity AS q FROM lineitem WHERE l_shipmode = 'AIR'
			UNION ALL SELECT ps_suppkey, ps_availqty FROM partsupp WHERE ps_supplycost > 500)
			SELECT s % 7 AS k, count(DISTINCT s), sum(q), count(*) FROM u GROUP BY 1 ORDER BY 1`, 1},
		// Aggregates that do not compute in parts, over a relation keyed by
		// the groups, whose partitions hold them whole.
		{`WITH items AS (SELECT 'lines' AS section, l_shipmode AS name, l_quantity::DOUBLE AS v FROM lineitem
				UNION ALL SELECT 'supplies', CAST(ps_suppkey % 7 AS VARCHAR), ps_supplycost::DOUBLE FROM partsupp)
			SELECT section, name, median(v) AS m, quantile_cont(v, 0.9) AS q, count(*) AS n, avg(v) AS a,
				string_agg(DISTINCT CAST(v::INTEGER % 3 AS VARCHAR), ',' ORDER BY CAST(v::INTEGER % 3 AS VARCHAR)) AS s
			FROM items GROUP BY section, name ORDER BY 1, 2`, 1},
		{`WITH items AS (SELECT l_shipmode AS name, l_extendedprice AS v FROM lineitem WHERE l_quantity > 10),
				stats AS (SELECT name, median(v) AS m, count(DISTINCT v) AS d FROM items GROUP BY 1)
			SELECT name, m, d, rank() OVER (ORDER BY m DESC) AS r FROM stats ORDER BY name`, 1},
		// Aggregates over all of a relation's rows, as the reports' period
		// start and end: the relation's rows joined to the aggregates' row.
		{`WITH urgent AS (SELECT o_orderkey FROM orders WHERE o_orderpriority = '1-URGENT'),
			base AS (SELECT l.l_orderkey, l.l_shipmode, l.l_quantity, min(l.l_shipdate) OVER () AS first_ship, max(l.l_shipdate) OVER () AS last_ship,
				count(*) OVER () AS n, sum(l.l_quantity) FILTER (WHERE l.l_returnflag = 'R') OVER () AS returned,
				l.l_quantity / max(l.l_quantity) OVER () AS share
				FROM lineitem AS l INNER JOIN urgent AS u ON u.o_orderkey = l.l_orderkey)
			SELECT l_shipmode, first_ship, last_ship, n, returned, count(DISTINCT l_orderkey) AS orders, sum(l_quantity), sum(share)
			FROM base GROUP BY ALL ORDER BY 1`, 1},
		// Common table expressions of a set operation, read by its branches.
		{`WITH x AS (SELECT l_returnflag AS f, l_orderkey AS k, sum(l_quantity) AS q FROM lineitem GROUP BY 1, 2)
			SELECT 'q' AS m, f, sum(q) FROM x GROUP BY 1, 2 UNION ALL SELECT 'n', f, count(*) FROM x GROUP BY 1, 2 ORDER BY 1, 2`, 1},
		// A relation of one row every task reads; lineitem is split.
		{`SELECT l_returnflag, count(*) FROM lineitem, (SELECT avg(l_quantity) AS a FROM lineitem) AS s
			WHERE l_quantity > s.a GROUP BY 1 ORDER BY 1`, 2},
		// TPC-H Q18 with its subquery as a common table expression: three
		// inputs partitioned by order.
		{`WITH big AS (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 250)
			SELECT c_name, o_orderkey, o_totalprice, sum(lineitem.l_quantity) FROM customer, orders, lineitem, big
			WHERE o_orderkey = big.l_orderkey AND c_custkey = o_custkey AND o_orderkey = lineitem.l_orderkey
			GROUP BY c_name, o_orderkey, o_totalprice ORDER BY o_totalprice DESC, o_orderkey LIMIT 20`, 2},
		// A common table expression read twice by one part: computed once.
		{`WITH s AS (SELECT l_orderkey AS k, sum(l_quantity) AS q FROM lineitem GROUP BY 1)
			SELECT o_orderpriority, count(*), sum(a.q), sum(b.q) FROM s a JOIN orders o ON o.o_orderkey = a.k JOIN s b ON b.k = o.o_orderkey
			GROUP BY 1 ORDER BY 1`, 1},
		// Read by a distributed part and by the coordinator: computed once,
		// and the coordinator reads what workers wrote.
		{`WITH s AS (SELECT l_orderkey AS k, sum(l_quantity) AS q FROM lineitem GROUP BY 1),
			t AS (SELECT o_orderpriority, sum(q) AS total, count(*) AS n FROM s JOIN orders ON o_orderkey = k GROUP BY 1)
			SELECT (SELECT count(*) FROM s WHERE q > 100) AS heavy, o_orderpriority, total, n FROM t ORDER BY 2`, 1},
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
			}
		})
	}
}
