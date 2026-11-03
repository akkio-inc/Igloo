-- two large relations not joined on equal columns: a report shape of internal/distributed/reportshapes_test.go.
SELECT t2.* FROM (SELECT t0.*, t0.audience_share * 100 AS reach FROM (WITH cohorts AS (
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
ORDER BY category, rank_within_category, dimension, name, cohort) AS t0
		WHERE upper(t0.cohort) = 'AUDIENCE' AND t0.audience_share IS NOT NULL ORDER BY t0.audience_share DESC NULLS LAST, t0.people DESC, t0.name LIMIT 12) AS t2 LIMIT 10000;
