-- LIMIT without ORDER BY: a report shape of internal/distributed/reportshapes_test.go.
SELECT * FROM (SELECT t0.value FROM (WITH base AS (
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
SELECT * FROM ranked WHERE kind = 'overall' OR rnk <= 10 ORDER BY source, kind, rnk, members DESC NULLS LAST) AS t0 WHERE t0.kind <> 'overall' AND t0.rnk = 1
		ORDER BY t0.members DESC NULLS LAST, t0.activity DESC NULLS LAST, t0.value LIMIT 4) AS t2 LIMIT 10000;
