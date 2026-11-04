-- LEFT JOIN with a large right side: a report shape of internal/distributed/reportshapes_test.go.
SELECT c_custkey, c_name, count(o_orderkey) AS orders, sum(o_totalprice) AS spent FROM customer
		LEFT JOIN orders ON o_custkey = c_custkey GROUP BY 1, 2 ORDER BY 3 DESC, 1 LIMIT 20;
