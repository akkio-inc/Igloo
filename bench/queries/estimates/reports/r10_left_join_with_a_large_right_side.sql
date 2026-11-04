-- LEFT JOIN with a large right side: a report shape of internal/distributed/reportshapes_test.go.
SELECT o_orderpriority, count(*) AS n, count(l_orderkey) AS shipped FROM orders
		FULL JOIN (SELECT * FROM lineitem WHERE l_shipmode = 'AIR') AS l ON l_orderkey = o_orderkey GROUP BY 1 ORDER BY 1;
