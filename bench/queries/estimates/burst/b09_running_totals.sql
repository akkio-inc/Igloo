-- Running totals of events by site and day.
SELECT site, day, n, sum(n) OVER (PARTITION BY site ORDER BY day) AS running
FROM (SELECT site, CAST(ts AS DATE) AS day, count(*) AS n FROM events GROUP BY 1, 2) AS d
ORDER BY site, day;
