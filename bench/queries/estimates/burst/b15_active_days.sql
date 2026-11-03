-- The people active on the most days.
SELECT person_id, count(DISTINCT CAST(ts AS DATE)) AS days
FROM events GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 100;
