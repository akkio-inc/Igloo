-- Each person's first event, by site: a window over every event.
SELECT site, count(*) AS people
FROM (SELECT person_id, site, row_number() OVER (PARTITION BY person_id ORDER BY ts, site) AS rn FROM events) AS f
WHERE rn = 1 GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 20;
