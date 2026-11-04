-- How many views each person made, capped at 20.
SELECT least(n, 20) AS views, count(*) AS people
FROM (SELECT person_id, count(*) AS n FROM events WHERE event_type = 'view' GROUP BY 1) AS f
GROUP BY 1 ORDER BY 1;
