-- Approximate people by site.
SELECT site, approx_count_distinct(person_id) AS people, count(*) AS events
FROM events GROUP BY 1 ORDER BY 1;
