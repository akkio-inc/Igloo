-- An audience's share of each region against everyone's, as an index.
WITH audience AS (SELECT DISTINCT person_id FROM memberships WHERE audience_id = 'aud-7'),
aud AS (SELECT p.region, count(DISTINCT p.person_id) AS n FROM people p JOIN audience a ON a.person_id = p.person_id GROUP BY 1),
everyone AS (SELECT region, count(DISTINCT person_id) AS n FROM people GROUP BY 1),
totals AS (SELECT (SELECT sum(n) FROM aud) AS aud_total, (SELECT sum(n) FROM everyone) AS all_total)
SELECT e.region, a.n AS audience, e.n AS everyone,
       (a.n / t.aud_total) / (e.n / t.all_total) AS idx
FROM everyone e LEFT JOIN aud a ON a.region = e.region CROSS JOIN totals t
ORDER BY idx DESC NULLS LAST, e.region;
