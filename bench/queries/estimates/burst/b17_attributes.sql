-- An audience's people by age band and zip prefix.
SELECT p.age // 10 * 10 AS band, left(p.zip, 6) AS zip, count(DISTINCT p.person_id) AS people
FROM people p JOIN memberships m ON m.person_id = p.person_id
WHERE m.audience_id = 'aud-40'
GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2 LIMIT 100;
