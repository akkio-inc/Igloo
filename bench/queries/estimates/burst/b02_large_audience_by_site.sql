-- The same for an audience of half of everyone.
SELECT e.site, count(DISTINCT e.person_id) AS people, count(*) AS events
FROM events e JOIN memberships m ON m.person_id = e.person_id
WHERE m.audience_id = 'aud-0'
GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 50;
