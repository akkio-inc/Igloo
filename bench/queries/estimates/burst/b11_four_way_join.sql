-- Events of the latest versions of every audience, by region and audience.
SELECT p.region, a.name, count(DISTINCT e.person_id) AS people, sum(e.value) AS value
FROM events e
JOIN memberships m ON m.person_id = e.person_id
JOIN audience_meta a ON a.audience_id = m.audience_id AND a.ver = m.ver
JOIN people p ON p.person_id = e.person_id
WHERE a.latest AND m.audience_id IN ('aud-20', 'aud-21', 'aud-22', 'aud-23')
GROUP BY 1, 2 ORDER BY 1, 2;
