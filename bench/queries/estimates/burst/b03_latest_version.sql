-- An audience's members at its latest version, by event type.
SELECT e.event_type, count(DISTINCT e.person_id) AS people, sum(e.value) AS value
FROM events e JOIN memberships m ON m.person_id = e.person_id
WHERE m.audience_id = 'aud-3' AND m.ver = (SELECT max(ver) FROM audience_meta WHERE audience_id = 'aud-3')
GROUP BY 1 ORDER BY 1;
