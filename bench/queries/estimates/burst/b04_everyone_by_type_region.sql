-- Everyone's people by event type and region.
SELECT e.event_type, p.region, count(DISTINCT e.person_id) AS people, count(*) AS events
FROM events e JOIN people p ON p.person_id = e.person_id
GROUP BY 1, 2 ORDER BY 1, 2;
