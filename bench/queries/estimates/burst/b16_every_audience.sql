-- People and events in every audience: events joined to all memberships.
SELECT m.audience_id, count(DISTINCT e.person_id) AS people, count(*) AS events
FROM events e JOIN memberships m ON m.person_id = e.person_id
GROUP BY 1 ORDER BY 1;
