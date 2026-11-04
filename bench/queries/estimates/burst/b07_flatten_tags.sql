-- People by tag for an audience: each event's list of tags unnested.
SELECT t.tag, count(DISTINCT e.person_id) AS people, count(*) AS events
FROM events e JOIN memberships m ON m.person_id = e.person_id, unnest(e.tags) AS t(tag)
WHERE m.audience_id = 'aud-10'
GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 100;
