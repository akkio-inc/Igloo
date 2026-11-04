-- People in each of twenty audiences: each event joined to every audience
-- its person is in.
SELECT m.audience_id, count(DISTINCT e.person_id) AS people, count(*) AS events
FROM events e JOIN memberships m ON m.person_id = e.person_id
WHERE m.audience_id IN ('aud-1', 'aud-2', 'aud-3', 'aud-4', 'aud-5', 'aud-6', 'aud-7', 'aud-8', 'aud-9', 'aud-10',
  'aud-11', 'aud-12', 'aud-13', 'aud-14', 'aud-15', 'aud-16', 'aud-17', 'aud-18', 'aud-19', 'aud-20')
GROUP BY 1 ORDER BY 1;
