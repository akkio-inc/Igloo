-- Distinct counts over every event.
SELECT count(DISTINCT person_id) AS people, count(DISTINCT site) AS sites,
       count(DISTINCT person_id || '/' || site) AS pairs
FROM events;
