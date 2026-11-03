-- The overlap of two audiences.
SELECT count(DISTINCT a.person_id) AS overlap
FROM memberships a JOIN memberships b ON b.person_id = a.person_id
WHERE a.audience_id = 'aud-1' AND b.audience_id = 'aud-5';
