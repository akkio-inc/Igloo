-- Distinct sites a person visits, for everyone, and the share by count.
SELECT least(sites, 50) AS sites, count(*) AS people
FROM (SELECT person_id, count(DISTINCT site) AS sites FROM events GROUP BY 1) AS s
GROUP BY 1 ORDER BY 1;
