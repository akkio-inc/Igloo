-- The ten devices with the most spend.
SELECT device_id, sum(cost) AS spend, count(*) AS impressions
FROM impressions
GROUP BY 1
ORDER BY spend DESC, device_id
LIMIT 10;
