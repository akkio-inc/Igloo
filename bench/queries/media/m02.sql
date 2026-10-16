-- Reach (distinct devices) by advertiser for the last two months.
SELECT c.advertiser, count(DISTINCT i.device_id) AS reach, count(*) AS impressions
FROM impressions i JOIN campaigns c ON c.campaign_id = i.campaign_id
WHERE i.ts >= TIMESTAMP '2026-08-01'
GROUP BY 1
ORDER BY reach DESC, 1
LIMIT 20;
