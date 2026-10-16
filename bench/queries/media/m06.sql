-- Click-through rate by hour of day for connected TV.
SELECT hour(i.ts) AS hour, count_if(i.clicked) / count(*) AS ctr
FROM impressions i JOIN campaigns c ON c.campaign_id = i.campaign_id
WHERE c.channel = 'ctv'
GROUP BY 1
ORDER BY 1;
