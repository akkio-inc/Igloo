-- Weekly delivery by channel: a scan of every impression with a join to a
-- small table.
SELECT c.channel, date_trunc('week', i.ts) AS week, count(*) AS impressions,
       sum(i.cost) AS spend, count_if(i.clicked) AS clicks
FROM impressions i JOIN campaigns c ON c.campaign_id = i.campaign_id
GROUP BY ALL
ORDER BY ALL;
