-- Household reach by channel, through the device graph.
SELECT c.channel, count(DISTINCT g.household_id) AS households
FROM impressions i
JOIN device_graph g ON g.device_id = i.device_id
JOIN campaigns c ON c.campaign_id = i.campaign_id
GROUP BY 1
ORDER BY 1;
