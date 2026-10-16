-- Frequency distribution of a tenth of the campaigns: impressions per
-- device, capped at 20.
SELECT least(frequency, 20) AS frequency, count(*) AS devices
FROM (SELECT device_id, count(*) AS frequency FROM impressions WHERE campaign_id < 200 GROUP BY 1) AS f
GROUP BY 1
ORDER BY 1;
