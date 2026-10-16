-- The next day's load: a new day of delivery, a late row for a day already
-- reported, new and restated conversions, and a paused campaign.
INSERT INTO raw.ad_platform_daily
SELECT
    CASE c.channel WHEN 'search' THEN 'google_ads' WHEN 'social' THEN 'meta' WHEN 'video' THEN 'youtube' ELSE 'dv360' END,
    c.id,
    DATE '2026-10-01',
    2000 + c.id,
    (2000 + c.id) // 40,
    round((2000 + c.id) * 0.0042, 2),
    now() - INTERVAL 1 HOUR
FROM (VALUES (101, 'search'), (102, 'social'), (103, 'display'), (201, 'search'),
             (202, 'video'), (203, 'social'), (301, 'search'), (302, 'display')) AS c(id, channel);
INSERT INTO raw.ad_platform_daily VALUES
    ('google_ads', 101, DATE '2026-09-30', 1500, 30, 6.30, now() - INTERVAL 1 HOUR);

INSERT INTO raw.conversions
SELECT
    'cv-' || n,
    [101, 102, 103, 201, 202, 203, 301, 302][1 + n % 8],
    TIMESTAMP '2026-10-01 08:00:00' + to_minutes(n::BIGINT),
    25.00,
    now() - INTERVAL 1 HOUR
FROM range(400, 420) AS t(n);
INSERT INTO raw.conversions VALUES
    ('cv-1', 102, TIMESTAMP '2026-09-01 09:37:00', 0.00, now() - INTERVAL 1 HOUR);

UPDATE raw.campaign_settings SET status = 'paused', updated_at = TIMESTAMP '2026-10-01 12:00:00'
WHERE campaign_id = 102;
