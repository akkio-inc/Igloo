-- The ingestion pipeline's first load: 30 days of ad-platform delivery for
-- eight campaigns, their conversions and their bidding settings.
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.ad_platform_daily (
    platform VARCHAR,
    campaign_id INTEGER,
    event_date DATE,
    impressions BIGINT,
    clicks BIGINT,
    spend DECIMAL(14, 2),
    _loaded_at TIMESTAMPTZ
);
INSERT INTO raw.ad_platform_daily
SELECT
    CASE c.channel WHEN 'search' THEN 'google_ads' WHEN 'social' THEN 'meta' WHEN 'video' THEN 'youtube' ELSE 'dv360' END,
    c.id,
    DATE '2026-09-01' + d.i::INTEGER,
    1000 + (c.id * 37 + d.i * 101) % 5000,
    (1000 + (c.id * 37 + d.i * 101) % 5000) // 40,
    round((1000 + (c.id * 37 + d.i * 101) % 5000) * 0.0042, 2),
    now() - INTERVAL 6 HOUR
FROM (VALUES (101, 'search'), (102, 'social'), (103, 'display'), (201, 'search'),
             (202, 'video'), (203, 'social'), (301, 'search'), (302, 'display')) AS c(id, channel),
     range(30) AS d(i);

CREATE TABLE raw.conversions (
    conversion_id VARCHAR,
    campaign_id INTEGER,
    converted_at TIMESTAMP,
    revenue DECIMAL(12, 2),
    _loaded_at TIMESTAMPTZ
);
INSERT INTO raw.conversions
SELECT
    'cv-' || n,
    [101, 102, 103, 201, 202, 203, 301, 302][1 + n % 8],
    TIMESTAMP '2026-09-01 08:00:00' + to_minutes((n * 97 % 43200)::BIGINT),
    CASE WHEN n % 5 = 0 THEN NULL ELSE round(20 + (n * 13 % 400) * 0.75, 2) END,
    now() - INTERVAL 6 HOUR
FROM range(400) AS t(n);

CREATE TABLE raw.campaign_settings (
    campaign_id INTEGER,
    bid_strategy VARCHAR,
    status VARCHAR,
    updated_at TIMESTAMP
);
INSERT INTO raw.campaign_settings VALUES
    (101, 'target_cpa', 'active', TIMESTAMP '2026-09-01 00:00:00'),
    (102, 'max_conversions', 'active', TIMESTAMP '2026-09-15 00:00:00'),
    (103, 'manual_cpm', 'active', TIMESTAMP '2026-09-01 00:00:00'),
    (201, 'target_roas', 'active', TIMESTAMP '2026-08-20 00:00:00'),
    (202, 'manual_cpv', 'active', TIMESTAMP '2026-09-10 00:00:00'),
    (203, 'max_clicks', 'active', TIMESTAMP '2026-09-20 00:00:00'),
    (301, 'target_cpa', 'active', TIMESTAMP '2026-09-05 00:00:00'),
    (302, 'manual_cpm', 'paused', TIMESTAMP '2026-09-05 00:00:00');
