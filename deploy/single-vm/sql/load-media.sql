-- Generates a synthetic media workload, for bench/queries/media: ad
-- impressions, the campaigns they belong to, and a device graph that maps
-- devices to households. Rows are computed from their number, so every run
-- and platform gets the same data. The default is 10 million impressions;
-- pick another with -v rows=1000000000 (a quarter of a large agency's
-- log-level data is on the order of 100 billion).
\if :{?rows}
\else
  \set rows 10000000
\endif

\timing on

DROP TABLE IF EXISTS impressions;
DROP TABLE IF EXISTS campaigns;
DROP TABLE IF EXISTS device_graph;

-- 2,000 campaigns of 200 advertisers on four channels.
CREATE TABLE campaigns AS
SELECT i AS campaign_id,
       'advertiser_' || (i % 200) AS advertiser,
       ['display', 'video', 'ctv', 'audio'][1 + i % 4] AS channel
FROM range(2000) t(i);

-- One impression per row, over a quarter: a timestamp, a campaign, one of
-- rows / 20 devices, a cost and whether it was clicked. Written in eight
-- parts, so the table has several data files at any size.
CREATE TABLE impressions (ts TIMESTAMP, campaign_id INTEGER, device_id BIGINT, cost DECIMAL(10, 4), clicked BOOLEAN);
INSERT INTO impressions
SELECT TIMESTAMP '2026-07-01' + to_microseconds(CAST(hash(i) % 7948800000000 AS BIGINT)),
       CAST(hash(i, 1) % 2000 AS INTEGER), CAST(hash(i, 2) % greatest(:rows // 20, 1) AS BIGINT),
       CAST((hash(i, 3) % 5000) / 1000.0 AS DECIMAL(10, 4)), hash(i, 4) % 100 = 0
FROM range(0, 1 * :rows // 8) t(i);
INSERT INTO impressions
SELECT TIMESTAMP '2026-07-01' + to_microseconds(CAST(hash(i) % 7948800000000 AS BIGINT)),
       CAST(hash(i, 1) % 2000 AS INTEGER), CAST(hash(i, 2) % greatest(:rows // 20, 1) AS BIGINT),
       CAST((hash(i, 3) % 5000) / 1000.0 AS DECIMAL(10, 4)), hash(i, 4) % 100 = 0
FROM range(1 * :rows // 8, 2 * :rows // 8) t(i);
INSERT INTO impressions
SELECT TIMESTAMP '2026-07-01' + to_microseconds(CAST(hash(i) % 7948800000000 AS BIGINT)),
       CAST(hash(i, 1) % 2000 AS INTEGER), CAST(hash(i, 2) % greatest(:rows // 20, 1) AS BIGINT),
       CAST((hash(i, 3) % 5000) / 1000.0 AS DECIMAL(10, 4)), hash(i, 4) % 100 = 0
FROM range(2 * :rows // 8, 3 * :rows // 8) t(i);
INSERT INTO impressions
SELECT TIMESTAMP '2026-07-01' + to_microseconds(CAST(hash(i) % 7948800000000 AS BIGINT)),
       CAST(hash(i, 1) % 2000 AS INTEGER), CAST(hash(i, 2) % greatest(:rows // 20, 1) AS BIGINT),
       CAST((hash(i, 3) % 5000) / 1000.0 AS DECIMAL(10, 4)), hash(i, 4) % 100 = 0
FROM range(3 * :rows // 8, 4 * :rows // 8) t(i);
INSERT INTO impressions
SELECT TIMESTAMP '2026-07-01' + to_microseconds(CAST(hash(i) % 7948800000000 AS BIGINT)),
       CAST(hash(i, 1) % 2000 AS INTEGER), CAST(hash(i, 2) % greatest(:rows // 20, 1) AS BIGINT),
       CAST((hash(i, 3) % 5000) / 1000.0 AS DECIMAL(10, 4)), hash(i, 4) % 100 = 0
FROM range(4 * :rows // 8, 5 * :rows // 8) t(i);
INSERT INTO impressions
SELECT TIMESTAMP '2026-07-01' + to_microseconds(CAST(hash(i) % 7948800000000 AS BIGINT)),
       CAST(hash(i, 1) % 2000 AS INTEGER), CAST(hash(i, 2) % greatest(:rows // 20, 1) AS BIGINT),
       CAST((hash(i, 3) % 5000) / 1000.0 AS DECIMAL(10, 4)), hash(i, 4) % 100 = 0
FROM range(5 * :rows // 8, 6 * :rows // 8) t(i);
INSERT INTO impressions
SELECT TIMESTAMP '2026-07-01' + to_microseconds(CAST(hash(i) % 7948800000000 AS BIGINT)),
       CAST(hash(i, 1) % 2000 AS INTEGER), CAST(hash(i, 2) % greatest(:rows // 20, 1) AS BIGINT),
       CAST((hash(i, 3) % 5000) / 1000.0 AS DECIMAL(10, 4)), hash(i, 4) % 100 = 0
FROM range(6 * :rows // 8, 7 * :rows // 8) t(i);
INSERT INTO impressions
SELECT TIMESTAMP '2026-07-01' + to_microseconds(CAST(hash(i) % 7948800000000 AS BIGINT)),
       CAST(hash(i, 1) % 2000 AS INTEGER), CAST(hash(i, 2) % greatest(:rows // 20, 1) AS BIGINT),
       CAST((hash(i, 3) % 5000) / 1000.0 AS DECIMAL(10, 4)), hash(i, 4) % 100 = 0
FROM range(7 * :rows // 8, :rows) t(i);

-- Every device belongs to one household of about three devices.
CREATE TABLE device_graph AS
SELECT d AS device_id, CAST(hash(d, 5) % greatest(:rows // 60, 1) AS BIGINT) AS household_id
FROM range(greatest(:rows // 20, 1)) t(d);

SELECT count(*) AS impressions, count(DISTINCT device_id) AS devices FROM impressions;
