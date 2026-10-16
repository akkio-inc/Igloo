-- Generates TPC-H benchmark data and stores it as Iceberg tables in the
-- default schema. Scale factor 1 (the default) is about 1 GB of raw data and
-- 6 million lineitem rows. Pick another with: -v sf=10
\if :{?sf}
\else
  \set sf 1
\endif

\timing on

-- Scratch database for the generator. Attachments are shared by every
-- session, so clear out any left by an interrupted run.
DETACH DATABASE IF EXISTS tpch_gen;
ATTACH ':memory:' AS tpch_gen;
CALL dbgen(sf = :sf, catalog = 'tpch_gen', schema = 'main');

DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS lineitem;

CREATE TABLE region AS SELECT * FROM tpch_gen.main.region;
CREATE TABLE nation AS SELECT * FROM tpch_gen.main.nation;
CREATE TABLE supplier AS SELECT * FROM tpch_gen.main.supplier;
CREATE TABLE customer AS SELECT * FROM tpch_gen.main.customer;
CREATE TABLE part AS SELECT * FROM tpch_gen.main.part;
CREATE TABLE partsupp AS SELECT * FROM tpch_gen.main.partsupp;
CREATE TABLE orders AS SELECT * FROM tpch_gen.main.orders;
CREATE TABLE lineitem AS SELECT * FROM tpch_gen.main.lineitem;

DETACH tpch_gen;

SELECT count(*) AS lineitem_rows FROM lineitem;
