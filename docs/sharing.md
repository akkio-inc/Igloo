# Delta Sharing

[Delta Sharing](https://github.com/delta-io/delta-sharing) is an open
protocol for sharing tables across organizations. Igloo speaks it both ways:

- **As a provider**, Igloo shares its tables with clients and partners, who
  read them with any Delta Sharing client: Databricks and Spark, pandas,
  Power BI, Tableau. They need no Igloo account and no access to your cloud.
- **As a consumer**, Igloo reads a partner's share, such as data delivered
  by a TV or measurement partner, as external tables that SQL queries and
  grants treat like any other table.

Both are managed in SQL by admins, and both need the metadata database
(`IGLOO_METADATA_DATABASE_URL`) and the Iceberg catalog.

## Sharing tables with recipients

A share is a named group of tables. A recipient is an outside party with a
bearer token. Grant a share to a recipient and it can read the share's
tables, and nothing else.

```sql
CREATE SHARE acme_reporting COMMENT 'Campaign results for Acme';
ALTER SHARE acme_reporting ADD TABLE marts.campaign_daily;
ALTER SHARE acme_reporting ADD TABLE marts.acme_spend AS reporting.spend WITH HISTORY;

CREATE RECIPIENT acme COMMENT 'Acme analytics team';
GRANT SELECT ON SHARE acme_reporting TO RECIPIENT acme;
```

`CREATE RECIPIENT` returns an activation link:

```
 recipient |                        activation_link                         |    link_expires_at
-----------+----------------------------------------------------------------+------------------------
 acme      | https://data.example.com:8444/delta-sharing/activate/dsa_9f... | 2026-10-23 12:06:40+00
```

Send the link to the recipient. It works once, for seven days: opening it
shows a page with a button that downloads `acme.share`, the profile file
every Delta Sharing client reads. The bearer token is created at that
moment and goes only into that download; Igloo keeps only its hash, and
neither you nor the SQL history ever see it. Preview bots in chat and mail
tools fetch links but do not press buttons, so they cannot use the link up.
To download from a terminal, `curl -X POST -o acme.share <link>`.

| Statement | Does |
|---|---|
| `CREATE SHARE [IF NOT EXISTS] name [COMMENT 'text']` | Create an empty share |
| `ALTER SHARE name ADD [TABLE] table [AS [schema.]alias] [WITH HISTORY]` | Add a table. Recipients see it as `share.schema.table`: its own schema and name, or the alias. `WITH HISTORY` lets them read earlier versions. |
| `ALTER SHARE name REMOVE [TABLE] [schema.]alias` | Remove a table, by the name recipients see |
| `DROP SHARE [IF EXISTS] name` | Drop a share; its recipients lose it at once |
| `SHOW SHARES`, `SHOW ALL IN SHARE name`, `SHOW GRANTS ON SHARE name` | List shares, a share's tables, and its recipients |
| `CREATE RECIPIENT [IF NOT EXISTS] name [COMMENT 'text'] [TOKEN LIFETIME '90 days']` | Create a recipient and its activation link. Its token expires a year after activation unless `TOKEN LIFETIME` says otherwise. |
| `ALTER RECIPIENT name ROTATE TOKEN [KEEP OLD FOR '1 day']` | Issue a new activation link. The current token stops working at once, or after `KEEP OLD FOR`, so the recipient can switch without a gap. |
| `DROP RECIPIENT [IF EXISTS] name` | Drop a recipient; its token stops working at once |
| `SHOW RECIPIENTS` | Recipients, their shares, and their token's state and expiry |
| `GRANT SELECT ON SHARE share TO RECIPIENT name`, `REVOKE SELECT ON SHARE share FROM RECIPIENT name` | Give or take a share |

Share, recipient and provider names are lowercase letters, digits and
underscores. Only admins run these statements.

### The sharing endpoint

Recipients reach Igloo's sharing endpoint on port 8444
(`IGLOO_SHARING_ADDR`), at the path `/delta-sharing`. Bearer tokens and data
cross that connection, so it needs TLS: with `IGLOO_TLS_CERT_FILE` and
`IGLOO_TLS_KEY_FILE` set it serves HTTPS with the same certificate as SQL
clients, and without them it answers only requests addressed to `localhost`.

Set `IGLOO_SHARING_URL` to the address recipients use, such as
`https://data.example.com:8444`; activation links and profile files name it.
Without it, profile files name the address the recipient activated at, and
activation links name `localhost`.

The single-machine stack publishes the endpoint on `127.0.0.1:8444`. To
share with the outside world, turn on TLS, set `IGLOO_SHARING_BIND=0.0.0.0`
and `IGLOO_SHARING_URL` in `.env`, and open the port. The Helm chart does
not publish the endpoint yet: its Service and network policy expose only
the SQL, console and metrics ports, so the endpoint is unreachable from
outside the pod until you add port 8444 to both. Put a load balancer that
passes TLS through in front of it; one that terminates TLS and forwards
plain HTTP is refused, as the console's sign-ins are.

### What recipients receive

Igloo translates each Iceberg table into the protocol's terms:

- **Versions** are the Iceberg snapshot's sequence number, which grows by
  one with every write. With `WITH HISTORY`, recipients can read any
  version, or the version current at a time, that table maintenance has
  not yet expired (`IGLOO_SNAPSHOT_RETENTION`, 7 days by default).
- **Files** are the snapshot's Parquet data files, each with its row count
  and the minimum and maximum of its numeric, date and timestamp columns,
  from the Iceberg manifests. Iceberg's partitioning is hidden, so shared
  tables have no partition columns: every column is in the files.
- **Deleted rows.** Igloo writes `DELETE`, `UPDATE` and `MERGE` as Iceberg
  delete files, which Delta readers do not apply. A data file with deletes
  is therefore served as a copy with them applied, written once to the
  warehouse's `.igloo/sharing/` directory the first time a recipient asks
  for it, and deleted two days after no current version needs it. A query
  after a large delete can take a while for that reason; recipients never
  see deleted rows.
- **Types** map as follows. A table with a column of another type cannot be
  shared: `ALTER SHARE ... ADD TABLE` says which column.

  | Iceberg (DuckDB) | Delta |
  |---|---|
  | `boolean` | `boolean` |
  | `int` (`INTEGER`), `long` (`BIGINT`) | `integer`, `long` |
  | `float`, `double` | `float`, `double` |
  | `decimal(p, s)` | `decimal(p,s)` |
  | `date` | `date` |
  | `timestamp`, `timestamptz` | `timestamp` (values without a time zone are read as UTC) |
  | `string` (`VARCHAR`) | `string` |
  | `binary`, `fixed`, `uuid` | `binary` |
  | `struct`, `list`, `map` | `struct`, `array`, `map` |
  | `time`, nanosecond timestamps, `variant` | not shared |

- **Renamed and dropped columns.** Delta readers match a file's columns by
  name, and recipients download whole data files. A column renamed after
  files were written would read as empty, and a dropped column's values
  would still be in older files, readable by anyone with the file. Igloo
  therefore refuses to serve a table whose columns were renamed or dropped,
  and says so; rewrite it to share it, with
  `CREATE OR REPLACE TABLE t AS SELECT * FROM t`, which keeps its grants and
  shares. Added columns are fine.
- Change data feeds (`table_changes`) and streaming are not offered.
  Clients that ask only for the `delta` response format get it.

### Recipient instructions

Send recipients the activation link and these instructions for their tool.
Each needs the profile file or, for BI tools, the endpoint and bearer token
in it.

**pandas** (the official client, `pip install delta-sharing`):

```python
import delta_sharing

profile = "acme.share"
print(delta_sharing.SharingClient(profile).list_all_tables())
df = delta_sharing.load_as_pandas(profile + "#acme_reporting.marts.campaign_daily")
```

**Spark and Databricks.** With the
[delta-sharing Spark connector](https://github.com/delta-io/delta-sharing#apache-spark-connector):

```python
df = spark.read.format("deltaSharing").load("/path/to/acme.share#acme_reporting.marts.campaign_daily")
```

In a Databricks workspace, the credential file can also be imported as a
provider for open sharing; the shared tables then appear in the catalog.

**Power BI and Tableau** have Delta Sharing connectors: enter the profile's
`endpoint` as the server URL and its `bearerToken` as the token.

### Watching recipients

Every request a recipient makes is in the [query history](sql.md#query-history),
with `user_name` `recipient:<name>`, `role` `recipient`, `statement_type`
`DELTA SHARING`, the request in `query_text` and the table it read in
`tables_read`; admins see them. Requests with a wrong or expired token are in
`igloo.login_history`. Downloads are counted in the
`igloo_sharing_file_bytes_total` metric.

```sql
SELECT start_time, user_name, query_text, tables_read, client_address
FROM igloo.query_history WHERE role = 'recipient' ORDER BY start_time DESC;
```

## Reading partners' shares

A partner sends a profile file. Register it as a provider, then map the
shared tables you want to external tables in one of the catalog's schemas:

```sql
-- In psql: \set profile `cat tvco_ads.share`
CREATE PROVIDER tvco_ads FROM :'profile';
SHOW SHARES IN PROVIDER tvco_ads;          -- share, schema and table names

CREATE SCHEMA partners;
CREATE EXTERNAL TABLE partners.tvco_viewership FROM SHARE tvco_ads.tv_share.viewing.daily_viewership;

SELECT program, sum(minutes) FROM partners.tvco_viewership GROUP BY ALL;
GRANT SELECT ON TABLE partners.tvco_viewership TO analysts;
```

| Statement | Does |
|---|---|
| `CREATE [OR REPLACE] PROVIDER [IF NOT EXISTS] name FROM 'profile file contents'` | Register a partner's profile, after checking that its token works. `OR REPLACE` installs a new profile, as when the partner rotates the token. |
| `DROP PROVIDER [IF EXISTS] name` | Forget a provider; drop its external tables first |
| `SHOW PROVIDERS` | Providers, their endpoints and when their tokens expire |
| `SHOW SHARES IN PROVIDER name` | The tables the provider shares with you |
| `CREATE EXTERNAL TABLE [IF NOT EXISTS] [schema.]name FROM SHARE provider.share.schema.table` | Map a shared table to a name. The schema must exist. |
| `DROP EXTERNAL TABLE [IF EXISTS] [schema.]name` | Drop an external table, and its grants |
| `SHOW EXTERNAL TABLES` | External tables, and the version each last read |

External tables are read-only. Query them by any name, `lake.partners.t` or
`partners.t` (or `t` in `public`); SQL clients list them as views of the
database `memory`, where each replica keeps them. Grants work as for any
table, and users with role `none` need `SELECT` on an external table to
read it.

Each statement that names an external table reads the shared table's
current version: Igloo asks the provider for the version at most every 15
seconds, and lists its files again when it changed. A table read only
through a view is kept current while it is in use.

How it works, and its limits:

- Igloo asks for the protocol's `parquet` response format, which every
  sharing server supports. Tables a provider can share only in the `delta`
  format (with deletion vectors or column mapping) cannot be read until
  the provider shares them without those features.
- Profiles with a bearer token are supported; OAuth client-credential
  profiles are not yet.
- The provider's endpoint must use HTTPS, unless it is on this machine or
  a private network (a loopback or private address, or a host name without
  dots, such as a Docker or Kubernetes service).
- Files are read when queries need them, as DuckDB reads the warehouse,
  through a proxy inside Igloo. Nothing is copied ahead of time.

## Security

- **Recipients' tokens** are 256 random bits. Igloo stores only their
  SHA-256 hash and compares it in constant time. Tokens expire (a year after
  activation by default), and rotating or dropping a recipient ends theirs
  on every replica within ten seconds, and at once on the replica that ran
  the statement. Activation links work once and expire after seven days.
- **Transport.** The endpoint serves HTTPS with Igloo's certificate, and
  without one answers only requests to `localhost`.
- **Data files** are downloaded from URLs that Igloo signs and serves
  itself. Each names one file, for one recipient's token, for an hour; its
  path is encrypted, so recipients never learn bucket names or storage
  layout, and they never get storage credentials. Every download checks
  that the token is still valid and the table still shared with the
  recipient, so revoking ends downloads in progress too. Igloo serves only
  files inside the warehouse.
- **Least exposure.** Recipients see only shares granted to them; shares
  that exist but are not granted look like shares that do not exist. Time
  travel is off unless a table is added `WITH HISTORY`. Tables whose dropped
  columns may linger in their files are refused (see above).
- **Providers' tokens** are stored encrypted with a key kept in the
  metadata database, never shown again, and removed from the query history
  with the rest of `CREATE PROVIDER`.
- **The query sandbox** gains one address: a proxy on the loopback
  interface, under a random path, that serves only files a provider listed
  for an external table. SQL cannot reach any other URL through it. The
  proxy does not connect to loopback, link-local (cloud metadata) or
  multicast addresses, other than the provider's own endpoint, so a hostile
  provider cannot point Igloo at internal services, and it never forwards
  the bearer token to a redirect.
- **History.** Every recipient request is recorded, and failed tokens in
  the login history.
