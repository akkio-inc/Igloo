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

### Tables with masking or row access policies

[Masking and row access policies](security-policies.md) apply to what
Igloo reads for its users, evaluated for their roles. Recipients download a
table's data files and read them with their own tools, which cannot apply
Igloo's policies, and they have no Igloo roles to evaluate them for. So, as
Databricks does for open sharing, a table that has a row access policy, or a
masking policy on any column (set on the column or through a tag), is never
shared:

- `ALTER SHARE ... ADD TABLE` refuses it, naming the policies. A tag
  without a masking policy does not count.
- A shared table that gains a policy is refused to recipients from then on,
  at once on the replica that attached the policy and within 30 seconds on
  the others: its version, metadata and files, and every download of a file
  listed before. Recipients get `403 PERMISSION_DENIED`, saying the table
  is protected by the provider's policies; they are not told which. The
  share still lists the table, and its other tables are served as before.
  Removing the policy serves the table again.
- The query history records each refused request as the recipient's, with
  `outcome` `denied`, `sqlstate` `42501` and the policies in `policies`:

  ```sql
  SELECT start_time, user_name, query_text, policies FROM igloo.query_history
  WHERE role = 'recipient' AND policies IS NOT NULL ORDER BY start_time DESC;
  ```

- The copies of the table's files with deletes applied are deleted at the
  next collection, within six hours. Until then they are in the warehouse,
  whose files users other than admins cannot read directly while tables
  have policies.

To share what a recipient may see of a protected table, make a table of
just that and share it. Statements read protected tables through their
policies, admins' too unless a policy exempts them, so the new table holds
what its creator sees; it has no policies of its own. A materialized view
(see [Loading and scheduling](loading.md#materialized-views)) keeps it
current:

```sql
CREATE MATERIALIZED VIEW shared.acme_contacts SCHEDULE '1 hour' AS
  SELECT id, client_id, sha256(email) AS email_hash FROM crm.contacts WHERE client_id = 'acme';
ALTER SHARE acme_reporting ADD TABLE shared.acme_contacts;
```

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
read it. The [data catalog](catalog.md#external-tables-and-connections)
lists them as `EXTERNAL TABLE`s with their provider, owned by the admin
who created them, and its lineage records statements that read them;
shares and recipients stay out of it.

Each statement that names an external table reads the shared table's
current version: Igloo asks the provider for the version at most every 15
seconds, and lists its files again when it changed. A table read only
through a view is kept current while it is in use.

How it works, and its limits:

- **Response formats.** A provider answers in the protocol's `parquet`
  format, which every sharing server speaks, or in its `delta` format,
  which carries Delta features the `parquet` format cannot. Igloo offers
  both and reads the files in the one the provider picks for the table.
  Databricks picks `delta` for every table with deletion vectors enabled,
  even when nothing was deleted, and a workspace can enable them on every
  new table. In the `delta` format Igloo applies:
  - **Deletion vectors**, which mark rows deleted from a data file that
    still holds them. Igloo downloads a table's vectors when it lists its
    files, and the external table leaves those rows out. A query holds the
    deleted rows of the files it reads in memory, and DuckDB spills them
    to disk when they do not fit; files without deletion vectors are read
    as they are. A table whose vectors delete more than 20 million
    separate runs of rows is refused; the provider can purge them with
    `REORG TABLE ... APPLY (PURGE)`. Only external tables apply the
    vectors: a data file read directly through the proxy (`read_parquet`)
    still has its deleted rows.
  - **Column mapping**, modes `name` and `id`, under which the files name
    columns differently from the table. Igloo reads them by the files'
    names and shows the table's, nested fields too. In mode `id`, Igloo
    checks that every file's field IDs agree with those names, and refuses
    a file where they do not.
  - **`timestamp_ntz`** columns, as `TIMESTAMP`.

  A table that needs another Delta reader feature, such as type widening
  or variants, is refused, and the error names the feature.
- **Columns.** An external table has the shared table's columns, in its
  order, partition columns too. A column that only some files hold reads
  as NULL in the others; columns the files hold that the table does not
  have, such as dropped columns under column mapping or Delta's row
  tracking, are left out.
- Profiles with a bearer token are supported; OAuth client-credential
  profiles are not yet.
- The provider's endpoint must use HTTPS, unless it is on this machine or
  a private network (a loopback or private address, or a host name without
  dots, such as a Docker or Kubernetes service).
- Data files are read when queries need them, as DuckDB reads the
  warehouse, through a proxy inside Igloo. Nothing but deletion vectors is
  downloaded ahead of time.
- **Policies.** Masking and row access policies and tags attach only to
  the catalog's tables, so `ALTER TABLE` refuses to attach them to an
  external table and says so; to protect a partner's rows, copy what users
  may read into a table (`CREATE TABLE ... AS SELECT ... FROM` the external
  table, or a materialized view) and attach policies to that. An external
  table cannot be created under a name that still has policies or tags,
  left by a dropped table. External tables stay readable while other tables
  have policies, and statements that join them with protected tables read
  those through their policies as usual. While any table has policies,
  users other than admins cannot read external tables' files through the
  proxy directly (`read_parquet` and the like, `COPY ... FROM`, `COPY
  INTO`), as they cannot read the warehouse's.

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
- **Policies.** Tables with masking or row access policies are never
  shared or served, and downloads of their files listed before a policy was
  attached are refused ([above](#tables-with-masking-or-row-access-policies)).
  The endpoint serves no table until Igloo has loaded the policies.
- **Tokens apart.** Recipients' tokens (`dst_...`) and activation codes
  (`dsa_...`) are kept apart from Igloo's access tokens (`igloo_...`): they
  have different forms and are stored and checked separately. A
  recipient's token signs in to nothing else (SQL, Flight SQL, the web
  console, SCIM), and an Igloo access token reads no share. The one other
  thing a recipient's token reaches is the API of the
  [clean rooms](clean-rooms.md#partners) its recipient takes part in, at
  `/clean-rooms` on this endpoint.
- **Clean rooms.** A clean room's datasets and result tables are never
  shared or served, as tables with policies are not: recipients get a
  room's results through its API instead.
- **Providers' tokens** are stored encrypted with a key kept in the
  metadata database, never shown again, and removed from the query history
  with the rest of `CREATE PROVIDER`.
- **The query sandbox** gains one address: a proxy on the loopback
  interface, under a random path, that serves only files a provider listed
  for an external table, and the lists of deleted rows Igloo makes from
  their deletion vectors. SQL cannot reach any other URL through it. The
  proxy, and Igloo when it downloads deletion vectors, do not connect to
  loopback, link-local (cloud metadata) or multicast addresses, other than
  the provider's own endpoint, so a hostile provider cannot point Igloo at
  internal services; neither sends the bearer token with a file download
  or to a redirect.
- **History.** Every recipient request is recorded, and failed tokens in
  the login history.
