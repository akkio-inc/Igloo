# Other engines: Spark, Trino, PyIceberg, Databricks, Snowflake

Igloo's tables are Apache Iceberg tables, and Igloo serves them to other
engines through the standard [Iceberg REST catalog
protocol](https://iceberg.apache.org/rest-catalog-spec/) on port 8181
(`IGLOO_ICEBERG_ADDR`). Engines sign in as Igloo users, Igloo's grants decide
what they may read and change, every request lands in Igloo's query history,
and engines get storage credentials that last an hour and reach one table's
files only. Spark, Trino and PyIceberg work with their stock Iceberg REST
settings.

```
 Spark / Trino / PyIceberg / Snowflake / Databricks
   │  1. POST /v1/oauth/tokens  client_id=<Igloo user>, client_secret=<password>
   │  2. Iceberg REST requests with the token
   ▼
 ┌─────────────────────────────┐  checked against users and grants,   ┌────────────┐
 │ igloo :8181                 │  recorded in the query history, then │ Lakekeeper │
 │ (Iceberg REST endpoint)     │ ───────────────────────────────────▶ │ (private)  │
 └─────────────────────────────┘                                      └────────────┘
   │  3. with a table: credentials for that table's files, read-only
   │     unless the user may change it (STS, one hour)
   ▼
 Engines read and write Parquet and Iceberg metadata in object storage directly
```

Lakekeeper stays unreachable to anything but Igloo, as before; engines never
learn its address, and never receive Igloo's own storage keys.

## Users for engines

Give each job or team its own user, with the built-in role `none` and the
grants it needs, exactly as for SQL clients:

```sql
CREATE USER spark_etl WITH PASSWORD 'a-long-random-password' ROLE none;
GRANT SELECT ON SCHEMA raw TO spark_etl;
GRANT ALL ON SCHEMA marts TO spark_etl;      -- SELECT, INSERT, UPDATE, DELETE, CREATE
```

Engines send the user name and password as the OAuth2 client ID and secret
(`credential=spark_etl:<password>` in Spark, Trino and PyIceberg), with the
same failed-login limits and login history as SQL logins. Jobs are better
off with a [service account](sso.md#service-accounts) and an Igloo access
token, which has no password to share and can be revoked on its own:

```sql
CREATE USER airflow TYPE SERVICE ROLE none;
GRANT SELECT ON SCHEMA marts TO airflow;
CREATE TOKEN nightly FOR USER airflow EXPIRES '90d';   -- shows igloo_... once
```

Engines then send the token as the client secret, with or without the user
name: `credential=airflow:igloo_...`, or `credential=igloo_...`, which
Spark, Trino and PyIceberg send as the secret alone.

Igloo answers with an access token of its own that lasts an hour, or until
the Igloo access token it was made from expires, whichever comes first;
clients renew it before then, with their credentials or by exchanging the
token. It stops working at once when its user is dropped or disabled, when
their password changes (for one made from a password), or when the access
token it was made from is dropped (`DROP TOKEN`). Grants are read again on
every request, so revoking one takes effect on the next request.
Credentials an engine already holds keep working until they expire, at most
an hour later.

What each request needs, by the user's built-in role or their grants, as the
matching SQL statement would:

| Request | Needs |
|---|---|
| List schemas and tables | Lists only what the user has some grant on (readers and above see everything) |
| Load a table | `SELECT`. The credentials that come with it are read-only unless the user also has `INSERT`, `UPDATE` and `DELETE` on the table: with write access to a table's files an engine can change any row. |
| Change rows: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, compaction, rollback | `INSERT`, `UPDATE` and `DELETE`. A commit names files, which Igloo does not read, so an "append" could as well bring back an older version of the table. Users with only `INSERT` or `DELETE` use those through Igloo's SQL. |
| Change a table's schema, partitioning or sort order | `CREATE` on its schema, as `ALTER TABLE` |
| Change table properties, expire snapshots | the `writer` role, as `set_iceberg_table_properties` |
| Create a table, `CREATE TABLE AS`, drop or rename one | `CREATE` on its schema |
| Create, drop or change a schema | the `writer` role |

Engines load a table before writing to it, so writing needs `SELECT` as well.
Loading a table that does not exist answers 404 whatever the user's grants,
since engines check that way before `CREATE TABLE AS`; table names are no
secret in Igloo, where SQL clients see every table's name. Dropping a table
drops its grants, and renaming one moves them, as in SQL. The engine's user
[owns](catalog.md#owners) the schemas and tables it creates, as in SQL.

Only admins may choose a table's or a schema's location, move a table to
another location or change its UUID (otherwise every table gets a new
directory of its own, so credentials for it reach no other table's files,
not even a dropped one's), use nested namespaces (Igloo's SQL shows
top-level schemas only, so grants could not name them), or move a table to
another schema (whose grants would then reach it). No one may set table
properties that write files outside the table (`write.data.path` and the
like), or use a dot in a namespace's name. The commit that creates a
table, as in `CREATE TABLE AS`, restates the UUID and location the catalog
gave it; Igloo leaves those out, so the table keeps them. A commit whose
body names another table than its path is refused. Views, registering
existing tables and multi-table transactions are not served yet.

### Tables with data policies

Igloo applies masking and row access policies to the SQL it runs. An engine
that loads a table gets its metadata and credentials for its files, and
reading those files directly would bypass every policy. So, as Unity
Catalog does for engines outside it, Igloo refuses everyone but admins
loading a table with a masking policy on any column or a row access
policy, refreshing its credentials, and committing to it, with 403 and a
message to read it through Igloo's SQL or Flight SQL. Creating or renaming
a table to the name of a protected table, or renaming a protected one, is
refused too, since policies name the tables they protect. Listings still
show such tables. Igloo checks for policies on every request, so attaching
one stops new loads at once; credentials an engine got for the table
before still work until they expire, at most an hour later.

Tags and [data retentions](privacy.md#data-retention) name tables and
columns too, for [erasure requests](privacy.md): no one, admins included,
may rename a table with policies, tags or a data retention, nor commit a
schema that renames or drops one of its tagged or protected columns,
changes its type, or adds another column under its name, as in SQL.

### Clones

A [clone](sql.md#clones) reads the data files and manifests of the table
it was cloned from where they are, under that table's location, so other
engines are kept from deleting them and from reading past them:

- **Purging.** Dropping a table whose files clones read with
  `purgeRequested=true` (Spark's `DROP TABLE ... PURGE`) is refused with
  409, naming the clones: the catalog would delete every file under the
  table's location. Drop the clones first, or drop the table without
  purging, which keeps its files, as SQL's `DROP TABLE` does then. A table
  in the warehouse whose files no clone reads is purged when it is
  dropped, with `purgeRequested` or without, as in SQL; [erasure
  requests](privacy.md#what-is-left-of-dropped-tables) delete whatever a
  drop leaves.
- **Credentials for a clone.** Loading a clone with vended credentials, or
  refreshing them, is refused with 403: credentials reach one table's
  location, which holds none of the files the clone reads from its source,
  and credentials that reached the source's too would read its newer rows
  and its history as well. Read clones through Igloo's SQL or Flight SQL;
  an engine with access of its own to the bucket may load one without
  asking for credentials.
- **Writing to the source.** Write credentials for a table whose files
  clones read can add files but not delete any, so an engine's snapshot
  expiry or orphan cleanup cannot remove files a clone still reads: the
  deletes fail (Iceberg's Spark procedures log them and go on), and Igloo's
  maintenance deletes the files once no clone needs them. When Igloo
  cannot tell whether clones read a table's files, its write credentials
  delete nothing, and a purge is refused with 503.
- **Commits** delete nothing a clone reads: the catalog deletes only the
  table's own old metadata files when it commits, and removing snapshots
  changes the metadata alone; the engine deletes the files, with
  credentials that cannot. The table properties `igloo.clone.*`, which
  record what a clone reads, cannot be set or removed through the catalog.

A clone of a table with data policies has the same policies, so only admins
may load it.

## Connecting

| Setting | Value |
|---|---|
| Catalog URI | `http://<igloo>:8181`, or `https://` when TLS is on |
| Warehouse | `igloo` (`IGLOO_CATALOG_WAREHOUSE`); clients may leave it out |
| Credential | `<Igloo user>:<password>`, or an access token: `igloo_...` or `<Igloo user>:igloo_...` |
| Token endpoint | `<catalog URI>/v1/oauth/tokens`, which clients use by default |
| Storage credentials | ask for them: `X-Iceberg-Access-Delegation: vended-credentials` |

Without TLS, the endpoint serves clients on private networks only
(loopback, link-local, 10/8, 172.16/12, 192.168/16, 100.64/10 and fc00::/7),
since passwords, tokens and storage credentials cross the connection. With
`IGLOO_TLS_CERT_FILE` and `IGLOO_TLS_KEY_FILE` set it is served over HTTPS
only, with the SQL port's certificate. The check sees the address of
whatever connects to Igloo: behind the scale-to-zero gateway, or a load
balancer that replaces client addresses with its own, that is a private
address, so turn on TLS whenever clients outside private networks can reach
the gateway or load balancer.

- **One machine (Docker Compose):** the endpoint is published on
  `127.0.0.1:8181` (`IGLOO_ICEBERG_PORT`, `IGLOO_ICEBERG_BIND`). With the
  bundled object store, table files live at `http://storage:9000` on the
  stack's Docker network, so run engines on that network
  (`docker run --network igloo_default ...`) and use
  `http://igloo:8181`. With Amazon S3 they can run anywhere.
- **Kubernetes:** `http://<release>.<namespace>.svc:8181` on the SQL Service
  (`iceberg.port`); `networkPolicy.sqlFrom` decides which pods reach it. With
  the bundled object store, list the engines' pods in
  `networkPolicy.objectStorageFrom` too, so they reach the store at
  `http://<release>-object-storage:9000`.
- **Scale to zero:** the gateway forwards the endpoint too
  (`IGLOO_GATEWAY_ICEBERG_BACKEND`); an engine's first request after Igloo
  stopped waits while it starts. See [scale to zero](scale-to-zero.md).

### Apache Spark

Tested with Spark 3.5.9 and Iceberg 1.11.0: reads, `INSERT`, `MERGE INTO`,
`CREATE TABLE AS`, refusals by grants (`scripts/engines-e2e.sh`).

```properties
spark.jars.packages                      org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0
spark.sql.extensions                     org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.igloo                  org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.igloo.type             rest
spark.sql.catalog.igloo.uri              https://igloo.example.com:8181
spark.sql.catalog.igloo.warehouse        igloo
spark.sql.catalog.igloo.credential       spark_etl:<password>
spark.sql.catalog.igloo.header.X-Iceberg-Access-Delegation  vended-credentials
```

```sql
SELECT * FROM igloo.marts.orders;
MERGE INTO igloo.marts.orders t USING updates s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
```

Table responses name the catalog's credentials endpoint
(`client.refresh-credentials-endpoint`), through which recent Iceberg Java
clients renew a long job's storage credentials before they expire; with
clients that do not, a job that runs longer than an hour should reload its
tables.

### Databricks

Databricks reaches Igloo over the network, so serve the endpoint over TLS at
an address its compute can reach. On classic compute in dedicated (single
user) access mode, add the Iceberg runtime and AWS bundle above as cluster
libraries and the `spark.sql.catalog.igloo.*` settings to the cluster's Spark
configuration; Databricks then uses Igloo's tables as Spark does. Where your
workspace offers Unity Catalog federation of Iceberg REST catalogs, create
the connection with the same catalog URI, warehouse, token endpoint, and the
Igloo user and password as OAuth client ID and secret, with vended
credentials. Neither has been tested against Igloo yet.

### Trino

Tested with Trino 483: reads, `INSERT`, `UPDATE`, `DELETE`, refusals by
grants. In `etc/catalog/igloo.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://igloo.example.com:8181
iceberg.rest-catalog.warehouse=igloo
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=trino:<password>
iceberg.rest-catalog.vended-credentials-enabled=true
fs.native-s3.enabled=true
s3.region=us-east-1
# S3-compatible stores only:
# s3.endpoint=http://storage:9000
# s3.path-style-access=true
```

Trino signs in as one Igloo user for all of its own users, so Igloo's grants
apply to the Trino catalog as a whole; give each Trino catalog the user whose
grants it should have.

### PyIceberg

Use PyIceberg 0.11 or later (tested with 0.12.0): 0.10 misreads the
manifests Igloo writes and so returns rows that Igloo's `DELETE`, `UPDATE`
or `MERGE` removed, until table maintenance compacts the table. PyIceberg
0.12 also fails to read a table while it holds delete files that Trino 483
wrote (pyarrow cannot decode their `file_path` column), until maintenance
compacts them away.

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog("igloo", uri="https://igloo.example.com:8181", warehouse="igloo",
                      credential="analyst:<password>")
orders = catalog.load_table("marts.orders").scan().to_arrow()
```

PyIceberg asks for storage credentials by default.

### Snowflake

Snowflake reads Igloo's tables through a catalog integration for Iceberg
REST. Snowflake connects from its own network, so the endpoint needs TLS
and an address Snowflake can reach. Not tested against Igloo yet.

```sql
CREATE CATALOG INTEGRATION igloo
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'marts'
  REST_CONFIG = (
    CATALOG_URI = 'https://igloo.example.com:8181'
    CATALOG_NAME = 'igloo'
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_TOKEN_URI = 'https://igloo.example.com:8181/v1/oauth/tokens'
    OAUTH_CLIENT_ID = 'snowflake_reader'
    OAUTH_CLIENT_SECRET = '<password>'
    OAUTH_ALLOWED_SCOPES = ('catalog')
  )
  ENABLED = TRUE;

CREATE ICEBERG TABLE orders CATALOG = 'igloo' CATALOG_TABLE_NAME = 'orders' AUTO_REFRESH = TRUE;
```

## Storage credentials

When an engine asks for them, Igloo hands it credentials for the table it
loads or creates: they last an hour, reach only the files under that table's
location, and read only, unless the user may change every row. They are never
Igloo's own keys. Only a table where the catalog put it, in its schema's
location in a directory named for its UUID, gets them: anywhere else, its
directory could hold another table's files, or lie above or below another
table's. Tables an admin placed elsewhere get credentials for admins only.
[Clones](#clones) get none, and writers of a table whose files clones read
get credentials that delete nothing.

Credentials cannot be taken back. Revoking a grant, dropping a user or
attaching a data policy stops new loads and new credentials at once, but
credentials an engine already holds keep working until they expire, at most
an hour later.

How Igloo makes them depends on the storage:

### Amazon S3

Igloo assumes `IGLOO_S3_STS_ROLE_ARN` with a session policy that allows
`s3:GetObject` (and `PutObject`, `DeleteObject` and multipart uploads, for
writers; no `DeleteObject` for a table whose files clones read) on
`<table location>/*` and listing that prefix, nothing else. The
role must allow the warehouse prefix and trust Igloo's own identity (its IAM
role or user); the session policy narrows it to the table. The
[AWS module](../deploy/aws/README.md) creates the role, `<name_prefix>-engines`;
elsewhere, create one like this:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": "arn:aws:iam::123456789012:role/igloo"},
    "Action": "sts:AssumeRole"
  }]
}
```

with a policy allowing `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`,
`s3:AbortMultipartUpload` and `s3:ListMultipartUploadParts` on
`arn:aws:s3:::<bucket>/<prefix>/*`, and `s3:ListBucket` on the bucket for
that prefix (plus `kms:Decrypt` and `kms:GenerateDataKey` on the bucket's
key, if it has one), and allow Igloo's identity `sts:AssumeRole` on it. AWS
caps sessions of a role assumed by another role at an hour. CloudTrail shows
each session as `igloo-<user>`.

Without `IGLOO_S3_STS_ROLE_ARN`, engines get table metadata but no
credentials; they then need access of their own to the bucket.

### S3-compatible stores

MinIO, RustFS, Ceph and similar stores serve STS on their own endpoint: Igloo
calls `AssumeRole` there with its own keys and the same session policy, which
the store applies to them. A store that accepted the policy but ignored it
would hand out Igloo's whole access, so before the first credentials leave,
Igloo checks once that credentials for a scratch prefix are refused outside
it, and hands out none if they are not. The store's endpoint
(`IGLOO_S3_ENDPOINT`) is what engines are told, so it must be reachable from
them.

### Google Cloud Storage and Azure Data Lake Storage

Igloo does not hand out credentials for `gs://` and `abfss://` warehouses
yet: engines get table metadata only, and need access of their own to the
files. Igloo's grants then still decide what the catalog shows and lets them
change, but not what their own access can read in the bucket. The
equivalents, not built yet, are:

- **Google Cloud Storage:** a downscoped OAuth token. Igloo exchanges its
  service account's token at Google's STS for one limited by a Credential
  Access Boundary to `roles/storage.objectViewer` (or `objectAdmin` for
  writers) with the condition
  `resource.name.startsWith('projects/_/buckets/<bucket>/objects/<table prefix>/')`,
  and hands it out as `gcs.oauth2.token` with its expiry.
- **Azure Data Lake Storage Gen2:** a user delegation SAS for the table's
  directory (`sr=d`), with permissions `rl` (or `racwdl` for writers), an
  hour long, signed with a user delegation key that Igloo's managed identity
  obtains, handed out as `adls.sas-token.<account>.dfs.core.windows.net`.

## History

Every catalog request goes to `igloo.query_history` like a statement. The
operation is the statement type, the request and what it got the text, and
the tables it read or changed are listed; the application name is
`iceberg-rest`, with the engine's client version:

```sql
SELECT start_time, user_name, statement_type, outcome, query_text, tables_read, tables_written
FROM igloo.query_history
WHERE application_name LIKE 'iceberg-rest%'
ORDER BY start_time DESC;
```

```
 user_name | statement_type | outcome | query_text
 spark_etl | loadTable      | ok      | GET /v1/namespaces/marts/tables/orders (read-write credentials)
 spark_etl | updateTable    | ok      | POST /v1/namespaces/marts/tables/orders (add-snapshot (overwrite), set-snapshot-ref)
 analyst   | loadTable      | denied  | GET /v1/namespaces/finance/tables/payroll
```

Token requests are logins, in `igloo.login_history` with application name
`iceberg rest` (and `auth_method` `token` and the token's `token_name` for
access tokens). Prometheus counts requests in `igloo_iceberg_requests_total`, by
operation and outcome.

## Why Igloo serves the catalog itself

Igloo answers the Iceberg REST protocol itself and passes checked requests on
to Lakekeeper, rather than opening Lakekeeper to engines. Lakekeeper can
authenticate and authorize requests itself, but only with tokens from an
OpenID Connect provider and with permissions in OpenFGA, a separate service
with its own database (its policy-based authorizer is a commercial add-on).
Igloo would have had to become an identity provider, copy every grant into
OpenFGA and keep the two in step, and engine access would be missing from
Igloo's history: more moving parts for operators to run and watch
([principles](principles.md) 2 and 4), and two sources of truth for who may
read what. Lakekeeper's remote request signing, which needs no cloud role,
was not used either: Trino, Snowflake and Databricks do not support it, and
without an authorizer Lakekeeper signs writes for readers too.

## Limitations

- Storage credentials on Amazon S3 and S3-compatible stores only (see above).
- Writing from an engine needs `INSERT`, `UPDATE` and `DELETE` on the table;
  narrower grants apply through Igloo's SQL only.
- A snapshot committed to a table in the warehouse must have its manifest
  list in the warehouse, written before the commit, as engines write it
  under the table's location: one that is not there, or is outside the
  warehouse, is refused with 400, since no engine, and no [erasure
  request](privacy.md), could read the snapshot.
- Credentials outlive a revoked grant by up to an hour.
- Views, table registration and multi-table transactions are not served.
- Trino and other shared engines sign in as one Igloo user each; Igloo cannot
  tell their own users apart.
