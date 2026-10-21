# Connections to other databases

Igloo can query PostgreSQL, MySQL and Snowflake databases where they are,
next to its Iceberg tables: a client's CRM, an operational database, a
warehouse being migrated. It reads the Iceberg tables of other Iceberg REST
catalogs in place the same way: Snowflake-managed Iceberg tables,
Databricks shares, another team's Polaris (see [Iceberg REST
catalogs](#iceberg-rest-catalogs)). An admin registers a connection once;
every replica attaches it, read-only, as a catalog of the same name, and
anyone granted its tables queries them like any other:

```sql
CREATE CONNECTION crm TYPE postgres
  WITH (host 'crm.db.example.com', database 'crm', user 'igloo_ro', password 'a-long-password');

-- after the next restart of each replica (see below)
GRANT SELECT ON SCHEMA crm.public TO analysts;

SELECT a.segment, count(*)
FROM lake.public.audiences a
JOIN crm.public.customers c ON c.email = a.email
WHERE c.region = 'east'
GROUP BY ALL;
```

Nothing is copied: each query reads the rows it needs from the remote
database, with filters and column lists pushed down to it. To keep a copy,
use `CREATE TABLE ... AS SELECT` from the connection.

## Supported databases

| `TYPE` | Databases | Through |
|---|---|---|
| `postgres` | PostgreSQL, and managed services that run it (Amazon RDS and Aurora, Google Cloud SQL, Azure Database). Tested with PostgreSQL 17. | DuckDB's `postgres` extension |
| `mysql` | MySQL 5.7 and later. Tested with MySQL 8.4. MariaDB speaks the same protocol (`TYPE mariadb` is the same) but is not tested yet. | DuckDB's `mysql` extension |
| `snowflake` | Snowflake, on any cloud and region. Its tables, views, external, dynamic and Iceberg tables. | Igloo's gateway to Snowflake, with Snowflake's Go driver (see [Snowflake](#snowflake)) |
| `iceberg` | Iceberg REST catalogs: Snowflake's Horizon Catalog, Databricks' Iceberg shares and Unity Catalog, Apache Polaris and Snowflake Open Catalog, AWS Glue and S3 Tables, Lakekeeper. Tested with a Databricks share, Snowflake's Horizon Catalog (signing in) and Lakekeeper 0.14. | DuckDB's `iceberg` extension, as for Igloo's own catalog (see [Iceberg REST catalogs](#iceberg-rest-catalogs)) |

The extensions are DuckDB's own core extensions, built for linux/amd64 and
linux/arm64 and baked into the image like the others; Igloo loads them only
when a connection needs them. The extensions are MIT-licensed. The
`postgres` extension links libpq (PostgreSQL License); the `mysql` extension
links MariaDB Connector/C, which is LGPL-2.1. Snowflake's Go driver,
[gosnowflake](https://github.com/snowflakedb/gosnowflake), is under the
Apache License 2.0.

MySQL users must sign in with `caching_sha2_password` (the default since
MySQL 8), `mysql_native_password` or `sha256_password`. MariaDB's `ed25519`
and the PAM and GSSAPI methods are not supported.

**Not supported yet.** BigQuery has a DuckDB community extension, but it
signs in with Google's credentials and talks to BigQuery from inside
DuckDB, where Igloo cannot keep the credentials out of SQL's reach or
enforce timeouts the way it does for connections (see
[how it works](#how-it-works)); Snowflake's community extension, which
Igloo does not use for the same reasons, is described under
[Snowflake](#how-igloo-reads-snowflake). Until BigQuery is supported, copy
its data into Iceberg tables, or share it as Iceberg and read it in place
with a connection of `TYPE iceberg`. SQLite databases are files on the
server, which the [sandbox](../SECURITY.md) keeps SQL away from.

## Creating, changing and dropping connections

Only admins manage connections.

```sql
CREATE CONNECTION [IF NOT EXISTS] name TYPE postgres | mysql | snowflake | iceberg WITH (option value, ...);
ALTER CONNECTION name SET (option value, ...);
ALTER CONNECTION name REFRESH;
DROP CONNECTION [IF EXISTS] name;
SHOW CONNECTIONS;
```

Options may be written `option value` or `option = value`; values are
strings, numbers or words. A connection's name is a catalog name in SQL, so
it may not be one DuckDB has already, nor that of a schema of the Iceberg
catalog: `crm.contacts` would then mean two things, which DuckDB refuses.
For the same reason, do not create a schema of the catalog named like a
connection.

| Option | Default | Meaning |
|---|---|---|
| `host` | (required) | Host name or IP address of the database server |
| `port` | `5432`, `3306` | Its port |
| `database` (`dbname`) | (required) | The database. For MySQL it is the default schema; every database the user may see is attached. |
| `user` | (required) | The database user Igloo signs in as. Give it read access only to what Igloo should see. |
| `password` | | Its password, stored encrypted (see [credentials](#credentials)) |
| `password_secret` | | Instead of `password`: the name of a file in `IGLOO_CONNECTION_SECRETS_DIR` holding it, read at every sign-in |
| `tls` (`sslmode`, `ssl_mode`) | `verify-full` | How Igloo makes sure it reaches the right server; see [TLS](#tls). MySQL's names (`disabled`, `preferred`, `required`, `verify_identity`) work too. |
| `tls_ca` | the system's | PEM certificates of the authorities that may sign the server's certificate |
| `schema` | all | Comma-separated schemas to attach, for large databases or to expose only some |
| `connect_timeout` | `10s` | How long connecting and signing in may take |
| `query_timeout` | `5m` | How long the remote database may work on one statement (its `statement_timeout`, `max_execution_time` or `max_statement_time`) and how long it may leave Igloo waiting for the next part of a result before Igloo gives up; `0` for no limit |
| `read_only` | `true` | Connections are read-only; `false` is refused |

Snowflake connections take the options listed under
[Snowflake](#snowflake) instead, and Iceberg REST catalogs
[their own](#options-of-iceberg-rest-catalogs).

`CREATE CONNECTION` checks that Igloo can sign in before it stores
anything. `SHOW CONNECTIONS` lists every connection's settings, never its
password (`password` shows `stored`, `secret <name>` or `none`, for
Snowflake `private key stored` or `private key in secret <name>`, and for an
Iceberg REST catalog each credential it has), its type's own settings
(`settings`: a Snowflake connection's account, warehouse and role, an
Iceberg REST catalog's scope, region and storage endpoint), and its state
on the replica that answers: `attached`, `not attached` with the reason, or
`failed` with the error.

### TLS

By default (`verify-full`) Igloo connects only over TLS, and only to a
server whose certificate is signed by an authority the system trusts, or
one in `tls_ca`, and issued for `host`: no one on the network path can
read the password or the data, or pose as the server. Managed databases
need their provider's authority, such as Amazon RDS's certificate bundle,
in `tls_ca`. The other modes are there to be chosen explicitly, and
`CREATE` and `ALTER CONNECTION` answer every choice of one with a warning
(SQLSTATE `01000`, which psql prints as `WARNING:`):

| `tls` | Encrypted | The server's certificate |
|---|---|---|
| `verify-full` | always | signed by a trusted authority, for `host` |
| `verify-ca`, `require` | always | signed by a trusted authority, for any host: another server with a certificate from that authority could pose as it |
| `prefer` | when the server offers TLS, else not | not checked: anyone on the path could pose as the server |
| `disable` | never | none |

Unlike libpq, `require` does not take any certificate: it checks the chain
as `verify-ca` does, against `tls_ca` or the system's authorities. For a
server with a self-signed certificate, put that certificate in `tls_ca`
and keep `verify-full` if it names the host, or use `verify-ca`.

### When changes take effect

DuckDB attaches databases only until Igloo locks its sandbox at startup
(no SQL can attach anything after that, which is what keeps it from
reaching other servers). So:

- **A new connection** is attached by each replica when it next starts.
  Restart the replicas after `CREATE CONNECTION`: `docker compose restart
  igloo` on one machine, `kubectl rollout restart deployment/igloo` on
  Kubernetes. A replica scaled to zero attaches it when it resumes. A
  replica of an older version leaves out a connection of a type it does
  not know, such as `snowflake`; upgrade every replica before creating one.
- **Host, port, user, password, TLS and timeouts** (`ALTER CONNECTION ...
  SET`) apply at once on the replica that ran it and within 15 seconds on the
  others, without a restart. Sessions with the database are signed in again
  with the new settings once they finish what they are doing; running
  queries are not interrupted.
- **The database and schemas** apply when each replica restarts;
  `SHOW CONNECTIONS` says so until then.
- **Of an Iceberg REST catalog**, the token, credential, scope, token
  endpoint, region, TLS and timeouts apply at once, like a password; the
  endpoint, warehouse, schemas and storage settings when each replica
  restarts.
- **Dropping** a connection detaches it at once on the replica that ran it
  and within 15 seconds on the others, and removes its grants.

Replicas list a database's tables once, when they attach it, and serve
`information_schema`, `pg_catalog` and psql's `\dt` from that list. A
PostgreSQL table created since is found when a query names it; `ALTER
CONNECTION name REFRESH` reads the lists again on that replica (other
replicas at their next restart), so new tables appear in listings and new
MySQL tables can be queried. Because DuckDB keeps one list for all
connections of a type, `REFRESH` first checks that every connection of that
type can be reached, and refuses rather than lose a list while a database is
down. An Iceberg REST catalog's tables are the ones it had when the replica
started, whose locations the sandbox was opened to then: a table created
since is attached when each replica restarts, and `REFRESH` is refused. New
snapshots of the attached tables are read as they come.

## Iceberg REST catalogs

A connection of `TYPE iceberg` attaches another Iceberg REST catalog's
tables, which Igloo reads where they are, through the catalog, with the
storage credentials the catalog hands out for each table. Nothing is
copied, and the tables stay the catalog's: Igloo never writes to them.

```sql
CREATE CONNECTION partner TYPE iceberg
  WITH (endpoint 'https://catalog.example.com/api/catalog', warehouse 'shared',
        credential 'client-id:client-secret', scope 'PRINCIPAL_ROLE:ALL');

-- after the next restart of each replica
GRANT SELECT ON SCHEMA partner.campaigns TO analysts;
SELECT * FROM partner.campaigns.spend s JOIN lake.public.audiences a USING (campaign_id);
```

The catalog's namespaces are the connection's schemas (only top-level
ones), and their tables its tables. Grants, masking and row access
policies, views, `CREATE TABLE ... AS SELECT` and the query history work
as for [other connections](#grants); time travel works as for Iceberg
tables (`AT (VERSION => snapshot_id)`).

### Options of Iceberg REST catalogs

| Option | Default | Meaning |
|---|---|---|
| `endpoint` (`uri`) | (required) | The catalog's base URL, below which it answers `/v1/config`: what Spark calls `uri`. `https`, unless `tls 'disable'`. |
| `warehouse` | none | The warehouse to attach, as the catalog names it: a Snowflake database, a Databricks share, a Polaris catalog. |
| `token` | | A bearer token sent with every request, stored encrypted |
| `token_secret` | | Instead of `token`: the name of a file in `IGLOO_CONNECTION_SECRETS_DIR` holding it, read at every request |
| `credential` | | An OAuth2 client's `client_id:client_secret`, or the secret alone, exchanged for access tokens (the client credentials grant), stored encrypted |
| `credential_secret` | | Instead of `credential`: the name of a file holding it, read at every exchange |
| `oauth2_server_uri` | `endpoint` + `/v1/oauth/tokens` | Where the credential is exchanged |
| `scope` | `catalog` | The scope asked for with it |
| `signing_name` | | Instead of a token or OAuth2: sign requests with AWS's Signature Version 4 for this service (`glue`, `s3tables`) in `region`, with `credential` as an access key (`key_id:secret`) or, without one, AWS's default credentials (the environment, the instance's or the pod's role) |
| `schema` (`namespace`) | all | Comma-separated namespaces to attach |
| `tls` | `verify-full` | As for [other connections](#tls), for the catalog and the token endpoint. `disable` needs an `http` endpoint. |
| `tls_ca` | the system's | PEM certificates of the authorities that may sign the catalog's certificate |
| `region` | | The storage's region, for catalogs that hand out credentials without one |
| `storage_credential` | | `key_id:secret` of S3-compatible storage, for catalogs that hand out no storage credentials; stored encrypted |
| `storage_credential_secret` | | Instead of `storage_credential`: the name of a file holding it |
| `storage_endpoint` | Amazon S3 | The S3-compatible endpoint the tables' files are read from, as `host:port` or an `http(s)` URL, for storage other than Amazon S3 |
| `storage_url_style` | `path` | `path` or `vhost`, with `storage_endpoint`; an `http` endpoint needs `path` |
| `connect_timeout` | `10s` | How long connecting to the catalog, and getting a token, may take |
| `query_timeout` | `25s` | How long the catalog may take to answer one request (DuckDB itself gives up after 30 seconds and asks again) |
| `read_only` | `true` | Connections are read-only; `false` is refused |

Write `token`, `credential` and `storage_credential` as string literals,
in single quotes: the history keeps the string literals of every `CREATE`
and `ALTER CONNECTION` out, not unquoted words. Credentials are never shown
again (`password` in `SHOW CONNECTIONS` says which kinds the connection has
and where they come from).

`CREATE CONNECTION` reads the catalog's configuration and lists its
namespaces before it stores anything. Each replica, when it starts, lists
the catalog's tables and loads each to learn where its files are; that
takes at most two minutes per catalog, all catalogs at once, after which
the connection is left out (`failed` in `SHOW CONNECTIONS`) and Igloo starts
anyway. A table is left out, with why, when:

- the catalog will not load it, as Databricks answers for a shared table
  that is not readable as Iceberg;
- its files are not in an object store (`s3`, `gs`, `az`, `abfss`, `r2`),
  or are at a bucket's root (but for the bucket S3 Tables makes for each
  table);
- its location, its `write.data.path` or `write.metadata.path`, or the
  prefix the catalog scopes its storage credentials to, overlaps the
  warehouse or `IGLOO_ALLOWED_LOCATIONS`, which Igloo reads with its own
  credentials.

`SHOW CONNECTIONS` says how many tables were attached and which were left
out. Nested namespaces are not attached, and a catalog with more than 5000
tables in the namespaces attached is not attached at all: name the
namespaces to attach with `schema`.

### Snowflake Horizon Catalog

Snowflake serves its Snowflake-managed Iceberg tables to other engines
through the Horizon Catalog's Iceberg REST API, at
`https://<org>-<account>.snowflakecomputing.com/polaris/api/catalog`. The
warehouse is the database's name (in capitals, if it was created without
quotes), and its schemas are the connection's. Igloo signs in as a service
user with a programmatic access token, exchanged for access tokens with the
role to use:

```sql
CREATE CONNECTION snow TYPE iceberg
  WITH (endpoint 'https://myorg-myaccount.snowflakecomputing.com/polaris/api/catalog',
        warehouse 'ANALYTICS', credential_secret 'snowflake-pat', scope 'session:role:IGLOO_READER');
```

The role needs `USAGE` on the database and its schemas and `SELECT` on the
tables; the tables' owner keeps `USAGE` on their external volume. Snowflake
hands out storage credentials for Amazon S3, Google Cloud Storage and
Azure. A programmatic access token needs a network policy that lets Igloo's
addresses in. Horizon serves only Snowflake-managed Iceberg tables, not
Snowflake's native tables nor Iceberg tables another catalog manages. A
connection of `TYPE snowflake` (see [Snowflake](#snowflake)) reads those,
running its queries on a Snowflake warehouse; through Horizon, Igloo reads
the tables' files itself, and no warehouse runs.

### Databricks

A Databricks open sharing profile (`config.share`) of a share whose tables
are readable as Iceberg (UniForm, `delta.universalFormat.enabledFormats =
'iceberg'`) has an `icebergEndpoint`. The warehouse is the share's name, the
token the profile's `bearerToken`:

```sql
CREATE CONNECTION dbx TYPE iceberg
  WITH (endpoint 'https://<workspace>.cloud.databricks.com/api/2.0/opensharing/metastores/<id>/iceberg',
        warehouse 'my_share', token_secret 'dbx-share-token');
```

The share's other tables are left out; Delta Sharing's own protocol, for
tables that are not readable as Iceberg, is [another
feature](sharing.md#reading-partners-shares). Unity Catalog's own Iceberg
REST endpoint, `https://<workspace>/api/2.1/unity-catalog/iceberg-rest`,
takes the catalog's name as the warehouse and a personal access token
(`token`) or an OAuth2 service principal (`credential 'client-id:secret'`,
`scope 'all-apis'`, `oauth2_server_uri 'https://<workspace>/oidc/v1/token'`);
it needs the metastore's external data access turned on and `EXTERNAL USE
SCHEMA` granted, and is not tested yet.

Databricks writes the tables' Iceberg metadata after each change. On a
partitioned table with row tracking (`delta.enableRowTracking`), the files
an `UPDATE`, `DELETE` or `MERGE` rewrites keep row tracking's columns,
without Iceberg field IDs, before the partition columns, and DuckDB 1.5.6
reads the partition columns of those files as `NULL`: turn row tracking
off on partitioned tables shared with Igloo.

### Other catalogs

Apache Polaris and Snowflake Open Catalog take an OAuth2 client
(`credential`) with `scope 'PRINCIPAL_ROLE:ALL'` or a principal role's.
AWS Glue's endpoint, `https://glue.<region>.amazonaws.com/iceberg`, with
the account's ID as the warehouse, and S3 Tables',
`https://s3tables.<region>.amazonaws.com/iceberg`, with the table bucket's
ARN, take requests signed with AWS's Signature Version 4 (`signing_name
'glue'` or `'s3tables'`, and `region`); Glue hands out storage credentials
for tables Lake Formation manages, and otherwise needs `storage_credential`.
Signing is checked against a stand-in, not AWS itself, so far. A catalog
that hands out no storage credentials, such as a Lakekeeper warehouse
without STS, needs `storage_credential`:

```sql
CREATE CONNECTION other TYPE iceberg
  WITH (endpoint 'http://lakekeeper.internal:8181/catalog', warehouse 'analytics', tls 'disable',
        storage_credential 'AKIA...:...', storage_endpoint 'https://minio.internal:9000', region 'us-east-1');
```

### How Iceberg REST catalogs are read

DuckDB's iceberg extension attaches the catalog through a proxy inside
Igloo on the loopback interface, under a random path, as it attaches
Igloo's own catalog. The proxy:

- adds the catalog's credentials, getting OAuth2 tokens and new ones when
  they expire or the catalog refuses one, so DuckDB never sees them;
- answers the lists of namespaces and tables itself, from what the replica
  found at startup, so listing tables never waits on the catalog;
- forwards only the loading of a table, with the connection's timeouts, and
  answers anything that would change the catalog with an error, so not even
  an admin changes the catalog through Igloo (the catalog is also attached
  `READ_ONLY`);
- refuses a table whose files are no longer where they were at startup;
- hands DuckDB only the storage credentials, their expiry and the region
  from the catalog's answer, never the catalog's choice of storage endpoint
  or of plain HTTP: the connection's `storage_endpoint` and the storage
  provider's own endpoints are the only ones reached.

DuckDB's sandbox, locked at startup, lets SQL read the files of the tables
found then, besides the warehouse and the allowed locations. Users other
than admins never name those files themselves (file functions read only
the warehouse and the allowed locations); admins can. Igloo's own storage
credentials apply only to the warehouse and the allowed locations, so a
table is read with the credentials its catalog hands out, or the
connection's `storage_credential`, never Igloo's. DuckDB 1.5.6 keeps the
storage credentials it is handed, one set for each read of a table; Igloo
drops each set two hours after it appears, so a statement reading a table
for longer than that can fail.

## Credentials

A password given with `password` is stored in the metadata database,
`igloo_auth.connections`, encrypted with AES-256-GCM under a key kept in
`igloo_auth.secrets` that every replica shares; it is bound to the
connection's name, so it cannot be moved to another connection. That
protects it from reading the table alone (in a dump, a log, a screenshot),
not from someone with the whole metadata database, which holds the key too.
It is never shown again, in `SHOW CONNECTIONS`, the query history (which
replaces passwords with `'***'`) or the logs.

To keep passwords out of the metadata database altogether, set
`IGLOO_CONNECTION_SECRETS_DIR` to a directory of files, one per password,
such as a mounted Kubernetes Secret, and name the file with
`password_secret` (the Helm chart does not mount one yet; add the volume to
the Deployment yourself):

```sql
ALTER CONNECTION crm SET (password_secret 'crm-password');
```

Igloo reads the file every time it signs in to the database, so rotating
the password there (Kubernetes updates mounted Secrets in place) needs no
change in Igloo. A trailing newline is ignored. `password_secret` takes a
file name, not a path; nothing outside that directory can be read.

A Snowflake connection's private key (`private_key`, or a file named by
`private_key_secret`) is kept the same way: sealed in the metadata
database, bound to the connection's name and to being a private key, or
read from its file at every sign-in, and never shown.

To rotate a stored password: change it on the database, then run `ALTER
CONNECTION name SET (password '...')`; Igloo checks the new one works before
storing it. Sessions already signed in with the old password, which
PostgreSQL and MySQL let finish, sign in again with the new one once they
are idle.

An Iceberg REST catalog's `token`, `credential` and `storage_credential` are
stored the same way, sealed together under the connection's name, and
`token_secret`, `credential_secret` and `storage_credential_secret` name
files of `IGLOO_CONNECTION_SECRETS_DIR` instead (a `credential_secret` file
holds the credential as `credential` would, `client_id:client_secret` or
the secret alone). Igloo reads a token's file at every request to the
catalog and a credential's whenever it gets an access token, so rotating
them there needs nothing more; a storage credential's file is read when the
replica starts. The storage credentials the catalog hands out for a table
are DuckDB secrets, scoped to the table's location, which only admins can
list, with their secret parts redacted.

## Grants

A connection's tables are not covered by the built-in roles: `reader` and
`writer` read the Iceberg catalog, not other databases. Everyone but admins
needs a grant to read a connection, on a table, a schema or the whole
connection, and only `SELECT` can be granted:

```sql
GRANT SELECT ON TABLE crm.public.customers TO analysts;   -- one table; name it in full
GRANT SELECT ON SCHEMA crm.public TO analysts;            -- every table in it, now and later
GRANT SELECT ON ALL TABLES IN SCHEMA crm.public TO nora;  -- the tables there now
GRANT SELECT ON CONNECTION crm TO analysts;               -- everything, the remote catalogs included
REVOKE SELECT ON CONNECTION crm FROM analysts;
SHOW GRANTS ON CONNECTION crm;
```

- A statement that reads a connection's table runs only if that table is
  granted, whatever else the user's role allows. Joining it with Iceberg
  tables also needs whatever those need: nothing more for readers, grants
  for users with role `none`.
- Views read with the querying user's grants, also views of the Iceberg
  catalog that read a connection. Creating a view that reads a
  connection's table, in the catalog or temporary, needs a grant on that
  table as well, so nobody leaves views of what they may not read.
- Grants follow names, as for Iceberg tables. Dropping a connection removes
  its grants; a connection created again under the same name starts with
  none.
- Names of a connection's schemas and tables compare without regard to
  case, in grants as in SQL, where DuckDB resolves them so. PostgreSQL, and
  MySQL on Linux, can have two tables or two schemas whose names differ
  only in case, such as `customers` and `"Customers"`, which grants could
  not tell apart: Igloo refuses to read either for everyone but admins, and
  refuses grants that name either. Rename one on the database, or connect
  with a database user that sees only one.
- Table and column names and types of a connection's tables are visible to
  every user, through the system catalogs, as Iceberg tables' are. The
  [data catalog](catalog.md#external-tables-and-connections) (`igloo.tables`,
  its lineage and usage, the console's catalog) shows a connection's
  tables only to admins and to those with a grant that covers them.
- An Iceberg REST catalog's tables are looked up as the catalog names them,
  in their own case: `partner.campaigns.Spend` is not
  `partner.campaigns.spend`. `iceberg_scan`, `iceberg_metadata` and
  `iceberg_snapshots` on a connection's table, which read it without naming
  it as a table, are refused to everyone but admins; query the table.

How the check works: whenever connections are attached, Igloo has DuckDB
plan every statement of a user who is not an admin. DuckDB's plan shows each
scan of a connection's table (as `POSTGRES_SCAN` or `MYSQL_SCAN`, with the
table's name); DuckDB's parse of the statement, resolved along the session's
search path and with views expanded, says which connection, schema and
table each name means. The two must agree scan for scan, and every table a
name could mean must be granted; otherwise the statement is refused with
SQLSTATE `42501`. A name could mean the tables of that name where DuckDB
looks for it, as it binds names: `media_demo.audit` means the table of
schema `media_demo` in each database that has that schema on the search
path, or in the current database if none does. A connection whose schema
has the name of one of the catalog's, as a Snowflake connection's
`MEDIA_DEMO` may, is not read, and needs no grant, unless its schema is on
the search path. A scan the check cannot account for, such as one through
a macro an admin defined, is refused. The plan shows the scans of an
Iceberg REST catalog's tables as `ICEBERG_SCAN`, as it shows those of the
Iceberg catalog's own tables and of `iceberg_scan` calls, without the
table's name; whenever such a connection is attached, those scans are
counted together, and the plan must have as many as the parse's tables
and calls of either kind. Every prepared statement of a user
who is not an admin, whatever their role, is checked again before it runs
when grants, views, connections or the session's search path changed since
it was checked, including statements prepared before a connection was
attached. The same holds over Arrow Flight SQL and in the web console,
which go through the same checks, and for service accounts, which are
users like any other.

## Masking and row access policies

[Masking policies, row access policies and tags](security-policies.md)
apply to a connection's tables as to Iceberg tables, in every statement of
every user, admins included. Name the table in full:

```sql
ALTER TABLE crm.public.customers ALTER COLUMN email SET MASKING POLICY mask_email;
ALTER TABLE crm.public.customers ADD ROW ACCESS POLICY client_rows ON (client_id);
ALTER TABLE crm.public.customers ALTER COLUMN phone SET TAG pii = 'phone';
```

- Policies are set on tables of connections attached on the replica that
  runs the statement, whose columns Igloo knows, and can be unset at any
  time. Policy expressions still read only Iceberg tables.
- The database can rename or drop a column without Igloo. A connection's
  table with a masked column it no longer has is refused, to everyone,
  until its policy is unset or set on the column's new name; after a change
  on the database, `ALTER CONNECTION name REFRESH` shows Igloo the new
  columns.
- A policy protects the table it is set on. Other tables and views of the
  remote database that hold the same data are not protected: set policies
  on them too, or connect with a database user that cannot read them.
- Policies stay when a connection is dropped, as they do when a table is,
  so a connection created again under the name is protected from the start.
- `igloo.policy_references` names the connection in `ref_database_name`.

## What no one can do

- **Write to a connection.** Connections are attached read-only, and every
  session Igloo opens on the remote database is read-only too
  (`default_transaction_read_only` on PostgreSQL, `SET SESSION TRANSACTION
  READ ONLY` on MySQL), so not even an admin writes through Igloo. Igloo
  sends Snowflake only SELECTs it writes itself (see
  [Snowflake](#how-igloo-reads-snowflake)). An Iceberg REST catalog gets
  only reads from Igloo (see [how they are
  read](#how-iceberg-rest-catalogs-are-read)), and Igloo's maintenance,
  erasure, Delta Sharing, distributed execution and its own Iceberg REST
  catalog for other engines work on Igloo's catalog only.
- **Run SQL on the remote database.** The extensions' `postgres_query`,
  `postgres_execute`, `mysql_query` and `mysql_execute`, which run SQL given
  as a string there, and `postgres_scan`, `postgres_scan_pushdown` and
  `postgres_attach`, which connect to any server with credentials given in
  the call, are refused for everyone, admins included: Igloo cannot check
  that SQL against grants, and they would reach any server Igloo's network
  can. Query tables by name instead. Their cache and pool functions
  (`pg_clear_cache`, `mysql_clear_cache`, `postgres_configure_pool`) and
  settings (`pg_*`, `mysql_*`) are for admins only.
- **Detach a connection** with `DETACH`: use `DROP CONNECTION`.
- **Connect to Igloo's metadata database**, which holds its users, grants
  and history: `CREATE` and `ALTER CONNECTION` refuse a host and port that
  is `IGLOO_METADATA_DATABASE_URL`'s, however the name is written (case, a
  trailing dot, IPv6 and IPv4-in-IPv6 forms), or that resolves, then, to
  one of the addresses its host does. That guards against mistakes only:
  the server can still be reached through a name that resolves differently
  later, a proxy or a port forward, and other internal services such as
  the catalog's database are not guarded at all, so do not create
  connections to them.
- **Pivot on a connection's values.** A `PIVOT` that takes its columns from
  the data (no `IN (...)` after `ON`) runs a query while it is prepared,
  before it can be checked; for users who are not admins it is refused
  whenever it may read a connection. List the columns instead.
- **Read the server's local files through a malicious database.** A MySQL
  server can ask its client to upload a local file (`LOAD DATA LOCAL
  INFILE`); Igloo ends any session whose server asks.

The query history records, in `tables_read`, the connection tables each
statement read, named in full (`crm.public.customers`), for every user: for
admins as far as Igloo can tell, for everyone else as checked.

## How it works

DuckDB never sees a connection's credentials or address. Each connection
is attached through a small proxy inside Igloo, listening on the loopback
interface with a random password of its own; DuckDB signs in to the proxy,
and the proxy signs in to the database with the connection's current
settings and relays the session. That is what lets credentials and hosts
change without a restart, keeps `duckdb_databases()` and error messages
from showing a password, and lets Igloo:

- sign in lazily: DuckDB's extensions open, check and reset sessions and
  begin transactions just to list tables, and the proxy answers those
  itself, so listing tables never waits on a remote database and keeps
  working while one is down;
- give up on a database that stops answering, after `query_timeout`, instead
  of leaving DuckDB waiting;
- make every remote session read-only, and refuse requests for local files.

Startup checks every connection's database at once, so it waits at most
about the longest `connect_timeout` (for Snowflake, plus the time to read
its list of tables; for an Iceberg REST catalog, the time to find its
tables, at most two minutes), and never past 30 seconds before the end of
the three minutes Igloo gives itself to start. A database that cannot be
reached yet (no route, refused, timed out, name not resolved), as when the
machine has just started and its network is not up, is tried again for up
to 30 seconds; a refused sign-in or a wrong setting is not. A connection
whose database cannot be reached by then is left out on that replica until
it restarts, with the reason in `SHOW CONNECTIONS`, and Igloo starts
regardless.

Snowflake has no protocol DuckDB speaks, so a Snowflake connection is
attached at a gateway inside Igloo instead of a proxy; see
[how Igloo reads Snowflake](#how-igloo-reads-snowflake). Iceberg REST
catalogs go through an HTTP proxy of their own; see [how they are
read](#how-iceberg-rest-catalogs-are-read).

## Performance

- DuckDB pushes `WHERE` filters on columns and the list of columns down to
  the remote database, so a selective query moves only the rows and columns
  it needs. `EXPLAIN` shows them under `POSTGRES_SCAN` or `MYSQL_SCAN`.
  Joins, aggregates and `ORDER BY` run in Igloo; join small remote tables
  to large Iceberg ones rather than the other way round, or copy large
  remote tables into Iceberg with `CREATE TABLE AS` and refresh them on a
  schedule.
- PostgreSQL tables are read in the binary `COPY` format, large ones in
  parallel by page range (PostgreSQL 14 and later) under one snapshot.
- DuckDB keeps a pool of sessions per connection and reuses them, so most
  queries do not sign in again; listing tables costs the remote database
  nothing.
- Each query reads the remote database as it is then: there is no cache of
  rows.

## Snowflake

```sql
CREATE CONNECTION wh TYPE snowflake WITH (
  account 'myorg-myaccount', user 'IGLOO_READER', private_key_secret 'wh-key.p8',
  warehouse 'IGLOO_WH', role 'IGLOO_READER', database 'ANALYTICS',
  schema 'PUBLIC, MARTS', query_timeout '15m');

-- after the next restart of each replica
GRANT SELECT ON SCHEMA wh.marts TO analysts;
SELECT c.region, sum(o.amount)
FROM wh.marts.orders o JOIN lake.public.customers c ON c.id = o.customer_id
WHERE o.order_date >= DATE '2026-01-01'
GROUP BY ALL;

-- a copy in Igloo, as an Iceberg table
CREATE TABLE lake.public.orders AS SELECT * FROM wh.marts.orders;
```

| Option | Default | Meaning |
|---|---|---|
| `account` | (required) | The [account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier), `orgname-accountname`, or an account locator with its region (`xy12345.us-east-2.aws`) |
| `user` | (required) | The Snowflake user Igloo signs in as |
| `private_key_secret` | | The name of a file in `IGLOO_CONNECTION_SECRETS_DIR` holding the user's private key, read at every sign-in (see [key pairs](#key-pairs)) |
| `private_key` | | Instead: the private key itself, in PEM, stored encrypted |
| `password_secret`, `password` | | Instead of a key: a password, as for other connections. Snowflake is phasing out sign-in with a password alone; prefer a key pair. |
| `warehouse` | the user's default | The warehouse that runs Igloo's queries |
| `role` | the user's default | The role they run as |
| `database` | (required) | The database whose schemas become the connection's |
| `schema` | all but `INFORMATION_SCHEMA` | Comma-separated schemas to attach |
| `host`, `port` | the account's, `443` | Another address of the account, such as its PrivateLink one |
| `connect_timeout` | `10s` | How long signing in may take |
| `query_timeout` | `5m` | How long Snowflake may run one of Igloo's queries (Snowflake's `STATEMENT_TIMEOUT_IN_SECONDS`, which stops the warehouse working on it), and how long one part of a result may take to download; `0` for no limit |

Exactly one of `private_key_secret`, `private_key`, `password_secret` and
`password` is needed; setting one replaces another. Names in `database`,
`schema`, `warehouse` and `role` are read as Snowflake reads names written
without quotes, in any case; write a name Snowflake keeps in mixed case in
double quotes, as in `database '"Analytics"'`. Snowflake connections always
use HTTPS and check Snowflake's certificate; `tls` and `tls_ca` do not
apply. `account`, `user`, the key or password, `warehouse`, `role`, `host`
and the timeouts apply without a restart, as for other connections;
`ALTER CONNECTION ... REFRESH` reads the list of tables again.

### Key pairs

Snowflake users that sign in from programs use a
[key pair](https://docs.snowflake.com/en/user-guide/key-pair-auth): an RSA
key whose public half Snowflake holds. Create one, unencrypted, and give
Snowflake the public key:

```sh
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out wh-key.p8 -nocrypt
openssl rsa -in wh-key.p8 -pubout -out wh-key.pub
```

```sql
-- in Snowflake, as a user administrator; the key without its BEGIN and END lines
ALTER USER IGLOO_READER SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqh...';
```

Put `wh-key.p8` in `IGLOO_CONNECTION_SECRETS_DIR` (a mounted Kubernetes
Secret, say) and name it with `private_key_secret`. Igloo reads it at every
sign-in, so a new key takes effect there without a change in Igloo:
Snowflake holds two public keys per user (`RSA_PUBLIC_KEY_2`) for rotating
without a gap. Encrypted private keys are not supported. A key given with
`private_key` is stored like a password (see [credentials](#credentials)),
and the statement that sets it reaches the history and the logs with the
key replaced by `'***'`.

### What Snowflake needs

Give Igloo a user and role that can read only what it should, with a
warehouse of its own so its cost is visible:

```sql
CREATE ROLE IGLOO_READER;
GRANT USAGE ON WAREHOUSE IGLOO_WH TO ROLE IGLOO_READER;
GRANT USAGE ON DATABASE ANALYTICS TO ROLE IGLOO_READER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ANALYTICS TO ROLE IGLOO_READER;
GRANT SELECT ON ALL TABLES IN DATABASE ANALYTICS TO ROLE IGLOO_READER;
GRANT SELECT ON ALL VIEWS IN DATABASE ANALYTICS TO ROLE IGLOO_READER;
GRANT SELECT ON FUTURE TABLES IN DATABASE ANALYTICS TO ROLE IGLOO_READER;
CREATE USER IGLOO_READER TYPE = SERVICE DEFAULT_ROLE = IGLOO_READER DEFAULT_WAREHOUSE = IGLOO_WH;
GRANT ROLE IGLOO_READER TO USER IGLOO_READER;
```

A database shared with the account, such as `SNOWFLAKE_SAMPLE_DATA`, needs
`GRANT IMPORTED PRIVILEGES ON DATABASE ... TO ROLE ...` instead. Igloo sees
the tables, views and columns the role can see; the role needs nothing but
`USAGE` and `SELECT`, since Igloo never writes.

### How Igloo reads Snowflake

DuckDB attaches a Snowflake connection as a PostgreSQL database, with its
`postgres` extension, at a gateway inside Igloo on the loopback interface,
which it signs in to with a random per-process token. The gateway speaks
just enough of PostgreSQL's protocol for the extension and nothing more:

- **Listing tables.** When a replica attaches the connection, the gateway
  reads the database's schemas, tables, views and columns, with their
  comments, using Snowflake's `SHOW` commands, which run without a
  warehouse. It serves DuckDB's listings (`information_schema`,
  `duckdb_tables()`, the [data catalog](catalog.md), the grant checks)
  from that list, so they cost nothing and work while Snowflake is down.
  `ALTER CONNECTION ... REFRESH` reads it again.
- **Scans.** For each scan of a table DuckDB plans, the extension asks for
  its columns, with the filters and any `LIMIT` DuckDB pushed down. The
  gateway parses exactly that request, refusing anything else, and writes
  a Snowflake query of its own from it: `SELECT` of those columns of that
  one table, with the filters and limit in Snowflake's SQL, names quoted
  as Snowflake spells them and constants converted to the column's type.
  No SQL a user wrote reaches Snowflake. It runs the query with Snowflake's
  Go driver, signed in with the connection's key or password, and streams
  the result, in Snowflake's Arrow format, to DuckDB as it downloads;
  nothing is held in memory whole. Each scan holds the part of the result
  it is sending, and all scans together download at most eight parts
  ahead, a part taking up to about 160 MB. `count(*)` becomes
  `COUNT(*)` in Snowflake, which Snowflake answers from its metadata
  without a warehouse when there is no filter.
- **Timeouts.** Signing in is bounded by `connect_timeout`; Snowflake stops
  a query at `query_timeout` itself (its `STATEMENT_TIMEOUT_IN_SECONDS`),
  and Igloo cancels it there too, so the warehouse does not go on working
  for nothing. The sessions are tagged `QUERY_TAG = 'igloo connection
  <name>'`, so Snowflake's query history shows each query Igloo sent:

  ```sql
  SELECT start_time, query_text, execution_status, total_elapsed_time
  FROM TABLE(ANALYTICS.INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(USER_NAME => 'IGLOO_READER'))
  WHERE query_tag LIKE 'igloo connection %' ORDER BY start_time DESC;
  ```

That is why Igloo does not use DuckDB's community Snowflake extension: it
signs in with Snowflake's credentials from inside DuckDB, where SQL can
reach them and Igloo cannot bound or cancel what it runs, and needs
Snowflake's ADBC driver beside it. Attaching through the `postgres`
extension also gives Snowflake's tables everything other connections'
have: DuckDB's filter and limit pushdown, its catalog, and plans that name
each scan's table, which [grants](#grants) and
[policies](#masking-and-row-access-policies) are checked against.

### Names and types

A name Snowflake keeps in upper case, as it keeps every name written
without quotes, is shown in lower case, as PostgreSQL shows unquoted names:
`ANALYTICS.MARTS.ORDERS` is `wh.marts.orders`, and `CREATE TABLE AS SELECT
*` makes lower-case columns. Any other name is shown as it is (`"Order
Lines"` stays `Order Lines`). Two names that would then look alike keep
their own spellings; two columns of a table whose names differ only in
case, which DuckDB cannot hold, are shown with a suffix (`_2`) on the
second, and Igloo logs a warning.

| Snowflake | DuckDB |
|---|---|
| `NUMBER(p,s)`, `INT` and the like | `DECIMAL(p,s)`: `NUMBER(38,0)` is `DECIMAL(38,0)` |
| `FLOAT`, `DOUBLE`, `REAL` | `DOUBLE` |
| `VARCHAR`, `STRING`, `TEXT`, `CHAR` | `VARCHAR` |
| `BINARY` | `BLOB` |
| `BOOLEAN` | `BOOLEAN` |
| `DATE` | `DATE` |
| `TIME` | `TIME`, to the microsecond |
| `TIMESTAMP_NTZ` | `TIMESTAMP`, to the microsecond |
| `TIMESTAMP_LTZ`, `TIMESTAMP_TZ` | `TIMESTAMP WITH TIME ZONE`, to the microsecond; `TIMESTAMP_TZ`'s offset is not kept, only the instant |
| `VARIANT`, `OBJECT`, `ARRAY`, `MAP`, `VECTOR` | `VARCHAR` holding the value as JSON, as Snowflake's `TO_JSON` writes it; cast to `JSON` to query it |
| `GEOGRAPHY`, `GEOMETRY` | `VARCHAR`, as well-known text |
| others | `VARCHAR`, as Snowflake's `TO_VARCHAR` writes them |

Times and timestamps with more than six digits of fractions of a second
are cut to the microsecond, and filters on them compare what DuckDB sees:
`ts = TIMESTAMP '2026-01-02 03:04:05.123456'` matches every value within
that microsecond.

Filters on strings compare them byte by byte, as DuckDB does, whatever
collation a Snowflake column has: Igloo passes them on under Snowflake's
`utf8` collation, with which Snowflake still skips the micro-partitions
they rule out. In a column created with `en-ci`, say, `region = 'EU'`
keeps only `EU`, not `eu`. DuckDB passes on the conditions of
[row access policies](#masking-and-row-access-policies) too, and does not
check again what Snowflake returns, so this is what keeps a policy from
letting rows through.

### Cost

Snowflake bills the warehouse by the second, with a minute's minimum each
time it resumes. Igloo's queries use it as any client's would:

- Listing tables, `ALTER CONNECTION ... REFRESH`, signing in and `count(*)`
  of a whole table cost no warehouse time.
- A query reads only the columns it needs, and only the rows its filters
  keep where DuckDB pushes them down: comparisons of a column with
  constants, `IN` lists, `IS NULL`, `BETWEEN`, prefix `LIKE`, and `AND` and
  `OR` of those on one column. Snowflake then skips the micro-partitions
  they rule out. Joins, aggregates, `ORDER BY` and other filters run in
  Igloo, on the rows Snowflake sent. `EXPLAIN` shows what a scan
  (`POSTGRES_SCAN`) passes on.
- A `LIMIT` reaches Snowflake when it applies to the scan itself, with all
  of the query's filters passed on, as in `SELECT * FROM wh.marts.orders
  WHERE order_date > DATE '2026-01-01' LIMIT 100`.
- Snowflake serves a query it answered in the last day from its result
  cache, without the warehouse, unless the table changed.
- Every query of a statement is its own Snowflake query: join large
  Snowflake tables in Snowflake, or copy them into Iceberg tables with
  `CREATE TABLE AS` and query the copies.

### Copying into Igloo

`CREATE TABLE lake.public.orders AS SELECT * FROM wh.marts.orders` copies a
table in one Snowflake query, streamed into the new Iceberg table as it
downloads. The download is usually what takes longest: run Igloo in the
same cloud region as the Snowflake account, or expect a copy to go at the
speed of the network between them. To copy part of a table, filter it
(`WHERE updated_at >= ...`), which Snowflake applies before sending
anything.

### Limits

- Canceling a statement in Igloo, or its `statement_timeout`, does not
  cancel a Snowflake query that is still running: DuckDB's `postgres`
  extension waits for its first rows and cannot be interrupted before
  then. The query stops at `query_timeout`, which bounds what it can cost;
  set it to what the longest query should take.
- A scan of a Snowflake table runs on one of DuckDB's threads (the
  downloads run in parallel); scans of PostgreSQL tables can be split by
  page range, but Snowflake's results cannot be split that way yet.
- Encrypted private keys, OAuth and single sign-on are not supported;
  sign in with an unencrypted key or a password.
- `TIMESTAMP_TZ`'s offsets, and fractions of a second finer than a
  microsecond, are not kept (see [names and types](#names-and-types)).
- Tables created in Snowflake since the replica started appear after
  `ALTER CONNECTION ... REFRESH` on that replica, or its restart.

## Limitations

- New connections, and changes to a connection's database or schemas, need
  a restart of each replica (see [when changes take effect](#when-changes-take-effect)),
  as do tables an Iceberg REST catalog created since a replica started.
- Read-only: no writes to connections, not even by admins.
- Iceberg REST catalogs: no remote signing of storage requests, no nested
  namespaces, no Iceberg views, and no storage credentials from AWS's
  default credentials (only from the catalog or `storage_credential`). A
  remote table's metadata decides which
  files DuckDB reads for it. Igloo opens the sandbox only to the tables'
  locations, apart from the warehouse, but a remote catalog's manifests can
  still name files in the warehouse or the allowed locations, which DuckDB
  would read with Igloo's credentials for whoever may read that remote
  table, or files of another connection's tables, read with credentials
  that catalog handed out for another statement (DuckDB keeps them for up
  to two hours). File paths hold random IDs and are shown only to those who
  may read the table; connect only catalogs whose writers you trust that
  far.
- Not served to other engines: the [Iceberg REST catalog](external-engines.md)
  and [Delta Sharing](sharing.md) serve Iceberg tables only. To share a
  connection's data, copy it into an Iceberg table first.
- Grants stop at tables, as for Iceberg tables; masking and row access
  policies restrict columns and rows (see above). Restrict what the
  connection's database user can see on the database itself as well.
- Tables and schemas whose names differ only in case from another's in the
  same connection cannot be read by users who are not admins (see
  [grants](#grants)).
- Without a metadata database (`IGLOO_METADATA_DATABASE_URL`) there are no
  connections.
