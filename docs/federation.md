# Connections to other databases

Igloo can query PostgreSQL and MySQL databases where they are, next to its
Iceberg tables: a client's CRM, an operational database, a database being
migrated. An admin registers a connection once; every replica attaches it,
read-only, as a catalog of the same name, and anyone granted its tables
queries them like any other:

```sql
CREATE CONNECTION crm TYPE postgres
  WITH (host 'crm.db.example.com', database 'crm', user 'igloo_ro', password 'a-long-password', tls 'verify-full');

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

Both extensions are DuckDB's own core extensions, built for linux/amd64 and
linux/arm64 and baked into the image like the others; Igloo loads them only
when a connection needs them. The extensions are MIT-licensed. The
`postgres` extension links libpq (PostgreSQL License); the `mysql` extension
links MariaDB Connector/C, which is LGPL-2.1.

MySQL users must sign in with `caching_sha2_password` (the default since
MySQL 8), `mysql_native_password` or `sha256_password`. MariaDB's `ed25519`
and the PAM and GSSAPI methods are not supported.

**Not supported yet.** BigQuery and Snowflake have DuckDB community
extensions, but they sign in with Google's and Snowflake's credentials and
talk to those services from inside DuckDB, where Igloo cannot keep the
credentials out of SQL's reach or enforce timeouts the way it does for
connections (see [how it works](#how-it-works)); Snowflake's also needs
Snowflake's ADBC driver installed beside it. Until they are, copy their data
into Iceberg tables, or share it as Iceberg and read it in place. SQLite
databases are files on the server, which the [sandbox](../SECURITY.md) keeps
SQL away from.

## Creating, changing and dropping connections

Only admins manage connections.

```sql
CREATE CONNECTION [IF NOT EXISTS] name TYPE postgres | mysql WITH (option value, ...);
ALTER CONNECTION name SET (option value, ...);
ALTER CONNECTION name REFRESH;
DROP CONNECTION [IF EXISTS] name;
SHOW CONNECTIONS;
```

Options may be written `option value` or `option = value`; values are
strings, numbers or words.

| Option | Default | Meaning |
|---|---|---|
| `host` | (required) | Host name or IP address of the database server |
| `port` | `5432`, `3306` | Its port |
| `database` (`dbname`) | (required) | The database. For MySQL it is the default schema; every database the user may see is attached. |
| `user` | (required) | The database user Igloo signs in as. Give it read access only to what Igloo should see. |
| `password` | | Its password, stored encrypted (see [credentials](#credentials)) |
| `password_secret` | | Instead of `password`: the name of a file in `IGLOO_CONNECTION_SECRETS_DIR` holding it, read at every sign-in |
| `tls` (`sslmode`, `ssl_mode`) | `prefer` | `disable`, `prefer`, `require`, `verify-ca` or `verify-full`, as libpq means them; MySQL's names (`disabled`, `preferred`, `required`, `verify_identity`) work too. Use `verify-full` across untrusted networks. |
| `tls_ca` | the system's | PEM certificates of the authorities that may sign the server's certificate. With `require`, setting it checks the chain, as libpq does. |
| `schema` | all | Comma-separated schemas to attach, for large databases or to expose only some |
| `connect_timeout` | `10s` | How long connecting and signing in may take |
| `query_timeout` | `5m` | How long the remote database may work on one statement (its `statement_timeout`, `max_execution_time` or `max_statement_time`) and how long it may leave Igloo waiting for the next part of a result before Igloo gives up; `0` for no limit |
| `read_only` | `true` | Connections are read-only; `false` is refused |

`CREATE CONNECTION` checks that Igloo can sign in before it stores
anything. `SHOW CONNECTIONS` lists every connection's settings, never its
password (`password` shows `stored`, `secret <name>` or `none`), with its
state on the replica that answers: `attached`, `not attached` with the
reason, or `failed` with the error.

### When changes take effect

DuckDB attaches databases only until Igloo locks its sandbox at startup
(no SQL can attach anything after that, which is what keeps it from
reaching other servers). So:

- **A new connection** is attached by each replica when it next starts.
  Restart the replicas after `CREATE CONNECTION`: `docker compose restart
  igloo` on one machine, `kubectl rollout restart deployment/igloo` on
  Kubernetes. A replica scaled to zero attaches it when it resumes.
- **Host, port, user, password, TLS and timeouts** (`ALTER CONNECTION ...
  SET`) apply at once on the replica that ran it and within 15 seconds on the
  others, without a restart. Sessions with the database are signed in again
  with the new settings once they finish what they are doing; running
  queries are not interrupted.
- **The database and schemas** apply when each replica restarts;
  `SHOW CONNECTIONS` says so until then.
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
down.

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

To rotate a stored password: change it on the database, then run `ALTER
CONNECTION name SET (password '...')`; Igloo checks the new one works before
storing it. Sessions already signed in with the old password, which
PostgreSQL and MySQL let finish, sign in again with the new one once they
are idle.

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
  catalog that read a connection.
- Grants follow names, as for Iceberg tables. Dropping a connection removes
  its grants; a connection created again under the same name starts with
  none.
- Names compare without regard to case, as DuckDB resolves them: a grant
  on `crm.public.customers` also covers a PostgreSQL table named
  `"Customers"` in the same schema, should there be both.
- Table and column names and types of a connection's tables are visible to
  every user, through the system catalogs, as Iceberg tables' are.

How the check works: whenever connections are attached, Igloo has DuckDB
plan every statement of a user who is not an admin. DuckDB's plan shows each
scan of a connection's table (as `POSTGRES_SCAN` or `MYSQL_SCAN`, with the
table's name); DuckDB's parse of the statement, resolved along the session's
search path and with views expanded, says which connection, schema and
table each name means. The two must agree scan for scan, and every table a
name could mean must be granted; otherwise the statement is refused with
SQLSTATE `42501`. A scan the check cannot account for, such as one through
a macro an admin defined, is refused. Prepared statements are checked
again when grants, views, connections or the session's search path change.

## What no one can do

- **Write to a connection.** Connections are attached read-only, and every
  session Igloo opens on the remote database is read-only too
  (`default_transaction_read_only` on PostgreSQL, `SET SESSION TRANSACTION
  READ ONLY` on MySQL), so not even an admin writes through Igloo.
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
  and history: a connection to the host and port of
  `IGLOO_METADATA_DATABASE_URL` is refused. That guards against mistakes
  only; the same server under another name, and other internal services
  such as the catalog's database, are reached just the same, so do not
  create connections to them.
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
about the longest `connect_timeout`; a connection whose database cannot be
reached then is left out on that replica until it restarts, with the
reason in `SHOW CONNECTIONS`, and Igloo starts regardless.

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

## Limitations

- New connections, and changes to a connection's database or schemas, need
  a restart of each replica (see [when changes take effect](#when-changes-take-effect)).
- Read-only: no writes to connections, not even by admins.
- Grants stop at tables, as for Iceberg tables: no column or row
  restrictions on a connection's tables yet. Restrict what the connection's
  database user can see on the database itself.
- Without a metadata database (`IGLOO_METADATA_DATABASE_URL`) there are no
  connections.
