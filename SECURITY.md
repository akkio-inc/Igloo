# Security policy

## Supported versions

| Version | Supported |
|---|---|
| 1.x | Yes |
| Earlier than 1.0 | No |

Security fixes are released as new 1.x versions. Run the latest one.

## Reporting a vulnerability

Please do not open a public issue, pull request or discussion for a security
problem. Report it privately through GitHub:

1. Open the **Security** tab of
   [akkio-inc/Igloo](https://github.com/akkio-inc/Igloo).
2. Click **Report a vulnerability**
   ([direct link](https://github.com/akkio-inc/Igloo/security/advisories/new)).

Include the Igloo version, how it is deployed, steps or SQL that reproduce the
problem, and what an attacker could do with it.

The maintainers will acknowledge the report, work with you on a fix in the
private advisory, and publish the advisory when the fixed release is out. We
credit reporters in the advisory unless you ask us not to.

Vulnerabilities in components Igloo builds on (DuckDB, Lakekeeper, PostgreSQL,
RustFS) belong with those projects. Tell us as well if Igloo's defaults or
deployment files make one exploitable.

## Security model

A short summary. [docs/configuration.md](docs/configuration.md) is the full
reference for every setting mentioned here.

- **Authentication.** Clients sign in with SCRAM-SHA-256, the PostgreSQL
  default, so passwords are not sent over the connection. The first
  administrator is created from `IGLOO_ADMIN_PASSWORD` on first start; users
  and roles are stored in the PostgreSQL database named by
  `IGLOO_METADATA_DATABASE_URL`. After 10 failed logins for a user, or 50
  from an address, in 15 minutes, each replica refuses further ones for a
  while. Disabled users (`NOLOGIN`) cannot sign in by any means, and their
  sessions end.
- **Access tokens** ([details](docs/sso.md#access-tokens)) stand in for
  passwords in SQL clients. They are 256-bit random secrets with an
  `igloo_` prefix, shown once and stored only as a SHA-256 hash and SCRAM
  keys; SQL logins with them use SCRAM, so the token does not cross the
  network, and hashes are compared in constant time. Every token expires
  (90 days by default, at most `IGLOO_TOKEN_MAX_LIFETIME`); its use is
  recorded; revoking it ends its sessions within 30 seconds. A session that
  signed in with a token, or a task, cannot create tokens for its user or
  set its password; an admin's token can still do anything an admin can. Tokens do
  not sign in to the web console. Service accounts have no password and
  only admin-made tokens.
- **Single sign-on** ([details](docs/sso.md)) to the web console uses
  OpenID Connect's authorization code flow with PKCE, with state and nonce
  checked in constant time, and verifies the ID token's signature against
  the provider's published keys, its issuer, audience and expiry. The
  issuer must be `https`. Identity provider accounts are matched to users by
  their stable subject, then a verified email, and never by name to a user
  with a password; allowed domains and groups limit who may sign in. No
  code, token, state or verifier is logged or put in a URL Igloo makes.
  Optionally, identity provider access tokens are accepted as SQL passwords,
  only inside TLS and for users without a password, after the same
  verification with a configured audience; with that on, whether a user
  has no password can be told from the login. SCIM provisioning, with single
  sign-on, accepts only access tokens of admins as bearer tokens, inside
  TLS (or at `localhost`); it can create, change and disable people and
  their groups, never service accounts, and never disables the last
  enabled admin who is a person. Its token is a full admin credential in
  SQL too.
- **Roles.** Every user has one built-in role. `admin` can do everything,
  including managing users, roles and grants. `writer` can read and write
  tables and create and drop tables and schemas. `reader` can read tables.
  `none` can do nothing on its own. Each statement is checked before it runs,
  using DuckDB's own classification of the statement plus its SQL code (never
  its comments or literals); anything Igloo cannot classify needs `admin`.
  Non-admins may send only one statement per query (DuckDB's own expansion
  of a `PIVOT` in a `SELECT` aside, whose generated query is checked
  first), and their
  `SET` commands apply to their own session only. No one, admins included,
  may run SQL built from strings (`query()`, `json_execute_serialized_sql()`),
  which a check of the statement's text cannot see into, or move the read
  cache or change what it caches; its other functions and settings, and
  `query_table()`, are limited to admins. `query_table()` needs its table
  name written out as text, since a name built at run time could resolve to
  a file, GDAL formats included, that a text check cannot see.
- **Grants.** Admins grant `SELECT`, `INSERT`, `UPDATE`, `DELETE` on tables
  and schemas, and `CREATE` on schemas, to users or to custom roles. A
  statement the built-in role does not allow runs only if every catalog
  table it touches is granted. The table it writes comes from its text,
  resolved along the session's search path; every object the name could
  mean must be granted. What it reads, for users with role `none`, comes
  from DuckDB: Igloo parses the statement with DuckDB's own parser, resolves
  and expands views the way DuckDB does, and checks the result against
  DuckDB's plan of the statement, which must scan exactly as many catalog
  tables as the parse found. A statement whose reads cannot be accounted for
  is refused. Users with role `none` may not call functions that read files
  or take a table's name (`read_parquet`, `iceberg_scan`, `query_table` and
  the like), nor macros an admin defined, since those could read data
  without naming it. Views run with the querying user's privileges. Grants
  never allow what needs `admin`. A prepared statement is checked again
  before it runs when grants, the catalog or the session's settings changed,
  so replacing a view or changing `search_path` cannot widen it. Table and
  column names are visible to every user through the system catalogs.
- **Policies.** Masking policies, row access policies and tags
  ([details](docs/security-policies.md)) apply to every statement of every
  user, admins included, unless a policy's own expression exempts them.
  Igloo rewrites a statement that reads a protected table so it reads the
  table through a subquery that applies the policies, in which every column
  is masked and guarded by the row access policy, so conditions anywhere in
  the statement, and error messages, see only what the user may see. It
  checks the result (rewritten again, it must not change; DuckDB's plan of
  it must read exactly the tables its text names) and refuses statements it
  cannot rewrite with certainty, including several statements in one query
  that name a protected table, renames of protected tables and columns, and
  DDL that would evaluate an expression over the table's rows (a column type
  change, `ALTER ... USING`, a non-constant `DEFAULT`, a generated column, a
  `CHECK` constraint, or `CREATE INDEX`), which could otherwise read masked
  values and hidden rows.
  Policy expressions are stored with their tables qualified and may not
  call macros, read files or depend on session state, and a statement is
  refused if a macro of its session replaces a function a policy calls, or
  its session changed a setting that changes what expressions mean
  (`integer_division`, for one).
  While any table has policies, users other than admins may not read the
  warehouse's files directly (file functions, `COPY INTO`, pipes) nor call
  table functions that read tables by name. Tasks, pipes and materialized
  views run through their owners' policies. A [clone](docs/sql.md#clones)
  of a table gets the table's policies and tags before it exists, waiting
  for other replicas to learn of them the first time a name gets policies,
  so no statement reads it unprotected; cloning needs `SELECT` on the table
  and `CREATE` on the clone's schema, and a clone holds every row of its
  source behind those policies. Policies are not a boundary for admins,
  who manage them and can read the warehouse's files, nor for other
  engines reading the Iceberg tables directly. Policy changes reach other
  replicas within 30 seconds.
- **Connections to other databases.** Only admins create, change and drop
  [connections](docs/federation.md) to PostgreSQL and MySQL databases. A
  connection's tables need a `SELECT` grant (on the table, its schema or the
  connection) for everyone but admins: the built-in roles `reader` and
  `writer` do not cover them. Whenever connections are attached, every
  statement of a user who is not an admin is checked the way reads are for
  role `none`: DuckDB's plan must scan exactly the connection tables
  DuckDB's parse of the statement names, by type and table, with views
  expanded, and each must be granted, or the statement is refused. Every
  statement of a user who is not an admin carries a record of that check
  tied to the connections and the catalog, so a prepared statement is
  checked again when either changes. Creating a view that reads a
  connection's table needs a grant on it too. Igloo names a connection's
  schemas and tables without case, so tables or schemas whose names differ
  only in case are refused to everyone but admins. Masking and row access
  policies apply to connections' tables too; the check of a statement
  rewritten for them compares the plan's scans of connections' tables with
  the statement's, and a table whose masked column the remote database
  renamed is refused until the policy follows it. A policy does not protect
  other tables or views of the remote database holding the same data.
  Connections are attached read-only, and every remote session is
  read-only too. The extensions' functions that run SQL on a remote
  database or connect to any server (`postgres_query`, `postgres_execute`,
  `postgres_scan`, `mysql_query` and the like) are refused for everyone,
  admins included; a data-driven `PIVOT` that may read a connection is
  refused for non-admins. Connections are attached only at startup, before
  the sandbox locks, so SQL cannot attach a database or reach a server that
  is not a connection. DuckDB never sees a connection's credentials: it
  reaches each database through a proxy inside Igloo on the loopback
  interface, signing in with a random per-process token, and the proxy
  signs in to the database. Stored passwords are encrypted (AES-256-GCM,
  bound to the connection's name) under a key in the metadata database, so
  they are safe from a copy of the connections table but not from someone
  with the whole metadata database; `password_secret` keeps them in files
  of `IGLOO_CONNECTION_SECRETS_DIR` instead. Passwords never appear in
  `SHOW CONNECTIONS`, the history or the logs. The proxy refuses a MySQL
  server's request for a local file. Connections use TLS and verify their
  server's certificate and host name unless an admin chooses otherwise,
  which draws a warning. A connection to the server of the metadata
  database, by any spelling of its name or by the address it resolves to,
  is refused, as a guard against mistakes; do not connect the catalog's
  database or other internal services either. Give each connection a
  database user that can read only what Igloo should.
- **History.** Every statement (with passwords and secrets removed) and
  every login attempt is recorded in the metadata database, schema
  `igloo_audit`, for `IGLOO_AUDIT_RETENTION`. Users read their own history
  through `igloo.query_history`, `igloo.login_history` and `igloo.usage`;
  only admins see others'. The functions behind the views decide what to
  show from the session running them, so calling them directly shows no
  more.
- **Catalog.** The [catalog](docs/catalog.md) (descriptions, owners, sizes,
  usage and lineage, in `igloo.tables` and the other catalog views,
  `information_schema`'s additions, `igloo_comments()`, the policy and tag
  references, and the console's search and pages) shows users with the role
  none only the tables and views they have a grant on or own, and their
  schemas; of the others it shows nothing, not even names. A lineage edge
  to a table they may not see shows that end unnamed, and the statement
  that made an edge shows only to those who may see it in the history.
  Connections' tables show only to admins and to whoever has a grant that
  covers them, whatever their role; external tables' files, under the
  sharing proxy's secret prefix, are never recorded as locations read.
  Owning an object lets its owner set its comments and hand it on, and
  grants nothing else. Lineage events go only to the endpoint Igloo is
  configured with (`IGLOO_OPENLINEAGE_URL`), over `https`; never to
  link-local, reserved or cloud metadata addresses, nor IPv6 addresses
  that carry one, and to private ones only when allowed; to the addresses
  checked, not ones a name resolves to later; never through a proxy or a
  redirect. They carry no query results, nor statements' text unless
  configured.
- **Loading and scheduled work.** `COPY INTO` loads only from the warehouse
  and `IGLOO_ALLOWED_LOCATIONS`, never the server's own directories, and is
  checked as the `INSERT ... SELECT FROM read_csv(...)` statements it runs,
  one per file, as the user's own: writing needs `INSERT` on the table, and
  reading files the reader role. Tasks, pipes and materialized view
  refreshes run as their owner, in a new session of the owner's, checked
  against the owner's role and grants at the time they run, so they never
  have more privileges than the owner has then. A dropped owner's tasks,
  pipes, materialized views and alerts are suspended and marked as a
  dropped user's (`ana (dropped)`, a name no user can have), so a user
  created later with the same name does not inherit them. Scheduling needs
  the writer role, and only owners and admins may change, run or drop
  scheduled work. Reading a materialized view
  needs `SELECT` on it, like a table.
- **Alerts and notifications** ([details](docs/alerts.md#security)). Only
  admins create notification integrations and grant `USAGE` on them. An
  alert runs as its owner, through the owner's grants and policies, and
  sends at most 20 rows of what its query returns, only to integrations
  its owner has `USAGE` on when it runs; `SYSTEM$SEND_NOTIFICATION` sends
  only text written in the statement. What any notification carries is
  capped: its text, details, and a result's rows, columns and bytes.
  Integrations' URLs, headers and signing secrets are stored encrypted
  (AES-256-GCM, under a key in `igloo_auth.secrets`, as connections'
  passwords are), never shown, and replaced by `***`, along with the parts
  of header values, in errors, logs, the query history and the alert
  history; errors quote a destination's answer only when it is a short
  error code. Values from data are escaped for Slack and Teams, so they
  cannot mention people or make links. Failed-login notices, which clients
  can cause, are raised only for addresses and existing users, at most 5 in
  15 minutes from a replica, and cannot use the queue or rate limit the
  other alerts need.
- **Outbound connections for notifications.** Besides the catalog, object
  storage, the identity provider, and the databases and Delta Sharing
  servers admins connect it to, Igloo connects out only to send
  notifications. These connections reach only public addresses: Igloo
  resolves the host for
  every connection, refuses it if any address is loopback, private,
  link-local (which holds cloud metadata endpoints), multicast, reserved or
  an IPv6 form of one, connects to the address it checked, and checks it
  again as the socket connects, so DNS rebinding cannot redirect it. It
  uses no proxy, follows no redirects, and requires TLS: `https` for
  webhooks, implicit TLS or STARTTLS for mail.
  `IGLOO_NOTIFICATION_ALLOW_HOSTS` exempts the hosts it names, which may
  then be private and unencrypted; list only hosts meant to receive
  notifications. The Helm chart's network policies restrict ingress only;
  restrict egress there too if you need a second line.
- **Query sandbox.** Queries run in an embedded DuckDB that cannot read or
  write files on the server's local disk and cannot reach any URL outside the
  warehouse location and the prefixes in `IGLOO_ALLOWED_LOCATIONS`. DuckDB
  extensions are installed in the image at build time; the server does not
  download code while running. The spatial extension's GDAL readers and
  writers (`ST_Read`, `ST_Read_Meta`, `FROM '*.gpkg'` and `'*.fgb'`, `COPY
  ... (FORMAT GDAL)`) are refused for everyone: GDAL opens files that a
  dataset names inside itself, such as a VRT file's sources, on its own and
  outside the sandbox. PROJ, behind `ST_Transform`, can open a local file
  that a coordinate system definition names, without returning its
  contents ([details](docs/media-analytics.md#security)).
- **File paths.** For users other than admins, Igloo checks the files a
  statement names itself, before DuckDB opens them, in file-reading
  functions (`read_parquet`, `read_csv`, `glob`, `iceberg_scan` and the
  like), `COPY FROM`/`TO`, `COPY INTO`, pipes and replacement scans, whether
  or not a table has a policy. A file must be named as a plain string
  literal (not a value built as the statement runs) with no way to climb out
  of its prefix: no `.`/`..` segments, no percent-encoding (so `%2e`/`%2f`
  cannot stand in for a separator), no backslashes, no `?` or `#`, and no
  empty segments. This is needed because DuckDB 1.5.6 prefix-matches a
  remote path against `allowed_directories` without canonicalising it, so a
  path such as `https://host/allowed/../other` or an `s3://` path with a
  `?s3_endpoint=` query would otherwise pass its check and reach elsewhere.
  Allowed locations are validated and given a trailing slash at startup so a
  prefix cannot match a sibling (`s3://b/land` not matching
  `s3://b/landing-private`). Without a policy, readers and writers may still
  read the warehouse's own files; with one, those go through the tables.
- **Transport.** TLS is on when `IGLOO_TLS_CERT_FILE` and `IGLOO_TLS_KEY_FILE`
  are set, and unencrypted SQL sessions are then refused unless
  `IGLOO_TLS_REQUIRED=false` (the web console and Flight SQL then serve TLS
  only). Without TLS, queries and results cross the
  network unencrypted. Enable TLS before accepting connections from outside a
  private network. The single-machine stack accepts local connections only by
  default.
- **Catalog.** The Lakekeeper catalog and its PostgreSQL database must not be
  reachable by SQL clients or from the internet. Whoever can reach the catalog
  can change table metadata and may obtain object-storage credentials. Keep it
  on a private network that only Igloo can reach; other engines use the
  catalog through Igloo instead. A client that reaches it and asks it to
  drop a table with a purge deletes every file under the table's location,
  files that clones of the table read included; Igloo never asks for one,
  and refuses engines' purges of tables whose files clones read.
- **Other engines.** Port 8181 (`IGLOO_ICEBERG_ADDR`) serves the Iceberg REST
  catalog to Spark, Trino and other engines
  ([details](docs/external-engines.md)). Engines get an access token by
  sending an Igloo user's password or Igloo access token (OAuth2 client
  credentials), checked like a SQL login, with the same failed-login delay,
  limits and login history; at most 32 sign-ins run at once. Tokens are
  encrypted and authenticated with a key every replica shares, last an hour,
  never longer than the access token they were made from, and end at the
  next request when the user is dropped or disabled, their password changes,
  or that access token is dropped. Every request
  is checked against the user's role and grants, which are read again each
  time, before Igloo forwards it to Lakekeeper, and is recorded in the query
  history. Engines never learn Lakekeeper's address and never receive Igloo's
  storage keys: with a table they get STS credentials (an hour) whose session
  policy allows only that table's files, read-only unless the user has
  `INSERT`, `UPDATE` and `DELETE` on it, which any commit that changes the
  table's snapshots needs as well. On S3-compatible stores Igloo first
  checks that the store enforces the session policy, and hands out nothing if
  it does not; on Google Cloud Storage and Azure it hands out nothing yet.
  Clones, whose files lie under their sources' locations, get no
  credentials; write credentials for a table whose files clones read
  cannot delete files, and when Igloo cannot tell whether clones do, they
  cannot either.
  Only admins may choose where a table's or schema's files go, move a table
  or change its UUID, so every table has a directory no other table shares;
  Lakekeeper refuses moves and new UUIDs too, and Igloo hands non-admins
  credentials only for a table in the directory the catalog gave it. Names
  in paths are read as Lakekeeper reads them, nested namespaces are for
  admins only, and a commit naming another table than its path is refused,
  so a request is checked against the table it changes. Tables with masking
  or row access policies, which Igloo enforces in the SQL it runs, cannot be
  loaded or changed through the endpoint except by admins: their files hold
  the unmasked data. Without TLS the port serves clients on private networks
  only, since passwords, tokens and credentials cross it (behind a proxy,
  such as the scale-to-zero gateway, the proxy's address counts, so enable
  TLS before exposing the proxy); with TLS it serves HTTPS only. What
  remains: storage credentials outlive a revoked grant, a dropped user or
  token, or a newly attached data policy by up to an hour; an engine with
  write credentials for a table can
  rewrite that table's files and manifests as it likes, including making them
  name files of tables whose paths it knows, which Igloo would then read with
  its own access when that table is queried (paths contain random IDs, shown
  only to those who may read the table); and engines that bring their own
  storage access are bound by Igloo's grants in the catalog only.
- **Health and metrics.** Port 8080 (`/healthz`, `/readyz`, `/metrics`) has no
  authentication. Expose it only to your orchestrator and monitoring.
- **Web console.** Port 8443 serves the [web console](docs/console.md). Users
  sign in with their Igloo password, checked against the same SCRAM verifier,
  with the same failed-login delay, limits and logging as SQL logins, or
  with single sign-on. The browser sends the password inside HTTPS; without
  TLS the console accepts sign-ins, single sign-on included, only from a
  browser at `localhost`. Every statement runs as the signed-in user through
  the same authorization as SQL clients, and admin pages are refused to
  non-admins on the server. Sessions are encrypted, signed, `HttpOnly`,
  `SameSite=Strict` cookies (`Secure` under TLS) that expire after 12 hours
  or an hour idle and end when the user is dropped or disabled or their
  password changes. Changes need a CSRF token; pages have a strict
  Content-Security-Policy and cannot be framed. Set `IGLOO_CONSOLE_ADDR=` to
  turn the console off.
- **Arrow Flight SQL.** Port 31337 serves [Arrow Flight SQL](docs/flight-sql.md)
  (gRPC). Clients sign in with a handshake carrying their Igloo password in
  HTTP basic authentication, checked against the same SCRAM verifier with
  the same failed-login delay, logging and login history as SQL logins.
  Basic authentication sends the password itself, so with TLS configured the
  port serves TLS only, and without TLS it serves only clients that connect
  to it as `localhost` (on the same machine, or through an SSH tunnel or
  `kubectl port-forward`); others are refused before their password is
  checked. The handshake returns a random bearer token that is valid only on
  the connection it was issued on and ends with it. Every statement and
  metadata call runs as the user through the same authorization, sandbox,
  statement timeout and query history as SQL clients. Set
  `IGLOO_FLIGHT_ADDR=` to turn it off.
- **Delta Sharing.** Port 8444 serves [Delta Sharing](docs/sharing.md) to
  outside recipients: over HTTPS, or without TLS only to requests addressed
  to `localhost`. Only admins create shares, recipients and providers.
  Recipients authenticate with 256-bit bearer tokens that Igloo stores as
  SHA-256 hashes and checks in constant time; tokens expire, can be rotated,
  and are delivered through one-time activation links that only a button
  press redeems. A recipient reads only the shares granted to it, and only
  the current version of a table unless the table was shared `WITH
  HISTORY`. Data files are downloaded from URLs Igloo signs and serves,
  each for one file, one token and one hour, with the path encrypted; every
  download rechecks the token and the grant, so revocation is immediate on
  the replica that ran it and within ten seconds on the others. Recipients
  never receive storage credentials. Rows deleted from a table are never
  served (files with deletes are served as copies with them applied), and
  tables with renamed or dropped columns, whose files may hold data the
  table no longer shows, are refused. Every recipient request is in the
  query history. Partners' bearer tokens (`CREATE PROVIDER`) are stored
  encrypted and kept out of the history. DuckDB reads partners' files only
  through a loopback proxy, under a random path, that serves files the
  provider listed for an external table and does not connect to loopback,
  link-local or multicast addresses other than the provider's endpoint;
  the sandbox is otherwise unchanged. Set `IGLOO_SHARING_ADDR=` to turn the
  endpoint off. Tables with masking or row access policies are never
  shared: recipients read files with their own tools, which cannot apply
  Igloo's policies. `ALTER SHARE ... ADD TABLE` refuses them, and a shared
  table that gains a policy is refused to recipients from then on (on other
  replicas within 30 seconds), downloads of files listed before included,
  with each refusal and the policies behind it in the query history; until
  Igloo has loaded the policies, the endpoint serves no table. Policies do
  not attach to external tables, and while tables have policies users
  other than admins cannot read external tables' files through the proxy,
  nor the copies of shared files in the warehouse. Recipients' tokens
  (`dst_`) and Igloo's access tokens (`igloo_`) are distinct: neither
  signs in where the other does.

## Verifying release images

Release images at `ghcr.io/akkio-inc/igloo` are signed with Sigstore keyless
signing from the release workflow, and carry SBOM and build provenance
attestations. To verify an image:

```sh
cosign verify ghcr.io/akkio-inc/igloo:1.0.0 \
  --certificate-identity-regexp '^https://github\.com/akkio-inc/Igloo/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

gh attestation verify oci://ghcr.io/akkio-inc/igloo:1.0.0 --repo akkio-inc/Igloo
```
