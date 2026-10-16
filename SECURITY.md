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
  `query_table()`, are limited to admins.
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
  that name a protected table, and renames of protected tables and columns.
  Policy expressions are stored with their tables qualified and may not
  call macros, read files or depend on session state, and a statement is
  refused if a macro of its session replaces a function a policy calls, or
  its session changed a setting that changes what expressions mean
  (`integer_division`, for one).
  While any table has policies, users other than admins may not read the
  warehouse's files directly (file functions, `COPY INTO`, pipes) nor call
  table functions that read tables by name. Tasks, pipes and materialized
  views run through their owners' policies. Policies are not a boundary for admins, who manage them and can
  read the warehouse's files, nor for other engines reading the Iceberg
  tables directly. Policy changes reach other replicas within 30 seconds.
- **History.** Every statement (with passwords and secrets removed) and
  every login attempt is recorded in the metadata database, schema
  `igloo_audit`, for `IGLOO_AUDIT_RETENTION`. Users read their own history
  through `igloo.query_history` and `igloo.login_history`; only admins see
  others'. The functions behind the views decide what to show from the
  session running them, so calling them directly shows no more.
- **Loading and scheduled work.** `COPY INTO` loads only from the warehouse
  and `IGLOO_ALLOWED_LOCATIONS`, never the server's own directories, and is
  checked as the `INSERT ... SELECT FROM read_csv(...)` statements it runs,
  one per file, as the user's own: writing needs `INSERT` on the table, and
  reading files the reader role. Tasks, pipes and materialized view
  refreshes run as their owner, in a new session of the owner's, checked
  against the owner's role and grants at the time they run, so they never
  have more privileges than the owner has then; a dropped owner's work is
  suspended. Scheduling needs the writer role, and only owners and admins
  may change, run or drop scheduled work. Reading a materialized view
  needs `SELECT` on it, like a table.
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
  catalog through Igloo instead.
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
