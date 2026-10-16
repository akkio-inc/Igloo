# Configuration

The `igloo` server is configured with environment variables. Deployment tools
(Docker Compose, the Helm chart, the OpenTofu modules) set these for you; this
page is the reference they are built on.

## Ports

| Port | Protocol | Purpose |
|---|---|---|
| 5432 | PostgreSQL wire protocol | SQL clients |
| 8080 | HTTP | `GET /healthz` (process is up), `GET /readyz` (catalog and storage reachable), `GET /metrics` (Prometheus), `GET /activity` (running and scheduled work, for the [scale-to-zero gateway](scale-to-zero.md)) |
| 8443 | HTTP, or HTTPS with TLS | The [web console](console.md), its [single sign-on](sso.md) callback `/sso/callback`, and [SCIM provisioning](sso.md#scim-provisioning) at `/scim/v2` |
| 8444 | HTTP, or HTTPS with TLS | The [Delta Sharing](sharing.md) endpoint, for recipients of shares |
| 31337 | Arrow Flight SQL (gRPC), with TLS when configured | [Flight SQL clients](flight-sql.md): ADBC, pyarrow, the Flight SQL JDBC driver |
| 8181 | HTTP, or HTTPS with TLS | The [Iceberg REST catalog](external-engines.md) for Spark, Trino, PyIceberg and other engines |

## Server

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_LISTEN_ADDR` | `:5432` | SQL listen address |
| `IGLOO_HTTP_ADDR` | `:8080` | Health and metrics listen address. Empty disables it. |
| `IGLOO_CONSOLE_ADDR` | `:8443` | [Web console](console.md) listen address. Empty disables it. |
| `IGLOO_FLIGHT_ADDR` | `:31337` | [Arrow Flight SQL](flight-sql.md) listen address. Empty disables it. |
| `IGLOO_ICEBERG_ADDR` | `:8181` | [Iceberg REST catalog](external-engines.md) listen address, for other engines. Empty disables it; it is off without `IGLOO_CATALOG_URI`. Without TLS it serves clients on private networks only. |
| `IGLOO_SHARING_ADDR` | `:8444` | [Delta Sharing](sharing.md) endpoint listen address. Empty disables it; shares can still be managed in SQL and served by other replicas. The endpoint runs only with a metadata database and a catalog. |
| `IGLOO_SHARING_URL` | | The sharing endpoint's public address, such as `https://data.example.com:8444`, for activation links and recipients' profile files. Must use `https://` unless it is `localhost`. Empty uses the address a recipient reaches Igloo at, and `localhost` in activation links. |
| `IGLOO_TLS_CERT_FILE`, `IGLOO_TLS_KEY_FILE` | | PEM certificate and key. Enables TLS for SQL clients, and TLS only for the web console (HTTPS), Flight SQL, the Iceberg REST catalog (HTTPS) and the Delta Sharing endpoint (HTTPS). Without them the console accepts sign-ins only from a browser at `localhost`, Flight SQL serves only clients at `localhost`, the Iceberg REST catalog serves only clients on private networks, and the sharing endpoint answers only requests to `localhost`. |
| `IGLOO_TLS_REQUIRED` | `true` | With a certificate configured, refuse sessions that do not use TLS. Set `false` to allow both. |
| `IGLOO_LOG_FORMAT` | `text` | `text` or `json` |
| `IGLOO_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `IGLOO_STATEMENT_TIMEOUT` | `0` (none) | Cancel statements that run longer, e.g. `30m`. Sessions can change their own limit with `SET statement_timeout`. |
| `IGLOO_MAX_CONNECTIONS` | `100` | Concurrent client sessions per Igloo process, counting Flight SQL sessions and web console requests while they run. Further logins get `too many clients`. |
| `IGLOO_SHUTDOWN_DELAY` | `0` | On SIGTERM, keep accepting connections this long (e.g. `5s`) while `/readyz` reports not ready, so load balancers stop routing new connections first. Running queries then get 30 seconds to finish. |
| `IGLOO_POOL` | `default` | The [compute pool](workload-management.md#compute-pools) this replica belongs to, recorded with every statement in the history and `igloo.usage`: up to 63 lowercase letters, digits, hyphens and underscores. |
| `IGLOO_PRICE_PER_HOUR` | `0` | What this replica's machine costs an hour, e.g. `0.58`, for the cost estimates of [`igloo.usage`](workload-management.md#usage-and-cost). `0` means unknown, and costs are `NULL`. |

## Users

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_ADMIN_USER` | `igloo` | Name of the first administrator |
| `IGLOO_ADMIN_PASSWORD` | (required) | That administrator's password. Used to create the account on first start only; afterwards change it with `ALTER USER`. |
| `IGLOO_METADATA_DATABASE_URL` | | PostgreSQL URL where users, roles and grants, access tokens (hashed) and single sign-on links are stored (schema `igloo_auth`), with the keys that seal web console sessions and Iceberg REST tokens, the query and login history (schema `igloo_audit`), tasks, pipes, materialized views and the load history (schema `igloo_tasks`), masking policies, row access policies and tags (schema `igloo_policy`), and Delta Sharing's shares, recipients, providers and external tables (schema `igloo_sharing`). It can be the same database the catalog uses, as long as no database role is named `igloo_auth`, `igloo_audit`, `igloo_tasks`, `igloo_policy` or `igloo_sharing`. Without it, the administrator is the only account, `CREATE USER`, access tokens, single sign-on and Delta Sharing are unavailable, console sessions and Iceberg REST tokens end when Igloo restarts, each replica keeps only recent history in memory, and tasks, pipes, the load history and policies are lost on restart. |

Users sign in with SCRAM-SHA-256, the PostgreSQL default, with a password
or an [access token](sso.md#access-tokens). Each has one built-in role, and
[grants](sql.md#roles-and-grants) can add privileges on schemas and tables:

| Role | Can |
|---|---|
| `admin` | Everything, including managing users, roles and grants |
| `writer` | Read and write tables, create and drop tables, views and schemas, set comments and table properties |
| `reader` | Read tables |
| `none` | Only what grants allow |

## Single sign-on and access tokens

People can sign in to the web console with an OpenID Connect identity
provider, and SQL clients with access tokens; see
[single sign-on](sso.md). Single sign-on needs `IGLOO_METADATA_DATABASE_URL`,
and brings [SCIM provisioning](sso.md#scim-provisioning), which needs no
setting of its own.

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_OIDC_ISSUER` | | The identity provider's issuer URL, `https` (plain `http` only at `localhost`). Empty turns single sign-on off. |
| `IGLOO_OIDC_CLIENT_ID` | | Igloo's client ID at the provider. Required with an issuer. |
| `IGLOO_OIDC_CLIENT_SECRET_FILE` | | File holding the client secret, such as a mounted Kubernetes Secret. |
| `IGLOO_OIDC_CLIENT_SECRET` | | The client secret itself, if not in a file. Leave both empty for a public client, which PKCE protects. |
| `IGLOO_OIDC_REDIRECT_URL` | from the request | The console's callback as registered at the provider, `https://<host>:8443/sso/callback`. Empty uses the address the browser reached the console at. |
| `IGLOO_OIDC_SCOPES` | `openid profile email` | Scopes to ask for, space or comma separated. Add `groups` for providers that send groups only when asked. |
| `IGLOO_OIDC_USERNAME_CLAIM` | `email` | Claim users are named and recognised by. `email` counts only when `email_verified` is true; use `preferred_username` for Microsoft Entra ID. |
| `IGLOO_OIDC_GROUPS_CLAIM` | `groups` | Claim listing the user's groups, for allowed groups and [group mappings](sso.md#groups-and-roles). |
| `IGLOO_OIDC_ALLOWED_DOMAINS` | | Comma-separated email domains whose people may sign in. Required for Google, where it is checked against the Workspace domain. |
| `IGLOO_OIDC_ALLOWED_GROUPS` | | Comma-separated groups; people in none of them may not sign in. |
| `IGLOO_OIDC_CREATE_USERS` | `true` | Create a user, with role `none` and no password, at the first sign-in of someone without one. `false` admits only users admins created. |
| `IGLOO_OIDC_PASSWORD_SIGNIN` | `true` | Let users linked to the provider also sign in with a password. `false` makes them use single sign-on in the console and access tokens in SQL clients. |
| `IGLOO_OIDC_DISPLAY_NAME` | `SSO` | Label of the console's sign-in button: *Sign in with ...*. |
| `IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE` | | When set, SQL clients of users without a password may send the provider's own access tokens for this audience, inside TLS ([details](sso.md#identity-provider-access-tokens)). |
| `IGLOO_TOKEN_MAX_LIFETIME` | `365d` | Longest an access token may last (`CREATE TOKEN ... EXPIRES`). Tokens last 90 days by default. |

## Query history

Igloo records every statement and login in `igloo.query_history` and
`igloo.login_history`; see [query and login history](operations.md#query-and-login-history).

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_AUDIT` | `true` | Record the query and login history |
| `IGLOO_AUDIT_RETENTION` | `90d` | How long history is kept, e.g. `30d` or `720h`: queries, logins, runs of tasks and pipes, and failed loads. Files loaded stay in the load history as long as their table exists. |

## Catalog

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_CATALOG_URI` | | Iceberg REST catalog URL, e.g. `http://lakekeeper:8181/catalog`. Empty runs without persistence (development only). |
| `IGLOO_CATALOG_WAREHOUSE` | `igloo` | Warehouse name in the catalog |
| `IGLOO_CATALOG_NAME` | `lake` | Catalog name in SQL (`lake.public.events`) |
| `IGLOO_CATALOG_SCHEMA` | `public` | Default schema for unqualified table names |
| `IGLOO_CATALOG_SETUP` | `true` | On start, bootstrap the Lakekeeper catalog and create the warehouse if it does not exist. Safe with several replicas. |

## Storage

`IGLOO_WAREHOUSE_LOCATION` says where table data lives; its scheme selects the
storage type. It is required whenever `IGLOO_CATALOG_URI` is set.

| Scheme | Example |
|---|---|
| Amazon S3 or S3-compatible | `s3://my-bucket/warehouse` |
| Google Cloud Storage | `gs://my-bucket/warehouse` |
| Azure Data Lake Storage Gen2 | `abfss://my-filesystem@myaccount.dfs.core.windows.net/warehouse` |

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_ALLOWED_LOCATIONS` | | Comma-separated URL prefixes users may also read and write, e.g. a landing bucket for [`COPY INTO`](loading.md), pipes, `COPY` and `read_parquet`. Everything else outside the warehouse is blocked. |

### S3 and S3-compatible stores

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_S3_ENDPOINT` | | `host:port` of an S3-compatible store. Empty means AWS. |
| `IGLOO_S3_REGION` | `us-east-1` | |
| `IGLOO_S3_URL_STYLE` | `vhost` | `path` for most self-hosted stores |
| `IGLOO_S3_USE_SSL` | `true` | |
| `IGLOO_S3_ACCESS_KEY_ID`, `IGLOO_S3_SECRET_ACCESS_KEY` | | Static keys. Empty uses the AWS credential chain (IAM roles for service accounts, instance profiles, environment). |
| `IGLOO_S3_STS_ROLE_ARN` | | IAM role Igloo assumes to hand [other engines](external-engines.md#amazon-s3) credentials limited to one table. Without it, engines on AWS get table metadata but no storage credentials. S3-compatible stores need none: Igloo calls STS on `IGLOO_S3_ENDPOINT` with its own keys. |

With static keys, the catalog uses the same keys. Without them it uses its own
AWS identity; run Lakekeeper with `LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS=true`
and `LAKEKEEPER__S3_ENABLE_DIRECT_SYSTEM_CREDENTIALS=true`.

### Google Cloud Storage

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_GCS_HMAC_KEY_ID`, `IGLOO_GCS_HMAC_SECRET` | (required) | HMAC key for a service account with access to the bucket. DuckDB reads and writes GCS with HMAC keys. |

The catalog uses its own GCP identity (Workload Identity); run Lakekeeper with
`LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS=true`.

### Azure Data Lake Storage Gen2

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_AZURE_ACCOUNT_KEY` | | Storage account key. Empty uses the Azure credential chain (Workload Identity, managed identity). |

With an account key, the catalog uses the same key. Without one it uses its own
Azure identity; run Lakekeeper with `LAKEKEEPER__ENABLE_AZURE_SYSTEM_CREDENTIALS=true`.

## Query engine

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_DUCKDB_MEMORY_LIMIT` | 80% of memory | e.g. `8GB`. Larger queries spill to disk. |
| `IGLOO_DUCKDB_THREADS` | one per core | |
| `IGLOO_DUCKDB_TEMP_DIR` | `/var/lib/igloo/tmp` in the image | Spill directory. Give it fast local disk. |
| `IGLOO_DUCKDB_MAX_TEMP_SIZE` | 90% of the spill disk's free space | Cap on spill files, e.g. `40GiB`. Queries that need more fail. |
| `IGLOO_DUCKDB_EXTENSION_DIR` | set in the image | Pre-installed DuckDB extensions |

## Read cache

Igloo keeps copies of the table files it reads from object storage on local
disk, so that queries after a restart read them from there; see
[the read cache](operations.md#read-cache).

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_CACHE_SIZE` | automatic | Largest size of the cache, e.g. `50GB` or `40GiB`. Automatic is half of the disk space other files leave free, up to 100 GiB. `0` turns the cache off. |
| `IGLOO_CACHE_DIR` | `/var/lib/igloo/cache` in the image | Where the cache lives. Mount a volume here to keep it across restarts. |

## Table maintenance

Igloo compacts small files, expires old snapshots and deletes unreferenced
files on its own; see [table maintenance](operations.md#table-maintenance).

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_MAINTENANCE` | `true` | Run table maintenance. Turn it off only if another tool maintains the tables. |
| `IGLOO_SNAPSHOT_RETENTION` | `7d` | How long old snapshots are kept, and so how far back time travel reaches, e.g. `1d` or `36h`. A table's `history.expire.max-snapshot-age-ms` property overrides it. |
| `IGLOO_MAINTENANCE_INTERVAL` | `1h` | Time between passes over every table |
