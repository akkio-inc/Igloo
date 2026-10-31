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
| `IGLOO_TLS_CERT_FILE`, `IGLOO_TLS_KEY_FILE` | | PEM certificate and key. Enables TLS for SQL clients, and TLS only for the web console (HTTPS), Flight SQL, the Iceberg REST catalog (HTTPS) and the Delta Sharing endpoint (HTTPS). Without them the console accepts sign-ins only from a browser at `localhost`, Flight SQL serves only clients at `localhost`, the Iceberg REST catalog serves only clients on private networks, and the sharing endpoint answers only requests to `localhost`. At `localhost` means a request addressed to `localhost` from this machine or a private network, as through Docker's published ports, an SSH tunnel or `kubectl port-forward`. That guards against sending passwords and tokens across a network unencrypted by mistake; it is not an access control, since a client on a private network can address a port it reaches as `localhost`, so keep these ports bound to loopback without TLS, as the single-machine setup does. |
| `IGLOO_TLS_REQUIRED` | `true` | With a certificate configured, refuse sessions that do not use TLS. Set `false` to allow both. |
| `IGLOO_LOG_FORMAT` | `text` | `text` or `json` |
| `IGLOO_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `IGLOO_STATEMENT_TIMEOUT` | `0` (none) | Cancel statements that run longer, e.g. `30m`. Sessions can change their own limit with `SET statement_timeout`. |
| `IGLOO_MAX_CONNECTIONS` | `100` | Concurrent client sessions per Igloo process, counting Flight SQL sessions and web console requests while they run. Further logins get `too many clients`. As many SQL connections, but at least 100, may be signing in at once, at most half of them from one public client address (a loopback or private address, which the scale-to-zero gateway, a pooler or a Kubernetes Service gives all the clients it passes on, counts only toward the limit of all); further ones are refused with `too many clients` before they send anything. |
| `IGLOO_LOGIN_TIMEOUT` | `30s` | How long a SQL client may take to sign in, from connecting, through TLS and the password exchange, like PostgreSQL's `authentication_timeout`. A connection that takes longer is closed, and its login ends. |
| `IGLOO_SHUTDOWN_DELAY` | `0` | On SIGTERM, keep accepting connections this long (e.g. `5s`) while `/readyz` reports not ready, so load balancers stop routing new connections first. Running queries then get 30 seconds to finish. |
| `IGLOO_POOL` | `default` | The [compute pool](workload-management.md#compute-pools) this replica belongs to, recorded with every statement in the history and `igloo.usage`: up to 63 lowercase letters, digits, hyphens and underscores. |
| `IGLOO_PRICE_PER_HOUR` | `0` | What this replica's machine costs an hour, e.g. `0.58`, for the cost estimates of [`igloo.usage`](workload-management.md#usage-and-cost). `0` means unknown, and costs are `NULL`. |

## Users

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_ADMIN_USER` | `igloo` | Name of the first administrator |
| `IGLOO_ADMIN_PASSWORD` | (required) | That administrator's password. Used to create the account on first start only; afterwards change it with `ALTER USER`. Igloo creates the account only with at least 16 characters, and refuses placeholders and easily guessed values whatever their length: anything containing `change-me` or `changeme`, `password`, `admin`, `igloo`, `secret`, `default`, the user's name, or one character repeated. Use a long random value, e.g. from `openssl rand -base64 24`; `make up` (`deploy/single-vm/init-env.sh`), the Helm chart and the OpenTofu modules generate 32 characters. Once the account exists the variable is not used, so a weaker value only logs a warning, without the value. Without `IGLOO_METADATA_DATABASE_URL` the account is created on every start, and always checked. |
| `IGLOO_METADATA_DATABASE_URL` | | PostgreSQL URL where users, roles and grants, access tokens (hashed) and single sign-on links are stored (schema `igloo_auth`), with the keys that seal web console sessions, Iceberg REST tokens, connection passwords and notification integrations' secrets, the query and login history (schema `igloo_audit`), tasks, pipes, materialized views, alerts and the load history (schema `igloo_tasks`), masking policies, row access policies and tags (schema `igloo_policy`), Delta Sharing's shares, recipients, providers and external tables (schema `igloo_sharing`), and notification integrations, encrypted, and the alert history (schema `igloo_notify`). It can be the same database the catalog uses, as long as no database role is named `igloo_auth`, `igloo_audit`, `igloo_tasks`, `igloo_policy`, `igloo_sharing` or `igloo_notify`. Without it, the administrator is the only account, `CREATE USER`, access tokens, single sign-on and Delta Sharing are unavailable, console sessions and Iceberg REST tokens end when Igloo restarts, each replica keeps only recent history in memory, and tasks, pipes, alerts, notification integrations, the load history and policies are lost on restart. |
| `IGLOO_ACCESS_MAX_STALENESS` | `2m` | Each replica reads its sessions' users, grants and the policies from the metadata database again every 30 seconds, and ID domains' members every 5 seconds. While it cannot, it goes on with what it read last for up to this long, then refuses statements with SQLSTATE `58000`, and Delta Sharing requests for tables with `503`, until it can read them again, since a user disabled, a token or grant revoked or a policy attached meanwhile would not apply. `30s` or less refuses them as soon as one read fails. |
| `IGLOO_ERASURE_KEY` | | At least 32 random characters, the same on every replica: the key that seals the identifiers of [erasure requests](privacy.md) while they run, and salts their hashes. Spaces and line breaks around it are dropped. Keep it outside the metadata database, which never holds it, so that the database alone tells nothing about whose data was erased. A replica with another key, or none, leaves open requests to the replicas with the right one, logging at error level: set it back, and they go on. Without it, and with a metadata database, `ERASE SUBJECT` is refused (`DRY RUN` works); without a metadata database, each replica makes a key of its own, as its requests are kept in memory. The single-VM setup generates one. |
| `IGLOO_ERASURE_KEY_FILE` | | A file that holds the erasure key instead, as a Kubernetes secret or Docker secret mounts one; spaces and line breaks around it are dropped. Set it or `IGLOO_ERASURE_KEY`, not both. |
| `IGLOO_CONNECTION_SECRETS_DIR` | | Absolute path of a directory of files, such as a mounted Kubernetes Secret, holding the passwords of [connections to other databases](federation.md#credentials) that name them with `password_secret`, the private keys of Snowflake connections that name them with `private_key_secret`, and the tokens and credentials of Iceberg REST catalogs that name them with `token_secret`, `credential_secret` or `storage_credential_secret`. Igloo reads a file each time it signs in to that database, so rotating it there needs no restart. Without it, connections store their passwords, keys and tokens, encrypted, in the metadata database. |

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
| `IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM` | | For providers whose access tokens have a subject other than the ID token's: the claim holding the account's immutable ID, `uid` for Okta or `oid` for Entra ID. Such a token is the linked user's when it matches the ID recorded at their last console sign-in. Needs `IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE`. |
| `IGLOO_TOKEN_MAX_LIFETIME` | `365d` | Longest an access token may last (`CREATE TOKEN ... EXPIRES`). Tokens last 90 days by default. |

## Query history

Igloo records every statement and login in `igloo.query_history` and
`igloo.login_history`; see [query and login history](operations.md#query-and-login-history).

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_AUDIT` | `true` | Record the query and login history |
| `IGLOO_AUDIT_RETENTION` | `90d` | How long history is kept, e.g. `30d` or `720h`: queries, logins, runs of tasks, pipes and alerts, the alert history, and failed loads. Files loaded stay in the load history as long as their table exists. |

## Alerts and notifications

[Alerts and notifications](alerts.md) go to webhooks, Slack, Microsoft
Teams and email. Webhooks need no setting; email needs a mail server.
Notifications reach only public addresses, over TLS, unless the allow list
names the host.

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_NOTIFICATION_ALLOW_HOSTS` | | Comma-separated host names and IP addresses, as written in integrations' URLs and `IGLOO_SMTP_HOST`, that notifications may reach although they are not public, such as a webhook receiver or a mail relay on the private network, and over plain HTTP or SMTP. No wildcards; list only hosts meant to receive notifications. |
| `IGLOO_SMTP_HOST` | | The mail server email integrations send through. Empty: no email integrations. |
| `IGLOO_SMTP_PORT` | `587` | Its port. On 465 Igloo speaks TLS from the start; on others it requires STARTTLS, unless the host is in `IGLOO_NOTIFICATION_ALLOW_HOSTS`. |
| `IGLOO_SMTP_FROM` | (required with a host) | The sender of every email, such as `alerts@agency.com`. |
| `IGLOO_SMTP_ALLOWED_DOMAINS` | (required with a host) | Comma-separated email domains integrations may send to, such as `agency.com`. Recipients elsewhere are refused. |
| `IGLOO_SMTP_USERNAME` | | The user to sign in to the mail server as, with PLAIN authentication inside TLS. Empty: no sign-in. |
| `IGLOO_SMTP_PASSWORD_FILE` | | File holding its password, such as a mounted Kubernetes Secret. |
| `IGLOO_SMTP_PASSWORD` | | The password itself, if not in a file. |

## AI functions

[AI functions](ai-functions.md) are off until an admin creates an AI
integration with a provider's keys. These settings bound what they may do
once one exists. Providers are reached only at public addresses, over TLS,
unless the allow list names the host.

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_AI_ALLOW_HOSTS` | | Comma-separated host names and IP addresses, as written in AI integrations' `BASE_URL`, that AI integrations may reach although they are not public, and over plain HTTP: models served on the private network by vLLM, llama.cpp or Ollama. No wildcards. |
| `IGLOO_AI_CACHE` | `on` | Keep AI functions' results across statements, in the metadata database, so that a statement asking what an earlier one asked calls no provider. `off` keeps nothing. |
| `IGLOO_AI_CACHE_RETENTION` | `30d` | How long a cached result is kept, e.g. `7d` or `12h`. Erasure requests purge what was derived from their tables sooner ([privacy](privacy.md)). |
| `IGLOO_AI_MAX_CALLS` | `1000` | Calls to providers a statement may make, unless its user's roles set `ai_max_calls`. A statement that would make more fails before it sends them. `0` is no cap. |
| `IGLOO_AI_MAX_COST_USD` | `1` | Estimated cost, in US dollars at the integrations' prices, of a statement's calls, unless its user's roles set `ai_max_cost_usd`. `0` is no cap. |
| `IGLOO_AI_REQUEST_TIMEOUT` | `2m` | How long each request to a provider may take; one that takes longer is sent again, up to five times in all. |

## Model pack

The [model pack](models.md) holds the open-weight models Igloo runs in its
own processes. An admin installs them with `igloo models install`; nothing
is downloaded at query time. Set the same directory on servers and workers.

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_MODEL_DIR` | | The pack's directory, an absolute path. Unset, no model is installed, and functions that need one fail saying so. |
| `IGLOO_MODEL_MIRROR` | | A base URL from which `igloo models install` reads `<model>/<file>` in place of the files' upstream URLs, for hosts that cannot reach Hugging Face. Files are checked against their pinned SHA-256 all the same. |

## Lineage events

Statements that write tables can also be sent as
[OpenLineage](catalog.md#openlineage) events, to Marquez, DataHub or any
other OpenLineage endpoint.

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_OPENLINEAGE_URL` | | Where events are POSTed, `https`. Empty sends nothing. |
| `IGLOO_OPENLINEAGE_API_KEY` | | Sent as a bearer token |
| `IGLOO_OPENLINEAGE_NAMESPACE` | `igloo` | The namespace of Igloo's jobs and tables in events |
| `IGLOO_OPENLINEAGE_SEND_SQL` | `false` | Include each statement's text, with passwords and secrets removed |
| `IGLOO_OPENLINEAGE_ALLOW_PRIVATE` | `false` | Allow an endpoint at a private or loopback address, and `http`. Link-local addresses, such as cloud metadata services, are always refused. |

## Catalog

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_CATALOG_URI` | | Iceberg REST catalog URL, e.g. `http://lakekeeper:8181/catalog`. Empty runs without persistence (development only). |
| `IGLOO_CATALOG_WAREHOUSE` | `igloo` | Warehouse name in the catalog |
| `IGLOO_CATALOG_NAME` | `lake` | Catalog name in SQL (`lake.public.events`) |
| `IGLOO_CATALOG_SCHEMA` | `public` | Default schema for unqualified table names |
| `IGLOO_CATALOG_SETUP` | `true` | On start, bootstrap the Lakekeeper catalog, create the warehouse if it does not exist, and limit it to Iceberg format versions 1 and 2, which [erasure requests](privacy.md) can rewrite. Safe with several replicas. `false` does none of it, and logs a warning that the format-version limit is then the operator's to set. |

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
| `IGLOO_ALLOWED_LOCATIONS` | | Comma-separated URL prefixes users may also read and write, e.g. a landing bucket for [`COPY INTO`](loading.md), pipes, `COPY` and `read_parquet`. Everything else outside the warehouse is blocked. Each entry is given a trailing slash so it matches only its own prefix (`s3://b/land` does not match `s3://b/landing-private`), and an entry with a `.`/`..` segment, percent-encoding, a backslash, `?`, `#` or an empty segment is refused at startup. Users other than admins may name files here only as plain string literals that cannot climb out of the prefix (see [security](../SECURITY.md)). |

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
| `GOMEMLIMIT` | what DuckDB's limit leaves | Go's soft memory limit for the rest of Igloo, e.g. `4GiB`. By default, the container's memory limit (or the machine's memory) less `IGLOO_DUCKDB_MEMORY_LIMIT` and a twentieth in reserve (at least 256 MiB), but at least 256 MiB; see [sizing](operations.md#sizing). |
| `IGLOO_DUCKDB_THREADS` | two per core, four on workers, as memory allows | DuckDB's threads. With the warehouse in object storage, two per core on the server and four on [workers](distributed-execution.md), but only as many as leave each 2 GiB of DuckDB's memory limit, and never fewer than the cores; without one, one per core. Reading object storage cold waits on requests more than on cores: on 8 cores, a cold scan read 64 MB/s on 8 threads, 100 MB/s on 16 and 172 MB/s on 32. Every thread keeps its own hash tables, so too many for the memory make aggregates and joins spill. The cores are those the container's CPU limit allows, or the machine's. |
| `IGLOO_DUCKDB_TEMP_DIR` | `/var/lib/igloo/tmp` in the image | Spill directory. Give it fast local disk, local NVMe where there is one ([spill on local NVMe](operations.md#spill-on-local-nvme)), and each Igloo process its own ([spill directory](operations.md#spill-directory)). |
| `IGLOO_DUCKDB_MAX_TEMP_SIZE` | 90% of the spill disk's free space | Cap on spill files, e.g. `40GiB`. Queries that need more fail. |
| `IGLOO_SORT_SPILL_BUDGET` | half the spill disk's free space | How much a write of rows in order (`CREATE TABLE ... AS` or `INSERT` with `ORDER BY`) may spill to sort them at once, e.g. `50GB`; one that would spill more is sorted [a range of the order at a time](sql.md#writing-rows-in-order). `off`: every write sorts at once. |
| `IGLOO_DUCKDB_EXTENSION_DIR` | set in the image | Pre-installed DuckDB extensions |

## Read cache

Igloo keeps copies of the table files it reads from object storage on local
disk, so that queries after a restart read them from there; see
[the read cache](operations.md#read-cache).

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_CACHE_SIZE` | automatic | Largest size of the cache, e.g. `50GB` or `40GiB`. Automatic is half of the disk space other files leave free, up to 100 GiB. `0` turns the cache off. |
| `IGLOO_CACHE_DIR` | `/var/lib/igloo/cache` in the image | Where the cache lives. Mount a volume here to keep it across restarts. |
| `IGLOO_PREFETCH` | `true` | Read ahead, into the read cache, the blocks scans are about to read, with requests of its own; see [prefetch](operations.md#prefetch). |
| `IGLOO_PREFETCH_CONCURRENCY` | `256` | Requests the prefetcher keeps in flight, for all statements together. |
| `IGLOO_MAX_CONCURRENT_STATEMENTS` | `off` | How many queries and changes of data may run at once on a replica, for every user together, e.g. `8`, or `auto` for as many as its cores; further ones wait in line. See [the replica's limit](workload-management.md#the-replicas-limit). |
| `IGLOO_STATEMENT_RESERVATIONS` | `auto` | The fraction of what a statement's shape allocated last that it reserves of the replica's memory before it starts, when its role sets no `memory_limit`: `auto` for all of it (1), a fraction up to 4, or `off`. See [reservations from history](workload-management.md#reservations-from-history). |
| `IGLOO_RESULT_CACHE_SIZE` | `256MiB` | Memory the results of queries Igloo keeps, to send them again to the same query while none of the tables it reads changes, may take; see [the result cache](operations.md#result-cache). `0` or `off` keeps none. |

## Table maintenance

Igloo compacts small files, expires old snapshots and deletes unreferenced
files on its own; see [table maintenance](operations.md#table-maintenance).

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_MAINTENANCE` | `true` | Run table maintenance. Turn it off only if another tool maintains the tables. |
| `IGLOO_ID_DOMAINS` | `false` | Let columns join [ID domains](sql.md#id-domains), which give identifiers dense integer ids. |
| `IGLOO_ID_SYNC_MIN_ROWS` | `0` | Rows a write to a member table of an [ID domain](sql.md#id-domains) must write for Igloo to give its new values ids before it returns; smaller writes leave them to a mapping pass about 30 seconds later, which the writes to a table meanwhile share. `0`: every write. |
| `IGLOO_BITMAP_INDEXES` | `false` | Let tables have [bitmap indexes](sql.md#bitmap-indexes) of ID domains' member columns, which Igloo builds and keeps up to date. Needs `IGLOO_ID_DOMAINS=true`. |
| `IGLOO_SHARED_PREPARES` | `true` | Prepare queries alongside other statements' prepares and binds, under the shared side of the catalog lock, so that a dashboard's statements are prepared at once rather than one at a time. `false` prepares every statement on its own. See [concurrency on a replica](operations.md#concurrency-on-a-replica). |
| `IGLOO_REPORT_REWRITE` | `false` | Answer the counting that reports write as SQL through the [report API](media-analytics.md#reports-as-written), where Igloo can tell the result is the same; the rest runs as written. |
| `IGLOO_BITMAP_MIN_ROWS` | `10000000` | Rows a statement counting ID domains' ids must read, as the catalog's metadata counts its tables' rows, for Igloo to count them in roaring bitmaps rather than DuckDB's hash; `0` counts every such statement in bitmaps, `off` none. See [ID domains](sql.md#id-domains). |
| `IGLOO_SNAPSHOT_RETENTION` | `7d` | How long old snapshots are kept, and so how far back time travel reaches, e.g. `1d` or `36h`. A table's `history.expire.max-snapshot-age-ms` property overrides it. |
| `IGLOO_MAINTENANCE_INTERVAL` | `1h` | Time between passes over every table |
| `IGLOO_MAINTENANCE_MEMORY` | a quarter of Go's memory limit | Memory compaction, and the rewrite of a table's files for an [erasure request](privacy.md), may hold, e.g. `2GB`: by default between 256 MiB (or half of Go's limit, if less) and 4 GiB, or 1 GiB when Igloo cannot tell how much memory it has. A table that cannot be compacted within it is left as it is, and an erasure request that cannot rewrite one within it stops with the reason; see [compaction memory](operations.md#compaction-memory). It also bounds, over `IGLOO_DUCKDB_THREADS`, the row groups inserts write into the tables Igloo creates; see [row groups](operations.md#row-groups). |

## Distributed execution

Statements that read very large tables can run in parts on workers;
[distributed-execution.md](distributed-execution.md) describes when, and how
to deploy workers. On the Igloo server:

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_WORKERS` | | Workers' addresses, `host[:port]` comma separated (port 7433 by default). A host may resolve to several workers, as a Kubernetes headless Service does. Empty turns distributed execution off. |
| `IGLOO_WORKER_SECRET` | (required with `IGLOO_WORKERS`) | At least 32 characters, shared with the workers, which accept only holders of it |
| `IGLOO_WORKER_COMPUTE` | | How Igloo starts and stops workers: `kubernetes`, `ec2`, `command`, or empty when they always run |
| `IGLOO_WORKER_MAX` | `4` | The most workers Igloo starts |
| `IGLOO_WORKER_IDLE_AFTER` | `auto` | Stop workers after this long without tasks; `auto` for three times what their last start took, from 2 to 10 minutes |
| `IGLOO_WORKER_WARM` | `1` | While some workers work, how many idle ones past them stay up; the others stop alone, on Kubernetes and EC2 (see [idle workers](distributed-execution.md#idle-workers)); `off` stops workers only all together |
| `IGLOO_WORKER_START_TIMEOUT` | `3m` | How long a statement waits for workers to start before running on the server |
| `IGLOO_WORKER_THRESHOLD` | `1m` | In `auto` mode, how long a statement must take on the server alone, by its estimate or its last run there, before it may run on workers (unless it would run short of memory), and then only if workers are estimated, or were measured, to be faster |
| `IGLOO_EXCHANGES` | `auto` | Where workers write the rows they exchange between a statement's stages: `auto`, their own disks (`IGLOO_DUCKDB_TEMP_DIR`), from which the workers that read them fetch them, when every worker of the statement reports at least 1 GiB of room there and runs outside Kubernetes, and the warehouse's object storage otherwise; `object`, always object storage; or `local`, always their disks (see [exchanges on workers' disks](distributed-execution.md#exchanges-on-workers-disks)). A worker whose disk is short of room writes to object storage instead |
| `IGLOO_WORKER_K8S_DEPLOYMENT`, `IGLOO_WORKER_K8S_NAMESPACE` | | `kubernetes`: the workers' Deployment, and its namespace if not Igloo's |
| `IGLOO_WORKER_EC2_INSTANCE_IDS` | | `ec2`: the workers' instances, comma separated |
| `IGLOO_WORKER_START_COMMAND`, `IGLOO_WORKER_STOP_COMMAND` | | `command`: shell commands that start and stop every worker |
| `IGLOO_WORKER_PRICE_PER_HOUR` | `0` | What a worker costs an hour, for weighing how many workers, or the large worker, a statement runs on (see [when](distributed-execution.md#when)); `0` for unknown, when the list price of `IGLOO_WORKER_INSTANCE_TYPE` stands for it (see [prices](distributed-execution.md#prices)) |
| `IGLOO_WORKER_INSTANCE_TYPE`, `IGLOO_WORKER_SPOT`, `IGLOO_CLOUD_REGION` | | The workers' instance type (`m7gd.4xlarge`, `Standard_D16pds_v6`, `n4-standard-16`), whether they run on Spot (`false`), and the region, for their list price where no price is set |
| `IGLOO_LARGE_WORKER_INSTANCE_TYPE`, `IGLOO_LARGE_WORKER_SPOT` | | The same for the large worker |
| `IGLOO_LARGE_WORKER` | | The pool's [large worker](distributed-execution.md#a-large-worker), `host[:port]` (port 7433 by default), which auto mode may run a statement on alone; empty for none. Needs `IGLOO_WORKERS` |
| `IGLOO_LARGE_WORKER_COMPUTE` | | How to start and stop it: `kubernetes` (`IGLOO_LARGE_WORKER_K8S_DEPLOYMENT`, in the workers' namespace), `ec2` (`IGLOO_LARGE_WORKER_EC2_INSTANCE_IDS`), `command` (`IGLOO_LARGE_WORKER_START_COMMAND`, `IGLOO_LARGE_WORKER_STOP_COMMAND`), or empty when it always runs |
| `IGLOO_LARGE_WORKER_CORES`, `IGLOO_LARGE_WORKER_MEMORY` | | Its cores and DuckDB memory limit (a size such as `400GiB`), for estimates while it is stopped and has not answered; required with `IGLOO_LARGE_WORKER_COMPUTE` |
| `IGLOO_LARGE_WORKER_PRICE_PER_HOUR` | `0` | What it costs an hour; `0` for unknown |

Distributed execution keeps partial results in `IGLOO_DUCKDB_TEMP_DIR`, which
must be set (the image sets it).

A worker (`igloo worker`) reads `IGLOO_WORKER_SECRET`,
`IGLOO_WORKER_LISTEN_ADDR` (`:7433`), `IGLOO_WORKER_SPOT_NOTICE` (`on`: on
EC2, take no more tasks and finish those running once AWS says it is taking a
Spot Instance back; `off`), `IGLOO_WORKER_ISOLATED_PURGE_AFTER` (`1m`, at
least `10s`: how long it goes without hearing from any coordinator before it
empties its caches by itself, for [erasure requests](privacy.md#workers)),
and the server's settings for storage
(`IGLOO_WAREHOUSE_LOCATION`, `IGLOO_ALLOWED_LOCATIONS`, `IGLOO_S3_*`,
`IGLOO_GCS_*`, `IGLOO_AZURE_*`), DuckDB (`IGLOO_DUCKDB_*`), the read cache
(`IGLOO_CACHE_*`), `IGLOO_HTTP_ADDR` (health checks and metrics) and logs.
It needs no catalog, database or users. With `IGLOO_EXCHANGES=local` on the
server, `IGLOO_EXCHANGE_MAX_SIZE` on a worker (a size such as `30GiB`) caps
what exchanges may take on its disk, for a volume whose free space the
worker cannot see, such as a Kubernetes `emptyDir` with a size limit.
