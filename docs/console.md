# Web console

Igloo serves a web console for running SQL and looking after the
warehouse from a browser: worksheets, a data browser, a catalog with owners,
descriptions and lineage, query history, tasks and loads, and pages for
administrators to manage users, roles and grants, and to watch the
replica's sessions. It is part of the `igloo` binary; there is nothing else to
install or run.

Open `http://127.0.0.1:8443` on the machine running the single-machine stack
(`make up`), and sign in with an Igloo user: the same name and password as
for `psql`. With TLS configured, the console is at `https://<host>:8443`.
With [single sign-on](sso.md) configured, the sign-in page also offers
*Sign in with SSO*, through your identity provider.

## Pages

**Worksheets.** A SQL editor with line numbers and highlighting. Run the
whole worksheet, or select some text and run just that, with Ctrl+Enter
(Cmd+Enter on a Mac); Cancel stops the running statement. A worksheet runs
its statements one at a time, as psql does, and stops at the first error,
which is shown with its SQLSTATE and the line its statement starts on. Each
statement's result appears in its own tab, with column names and types, the
command tag, the row count and the time taken. The grid shows the first
10,000 rows of a result; Download CSV runs the query again and saves every
row. It runs the query in a new session, after making again the settings
that statements before it in the run made (`SET`, `RESET`, `USE`,
`DISCARD`, and Snowflake's `ALTER SESSION` and `UNSET`), so that it reads
the same tables with the same settings. Nothing that changes data is run
again: the download reads tables as they are when it runs. It refuses a
query that ran after temporary objects were made, inside a transaction
block, or after a setting computed by an expression
(`SET VARIABLE x = (SELECT ...)`), which it could not match without
running more. Worksheets are kept in the browser's local storage, per
user, and results only in memory: nothing about them is stored in Igloo.
Console tabs open side by side share them: a worksheet added, changed or
closed in one tab shows in the others.

Keys in the editor: Tab and Shift+Tab indent and outdent, Escape leaves the
editor. In the results grid: arrow keys move, Enter (or a double click)
opens a value in full, Ctrl+C (Cmd+C) copies it.

**Data.** Schemas, tables and views with their columns, types and comments, a
view's definition, and a sample of 100 rows. A table's row access policy and
each column's masking policy and tags show as badges
([policies](security-policies.md)); the sample shows what policies let the
signed-in user see. Query in a worksheet opens a worksheet with a `SELECT` on
the table, and Catalog page its page in the catalog.

**Catalog.** Search over the names, descriptions, columns, column
descriptions, tags and owners of every table and view the signed-in user
may see, external tables and connections' tables included, most used first
(see [the data catalog](catalog.md)). Each has a page, which can be linked
to (`#/catalog/catalog/schema/name`, the catalog being the Iceberg
catalog's or a connection's name): its
description, owner, size and freshness, reads, writes and users of the last
30 days, its columns with their descriptions, tags and masking policies, a
sample of its rows as the user sees them, and its lineage, drawn as a graph
of the tables and locations its data came from and the tables made from it,
three steps each way. Tables the user may not see are counted in the graph,
as "1 hidden upstream table", never named. Owners, writers and admins can
edit descriptions there, which the console does with `COMMENT ON`.

**Query history.** Queries and sign-ins, filtered by user, outcome, time and
text, read from `igloo.query_history` and `igloo.login_history`. The
console's own lookups, which its pages run to show themselves (a catalog
page reads `igloo.tables`, `igloo.columns`, `igloo.lineage` and more; the
Tasks page runs `SHOW TASKS`), are left out unless **Console lookups** is
ticked. They are recorded all the same, with `application_name` `Igloo
console lookup`; what users run in worksheets and the changes they make on
the console's pages, sample rows included, have `Igloo console`. On a
server without those views the page says that history is not available.

**Tasks.** [Tasks, pipes, materialized views](loading.md) and
[alerts](alerts.md), each with its owner, state, schedule, next run and
last outcome, from `SHOW TASKS`, `SHOW PIPES`, `SHOW MATERIALIZED VIEWS`
and `SHOW ALERTS`. Selecting one shows its definition and its recent runs
from `igloo.task_history` (for a pipe, the files it loaded; for an alert,
what it found and sent, from `igloo.alert_history`), and buttons that run
it now (`EXECUTE TASK`, `REFRESH MATERIALIZED VIEW`, `EXECUTE ALERT`),
suspend it and resume it, which the server allows its owner and admins.
Admins see everyone's; other users their own.

**Load history.** The files `COPY INTO` and pipes loaded, with their
status, rows and first error, from `igloo.load_history`, filtered by table,
status, time and file name. Admins see every load; other users the files
they and their pipes loaded.

**Clean rooms.** The [clean rooms](clean-rooms.md) you take part in (every
room for admins): each room's templates with their SQL and who has approved
the current version, its run history, its datasets with their policies, and
its collaborators. Buttons approve a template for the collaborators you act
for and run it, asking for its parameters, and the latest result of yours
opens in a grid or a worksheet. They send `ALTER CLEAN ROOM ... APPROVE
TEMPLATE` and `RUN CLEAN ROOM`, which the server allows as it would from any
SQL client.

**Access tokens.** Create, list and revoke your own
[access tokens](sso.md#access-tokens), which SQL clients such as dbt and BI
tools send instead of a password. A new token is shown once, with a copy
button; this is how people who sign in with single sign-on, and have no
password, connect SQL clients.

**Users** (admins). Create users (with a password, or with an email for
someone who will sign in with single sign-on), change their roles, set new
passwords, disable and enable them, and drop them. The list says how each
user signs in.

**Service accounts** (admins). Create [service accounts](sso.md#service-accounts)
for programs, give them access tokens (shown once), disable and drop them,
and see and revoke every user's tokens.

**Roles and grants** (admins). Lists roles and grants with `SHOW ROLES` and
`SHOW GRANTS`, and runs `GRANT`, `REVOKE`, `CREATE ROLE` and `DROP ROLE`
(see [roles and grants](sql.md#roles-and-grants)). On a server without
custom roles the page says so.

**Single sign-on** (admins). The single sign-on settings and whether Igloo
reaches the identity provider, the redirect URI to register there, the
mappings of the provider's groups to roles (`GRANT role TO SSO GROUP`), and
who has signed in with which groups.

**Compute** (admins). This replica's version and uptime, its open sessions
(SQL clients and console requests alike) with the statements running in
them, which can be canceled, when table maintenance last finished a pass,
whether a scale-to-zero gateway is watching the replica, and when
scheduled work is next due. Behind a load
balancer, each refresh may reach a different replica; the page names the one
it shows.

## How it works

Everything the console shows or changes, it reads or does with SQL, as the
signed-in user, through the same code path as a statement from a SQL client:
the same authorization, statement timeout, cancellation, metrics and logs.
The console itself never decides what a user may see or do. The admin pages
issue the statements an administrator would type in psql (`CREATE USER`,
`ALTER USER`, `DROP USER`, `SHOW USERS`, `CREATE TOKEN`, `SHOW TOKENS`,
`GRANT` and so on), and are also refused by the server to anyone who is not
an admin. The catalog pages read the [catalog's views](catalog.md) and
change comments with `COMMENT ON`. Only the compute page's sessions and the
single sign-on page's settings and linked users are read from the server
directly.

Each request runs in a SQL session of its own, which counts against
`IGLOO_MAX_CONNECTIONS` while it lasts and shows on the compute page as
`console <address>`, the session's `client_address` in the history. Settings such as `SET search_path` therefore last for
one run of a worksheet, and its downloads, not across runs. Closing the browser tab or pressing
Cancel cancels the running statement.

The console keeps no state on the server. Its session lives in a cookie,
encrypted and signed with a key every replica reads from the metadata
database, so any replica serves any request and a restart signs no one out.
Run several replicas behind one load balancer without sticky sessions.

The console sends nothing while you are not using it, so an open console
does not keep a [scale-to-zero](scale-to-zero.md) deployment awake.

## Security

- **Sign-in** checks the password against the user's stored SCRAM-SHA-256
  verifier, as a SQL login does: the same 500 ms delay after a failure, the
  same limits on repeated failures, the same `login` and `login failed` log
  lines (with `client=console`), login history and `igloo_logins_total`
  metric. Unknown users and wrong passwords fail alike. Access tokens do not
  sign in to the console, and service accounts cannot.
- **Single sign-on** uses the authorization code flow with PKCE; the
  sign-in's state, nonce and PKCE verifier wait in an encrypted,
  ten-minute, `HttpOnly` cookie (`SameSite=Lax`, so that it comes back with
  the identity provider's redirect), used once. Igloo verifies the ID
  token's signature, issuer, audience, expiry and nonce before it starts a
  session like a password sign-in's. Without TLS it works only at
  `localhost`, like password sign-in. See [single sign-on](sso.md).
- **TLS.** With `IGLOO_TLS_CERT_FILE` and `IGLOO_TLS_KEY_FILE` set, the
  console serves HTTPS only, with the same certificate as the SQL port.
  Unlike SQL clients, which use SCRAM, a browser sends the password itself,
  inside the HTTPS connection. Without TLS the console accepts sign-ins only
  from a browser that reached it as `localhost` (on the same machine, or
  through Docker's published ports, an SSH tunnel or `kubectl port-forward`),
  so a password does not cross a network unencrypted by mistake: the
  request must name `localhost` in its `Host` header and come from this
  machine or a private network. That is a guard, not an access control: a
  client on a private network that can reach the port can address it as
  `localhost`, so keep a console without TLS bound to loopback, as the
  single-machine setup does. Behind a load balancer or ingress that ends
  TLS, configure TLS on Igloo too, so the hop to Igloo is encrypted, and
  keep the request's `Host` header, which the console compares with the
  browser's `Origin`.
- **Sessions** end after 12 hours, after an hour without a request, at sign
  out, and at once when the user is dropped or disabled or their password
  changes. The user's role is read again on every request. The session
  cookie is `HttpOnly`, `SameSite=Strict`, and under TLS `Secure` with the
  `__Host-` prefix. Signing out removes the cookie from the browser; to end
  every session of a user everywhere, change their password, or disable
  them (`ALTER USER name NOLOGIN`) and enable them again: their old
  sessions stay ended.
- **Requests** that change anything need the session's CSRF token in a
  header and must come from the console's own origin; sign-ins must be JSON
  from the same origin.
- **Pages** carry a strict Content-Security-Policy (only the console's own
  scripts and styles, no inline code, no framing), `nosniff`, and
  `Referrer-Policy: no-referrer`. They render every value as text, never as
  markup. No secret is ever put in a URL.
- **Running statements** are visible to admins on the compute page, as
  `pg_stat_activity` shows them to PostgreSQL superusers. User-management
  statements, which carry passwords, are never shown or logged.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_CONSOLE_ADDR` | `:8443` | Where the console listens. Empty turns it off. |

The console uses the server's TLS settings and `IGLOO_MAX_CONNECTIONS`, and
the `IGLOO_OIDC_*` settings of [single sign-on](sso.md); there is nothing
else to configure. In the single-machine stack,
`IGLOO_CONSOLE_PORT` (8443) and `IGLOO_CONSOLE_BIND` (127.0.0.1) in
`deploy/single-vm/.env` choose where the host publishes it; set TLS up before
binding it to anything but loopback.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Sign-in says the connection is not encrypted | The console has no TLS and the browser did not reach it as `localhost`. Configure TLS, or use an SSH tunnel (`ssh -L 8443:127.0.0.1:8443 host`) or `kubectl port-forward`. |
| `password authentication failed` | Wrong user or password, the same as for psql. Access tokens do not sign in to the console. |
| `too many failed sign-ins` | 10 failures for the user, or 50 from the address, in 15 minutes; wait. |
| Signed out unexpectedly | The session was idle for an hour or is 12 hours old, or the password changed, or the user was disabled or dropped. |
| A single sign-on error on the sign-in page | See [single sign-on troubleshooting](sso.md#troubleshooting). |
| `sorry, too many clients already` | The replica is at `IGLOO_MAX_CONNECTIONS`; each running console request holds a session. |
| Only the first 10,000 rows | The grid's limit. Download CSV for every row. |
| History, roles, tasks or load history pages say "not available" | The server predates query history, custom roles, or tasks and loading. |
