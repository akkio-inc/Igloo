# Single sign-on, access tokens and service accounts

People sign in to the [web console](console.md) through their company's
identity provider (Okta, Microsoft Entra ID, Google Workspace, Keycloak or
any other OpenID Connect provider). SQL clients, such as BI tools, dbt and
scripts, sign in with **access tokens**: secrets that stand in for a
password, which people create for themselves in the console. Programs run as
**service accounts**, users without a password that admins give tokens.

```sql
-- An admin maps the identity provider's groups to roles.
CREATE ROLE analysts;
GRANT analysts TO SSO GROUP 'Data Team';
GRANT SELECT ON SCHEMA marts TO analysts;

-- Someone who signed in with single sign-on makes a token for dbt.
CREATE TOKEN dbt EXPIRES '90d';

-- An admin sets up a service account for a scheduled job.
CREATE USER airflow_prod TYPE SERVICE ROLE none;
GRANT SELECT, INSERT ON SCHEMA staging TO airflow_prod;
CREATE TOKEN ci FOR USER airflow_prod EXPIRES '180d';
```

## How it works

- **Console sign-in.** The sign-in page shows *Sign in with SSO*. Igloo
  sends the browser to the provider with the OpenID Connect authorization
  code flow and PKCE, and when the provider sends it back, exchanges the
  code for an ID token and verifies it: the provider's signature (with the
  keys it publishes), the issuer, the audience (Igloo's client ID), the
  expiry and the sign-in's nonce. Then it admits the user by the allowed
  domains and groups, finds or creates their Igloo user, applies the group
  mappings, and starts an ordinary console session.
- **SQL clients** send an access token where they would send a password.
  The login is SCRAM-SHA-256, as for passwords, so the token itself never
  crosses the network, encrypted or not; Igloo keeps only hashes of it.
- **Service accounts** have no password and never sign in to the console;
  they exist to hold tokens for programs.
- **Provisioning.** Okta and Entra ID can also create, deactivate and group
  users ahead of sign-in with [SCIM](#scim-provisioning).

Single sign-on needs the metadata database (`IGLOO_METADATA_DATABASE_URL`),
which keeps who is who, and TLS on the console unless browsers reach it only
as `localhost`.

## Set up

### 1. Register Igloo at the identity provider

Create a web application (an OpenID Connect client) with:

- **Redirect URI** `https://<console host>:8443/sso/callback`, exactly as
  browsers reach the console (behind a load balancer, its address).
- **Grant** authorization code, with PKCE (S256) if the provider asks.
- **Client authentication** a client secret. Public clients without a
  secret also work, protected by PKCE alone.
- **Scopes** `openid profile email`, and `groups` where groups need it.

| Provider | `IGLOO_OIDC_ISSUER` | Notes |
|---|---|---|
| Okta | `https://<org>.okta.com`, or a custom authorization server such as `https://<org>.okta.com/oauth2/default` | Add a groups claim to the ID token (application *Sign On* tab, groups claim filter), and set `IGLOO_OIDC_SCOPES=openid profile email groups` if your authorization server needs the scope. |
| Microsoft Entra ID | `https://login.microsoftonline.com/<tenant ID>/v2.0` | Set `IGLOO_OIDC_USERNAME_CLAIM=preferred_username`: Entra ID does not verify the `email` claim. Add the groups claim under *Token configuration*; it holds group object IDs, so map those (`GRANT analysts TO SSO GROUP '5f1c…'`). With more than 200 groups Entra ID leaves them out; assign the groups that matter to the application and emit only those. |
| Google Workspace | `https://accounts.google.com` | Every Google account shares this issuer, so `IGLOO_OIDC_ALLOWED_DOMAINS` is required; Igloo checks it against the Workspace domain (`hd`), not the email. Google sends no groups: map roles in Igloo instead. |
| Keycloak | `https://<host>/realms/<realm>` | Add a *Group Membership* mapper to the client, token claim name `groups`, full group path off. |
| Dex, Authentik, Zitadel, others | the issuer they document | Anything that implements OpenID Connect discovery. |

### 2. Configure Igloo

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_OIDC_ISSUER` | | The provider's issuer URL. Empty turns single sign-on off. It must be `https` (plain `http` only at `localhost`, for trying it out). |
| `IGLOO_OIDC_CLIENT_ID` | | The client ID from step 1. |
| `IGLOO_OIDC_CLIENT_SECRET_FILE`, `IGLOO_OIDC_CLIENT_SECRET` | | The client secret, from a file (preferred: a mounted Kubernetes Secret) or the environment. |
| `IGLOO_OIDC_ALLOWED_DOMAINS` | | Comma-separated email domains whose people may sign in, such as `agency.com,brand.com`. Required for Google. |
| `IGLOO_OIDC_ALLOWED_GROUPS` | | Comma-separated groups; people in none of them may not sign in. |

The rest rarely need changing; [configuration](configuration.md#single-sign-on-and-access-tokens)
lists them all. With neither allowed domains nor groups, every account the
provider signs in may use Igloo (with role `none` until given more), and
Igloo says so in its log at start.

**Docker Compose**: set the variables in `deploy/single-vm/.env` and turn on
TLS there. **Helm**: the `sso` values, with the client secret in a Secret:

```sh
kubectl create secret generic igloo-oidc --from-literal=client-secret='<secret>'
helm upgrade igloo ./igloo --reuse-values \
  --set sso.issuer=https://agency.okta.com --set sso.clientId=0oa1b2c3 \
  --set sso.existingSecret=igloo-oidc --set 'sso.allowedDomains={agency.com}'
```

### 3. Map groups to roles

```sql
GRANT analysts TO SSO GROUP 'Data Team';       -- a custom role
GRANT reader TO SSO GROUP 'Igloo Users';       -- a built-in role
GRANT admin TO SSO GROUP 'Igloo Admins';
REVOKE reader FROM SSO GROUP 'Igloo Users';
SHOW SSO GROUPS;
```

Or on the console's **Single sign-on** page, which also shows the settings,
whether Igloo reaches the provider, the redirect URI to register, and who
has signed in with which groups.

## Who is who

At each sign-in, Igloo finds the user for the provider's account by, in
order:

1. an earlier sign-in of the same account (the provider's issuer and
   subject identifier, which never change);
2. a user whose email is the account's verified email;
3. a user named like the account (see below) who has no password, no email,
   no other account and no SCIM provisioning, as an admin creates one for
   single sign-on;
4. otherwise, unless `IGLOO_OIDC_CREATE_USERS=false`, a new user of that
   name, with role `none` and no password.

The user is then linked to the account, so later sign-ins take step 1.
Service accounts and disabled users are refused. A user with a password is
never taken over by name: link one by giving them the email their account
has (`ALTER USER ana EMAIL 'ana@agency.com'`).

The name comes from the username claim (`IGLOO_OIDC_USERNAME_CLAIM`, by
default the verified `email`): the part before the `@`, lowercase, with
other characters as `_`, so `Ana.Lopez@agency.com` becomes `ana_lopez`. If
that name belongs to someone else, the domain is added: `ana_lopez_agency_com`.
Agencies with people at several domains may prefer to create users ahead of
time with the name they want:

```sql
CREATE USER ana PASSWORD NULL EMAIL 'ana.lopez@brand.com' ROLE reader;
```

`SHOW USERS` says who signs in how (`has_password`, `sso`) and whether they
may (`can_login`). If someone's account at the provider is replaced, so its
subject changes, their next sign-in is refused as belonging to someone else
until an admin unlinks the old account: `ALTER USER ana SSO NULL` (console:
*Unlink SSO*). Igloo never relinks by itself, since a reused email may now be
another person's. For the same reason, once a user is linked, the
provider's [access tokens](#identity-provider-access-tokens) are theirs only
by the linked account's subject or immutable ID, never by email.

## Groups and roles

Group mappings apply at each sign-in, to the groups the provider's token
lists and, with [SCIM](#scim-provisioning), the groups it pushed:

- **Custom roles.** The user becomes a member of the custom roles their
  groups map to, and stops being a member of those single sign-on gave them
  before and their groups no longer do. Memberships an admin granted with
  `GRANT role TO user` are never removed.
- **Built-in roles.** Once any group maps to a built-in role, the identity
  provider decides the built-in role of every single sign-on user: at each
  sign-in it becomes the highest role their groups give, or `none`. Map a
  catch-all group to `reader` if everyone should read. Igloo never demotes
  the last enabled admin who is a person this way.

Without [SCIM](#scim-provisioning), roles change at sign-in only: a user who
leaves a group keeps its roles in SQL sessions and access tokens until they
next sign in to the console, or an admin changes them. With SCIM they change
as soon as the provider pushes the group. SCIM alone decides who is in a
group it pushes: a token that lists that group counts for nothing, so
removing someone from it at the provider takes its roles away at once. The
token's other groups still count, as without SCIM.

## Passwords and single sign-on users

Users created by single sign-on have no password. An admin may still give
one a password (`ALTER USER ana PASSWORD '...'`), for instance as a way in
if the provider is down. To make everyone linked to the provider use it,
with its MFA and policies, set `IGLOO_OIDC_PASSWORD_SIGNIN=false`: their
passwords are then refused in the console and in SQL clients, and they use
access tokens for SQL. Users not linked to the provider, such as the first
administrator, keep their passwords.

## Access tokens

```sql
CREATE TOKEN [name] [FOR USER service_account] [EXPIRES [IN] '90d']
SHOW TOKENS [FOR USER name]
DROP TOKEN [IF EXISTS] name [FOR USER name]
```

`CREATE TOKEN` returns the token, once:

```
 user_name | name |                                   token                                   |       expires_at
-----------+------+---------------------------------------------------------------------------+------------------------
 ana       | dbt  | igloo_k3j5…                                                               | 2027-01-14 10:12:03+00
```

The console's **Access tokens** page does the same, with a copy button.
Then use the token as the password of the user, in any client:

```sh
PGPASSWORD="$IGLOO_TOKEN" psql "host=igloo.example.com user=ana dbname=igloo sslmode=require"
```

```yaml
# dbt profiles.yml
igloo:
  target: prod
  outputs:
    prod:
      type: postgres
      host: igloo.example.com
      port: 5432
      user: ana
      password: "{{ env_var('IGLOO_TOKEN') }}"
      dbname: igloo
      schema: analytics
      sslmode: require
```

Tableau, Power BI, Looker, Metabase and DBeaver take it in their password
field; JDBC as `password=`. Spark, Trino and PyIceberg send it to the
[Iceberg REST catalog](external-engines.md#users-for-engines) as the OAuth
client secret, `credential=igloo_...` or `credential=user:igloo_...`; the
catalog's session lasts an hour at most, never longer than the token.

- **Expiry.** Tokens last 90 days unless `EXPIRES` says otherwise, at most
  `IGLOO_TOKEN_MAX_LIFETIME` (365 days). Durations are days (`90d`), weeks
  (`2w`), years (`1y`) or hours (`12h`).
- **Privileges.** A token signs in as its user, with all their privileges:
  give programs service accounts with only what they need, rather than a
  person's token.
- **Who manages them.** Anyone creates, lists and drops their own tokens.
  Admins create tokens for service accounts, and list (`SHOW TOKENS` shows
  them everyone's) and drop anyone's. A session that signed in with a token
  cannot create tokens for its user or change its password, and neither
  can a task, so a stolen token cannot simply be renewed as a credential of
  that user. An admin's
  token still does everything an admin can, including creating users,
  service accounts and their tokens: give admins short-lived tokens, or
  none.
- **At rest** Igloo keeps a SHA-256 hash of each token and SCRAM keys
  derived from it, never the token. `SHOW TOKENS` shows its first 22
  characters (`prefix`), which is how you find a token found where it should
  not be: the `igloo_` prefix makes them easy for secret scanners to spot.
- **Use** is recorded: `last_used_at` in `SHOW TOKENS`, to the minute, and
  every login in `igloo.login_history` with `auth_method` `token` and the
  token's `token_name`.
- **Revoking** a token (`DROP TOKEN`) stops new logins at once and ends the
  SQL and Arrow Flight SQL sessions it opened within 30 seconds (a replica
  that cannot read the metadata database refuses their statements after
  `IGLOO_ACCESS_MAX_STALENESS` instead), and its
  Iceberg REST catalog sessions at their next request. A session also ends,
  before its next statement or request, once its token has expired. Storage
  credentials the catalog already handed an engine for a table's files last
  until they expire, at most an hour.
- **Passwords and tokens** of a user share a SCRAM salt, which is what lets
  one SQL login accept either. Changing the password keeps the salt, so
  tokens keep working, except that psql's `\password`, which encrypts the
  new password on the client with a salt of its own, is refused for users
  with tokens; use `ALTER USER name PASSWORD '...'` over TLS instead.
- A user may have at most 50 unexpired tokens.

### Identity provider access tokens

Optionally, SQL clients of single sign-on users may send an access token
from the identity provider itself as the password, for scripts and tools
that already get one (for instance with a device-code login). Set
`IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE` to the audience the provider puts in
access tokens meant for Igloo, such as `api://igloo` (Entra ID) or a custom
authorization server's audience (Okta). Igloo then checks such a token's
signature, issuer, audience and expiry, and that it is the user's. For a
user linked to an account, it must be that account's: the same subject as
the ID token they sign in to the console with, or, with
`IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM`, the same immutable ID. A user not linked
yet is matched as their first console sign-in would match them: by their
email, or by a username that maps to their name.

Some providers give access tokens a subject of their own: Okta's is the
username, and Entra ID's differs between applications. Set
`IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM` to the claim that holds the account's
immutable ID in their access tokens, `uid` for Okta and `oid` for Entra ID.
Igloo records it at each console sign-in, from the ID token's claim of that
name (Entra ID's has `oid` with the default `profile` scope), or from its
subject when it has none, as Okta's ID tokens carry the `uid` of its access
tokens as their subject. Each user then signs in to the console once before
their access tokens work. The claim must never pass to another person, so
`email`, `preferred_username`, `upn` and the username claim are refused.
Earlier versions accepted such tokens when their verified email was the
linked account's; they no longer do, since a provider may give a deleted
account's email to someone new.

Because SCRAM never shows the server the secret, which checking such a
token needs, Igloo asks these users' clients for the password in clear text
instead, and only inside TLS: only for users without a password, only on
TLS connections. Igloo's own access tokens work there too. Without TLS, or
for users with a password, logins stay SCRAM. Google's access tokens are not
JWTs and cannot be used.

One consequence: a client that tries a name over TLS learns, from whether
Igloo asks for the password in clear text or with SCRAM, whether that user
exists without a password (single sign-on users and service accounts).
Unknown users and users with a password look alike. Leave the setting
off if your user names must stay unguessable.

## Service accounts

A service account is a user for a program: dbt in production, Airflow,
scheduled extracts of a BI tool, an Igloo task, a reverse-ETL sync. It has
no password, cannot sign in to the console or through single sign-on, and
signs in to SQL only with access tokens, which only admins create for it.
An admin owns it (the one who created it, unless `OWNER` says otherwise);
an owner cannot be dropped while they own service accounts.

```sql
CREATE USER dbt_prod TYPE SERVICE ROLE none;   -- owned by you
GRANT SELECT ON SCHEMA raw TO dbt_prod;
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON SCHEMA analytics TO dbt_prod;
CREATE TOKEN prod_2026q4 FOR USER dbt_prod EXPIRES '90d';
ALTER USER dbt_prod OWNER boss;                -- hand it to another admin
ALTER USER dbt_prod NOLOGIN;                   -- stop it, keeping its grants
```

Good practice:

- **One account per program and environment**, such as `dbt_prod`,
  `dbt_ci` and `tableau_extracts`, so history shows what ran what, and one
  can be stopped without the others.
- **Least privilege.** Role `none` with grants on the schemas the program
  uses, or `reader` for read-only tools; `writer` or `admin` only when the
  program needs them everywhere.
- **Keep tokens in a secret store** (Airflow connections, dbt Cloud
  environment variables, Kubernetes Secrets, a vault), never in code.
- **Rotate before expiry**: create a second token, deploy it, check
  `last_used_at` of the old one stops moving, then drop it. Name tokens
  after when or where they are used (`prod_2026q4`).
- **Watch** `SHOW TOKENS` and `igloo.login_history` for tokens nobody uses
  or that are used from unexpected places.

## Ending access

| To | Do | Takes effect |
|---|---|---|
| Stop a person or service account | `ALTER USER name NOLOGIN` (console: *Disable*) | At once for new logins, single sign-on and console sessions, Iceberg REST catalog requests, and the runs of their tasks, pipes and materialized views; open SQL sessions end within 30 seconds. `ALTER USER name LOGIN` enables them again, without bringing back their old console sessions. |
| Revoke one token | `DROP TOKEN name [FOR USER user]` | At once for new logins and Iceberg REST catalog requests; its open SQL sessions end within 30 seconds. |
| Remove someone | `DROP USER name` | Also deletes their tokens and their link to the provider. |
| Deactivate someone at the provider | With [SCIM](#scim-provisioning), the provider disables them in Igloo | As soon as the provider pushes the change, as for `NOLOGIN`. |

Disabling someone at the identity provider stops their single sign-on, but
Igloo hears about it only through [SCIM](#scim-provisioning): without it,
access tokens they already made keep working until they expire. When people
leave, have SCIM deactivate them, or disable them in Igloo too.

## SCIM provisioning

With single sign-on on, Igloo also accepts SCIM 2.0 provisioning from the
identity provider at `https://<console host>:8443/scim/v2`: Okta and
Microsoft Entra ID then create users before their first sign-in, disable
them the moment they are deactivated (which ends their sessions and stops
their access tokens), and keep groups and their members, whose
[mappings](#groups-and-roles) apply at once rather than at the next
sign-in.

1. Create a service account with the admin role for the provider, and a
   token for it. The token is a full admin credential in SQL too: keep it
   only in the provider, and rotate it as you would an admin password.

   ```sql
   CREATE USER okta_scim TYPE SERVICE ROLE admin;
   CREATE TOKEN provisioning FOR USER okta_scim EXPIRES '365d';
   ```
2. At the provider, enable SCIM provisioning for the Igloo application:
   base URL `https://<console host>:8443/scim/v2`, authentication *HTTP
   header* / *Bearer token* with the token. In Okta, enable *Create users*,
   *Update user attributes*, *Deactivate users* and *Push groups*; in Entra
   ID, set the provisioning mode to automatic and map `userPrincipalName`
   (or `mail`) to `userName`.

How Igloo treats what the provider sends:

| SCIM | Igloo |
|---|---|
| Create a user | Finds the user as a sign-in would (by email, then a free name, see [who is who](#who-is-who)) or creates one with role `none` and no password; the user's SCIM `id` is their Igloo name. A user found who was disabled stays disabled, even if the provider says `active: true`, until an admin enables them (`ALTER USER name LOGIN`) or the provider changes `active` later. |
| `active: false` / `true` | `ALTER USER name NOLOGIN` / `LOGIN`. |
| `userName`, `emails`, `name`, `displayName`, `externalId` | Kept; the primary email becomes the user's email. Other attributes are accepted and ignored. |
| Delete a user | Disables the user and stops managing them; their groups' roles go. An admin drops the user when they wish (`DROP USER`). |
| Groups and members | Kept; a group's display name is what `GRANT role TO SSO GROUP 'name'` matches. Members must be users the provider provisioned. Roles follow membership changes at once. A token's groups count at sign-in too, except groups SCIM pushes, whose SCIM members alone have their roles. |

Changes are logged with the token that made them. Without TLS, SCIM is
accepted only at `localhost`, as sign-ins are. Service accounts are never
provisioned, and Igloo never deactivates its last enabled admin who is a
person.

Igloo records whose groups each change affects along with the change. If
it then cannot apply their roles, for instance because the metadata
database is briefly out of reach, it keeps the change, answers a change or
deletion of a group with an error (500), and applies the roles when the
provider sends the request again, or by itself within a minute. Replicas
apply one user's roles one at a time, through a lock in the metadata
database that sign-ins take too, so when two change the user's groups at
once, the roles that stay are those of the groups as both left them.

## Failed logins

Each replica refuses logins for a user after 10 failures within 15 minutes,
and from an address after 50, until the oldest failure is 15 minutes old,
on top of the half-second delay after each failure. Unknown users count like
real ones. This covers passwords and tokens, in SQL clients and the console.
Behind a proxy that hides clients' addresses, the address limit counts all
of its clients together.

Logins still running count as failures to come, so parallel connections get
no more guesses: while as many logins for a user, or from an address, are
running as it has failures left, the next one waits for one of them to end,
for up to 10 seconds. A pool that opens many sessions at once, such as dbt's
threads or a BI tool's, signs them all in; a login that has waited 10
seconds is refused with `too many failed logins`. A SQL client that has not
signed in within 30 seconds of connecting (`IGLOO_LOGIN_TIMEOUT`) is
disconnected, which ends its login, so connections left hanging halfway
through hold none for long.

## Troubleshooting

| Message on the sign-in page | Cause and fix |
|---|---|
| The sign-in took too long, or was started in another tab | More than 10 minutes at the provider, or two sign-ins at once, or the browser blocked cookies. Try again. |
| The identity provider did not sign you in | The provider refused or the user cancelled; its error is in Igloo's log. |
| Igloo could not verify the identity provider's answer | Wrong client secret or redirect URI, or clocks out of step. The log says which. |
| Igloo could not reach the identity provider | `IGLOO_OIDC_ISSUER` wrong or unreachable from Igloo, or its certificate untrusted. The *Single sign-on* page shows the error. |
| The identity provider did not confirm who you are | The username claim is missing, or it is `email` and not verified. For Entra ID use `preferred_username`. |
| Accounts of your email domain may not sign in | The domain is not in `IGLOO_OIDC_ALLOWED_DOMAINS`. |
| Your account is in none of the groups that may sign in | Not in `IGLOO_OIDC_ALLOWED_GROUPS`, or the groups claim is missing: check `IGLOO_OIDC_GROUPS_CLAIM` and the provider's claim settings. |
| There is no Igloo user for your account | `IGLOO_OIDC_CREATE_USERS=false`: create the user first, with the account's email. |
| Your account matches an Igloo user that belongs to someone else | The name and the name with domain are taken by other users or roles. Create the user with another name and the account's email. |
| `password authentication failed` with a token | The token is wrong, expired, revoked, or another user's; check `SHOW TOKENS` and the login history's `reason`. |
| `too many failed logins` | See [failed logins](#failed-logins); wait 15 minutes. |
