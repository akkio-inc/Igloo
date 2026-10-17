# Alerts and notifications

Igloo tells people when something needs them, in Slack, Microsoft Teams,
email or any webhook, with nothing else to run:

- **Alerts** run a query on a schedule and send a notification when it
  returns rows: spend pacing off plan, a partner's daily file missing, a
  KPI out of range.
- **Built-in operational alerts** report failed task, pipe and
  materialized view runs, files that failed to load, pipes that stopped
  receiving files, long-running queries, bursts of failed logins and table
  maintenance failures.
- **`SYSTEM$SEND_NOTIFICATION`** sends a message from a task, such as
  "the client report is ready".

All of them go to **notification integrations**, which admins create and
let roles use. An alert runs as its owner, with the owner's grants and
[policies](security-policies.md), so it can only send what its owner may
see, and only to integrations its owner may use.

A media agency's analytics team might set up:

```sql
-- Admins: where notifications go, and who may send there.
CREATE NOTIFICATION INTEGRATION analytics_slack
  TYPE slack URL 'https://hooks.slack.com/services/T0000/B0000/XXXXXXXX'
  OPERATIONAL_ALERTS = (FAILED_RUNS, LOAD_ERRORS, IDLE_PIPES = 26);
GRANT USAGE ON INTEGRATION analytics_slack TO ROLE analysts;

-- Analysts: campaigns pacing more than 20% off plan, every weekday at 9.
CREATE ALERT pacing SCHEDULE '0 9 * * MON-FRI' TIMEZONE 'America/New_York'
  IF (EXISTS (
    SELECT p.campaign, sum(d.spend) AS spend, any_value(p.planned_to_date) AS planned
    FROM marts.daily_spend d JOIN marts.plan p USING (campaign)
    WHERE d.day < current_date
    GROUP BY p.campaign
    HAVING abs(sum(d.spend) / any_value(p.planned_to_date) - 1) > 0.2))
  THEN NOTIFY analytics_slack
    MESSAGE '<!here> Pacing is off plan for {{campaign}} and others:' WITH RESULT;
ALTER ALERT pacing RESUME;
```

With `IDLE_PIPES = 26`, the channel hears of any pipe that has loaded no
file for 26 hours, such as a partner's daily drop that did not arrive; with
`FAILED_RUNS`, of every scheduled task, pipe or materialized view run that
fails.

## Notification integrations

```sql
CREATE [OR REPLACE] NOTIFICATION INTEGRATION [IF NOT EXISTS] name TYPE [=] webhook | slack | teams | email [option ...]
ALTER NOTIFICATION INTEGRATION [IF EXISTS] name SET option ... | UNSET HEADERS | SECRET | COMMENT | OPERATIONAL_ALERTS, ...
DROP [NOTIFICATION] INTEGRATION [IF EXISTS] name
SHOW [NOTIFICATION] INTEGRATIONS
GRANT USAGE ON INTEGRATION name TO [ROLE | USER] name
REVOKE USAGE ON INTEGRATION name FROM [ROLE | USER] name
```

Only admins create, change, drop and grant integrations. Options come in
any order, with or without `=` and commas:

| Option | Types | Meaning |
|---|---|---|
| `URL 'https://...'` | webhook, slack, teams | Where to post. Slack's and Teams' incoming webhook URLs, or any HTTPS endpoint. Stored encrypted and never shown again. `WEBHOOK_URL` works too. |
| `HEADERS ('name' = 'value', ...)` | webhook | Headers to send, such as `'Authorization' = 'Bearer ...'`. Stored encrypted. |
| `SECRET 'at least 16 characters'` | webhook | Signs every request (see [webhooks](#webhooks)). Stored encrypted. |
| `RECIPIENTS ('ana@agency.com', ...)` | email | Who gets the email, in the domains `IGLOO_SMTP_ALLOWED_DOMAINS` allows. `ALLOWED_RECIPIENTS` works too. |
| `ENABLED TRUE \| FALSE` | all | A disabled integration sends nothing; alerts that use it fail. Default `TRUE`. |
| `OPERATIONAL_ALERTS (name [= n], ...)` | all | The [built-in alerts](#built-in-operational-alerts) it receives; `()` for none. |
| `COMMENT '...'` | all | A note for people. |

```sql
CREATE NOTIFICATION INTEGRATION client_success_teams
  TYPE teams URL 'https://agency.webhook.office.com/webhookb2/...';
CREATE NOTIFICATION INTEGRATION incident_hook
  TYPE webhook URL 'https://events.pagerduty.example/igloo'
  HEADERS ('Authorization' = 'Token token=...') SECRET 'a-long-random-signing-secret';
CREATE NOTIFICATION INTEGRATION media_team_email
  TYPE email RECIPIENTS ('media-ops@agency.com', 'ana@agency.com');
ALTER NOTIFICATION INTEGRATION analytics_slack SET ENABLED = FALSE;
```

Creating or changing an integration checks its URL: `https`, a host that
resolves only to public addresses (see [security](#security)), and no user
name or password in it.

`SHOW INTEGRATIONS` lists each integration's `name`, `type`, `enabled`,
`target` (the URL's scheme and host, or the email recipients, never the
token), `operational_alerts`, `usage` (who may use it; admins only),
`comment`, `created_by` and `created_at`. Admins see every integration;
other users the ones they may use.

### Who may use an integration

Sending to an integration, from an alert or with
`SYSTEM$SEND_NOTIFICATION`, needs `USAGE` on it, granted to the user or to
a custom role they have. Admins may use every integration. `USAGE` is
checked when an alert is created and again every time it runs: revoking it
stops the alert's notifications at its next run. Dropping a user or role
drops its grants.

## Alerts

```sql
CREATE [OR REPLACE] ALERT [IF NOT EXISTS] name SCHEDULE [=] 'schedule' [TIMEZONE [=] 'zone']
  IF (EXISTS (query))
  THEN NOTIFY integration [MESSAGE 'text'] [WITH RESULT]
ALTER ALERT [IF EXISTS] name RESUME | SUSPEND | SET SCHEDULE 'schedule' [TIMEZONE 'zone']
EXECUTE ALERT name
DROP ALERT [IF EXISTS] name
SHOW ALERTS
```

- **The condition** is one query. When it returns at least one row, the
  alert sends its notification; when it returns none, nothing is sent.
- **A new alert is suspended**, as in Snowflake: `ALTER ALERT ... RESUME`
  starts its schedule. `EXECUTE ALERT` runs it once, now, and returns the
  error if the query or the notification fails.
- **Schedules** are those of [tasks](loading.md#schedules): an interval
  such as `'1 hour'`, or cron such as `'0 9 * * MON-FRI'`, in any time zone;
  Snowflake's `'60 MINUTE'` and `'USING CRON ...'` work too. As in
  Snowflake, an alert runs at most once a minute.
- **It runs as its owner**, the user who created it, in a new session of
  theirs, with their grants and their [masking and row access
  policies](security-policies.md) at the time it runs: masked columns stay
  masked in what it sends. Its query is in the owner's query history, with
  client `alert <name>`. Unqualified names resolve in the default schema,
  as in a new session.
- **While its owner is disabled** (`NOLOGIN`), its runs are skipped and
  recorded as `skipped`. When the owner is dropped, their alerts, like their
  tasks, pipes and materialized views, are suspended and marked as a
  dropped user's: `SHOW ALERTS` gives their owner as `ana (dropped)`. A user
  created later with the same name does not get them; admins may drop them,
  or `CREATE OR REPLACE` them as their own.
- **Creating an alert needs the writer role** and `USAGE` on its
  integration. A user owns at most 50 alerts. Only its owner and admins may
  change, run or drop it; users other than admins see only their own in
  `SHOW ALERTS`.
- **One run at a time**, on one replica, as for tasks: a run that comes due
  while the previous one still runs is skipped. Failed runs are not tried
  again before the next scheduled time.
- **A scheduled run ends once its query has**: the notification waits in
  its integration's outbox, so a slow destination holds up no other
  scheduled work. The run's outcome in `igloo.task_history` is about the
  query; whether the notification went out is in `igloo.alert_history`,
  once it has or has failed. `EXECUTE ALERT` waits for the notification.

`SHOW ALERTS` lists each alert's owner, state, schedule, time zone, next
and last run, last outcome and error, and definition. Alerts' runs are in
`igloo.task_history` (kind `alert`) like other scheduled work, and in more
detail in [`igloo.alert_history`](#alert-history).

### What it sends

Without `MESSAGE`, the notification says how many rows the condition
returned. `MESSAGE` replaces that with your text, in which `{{column}}`
stands for that column's value in the first row:

```sql
CREATE ALERT big_spender SCHEDULE '1 hour'
  IF (EXISTS (SELECT client, sum(spend) AS spend FROM raw.dsp_delivery
              WHERE day = current_date GROUP BY client HAVING sum(spend) > 50000))
  THEN NOTIFY analytics_slack MESSAGE '{{client}} has spent {{spend}} today';
```

`WITH RESULT` adds the rows themselves, as a table: at most 20 rows, 30
columns, 16 KiB of values, and 500 bytes of each value, in lines of at most
1,000 bytes and 8 KiB in all, which every destination takes; the
notification says when there was more. A `MESSAGE` is at most 4,000 bytes
as written, and 4 KiB once its placeholders are filled in. Every
placeholder must name a column of the query.

Your `MESSAGE` text is sent as written, so it may mention `<!here>` or link
to a dashboard in Slack. Values from the query are not: in Slack they are
shown as code, with `<`, `>` and `&` escaped, so a value cannot mention
anyone, make a link or change the formatting; in Teams they are plain
text that Teams makes no links of. Teams shows the whole message as plain
text: Markdown in a `MESSAGE` is not formatted there.

### Alert history

`igloo.alert_history` has a row for every run of an alert, every built-in
alert's notification, and every `SYSTEM$SEND_NOTIFICATION`, newest first.
Admins see every row; other users the runs of their own alerts and the
messages they sent.

```sql
SELECT start_time, alert_name, state, row_count, integration, error
FROM igloo.alert_history WHERE state IN ('condition_failed', 'action_failed')
ORDER BY start_time DESC LIMIT 20;
```

| Column | Meaning |
|---|---|
| `alert_id` | Identifies the row; for an alert's run, the run's `run_id` in `igloo.task_history` |
| `alert_type` | `alert`, `builtin` (a built-in alert) or `call` (`SYSTEM$SEND_NOTIFICATION`) |
| `alert_name` | The alert, the built-in alert (such as `failed_runs`), or the task that sent a message |
| `user_name` | Whom the alert ran as, or who sent the message; `NULL` for built-in alerts |
| `trigger` | `schedule`, `manual` (`EXECUTE ALERT`) or `event` (built-in alerts) |
| `scheduled_time`, `start_time`, `end_time`, `duration_ms` | When it was due, when it ran, and for how long |
| `state` | `condition_false` (no rows, nothing sent), `triggered` (sent), `condition_failed` (the query failed), `action_failed` (the notification could not be sent), or `skipped` (the owner is disabled or gone) |
| `row_count` | Rows the condition returned; Igloo reads at most 21 |
| `integration` | Where the notification went |
| `attempts` | How many times sending was tried |
| `error` | Why it failed, with the integration's secrets replaced by `***` |

Alert history is kept for `IGLOO_AUDIT_RETENTION` (90 days by default).

## Sending from tasks

`SYSTEM$SEND_NOTIFICATION` sends a message you write to an integration you
may use. In a task, it runs as the task's owner:

```sql
CREATE TASK report_ready SCHEDULE '30 6 * * *' TIMEZONE 'America/New_York' AS
  CALL SYSTEM$SEND_NOTIFICATION('client_success_teams', 'The client reports for today are ready.');
ALTER TASK report_ready RESUME;
```

Its arguments are the integration's name and the message, both written in
the statement: the message cannot be computed from a query, so it can
carry no data. To send what a query finds, use an alert. It works in any
session too, which is a way to try an integration out.

## Built-in operational alerts

Admins switch them on per integration with `OPERATIONAL_ALERTS`:

```sql
ALTER NOTIFICATION INTEGRATION analytics_slack
  SET OPERATIONAL_ALERTS = (FAILED_RUNS, LOAD_ERRORS, IDLE_PIPES = 26, LONG_QUERIES = 45,
                            FAILED_LOGINS, MAINTENANCE_FAILURES);
```

| Alert | Sends when | At most |
|---|---|---|
| `FAILED_RUNS` | A scheduled run of a task, pipe or materialized view fails, or an [erasure request](privacy.md) fails for good | Once an hour per task, pipe or view |
| `LOAD_ERRORS` | `COPY INTO`, or a pipe, could not load some files, or loaded them without some rows | Once an hour per table |
| `IDLE_PIPES [= hours]` | A started pipe has loaded no file for that many hours (24 by default) since its last load, or since it was created | Once per that many hours per pipe |
| `LONG_QUERIES [= minutes]` | A statement has run for that many minutes (60 by default) | Once per statement |
| `FAILED_LOGINS` | A replica starts refusing password logins from an address (50 failures in 15 minutes), or for a user who exists (10) | Once per 15 minutes per address or user, and at most 5 notices per 15 minutes from a replica |
| `MAINTENANCE_FAILURES` | [Table maintenance](operations.md#table-maintenance) of a table fails | Once a day per table |

They go to enabled integrations only, need no `USAGE`, and say what failed,
whose it is, and the error, which may quote values from the data; a long
query's notification carries the first 500 bytes of its statement, with
passwords and secrets removed. Send them where the people who see them may
see that. Alerts' own failures are in the alert history, not sent: an
integration that cannot be reached would otherwise be told so through
itself. A notification that could not be sent holds its problem back for
5 minutes rather than its whole window, so the problem's next occurrence
tries again.

Clients choose the user names they fail to log in with, and the addresses
they fail from, so failed-login notices cannot crowd out the rest: failures
of names that are no user's raise none, a replica sends at most 5 in 15
minutes and shows names quoted, as the data they are, and they never wait
for their integration's rate limit nor use its last 5 notifications or 16
places in its outbox, which are kept for the other alerts.

Igloo notices failures as they happen; it looks for idle pipes and long
queries every minute while it runs. With [scale to
zero](scale-to-zero.md), that is while it runs for clients or for scheduled
work, such as the pipe's own checks. Changes to an integration reach the
built-in alerts of other replicas within 30 seconds.

## Delivery

- **Outboxes.** Each integration has an outbox of 64 notifications on each
  replica, sent one after another, so a slow or unreachable destination
  delays only its own. When the outbox is full, a notification fails at
  once.
- **Rate limits.** Each integration may send 10 notifications at once,
  then one a second, on each replica. A notification waits up to a minute
  for its turn, then fails as rate limited.
- **Timeouts and retries.** Each attempt has 10 seconds. Failures that may
  pass (no connection, a timeout, HTTP 408, 429 or 5xx, a temporary SMTP
  error) are tried again after about 1 and then 3 seconds, or as long as a
  `Retry-After` header asks (up to 10 seconds), up to 3 attempts. Other
  failures (HTTP 4xx, permanent SMTP errors) are not. Redirects are never
  followed: use the URL they lead to. Errors quote a webhook's answer only
  when it is a short error code, such as Slack's `invalid_token`: anything
  else it answers might echo what it was sent.
- **Records.** Every attempt to notify ends up in the alert history, and in
  the metric `igloo_notifications_total{type, result}` (`sent`, `failed`,
  `rate_limited`).

### Webhooks

A webhook integration gets a `POST` with a JSON body:

```json
{
  "type": "alert",
  "name": "pacing",
  "user": "ana",
  "integration": "incident_hook",
  "title": "Igloo alert pacing",
  "message": "Pacing is off plan for acme and others:",
  "result": {"columns": ["campaign", "spend", "planned"], "rows": [["acme", "1200.50", "900.00"]], "more": false},
  "time": "2026-10-16T13:00:04Z"
}
```

`type` is `alert`, `builtin` or `call`; built-in alerts add `details`, such
as `{"kind": "task", "name": "nightly", "owner": "walt", "error": "..."}`.
Values are in PostgreSQL's text format, `null` for `NULL`.

With a `SECRET`, each request carries `X-Igloo-Timestamp` (Unix seconds)
and `X-Igloo-Signature: v1=<hex>`, the HMAC-SHA256 of the timestamp, a dot
and the body, keyed with the secret. Check it, in constant time, and refuse
old timestamps:

```python
import hashlib, hmac, time

def from_igloo(secret: bytes, headers, body: bytes) -> bool:
    ts = headers["X-Igloo-Timestamp"]
    if abs(time.time() - int(ts)) > 300:
        return False
    want = hmac.new(secret, ts.encode() + b"." + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(headers["X-Igloo-Signature"], "v1=" + want)
```

### Slack and Microsoft Teams

Create an [incoming webhook](https://api.slack.com/messaging/webhooks) in
Slack, or a workflow that posts webhook requests to a channel in Teams
("Post to a channel when a webhook request is received"), and give its URL
to `CREATE NOTIFICATION INTEGRATION ... TYPE slack` or `TYPE teams`. Slack
gets a message with link previews off; Teams an Adaptive Card.

### Email

Email goes through the mail server in `IGLOO_SMTP_HOST`, from
`IGLOO_SMTP_FROM`, and only to addresses in `IGLOO_SMTP_ALLOWED_DOMAINS`
(see [configuration](configuration.md#alerts-and-notifications)). It is
plain text: the message, then the result as a table. Igloo connects with
TLS from the start on port 465, and otherwise requires STARTTLS, unless the
server is in `IGLOO_NOTIFICATION_ALLOW_HOSTS`.

## Security

- **Only public destinations.** Integrations may reach only public
  addresses. Igloo resolves the host when an integration is created or
  changed, and again for every connection, and connects to the address it
  checked; it refuses the host if any of its addresses is loopback,
  private (10/8, 172.16/12, 192.168/16, fc00::/7), link-local
  (169.254/16, which holds cloud metadata endpoints, and fe80::/10),
  multicast, shared (100.64/10), reserved or documentation space, or an
  IPv6 form of any of them (IPv4-mapped, NAT64, 6to4, Teredo). A host that
  switches to an internal address after the check is refused at the next
  connection. Webhooks need `https`. Redirects are not followed, and no
  proxy is used. `IGLOO_NOTIFICATION_ALLOW_HOSTS` names hosts on the
  private network that may be reached anyway, over plain HTTP or SMTP
  too; list only hosts meant to receive notifications.
- **Secrets.** Integrations' URLs, headers and signing secrets are stored
  encrypted (AES-256-GCM) in the metadata database, under a key kept in
  `igloo_auth.secrets` that every replica shares, and bound to their
  integration. `SHOW INTEGRATIONS` shows only the URL's host; the query
  history keeps `CREATE` and `ALTER NOTIFICATION INTEGRATION` with their
  strings replaced by `'***'`; errors, logs and the alert history replace
  them with `***`. The SMTP password comes from the environment or a file,
  never from SQL. Someone who can read the whole metadata database, key
  included, can decrypt the secrets: protect it as the
  [security model](../SECURITY.md) says.
- **What leaves.** An alert sends only what its query returns as its
  owner, through the owner's grants and policies, only to integrations its
  owner may use at that moment, and at most 20 rows of it.
  `SYSTEM$SEND_NOTIFICATION` sends only text written in the statement.
  Built-in alerts send what an admin chose, to integrations an admin set up.

## Where it is kept

Integrations, who may use them and the alert history are in the metadata
database (`IGLOO_METADATA_DATABASE_URL`, schema `igloo_notify`); alerts are
with tasks (schema `igloo_tasks`). Without a metadata database they are in
memory and lost on restart.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `notifications may only reach public addresses, and ... resolves to 10.0.0.7, a private address` | The host is on a private network. If it is meant to receive notifications, add it to `IGLOO_NOTIFICATION_ALLOW_HOSTS`. |
| `the URL must use https` | Use the `https` URL; plain `http` is only for allowed hosts. |
| `walt has no USAGE on the notification integration ...` | `GRANT USAGE ON INTEGRATION ... TO ROLE ...` that the owner has, then run the alert again. |
| An alert never sends | It is suspended (`ALTER ALERT ... RESUME`), or its query returns no rows: `igloo.alert_history` says `condition_false`. Run the query as its owner to check. |
| `action_failed` with `answered 404 Not Found: "no_service"` | Slack or Teams no longer knows the webhook. Make a new one and `ALTER NOTIFICATION INTEGRATION ... SET URL = '...'`. |
| `rate limited` | More than 10 notifications at once went to one integration and the rest waited over a minute. Spread alerts' schedules, or use more integrations. |
| `the mail server ... does not offer STARTTLS` | Turn on STARTTLS at the server, use port 465, or allow a relay on the private network in `IGLOO_NOTIFICATION_ALLOW_HOSTS`. |
| `skipped: the owner ... is disabled` | `ALTER USER ... LOGIN`, or `CREATE OR REPLACE` the alert as someone else. |
| `alerts run at most once a minute` | Give the alert a schedule of `'1 minute'` or more, or a cron expression. |
| `... notifications are waiting to go to ...` | The integration's outbox is full: its destination is slow or down, or too many alerts use it. Check `igloo.alert_history` for its errors. |
