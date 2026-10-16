# Erasing personal data

The GDPR and the CCPA give people the right to have their data deleted, and
whoever holds it must do so within a month (GDPR) or 45 days (CCPA), and be
able to show it did. An agency's warehouse holds emails, device IDs and
hashes of them in many tables, copies and snapshots, and a plain `DELETE`
leaves the rows in data files, old snapshots, query history and caches.

`ERASE SUBJECT` erases a person's identifiers from all of them, physically,
and keeps a record of what it did that no longer says whose data it was:

```sql
ERASE SUBJECT WHERE pii.email = 'ann@example.com' OR pii.device_id = '6F9619FF-8B86-D011-B42D-00C04FC964FF';
--  request_id                           | status
--  01a14588-18d9-7271-8635-6fed7a56e198 | queued

SELECT status, verification FROM igloo.erasure_requests;
--  completed | {"rows":0,"files":0,"bounds":0,"statistics":0,"storage":0,"orphans":0,"sharing":0,"history":0,"caches":2,...}
```

A request usually completes within a minute or two. Data retentions
([below](#data-retention)) limit how long tables keep old versions of their
rows in the first place.

## Tag the columns that hold identifiers

A request finds a person's data through [tags](security-policies.md#tags):
it erases the rows whose columns tagged with the tag value it names hold one
of its identifiers. Tag every column that holds an identifier, in every
table, copies and materialized views included:

```sql
CREATE TAG pii ALLOWED_VALUES 'email', 'email:sha256', 'device_id';
ALTER TABLE crm.contacts ALTER COLUMN email SET TAG pii = 'email';
ALTER TABLE marts.impressions ALTER COLUMN device_id SET TAG pii = 'device_id';
ALTER TABLE audiences.segments ALTER COLUMN email_hash SET TAG pii = 'email:sha256';
```

- **Values compare lower-cased and without surrounding spaces**, so
  `' Ann@Example.com'` finds `ann@example.com`, and UUIDs, numbers and dates
  compare as their text. Binary columns compare as hex.
- **Hashed identifiers**: a tag value with `:sha256`, `:sha1` or `:md5`
  after it marks a column of the identifier's hex digest, as ad platforms
  match on: of the identifier as given, or lower-cased and trimmed. A
  request for `pii.email` erases from columns tagged `pii = 'email'` and
  `pii = 'email:sha256'` alike. Binary digests work too. To erase by the
  hashes a partner's opt-out list holds, name the hashed value:
  `ERASE SUBJECT WHERE pii."email:sha256" = '5d41402a...'` erases from the
  columns tagged `pii = 'email:sha256'`.
- A request for a tag value no column has is refused, so a request never
  silently erases nothing.
- Tags stay with a table's name, and renaming a table or column with tags is
  refused, so it cannot slip out of later requests.

## Who may erase

Admins, and users granted `ERASE` on the account, directly or through a
custom role. A privacy officer needs nothing else, not even the reader
role:

```sql
CREATE ROLE privacy_officer;
GRANT ERASE ON ACCOUNT TO privacy_officer;
CREATE USER pat WITH PASSWORD '...' ROLE none;
GRANT privacy_officer TO pat;
```

They may ask for erasures, list the requests and set data retentions. Other
users see no requests. A request is carried out whatever later happens to
the account that made it.

## Asking for an erasure

```sql
ERASE SUBJECT WHERE tag.value = 'identifier' [OR tag.value IN ('identifier', ...)] ...
```

A request takes up to 1,000 identifiers, of one or more tag values; send a
batch of people in one request when you can, since each request reads the
tagged tables in full. It returns the request's ID, and runs in the
background. The identifiers never reach the query history (the statement is
recorded as `ERASE SUBJECT WHERE "pii"."email" IN ('***')`), the logs or
the list of running statements; Igloo keeps only their SHA-256, salted with
a random value per request, and finds rows by hashing column values with
the same salt.

## What a request does

A request runs as a job on Igloo's scheduler, on one replica at a time, one
request at a time, every minute until it completes. Each step is recorded
when it ends and works from the tables as they are when it runs, so a
request that a failure or a restart interrupts goes on where it stopped.
With [scale to zero](scale-to-zero.md), the gateway starts Igloo while a
request has work to do.

| Step | What it does |
|---|---|
| `find` | Lists the tables with columns tagged with the request's tag values; the tables that share data files with them, as zero-copy clones do, whose columns of the same names count as tagged; and the tables that the query history and the definitions of materialized views and tasks say were written from them. |
| `delete` | Deletes the matching rows, as a `DELETE` would: queries no longer see them. |
| `rewrite` | Writes every data file that still holds matching rows again, without them, and then the manifests that still list the old files: a manifest keeps a removed file's entry, with the column bounds that can hold the value itself. |
| `expire` | Expires the snapshots that reference a file that held matching rows, or whose bounds may, however recent, so time travel to them ends. Snapshots that reference none stay, older ones included. Branches and tags pointing at an expired snapshot go too. |
| `purge` | Deletes from object storage the files only the expired snapshots referenced, and the tables' orphan files that are at least 3 days old. A newer orphan may belong to a write in progress; if one holds matching rows, the request waits until it is 3 days old, then deletes it. |
| `sharing` | Deletes the Delta Sharing copies of data files that hold matching rows, which Delta Sharing keeps under the warehouse's `.igloo/sharing/`; it writes them again from the files that remain. |
| `history` | Replaces the identifiers in the query history, and in the errors of the load history and of task runs, with `***`. |
| `cache` | Waits until every running replica has dropped the deleted files from its [read cache](operations.md#read-cache), on disk and in memory. A replica that is not running drops them when it starts. |
| `verify` | Looks for the identifiers in what is left: the tables' rows; every data and delete file any snapshot references, deleted rows included; the bounds of every manifest entry, live or not; the Parquet statistics of every data file; recent orphan files; sharing copies; history. Anything found sends the request back to the step that removes it, at most five times. |

Once a request completes, its hashes and salt are deleted too: its record
says what was erased, from which tables and when, but not whose data it
was. Erasing a table's snapshots does not change the snapshot retention
it has otherwise.

## Requests as a record

`SHOW ERASURE REQUESTS` and the view `igloo.erasure_requests` list every
request, newest first, to admins and holders of `ERASE`:

| Column | Is |
|---|---|
| `request_id`, `requester` | The request, and who made it |
| `status` | `queued`, `running`, `waiting` (for replicas, or for a recent file to be old enough to delete), `retrying` (the last run failed; `error` says why) or `completed` |
| `step` | The step it runs next, `done` once complete |
| `created_at`, `updated_at`, `completed_at` | When |
| `subjects` | JSON: the tag values, and how many identifiers of each |
| `tables` | JSON: per table, its tagged columns, why it was included, rows deleted, files rewritten, snapshots expired, files deleted, and when each step ended for it |
| `downstream` | JSON: tables written from those, as history tells, and whether the request erased from them because they have the tags too |
| `history_redacted` | History records redacted |
| `steps` | JSON: when each step started and last finished, and how often it ran |
| `verification` | JSON: what the last check found, counts only (see the `verify` step); all zero once complete, with the number of read caches that dropped the files and of those that will when their replica starts |
| `error` | Why the last run failed |

```sql
SELECT request_id, completed_at, t->>'table', t->>'rows_deleted'
FROM (SELECT *, unnest(json_extract(tables::JSON, '$[*]')) AS t FROM igloo.erasure_requests);
```

Each run of a request is in `igloo.task_history`, as kind `erasure`. The
log says `erasure requested`, `erasure request completed`, and why a run
failed. Requests are kept in the metadata database (schema
`igloo_erasure`), with the files each deleted; without one, in memory, lost
on restart.

A downstream table that does not have the tags is reported and left alone:
tag its columns and ask again. Materialized views refresh from the erased
tables, but keep what they held in their own files until erased or
expired.

## Data retention

Tables that hold personal data can be kept from holding old versions of
rows for long:

```sql
ALTER TABLE marts.impressions SET (data_retention_days = 30);
ALTER TABLE marts.impressions RESET (data_retention_days);
SELECT * FROM igloo.data_retention;
```

[Table maintenance](operations.md#table-maintenance) then keeps no snapshot
older than that but the current one, whatever the table's snapshot
retention and `history.expire.min-snapshots-to-keep` say, and rewrites the
data files whose rows were deleted longer ago, so the rows leave the files
too. Time travel reaches back that far at most. With an hourly maintenance
pass, a deleted row is gone from storage about an hour after it is older
than the retention.

Admins and holders of `ERASE` set and remove data retentions; everyone may
list them. They are kept with policies and tags, stay with a table's name,
and renaming such a table is refused.

## What it does not cover

- **Columns without tags.** A request finds rows only by tagged columns: an
  email in a free-text note stays, as does the rest of a row in a table
  without the tags. Lineage reports tables written from tagged ones; tag
  them.
- **Other forms of the identifier**: digests of other normalizations
  (Gmail's dots, phone numbers' formats), salted or keyed hashes, base64.
- **Backups and versions.** Backups and point-in-time recovery of the
  metadata database keep the history as it was. Bucket versioning, soft
  delete (7 days by default in the Google Cloud and Azure modules),
  replication and storage snapshots keep deleted files; the erasure is
  complete when they expire. Restoring an older backup brings the data
  back: erase again.
- **The metadata database itself.** Redacted history rows leave old row
  versions in PostgreSQL's pages until they are vacuumed and reused, and in
  its write-ahead log. Lakekeeper's tables (also in PostgreSQL) hold table
  metadata, such as snapshots, schemas and properties, not rows: only
  comments or properties that you wrote identifiers into.
- **Copies outside the warehouse**: files in `IGLOO_ALLOWED_LOCATIONS`,
  landing buckets that loads read and exports wrote, other engines' copies
  of the tables (Spark, Trino, Snowflake), BI tools' caches, and files
  partners already downloaded through Delta Sharing.
- **Tables dropped before the request**, whose files the catalog may keep
  for a while, and the definitions of tasks, pipes and materialized views,
  which are SQL and not history.
- **Tables partitioned by an identifier**, whose partition values are in
  file paths and manifests.
- **Statements run after the request completes**, which are recorded as
  usual, and logs at debug level, which can quote statements other than
  `ERASE SUBJECT`.

## Cost

A request reads the tagged columns of every tagged table in full, every
file of every snapshot, when it starts and again when it verifies, and
rewrites only the files that hold matching rows, at most 8 GB per commit.
DuckDB reads only the tagged columns, with all of a replica's cores; a
table of billions of rows takes minutes. Requests run one at a time, so
batching people into one request saves reading the same tables again.
