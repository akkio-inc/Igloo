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

Requests need the erasure key, `IGLOO_ERASURE_KEY`
([configuration](configuration.md)), which the single-VM setup and the Helm
chart generate.

## Tag the columns that hold identifiers

A request finds a person's data through [tags](security-policies.md#tags):
it erases the rows whose columns tagged with the tag value it names hold one
of its identifiers. Tag every column that holds an identifier, in every
table, copies and materialized views included:

```sql
CREATE TAG pii ALLOWED_VALUES 'email', 'email:sha256', 'email:null', 'device_id';
ALTER TABLE crm.contacts ALTER COLUMN email SET TAG pii = 'email';
ALTER TABLE marts.impressions ALTER COLUMN device_id SET TAG pii = 'device_id';
ALTER TABLE audiences.segments ALTER COLUMN email_hash SET TAG pii = 'email:sha256';
ALTER TABLE crm.referrals ALTER COLUMN referrer_email SET TAG pii = 'email:null';
```

- **Values compare as DuckDB's `lower(trim(...))` leaves them**, so
  `' Ann@Example.com'` finds `ann@example.com`, UUIDs and integers compare
  as their text, and binary columns as hex. Tagged columns must be strings,
  UUIDs, binary values or integers: a request is refused while a tag it
  needs is on a list, struct or map, whose values hold identifiers inside
  them. A JSON document in a string column compares as a whole, too.
- **Hashed identifiers**: a tag value with `:sha256`, `:sha1` or `:md5`
  after it marks a column of the identifier's hex digest, as ad platforms
  match on: of the identifier as given, or lower-cased and trimmed. A
  request for `pii.email` erases from columns tagged `pii = 'email'` and
  `pii = 'email:sha256'` alike. Binary digests work too. To erase by the
  hashes a partner's opt-out list holds, name the hashed value:
  `ERASE SUBJECT WHERE pii."email:sha256" = '5d41402a...'` erases from the
  columns tagged `pii = 'email:sha256'`.
- **Whole rows go.** A request deletes every row in which a tagged column
  holds an identifier. A column that names the person in a row about
  someone else, such as a referrer's email, is tagged with `:null` after the
  value (`pii = 'email:null'`, `pii = 'email:sha256:null'`): the request
  sets it to NULL in such rows and keeps them. Such a column must allow
  NULL.
- A request for a tag value no column of the catalog's tables has is
  refused, so a request never silently erases nothing; so is one while a
  table with such a tag does not exist, as after it was dropped or renamed
  by another engine: unset its tags, or tag it under its new name.
- Zero-copy clones count as tagged where their source is, and the reverse
  ([below](#clones)): tag either.
- Tags stay with a table's name and a column's: renaming a table with
  tags, or renaming, dropping or changing the type of a tagged column, is
  refused, in SQL and through the [Iceberg REST
  catalog](external-engines.md) alike, admins included, so nothing slips
  out of later requests. A column that another engine dropped and added
  again under its name anyway is still found: a request looks in every
  field that had the name, older files included.

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

They may ask for erasures, preview, confirm and cancel them, list the
requests, and set data retentions of tables with tags. Other users see no
requests. A request is carried out whatever later happens to the account
that made it.

## Asking for an erasure

```sql
ERASE SUBJECT WHERE tag.value = 'identifier' [OR tag.value IN ('identifier', ...)] ... [DRY RUN]
CONFIRM ERASURE REQUEST 'request_id'
CANCEL ERASURE REQUEST 'request_id'
```

A request takes up to 1,000 identifiers, of one or more tag values; send a
batch of people in one request when you can, since each request reads the
tagged tables in full. It returns the request's ID, and runs in the
background. The identifiers never reach the query history, in whatever
statement they are (the request is recorded as
`ERASE SUBJECT WHERE "pii"."email" IN ('***')`, and any other statement that
says `ERASE SUBJECT`, such as an `EXPLAIN`, as `ERASE ...`), the logs or the
list of running statements.

Safeguards against erasing the wrong people:

- **Placeholders are refused**: identifiers that stand for no one, such as
  an opted-out iPhone's all-zero advertising ID, `0`, `null`, `unknown`,
  `n/a` or `-`, which systems write by the million.
- **`DRY RUN`** shows, without making a request, which tables a request
  would erase from, why, and how many of their rows match, of how many.
- **Large deletions wait to be confirmed.** A request that would delete
  more than 1% of a table's rows, and more than 100 of them, is `held`
  once it has counted them, before it deletes anything, until someone who
  may erase runs `CONFIRM ERASURE REQUEST`. The metric
  `igloo_erasure_requests_held_total` counts held requests.
- **`CANCEL ERASURE REQUEST`** cancels a request that has not begun
  deleting. Once it has, it must finish.

## What a request does

A request runs as a job on Igloo's scheduler, on one replica at a time, one
request at a time. A run that finds another request running ends at once
and tries again a minute later, so requests never hold up tasks, pipes and
materialized views. Each step is recorded when it ends and works from the
tables as they are when it runs, so a request that a failure or a restart
interrupts goes on where it stopped. Before each step, a run checks that
it still holds its lock, which it loses if its connection to the metadata
database drops. With [scale to zero](scale-to-zero.md), the gateway starts
Igloo while a request has work to do.

| Step | What it does |
|---|---|
| `find` | Lists the tables with columns tagged with the request's tag values; the tables that share data files with them ([clones](#clones)); and the tables that the query history and the definitions of materialized views and tasks say were written from them. Tagged tables of [connections](federation.md) are listed, but nothing is erased from them. Counts their rows and those that match, and holds a request that would delete much of a table. |
| `delete` | Deletes the matching rows, as a `DELETE` would: queries no longer see them. |
| `rewrite` | On every branch and tag of every table, writes each data file that holds matching rows again with its deletes applied, leaving out the rows that match, or setting columns tagged `:null` to NULL in them, in the same commit that replaces the file: rows that arrived since the delete step go too. Equality delete files that hold matches go with the files they apply to, or at once if they apply to none. Then writes again the manifests that still list the old files: a manifest keeps a removed file's entry, with column bounds that can hold the value itself. A tag is moved to a snapshot like the one it pointed at, without the person's rows; branches keep their unpublished rows. |
| `expire` | Expires the snapshots that reference a file that held matching rows, or whose bounds may, however recent, so time travel to them ends. Snapshots that reference none stay, older ones included, and so do the snapshots branches and tags point at, which the rewrite step made reference none. |
| `purge` | Deletes from object storage the files only the expired snapshots referenced, and the tables' orphan files that are at least 3 days old. A newer orphan may belong to a write in progress; if one holds matching rows, the request waits until it is 3 days old, then deletes it. It deletes files only in the warehouse, and never one that another table references ([clones](#clones)). |
| `sharing` | Deletes the Delta Sharing copies of data files that hold matching rows, which Delta Sharing keeps under the warehouse's `.igloo/sharing/`, those of tables since replaced included; it writes them again from the files that remain. |
| `history` | Replaces the identifiers in the query history (statements, query tags and application names), the login history's application names, and the errors of the load history and of task runs, with `***`. |
| `cache` | Waits until every running replica has emptied its [read cache](operations.md#read-cache), on disk and in memory, since the request deleted files. A cache is emptied whole: it may also hold blocks of files deleted before, by maintenance or another engine. A replica that is not running empties its cache when it starts. |
| `verify` | Looks for the identifiers in what is left: the tables' rows; every data and delete file any snapshot references, deleted rows included; the bounds of every manifest entry, live or not; the Parquet statistics of every data file; recent orphan files; sharing copies; history. Anything found sends the request back to the step that removes it. |

A failed run is tried again after a minute, then 2, 4 and so on up to an
hour. A request fails for good (`failed`) when its runs fail 10 times in a
row, as when matching rows keep arriving faster than it removes them, when
verification sends it back more than five times, or when the erasure key
changed: it stops, forgets its identifiers, logs `erasure request failed`
at error level, raises the built-in alert `FAILED_RUNS`
([alerts](alerts.md#built-in-operational-alerts)), and counts in
`igloo_erasure_requests_failed_total`. What it erased stays erased; fix
what `error` says and make the request again.

### Clones

A zero-copy clone references its source's data files, under the source's
location, and its compactions and updates write copies of the source's
rows under its own. A request therefore erases from the clones of the
tables it finds and from the tables those were cloned from, clones of
clones included, whichever of them has the tags; a clone's columns are its
source's, renamed or not. A file one of them references is rewritten in
each, and its snapshots expired in each, before it is deleted. Tables that
reference another's files otherwise, as another engine's `add_files` can
make them, are found by their manifests, and their columns named like the
tagged ones count as tagged.

While a request deletes files, it holds the locks that making a clone of
one of its tables takes (`CREATE TABLE ... CLONE`, see
[SQL](sql.md#clones)), so no clone is made meanwhile; while one is being
made, the request tries again later. A clone made after the request began
is added to it. If a table the request does not erase from references a
file it would delete, the request stops with an error that names the table
instead of breaking it: tag that table's columns and ask again for the
same identifiers, and the first request goes on once the second has erased
from it. So does a table the request found that was dropped or renamed
while it ran.

### What the record keeps

While a request is open, its identifiers are kept sealed (AES-256-GCM)
under a key derived from the erasure key, and rows are found by SHA-256
hashes of their values salted with a value derived from the erasure key and
a random salt of the request's. Neither the identifiers nor the hashes are
in the metadata database in the clear, and the erasure key never is: the
database alone does not tell whose data a request erases, and nothing about
them is sent to it, not even hashes. A replica unseals the identifiers in
memory while it runs the request. Once a request completes, fails or is
canceled, its sealed identifiers, its salt and the paths of the files it
deleted, which can hold identifiers as partition values, are deleted too:
its record says what was erased, from which tables and when, but not whose
data it was. Erasing a table's snapshots does not change the snapshot
retention it has otherwise.

## Requests as a record

`SHOW ERASURE REQUESTS` and the view `igloo.erasure_requests` list every
request, newest first, to admins and holders of `ERASE`:

| Column | Is |
|---|---|
| `request_id`, `requester` | The request, and who made it |
| `status` | `queued`, `running`, `held` (waits for `CONFIRM ERASURE REQUEST`), `waiting` (for replicas, or for a recent file to be old enough to delete), `retrying` (the last run failed; `error` says why), `completed`, `canceled` or `failed` |
| `step` | The step it runs next, `done` once complete |
| `created_at`, `updated_at`, `completed_at` | When |
| `subjects` | JSON: the tag values, and how many identifiers of each |
| `tables` | JSON: per table, its tagged columns, why it was included, rows that matched of how many, rows deleted, values nulled, files rewritten, snapshots expired, files deleted, files outside the warehouse it left, the branches and tags it rewrote, and when each step ended for it; `remote` for a table of a connection, which it did not erase from |
| `downstream` | JSON: tables written from those, as history tells, and whether the request erased from them because they have the tags too |
| `history_redacted` | History records redacted |
| `steps` | JSON: when each step started and last finished, and how often it ran |
| `verification` | JSON: what the last check found, counts only (see the `verify` step); all zero once complete, with the number of read caches emptied since it deleted files and of those that will be when their replica starts |
| `error` | Why the last run failed, without identifiers or hashes |

```sql
SELECT request_id, completed_at, t->>'table', t->>'rows_deleted'
FROM (SELECT *, unnest(json_extract(tables::JSON, '$[*]')) AS t FROM igloo.erasure_requests);
```

Each run of a request is in `igloo.task_history`, as kind `erasure`. The
log says `erasure requested`, `erasure request completed`, `erasure request
held`, `confirmed`, `canceled` and `failed`, and why a run failed. Requests
are kept in the metadata database (schema `igloo_erasure`); without one, in
memory, lost on restart.

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
too. Time travel on main reaches back that far at most. With an hourly
maintenance pass, a deleted row is gone from storage about an hour after it
is older than the retention. Branches and tags, and the snapshots they point
at, are kept as long as their own `max-ref-age-ms` or the table's says,
forever by default: a tag can be someone's reference point, such as an
audit's. Drop the ones that must not outlive the retention, or let an
erasure request rewrite them.

A retention is at least a day. Admins set and remove data retentions of any
table, holders of `ERASE` of tables with tagged columns; everyone may list
those of the tables they may see. They are kept with policies and tags, stay with a table's name, and
renaming such a table is refused.

## What it does not cover

- **Columns without tags.** A request finds rows only by tagged columns: an
  email in a free-text note stays, as does the rest of a row in a table
  without the tags. Lineage reports tables written from tagged ones; tag
  them.
- **Other forms of the identifier**: digests of other normalizations
  (Gmail's dots, phone numbers' formats), salted or keyed hashes, base64.
  In history, each identifier is found as it is, compared without regard to
  case; a form written otherwise stays.
- **A dropped column's values in rows that stay.** A column that another
  engine dropped is not in the rows' current form: a request rewrites the
  files that hold the person's values in it, so the values go, but cannot
  tell which rows were the person's by it, so those rows stay.
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
- **Tables of connections** ([federation](federation.md)), which are in
  another database: Igloo reads them but cannot erase from them. A tagged
  column of one is listed in the request's `tables` as `remote`; erase the
  person in that database. A request for a tag value only connections'
  tables have is refused.
- **Files outside the warehouse**, such as files another engine added to a
  table from a landing bucket: a request removes them from the table but
  does not delete them with Igloo's credentials; it counts them in
  `files_outside_warehouse` and logs their paths, for whoever keeps them to
  delete.
- **Copies outside the warehouse**: files in `IGLOO_ALLOWED_LOCATIONS`,
  landing buckets that loads read and exports wrote, other engines' copies
  of the tables (Spark, Trino, Snowflake), BI tools' caches, and files
  partners already downloaded through [Delta Sharing](sharing.md).
- **Engines that read through the [Iceberg REST
  catalog](external-engines.md)** get storage credentials that last up to
  an hour. Until a request deletes the files, such an engine can read them
  with credentials it already holds, and what it read or cached is its own
  to erase. An engine that holds a table's metadata from before the
  request still lists the deleted files, and fails to read them once they
  are gone.
- **Tables dropped before the request**, whose files the catalog may keep
  for a while, and the definitions of tasks, pipes and materialized views,
  which are SQL and not history.
- **Tables partitioned by an identifier**, whose partition values are in
  file paths and manifests.
- **Statements run after the request completes**, which are recorded as
  usual, and logs at debug level, which can quote statements other than
  those with `ERASE SUBJECT`.

## Cost

A request reads the tagged columns of every tagged table in full, every
file of every snapshot, when it starts and again when it verifies, and
rewrites only the files that hold matching rows, at most 8 GB per commit,
on every branch and tag that holds them. It reads every record of the
history stores twice, as it redacts and as it verifies, and, before it
deletes files, the metadata of every table in the catalog, to be sure no
other table references them. DuckDB reads only the tagged columns, with all
of a replica's cores; a table of billions of rows takes minutes. Requests
run one at a time, so batching people into one request saves reading the
same tables again. Each replica's read cache is emptied once after a
request deletes files, and fills again as queries read.
