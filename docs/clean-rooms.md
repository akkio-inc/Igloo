# Clean rooms

A clean room lets an agency and its partners, such as a publisher, a
retailer or a TV maker like TVCo Ads, measure what they could only measure
together: how much of one audience another has, the reach and frequency of
a campaign among a brand's customers, the conversions that followed
exposure. They join their people-level data on hashed emails or device IDs,
and neither sees the other's rows.

Igloo's clean rooms work like Snowflake Data Clean Rooms and Databricks
Clean Rooms. Each party contributes datasets with a policy that says which
columns are identifiers, which may be grouped by and which aggregated.
Analyses are templates, SQL that Igloo checks against those policies and
that runs only after every party whose data it reads has approved that
exact version. A run executes as the room's own identity, not as whoever
asked; every result row counts at least a threshold of each dataset's
people, or it is left out; and only the party that asked receives the
result.

Clean rooms need the metadata database (`IGLOO_METADATA_DATABASE_URL`) and
the Iceberg catalog, as [Delta Sharing](sharing.md) does. Partners take part
with their Delta Sharing recipient's bearer token.

## A room between an agency and a TV maker

The agency's analysts are the role `agency_analysts`. TVCo Ads is a Delta
Sharing recipient, `tvco_ads`, and shares its exposure log with the agency,
which reads it as an external table of the provider `tvco`:

```sql
-- An admin: the room and its parties.
CREATE CLEAN ROOM tv COMMENT 'Agency x TVCo Ads, Q4';
ALTER CLEAN ROOM tv ADD COLLABORATOR agency ROLE agency_analysts;
ALTER CLEAN ROOM tv ADD COLLABORATOR tvco RECIPIENT tvco_ads PROVIDER tvco;

-- An admin adds the partner's data, an external table of its provider.
CREATE EXTERNAL TABLE partners.tvco_exposures FROM SHARE tvco.tv.measurement.exposures;
ALTER CLEAN ROOM tv ADD DATASET exposures OF tvco FROM partners.tvco_exposures
  JOIN ON (email_sha256) GROUP BY (campaign_id, exposed_on, test_group) THRESHOLD 100;

-- An agency analyst adds the agency's own, a table its role owns
-- (ALTER TABLE marts.crm_audience OWNER TO agency_analysts).
ALTER CLEAN ROOM tv ADD DATASET crm OF agency FROM marts.crm_audience
  JOIN ON (email_sha256) GROUP BY (segment, region);

-- An analysis, written by Igloo from a built-in template.
ALTER CLEAN ROOM tv ADD TEMPLATE overlap
  FROM BUILTIN audience_overlap (left => crm.email_sha256, right => exposures.email_sha256, by => crm.segment)
  RUN BY (agency);
SHOW TEMPLATES IN CLEAN ROOM tv;            -- its SQL, version 1, and who must approve it

ALTER CLEAN ROOM tv APPROVE TEMPLATE overlap VERSION 1;    -- the agency's approval
```

TVCo Ads reviews the template and approves it over the room's
[API](#partners), with the token in its Delta Sharing profile:

```sh
curl -H "Authorization: Bearer $LG_TOKEN" https://data.agency.example:8444/clean-rooms/tv
curl -H "Authorization: Bearer $LG_TOKEN" -d '{"version": 1}' \
  https://data.agency.example:8444/clean-rooms/tv/templates/overlap/approval
```

Then the agency runs it, and reads its result:

```sql
RUN CLEAN ROOM tv TEMPLATE overlap;
```
```
 run_id |  status   | row_count | truncated |          result_table
--------+-----------+-----------+-----------+---------------------------------
      1 | succeeded |         2 | f         | clean_rooms.tv__overlap__agency
```
```sql
SELECT * FROM clean_rooms.tv__overlap__agency;
```
```
 segment | audience | overlap | overlap_percent
---------+----------+---------+-----------------
 lapsed  |     1500 |     500 |           33.33
 loyal   |     1500 |    1500 |             100
```

A third segment had too few people to show, so the result left it out,
without a trace. Nobody at the agency, its admins included, can read TVCo's
exposure log, and nobody but the agency's analysts can read or change its
CRM or read the result.

## Rooms and collaborators

| Statement | Does |
|---|---|
| `CREATE CLEAN ROOM [IF NOT EXISTS] room [COMMENT 'text']` | Create an empty room (admins) |
| `DROP CLEAN ROOM [IF EXISTS] room` | Drop a room with its templates, runs and result tables; its datasets' tables are readable as before (admins) |
| `SHOW CLEAN ROOMS` | The rooms you are in, with their collaborators and the ones you act for; every room for admins |
| `ALTER CLEAN ROOM room ADD COLLABORATOR name USER user` | A party that is one local user |
| `ALTER CLEAN ROOM room ADD COLLABORATOR name ROLE role` | A party that is the holders of a custom role (`CREATE ROLE`) |
| `ALTER CLEAN ROOM room ADD COLLABORATOR name RECIPIENT recipient [PROVIDER provider]` | An outside partner: its Delta Sharing recipient, whose token reaches the room's API, and the provider its data comes from |
| `ALTER CLEAN ROOM room REMOVE COLLABORATOR name` | Remove a party that contributes no datasets and no template names, with its approvals and result tables |
| `SHOW COLLABORATORS IN CLEAN ROOM room` | The room's parties, and whether you act for each |

A user acts for a collaborator when it is their user or they hold its role.
Admins set rooms up but act for no collaborator unless they are its user or
hold its role: they cannot read a party's data or approve for it. Names are
lowercase letters, digits and single underscores.

## Datasets and their policies

```sql
ALTER CLEAN ROOM room {ADD | REPLACE} DATASET name OF collaborator FROM [schema.]table
  JOIN ON (column, ...)                 -- identifiers: required
  [GROUP BY (column, ...)]              -- dimensions
  [AGGREGATE (function(column), ...)]   -- sum, avg, min, max, median, stddev
  [BOUNDS (column BETWEEN lo AND hi, ...)]  -- a measure's range per person
  [THRESHOLD n]                         -- 50 unless given; at least 10
  [MAX ROWS n]                          -- a cap on a result's rows
  [RUNS PER DAY n]                      -- 100 unless given, 10 with NOISE
  [NOISE epsilon];                      -- Laplace noise on counts and sums; none unless given
ALTER CLEAN ROOM room REMOVE DATASET name;
SHOW DATASETS IN CLEAN ROOM room;
```

A dataset is a table of the catalog, or a partner's shared table read as an
[external table](sharing.md#reading-partners-shares). Its policy says what
templates may do with each column, and nothing else is reachable:

- **Identifiers** (`JOIN ON`): hashed emails, device IDs, household IDs.
  Templates use them only to join datasets (`ON a.email_sha256 =
  b.email_sha256`) and to count them (`count(DISTINCT a.email_sha256)`,
  `approx_count_distinct`, an HLL or theta sketch's estimate). They never
  reach a result, a filter or a `GROUP BY`, through a function or otherwise.
- **Dimensions** (`GROUP BY`): campaign, day, segment, region. Results may
  show them, be grouped by them and filter on them.
- **Measures** (`AGGREGATE`): only the aggregates listed, such as
  `sum(spend)`. Rows are counted with `count(*)`, and people with counts
  of identifiers. A template that joins datasets of different parties may
  only sum a measure, and only one whose policy gives its range per person
  (`BOUNDS (spend BETWEEN 0 AND 1000)`), when every dataset it reads has
  `NOISE` (see [sums across parties](#sums-across-parties)).
- **Threshold**: the fewest distinct identifiers of the dataset a result
  row must count, for each of its identifier columns; rows with fewer are
  left out (see [thresholds](#thresholds)).
- **Max rows**, **runs per day** and **noise** are the dataset's part of
  the [defences against differencing](#differencing).

A run follows the strictest policy of the datasets it reads: each dataset's
threshold applies to its own identifiers, the smallest row cap and the
smallest epsilon apply to the whole result, and each dataset's budget
counts the runs that read it.

Who adds what: a local collaborator's members (or admins) add its datasets,
from tables they own in the [catalog](catalog.md#owners), themselves or
through a role, and can read; only admins add a partner's, and only
external tables of the partner's provider. Reading a table is not enough,
as readers read every table: a room keeps its dataset from everyone else,
which is the owner's decision, or an admin's (`ALTER TABLE ... OWNER TO`
gives a table to a collaborator's role). The same goes for `REPLACE
DATASET`'s new source. These cannot be
datasets; contribute a table of what the room may read instead (`CREATE
TABLE ... AS`, or a [materialized view](loading.md#materialized-views), which
Igloo keeps fresh):

- a table with masking or row access policies: a run reads the raw data;
- a view: its rows are those of the tables it reads, which the room could
  keep to the party only by taking them from everyone else who reads them;
- a [connection](federation.md)'s table: its database keeps it, and whoever
  has access there, or another connection to it, reads it.

A table can be in several rooms, for one party only. `REPLACE DATASET`
changes the source or the policy at once for every template, and every
template that reads the dataset needs every approval again (see
[approvals](#approvals)); templates are checked against the policies again
at every run. A dataset's source must stay the table it was: a run is
refused once the table is made again or its columns change, until its
party replaces the dataset and every party approves again.

## Templates

```sql
ALTER CLEAN ROOM room {ADD | REPLACE} TEMPLATE name [PARAMETERS (name type, ...)] RUN BY (collaborator, ...) AS query;
ALTER CLEAN ROOM room {ADD | REPLACE} TEMPLATE name FROM BUILTIN builtin (argument => value, ...) RUN BY (collaborator, ...);
ALTER CLEAN ROOM room REMOVE TEMPLATE name;
SHOW TEMPLATES IN CLEAN ROOM room;
```

`RUN BY` names the collaborators that may run the template and receive its
results; approving a template approves that too. Each `REPLACE TEMPLATE`
makes a new version, which needs every approval again. Members of any
collaborator, and admins, add templates.

A template is one `SELECT` over the room's datasets, named as they are in
the room. Igloo checks it when it is added and before every run, on DuckDB's
own parse of it (`json_serialize_sql`), node by node, never on its text;
anything it does not know is refused, so new SQL syntax stays closed until
someone decides it is safe. It accepts:

- `FROM` the room's datasets, each with an optional alias, joined with
  `INNER`, `LEFT`, `RIGHT` or `FULL JOIN ... ON`. Every join condition
  equates an identifier of a dataset on one side of the join with one of a
  dataset on the other, and may add conditions on dimensions, such as an
  attribution window (`c.converted_on BETWEEN e.exposed_on AND e.exposed_on
  + INTERVAL 7 DAY`). No subqueries, `WITH`, table functions, files, set
  operations, `USING`, `NATURAL`, `ASOF`, `POSITIONAL`, `LATERAL`, `SEMI`
  or `ANTI` joins, time travel or samples.
- Columns written `alias.column`, each in its dataset's policy.
- An aggregated result: `GROUP BY` dimensions, or only aggregates. Every
  result column has a name: a column's own, or `AS`.
- Aggregates of a bare column, never of an expression: `count(*)`,
  `count(a.id)`, `count(DISTINCT a.id)`, `approx_count_distinct(a.id)` and
  a sketch's estimate (`datasketch_hll_estimate(datasketch_hll(14, a.id))`,
  with `lg_k` from 10 to 21) of identifiers, and the aggregates a policy
  allows of its measures, such as `sum(e.spend)`; across parties, only
  bounded sums with noise ([sums across parties](#sums-across-parties)). A
  part of a group is taken with `FILTER (WHERE ...)` on dimensions, such
  as exposed against control, at most four different conditions in a
  template, which [thresholds](#thresholds) count.
- Scalar functions from a short list, all of which read only their
  arguments: arithmetic, `round`, `greatest`, `nullif`, `CASE`, `CAST`,
  comparisons, `IN`, `LIKE`, text functions such as `lower` and
  `split_part`, and date functions such as `date_trunc`, `date_part`,
  `strftime` and `INTERVAL`s, of dimensions and of aggregates.
- `WHERE` on dimensions and parameters, `ORDER BY` result columns or their
  positions, and `LIMIT` a number.
- Parameters: `$name`, declared in `PARAMETERS` with a type (`VARCHAR`,
  `BIGINT`, `INTEGER`, `DOUBLE`, `DECIMAL(p,s)`, `BOOLEAN`, `DATE`,
  `TIMESTAMP` or `TIMESTAMPTZ`), used only in `WHERE`, each in a condition
  (of those `AND`ed together) on one dataset's columns. They are bound as
  values of their type when the template runs, never pasted into SQL.

And it refuses, among others: an identifier in the result, under another
name or in another case; any function of one (`substring`, `left`, `hash`,
`md5`, `lower`, `||`, `CAST`, `coalesce`, `CASE`, `list`, `array_agg`,
`string_agg`, `list_transform`); `min`, `max`, `any_value`, `first`,
`arg_min`, `mode`, `histogram` or `median` of one; a bare sketch, which
holds hashes of the identifiers it saw; an identifier in `GROUP BY`, `WHERE`,
`FILTER`, `ORDER BY` or `IS NULL`, or joined to anything but another
dataset's identifier; an aggregate of an expression (`count(CASE WHEN ...
END)`, `max(CASE ...)`, `sum(1)`), which would pick out a part of a group
that no threshold counts; any aggregate of a dimension (the `max` of one is
one person's value) other than grouping by it; `ROLLUP`, `CUBE`, `GROUPING
SETS` and `grouping()`, since a total less the groups shown is the groups
left out; `SELECT DISTINCT`, `HAVING`, `QUALIFY`, window functions,
`OFFSET`; and functions such as `random`, `current_setting`, `getvariable`,
`query` or `read_text`. The Go tests list every case
(`internal/cleanroom/template_test.go` and `attacks_test.go`).

```sql
ALTER CLEAN ROOM tv ADD TEMPLATE genre_reach PARAMETERS (first DATE, last DATE) RUN BY (agency) AS
SELECT e.campaign_id, count(DISTINCT e.email_sha256) AS reach, count(*) AS impressions
FROM exposures e JOIN crm c ON e.email_sha256 = c.email_sha256
WHERE e.exposed_on BETWEEN $first AND $last
GROUP BY ALL ORDER BY reach DESC;
```

### Built-in templates

Igloo writes these from arguments that name the room's datasets and
columns. Their SQL is checked like any template's and shown by `SHOW
TEMPLATES`, so collaborators approve exactly what runs.

| Built-in | Arguments | Result |
|---|---|---|
| `audience_overlap` | `left => d.id`, `right => d.id`, optional `by => d.column` (of left), `right_by => d.column` (of right), `approximate => true` | Without `right_by`: by left's dimension, `audience`, `overlap` and `overlap_percent` (a `LEFT JOIN`). With it: the `overlap` by both sides' dimensions. |
| `reach_frequency` | `exposures => d.id`, `campaign => d.column`, `time => d.column` (a date or timestamp), optional `audience => d.id` (count only these people), `approximate => true`; parameters `start_date` and `end_date` | `impressions`, `reach` and `frequency` by campaign and day. A campaign's deduplicated reach over its flight is a template of its own, grouped by campaign: one result with both would show, less the days shown, the days left out |
| `conversion_lift` | `exposures => d.id`, `group => d.column` (exposed or control), `conversions => d.id`, optional `campaign => d.column`, `exposed => 'exposed'`, `control => 'control'`, and `exposure_time`, `conversion_time`, `window_days` together | For exposed and control people: how many, how many converted (within the window after exposure), the conversion rates and `lift_percent` |

`approximate => true` counts people with an HLL sketch's estimate
(`datasketch_hll_estimate(datasketch_hll(14, id))`) instead of exactly, for
audiences of hundreds of millions where exact distinct counts need much
memory. Thresholds count exactly either way. Lift uses the exposure log's
own exposed and control groups (a holdout), each of which must reach the
thresholds.

## Approvals

```sql
ALTER CLEAN ROOM room APPROVE TEMPLATE name VERSION n [AS collaborator];
ALTER CLEAN ROOM room REVOKE APPROVAL OF TEMPLATE name [AS collaborator];
```

A template runs only once every collaborator whose datasets it reads has
approved its current version; `SHOW TEMPLATES` lists who must and who did.
The version you approve must be the current one, so nobody approves a
template they did not see. Version numbers are never used twice in a room,
removed templates' included, so a version you reviewed is the one you
approve. Each approval also holds a digest (`SHOW TEMPLATES`, and the API,
show it): a SHA-256 hash of the version's SQL, parameters and `RUN BY`,
and of each dataset the template reads, its party, its source (the table,
by its Iceberg table UUID or its partner's provider, and its columns) and
its policy. An approval counts only while the digest is still the same,
so a new version, or a dataset replaced or given another policy (`REPLACE
DATASET`), needs every approval again, whoever's dataset it is; and
withdrawing an approval stops runs at once. Only a collaborator's members
approve for it, and only a partner's own token approves for a partner.
`AS` says which collaborator you act for when you act for several.

## Runs and results

```sql
RUN CLEAN ROOM room TEMPLATE name [(parameter => value, ...)] [AS collaborator];
SHOW RUNS IN CLEAN ROOM room;
```

Values are strings, numbers, `TRUE`, `FALSE` or `NULL`, cast to the
parameter's type. A local collaborator's result goes to the table
`clean_rooms.<room>__<template>__<collaborator>`, which each run replaces and
which only that collaborator's user or role reads (it is granted `SELECT` on
it, for users with role `none`). A partner's comes back in the API's
response and stays in the run history, for the partner alone. Igloo creates
the schema `clean_rooms` and owns the tables in it.

Every run is in the room's history, which every collaborator of the room
sees: its template and version, its parameters, who asked and for which
collaborator, whose approvals it relied on, the datasets it read, its status
(`succeeded`, `refused` when Igloo stopped it to protect the data, or
`failed`), its rows, and whether a row cap cut it short. How many groups
were left out is said nowhere: with an inner join, a group of a
requester's own data that exists only when someone in it matched would
tell who. Runs are kept for `IGLOO_AUDIT_RETENTION`. `RUN CLEAN ROOM` and
partners' requests are also in the query history.

### How a run works

1. Igloo checks the template again against the policies as they are now,
   and that every approval is there, that every dataset is confined to its
   party (below), is still the table it was, and has no policies, and that
   the requester has runs left in each dataset's budget.
2. It writes the run's SQL from the template's parse: the datasets' tables
   instead of their names, the parameters as `$1, $2...` cast to their
   types, every function qualified with DuckDB's own schema (`"system".main`)
   so no macro can stand in for one, Laplace noise on counts when a policy
   asks for it, and hidden columns per dataset in `FROM` and per identifier
   column: `count(DISTINCT alias.identifier)` in the whole group, within
   each `FILTER` of the result, and within each combination of the
   `FILTER`s and their opposites.
3. It opens a session of the room's own identity, `clean_room:<room>`, which
   no user can be called and nobody signs in as: role `none`, with `SELECT`
   on the run's datasets' tables and nothing else. The requester's grants,
   roles and policies play no part. Its statements take every check a user's
   do: DuckDB's plan must read exactly the granted tables, and the
   confinements must admit the identity.
4. With parameters, it runs the [population check](#differencing).
5. It computes the groups into a temporary table of that session, which
   nobody else can see, and keeps only those whose hidden counts meet the
   thresholds, in the template's order, up to the row cap (10,000 rows at
   most). Only those leave the session. A sum across parties reads the
   joins through a subquery that totals each person's rows per group first,
   so it can clamp each total to the bounds. A run that fails from the
   population check on ends as `refused`, without saying why: its error,
   such as an overflow, could depend on another party's rows. The host's
   admins find it in Igloo's log.

## Thresholds

Each result row must count, for every dataset in `FROM` and every one of
its identifier columns, at least that dataset's threshold of distinct
values, or the row is left out, not flagged. Within a `FILTER`, the part of
the row it picks out must reach the thresholds too, so an exposed group of
10,000 cannot hide a control group of 8. So must every combination of the
`FILTER`s and their opposites, or have no one in it: `sum(spend)` less
`sum(spend) FILTER (WHERE segment <> 'vip')` would otherwise be the vip's
spend. A dataset that contributes no identifiers to a row (as the right
side of a `LEFT JOIN` without a match does) counts zero, and the row is
left out: that a whole segment of one party's customers is absent from the
other's data says something about each of them.

The default is 50 and the least is 10: k-anonymity of 50 is common practice
in measurement, smaller groups are easy to tell people apart in, and a
party may only raise the protection of its own data, never lower another's.
The count is of distinct identifier values, the closest Igloo can come to
people; a dataset with two identifier columns must reach its threshold in
both, so a sparse column (one that is often NULL) makes more rows fall short,
never fewer.

## Differencing

Thresholds stop a result from showing a small group. They do not stop two
results whose difference is a small group: the overlap of segment A with
the partner's audience, and of A with one more person added, differ by
whether that person is the partner's. Igloo defends against what it can
tell, and says what remains:

- **Parameters that narrow too little or too much are refused.** Before a
  template with parameters runs, Igloo counts the distinct identifiers of
  each dataset a parameter's condition reads, in that dataset alone, with
  and without those conditions. If they keep between one and threshold-1
  of them, or leave out between one and threshold-1, the run is refused
  and recorded as `refused`: comparing it with a run without the filter,
  or with one that leaves nothing out, would show those few people.
  Counting each dataset alone, not the joined data, keeps the refusal from
  telling whether the people a requester's own values pick out are in the
  other party's data. Parameters appear only in `WHERE`, so this covers
  everything a requester chooses at run time; joins, groups and `FILTER`s
  are fixed by the approved template.
- **Budgets.** Each dataset allows each collaborator `RUNS PER DAY` runs of
  templates that read it, in any 24 hours, refused runs included: 100
  unless its policy says otherwise, or 10 with `NOISE`. A run with noise
  counts once for each different count or sum it noises. This bounds how
  many comparisons anyone can make, and how far noise can be averaged
  away: the mean of n runs' noisy answers is off by the noise divided by
  the square root of n, so ten runs a day leave about a third of it.
- **Noise.** With `NOISE epsilon`, every count in a result (`count`,
  `count(DISTINCT)`, estimates) gets Laplace noise of scale 1/epsilon,
  rounded and never below zero, every sum across parties noise of scale
  (hi - lo)/epsilon (see [below](#sums-across-parties)), and figures
  computed from them (rates, frequency, lift) are computed from the noisy
  ones. A count of distinct people changes by at most one when one person
  joins or leaves, so each such count is epsilon-differentially private;
  with several counts and several runs the privacy loss adds up, which the
  budget bounds. The noise comes from a SHA-256 hash of a secret new to
  each run, the aggregate and its group, so the same count written a
  hundred times has the same noise, and averaging the copies gains
  nothing; different aggregates each cost the budget. The smallest epsilon
  of the datasets a run reads applies. Thresholds are applied to exact
  counts, before noise.
- **History.** Every collaborator sees every run's parameters and who asked,
  so a pattern of runs that isolates someone is there to be seen.

What remains possible, and why to approve templates with care:

- A party that controls the grouping of its own data can make two groups
  that differ by one known person, such as two segments of its CRM, and
  compare their overlap with the other's data in one run; or a group of
  threshold-1 people it knows are in the other's data and one it does not,
  which shows only if that one is; or a `FILTER` that leaves out that one
  person, whose row shows only if they are not there. Each tells it
  whether the person is in the other's data. No threshold can stop this;
  noise blurs it, and budgets bound how often it can be repeated. Approve
  templates that group by a partner's own dimensions only with partners you
  trust, or with noise.
- If all, or none, of a group are in the other's data, a result says so of
  each of them (a 100% overlap of a segment of 50 known people). Igloo
  leaves out groups with no overlap; it does not leave out complete ones.
- Sums across parties are under review, do not rely on them for privacy
  (see [below](#sums-across-parties)). Approve them only with partners you
  would trust with the rows the sum reads.
- Templates of one party's datasets keep every aggregate the policy allows,
  without bounds or noise: the requester controls none of the data, but a
  sum, minimum or maximum over a small difference of groups, across runs
  with different parameters, can still be one person's value. Allow
  `min`, `max` and `median` only of columns where that is acceptable.
- Counts that are not `DISTINCT`, such as `count(*)`, count rows, not
  people: a person with many rows, or with rows the joins multiply, moves
  them by as many, under noise scaled to one. They are under review, do
  not rely on them for privacy; count people with `count(DISTINCT id)`.
- Runs of different templates, or of one template by different
  collaborators, are not compared with each other.

## Sums across parties

A template that joins datasets of different parties sums a measure only
when the measure's policy gives its range per person, `BOUNDS (spend
BETWEEN lo AND hi)`, and every dataset it reads has `NOISE`. Unbounded, one
person could decide a sum: a party sets a column of its own to 1 for one
person and 0 for everyone else, allows `sum` of it, and sums it over a
group far above the thresholds; the sum is over zero exactly when that
person is in the other party's data, in one run, noise or not. Moving one
person between groups of its own between two runs would likewise show the
other party's value for them.

So such a sum is bounded per person and noised: each person (by the first
identifier of the measure's dataset) adds to a group their total of the
group's rows, clamped to the bounds however many rows the joins give
them, and the sum gets Laplace noise of scale (hi - lo)/epsilon, the
strictest epsilon of the datasets read. Bounds are each party's own
choice, for its own columns; they must include 0 (`lo <= 0 <= hi`) and
lie within ±10^15, and values are clamped as `DOUBLE`, so no sum
overflows. The minimum, maximum, median and spread of a measure are one
person's value or close to it, and no added noise makes them safe, so
they are refused across parties; an average is a sum over a count, both
of which a template can show (`sum(v.minutes) / count(DISTINCT v.email)`).

These sums are under review, do not rely on them for privacy. The noise
does not hide everything one person can do to a result: the identifier a
total is clamped by is the one its party lists first, which need not be
the one the template joins on, nor one per person; one person can be in
many groups of a result, each noised apart; and the thresholds read
exact counts. Approve a sum across parties only with partners you would
trust with the rows it reads.

## Partners

A partner reaches the room's API at the same address and with the same
bearer token as its Delta Sharing profile (the `endpoint` and `bearerToken`
of its `.share` file), with `/clean-rooms` in place of `/delta-sharing`.
Create its recipient as for sharing (`CREATE RECIPIENT`), send it the
activation link, and add the recipient as a collaborator. The token reads
nothing but the shares granted to the recipient and the rooms it is in; it
signs in to nothing else.

| Request | Does |
|---|---|
| `GET /clean-rooms` | The rooms the partner is in |
| `GET /clean-rooms/{room}` | Its collaborators, the datasets and their policies (not where they are kept), and the templates with their SQL, parameters, versions, digests and approvals |
| `POST /clean-rooms/{room}/templates/{template}/approval` | `{"version": n}` approves that version for the partner |
| `DELETE /clean-rooms/{room}/templates/{template}/approval` | Withdraws the partner's approval |
| `POST /clean-rooms/{room}/templates/{template}/runs` | `{"parameters": {"start_date": "2026-10-01"}}` runs a template the partner may run; the response holds the run and its result |
| `GET /clean-rooms/{room}/runs` | The run history |
| `GET /clean-rooms/{room}/runs/{id}` | A run, with its result if the partner asked for it |

```sh
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"parameters": {"start_date": "2026-10-01", "end_date": "2026-10-31"}}' \
  https://data.agency.example:8444/clean-rooms/tv/templates/reach/runs
```
```json
{"id": 7, "template": "reach", "version": 1, "requester": "tvco", "status": "succeeded", "row_count": 14, "truncated": false,
 "result": {"columns": [{"name": "campaign", "type": "text"}, {"name": "day", "type": "date"}, ...],
            "rows": [["c0", "2026-10-01", "596", "298", "2"], ...]}}
```

Errors come as `{"errorCode": ..., "message": ...}` with HTTP status 401
(no or wrong token), 403 (`PERMISSION_DENIED`, or `RUN_REFUSED` for a run
Igloo refused to protect the data), 404, 400 (`INVALID_PARAMETER_VALUE`) or
429 (`RESOURCE_EXHAUSTED`, a run budget spent). Values in results are text,
as PostgreSQL clients receive them. Without TLS, the API answers only
requests to `localhost`, as the sharing endpoint does. Every request is in
the query history as `recipient:<name>`, with `statement_type` `CLEAN ROOM`.

A partner's data comes into the room through a share it grants to the
agency: the agency registers the partner's profile as a provider and maps
the shared table to an external table (see [Delta Sharing](sharing.md)), and
an admin adds that as the partner's dataset. Results go back through the
API. A partner that would rather host the room itself can: Igloo's API is
the same wherever it runs.

## The web console

The console's **Clean rooms** page lists the rooms you are in (every room
for admins), and for each its templates with their SQL and approvals, its
run history, its datasets and policies, and its collaborators. It approves
and runs templates with the statements above, asking for parameter values,
and shows the latest result of yours. The server decides who may do what,
as for any SQL.

## Security

What a clean room protects: each party's rows from the other parties, and
results from everyone but the party that asked. It is enforced in the same
places Igloo enforces policies, and fails closed:

- **Raw datasets** are confined: a dataset's table or external table is
  read only by its own party's users (a local collaborator's user or role
  holders), as before, and by its room's runs. Everyone else is refused,
  admins included, in every position and through any view, over the
  PostgreSQL protocol, Flight SQL and the web console alike, and so are
  alerts, tasks and materialized views they own, which run as them. A
  partner's dataset is read by no local user at all. While any table is
  confined, users other than admins cannot read the warehouse's files, nor
  external tables' files, directly, as while tables have policies. The
  Iceberg REST catalog refuses a dataset to everyone but admins and its own
  party, and Delta Sharing neither shares nor serves one. Confinements are
  kept with the policies, reach other replicas within 30 seconds as policies
  do, and stay with a table's name: a table of that name created again is
  confined too, and a confined table cannot be renamed.
- **Changes.** Only a dataset's own party writes to it (`INSERT`, `UPDATE`,
  `DELETE`, `MERGE`, `COPY ... FROM`), replaces, drops, alters or renames it,
  or drops its schema, in SQL, with `CLONE` or through the Iceberg REST
  catalog: otherwise someone else could change what a party contributes,
  and its runs would read that. A result table is written by its room's
  runs alone, its own party's users included.
- **Copies and snapshots.** Only a dataset's party may
  [clone](sql.md#clones) it (a clone carries no confinement: it is the
  party's own copy, outside the room), or a schema holding it, and only the
  party reads it at a branch, tag or snapshot (`AT (...)`), lists its
  branches and tags, or makes, moves or drops them.
- **The catalog** shows a dataset, and a result table, to its own party
  alone: to everyone else, whatever their role, admins included, its name,
  description, owner, columns, tags, size, usage, sample rows and
  `igloo.policy_references` row (`policy_kind` `CLEAN ROOM`) are not in
  `igloo.tables`, `igloo.columns`, `SHOW OBJECTS`, search or the console's
  catalog pages, and no lineage edge to or from it shows, not even without
  its name. Table and column names stay in the system catalogs
  (`information_schema`, `duckdb_tables()`), as for every table.
- **Runs** execute as the room's identity, with `SELECT` on the run's
  datasets alone, through the same checks as any statement. A requester's
  grants and policies cannot widen what a run reads, nor apply to another
  party's data.
- **Templates** are checked on DuckDB's parse, with every unknown node
  refused, when they are added and before every run.
- **Results** hold only rows that meet every threshold, and only aggregates
  of what the policies allow. A local result table is confined to the
  requester's party from the moment its template exists, before any result
  is written, so no replica serves it to anyone else.
- **Approvals** bind one version, whose number is never used again, and its
  digest; changing a template needs every approval again.
- **Partners** authenticate with their recipient's 256-bit bearer token,
  checked as the sharing endpoint checks it (stored hashed, compared in
  constant time, expiring, rotated with `ALTER RECIPIENT ... ROTATE TOKEN`),
  over TLS or at `localhost` only. A partner sees the policies and SQL of
  the room, not where the other party keeps its data.

**The trust model.** The Igloo operator's admins can read everything on
their own system: they can read the warehouse's files and attach other
databases, read the metadata database, and change Igloo itself. A clean
room protects collaborators from each other, not from the host. SQL refuses
datasets to admins who are not of a dataset's party, so that nobody reads
them by accident, and the query history records what admins do, but admins
are not a boundary. A partner that needs protection from the host should
host the room itself, or exchange only what it would share anyway, through
Delta Sharing both ways. Personal data erasure, which runs with Igloo's own
access, reaches confined datasets too.

## Design choices

Clean rooms serve [principle](principles.md) 5: agencies that measure with
publishers in Snowflake or Databricks clean rooms cannot move without them.
They follow principle 2 with safe defaults and few settings, all of them a
data owner's choices for its own data; and they cost a few milliseconds per
statement that names a dataset, for the checks every protected table takes.

- **Templates, not arbitrary SQL.** A restricted shape (one `SELECT` of
  joins and aggregates) is what makes the rules checkable: Igloo can tell,
  node by node, that no identifier reaches a result, and add the counts
  thresholds need. This is the shape of AWS Clean Rooms' aggregation
  analysis and of Snowflake's approved templates; Databricks runs notebooks
  that every party approves, which Igloo does not attempt to check.
- **DuckDB's parse, not text.** Templates are checked and rewritten on
  `json_serialize_sql`, and the SQL that runs is written back by
  `json_deserialize_sql`, so what Igloo checked is what DuckDB runs. What
  Igloo adds (hidden counts, casts, noise) it parses first from text of its
  own in which every name is quoted.
- **Approvals per version, by data owners.** Every party whose data a
  template reads approves exactly what runs, including who may run it; a
  party whose data it does not read has nothing to approve.
- **The room's own identity.** Running as the requester would let their
  grants or policies decide what another party's data looks like, and would
  give a run whatever else they may read. A fixed identity reads the
  datasets and nothing else, with every check still in place.
- **No policies on datasets.** A run reads raw data; a dataset with masking
  or row access policies would let a party's user contribute what they
  cannot see. Contribute a table of what the room may read instead.
- **Tables, not views or connections.** A room keeps exactly the tables a
  party put in it. Confining the tables behind a view would take them from
  everyone else who reads them, and leaving them would leave the view's rows
  readable; a connection's database is not Igloo's to keep. A table made for
  the room, such as a materialized view, is what the party decided to share.
- **Partners are Delta Sharing recipients.** They already have a token, an
  activation flow, rotation and revocation, and they already send data that
  way; one credential per partner is simpler to run and to revoke.
- **Results to the requester only.** A result is the requester's; other
  parties see that it ran, with what, but not what it said.
- **Defaults.** Threshold 50, 100 runs a day per dataset and party (10 with
  noise, which averaging more runs would wear down), no noise unless a party
  asks: thresholds and budgets cost nothing to correct results, while noise
  changes every number and is a party's decision for its own data. Sums
  across parties need it, since without it a sum can answer for one person.
