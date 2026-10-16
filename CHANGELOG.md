# Changelog

All notable changes to Igloo are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Igloo uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The release workflow publishes the section whose heading matches the tag
(`## [1.2.3] - YYYY-MM-DD` for `v1.2.3`) as that GitHub Release's notes.

## [Unreleased]

First release. Move this section under `## [1.0.0] - YYYY-MM-DD` when tagging.

### Added

- SQL endpoint that speaks the PostgreSQL wire protocol (simple and extended
  query protocols, prepared statements, transactions, query cancel), so
  `psql`, JDBC, psycopg, pgx and BI tools connect unchanged.
- Apache Iceberg tables through the Lakekeeper REST catalog, on Amazon S3 and
  S3-compatible stores, Google Cloud Storage and Azure Data Lake Storage Gen2:
  `CREATE TABLE`, `CREATE TABLE AS`, `INSERT`, `UPDATE`, `DELETE`, schemas and
  time travel. Igloo bootstraps the catalog and creates its warehouse on start.
- Users with SCRAM-SHA-256 passwords and `admin`, `writer` and `reader` roles,
  managed with `CREATE USER`, `ALTER USER`, `DROP USER` and `SHOW USERS`, and
  stored in PostgreSQL so every replica shares them.
- A query sandbox: SQL cannot read the server's files or reach locations
  outside the warehouse and `IGLOO_ALLOWED_LOCATIONS`.
- Views on the Iceberg catalog (`CREATE [OR REPLACE] VIEW`, `DROP VIEW`),
  stored as Iceberg views and kept in sync across replicas.
- `CREATE OR REPLACE TABLE` on Iceberg tables.
- Scale to zero: `igloo gateway` accepts client connections in Igloo's
  place, starts Igloo's EC2 instances, Kubernetes Deployment or (through
  commands) any VM when a client connects, and stops them after
  `IGLOO_GATEWAY_SUSPEND_AFTER` without traffic or running work. Igloo
  reports running statements and maintenance on `GET /activity`, and
  records maintenance passes in the metadata database so a pass that is due
  runs soon after a start.
- `scaleToZero.enabled` in the Helm chart runs the gateway in front of
  Igloo's Deployment, behind the same SQL Service, with the RBAC and network
  policies it needs.
- `COMMENT ON TABLE`, `VIEW` and `COLUMN`, stored as Iceberg metadata and
  shown through `obj_description`, `col_description`, `pg_description` and
  `igloo_comments()`.
- SQL cursors (`DECLARE`, `FETCH`, `CLOSE`) and array parameters, so
  psycopg's server-side cursors and Ibis's PostgreSQL backend work.
- Automatic table maintenance: Igloo compacts small files and applies
  deletes, expires snapshots older than `IGLOO_SNAPSHOT_RETENTION` (7 days by
  default, overridable per table with `history.expire.max-snapshot-age-ms`),
  and deletes unreferenced files. Replicas take turns through a lock in the
  metadata database.
- Several replicas can serve the same tables; conflicting concurrent writes
  are retried automatically.
- Compatibility with PostgreSQL catalog queries used by `psql` meta-commands,
  JDBC metadata calls and schema browsers.
- Statement timeouts, connection limits, TLS, health and readiness endpoints,
  Prometheus metrics, and text or JSON logs.
- A web console on port 8443 (`IGLOO_CONSOLE_ADDR`): SQL worksheets with
  cancel and CSV download, a data browser, query history, and admin pages
  for users, roles and grants, and the replica's sessions. Users sign in
  with their Igloo password; statements run as them through the same
  authorization as SQL clients. Sessions are stateless sealed cookies, so
  the console works behind a load balancer. The scale-to-zero gateway can
  forward it too (`IGLOO_GATEWAY_CONSOLE_BACKEND`).
- Deployment with Docker Compose on one machine, a Helm chart, and OpenTofu
  modules for AWS, Google Cloud and Azure.

[Unreleased]: https://github.com/akkio-inc/Igloo/commits/main
