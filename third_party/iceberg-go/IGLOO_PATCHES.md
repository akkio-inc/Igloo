# Igloo's copy of iceberg-go

This is [iceberg-go](https://github.com/apache/iceberg-go) v0.6.0 (Apache
License 2.0, see LICENSE and NOTICE), used through a `replace` directive in
Igloo's `go.mod`. Tests, test data, the `iceberg` command and documentation
were removed; the code is otherwise unchanged except for these patches:

- `manifest.go`, `ReadManifestList`: when a manifest list has no
  `format-version` metadata key, infer the version from its Avro schema
  (`sequence_number` means 2, `first_row_id` means 3) instead of assuming 1.
  DuckDB writes version 2 manifest lists without the key, and iceberg-go then
  refuses to read their manifests.
- `manifest.go`, `dataFile.FileFormat`: return the file format in upper case.
  The spec's format names are case-insensitive, DuckDB writes `parquet`, and
  iceberg-go compares against `PARQUET`, so it refused to read DuckDB's data
  files.
- `table/internal/parquet_files.go`: the default
  `write.parquet.row-group-limit` is 122,880 rows, DuckDB's row group size,
  instead of 1,048,576. DuckDB scans one row group per thread, so compacted
  files with larger groups were read by fewer cores. This is Igloo tuning,
  not a bug fix.
- `table/internal/parquet_files.go`, `GetWriteProperties`: dictionary
  encoding is on by default, as in DuckDB and Java Iceberg. iceberg-go turns
  it off, which made compacted files with repetitive string columns slower
  for DuckDB to scan. Also Igloo tuning.
- `catalog/rest/rest.go`, `RenameView` (new): renames a view through the
  REST catalog's `views/rename` endpoint, as `RenameTable` does for tables.
  Igloo renames views for `ALTER VIEW ... RENAME TO`, which dbt runs.
- `data_file_codec.go`: the manifest-entry schema cache is a small built-in
  LRU instead of `github.com/hashicorp/golang-lru`, which is MPL-2.0. Igloo
  links only permissively licensed code.

The next three patches make files written by compaction match the Parquet
DuckDB writes. Without them, TPC-H queries at scale factor 10 ran 10-40%
slower after compaction than on the files DuckDB had written; with them they
run about 13% faster in total. `internal/maintenance/parquet_test.go` checks
the layout.

- `table/internal/parquet_files.go`, `GetWriteProperties`: decimals with
  precision up to 18 are stored as INT32 or INT64, as Java Iceberg and DuckDB
  store them, instead of FIXED_LEN_BYTE_ARRAY, which DuckDB decodes much more
  slowly.
- `table/internal/parquet_files.go`: the default
  `write.parquet.compression-codec` is `snappy`, DuckDB's codec, instead of
  `zstd`. DuckDB decompresses Snappy faster. Tables that set the property
  keep their codec.
- `table/internal/parquet_files.go`, `ParquetFileWriter`: the Parquet writer
  is created once a row group's worth of rows has arrived, and dictionary
  encoding is turned off for each top-level column in which more than a fifth
  of those values are distinct. This is the rule DuckDB's writer applies per
  row group. The Arrow writer otherwise dictionary-encodes every column until
  the dictionary reaches `write.parquet.dict-size-bytes` (2 MB), so keys,
  prices and free text got dictionaries nearly as large as the data, which
  made DuckDB's scans slower without saving space.

- `table/table.go`, `table/transaction.go` and
  `table/igloo_commit_retries.go` (new): a `commit.retry.num-retries` greater
  than 0 makes a conflicting commit rebuild its manifest list from the fresh
  parent, by adding the fresh parent's whole manifest set to the producer's
  own manifests, and move its requirement on the branch head to the fresh
  head. That is right for a plain append. For a rewrite (compaction's
  `RewriteDataFiles`), a delete or an overwrite, the fresh parent's manifests
  still list the files the commit replaced, so a replaced file came back live
  beside its replacement and its rows duplicated. A commit that adds no
  snapshot (expiry, moving or dropping a ref, a rollback) would be applied to
  refs it never saw, and an append to a table with
  `commit.manifest-merge.enabled` can carry the parent's entries in its own
  merged manifests, which the rebuild adds again. `doCommit` now retries only
  commits that add snapshots, all of them appends, to tables that do not
  merge manifests, and `Transaction` gains a `CommitWithoutRetry` that forces
  retries off for any commit (a new unexported `noRetry` commit option).
  Igloo's maintenance (compaction, snapshot expiry and the rewrite of deleted
  rows) uses it, and retries at its own level on `ErrCommitFailed`, so a
  table that sets `commit.retry.num-retries` (for another engine) cannot make
  it duplicate rows.

One more keeps table maintenance (`internal/maintenance`) from dropping
branches and tags:

- `table/transaction.go`, `ExpireSnapshots`: a `WithMaxRefAge` option
  sets the age at which refs other than main are removed, for refs that do
  not set their own. iceberg-go otherwise removes every ref whose snapshot
  is older than `WithOlderThan`'s age, so expiring snapshots older than
  seven days dropped all tags and branches older than that; Java's Iceberg
  removes refs only by `history.expire.max-ref-age-ms`, forever by default.
  Maintenance passes that property, which iceberg-go itself reads under
  another name.

- `table/scanner.go`, `isDeletionVector`: only a Puffin file is a deletion
  vector. iceberg-go took any position delete file with a referenced data
  file for one, and refused to read it, but Java's Iceberg (1.7 and later)
  writes Parquet position delete files scoped to one data file with it set,
  as for Spark's deletes, and those are read as any position delete file.

- `table/snapshot_producers.go` and `table/igloo_partition_schema.go`
  (new): the manifests a commit writes, for the files it adds, removes or
  keeps, take their partition type from a schema that has every column
  their spec partitions by: the current one, or else the newest of the
  table's schemas that does. iceberg-go used the current schema, which
  lacks a column an older spec partitions by once it is dropped (as Spark
  allows when a newer spec no longer uses it), and left that field out, so
  every file of that spec in a manifest a compaction or a rewrite wrote
  again lost its partition value. `HasSourceColumns` (new, exported)
  tells whether a schema has a spec's columns.

One addition serves the erasure of a person's data (`internal/erasure`):

- `table/igloo_forget_files.go` (new): `Transaction.ForgetFiles` commits a
  snapshot of the same data whose manifests no longer mention given files.
  A removed file stays in its manifest as a DELETED entry, with column
  bounds that can hold the very values erased, and iceberg-go carries such
  manifests into every later snapshot. Java Iceberg has no such operation
  either; `RewriteManifests` comes closest.

Drop this copy and the `replace` directive once an iceberg-go release reads
DuckDB-written tables, lets callers choose these Parquet writer defaults,
no longer needs an MPL-licensed dependency and can rewrite manifests.
