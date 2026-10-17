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

- `table/table.go` and `table/transaction.go`: a `commit.retry.num-retries`
  greater than 0 makes a conflicting commit rebuild its manifest list from the
  fresh parent. For a rewrite (compaction's `RewriteDataFiles`), that rebuild
  concatenates the fresh parent's whole manifest set with the producer's own
  manifests, and the overwrite producer's `processManifests` does not drop the
  files the rewrite replaced, so a replaced file comes back live and its rows
  duplicate. Until the rebuild drops replaced files, `Transaction` gains a
  `CommitWithoutRetry` that forces retries off (a new unexported `noRetry`
  commit option); Igloo's maintenance (compaction and snapshot expiry) uses it
  and retries at its own level on `ErrCommitFailed`, so a table that sets
  `commit.retry.num-retries` (for another engine) cannot make Igloo's
  compaction duplicate rows.

Drop this copy and the `replace` directive once an iceberg-go release reads
DuckDB-written tables, lets callers choose these Parquet writer defaults and
no longer needs an MPL-licensed dependency.
