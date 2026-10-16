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

Drop this copy and the `replace` directive once an iceberg-go release reads
DuckDB-written tables, lets callers choose these Parquet writer defaults and
no longer needs an MPL-licensed dependency.
