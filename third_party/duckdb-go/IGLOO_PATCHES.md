# Igloo's copy of duckdb-go

This is [duckdb-go](https://github.com/duckdb/duckdb-go) v2.10506.0 (MIT
License, see LICENSE), used through a `replace` directive in Igloo's
`go.mod`. Tests, test data, examples, documentation, scripts and CI files
were removed; the code is otherwise unchanged except for this patch:

- `igloo_arrow.go` (new, built with the `duckdb_arrow` tag like upstream's
  `arrow.go`): `Stmt.ArrowSchema`, `Stmt.ParamArrowSchema` and
  `Stmt.QueryArrowContext`. Upstream returns Arrow record batches only from
  `Arrow.QueryContext`, which prepares the query text again and so runs
  every statement but the last a second time. Igloo's Arrow Flight SQL
  endpoint authorizes a statement once, tells the client the Arrow schemas
  of its parameters and result before running it, and streams DuckDB's
  record batches from that same prepared statement.

Drop this copy and the `replace` directive once a duckdb-go release can
describe and run a prepared statement as Arrow.
