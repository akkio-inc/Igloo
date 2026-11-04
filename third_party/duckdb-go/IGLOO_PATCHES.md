# Igloo's copy of duckdb-go

This is [duckdb-go](https://github.com/duckdb/duckdb-go) v2.10506.0 (MIT
License, see LICENSE), used through a `replace` directive in Igloo's
`go.mod`. Tests, test data, examples, documentation, scripts and CI files
were removed; the code is otherwise unchanged except for these patches:

- `igloo_arrow.go` (new, built with the `duckdb_arrow` tag like upstream's
  `arrow.go`): `Stmt.ArrowSchema`, `Stmt.ParamArrowSchema` and
  `Stmt.QueryArrowContext`. Upstream returns Arrow record batches only from
  `Arrow.QueryContext`, which prepares the query text again and so runs
  every statement but the last a second time. Igloo's Arrow Flight SQL
  endpoint authorizes a statement once, tells the client the Arrow schemas
  of its parameters and result before running it, and streams DuckDB's
  record batches from that same prepared statement.
- `igloo_bind.go` (new), with a `bindHold` field on `Conn` in
  `connection.go` and calls in `Conn.prepareExtractedStmt` and
  `Stmt.executeBound` (`statement.go`): `Conn.SetBindHold`, a hook the
  connection calls around each step in which DuckDB binds a statement,
  preparing it or creating the pending result that runs it. DuckDB binds a
  statement that reads an Iceberg REST catalog again on every run, and
  binding looks its tables up in DuckDB-Iceberg's one in-memory set of the
  catalog's tables and replaces their entries there, while a committed
  rename or drop removes entries from that set without DuckDB-Iceberg's
  lock, and a listing can free entries a bind replaced while it reads them.
  Igloo holds its catalog lock shared over each bind, so binds run alongside
  each other but never alongside such DDL or a listing, which hold it
  exclusively (see `Server.catalogRunLock` in `internal/pgwire`); upstream
  has no way to run code between binding a statement and running it.
- `igloo_guard.go` (new), with a `callGuard` field on `Conn` in
  `connection.go` and calls in `Conn.prepareExtractedStmt`, `Stmt.Close`,
  `Stmt.ExecContext`, `Stmt.ExecBound`, `Stmt.executeBound`
  (`statement.go`), `Stmt.executeBoundStreaming` (`igloo_stream.go`),
  `rows.Next` and `rows.Close` (`rows.go`) and `recordReader.Next`
  (`arrow.go`): `Conn.SetCallGuard`, a hook the connection runs each call
  through that binds, runs or drops a statement's plan on the calling
  thread. Igloo locks the goroutine to its thread for the call and marks
  the thread as the statement's, so a C++ exception that escapes DuckDB
  there, as the iceberg extension throws Out of Memory out of a destructor
  when a statement is bound at the memory limit, ends that statement's
  session rather than the process (see `internal/engine/terminate`).
  Upstream has no way to run code around those calls.

- `igloo_interrupt.go` (new): `Conn.Interrupt`, `Conn.Progress` and
  `RegisterScalarUDFOn`. Igloo's memory watch cancels the statement that has
  run longest when the process runs low on memory; when that statement does
  not stop, because DuckDB is in a step that does not check for
  interruptions, the watch interrupts it again and reads how far it got
  before it gives up on the process (see `Server.ShedMemory` in
  `internal/pgwire`). Upstream interrupts only through a context and has no
  way to read a query's progress; `RegisterScalarUDFOn` registers a scalar
  function on a connection Igloo holds directly, for tests of such
  statements, through `createScalarFuncIn` (`scalar_udf.go`), which is
  `createScalarFunc` with the connection's context store passed in rather
  than taken from a `*sql.Conn`.

- `igloo_stream.go` (new) and `mapping/igloo_stream.go` (new):
  `Stmt.QueryStreamContext`, a query whose result DuckDB streams, computing
  rows as `Next` reads them (`duckdb_pending_prepared_streaming`), with
  interrupts on the context's cancellation until the rows close and
  DuckDB's error at their end. Upstream materializes every result before it
  returns the first row, as Igloo's clients' results were: a sort of
  hundreds of millions of rows without `LIMIT` held every row in memory,
  past the server's, before the client saw one. `ErrFalseInterrupt` is what
  such a query fails with when DuckDB reports it interrupted while its
  context lasts: the thread reading the rows saw the mark another thread's
  failure left, and reported that instead of the failure.
  `Stmt.QueryArrowStreamContext` in `igloo_arrow.go` reads such a result as
  Arrow record batches.
- `igloo_pending.go` (new), called from `Stmt.executeBound` (`statement.go`)
  and `Stmt.executeBoundStreaming` (`igloo_stream.go`): `runPending`, which
  carries out a statement's tasks with `duckdb_pending_execute_task` and
  sleeps while DuckDB has none for its thread, before
  `duckdb_execute_pending`, which then only fetches the result. On its own
  that call asks for a task again and again without a pause, a core for
  every statement waiting on DuckDB's workers.
- `igloo_chunk.go` (new): `DataChunk.ChunkColumn`, for a table function's
  `FillChunk` to write a column at a time: the vector's memory, for values
  of a fixed size copied in bulk, NULLs set in the validity mask, strings of
  up to twelve bytes written into the vector, and longer ones gathered and
  assigned in one cgo call per chunk (`Strings`), through a C loop over
  `duckdb_vector_assign_string_element_len`, declared there with its
  vector as an opaque pointer. `SetChunkValue` checks the column and
  converts each value; Igloo's table function of a merge of sorted rows
  spent most of its time there.

Drop this copy and the `replace` directive once a duckdb-go release can
describe and run a prepared statement as Arrow and DuckDB-Iceberg's set of
tables is safe to bind against, list and change at once.
