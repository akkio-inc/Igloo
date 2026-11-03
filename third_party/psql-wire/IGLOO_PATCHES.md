# Igloo's copy of psql-wire

This is [psql-wire](https://github.com/jeroenrinzema/psql-wire) v0.20.0
(Apache License 2.0, see LICENSE), used through a `replace` directive in
Igloo's `go.mod`. Tests, examples and CI files were removed; the code is
otherwise unchanged except for these patches:

- `pkg/buffer/reader.go`, `GetBytes`: reject negative lengths other than -1
  (NULL). A client could send one in a Bind message's parameter lengths and
  make the server panic.
- `command.go`, `handleBind`: a Bind naming a prepared statement that does
  not exist gets error 26000 and the connection discards messages until
  Sync, as in PostgreSQL. It used to end the connection, for instance when a
  client reused statements after `DEALLOCATE ALL`.
- `igloo_embedded.go` (new): `PreparedStatement.Columns` and
  `PreparedStatement.Run`, so Igloo's in-process sessions (the web console)
  can run what its parse function prepared without a client connection.
- `igloo_tls.go` (new) and `wire.go`, `serve`: `IsTLS` reports whether a
  connection negotiated TLS, so authentication can ask for a clear-text
  password (an identity provider's access token) only inside TLS.
- `wire.go`, `serve`: an error from the `Session` handler, which opens the
  session once the client has signed in, reaches the client as a FATAL
  error, as an authentication failure does. The connection used to close
  without one.
- `igloo_notice.go` (new): the `Noticer` interface, which a statement's
  `DataWriter` implements, sends the client a warning as a NoticeResponse,
  as PostgreSQL does, for statements that succeed but leave something open
  (a connection to another database that does not verify its server), or
  do nothing (`BEGIN` inside a transaction block).
- `row.go`, `Column.Define`: a RowDescription carries the column's
  `TypeModifier` when it is set, instead of always -1, so clients learn a
  numeric's precision and scale.
- `igloo_wrap.go` (new): `PreparedStatement.WrapFn` wraps a prepared
  statement's function in place, so Igloo can have every run of a statement
  it translated from Snowflake's dialect send the translation's notices.
- `igloo_writer.go` (new): `DefineColumns`, so a statement of a simple query
  can describe its rows when it runs rather than when the query is parsed
  (Igloo prepares each statement of a multi-statement query only once the
  one before has run), and `CopyOut`, `CopyData` and `CopyDone`, the
  messages of `COPY ... TO STDOUT`.
- `pkg/buffer/reader.go`, `NewReader`, and `options.go`,
  `MessageBufferSize`: a connection's buffered reader is `ReadBufferSize`,
  8 KiB, instead of as large as the largest message allowed (16 MiB by
  default), which every connection allocated before it sent anything.
  Messages are still read into a buffer that grows to fit each one, up to
  that size.
- `igloo_conn.go` (new) and `wire.go`, `serve`: `AcceptedConn` returns the
  connection the listener accepted for a client, so that Igloo's listener,
  which gives each connection a deadline to sign in, can lift it once the
  client has.
- `igloo_writer.go`: `Drainer`, whose `Drains` says whether the client
  reads every row of a statement in one Execute, so that Igloo can have
  DuckDB stream the statement's result: a portal with a row limit suspends
  the statement, and the client may run others on the session before it
  reads on, which would end a result DuckDB streams.
- `igloo_writer.go`: `CopyInColumns`, which starts `COPY ... FROM STDIN` with
  a CopyInResponse for as many columns as the table it fills, as a
  statement that returns no rows describes none.

- `igloo_wrap.go` and `command.go`, `consumeSingleCommand`: `CommandWrapper`
  runs each command a client sends through a function of Igloo's, which runs
  it on a thread of its own, so that a C++ exception escaping DuckDB there
  ends the session rather than the process.

Drop this copy and the `replace` directive once a psql-wire release includes
equivalent fixes.
