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
- `igloo_notice.go` (new): the `Noticer` interface, which a statement's
  `DataWriter` implements, sends the client a warning as a NoticeResponse,
  as PostgreSQL does, for statements that succeed but leave something open
  (a connection to another database that does not verify its server).

Drop this copy and the `replace` directive once a psql-wire release includes
equivalent fixes.
