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

Drop this copy and the `replace` directive once a psql-wire release includes
equivalent fixes.
