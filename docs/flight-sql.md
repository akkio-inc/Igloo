# Arrow Flight SQL

Igloo serves [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
on port 31337, next to the PostgreSQL protocol, for clients that want
query results as Arrow: pandas, polars and Spark through
[ADBC](https://arrow.apache.org/adbc/), pyarrow, and BI tools through the
Flight SQL JDBC driver. Results go out as DuckDB's own Arrow record batches,
column by column, with no conversion to text and back, so pulling millions
of rows into a DataFrame is several times faster than over the PostgreSQL
protocol (see [performance](#performance)).

Flight SQL is a second way in, not a second set of rules: users sign in with
the same names and passwords, and every statement goes through the same
authorization, statement timeout, cancellation, metrics and query history as
one from `psql`.

## Connecting

The single-machine stack publishes Flight SQL at `127.0.0.1:31337`
(`IGLOO_FLIGHT_PORT`); sign in as any Igloo user. Without TLS, use the URI
`grpc://localhost:31337`; with TLS (`IGLOO_TLS_CERT_FILE` and
`IGLOO_TLS_KEY_FILE`), Flight SQL serves TLS only, at
`grpc+tls://<host>:31337`. On Kubernetes, `kubectl port-forward
service/igloo 31337:31337` and connect to localhost.

### Python: ADBC, pandas and polars

```sh
pip install adbc-driver-flightsql pyarrow pandas polars
```

```python
import adbc_driver_flightsql.dbapi as flightsql
import polars as pl

conn = flightsql.connect("grpc://localhost:31337",
                         db_kwargs={"username": "ana", "password": "a-long-password"})
with conn.cursor() as cur:
    cur.execute("SELECT * FROM lineitem WHERE l_shipdate >= ?", parameters=("1998-01-01",))
    table = cur.fetch_arrow_table()      # pyarrow.Table
    cur.execute("SELECT l_returnflag, sum(l_quantity) AS qty FROM lineitem GROUP BY 1")
    df = cur.fetch_df()                  # pandas.DataFrame

frame = pl.read_database("SELECT * FROM orders", conn)   # polars.DataFrame
conn.close()
```

Parameters are `?` or `$1`. ADBC warns `Cannot disable autocommit`: Igloo
does not implement Flight SQL's transaction calls, so each statement commits
on its own unless you run `BEGIN` and `COMMIT` as statements. For TLS with a
certificate your machine does not trust, add
`"adbc.flight.sql.client_option.tls_root_certs": open("ca.pem").read()` to
`db_kwargs`.

### pyarrow.flight

pyarrow speaks Flight but not Flight SQL's commands, so ADBC is the easier
client. The handshake works as for any Flight server:

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:31337")
options = flight.FlightCallOptions(headers=[client.authenticate_basic_token("ana", "a-long-password")])
```

A `CommandStatementQuery` descriptor then gets a `FlightInfo` with
`get_flight_info`, and `do_get` on its ticket returns the result;
`scripts/clients/flight_test.py` shows how to build the descriptor.

### JDBC and BI tools

Use the Apache Arrow
[Flight SQL JDBC driver](https://arrow.apache.org/docs/java/flight_sql_jdbc_driver.html)
(`org.apache.arrow:flight-sql-jdbc-driver`, tested with 19.0.0):

```
jdbc:arrow-flight-sql://localhost:31337/?useEncryption=false
jdbc:arrow-flight-sql://igloo.example.com:31337/?useEncryption=true
```

with the user and password as connection properties. On Java 17 and later,
start the JVM with `--add-opens=java.base/java.nio=ALL-UNNAMED`, which
Arrow's memory allocator needs. Tools that take a JDBC driver (DBeaver,
DataGrip, Tableau's "Other Databases (JDBC)") browse schemas through it:
`getTables`, `getColumns` and `getSchemas` answer from the information
schema, and key listings are empty, since Iceberg tables have no enforced
keys. Tools that offer a PostgreSQL connector can keep using port 5432.

### Access tokens and service accounts

An Igloo [access token](sso.md#access-tokens) (`CREATE TOKEN`) works in
place of a password: send it as the password in the handshake, with the
user name, or skip the handshake and send it as the bearer token of every
call (`Authorization: Bearer igloo_...`). That is how service accounts,
which have no password, and people who sign in with single sign-on connect:

```python
import adbc_driver_flightsql.dbapi as flightsql

conn = flightsql.connect("grpc+tls://igloo.example.com:31337",
                         db_kwargs={"username": "dbt_prod", "password": token})
```

A session opened with a token acts on the token's authority, as a SQL
session that signed in with one does: it cannot create access tokens or
change its user's password, and it ends when the token is revoked
(`DROP TOKEN`) or expires.

## Sessions and statements

A client's handshake opens a session, and the bearer token it returns
identifies the session on later calls. A session behaves like a PostgreSQL
connection: settings (`SET search_path`, `SET statement_timeout`),
temporary tables and transactions (`BEGIN`, `COMMIT`, `ROLLBACK` as
statements) last until it ends, and it runs one statement at a time. A
second statement waits for the first; if the first is still running after
30 seconds, as when a client sends a new query before reading the previous
result, the second fails with `FAILED_PRECONDITION`. Read or cancel a
result before starting the next, or use another connection.

A session lives as long as the connection it was opened on: closing the
client, `CloseSession`, a dropped connection, or Igloo restarting (or
scaling to zero) ends it, and its token stops working. Clients then sign in
again. Each session counts against `IGLOO_MAX_CONNECTIONS` and appears on
the console's compute page as `flight sql <address>`.

| Flight SQL | Supported |
|---|---|
| `CommandStatementQuery`, `CommandStatementUpdate` | Yes. A query is prepared, and so authorized, when the client asks for its `FlightInfo`, and runs when it fetches the ticket with `DoGet`. A statement that returns no rows (DDL, DML, `SET`) runs when the client executes it: its `FlightInfo` has no endpoints, and the rows it changed as `total_records` (ADBC's `rowcount`). |
| Prepared statements with parameters | Yes, with the Arrow types DuckDB infers for parameters and results. A query bound with several rows of parameters runs once per row. |
| `GetCatalogs`, `GetDbSchemas`, `GetTables` (with schemas), `GetTableTypes`, `GetSqlInfo` | Yes. Table types are `TABLE`, `VIEW` and `LOCAL TEMPORARY`; search patterns use `%` and `_`, escaped with `\`. |
| `GetPrimaryKeys`, `GetImportedKeys`, `GetExportedKeys`, `GetCrossReference` | Empty |
| Cancellation | Canceling the `DoGet` call cancels the statement, as does `CancelFlightInfo` |
| `CloseSession` | Ends the session |
| Transactions API, bulk ingestion (`CommandStatementIngest`), Substrait, `GetXdbcTypeInfo`, session options | No |

Results arrive in DuckDB's record batches of 2,048 rows, as DuckDB's Arrow
types: `VARCHAR` is `utf8`, `DECIMAL(p,s)` `decimal128(p,s)`, `HUGEINT`
`decimal128(38,0)`, `TIMESTAMPTZ` a timestamp in the session's time zone,
`INTERVAL` `month_day_nano`, `UUID` `utf8`, lists, structs and maps their
nested Arrow types, and `ENUM` dictionary-encoded strings. Results Igloo
produces itself rather than DuckDB, such as `SHOW GRANTS` or `FETCH` from a
cursor, are strings, as the PostgreSQL protocol sends them.

## Security

- **Sign-in.** The handshake carries the user name and password with HTTP
  basic authentication. Igloo checks the password against the user's stored
  SCRAM-SHA-256 verifier exactly as for a SQL login, including whether the
  user may use a password at all (see [single sign-on](sso.md)): the same 500 ms delay
  after a failure, unknown users and wrong passwords failing alike, the same
  `login` and `login failed` log lines (with `client="flight sql"`), the
  `igloo_logins_total` metric, and an entry in `igloo.login_history` with
  the application `flight sql`.
- **Tokens.** The bearer token a handshake returns is 32 random bytes,
  valid only on the connection it was issued on and only while that
  connection lasts, so a token copied elsewhere is useless. Igloo keeps a
  hash of it, not the token. An Igloo access token sent instead is checked
  like one sent to the SQL port, opens a session on each connection it is
  first used on, and is recorded in the login history with `auth_method`
  `token` and its name.
- **Limits.** Failed password sign-ins count towards the same limits as SQL
  logins: 10 per user and 50 per address in 15 minutes. Access tokens still
  sign in while a user's passwords are blocked.
- **Transport.** Basic authentication sends the password itself, not a
  SCRAM proof, so Flight SQL follows the [web console](console.md)'s rule:
  with a certificate configured it serves TLS only, with the SQL port's
  certificate; without one it serves only clients that connect to it as
  `localhost` (or `127.0.0.1`, `::1`), that is, on the same machine or
  through an SSH tunnel or `kubectl port-forward`. Other clients get
  `PERMISSION_DENIED` before their password is checked. The check uses the
  address the client dialed (the request's `:authority`), as the console
  uses the `Host` header, because behind Docker's or Kubernetes' port
  forwarding Igloo cannot tell a local client by its address. Configure
  TLS before publishing the port beyond loopback.
- **Authorization.** Every statement and metadata call runs as the user, in
  an embedded session of the SQL server, through the same checks as a
  statement from `psql`: built-in roles, grants, the sandbox, and
  [masking and row access policies](security-policies.md), which rewrite it
  before it runs and are applied again when a prepared statement runs after
  they change. Nothing about Flight SQL bypasses them. A refused statement
  fails with `PERMISSION_DENIED` and its SQLSTATE in the message, as below.
- **History and metrics.** Statements appear in `igloo.query_history` with
  `client_address` `flight sql <address>` and the client's user agent as
  `application_name`, and count in `igloo_queries_total` like any other.

Errors keep their SQLSTATE at the end of the message, such as
`permission denied for rita: INSERT on table public.e2e needs the writer
role or an INSERT grant (SQLSTATE 42501)`, and map to gRPC status codes:

| SQLSTATE | gRPC status |
|---|---|
| `42501` (insufficient privilege) | `PERMISSION_DENIED` |
| `28xxx` (authorization) | `UNAUTHENTICATED` |
| `57014` (canceled) | `CANCELLED`, or `DEADLINE_EXCEEDED` for the statement timeout |
| `42P01`, `42703`, `42883`, `42704` (undefined object) | `NOT_FOUND` |
| `42P07`, `42710`, `23505` (duplicate) | `ALREADY_EXISTS` |
| other `42xxx`, `22xxx` (syntax, data) | `INVALID_ARGUMENT` |
| `0Axxx` (not supported) | `UNIMPLEMENTED` |
| `40001` (serialization failure) | `ABORTED`: retry |
| `25xxx`, `23xxx` (transaction state, constraints) | `FAILED_PRECONDITION` |
| `53300` (too many connections) | `RESOURCE_EXHAUSTED` |
| anything else | `INTERNAL` |

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_FLIGHT_ADDR` | `:31337` | Where Flight SQL listens. Empty turns it off. |

Flight SQL uses the server's TLS settings and `IGLOO_MAX_CONNECTIONS`; there
is nothing else to configure. In the single-machine stack,
`IGLOO_FLIGHT_PORT` (31337) and `IGLOO_FLIGHT_BIND` (127.0.0.1) in
`deploy/single-vm/.env` choose where the host publishes it. The Helm chart
has `flightSQL.enabled` and `flightSQL.port`, and its network policy treats
the port like the SQL port. The [scale-to-zero](scale-to-zero.md) gateway
forwards it with `IGLOO_GATEWAY_FLIGHT_BACKEND`.

## Performance

`bench/extract.py` pulls TPC-H lineitem rows into Python both ways, in turn,
and keeps each way's best of three rounds. For 3,000,000 rows of all 16
columns (TPC-H scale factor 1), with the single-machine stack and the client
on one Apple-silicon Mac (18 cores, 16 GB for Docker) that other work was
also using:

| Client | Seconds | Rows per second | Speedup |
|---|---:|---:|---:|
| psycopg 3.3, Python rows | 56.2 | 53,000 | |
| psycopg 3.3, pandas DataFrame | 65.2 | 46,000 | |
| ADBC 1.12, Arrow table | 1.9 | 1,557,000 | 29x psycopg's rows |
| ADBC 1.12, pandas DataFrame | 4.5 | 663,000 | 14x psycopg's DataFrame |
| ADBC 1.12, polars DataFrame | 1.4 | 2,138,000 | 46x psycopg's DataFrame |

Over the PostgreSQL protocol, Igloo renders every value as text and the
client parses it back, row by row. Over Flight SQL, DuckDB's columnar result
goes out as Arrow buffers that pyarrow uses as they arrive; polars takes the
same Arrow data without copying, and its lead over the Arrow table here is
within the run-to-run noise of a shared machine (rounds varied up to 2x).
Measure on your own hardware with:

```sh
make up && make load-tpch
IGLOO_PASSWORD=... python bench/extract.py --rows 3000000
```

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `PERMISSION_DENIED: this connection is not encrypted` | No TLS, and the client did not connect as localhost. Configure TLS, or use an SSH tunnel (`ssh -L 31337:127.0.0.1:31337 host`) or `kubectl port-forward`. |
| `UNAUTHENTICATED: this bearer token has no session` | The connection the session was opened on closed, or Igloo restarted. Connect again. |
| `FAILED_PRECONDITION: this session is still running another statement` | The client started a statement before reading the last one's result, on the same connection. |
| `Cannot disable autocommit` (ADBC warning) | Expected; see [Python](#python-adbc-pandas-and-polars). |
| `unsupported data type: INVALID` | DuckDB cannot infer a parameter's type, as in `SELECT ? AS x`; cast it (`?::VARCHAR`). |
