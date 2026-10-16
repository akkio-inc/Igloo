#!/usr/bin/env bash
# End-to-end test of the single-machine stack: builds the image, starts an
# isolated copy of deploy/single-vm (its own project name, image, ports and
# data), and checks Igloo through real psql clients and the web console.
# Removes everything afterwards unless KEEP_E2E=1. E2E_PROJECT and the
# E2E_*_PORT variables let several runs share a Docker host.
#
#   scripts/e2e.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
project="${E2E_PROJECT:-igloo-e2e}"
sql_port="${E2E_SQL_PORT:-15433}"
http_port="${E2E_HTTP_PORT:-18432}"
console_port="${E2E_CONSOLE_PORT:-18443}"
work="$(mktemp -d)"
env_file="$work/e2e.env"

compose() {
  docker compose -p "$project" --project-directory "$root/deploy/single-vm" --env-file "$env_file" "$@"
}

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "--- igloo logs"
    compose logs --no-color --tail 80 igloo || true
  fi
  if [ "${KEEP_E2E:-}" != 1 ]; then
    compose --profile local-storage --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$work"
  else
    echo "kept the stack: docker compose -p $project --env-file $env_file ..."
  fi
  exit "$status"
}
trap cleanup EXIT

failures=0
pass() { printf '  ok    %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; failures=$((failures + 1)); }

"$root/deploy/single-vm/init-env.sh" "$env_file" >/dev/null
sed -i.bak -e "s/^IGLOO_PORT=.*/IGLOO_PORT=$sql_port/" -e "s/^IGLOO_HTTP_PORT=.*/IGLOO_HTTP_PORT=$http_port/" \
  -e "s/^IGLOO_CONSOLE_PORT=.*/IGLOO_CONSOLE_PORT=$console_port/" -e "s/^IGLOO_IMAGE=.*/IGLOO_IMAGE=$project:dev/" "$env_file"
# Maintain tables every few seconds, so the maintenance checks need not wait
# an hour.
echo "IGLOO_MAINTENANCE_INTERVAL=5s" >>"$env_file"
admin_password="$(grep '^IGLOO_ADMIN_PASSWORD=' "$env_file" | cut -d= -f2)"

echo "starting the stack"
compose up -d --build --wait --wait-timeout 300 >/dev/null

network="${project}_default"

# psql_as USER PASSWORD SQL... runs SQL through a fresh psql container and
# prints tuples-only output (errors on stderr, merged).
psql_as() {
  local user=$1 password=$2
  shift 2
  docker run --rm -i --network "$network" -e PGPASSWORD="$password" postgres:17 \
    psql -h igloo -U "$user" -d igloo -X -At -v ON_ERROR_STOP=1 -v VERBOSITY=verbose "$@" 2>&1
}
psql_admin() { psql_as igloo "$admin_password" "$@"; }

# expect NAME WANT USER PASSWORD SQL: output must equal WANT.
expect() {
  local name=$1 want=$2 got
  got="$(psql_as "$3" "$4" -c "$5")" || true
  if [ "$got" = "$want" ]; then pass "$name"; else fail "$name: got [$got], want [$want]"; fi
}

# expect_error NAME SQLSTATE USER PASSWORD SQL: must fail with SQLSTATE.
expect_error() {
  local name=$1 code=$2 got
  got="$(psql_as "$3" "$4" -c "$5")" && { fail "$name: succeeded, want $code"; return; }
  if grep -q "ERROR:  $code" <<<"$got"; then pass "$name"; else fail "$name: want $code, got [$got]"; fi
}

# expect_soon NAME WANT USER PASSWORD SQL: like expect, but waits up to a
# minute for the output, for history that is written in the background.
expect_soon() {
  local name=$1 want=$2 got="" deadline=$((SECONDS + 60))
  while [ "$SECONDS" -lt "$deadline" ]; do
    got="$(psql_as "$3" "$4" -c "$5")" || true
    [ "$got" = "$want" ] && break
    sleep 2
  done
  if [ "$got" = "$want" ]; then pass "$name"; else fail "$name: got [$got], want [$want]"; fi
}

echo "tables and Iceberg writes"
expect "create, insert, CTAS, update, delete" "2|beta|40.50" igloo "$admin_password" "
  CREATE TABLE e2e (id INTEGER, name VARCHAR, amount DECIMAL(10,2));
  INSERT INTO e2e VALUES (1, 'alpha', 10.50), (2, 'beta', 20.25);
  CREATE TABLE e2e_copy AS SELECT * FROM e2e;
  UPDATE e2e_copy SET amount = amount * 2 WHERE id = 2;
  DELETE FROM e2e_copy WHERE id = 1;
  SELECT * FROM e2e_copy;"

echo "users and roles"
expect "create users" "CREATE ROLE
CREATE ROLE" igloo "$admin_password" "
  CREATE USER rita WITH PASSWORD 'rita-password-1' ROLE reader;
  CREATE USER walt WITH PASSWORD 'walt-password-1' ROLE writer;"
expect "reader can read" "2" rita rita-password-1 "SELECT count(*) FROM e2e"
expect_error "reader cannot insert" 42501 rita rita-password-1 "INSERT INTO e2e VALUES (3, 'x', 1)"
expect "writer can insert" "INSERT 0 1" walt walt-password-1 "INSERT INTO e2e VALUES (3, 'gamma', 1)"
expect_error "writer cannot create users" 42501 walt walt-password-1 "CREATE USER eve PASSWORD 'eve-password-1'"
if got="$(psql_as rita wrong-password -c "SELECT 1")"; then
  fail "wrong password accepted"
elif grep -q "password authentication failed" <<<"$got"; then
  pass "wrong password is rejected"
else
  fail "wrong password: $got"
fi

echo "web console"
console="http://127.0.0.1:$console_port"
jar="$work/console.cookies"
# console_post PATH JSON [CSRF] posts JSON with the console session's cookie.
console_post() {
  curl -sS -b "$jar" -c "$jar" -H 'Content-Type: application/json' -H "X-CSRF-Token: ${3:-}" -d "$2" "$console$1"
}
code="$(curl -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' \
  -d '{"user":"walt","password":"not-the-password"}' "$console/api/login")"
if [ "$code" = 401 ]; then pass "console refuses a wrong password"; else fail "console wrong password: $code"; fi
code="$(curl -s -o /dev/null -w '%{http_code}' -H 'Host: igloo.example.com' -H 'Content-Type: application/json' \
  -d '{"user":"walt","password":"walt-password-1"}' "$console/api/login")"
if [ "$code" = 403 ]; then pass "console refuses unencrypted sign-ins from afar"; else fail "console sign-in without TLS: $code"; fi
login="$(console_post /api/login '{"user":"walt","password":"walt-password-1"}')"
csrf="$(sed -n 's/.*"csrf":"\([^"]*\)".*/\1/p' <<<"$login")"
if [ -n "$csrf" ]; then pass "console sign-in"; else fail "console sign-in: $login"; fi
got="$(console_post /api/query '{"sql":"SELECT count(*) AS n FROM e2e"}' "$csrf")"
if grep -q '"rows":\[\["3"\]\]' <<<"$got"; then pass "console runs a query as the user"; else fail "console query: $got"; fi
got="$(console_post /api/query "{\"sql\":\"SELECT count(*) FROM duckdb_columns() WHERE table_name = 'e2e'\"}" "$csrf")"
if grep -q '"rows":\[\["3"\]\]' <<<"$got"; then pass "console reads Iceberg columns"; else fail "console columns: $got"; fi
got="$(console_post /api/query '{"sql":"DROP USER rita"}' "$csrf")"
if grep -q '"code":"42501"' <<<"$got"; then pass "console statements are authorized as SQL ones"; else fail "console authorization: $got"; fi
code="$(curl -s -o /dev/null -w '%{http_code}' -b "$jar" "$console/api/admin/users")"
if [ "$code" = 403 ]; then pass "console admin pages refuse a writer"; else fail "console admin page as a writer: $code"; fi
got="$(console_post /api/download '{"sql":"SELECT id, name FROM e2e ORDER BY id"}' "$csrf")"
if [ "$got" = "$(printf 'id,name\n1,alpha\n2,beta\n3,gamma')" ]; then pass "console CSV download"; else fail "console CSV: $got"; fi
console_post /api/logout '{}' "$csrf" >/dev/null
code="$(curl -s -o /dev/null -w '%{http_code}' -b "$jar" "$console/api/session")"
if [ "$code" = 401 ]; then pass "console sign-out"; else fail "console session after sign-out: $code"; fi

echo "sandbox"
expect_error "no local files" 42501 igloo "$admin_password" "SELECT * FROM read_text('/etc/passwd')"
expect_error "no other buckets" 42501 igloo "$admin_password" "SELECT * FROM read_parquet('s3://elsewhere/x.parquet')"
expect_error "no arbitrary URLs" 42501 igloo "$admin_password" "SELECT * FROM read_csv('http://lakekeeper:8181/management/v1/warehouse')"
expect_error "cannot reopen the sandbox" 22P02 igloo "$admin_password" "SET enable_external_access = true"

echo "client compatibility"
got="$(psql_admin -c '\dt')"
if grep -q "public|e2e|table" <<<"$got"; then pass '\dt lists tables'; else fail "\\dt: $got"; fi
got="$(psql_admin -c '\d e2e_copy')"
if grep -q "amount|numeric(10,2)" <<<"$got"; then pass '\d shows columns'; else fail "\\d: $got"; fi

echo "TPC-H"
got="$(compose run --rm -T psql -X -v ON_ERROR_STOP=1 -v sf=0.1 -f /sql/load-tpch.sql 2>&1)"
if grep -q "^ *600572$" <<<"$got"; then pass "load TPC-H SF 0.1"; else fail "load TPC-H: $got"; fi
for q in 1 6 18; do
  if psql_admin -c "PRAGMA tpch($q)" >/dev/null; then pass "TPC-H Q$q"; else fail "TPC-H Q$q"; fi
done

echo "drivers"
pgjdbc_version=42.7.14
if docker run --rm --network "$network" -e PGHOST=igloo -e PGUSER=igloo -e PGPASSWORD="$admin_password" \
  -v "$root/scripts/clients:/clients:ro" eclipse-temurin:21-jdk sh -c "
    cd /tmp && curl -fsSLO https://repo1.maven.org/maven2/org/postgresql/postgresql/$pgjdbc_version/postgresql-$pgjdbc_version.jar &&
    java -cp postgresql-$pgjdbc_version.jar /clients/JdbcTest.java" >"$work/jdbc.out" 2>&1; then
  pass "JDBC (pgjdbc $pgjdbc_version)"
else
  fail "JDBC: $(tail -5 "$work/jdbc.out")"
fi
if docker run --rm --network "$network" -e PGHOST=igloo -e PGUSER=igloo -e PGPASSWORD="$admin_password" -e PGDATABASE=igloo \
  -v "$root/scripts/clients:/clients:ro" python:3.12-slim sh -c "
    pip install -q 'psycopg[binary]==3.3.6' >/dev/null 2>&1 && python /clients/psycopg_test.py" >"$work/psycopg.out" 2>&1; then
  pass "psycopg 3"
else
  fail "psycopg: $(tail -5 "$work/psycopg.out")"
fi

echo "concurrent writes"
psql_admin -c "CREATE TABLE contended (n INTEGER)" >/dev/null
pids=()
for i in $(seq 1 10); do
  psql_admin -c "INSERT INTO contended VALUES ($i)" >"$work/insert-$i.out" &
  pids+=($!)
done
ok=0
for pid in "${pids[@]}"; do wait "$pid" && ok=$((ok + 1)); done
expect "10 concurrent inserts all commit" "10" igloo "$admin_password" "SELECT count(*) FROM contended"

echo "replace, views and comments"
expect "writer replaces a table with a new schema" "CREATE TABLE" walt walt-password-1 \
  "CREATE OR REPLACE TABLE e2e_copy AS SELECT range AS n, chr(97 + range::INT) AS letter FROM range(3)"
expect "replaced table has the new rows" "3|b" walt walt-password-1 "SELECT count(*), min(letter) FILTER (WHERE n = 1) FROM e2e_copy"
# Statement by statement, as one query would run them all while preparing.
got="$(printf 'BEGIN;\nCREATE OR REPLACE TABLE e2e_copy AS SELECT 1 AS x;\nROLLBACK;\n' | psql_admin)" || true
if grep -q "ERROR:  25001" <<<"$got"; then pass "replace inside a transaction is refused"; else fail "replace inside a transaction: got [$got]"; fi
expect "writer creates a view" "CREATE VIEW" walt walt-password-1 \
  "CREATE VIEW e2e_big AS SELECT id, name FROM e2e WHERE amount > 15"
expect_error "reader cannot create views" 42501 rita rita-password-1 "CREATE VIEW nope AS SELECT 1"
expect_error "a view name must be free" 42P07 walt walt-password-1 "CREATE VIEW e2e_big AS SELECT 1"
expect "reader queries a view by every name" "1|1|1" rita rita-password-1 \
  "SELECT (SELECT count(*) FROM e2e_big), (SELECT count(*) FROM public.e2e_big), (SELECT count(*) FROM lake.public.e2e_big)"
expect "replace a view and read it from another view" "CREATE VIEW" walt walt-password-1 \
  "CREATE OR REPLACE VIEW e2e_big (big_id) AS SELECT id FROM lake.public.e2e WHERE amount > 0"
expect "views read views" "3" rita rita-password-1 \
  "SELECT count(*) FROM (SELECT big_id FROM lake.public.e2e_big)"
expect "replace a view with another table's columns" "CREATE VIEW" walt walt-password-1 \
  "CREATE OR REPLACE VIEW e2e_big AS SELECT n, letter FROM lake.public.e2e_copy"
expect "the replaced view reads the other table" "3|c" rita rita-password-1 "SELECT count(*), max(letter) FROM e2e_big"
expect "and back to columns and SQL it had before" "CREATE VIEW" walt walt-password-1 \
  "CREATE OR REPLACE VIEW e2e_big (big_id) AS SELECT id FROM lake.public.e2e WHERE amount > 0"
expect "the view reads the first table again" "3" rita rita-password-1 "SELECT count(*) FROM (SELECT big_id FROM e2e_big)"
expect_error "ALTER VIEW is refused" 0A000 walt walt-password-1 "ALTER VIEW e2e_big RENAME TO e2e_huge"
expect_error "writers cannot change the local view copies" 42501 walt walt-password-1 "DROP VIEW memory.public.e2e_big"
expect "comment on a table" "COMMENT" walt walt-password-1 "COMMENT ON TABLE e2e IS 'Test rows | :tag:e2e'"
expect "comment on a column" "COMMENT" walt walt-password-1 "COMMENT ON COLUMN e2e.amount IS 'In dollars'"
expect_error "reader cannot comment" 42501 rita rita-password-1 "COMMENT ON TABLE e2e IS 'x'"
expect "comments in obj_description and col_description" "Test rows | :tag:e2e|In dollars" rita rita-password-1 \
  "SELECT obj_description(c.oid, 'pg_class'), col_description(c.oid, a.attnum)
   FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid WHERE c.relname = 'e2e' AND a.attname = 'amount'"

echo "grants"
expect "a user with no role of their own, and a custom role" "CREATE ROLE
CREATE ROLE
GRANT
GRANT" igloo "$admin_password" "
  CREATE USER nora WITH PASSWORD 'nora-password-1' ROLE none;
  CREATE ROLE analysts;
  GRANT analysts TO nora;
  GRANT SELECT ON TABLE e2e TO analysts;"
expect "a granted table" "3" nora nora-password-1 "SELECT count(*) FROM e2e"
expect "the system catalogs need no grant" "t" nora nora-password-1 "SELECT count(*) > 0 FROM information_schema.tables"
expect_error "a table not granted" 42501 nora nora-password-1 "SELECT count(*) FROM contended"
expect_error "a join with a table not granted" 42501 nora nora-password-1 \
  "SELECT count(*) FROM e2e, (SELECT max(n) FROM lake.public.contended)"
expect "a view reads with the reader's grants" "3" nora nora-password-1 "SELECT count(*) FROM e2e_big"
psql_admin -c "CREATE VIEW e2e_contended AS SELECT * FROM lake.public.contended" >/dev/null
expect_error "a view of a table not granted" 42501 nora nora-password-1 "SELECT * FROM e2e_contended"
data_file="$(psql_admin -c "SELECT file_path FROM iceberg_metadata(lake.public.contended) LIMIT 1")"
metadata_file="$(psql_admin -c "SELECT file FROM glob(regexp_replace('$data_file', '/data/.*', '/metadata/*.metadata.json')) ORDER BY 1 DESC LIMIT 1")"
expect_error "no reading a table's files directly" 42501 nora nora-password-1 "SELECT * FROM read_parquet('$data_file')"
expect_error "no Iceberg scans of a table's metadata" 42501 nora nora-password-1 "SELECT * FROM iceberg_scan('$metadata_file')"
expect_error "no Iceberg metadata functions" 42501 nora nora-password-1 "SELECT * FROM iceberg_snapshots(lake.public.e2e)"
expect "a direct grant to a user" "GRANT" igloo "$admin_password" "GRANT SELECT ON contended TO nora"
expect "the direct grant applies" "10" nora nora-password-1 "SELECT count(*) FROM contended"
psql_admin -c "REVOKE SELECT ON contended FROM nora" >/dev/null
expect_error "a revoked grant no longer applies" 42501 nora nora-password-1 "SELECT count(*) FROM contended"
psql_admin -c "CREATE SCHEMA team" >/dev/null
expect "grants on a schema" "GRANT" igloo "$admin_password" "GRANT SELECT, INSERT, CREATE ON SCHEMA team TO analysts"
psql_admin -c "CREATE TABLE team.later AS SELECT 1 AS x" >/dev/null
expect "a schema grant covers tables created later" "1" nora nora-password-1 "SELECT count(*) FROM team.later"
expect "CREATE on a schema creates tables there" "CREATE TABLE" nora nora-password-1 "CREATE TABLE team.mine AS SELECT * FROM e2e"
expect "and replaces them" "CREATE TABLE" nora nora-password-1 "CREATE OR REPLACE TABLE team.mine AS SELECT id FROM e2e"
expect "INSERT through a schema grant" "INSERT 0 1" nora nora-password-1 "INSERT INTO team.mine VALUES (4)"
expect_error "but not in other schemas" 42501 nora nora-password-1 "CREATE TABLE public.mine (i INTEGER)"
expect_error "nor from tables not granted" 42501 nora nora-password-1 "CREATE TABLE team.copy AS SELECT * FROM contended"
expect "describing a granted table" "id|name|amount" nora nora-password-1 \
  "SELECT string_agg(name, '|' ORDER BY cid) FROM pragma_table_info('e2e')"
got="$(psql_as nora nora-password-1 -c "CALL pragma_table_info('e2e')")" || true
if [ "$(wc -l <<<"$got" | tr -d ' ')" = 3 ] && grep -q '^0|id|INTEGER|' <<<"$got"; then
  pass "CALL pragma_table_info as a user with no role"
else
  fail "CALL pragma_table_info as a user with no role: got [$got]"
fi
expect_error "CALL of other table functions" 42501 nora nora-password-1 "CALL range(3)"
expect_error "a PIVOT that reads its columns from the data" 42501 nora nora-password-1 \
  "PIVOT (SELECT name, amount FROM e2e) ON name USING sum(amount)"
expect "a PIVOT that lists its columns" "10.50|20.25" nora nora-password-1 \
  "SELECT alpha, beta FROM (PIVOT (SELECT name, amount FROM e2e) ON name IN ('alpha', 'beta') USING sum(amount))"
got="$(psql_as nora nora-password-1 -c 'SHOW GRANTS ON TABLE e2e')"
if grep -q "^analysts|role|SELECT|table|public.e2e|igloo|" <<<"$got"; then pass "SHOW GRANTS"; else fail "SHOW GRANTS: $got"; fi

echo "SQL from strings, PIVOT and the read cache"
expect_error "query() is refused" 42501 rita rita-password-1 "SELECT * FROM query('SELECT * FROM duckdb_secrets()')"
expect_error "even for admins" 42501 igloo "$admin_password" "SELECT * FROM query('SELECT 1')"
expect_error "so is serialized SQL" 42501 rita rita-password-1 \
  "SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT * FROM duckdb_secrets()'))"
expect_error "query_table is admin-only" 42501 rita rita-password-1 "SELECT * FROM query_table('e2e')"
expect "readers PIVOT on values from the data" "10.50|20.25|1.00" rita rita-password-1 \
  "SELECT alpha, beta, gamma FROM (PIVOT (SELECT name, amount FROM e2e) ON name USING sum(amount))"
expect_error "but not in CREATE TABLE AS, which DuckDB would run while preparing" 42501 walt walt-password-1 \
  "CREATE TABLE e2e_pivot AS PIVOT (SELECT name, amount FROM e2e) ON name USING sum(amount)"
expect_error "a PIVOT refused there created nothing" 42P01 walt walt-password-1 "SELECT * FROM e2e_pivot"
expect "writers create tables from a PIVOT that lists its columns" "CREATE TABLE" walt walt-password-1 \
  "CREATE TABLE e2e_pivot AS PIVOT (SELECT name, amount FROM e2e) ON name IN ('alpha', 'beta') USING sum(amount)"
expect_error "a PIVOT over admin-only functions" 42501 rita rita-password-1 \
  "PIVOT (SELECT * FROM duckdb_secrets()) ON name USING count(*)"
expect_error "readers cannot clear the read cache" 42501 rita rita-password-1 "SELECT cache_httpfs_clear_cache()"
expect_error "nor list it" 42501 rita rita-password-1 "SELECT * FROM cache_httpfs_cache_status_query()"
expect_error "no one moves the read cache" 42501 igloo "$admin_password" \
  "SET GLOBAL cache_httpfs_cache_directory = '/tmp/elsewhere'"
expect_error "or splits it across directories" 42501 igloo "$admin_password" \
  "SET GLOBAL cache_httpfs_cache_directories_config = '/tmp/a;/tmp/b'"
expect_error "or wraps other filesystems" 42501 igloo "$admin_password" \
  "SELECT cache_httpfs_wrap_cache_filesystem('LocalFileSystem')"
expect_error "or caches more than the warehouse" 42501 igloo "$admin_password" "SELECT cache_httpfs_reset_exclusion_regex()"
expect "the read cache still caches only the warehouse" "1" igloo "$admin_password" \
  "SELECT count(*) FROM cache_httpfs_list_exclusion_regex()"

echo "query history"
expect_soon "users see their own statements" "t" nora nora-password-1 \
  "SELECT count(*) > 0 AND count(*) FILTER (WHERE user_name <> 'nora') = 0 FROM igloo.query_history"
expect_soon "with the tables they read" '["lake.public.e2e"]' igloo "$admin_password" \
  "SELECT tables_read FROM igloo.query_history WHERE user_name = 'nora' AND query_text = 'SELECT count(*) FROM e2e' LIMIT 1"
expect_soon "denied statements are recorded" "t" igloo "$admin_password" \
  "SELECT count(*) > 0 FROM igloo.query_history WHERE user_name = 'nora' AND outcome = 'denied' AND sqlstate = '42501'"
expect "passwords are never recorded" "0" igloo "$admin_password" \
  "SELECT count(*) FROM igloo.query_history WHERE query_text LIKE '%password-1%'"
expect_soon "logins and failed logins are recorded" "t|t" igloo "$admin_password" \
  "SELECT bool_or(result = 'success' AND user_name = 'nora'), bool_or(result = 'failure' AND user_name = 'rita') FROM igloo.login_history"
got="$(psql_admin -c '\dv igloo.*')"
if grep -q "igloo|query_history|view" <<<"$got"; then pass '\dv lists the history views'; else fail "\\dv igloo.*: $got"; fi

echo "table maintenance"
psql_admin -c "CREATE TABLE fragmented (id INTEGER, v VARCHAR, amount DECIMAL(15,2))" >/dev/null
for i in 1 2 3 4 5 6; do psql_admin -c "INSERT INTO fragmented VALUES ($i, 'v$i', $i.25)" >/dev/null; done
psql_admin -c "DELETE FROM fragmented WHERE id = 3" >/dev/null
retention="SELECT * FROM set_iceberg_table_properties(lake.public.fragmented, {'history.expire.max-snapshot-age-ms': '0'})"
expect_error "reader cannot change table properties" 42501 rita rita-password-1 "$retention"
expect "writer sets zero retention on a table" "1" walt walt-password-1 "$retention"
# Six single-row inserts make six data files. Maintenance (every 5 seconds
# here) should leave at most two, depending on whether a pass ran between the
# inserts, and expire every snapshot but the current one.
live="SELECT (SELECT count(*) FROM iceberg_metadata(lake.public.fragmented)
    WHERE status <> 'DELETED' AND manifest_content = 'DATA') <= 2
  AND (SELECT count(*) FROM iceberg_snapshots(lake.public.fragmented)) = 1"
state=""
deadline=$((SECONDS + 240))
while [ "$SECONDS" -lt "$deadline" ]; do
  state="$(psql_admin -c "$live")" || true
  [ "$state" = "t" ] && break
  sleep 2
done
if [ "$state" = "t" ]; then pass "compaction and snapshot expiry"; else fail "compaction and snapshot expiry: not done after 4 minutes"; fi
# Compacted files must be laid out as DuckDB writes Parquet (decimals as
# integers, Snappy), or compaction makes scans slower.
files="$(psql_admin -c "SELECT string_agg(chr(39) || file_path || chr(39), ',') FROM iceberg_metadata(lake.public.fragmented)
  WHERE status <> 'DELETED' AND manifest_content = 'DATA'")"
expect "compacted files use DuckDB's Parquet layout" "INT64|SNAPPY" igloo "$admin_password" \
  "SELECT DISTINCT type, compression FROM parquet_metadata([$files]) WHERE path_in_schema = 'amount'"
expect "maintenance keeps every row" "5|v1v2v4v5v6" igloo "$admin_password" \
  "SELECT count(*), string_agg(v, '' ORDER BY id) FROM fragmented"
expect "writes after compaction" "6" igloo "$admin_password" \
  "INSERT INTO fragmented VALUES (7, 'v7', 7.25); SELECT count(*) FROM fragmented"

echo "health and metrics"
code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$http_port/readyz")"
if [ "$code" = 200 ]; then pass "readyz"; else fail "readyz returned $code"; fi
metrics="$(curl -s "http://127.0.0.1:$http_port/metrics")"
if grep -q '^igloo_queries_total' <<<"$metrics"; then pass "metrics"; else fail "metrics"; fi
if grep -q '^igloo_maintenance_passes_total{result="ok"}' <<<"$metrics"; then pass "maintenance metrics"; else fail "maintenance metrics"; fi
if grep -q '^igloo_audit_written_total{kind="query"}' <<<"$metrics" && ! grep -q '^igloo_audit_dropped_total' <<<"$metrics"; then
  pass "history metrics"
else
  fail "history metrics: $(grep '^igloo_audit' <<<"$metrics")"
fi

echo "restart"
# Read a table once so its files land in the read cache.
psql_admin -c "CREATE TABLE cached AS SELECT range AS n, md5(range::VARCHAR) AS s FROM range(200000)" >/dev/null
cached="$(psql_admin -c "SELECT sum(n), max(s) FROM cached")"
compose restart igloo >/dev/null
compose up -d --wait --wait-timeout 120 igloo >/dev/null
expect "data survives a restart" "3" walt walt-password-1 "SELECT count(*) FROM e2e"
if curl -s "http://127.0.0.1:$http_port/metrics" | grep -q '^igloo_cache_limit_bytes 0$'; then
  printf '  skip  %s\n' "read cache: paused, the disk is almost full"
else
  got="$(printf '%s\n' "SET cache_httpfs_profile_type = 'temp';" "SELECT sum(n), max(s) FROM cached;" \
    "SELECT cache_hit_count > 0 AND cache_miss_count = 0 FROM cache_httpfs_cache_access_info_query() WHERE cache_type = 'data';" \
    "SET cache_httpfs_profile_type = 'noop';" | psql_admin)" || true
  if [ "$got" = "$(printf 'SET\n%s\nt\nSET' "$cached")" ]; then
    pass "after a restart, table files come from the read cache"
  else
    fail "read cache after a restart: got [$got], want the result [$cached] and only cache hits"
  fi
fi
expect "views and comments survive a restart" "3|Test rows | :tag:e2e" rita rita-password-1 \
  "SELECT (SELECT count(*) FROM e2e_big), (SELECT comment FROM igloo_comments() WHERE table_name = 'e2e' AND column_name IS NULL)"
expect "grants survive a restart" "3" nora nora-password-1 "SELECT count(*) FROM e2e"
expect "drop a view" "DROP VIEW" walt walt-password-1 "DROP VIEW e2e_big"
expect_error "a dropped view is gone" 42P01 walt walt-password-1 "DROP VIEW e2e_big"

if [ "$failures" -ne 0 ]; then
  echo "$failures check(s) failed"
  exit 1
fi
echo "all checks passed"
