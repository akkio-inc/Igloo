#!/usr/bin/env bash
# End-to-end test of the single-machine stack: builds the image, starts an
# isolated copy of deploy/single-vm (its own project name, image, ports and
# data), and checks Igloo through real psql clients, the web console, Arrow
# Flight SQL clients and PyIceberg on the Iceberg REST catalog
# (scripts/engines-e2e.sh adds Spark and Trino).
# Removes everything afterwards unless KEEP_E2E=1. E2E_PROJECT and the
# E2E_*_PORT variables let several runs share a Docker host; E2E_IMAGE
# tests an image built already instead of building one.
#
#   scripts/e2e.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
project="${E2E_PROJECT:-igloo-e2e}"
sql_port="${E2E_SQL_PORT:-15433}"
http_port="${E2E_HTTP_PORT:-18432}"
console_port="${E2E_CONSOLE_PORT:-18443}"
flight_port="${E2E_FLIGHT_PORT:-13137}"
iceberg_port="${E2E_ICEBERG_PORT:-18181}"
sharing_port="${E2E_SHARING_PORT:-18444}"
image="${E2E_IMAGE:-$project:dev}"
build=--build
[ -z "${E2E_IMAGE:-}" ] || build=--no-build
pool_port="${E2E_POOL_PORT:-15434}"
work="$(mktemp -d)"
env_file="$work/e2e.env"

compose() {
  docker compose -p "$project" --project-directory "$root/deploy/single-vm" --env-file "$env_file" "$@"
}

# compose_pools adds the example compute pool (deploy/single-vm/compute-pool.yml).
compose_pools() {
  compose -f "$root/deploy/single-vm/docker-compose.yml" -f "$root/deploy/single-vm/compute-pool.yml" "$@"
}

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "--- igloo logs"
    compose logs --no-color --tail 80 igloo || true
  fi
  if [ "${KEEP_E2E:-}" != 1 ]; then
    compose_pools --profile local-storage --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
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
  -e "s/^IGLOO_CONSOLE_PORT=.*/IGLOO_CONSOLE_PORT=$console_port/" -e "s/^IGLOO_FLIGHT_PORT=.*/IGLOO_FLIGHT_PORT=$flight_port/" \
  -e "s/^IGLOO_ICEBERG_PORT=.*/IGLOO_ICEBERG_PORT=$iceberg_port/" -e "s/^IGLOO_SHARING_PORT=.*/IGLOO_SHARING_PORT=$sharing_port/" \
  -e "s|^IGLOO_IMAGE=.*|IGLOO_IMAGE=$image|" "$env_file"
# Maintain tables every few seconds, so the maintenance checks need not wait
# an hour.
echo "IGLOO_MAINTENANCE_INTERVAL=5s" >>"$env_file"
# A landing area in the bundled object store, for COPY INTO and pipes.
landing="s3://$(grep '^S3_BUCKET=' "$env_file" | cut -d= -f2)/landing"
sed -i.bak -e "s#^IGLOO_ALLOWED_LOCATIONS=.*#IGLOO_ALLOWED_LOCATIONS=$landing/#" "$env_file"
# A pool name and machine price, for the history and the cost estimates,
# and a small second pool.
cat >>"$env_file" <<EOF
IGLOO_POOL=e2e
IGLOO_PRICE_PER_HOUR=1.5
IGLOO_REPORTING_PORT=$pool_port
IGLOO_REPORTING_MEMORY_LIMIT=1GB
IGLOO_REPORTING_THREADS=2
EOF
admin_password="$(grep '^IGLOO_ADMIN_PASSWORD=' "$env_file" | cut -d= -f2)"
bucket="$(grep '^S3_BUCKET=' "$env_file" | cut -d= -f2)"

echo "starting the stack"
compose up -d "$build" --wait --wait-timeout 300 >/dev/null

network="${project}_default"

# psql_as USER PASSWORD SQL... runs SQL through a fresh psql container and
# prints tuples-only output (errors on stderr, merged).
psql_as() {
  local user=$1 password=$2
  shift 2
  docker run --rm -i --network "$network" -e PGPASSWORD="$password" postgres:17 \
    psql -h "${psql_host:-igloo}" -U "$user" -d igloo -X -At -v ON_ERROR_STOP=1 -v VERBOSITY=verbose "$@" 2>&1
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

# expect_refused NAME USER PASSWORD SQL: must fail without showing any of
# /etc/passwd.
expect_refused() {
  local name=$1 got
  got="$(psql_as "$2" "$3" -c "$4")" && { fail "$name: succeeded"; return; }
  if grep -q 'root:' <<<"$got"; then fail "$name: showed the file: $got"; else pass "$name"; fi
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

echo "access tokens and service accounts"
# token_of OUTPUT prints the token in a CREATE TOKEN row (user|name|token|expires).
token_of() { cut -d'|' -f3 <<<"$1"; }
expect "create a service account" "CREATE ROLE" igloo "$admin_password" "CREATE USER etl_bot TYPE SERVICE ROLE writer"
expect_error "service accounts have no password" 22023 igloo "$admin_password" "ALTER USER etl_bot PASSWORD 'some-password'"
got="$(psql_admin -c "SHOW USERS" | grep '^etl_bot|')"
if [ "$(cut -d'|' -f4,5,6,7 <<<"$got")" = "service|igloo|t|f" ]; then pass "SHOW USERS lists the service account and its owner"; else fail "SHOW USERS: $got"; fi
bot_token="$(token_of "$(psql_admin -c "CREATE TOKEN ci FOR USER etl_bot EXPIRES '1d'")")"
if [[ "$bot_token" == igloo_* ]]; then pass "CREATE TOKEN returns the token once"; else fail "CREATE TOKEN: $bot_token"; fi
expect "a service account signs in with its token" "3" etl_bot "$bot_token" "SELECT count(*) FROM e2e"
expect_error "service accounts cannot make themselves tokens" 42501 etl_bot "$bot_token" "CREATE TOKEN more"
if got="$(psql_as etl_bot "${bot_token%?}x" -c "SELECT 1")"; then
  fail "a wrong token was accepted"
elif grep -q "password authentication failed" <<<"$got"; then
  pass "a wrong token is rejected"
else
  fail "wrong token: $got"
fi
rita_token="$(token_of "$(psql_as rita rita-password-1 -c "CREATE TOKEN tableau EXPIRES '7d'")")"
expect "a person's token signs in where their password does" "rita" rita "$rita_token" "SELECT current_user"
expect "and the password still works" "rita" rita rita-password-1 "SELECT current_user"
expect_error "a token session cannot set the password" 42501 rita "$rita_token" "ALTER USER CURRENT_USER PASSWORD 'stolen-password'"
got="$(psql_as rita rita-password-1 -c "SHOW TOKENS")"
if [ "$(cut -d'|' -f1,2 <<<"$got")" = "rita|tableau" ] && ! grep -q "$rita_token" <<<"$got"; then
  pass "SHOW TOKENS lists one's own tokens, never the secret"
else
  fail "SHOW TOKENS: $got"
fi
expect_soon "token logins are recorded with the token's name" "t" igloo "$admin_password" \
  "SELECT bool_or(user_name = 'etl_bot' AND auth_method = 'token' AND token_name = 'ci') FROM igloo.login_history WHERE result = 'success'"
expect "an admin revokes a token" "DROP TOKEN" igloo "$admin_password" "DROP TOKEN ci FOR USER etl_bot"
if psql_as etl_bot "$bot_token" -c "SELECT 1" >/dev/null; then fail "a revoked token signed in"; else pass "a revoked token no longer signs in"; fi
expect "disable a user" "ALTER ROLE" igloo "$admin_password" "ALTER USER rita NOLOGIN"
if got="$(psql_as rita rita-password-1 -c "SELECT 1")"; then
  fail "a disabled user signed in"
elif grep -q "not permitted to log in" <<<"$got"; then
  pass "a disabled user cannot sign in, with a password or a token"
else
  fail "disabled user: $got"
fi
expect "enable the user again" "ALTER ROLE" igloo "$admin_password" "ALTER USER rita LOGIN"
expect "and they sign in again" "1" rita "$rita_token" "SELECT 1"

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
console_token="$(console_post /api/query '{"sql":"CREATE TOKEN from_console"}' "$csrf" | grep -o 'igloo_[a-z2-7]\{68\}' | head -1)"
expect "a token made in the console signs in to SQL" "walt" walt "${console_token:-none}" "SELECT current_user"
code="$(curl -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' \
  -d "{\"user\":\"walt\",\"password\":\"${console_token:-none}\"}" "$console/api/login")"
if [ "$code" = 401 ]; then pass "tokens do not sign in to the console"; else fail "console sign-in with a token: $code"; fi
console_post /api/logout '{}' "$csrf" >/dev/null
code="$(curl -s -o /dev/null -w '%{http_code}' -b "$jar" "$console/api/session")"
if [ "$code" = 401 ]; then pass "console sign-out"; else fail "console session after sign-out: $code"; fi

echo "sandbox"
expect_error "no local files" 42501 igloo "$admin_password" "SELECT * FROM read_text('/etc/passwd')"
expect_error "no other buckets" 42501 igloo "$admin_password" "SELECT * FROM read_parquet('s3://elsewhere/x.parquet')"
expect_error "no arbitrary URLs" 42501 igloo "$admin_password" "SELECT * FROM read_csv('http://lakekeeper:8181/management/v1/warehouse')"
expect_error "cannot reopen the sandbox" 22P02 igloo "$admin_password" "SET enable_external_access = true"

echo "media analytics"
expect "impressions" "CREATE TABLE" walt walt-password-1 "CREATE TABLE impressions AS
  SELECT (i % 4)::INT AS campaign_id, (ARRAY['ctv', 'social', 'display'])[1 + (i // 4) % 3] AS platform,
    DATE '2026-10-01' + (i % 7)::INT AS day, 'u' || (hash(i) % 20000)::VARCHAR AS user_id,
    -74.1 + (i % 300) / 1000.0 AS lon, 40.6 + (i % 290) / 1000.0 AS lat
  FROM range(100000) t(i)"
expect "sketches per campaign, platform and day, kept in Iceberg" "CREATE TABLE" walt walt-password-1 "CREATE TABLE daily_reach AS
  SELECT campaign_id, platform, day, count(*) AS impressions,
    datasketch_hll(12, user_id)::BLOB AS hll, datasketch_theta(12, user_id)::BLOB AS theta
  FROM impressions GROUP BY ALL"
expect "deduplicated reach from merged sketches" "t" rita rita-password-1 "
  SELECT abs(datasketch_hll_estimate(datasketch_hll_union(12, hll))
    / (SELECT count(DISTINCT user_id) FROM impressions WHERE campaign_id < 3) - 1) < 0.05
  FROM daily_reach WHERE campaign_id < 3"
expect "audience overlap from theta sketches" "t" rita rita-password-1 "
  WITH p AS (SELECT platform, datasketch_theta(12, theta::sketch_theta) AS s FROM daily_reach GROUP BY platform),
  exact AS (SELECT count(*) AS n FROM (SELECT user_id FROM impressions WHERE platform = 'ctv'
    INTERSECT SELECT user_id FROM impressions WHERE platform = 'social'))
  SELECT abs(datasketch_theta_estimate(datasketch_theta_intersect(a.s, b.s)) / exact.n - 1) < 0.1
  FROM p a, p b, exact WHERE a.platform = 'ctv' AND b.platform = 'social'"
expect "points in polygons" "t" rita rita-password-1 "
  SELECT count(*) FILTER (WHERE ST_Contains(ST_GeomFromText('POLYGON((-74.05 40.65, -73.9 40.65, -73.9 40.8, -74.05 40.8, -74.05 40.65))'),
    ST_Point(lon, lat))) = count(*) FILTER (WHERE lon > -74.05 AND lon < -73.9 AND lat > 40.65 AND lat < 40.8) FROM impressions"
expect "boundaries kept as WKB in Iceberg" "CREATE TABLE" walt walt-password-1 "CREATE TABLE zones AS
  SELECT 'z1' AS zone, ST_AsWKB(ST_GeomFromText('POLYGON((-74.05 40.65, -73.9 40.65, -73.9 40.8, -74.05 40.8, -74.05 40.65))')) AS boundary"
expect "and joined to points" "z1|t" rita rita-password-1 "
  SELECT z.zone, count(*) > 1000 FROM impressions i JOIN zones z ON ST_Contains(ST_GeomFromWKB(z.boundary), ST_Point(i.lon, i.lat)) GROUP BY ALL"
expect "geometries reach clients as WKT" "POINT (-73.98 40.75)" rita rita-password-1 "SELECT ST_Point(-73.98, 40.75)"
expect "distances in meters, longitude first" "6286" rita rita-password-1 \
  "SELECT round(ST_Distance_Sphere(ST_Point(-74.0060, 40.7128), ST_Point(-73.9352, 40.7306)))"
expect "H3 cells" "892a1072893ffff" rita rita-password-1 "SELECT h3_h3_to_string(h3_latlng_to_cell(40.7128, -74.0060, 9))"
expect "approximate and statistical functions" "t|t|t" rita rita-password-1 "
  SELECT approx_count_distinct(user_id) BETWEEN 18000 AND 22000, approx_quantile(lat, 0.5) BETWEEN 40.7 AND 40.8,
    regr_r2(lat, lon) BETWEEN 0 AND 1 FROM impressions"
expect "time zones" "2026-09-30 23:30" rita rita-password-1 \
  "SELECT strftime(timezone('America/New_York', TIMESTAMPTZ '2026-10-01 03:30:00+00'), '%Y-%m-%d %H:%M')"
# GDAL opens files a dataset names on its own: a VRT file a writer puts in
# the warehouse can name /etc/passwd. Igloo refuses GDAL for everyone.
vrt='<OGRVRTDataSource><OGRVRTLayer name="x"><SrcDataSource relativeToVRT="0">CSV:/etc/passwd</SrcDataSource></OGRVRTLayer></OGRVRTDataSource>'
expect "a writer can put any file in the warehouse" "COPY 1" walt walt-password-1 \
  "COPY (SELECT '$vrt' AS l) TO 's3://$bucket/warehouse/e2e/evil.vrt' (HEADER false, QUOTE '', DELIMITER E'\\x01')"
expect_error "ST_Read of that file is refused" 42501 igloo "$admin_password" "SELECT * FROM ST_Read('s3://$bucket/warehouse/e2e/evil.vrt')"
expect_error "ST_Read of a local file is refused" 42501 igloo "$admin_password" "SELECT * FROM ST_Read('/etc/passwd')"
expect_error "ST_Read of an outside URL is refused" 42501 igloo "$admin_password" \
  "SELECT * FROM ST_Read('http://lakekeeper:8181/management/v1/warehouse')"
expect_error "GeoPackages, which DuckDB reads with ST_Read, are refused" 42501 rita rita-password-1 \
  "SELECT * FROM 's3://$bucket/warehouse/e2e/zones.gpkg'"
expect_error "GDAL writes are refused" 42501 walt walt-password-1 \
  "COPY (SELECT ST_Point(1, 2) AS geom) TO 's3://$bucket/warehouse/e2e/x.geojson' (FORMAT GDAL, DRIVER 'GeoJSON')"
expect_refused "ST_ReadSHP cannot read local files" igloo "$admin_password" "SELECT * FROM ST_ReadSHP('/etc/passwd')"
expect_refused "nor outside URLs" igloo "$admin_password" "SELECT * FROM ST_ReadSHP('http://lakekeeper:8181/management/v1/x.shp')"
expect_refused "ST_ReadOSM cannot read local files" igloo "$admin_password" "SELECT * FROM ST_ReadOSM('/etc/passwd')"
expect_refused "nor outside URLs" igloo "$admin_password" "SELECT * FROM ST_ReadOSM('http://lakekeeper:8181/management/v1/x.osm.pbf')"

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

echo "Arrow Flight SQL"
# The clients run on this machine's network and reach the published
# loopback port: without TLS, Igloo serves only clients that connect to it
# as localhost.
flight_token="$(token_of "$(psql_admin -c "CREATE TOKEN flight FOR USER etl_bot EXPIRES '1d'")")"
if docker run --rm --network host -e FLIGHT_URI="grpc://127.0.0.1:$flight_port" -e IGLOO_ADMIN_PASSWORD="$admin_password" \
  -e FLIGHT_TOKEN="$flight_token" -v "$root/scripts/clients:/clients:ro" python:3.12-slim sh -c "
    pip install -q adbc-driver-flightsql==1.12.0 adbc-driver-manager==1.12.0 pyarrow==26.0.0 pandas==3.0.6 \
      polars==1.44.2 >/dev/null 2>&1 && python /clients/flight_test.py" >"$work/flight.out" 2>&1; then
  pass "ADBC with pandas and polars, pyarrow.flight, and access tokens"
else
  fail "Flight SQL from Python: $(tail -5 "$work/flight.out")"
fi
flight_jdbc_version=19.0.0
if docker run --rm --network host -e FLIGHT_HOST=127.0.0.1 -e FLIGHT_PORT="$flight_port" -e IGLOO_ADMIN_PASSWORD="$admin_password" \
  -v "$root/scripts/clients:/clients:ro" eclipse-temurin:21-jdk sh -c "
    cd /tmp && curl -fsSLO https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/$flight_jdbc_version/flight-sql-jdbc-driver-$flight_jdbc_version.jar &&
    java --add-opens=java.base/java.nio=ALL-UNNAMED -cp flight-sql-jdbc-driver-$flight_jdbc_version.jar /clients/FlightJdbcTest.java" \
  >"$work/flight-jdbc.out" 2>&1; then
  pass "Flight SQL JDBC driver $flight_jdbc_version"
else
  fail "Flight SQL JDBC: $(tail -5 "$work/flight-jdbc.out")"
fi
expect_soon "Flight SQL statements are in the query history" "t" igloo "$admin_password" \
  "SELECT count(*) FILTER (WHERE outcome = 'denied' AND user_name = 'rita') > 0 AND count(*) FILTER (WHERE outcome = 'ok') > 0
   FROM igloo.query_history WHERE client_address LIKE 'flight sql %'"

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

echo "policies and tags"
# An agency's contacts of two clients; nora's team sees acme's rows only,
# with emails and device IDs masked. Policy statements go one at a time.
psql_admin -c "
  CREATE SCHEMA crm;
  CREATE SCHEMA governance;
  CREATE TABLE crm.contacts (id INTEGER, client_id VARCHAR, email VARCHAR, device_id VARCHAR);
  INSERT INTO crm.contacts VALUES (1, 'acme', 'ann@acme.com', 'dev-1'), (2, 'globex', 'gus@globex.com', 'dev-2');
  CREATE TABLE governance.client_access (role_name VARCHAR, client_id VARCHAR);
  INSERT INTO governance.client_access VALUES ('acme_team', 'acme');" >/dev/null
contacts_file="$(psql_admin -c "SELECT file_path FROM iceberg_metadata(lake.crm.contacts) LIMIT 1")"
first_snapshot="$(psql_admin -c "SELECT snapshot_id FROM iceberg_snapshots(lake.crm.contacts) ORDER BY timestamp_ms LIMIT 1")"
if got="$(psql_admin \
  -c "CREATE ROLE acme_team" -c "CREATE ROLE pii_reader" -c "GRANT acme_team TO nora" \
  -c "GRANT SELECT ON TABLE crm.contacts TO analysts" \
  -c "CREATE ROW ACCESS POLICY client_rows AS (cid VARCHAR) RETURNS BOOLEAN ->
        is_role_in_session('admin') OR EXISTS (SELECT 1 FROM governance.client_access a
          WHERE a.client_id = cid AND is_role_in_session(a.role_name))" \
  -c "CREATE MASKING POLICY mask_email AS (val VARCHAR) RETURNS VARCHAR ->
        CASE WHEN is_role_in_session('pii_reader') THEN val ELSE '***@' || split_part(val, '@', 2) END" \
  -c "CREATE TAG pii ALLOWED_VALUES 'email', 'device_id'" \
  -c "CREATE MASKING POLICY pii_hash AS (val VARCHAR) RETURNS VARCHAR ->
        CASE WHEN is_role_in_session('pii_reader') THEN val ELSE tag_value('pii') || ':' || left(sha256(val), 8) END" \
  -c "ALTER TAG pii SET MASKING POLICY pii_hash" \
  -c "ALTER TABLE crm.contacts ADD ROW ACCESS POLICY client_rows ON (client_id)" \
  -c "ALTER TABLE crm.contacts ALTER COLUMN email SET MASKING POLICY mask_email" \
  -c "ALTER TABLE crm.contacts ALTER COLUMN device_id SET TAG pii = 'device_id'")"; then
  pass "create and attach policies and tags"
else
  fail "policies: $got"
fi
device="$(psql_admin -c "SELECT 'device_id:' || left(sha256('dev-1'), 8)")"
expect "a row access policy and masks apply" "1|acme|***@acme.com|$device" nora nora-password-1 \
  "SELECT * FROM crm.contacts"
expect "filters see masked values" "0|1" nora nora-password-1 \
  "SELECT count(*) FILTER (WHERE email = 'ann@acme.com'), count(*) FILTER (WHERE email LIKE '***@%') FROM crm.contacts"
expect "joins see visible rows" "1" nora nora-password-1 \
  "SELECT count(*) FROM crm.contacts a JOIN crm.contacts b USING (client_id)"
expect "time travel reads through the policies" "1|***@acme.com" nora nora-password-1 \
  "SELECT id, email FROM crm.contacts AT (VERSION => $first_snapshot)"
expect "admins too, unless a policy exempts them" "2|***@acme.com" igloo "$admin_password" \
  "SELECT count(*), min(email) FROM crm.contacts"
got="$(psql_as nora nora-password-1 -c "SELECT CAST(email AS INTEGER) FROM crm.contacts")" || true
if grep -q "ann@\|gus@" <<<"$got"; then fail "an error shows a raw email: $got"; else pass "errors show masked values"; fi
expect_error "the mapping table needs no grant, and gives none" 42501 nora nora-password-1 \
  "SELECT * FROM governance.client_access"
expect_error "readers cannot read a protected table's files" 42501 rita rita-password-1 \
  "SELECT * FROM read_parquet('$contacts_file')"
expect_error "nor list them" 42501 rita rita-password-1 "SELECT * FROM iceberg_metadata(lake.crm.contacts)"
expect_error "a protected table cannot be renamed" 42501 igloo "$admin_password" "ALTER TABLE crm.contacts RENAME TO contacts2"
expect_error "readers cannot change policies" 42501 rita rita-password-1 \
  "ALTER TABLE crm.contacts ALTER COLUMN email UNSET MASKING POLICY"
expect "who has the role sees raw values" "GRANT" igloo "$admin_password" "GRANT pii_reader TO nora"
expect "at once" "ann@acme.com|dev-1" nora nora-password-1 "SELECT email, device_id FROM crm.contacts"
psql_admin -c "REVOKE pii_reader FROM nora" >/dev/null
expect "policy and tag references" "client_rows|ROW ACCESS POLICY||
mask_email|MASKING POLICY|email|
pii_hash|MASKING POLICY|device_id|pii" nora nora-password-1 \
  "SELECT policy_name, policy_kind, ref_column_name, tag_name FROM igloo.policy_references
   WHERE ref_entity_name = 'contacts' ORDER BY policy_name"
expect "tag references" "pii|device_id|device_id" nora nora-password-1 \
  "SELECT tag_name, tag_value, column_name FROM igloo.tag_references"
expect_soon "the history names the policies that applied" '["client_rows","mask_email","pii_hash"]' igloo "$admin_password" \
  "SELECT policies FROM igloo.query_history WHERE user_name = 'nora' AND query_text = 'SELECT * FROM crm.contacts' LIMIT 1"
login="$(curl -sS -c "$work/nora.cookies" -H 'Content-Type: application/json' \
  -d '{"user":"nora","password":"nora-password-1"}' "$console/api/login")"
nora_csrf="$(sed -n 's/.*"csrf":"\([^"]*\)".*/\1/p' <<<"$login")"
got="$(curl -sS -b "$work/nora.cookies" -H 'Content-Type: application/json' -H "X-CSRF-Token: $nora_csrf" \
  -d '{"sql":"SELECT email FROM crm.contacts"}' "$console/api/query")"
if grep -q '"rows":\[\["\*\*\*@acme.com"\]\]' <<<"$got"; then pass "the console shows masked values"; else fail "console with policies: $got"; fi
# A materialized view is computed through its owner's policies, so it stores
# only masked values of the rows the owner may see; a reader of it, in no
# team, sees that and nothing more.
psql_admin -c "GRANT acme_team TO walt" >/dev/null
psql_as walt walt-password-1 -c "CREATE MATERIALIZED VIEW crm.contact_mv AS SELECT id, email FROM crm.contacts" >/dev/null || true
expect "a materialized view holds the owner's masked rows" "1|***@acme.com" rita rita-password-1 \
  "SELECT id, email FROM crm.contact_mv ORDER BY id"
mv_dump="$(psql_admin -c "SELECT email FROM crm.contact_mv")" || true
if grep -q "ann@acme.com\|gus@" <<<"$mv_dump"; then fail "a materialized view stored raw emails: $mv_dump"; else pass "a materialized view stores masked values"; fi
psql_admin -c "DROP MATERIALIZED VIEW crm.contact_mv" >/dev/null 2>&1 || true
psql_admin -c "REVOKE acme_team FROM walt" >/dev/null 2>&1 || true
echo "Iceberg REST catalog for other engines"
psql_admin -c "CREATE TABLE ice_events (id INTEGER, kind VARCHAR); INSERT INTO ice_events VALUES (1, 'signup'), (2, 'login');
  CREATE TABLE ice_private (id INTEGER)" >/dev/null
expect "users for other engines" "CREATE ROLE
CREATE ROLE
GRANT
GRANT" igloo "$admin_password" "
  CREATE USER ivy WITH PASSWORD 'ivy-password-1' ROLE none;
  CREATE USER ian WITH PASSWORD 'ian-password-1' ROLE none;
  GRANT SELECT ON ice_events TO ivy;
  GRANT SELECT, INSERT, UPDATE, DELETE ON ice_events TO ian;"
iceberg="http://127.0.0.1:$iceberg_port"
code="$(curl -s -o /dev/null -w '%{http_code}' "$iceberg/v1/config")"
if [ "$code" = 401 ]; then pass "the catalog needs a token"; else fail "catalog without a token: $code"; fi
got="$(curl -s -d grant_type=client_credentials -d client_id=ivy -d client_secret=wrong "$iceberg/v1/oauth/tokens")"
if grep -q '"error":"invalid_client"' <<<"$got"; then pass "the token endpoint refuses a wrong password"; else fail "token with a wrong password: $got"; fi
# catalog_token ARGS... signs in to the catalog with curl's -d ARGS and
# prints the access token.
catalog_token() {
  curl -s -d grant_type=client_credentials "$@" "$iceberg/v1/oauth/tokens" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p'
}
catalog_get() { curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $1" "$iceberg$2"; }
rest_token="$(token_of "$(psql_admin -c "CREATE TOKEN rest FOR USER etl_bot EXPIRES '1d'")")"
bearer="$(catalog_token --data-urlencode "client_secret=$rest_token")"
if [ "$(catalog_get "$bearer" /v1/namespaces)" = 200 ]; then
  pass "a service account signs in to the catalog with an access token"
else
  fail "catalog with an access token: $(catalog_get "$bearer" /v1/namespaces)"
fi
psql_admin -c "DROP TOKEN rest FOR USER etl_bot" >/dev/null
code="$(catalog_get "$bearer" /v1/namespaces)"
if [ "$code" = 401 ]; then pass "and the session ends when the token is dropped"; else fail "after DROP TOKEN: $code"; fi
# crm.contacts has a row access policy and a masked column, which an engine
# reading its files would bypass.
got="$(curl -s -H "Authorization: Bearer $(catalog_token -d client_id=rita -d client_secret=rita-password-1)" \
  "$iceberg/v1/namespaces/crm/tables/contacts")"
if grep -q "data policies" <<<"$got"; then pass "engines may not load a table with data policies"; else fail "protected table for a reader: $got"; fi
code="$(catalog_get "$(catalog_token -d client_id=igloo --data-urlencode "client_secret=$admin_password")" /v1/namespaces/crm/tables/contacts)"
if [ "$code" = 200 ]; then pass "admins may"; else fail "protected table for an admin: $code"; fi
pyiceberg_version=0.12.0
s3_key="$(grep '^S3_ACCESS_KEY_ID=' "$env_file" | cut -d= -f2)"
if docker run --rm --network "$network" -e ICEBERG_URI=http://igloo:8181 -e IGLOO_ACCESS_KEY_ID="$s3_key" \
  -v "$root/scripts/clients:/clients:ro" python:3.12-slim sh -c "
    pip install -q 'pyiceberg[pyarrow]==$pyiceberg_version' >/dev/null 2>&1 && python /clients/pyiceberg_test.py" >"$work/pyiceberg.out" 2>&1; then
  pass "PyIceberg $pyiceberg_version"
else
  fail "PyIceberg: $(tail -5 "$work/pyiceberg.out")"
fi
expect "SQL sees what PyIceberg wrote" "1:signup,2:login,3:purchase|1|staged" igloo "$admin_password" \
  "SELECT (SELECT string_agg(id || ':' || kind, ',' ORDER BY id) FROM ice_events), (SELECT count(*) FROM ice_created),
     (SELECT string_agg(kind, ',') FROM ice_staged)"
expect_soon "catalog requests are in the query history" "t|t|t" igloo "$admin_password" "
  SELECT bool_or(user_name = 'ivy' AND statement_type = 'loadTable' AND outcome = 'ok'),
         bool_or(user_name = 'ivy' AND outcome = 'denied' AND sqlstate = '42501'),
         bool_or(user_name = 'ian' AND statement_type = 'updateTable' AND query_text LIKE '%(add-snapshot (append)%')
  FROM igloo.query_history WHERE application_name LIKE 'iceberg-rest%'"
expect_soon "and their sign-ins in the login history" "t|t|t" igloo "$admin_password" \
  "SELECT bool_or(user_name = 'ivy' AND result = 'failure'), bool_or(user_name = 'ian' AND result = 'success'),
     bool_or(user_name = 'etl_bot' AND result = 'success' AND token_name = 'rest')
   FROM igloo.login_history WHERE application_name = 'iceberg rest'"

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

echo "workload management"
got="$(psql_as rita rita-password-1 -c "SET query_tag = 'client:acme;campaign:e2e'" -c "SELECT count(*) FROM impressions /* tagged */")" || true
if [ "$got" = "$(printf 'SET\n100000')" ]; then pass "a tagged query"; else fail "a tagged query: got [$got]"; fi
expect_soon "the history records its tag, pool and resources" "client:acme;campaign:e2e|e2e|t|t" igloo "$admin_password" "
  SELECT query_tag, pool, cpu_time_ms > 0, bytes_read IS NOT NULL FROM igloo.query_history
  WHERE user_name = 'rita' AND query_text LIKE '%/* tagged */' LIMIT 1"
# Replicas record their running time every minute, which prices the day.
expect_soon "usage per tag, with a share of the day's cost" "2|t|t" igloo "$admin_password" "
  SELECT statements, time_share > 0, estimated_cost > 0 FROM igloo.usage
  WHERE user_name = 'rita' AND query_tag = 'client:acme;campaign:e2e'"
expect "users see their own usage" "t" rita rita-password-1 "SELECT count(*) > 0 AND bool_and(user_name = 'rita') FROM igloo.usage"
slow="SELECT count(*) FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range"
expect "a role's default tag" "ALTER ROLE" igloo "$admin_password" "ALTER ROLE analysts SET query_tag = 'client:analysts'"
expect "reaches its members' sessions" "client:analysts" nora nora-password-1 "SHOW query_tag"
expect "a built-in role's statement timeout" "ALTER ROLE" igloo "$admin_password" "ALTER ROLE reader SET statement_timeout = '1s'"
expect_error "cancels its users' long statements" 57014 rita rita-password-1 "$slow"
expect "a role's concurrency limit" "ALTER ROLE" igloo "$admin_password" "ALTER ROLE analysts SET max_concurrent_statements = 1"
psql_as nora nora-password-1 -c "SET statement_timeout = '15s'" -c "$slow" >/dev/null 2>&1 &
holder=$!
sleep 5
got="$(psql_as nora nora-password-1 -c "SET statement_timeout = '1s'" -c "SELECT 1")" || true
if grep -q "behind the limit of 1 statement at once for role analysts" <<<"$got"; then
  pass "statements beyond it wait in line, until their timeout"
else
  fail "a role's concurrency limit: got [$got]"
fi
wait "$holder" || true
got="$(psql_admin -c 'SHOW ROLE SETTINGS')"
if grep -q '^role|analysts|max_concurrent_statements|1|igloo|' <<<"$got"; then pass "SHOW ROLE SETTINGS"; else fail "SHOW ROLE SETTINGS: $got"; fi
psql_admin -c "ALTER ROLE analysts RESET ALL" >/dev/null
psql_admin -c "ALTER ROLE reader RESET ALL" >/dev/null
expect "limits lifted" "100000" rita rita-password-1 "SELECT count(*) FROM impressions"

echo "compute pools"
compose_pools up -d --no-build --wait --wait-timeout 180 igloo-reporting >/dev/null
psql_host=igloo-reporting expect "a second compute pool reads the same tables" "3" rita rita-password-1 \
  "SELECT count(*) FROM e2e /* reporting */"
expect_soon "its statements carry its pool" "reporting" igloo "$admin_password" \
  "SELECT pool FROM igloo.query_history WHERE query_text = 'SELECT count(*) FROM e2e /* reporting */' LIMIT 1"
compose_pools rm -sf igloo-reporting >/dev/null

echo "delta sharing"
# The provider side over the protocol with curl, then Igloo reading its own
# share as a provider's. scripts/sharing-e2e.sh adds the official Python
# client and the reference sharing server.
sharing="http://127.0.0.1:$sharing_port/delta-sharing"
psql_admin -c "CREATE TABLE shared_spend AS SELECT range::INTEGER AS id, 'client_' || (range % 3) AS client,
  (range * 1.5)::DECIMAL(10,2) AS spend FROM range(30)" >/dev/null
expect "an admin creates a share" "CREATE SHARE
ALTER SHARE" igloo "$admin_password" "CREATE SHARE e2e_share COMMENT 'For partners';
  ALTER SHARE e2e_share ADD TABLE shared_spend WITH HISTORY;"
expect_error "only admins manage shares" 42501 walt walt-password-1 "CREATE SHARE nope"
recipient="$(psql_admin -c "CREATE RECIPIENT e2e_partner COMMENT 'E2E partner'")" || true
code="${recipient#*/activate/}"
code="${code%%|*}"
expect "and grants it to a recipient" "GRANT" igloo "$admin_password" "GRANT SELECT ON SHARE e2e_share TO RECIPIENT e2e_partner"
profile="$(curl -sS -X POST "$sharing/activate/$code")" || true
token="$(sed -n 's/.*"bearerToken": "\([^"]*\)".*/\1/p' <<<"$profile")"
if [ -n "$token" ]; then pass "the activation link gives a profile file"; else fail "activation: [$recipient] [$profile]"; fi
status="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$sharing/activate/$code")"
if [ "$status" = 404 ]; then pass "an activation link works once"; else fail "a second activation: $status"; fi
ds() { curl -sS -H "Authorization: Bearer $token" "$@"; }
got="$(ds "$sharing/shares")" || true
if grep -q '"name":"e2e_share"' <<<"$got"; then pass "the recipient lists its shares"; else fail "list shares: $got"; fi
got="$(ds -X POST -H 'Content-Type: application/json' -d '{}' "$sharing/shares/e2e_share/schemas/public/tables/shared_spend/query")" || true
file_url="$(sed -n 's/.*"url":"\([^"]*\)".*/\1/p' <<<"$got" | head -1)"
if grep -q '"protocol":{"minReaderVersion":1}' <<<"$got" && grep -q 'numRecords\\":30' <<<"$got" && [ -n "$file_url" ]; then
  pass "a query lists the snapshot's files"
else
  fail "query: $got"
fi
if [ "$(curl -sS -r 0-3 "$file_url")" = PAR1 ]; then pass "a signed URL serves the Parquet file"; else fail "file URL $file_url"; fi
status="$(curl -s -o /dev/null -w '%{http_code}' -H 'Host: igloo.example.com' -H "Authorization: Bearer $token" "$sharing/shares")"
if [ "$status" = 403 ]; then pass "the endpoint refuses unencrypted requests from afar"; else fail "plain HTTP from afar: $status"; fi
status="$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer ${token}x" "$sharing/shares")"
if [ "$status" = 401 ]; then pass "a wrong token is refused"; else fail "wrong token: $status"; fi

self_profile="{\"shareCredentialsVersion\": 1, \"endpoint\": \"http://localhost:8444/delta-sharing\", \"bearerToken\": \"$token\"}"
expect "an admin registers a provider's profile" "CREATE PROVIDER" igloo "$admin_password" "CREATE PROVIDER self FROM '$self_profile'"
expect "and lists its tables" "e2e_share|public|shared_spend" igloo "$admin_password" "SHOW SHARES IN PROVIDER self"
psql_admin -c "CREATE SCHEMA partners" >/dev/null
expect "an external table reads a shared table" "CREATE EXTERNAL TABLE" igloo "$admin_password" \
  "CREATE EXTERNAL TABLE partners.spend FROM SHARE self.e2e_share.public.shared_spend"
expect "readers query it by every name" "30|652.50|30" rita rita-password-1 \
  "SELECT count(*), sum(spend), (SELECT count(*) FROM lake.partners.spend) FROM partners.spend"
expect_error "a user with role none needs a grant" 42501 nora nora-password-1 "SELECT count(*) FROM partners.spend"
psql_admin -c "GRANT SELECT ON TABLE partners.spend TO nora" >/dev/null
expect "and reads it with one" "30" nora nora-password-1 "SELECT count(*) FROM partners.spend"
expect_error "but not with a table not granted" 42501 nora nora-password-1 \
  "SELECT count(*) FROM partners.spend, (SELECT max(n) FROM lake.public.contended)"
# The view's SQL, which anyone can list, names the proxy's file URLs.
proxy_url="$(psql_as nora nora-password-1 -c "SELECT regexp_extract(sql, '(http://127[.]0[.]0[.]1[^'']*)', 1)
  FROM duckdb_views() WHERE view_name = 'spend'")" || true
if [[ "$proxy_url" == http://127.0.0.1:* ]]; then pass "the external table's view lists proxy URLs"; else fail "proxy URL: $proxy_url"; fi
expect_error "which users with role none cannot read directly" 42501 nora nora-password-1 \
  "SELECT count(*) FROM read_parquet('$proxy_url')"
psql_admin -c "INSERT INTO shared_spend VALUES (100, 'client_9', 10.00)" >/dev/null
expect_soon "the external table follows new versions" "31" rita rita-password-1 "SELECT count(*) FROM partners.spend"
psql_admin -c "DELETE FROM shared_spend WHERE id < 10" >/dev/null
expect_soon "and rows deleted from the shared table" "21|10" rita rita-password-1 "SELECT count(*), min(id) FROM partners.spend"
got="$(psql_admin -c "SHOW EXTERNAL TABLES")" || true
if grep -q '^partners|spend|self|e2e_share|public|shared_spend|[0-9]' <<<"$got"; then pass "SHOW EXTERNAL TABLES"; else fail "SHOW EXTERNAL TABLES: $got"; fi
expect_soon "recipients' requests are in the query history" "t" igloo "$admin_password" \
  "SELECT count(*) > 0 FROM igloo.query_history WHERE user_name = 'recipient:e2e_partner'
   AND list_contains(tables_read, 'lake.public.shared_spend')"
expect "bearer tokens are never recorded" "0" igloo "$admin_password" \
  "SELECT count(*) FROM igloo.query_history WHERE query_text LIKE '%' || 'dst' || '_%'"
psql_admin -c "REVOKE SELECT ON SHARE e2e_share FROM RECIPIENT e2e_partner" >/dev/null
if [ "$(ds "$sharing/shares")" = '{"items":[]}' ]; then pass "a revoked share is gone at once"; else fail "after REVOKE: $(ds "$sharing/shares")"; fi
expect "dropping an external table" "DROP EXTERNAL TABLE" igloo "$admin_password" "DROP EXTERNAL TABLE partners.spend"
expect "and its provider" "DROP PROVIDER" igloo "$admin_password" "DROP PROVIDER self"
psql_admin -c "DROP RECIPIENT e2e_partner" >/dev/null
status="$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $token" "$sharing/shares")"
if [ "$status" = 401 ]; then pass "a dropped recipient's token stops working"; else fail "dropped recipient: $status"; fi

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

echo "loading, tasks, pipes and materialized views"
# Drops arrive as files in the landing area; the admin writes them there.
psql_admin -c "COPY (SELECT range AS id, 'item ' || range AS name, range * 1.5 AS amount FROM range(1, 4))
  TO '$landing/drops/2026-10-01.csv.gz' (FORMAT csv, HEADER true)" >/dev/null
psql_admin -c "COPY (SELECT range AS id, 'item ' || range AS name, range * 1.5 AS amount FROM range(4, 6))
  TO '$landing/drops/2026-10-02.csv.gz' (FORMAT csv, HEADER true)" >/dev/null
psql_admin -c "COPY (SELECT 99 AS id) TO '$landing/drops/readme.parquet' (FORMAT parquet)" >/dev/null
psql_as walt walt-password-1 -c "CREATE TABLE e2e_sales (id INTEGER, name VARCHAR, amount DECIMAL(10,2))" >/dev/null
load="COPY INTO e2e_sales FROM '$landing/drops/' (PATTERN '.*[.]csv[.]gz')"
got="$(psql_as walt walt-password-1 -c "$load")" || true
if [ "$(grep -c '[.]csv[.]gz|loaded|[23]|0|$' <<<"$got")" = 2 ]; then pass "COPY INTO loads the matching files"; else fail "COPY INTO: $got"; fi
got="$(psql_as walt walt-password-1 -c "$load")" || true
if [ -z "$got" ]; then pass "and skips them the next time"; else fail "COPY INTO loaded again: $got"; fi
expect "the table has each row once" "5|7.50" walt walt-password-1 "SELECT count(*), max(amount) FROM e2e_sales"
expect "the load history records each file" "2|5|walt" walt walt-password-1 \
  "SELECT count(*), sum(row_count), any_value(user_name) FROM igloo.load_history WHERE table_name = 'lake.public.e2e_sales' AND status = 'loaded'"
expect_error "readers need INSERT to load" 42501 rita rita-password-1 "$load"
expect_error "only from the allowed locations" 42501 walt walt-password-1 "COPY INTO e2e_sales FROM 's3://elsewhere/drops/'"
psql_as walt walt-password-1 -c "COPY INTO e2e_readme FROM '$landing/drops/readme.parquet' (CREATE_TABLE true)" >/dev/null || true
expect "CREATE_TABLE makes a table from the files" "1|99" walt walt-password-1 "SELECT count(*), max(id) FROM e2e_readme"

expect "a pipe" "CREATE PIPE" walt walt-password-1 \
  "CREATE PIPE e2e_pipe SCHEDULE '2 seconds' AS COPY INTO e2e_sales FROM '$landing/pipe/'"
psql_admin -c "COPY (SELECT 10 AS id, 'piped' AS name, 10.0 AS amount) TO '$landing/pipe/a.parquet' (FORMAT parquet)" >/dev/null
expect_soon "loads a file that arrives" "1" walt walt-password-1 "SELECT count(*) FROM e2e_sales WHERE name = 'piped'"
psql_admin -c "COPY (SELECT 11 AS id, 'piped' AS name, 11.0 AS amount) TO '$landing/pipe/b.parquet' (FORMAT parquet)" >/dev/null
expect_soon "and the next, once" "2" walt walt-password-1 "SELECT count(*) FROM e2e_sales WHERE name = 'piped'"
expect "pipe loads are in the load history" "2" walt walt-password-1 \
  "SELECT count(*) FROM igloo.load_history WHERE pipe_name = 'e2e_pipe' AND status = 'loaded'"

psql_as walt walt-password-1 -c "CREATE TABLE e2e_ticks (n INTEGER)" >/dev/null
expect "a task" "CREATE TASK" walt walt-password-1 "CREATE TASK e2e_task SCHEDULE '2 seconds' AS INSERT INTO e2e_ticks VALUES (1)"
got="$(psql_as walt walt-password-1 -c 'SHOW TASKS')" || true
if grep -q "^e2e_task|walt|suspended|2 seconds|UTC|" <<<"$got"; then pass "SHOW TASKS: suspended at first"; else fail "SHOW TASKS: $got"; fi
expect "EXECUTE TASK runs it now, as its owner" "EXECUTE TASK" igloo "$admin_password" "EXECUTE TASK e2e_task"
expect "ALTER TASK RESUME" "ALTER TASK" walt walt-password-1 "ALTER TASK e2e_task RESUME"
expect_soon "the task runs on its schedule" "t" walt walt-password-1 "SELECT count(*) >= 3 FROM e2e_ticks"
expect_soon "and its runs are recorded" "t" walt walt-password-1 \
  "SELECT count(*) >= 2 FROM igloo.task_history WHERE name = 'e2e_task' AND trigger = 'schedule' AND outcome = 'succeeded'"
expect_error "only its owner changes it" 42501 rita rita-password-1 "ALTER TASK e2e_task SUSPEND"
activity="$(curl -s "http://127.0.0.1:$http_port/activity")"
if grep -q '"next_due":"' <<<"$activity"; then pass "/activity reports when work is next due"; else fail "/activity: $activity"; fi
expect "ALTER TASK SUSPEND" "ALTER TASK" walt walt-password-1 "ALTER TASK e2e_task SUSPEND"

expect "a materialized view" "CREATE MATERIALIZED VIEW" walt walt-password-1 \
  "CREATE MATERIALIZED VIEW e2e_sales_totals AS SELECT count(*) AS orders, sum(amount) AS total FROM e2e_sales"
psql_as walt walt-password-1 -c "INSERT INTO e2e_sales VALUES (12, 'late', 1)" >/dev/null
expect "keeps its rows until refreshed" "7" rita rita-password-1 "SELECT orders FROM e2e_sales_totals"
expect "REFRESH MATERIALIZED VIEW" "REFRESH MATERIALIZED VIEW" walt walt-password-1 "REFRESH MATERIALIZED VIEW e2e_sales_totals"
expect "which replaces them" "8|44.50" rita rita-password-1 "SELECT orders, total FROM e2e_sales_totals"
psql_admin -c "GRANT SELECT ON TABLE e2e_sales_totals TO nora" >/dev/null
expect "reading it needs SELECT on it" "8" nora nora-password-1 "SELECT orders FROM e2e_sales_totals"
expect_error "not on what it reads" 42501 nora nora-password-1 "SELECT count(*) FROM e2e_sales"

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
got="$(psql_as walt walt-password-1 -c 'SHOW PIPES')" || true
if grep -q "^e2e_pipe|walt|started|2 seconds|" <<<"$got"; then pass "pipes survive a restart"; else fail "SHOW PIPES after a restart: $got"; fi
sleep 5
expect "and do not load their files again" "2" walt walt-password-1 "SELECT count(*) FROM e2e_sales WHERE name = 'piped'"
expect "drop a pipe" "DROP PIPE" walt walt-password-1 "DROP PIPE e2e_pipe"
expect "drop a task" "DROP TASK" walt walt-password-1 "DROP TASK e2e_task"
expect "drop a materialized view" "DROP MATERIALIZED VIEW" walt walt-password-1 "DROP MATERIALIZED VIEW e2e_sales_totals"
expect "policies survive a restart" "1|***@acme.com" nora nora-password-1 "SELECT id, email FROM crm.contacts"
expect "drop a view" "DROP VIEW" walt walt-password-1 "DROP VIEW e2e_big"
expect_error "a dropped view is gone" 42P01 walt walt-password-1 "DROP VIEW e2e_big"

if [ "$failures" -ne 0 ]; then
  echo "$failures check(s) failed"
  exit 1
fi
echo "all checks passed"
