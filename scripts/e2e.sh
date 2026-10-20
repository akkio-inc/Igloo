#!/usr/bin/env bash
# End-to-end test of the single-machine stack: builds the image, starts an
# isolated copy of deploy/single-vm (its own project name, image, ports and
# data), and checks Igloo through real psql clients, the web console, Arrow
# Flight SQL clients and PyIceberg on the Iceberg REST catalog
# (scripts/engines-e2e.sh adds Spark and Trino), with a webhook receiver
# for alerts.
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
    # The receivers are on the stack's network, which cannot go while they are.
    docker rm -f -v "$project-crm" "$project-ops" "$project-hooks" "$project-lineage" >/dev/null 2>&1 || true
    compose_pools --profile local-storage --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$work"
  else
    echo "kept the stack: docker compose -p $project --env-file $env_file ..."
  fi
  exit "$status"
}
trap cleanup EXIT
# A command outside the checks that fails ends the run; say which.
trap 'echo "e2e.sh: line $LINENO failed with status $?: $BASH_COMMAND" >&2' ERR

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
# and a small second pool. Lineage events go to a receiver on the stack's
# network, started below.
cat >>"$env_file" <<EOF
IGLOO_POOL=e2e
IGLOO_PRICE_PER_HOUR=1.5
IGLOO_REPORTING_PORT=$pool_port
IGLOO_REPORTING_MEMORY_LIMIT=1GB
IGLOO_REPORTING_THREADS=2
IGLOO_OPENLINEAGE_URL=http://lineage:8080/api/v1/lineage
IGLOO_OPENLINEAGE_ALLOW_PRIVATE=true
EOF
# A webhook receiver on the stack's network, as "hooks": notifications may
# reach it although it is not public.
sed -i.bak -e "s/^IGLOO_NOTIFICATION_ALLOW_HOSTS=.*/IGLOO_NOTIFICATION_ALLOW_HOSTS=hooks/" "$env_file"
admin_password="$(grep '^IGLOO_ADMIN_PASSWORD=' "$env_file" | cut -d= -f2)"
bucket="$(grep '^S3_BUCKET=' "$env_file" | cut -d= -f2)"

echo "starting the stack"
compose up -d "$build" --wait --wait-timeout 300 >/dev/null

network="${project}_default"

# An OpenLineage endpoint that prints the events it receives.
docker run -d --name "$project-lineage" --network "$network" --network-alias lineage python:3.12-slim python -u -c '
import http.server, sys
class Receiver(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        print(self.headers.get("Content-Type"), body.decode())
        self.send_response(201)
        self.end_headers()
    def log_message(self, *args):
        pass
http.server.HTTPServer(("", 8080), Receiver).serve_forever()
' >/dev/null

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

# expect_soon NAME WANT USER PASSWORD SQL [SECONDS]: like expect, but waits
# up to a minute (or SECONDS) for the output, for history that is written
# in the background.
expect_soon() {
  local name=$1 want=$2 got="" deadline=$((SECONDS + ${6:-60}))
  while [ "$SECONDS" -lt "$deadline" ]; do
    got="$(psql_as "$3" "$4" -c "$5")" || true
    [ "$got" = "$want" ] && break
    sleep 2
  done
  if [ "$got" = "$want" ]; then pass "$name"; else fail "$name: got [$got], want [$want]"; fi
}

echo "tables and Iceberg writes"
# psql prints the result of every statement in a multi-statement -c.
expect "create, insert, CTAS, update, delete" "CREATE TABLE
INSERT 0 2
CREATE TABLE
UPDATE 1
DELETE 1
2|beta|40.50" igloo "$admin_password" "
  CREATE TABLE e2e (id INTEGER, name VARCHAR, amount DECIMAL(10,2));
  INSERT INTO e2e VALUES (1, 'alpha', 10.50), (2, 'beta', 20.25);
  CREATE TABLE e2e_copy AS SELECT * FROM e2e;
  UPDATE e2e_copy SET amount = amount * 2 WHERE id = 2;
  DELETE FROM e2e_copy WHERE id = 1;
  SELECT * FROM e2e_copy;"
# DuckDB updated and deleted with position deletes: the old rows stay in
# the files, deleted, and the catalog does not count them.
expect_soon "the catalog counts rows without deleted ones" "1" igloo "$admin_password" \
  "SELECT row_count FROM igloo.tables WHERE table_schema = 'public' AND table_name = 'e2e_copy'"

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
# A non-admin may read a file in the landing area, but not one that climbs
# out of it: the path check refuses the climb before DuckDB opens anything.
expect_error "no climbing out of an allowed location" 42501 walt walt-password-1 \
  "SELECT * FROM read_text('$landing/../other.txt')"
expect_error "nor with percent-encoded dots" 42501 walt walt-password-1 \
  "SELECT * FROM read_text('$landing/%2e%2e/other.txt')"
expect_error "nor a path built as the query runs" 42501 walt walt-password-1 \
  "SELECT * FROM read_text('$landing/' || '../other.txt')"
expect_error "nor an s3 endpoint override" 42501 walt walt-password-1 \
  "SELECT * FROM read_text('$landing/x.txt?s3_endpoint=example.com')"

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
# A file in the warehouse that no table owns makes erasure requests made
# in the next three days wait for it (docs/privacy.md): the admin deletes
# these with the bucket's own tools.
deleted="$(docker run --rm --network "$network" -e S3_ENDPOINT=http://storage:9000 -e S3_BUCKET="$bucket" \
  -e S3_ACCESS_KEY_ID="$(grep '^S3_ACCESS_KEY_ID=' "$env_file" | cut -d= -f2)" \
  -e S3_SECRET_ACCESS_KEY="$(grep '^S3_SECRET_ACCESS_KEY=' "$env_file" | cut -d= -f2)" -e PREFIX=warehouse/e2e/ \
  -v "$root/scripts/clients:/clients:ro" python:3.12-slim sh -c "
    pip install -q boto3==1.40.61 >/dev/null 2>&1 && python /clients/delete_prefix.py" 2>&1)" || true
if [ "$deleted" -ge 1 ] 2>/dev/null; then
  pass "the admin deletes the file the writer put in the warehouse"
else
  fail "deleting the file the writer put in the warehouse: $deleted"
fi
# The benchmark's generator (deploy/single-vm/sql/load-media.sql), small,
# in a schema of its own. Its columns must be independent: they once came
# from DuckDB's hash(i, k), which differ by the same constant for every i,
# and every click landed on one channel and each campaign reached only the
# devices of one residue modulo 16.
if got="$(psql_admin -c "CREATE SCHEMA media_gen" -c "SET search_path = media_gen" -v rows=200000 -f - \
  <"$root/deploy/single-vm/sql/load-media.sql")"; then
  expect "load-media.sql: clicks on every channel, each campaign across the devices" "4|t" igloo "$admin_password" "
    SELECT (SELECT count(DISTINCT c.channel) FROM media_gen.impressions i JOIN media_gen.campaigns c USING (campaign_id) WHERE i.clicked),
      (SELECT min(r) > 8 FROM (SELECT count(DISTINCT device_id % 16) AS r FROM media_gen.impressions GROUP BY campaign_id))"
else
  fail "load-media.sql: $got"
fi

echo "client compatibility"
got="$(psql_admin -c '\dt')"
if grep -q "public|e2e|table" <<<"$got"; then pass '\dt lists tables'; else fail "\\dt: $got"; fi
got="$(psql_admin -c '\d e2e_copy')"
if grep -q "amount|numeric(10,2)" <<<"$got"; then pass '\d shows columns'; else fail "\\d: $got"; fi
# Superset sets the search path to the schema of every query, and
# load-media.sql above set it to one without views: nothing to warn about.
if compose logs --no-color igloo 2>&1 | grep -q "could not keep views"; then
  fail "SET search_path to a schema without views logs a warning: $(compose logs --no-color igloo 2>&1 | grep -m1 "could not keep views")"
else
  pass "SET search_path to a schema without views logs no warning"
fi

echo "TPC-H"
got="$(compose run --rm -T psql -X -v ON_ERROR_STOP=1 -v sf=0.1 -f /sql/load-tpch.sql 2>&1)"
if grep -q "^ *600572$" <<<"$got"; then pass "load TPC-H SF 0.1"; else fail "load TPC-H: $got"; fi
for q in 1 6 18; do
  if psql_admin -c "PRAGMA tpch($q)" >/dev/null; then pass "TPC-H Q$q"; else fail "TPC-H Q$q"; fi
done

echo "Snowflake SQL"
# Per session: a statement in Snowflake's dialect, with its functions,
# string escapes and result column names.
got="$(psql_as rita rita-password-1 -P tuples_only=off -P footer=off -c "SET sql_dialect = 'snowflake'" \
  -c "SELECT IFF(true, 1, 2) AS flag, DATEADD(month, 1, '2024-01-31'::date) AS \"next\", 'it\\'s' AS s, NVL(NULL, 'x')")"
if [ "$got" = "SET
FLAG|next|S|NVL(NULL, 'X')
1|2024-02-29|it's|x" ]; then
  pass "SET sql_dialect = 'snowflake' in a session, as a reader"
else
  fail "Snowflake dialect in a session: [$got]"
fi
expect "a session starts in DuckDB's dialect" "duckdb" rita rita-password-1 "SHOW sql_dialect"
# Per connection, from the startup options; RESET goes back to it.
got="$(docker run --rm -i --network "$network" -e PGPASSWORD=rita-password-1 -e PGOPTIONS='-c sql_dialect=snowflake' postgres:17 \
  psql -h "${psql_host:-igloo}" -U rita -d igloo -X -At -v ON_ERROR_STOP=1 \
  -c "SELECT ZEROIFNULL(NULL), SPLIT_PART('a,b', ',', 2)" -c "SET sql_dialect = 'duckdb'" -c "RESET sql_dialect" -c "SHOW sql_dialect" 2>&1)" || true
if [ "$got" = "0|b
SET
RESET
snowflake" ]; then
  pass "sql_dialect in the connection's options"
else
  fail "sql_dialect in the connection's options: [$got]"
fi
# Per user, set by an admin.
expect "sql_dialect as a user's setting" "ALTER USER" igloo "$admin_password" "ALTER USER walt SET sql_dialect = 'snowflake'"
expect "a user's sessions start in their dialect" "snowflake
2" walt walt-password-1 "SHOW sql_dialect; SELECT LEN('ab')"
expect "and SET overrides it" "SET
duckdb" walt walt-password-1 "SET sql_dialect = 'duckdb'; SHOW sql_dialect"
psql_admin -c "ALTER USER walt RESET sql_dialect" >/dev/null
# same_as_duckdb NAME SNOWFLAKE_SQL DUCKDB_SQL: the query in Snowflake's
# dialect must return what its DuckDB counterpart returns.
same_as_duckdb() {
  local got_sf got_duck
  got_sf="$(psql_admin -c "SET sql_dialect = 'snowflake'" -c "$2" | tail -n +2)" || true
  got_duck="$(psql_admin -c "$3")" || true
  if [ -n "$got_duck" ] && [ "$got_sf" = "$got_duck" ]; then pass "$1"; else fail "$1: [$got_sf], DuckDB's: [$got_duck]"; fi
}
same_as_duckdb "TPC-H Q1 in Snowflake's dialect" \
  "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order
    FROM lineitem WHERE l_shipdate <= DATEADD(day, -90, '1998-12-01'::date)
    GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus" \
  "SELECT l_returnflag, l_linestatus, sum(l_quantity), avg(l_discount), count(*)
    FROM lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL 90 DAY
    GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
same_as_duckdb "TPC-H Q3 in Snowflake's dialect" \
  "SELECT o_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, TO_CHAR(o_orderdate, 'YYYY-MM-DD') AS d
    FROM customer, orders, lineitem
    WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
      AND o_orderdate < '1995-03-15'::date AND l_shipdate > '1995-03-15'::date
    GROUP BY o_orderkey, o_orderdate
    QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(l_extendedprice * (1 - l_discount)) DESC, o_orderdate) <= 5
    ORDER BY revenue DESC, o_orderdate" \
  "SELECT o_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, strftime(o_orderdate, '%Y-%m-%d')
    FROM customer, orders, lineitem
    WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
      AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15'
    GROUP BY o_orderkey, o_orderdate ORDER BY revenue DESC, o_orderdate LIMIT 5"
expect "semi-structured data and FLATTEN" "SET
a|0
b|1" rita rita-password-1 "SET sql_dialect = 'snowflake'; SELECT f.value:name::string, f.index FROM TABLE(FLATTEN(input => PARSE_JSON('[{\"name\": \"a\"}, {\"name\": \"b\"}]'))) f"
expect_error "a construct it cannot translate is refused" 0A000 rita rita-password-1 "SET sql_dialect = 'snowflake'; SELECT * FROM @my_stage"
# A translation reaches only what its user may: a reader cannot write.
expect_error "a translated write is checked as written in DuckDB's dialect" 42501 rita rita-password-1 \
  "SET sql_dialect = 'snowflake'; CREATE OR REPLACE TRANSIENT TABLE sf_e2e AS SELECT IFF(true, 1, 2) AS x"
# A view created in Snowflake's dialect works in every session.
expect "a view in Snowflake's dialect" "SET
CREATE VIEW" igloo "$admin_password" \
  "SET sql_dialect = 'snowflake'; CREATE OR REPLACE VIEW sf_e2e_view AS SELECT DATE_TRUNC('month', '2024-03-14'::date) AS m, ARRAY_SIZE(ARRAY_CONSTRUCT(1, 2)) AS n"
expect "read from DuckDB's dialect" "2024-03-01|2" rita rita-password-1 "SELECT m, n FROM sf_e2e_view"
psql_admin -c "DROP VIEW sf_e2e_view" >/dev/null

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
# Notebooks: SQLAlchemy, pandas and polars, and how fast each pulls a large
# result.
if docker run --rm --network "$network" -e PGHOST=igloo -e PGUSER=walt -e PGPASSWORD=walt-password-1 \
  -v "$root/scripts/clients:/clients:ro" python:3.12-slim sh -c "
    pip install -q --disable-pip-version-check --root-user-action=ignore 'psycopg[binary]==3.3.6' 'sqlalchemy==2.1.4' \
      'pandas==3.0.6' 'polars==2.0.0' 'pyarrow==26.0.0' 'connectorx==0.4.6' 'adbc-driver-postgresql==1.12.0' >/dev/null &&
    python /clients/notebook_test.py" >"$work/notebook.out" 2>&1; then
  pass "SQLAlchemy, pandas and polars"
  grep "rows/s" "$work/notebook.out" | sed 's/^  notebook: /        /'
else
  fail "notebooks: $(tail -5 "$work/notebook.out")"
fi
# dbt with the standard dbt-postgres adapter, as a writer (see the
# project in scripts/clients/dbt).
if docker run --rm --network "$network" -e PGHOST=igloo -e PGUSER=walt -e PGPASSWORD=walt-password-1 \
  -v "$root/scripts/clients:/clients:ro" python:3.12-slim sh -c "
    pip install -q --disable-pip-version-check --root-user-action=ignore 'dbt-core==1.12.6' 'dbt-postgres==1.11.0' >/dev/null &&
    python /clients/dbt_test.py" >"$work/dbt.out" 2>&1; then
  pass "dbt (dbt-postgres 1.11): seeds, views, tables, incremental merge, delete+insert and append, snapshots, tests, docs, freshness"
else
  fail "dbt: $(tail -15 "$work/dbt.out")"
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
for i in "${!pids[@]}"; do
  if wait "${pids[$i]}"; then
    ok=$((ok + 1))
  else
    echo "  insert $((i + 1)) failed: $(tr '\n' ' ' <"$work/insert-$((i + 1)).out" | cut -c1-300)"
  fi
done
expect "10 concurrent inserts all commit" "10" igloo "$admin_password" "SELECT count(*) FROM contended"
# Sessions renaming tables in one schema at once, as dbt's threads do.
psql_admin -c "CREATE SCHEMA renames" >/dev/null
pids=()
for i in 1 2 3 4; do
  for round in 1 2 3; do
    printf '%s\n' "CREATE TABLE renames.t${i}_new AS SELECT $round AS v;" "ALTER TABLE IF EXISTS renames.t$i RENAME TO t${i}_old;" \
      "ALTER TABLE renames.t${i}_new RENAME TO t$i;" "DROP TABLE IF EXISTS renames.t${i}_old CASCADE;"
  done | psql_admin -v ON_ERROR_STOP=1 >"$work/rename-$i.out" &
  pids+=($!)
done
ok=0
for pid in "${pids[@]}"; do wait "$pid" && ok=$((ok + 1)); done
if [ "$ok" = 4 ]; then pass "concurrent renames in one schema"; else fail "concurrent renames: $(grep -h ERROR "$work"/rename-*.out | head -3)"; fi
# A dropped _old table can stay behind: DuckDB-Iceberg's shared list of the
# schema's tables can miss it (see docs/integrations.md, dbt).
expect "leave every table in place" "4|3" igloo "$admin_password" \
  "SELECT count(*), (SELECT min(v) FROM renames.t1) FROM information_schema.tables
   WHERE table_schema = 'renames' AND table_name IN ('t1', 't2', 't3', 't4')"

echo "replace, views and comments"
old_copy_files="$(psql_admin -c "SELECT string_agg(chr(39) || file_path || chr(39), ',') FROM iceberg_metadata(lake.public.e2e_copy)")"
expect "the table's files are in the bucket" "t" igloo "$admin_password" \
  "SELECT count(*) > 0 FROM glob('s3://$bucket/warehouse/**') WHERE file IN ($old_copy_files)"
expect "writer replaces a table with a new schema" "CREATE TABLE" walt walt-password-1 \
  "CREATE OR REPLACE TABLE e2e_copy AS SELECT range AS n, chr(97 + range::INT) AS letter FROM range(3)"
expect "replaced table has the new rows" "3|b" walt walt-password-1 "SELECT count(*), min(letter) FILTER (WHERE n = 1) FROM e2e_copy"
# A table dropped, as the old one is, that no clone reads has its files
# deleted with it: no table, and so no erasure request, reaches them.
expect_soon "the replaced table's files go with it" "0" igloo "$admin_password" \
  "SELECT count(*) FROM glob('s3://$bucket/warehouse/**') WHERE file IN ($old_copy_files)" 120
# In a transaction block, a change to the catalog commits the block first,
# as in Snowflake; dbt builds tables so. Statement by statement, as psycopg2
# sends them.
psql_as walt walt-password-1 -c "CREATE TABLE e2e_tx (x INTEGER)" >/dev/null
printf '%s\n' "BEGIN;" "INSERT INTO e2e_tx VALUES (1);" "CREATE TABLE e2e_tx_new AS SELECT x + 1 AS x FROM e2e_tx;" \
  "ALTER TABLE e2e_tx RENAME TO e2e_tx_old;" "ALTER TABLE e2e_tx_new RENAME TO e2e_tx;" "INSERT INTO e2e_tx VALUES (10);" \
  "ROLLBACK;" | psql_as walt walt-password-1 >/dev/null
expect "catalog changes in a block commit what came before them" "1|2" walt walt-password-1 \
  "SELECT (SELECT count(*) FROM e2e_tx_old), (SELECT max(x) FROM e2e_tx)"
printf '%s\n' "BEGIN;" "CREATE OR REPLACE TABLE e2e_tx AS SELECT 3 AS x;" "ROLLBACK;" | psql_as walt walt-password-1 >/dev/null
expect "replace inside a block" "3" walt walt-password-1 "SELECT max(x) FROM e2e_tx"
got="$(printf '%s\n' "BEGIN;" "INSERT INTO e2e_tx VALUES (4);" "SELECT * FROM e2e_missing;" "INSERT INTO e2e_tx VALUES (5);" \
  "COMMIT;" | psql_as walt walt-password-1 -v ON_ERROR_STOP=0)" || true
if grep -q "25P02" <<<"$got" && grep -q "^ROLLBACK$" <<<"$got"; then
  pass "a failed block refuses statements and rolls back at COMMIT"
else
  fail "failed block: got [$got]"
fi
expect "so nothing of it is kept" "1" walt walt-password-1 "SELECT count(*) FROM e2e_tx"
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
expect "a view in another schema reads a table by schema" "CREATE SCHEMA
CREATE TABLE
CREATE VIEW" walt walt-password-1 \
  "CREATE SCHEMA e2e_views; CREATE TABLE e2e_views.t AS SELECT 7 AS v; CREATE VIEW e2e_views.sevens AS SELECT v FROM e2e_views.t"
expect "and is found by schema and on the search path" "7
SET
7" rita rita-password-1 "SELECT v FROM e2e_views.sevens; SET search_path = e2e_views; SELECT v FROM sevens"
expect "replace a view with another table's columns" "CREATE VIEW" walt walt-password-1 \
  "CREATE OR REPLACE VIEW e2e_big AS SELECT n, letter FROM lake.public.e2e_copy"
expect "the replaced view reads the other table" "3|c" rita rita-password-1 "SELECT count(*), max(letter) FROM e2e_big"
expect "and back to columns and SQL it had before" "CREATE VIEW" walt walt-password-1 \
  "CREATE OR REPLACE VIEW e2e_big (big_id) AS SELECT id FROM lake.public.e2e WHERE amount > 0"
expect "the view reads the first table again" "3" rita rita-password-1 "SELECT count(*) FROM (SELECT big_id FROM e2e_big)"
expect "rename a view" "ALTER VIEW" walt walt-password-1 "ALTER VIEW e2e_big RENAME TO e2e_huge"
expect "and back with ALTER TABLE, as dbt does" "ALTER TABLE" walt walt-password-1 "ALTER TABLE e2e_huge RENAME TO e2e_big"
expect "the renamed view reads the same rows" "3" rita rita-password-1 "SELECT count(*) FROM e2e_big"
expect_error "the old name is gone" 42P01 rita rita-password-1 "SELECT count(*) FROM e2e_huge"
expect_error "writers cannot change the local view copies" 42501 walt walt-password-1 "DROP VIEW memory.public.e2e_big"
expect_error "nor replace one with a table" 42501 walt walt-password-1 "CREATE OR REPLACE TABLE lake.public.e2e_big AS SELECT 1 AS x"
expect "but build tables from views by any name" "CREATE TABLE" walt walt-password-1 \
  "CREATE TABLE e2e_from_view AS SELECT * FROM lake.public.e2e_big"
expect "comment on a table" "COMMENT" walt walt-password-1 "COMMENT ON TABLE e2e IS 'Test rows | :tag:e2e'"
expect "comment on a column" "COMMENT" walt walt-password-1 "COMMENT ON COLUMN e2e.amount IS 'In dollars'"
expect_error "reader cannot comment" 42501 rita rita-password-1 "COMMENT ON TABLE e2e IS 'x'"
expect "comments in obj_description and col_description" "Test rows | :tag:e2e|In dollars" rita rita-password-1 \
  "SELECT obj_description(c.oid, 'pg_class'), col_description(c.oid, a.attnum)
   FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid WHERE c.relname = 'e2e' AND a.attname = 'amount'"
expect "and in information_schema, for every session" "Test rows | :tag:e2e|In dollars" rita rita-password-1 \
  "SELECT t.table_comment, c.column_comment FROM information_schema.tables t
   JOIN information_schema.columns c USING (table_catalog, table_schema, table_name)
   WHERE t.table_name = 'e2e' AND c.column_name = 'amount'"

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
expect_soon "tables an engine creates are owned by its user" "ice_created|walt
ice_staged|walt" igloo "$admin_password" \
  "SELECT table_name, table_owner FROM igloo.tables WHERE table_name IN ('ice_created', 'ice_staged') ORDER BY 1"

echo "data catalog and lineage"
# walt builds a small mart; cleo, whose role is none, may read one table of
# it, mart.daily, and must learn nothing of the others.
if got="$(psql_as walt walt-password-1 \
  -c "CREATE SCHEMA mart" \
  -c "CREATE TABLE mart.raw_spend (client VARCHAR, day DATE, spend DECIMAL(10,2))" \
  -c "INSERT INTO mart.raw_spend VALUES ('acme', DATE '2026-10-01', 10.00), ('globex', DATE '2026-10-01', 20.00)" \
  -c "CREATE TABLE mart.secret_margin (client VARCHAR, margin DECIMAL(4,2))" \
  -c "INSERT INTO mart.secret_margin VALUES ('acme', 0.20), ('globex', 0.30)" \
  -c "CREATE TABLE mart.daily AS SELECT client, day, sum(spend) AS spend FROM mart.raw_spend GROUP BY ALL" \
  -c "CREATE TABLE mart.profit AS SELECT d.client, d.spend * m.margin AS profit FROM mart.daily d JOIN mart.secret_margin m USING (client)" \
  -c "COMMENT ON SCHEMA mart IS 'Reporting mart'" \
  -c "COMMENT ON TABLE mart.daily IS 'Spend per client and day'" \
  -c "COMMENT ON COLUMN mart.daily.spend IS 'In USD'" \
  -c "COMMENT ON TABLE mart.secret_margin IS 'Negotiated margins'")"; then
  pass "a writer builds and describes a mart"
else
  fail "building the mart: $got"
fi
psql_admin -c "CREATE USER cleo WITH PASSWORD 'cleo-password-1' ROLE none" -c "GRANT SELECT ON TABLE mart.daily TO cleo" >/dev/null
psql_as cleo cleo-password-1 -c "SELECT count(*) FROM mart.daily" -c "SELECT count(*) FROM mart.daily" >/dev/null
expect_soon "owners, descriptions and sizes" "daily|walt|user|Spend per client and day|2|t|t" igloo "$admin_password" \
  "SELECT table_name, table_owner, owner_type, comment, row_count, bytes > 0, last_altered > now() - INTERVAL 1 HOUR
   FROM igloo.tables WHERE table_schema = 'mart' AND table_name = 'daily'"
expect "schema comments in Iceberg" "mart|walt|Reporting mart" igloo "$admin_password" \
  "SELECT schema_name, schema_owner, comment FROM igloo.schemata WHERE schema_name = 'mart'"
expect "column comments" "spend|In USD" igloo "$admin_password" \
  "SELECT column_name, comment FROM igloo.columns WHERE table_schema = 'mart' AND table_name = 'daily' AND comment IS NOT NULL"
expect "information_schema has owners and sizes" "daily|walt|Spend per client and day|2" igloo "$admin_password" \
  "SELECT table_name, table_owner, comment, row_count FROM information_schema.tables WHERE table_schema = 'mart' AND table_name = 'daily'"
expect_soon "lineage from the history" "raw_spend|daily|CREATE TABLE|walt
daily|profit|CREATE TABLE|walt
secret_margin|profit|CREATE TABLE|walt" igloo "$admin_password" \
  "SELECT upstream_name, downstream_name, last_statement_type, last_user FROM igloo.lineage
   WHERE downstream_schema = 'mart' ORDER BY downstream_name, upstream_name"
expect_soon "usage from the history" "daily|3|1|2" igloo "$admin_password" \
  "SELECT table_name, reads_30d, writes_30d, users_30d FROM igloo.table_usage WHERE table_schema = 'mart' AND table_name = 'daily'"
expect "a user with one grant sees that table" "mart|daily|walt|Spend per client and day" cleo cleo-password-1 \
  "SELECT table_schema, table_name, table_owner, comment FROM igloo.tables"
expect "and its lineage, the other ends unnamed" "(hidden)|daily|-
daily|(hidden)|-" cleo cleo-password-1 \
  "SELECT coalesce(upstream_name, '(hidden)'), coalesce(downstream_name, '(hidden)'), coalesce(last_user, '-')
   FROM igloo.lineage ORDER BY 1, 2"
expect "and only its usage" "daily|3" cleo cleo-password-1 "SELECT table_name, reads_30d FROM igloo.table_usage"
got="$(psql_as cleo cleo-password-1 -c "SELECT * FROM igloo.tables" -c "SELECT * FROM igloo.schemata" \
  -c "SELECT * FROM igloo.columns" -c "SELECT * FROM igloo.lineage" -c "SELECT * FROM igloo.table_usage" \
  -c "SELECT * FROM igloo_comments()" -c "SELECT * FROM pg_description" -c "SHOW OBJECTS" \
  -c "SELECT * FROM igloo.tag_references" -c "SELECT * FROM igloo.policy_references" \
  -c "SELECT table_owner, comment FROM information_schema.tables WHERE table_owner IS NOT NULL OR comment IS NOT NULL")" || true
if grep -qi "raw_spend\|secret_margin\|profit\|Negotiated\|e2e\|contacts\|pii" <<<"$got"; then
  fail "the catalog shows a user with one grant what they may not see: $got"
else
  pass "nothing else leaks to a user with one grant"
fi
psql_admin -c "CREATE ROLE mart_owners" -c "GRANT mart_owners TO cleo" >/dev/null
expect "an owner hands a table to a role" "ALTER TABLE" walt walt-password-1 "ALTER TABLE mart.daily OWNER TO mart_owners"
expect "whose members may describe it" "COMMENT" cleo cleo-password-1 "COMMENT ON TABLE mart.daily IS 'Spend per client and day, in USD'"
expect_error "but owning grants nothing more" 42501 cleo cleo-password-1 "INSERT INTO mart.daily VALUES ('x', DATE '2026-10-02', 1)"
expect_error "nor describes what they do not own" 42501 cleo cleo-password-1 "COMMENT ON TABLE mart.profit IS 'x'"
got="$(psql_as cleo cleo-password-1 -c "SHOW OBJECTS")" || true
if [ "$(wc -l <<<"$got" | tr -d ' ')" = 2 ] && grep -q "^mart|daily|TABLE|mart_owners|role|Spend per client and day, in USD|2|" <<<"$got"; then
  pass "SHOW OBJECTS"
else
  fail "SHOW OBJECTS: $got"
fi
login="$(curl -sS -c "$work/cleo.cookies" -H 'Content-Type: application/json' \
  -d '{"user":"cleo","password":"cleo-password-1"}' "$console/api/login")"
cleo_csrf="$(sed -n 's/.*"csrf":"\([^"]*\)".*/\1/p' <<<"$login")"
got="$(curl -sS -b "$work/cleo.cookies" "$console/api/catalog/search?q=spend")"
if grep -q '"name":"daily"' <<<"$got" && ! grep -qi "raw_spend\|profit\|margin" <<<"$got"; then
  pass "console search shows a user only what they may see"
else
  fail "console search: $got"
fi
got="$(curl -sS -b "$work/cleo.cookies" "$console/api/catalog/object?catalog=lake&schema=mart&name=daily")"
if grep -q '"label":"1 hidden upstream table"' <<<"$got" && grep -q '"label":"1 hidden downstream table"' <<<"$got" &&
  grep -q '"can_comment":true' <<<"$got" && ! grep -qi "raw_spend\|profit\|margin" <<<"$got"; then
  pass "a console table page counts hidden tables, never names them"
else
  fail "console table page: $got"
fi
code="$(curl -s -o /dev/null -w '%{http_code}' -b "$work/cleo.cookies" "$console/api/catalog/object?catalog=lake&schema=mart&name=secret_margin")"
if [ "$code" = 404 ]; then pass "a table one may not see has no page"; else fail "console page of a hidden table: $code"; fi
# The page's reads are in the history, under a name the history page
# leaves out by default.
expect_soon "the console's own lookups are recorded under their own name" "t" igloo "$admin_password" \
  "SELECT count(*) >= 8 FROM igloo.query_history WHERE user_name = 'cleo' AND application_name = 'Igloo console lookup'"
got="$(curl -sS -b "$work/cleo.cookies" -H 'Content-Type: application/json' -H "X-CSRF-Token: $cleo_csrf" \
  -d '{"kind":"column","schema":"mart","name":"daily","column":"day","comment":"The day spent"}' "$console/api/catalog/comment")"
if grep -q '"tag":"COMMENT"' <<<"$got"; then pass "an owner describes a column in the console"; else fail "console comment: $got"; fi
expect "the console's comment is the column's" "The day spent" igloo "$admin_password" \
  "SELECT comment FROM igloo.columns WHERE table_schema = 'mart' AND table_name = 'daily' AND column_name = 'day'"
login="$(console_post /api/login '{"user":"walt","password":"walt-password-1"}')"
got="$(curl -sS -b "$jar" "$console/api/catalog/object?catalog=lake&schema=mart&name=profit")"
if grep -q '"label":"mart.secret_margin"' <<<"$got" && grep -q '"label":"mart.daily"' <<<"$got"; then
  pass "a writer's table page draws the whole lineage"
else
  fail "a writer's table page: $got"
fi
lineage_event=""
for _ in $(seq 30); do
  lineage_event="$(docker logs "$project-lineage" 2>&1 | grep '"name":"lake.mart.profit"' | head -1)" || true
  [ -n "$lineage_event" ] && break
  sleep 2
done
if grep -q '"eventType":"COMPLETE"' <<<"$lineage_event" && grep -q '"name":"lake.mart.secret_margin"' <<<"$lineage_event" &&
  grep -q '"outputs":\[{"namespace":"igloo","name":"lake.mart.profit"}\]' <<<"$lineage_event"; then
  pass "an OpenLineage event for the statement that made a table"
else
  fail "OpenLineage event: [$lineage_event]"
fi
if docker logs "$project-lineage" 2>&1 | grep -qi '"sql"\|0\.30\|globex'; then
  fail "an OpenLineage event carries SQL or data"
else
  pass "OpenLineage events carry neither SQL nor data"
fi

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
# Two exclusions: everything outside the warehouse, and the rows workers
# exchange under it.
expect "the read cache still caches only the warehouse" "2" igloo "$admin_password" \
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
psql_as nora nora-password-1 -c "SET statement_timeout = '30s'" -c "$slow" >/dev/null 2>&1 &
holder=$!
# On a busy host the holder can take a while to start: until it holds the
# slot, a second statement just runs, so try again while the holder lives.
for _ in $(seq 30); do
  got="$(psql_as nora nora-password-1 -c "SET statement_timeout = '1s'" -c "SELECT 1")" || true
  if grep -q "behind the limit of 1 statement at once for role analysts" <<<"$got" || ! kill -0 "$holder" 2>/dev/null; then
    break
  fi
  sleep 2
done
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
file_url="$(sed -n 's/.*"url":"\([^"]*\)".*/\1/p' <<<"$got" | sed -n 1p)"
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
expect "the catalog lists it, with its provider and creator" "lake|EXTERNAL TABLE|self|igloo|3" nora nora-password-1 \
  "SELECT table_catalog, table_type, provider, table_owner,
     (SELECT count(*) FROM igloo.columns WHERE table_schema = 'partners' AND table_name = 'spend')
   FROM igloo.tables WHERE table_schema = 'partners' AND table_name = 'spend'"
psql_admin -c "CREATE TABLE partners.spend_by_client AS SELECT client, sum(spend) AS spend FROM partners.spend GROUP BY 1" >/dev/null
expect_soon "its lineage" "TABLE|partners|spend" igloo "$admin_password" \
  "SELECT upstream_type, upstream_schema, upstream_name FROM igloo.lineage WHERE downstream_name = 'spend_by_client'"
expect_error "but not with a table not granted" 42501 nora nora-password-1 \
  "SELECT count(*) FROM partners.spend, (SELECT max(n) FROM lake.public.contended)"
# The view's SQL, which anyone can list, names the proxy's file URLs.
proxy_url="$(psql_as nora nora-password-1 -c "SELECT regexp_extract(sql, '(http://127[.]0[.]0[.]1[^'']*)', 1)
  FROM duckdb_views() WHERE view_name = 'spend'")" || true
if [[ "$proxy_url" == http://127.0.0.1:* ]]; then pass "the external table's view lists proxy URLs"; else fail "proxy URL: $proxy_url"; fi
expect_error "which users with role none cannot read directly" 42501 nora nora-password-1 \
  "SELECT count(*) FROM read_parquet('$proxy_url')"
psql_admin -c "CREATE TABLE partners.spend_files AS SELECT * FROM read_parquet('$proxy_url')" >/dev/null
expect_soon "the history keeps the proxy's URLs out of the locations read" "lake.partners.spend_files|t" igloo "$admin_password" \
  "SELECT tables_written[1], locations_read IS NULL FROM igloo.query_history
   WHERE query_text LIKE 'CREATE TABLE partners.spend_files%' AND outcome = 'ok'"
expect "and out of the lineage" "0" igloo "$admin_password" \
  "SELECT count(*) FROM igloo.lineage WHERE upstream_name LIKE '%127.0.0.1%'"
psql_admin -c "DROP TABLE partners.spend_files" -c "DROP TABLE partners.spend_by_client" >/dev/null
psql_admin -c "INSERT INTO shared_spend VALUES (100, 'client_9', 10.00)" >/dev/null
expect_soon "the external table follows new versions" "31" rita rita-password-1 "SELECT count(*) FROM partners.spend"
psql_admin -c "DELETE FROM shared_spend WHERE id < 10" >/dev/null
expect_soon "and rows deleted from the shared table" "21|10" rita rita-password-1 "SELECT count(*), min(id) FROM partners.spend"

# Policies (crm.contacts has some, from above): recipients cannot apply
# them, so tables with policies are neither shared nor served.
warehouse_root="s3://$(grep '^S3_BUCKET=' "$env_file" | cut -d= -f2)/warehouse"
expect_error "a table with policies cannot be shared" 0A000 igloo "$admin_password" "ALTER SHARE e2e_share ADD TABLE crm.contacts"
psql_admin -c "CREATE MATERIALIZED VIEW crm.contacts_shared AS SELECT id, client_id, email FROM crm.contacts" >/dev/null
expect "but a materialized view of what its creator sees can" "ALTER SHARE" igloo "$admin_password" \
  "ALTER SHARE e2e_share ADD TABLE crm.contacts_shared"
got="$(ds -X POST -d '{}' "$sharing/shares/e2e_share/schemas/crm/tables/contacts_shared/query")" || true
mv_url="$(sed -n 's/.*"url":"\([^"]*\)".*/\1/p' <<<"$got" | sed -n 1p)"
[ -z "$mv_url" ] || curl -sS -o "$work/mv.parquet" "$mv_url" || true
if grep -q 'numRecords\\":2' <<<"$got" && [ -s "$work/mv.parquet" ] && ! grep -qa 'ann@acme' "$work/mv.parquet"; then
  pass "and its files hold masked values"
else
  fail "a shared materialized view: $got"
fi
psql_admin -c "ALTER SHARE e2e_share REMOVE TABLE crm.contacts_shared" >/dev/null
psql_admin -c "DROP MATERIALIZED VIEW crm.contacts_shared" >/dev/null
expect_error "external tables take no policies" 42809 igloo "$admin_password" \
  "ALTER TABLE partners.spend ALTER COLUMN client SET MASKING POLICY mask_email"
expect_error "readers cannot read external tables' files while tables have policies" 42501 rita rita-password-1 \
  "SELECT count(*) FROM read_parquet('$proxy_url')"
expect_error "nor the copies of shared files with deletes applied" 42501 rita rita-password-1 \
  "SELECT count(*) FROM read_parquet('$warehouse_root/.igloo/sharing/*/*.parquet')"
got="$(ds -X POST -d '{}' "$sharing/shares/e2e_share/schemas/public/tables/shared_spend/query")" || true
file_url="$(sed -n 's/.*"url":"\([^"]*\)".*/\1/p' <<<"$got" | sed -n 1p)"
psql_admin -c "ALTER TABLE shared_spend ALTER COLUMN client SET MASKING POLICY mask_email" >/dev/null
status="$(curl -s -o "$work/refused.json" -w '%{http_code}' -X POST -H "Authorization: Bearer $token" -d '{}' \
  "$sharing/shares/e2e_share/schemas/public/tables/shared_spend/query")"
if [ "$status" = 403 ] && grep -q 'masking or row access policies' "$work/refused.json" && ! grep -q mask_email "$work/refused.json"; then
  pass "a shared table that gains a policy is refused, and the recipient is told why"
else
  fail "a shared table with a policy: $status $(cat "$work/refused.json")"
fi
status="$(curl -s -o /dev/null -w '%{http_code}' -r 0-3 "$file_url")"
if [ "$status" = 403 ]; then pass "and so are its files listed before"; else fail "a file listed before the policy: $status"; fi
expect_soon "the history names the policies that stopped the recipient" '["mask_email"]|denied|42501' igloo "$admin_password" \
  "SELECT policies, outcome, sqlstate FROM igloo.query_history WHERE user_name = 'recipient:e2e_partner'
   AND policies IS NOT NULL ORDER BY start_time DESC LIMIT 1"
refused=""
for _ in $(seq 1 30); do
  if ! psql_as rita rita-password-1 -c "SELECT count(*) FROM partners.spend" >/dev/null 2>&1; then refused=1; break; fi
  sleep 2
done
if [ -n "$refused" ]; then pass "an external table of it stops reading"; else fail "an external table read a table with a policy"; fi
psql_admin -c "ALTER TABLE shared_spend ALTER COLUMN client UNSET MASKING POLICY" >/dev/null
expect_soon "without the policy, it is served again" "21|10" rita rita-password-1 "SELECT count(*), min(id) FROM partners.spend"
# Recipients' tokens and Igloo's access tokens do not stand in for each other.
status="$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $bot_token" "$sharing/shares")"
if [ "$status" = 401 ]; then pass "an Igloo access token is not a recipient's token"; else fail "an access token at the sharing endpoint: $status"; fi
if got="$(psql_as rita "$token" -c "SELECT 1")"; then
  fail "a recipient's token signed in to SQL"
elif grep -q 'password authentication failed' <<<"$got"; then
  pass "nor is a recipient's token a SQL password"
else
  fail "a recipient's token as a password: $got"
fi
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
expect "writes after compaction" "INSERT 0 1
6" igloo "$admin_password" \
  "INSERT INTO fragmented VALUES (7, 'v7', 7.25); SELECT count(*) FROM fragmented"

echo "clones, branches and tags"
psql_admin -c "CREATE SCHEMA dev" >/dev/null
# The tables that have data files: a clone adds none, as it has no files
# of its own until it is written to. (Tables dropped earlier can lose
# theirs meanwhile, as the catalog purges them.)
data_dirs="$(psql_admin -c "SELECT string_agg(DISTINCT chr(39) || regexp_extract(file, '^(.*)/data/', 1) || chr(39), ',')
  FROM glob('s3://$bucket/warehouse/**/*.parquet')")"
tables_with_data="SELECT count(DISTINCT regexp_extract(file, '^(.*)/data/', 1)) FROM glob('s3://$bucket/warehouse/**/*.parquet')
  WHERE regexp_extract(file, '^(.*)/data/', 1) NOT IN ($data_dirs)"
data_tables=0
expect "clone a table" "CREATE TABLE" walt walt-password-1 "CREATE TABLE dev.lineitem CLONE lineitem"
expect "the clone has every row" "600572" walt walt-password-1 "SELECT count(*) FROM dev.lineitem"
expect "it reads the source's files" "t" igloo "$admin_password" \
  "SELECT (SELECT list(file_path ORDER BY file_path) FROM iceberg_metadata(lake.dev.lineitem))
        = (SELECT list(file_path ORDER BY file_path) FROM iceberg_metadata(lake.public.lineitem))"
expect "cloning wrote no data files" "$data_tables" igloo "$admin_password" "$tables_with_data"
expect_soon "cloning takes moments" "t" igloo "$admin_password" \
  "SELECT max(duration_ms) < 5000 FROM igloo.query_history WHERE query_text = 'CREATE TABLE dev.lineitem CLONE lineitem' AND outcome = 'ok'"
expect_soon "the history names the clone's source and target" '["lake.public.lineitem"]|["lake.dev.lineitem"]' igloo "$admin_password" \
  "SELECT tables_read, tables_written FROM igloo.query_history WHERE query_text = 'CREATE TABLE dev.lineitem CLONE lineitem'"
expect_soon "the cloner owns the clone" "walt" igloo "$admin_password" \
  "SELECT table_owner FROM igloo.tables WHERE table_schema = 'dev' AND table_name = 'lineitem'"
expect_error "readers cannot clone" 42501 rita rita-password-1 "CREATE TABLE dev.mine CLONE lineitem"
expect_error "a clone's name must be free" 42P07 walt walt-password-1 "CREATE TABLE dev.lineitem CLONE orders"

# Through the Iceberg REST catalog: no purge, no storage credentials for the
# clone, and write credentials for its source that delete nothing.
walt_bearer="$(catalog_token -d client_id=walt -d client_secret=walt-password-1)"
vend() { curl -s -H "Authorization: Bearer $walt_bearer" -H 'X-Iceberg-Access-Delegation: vended-credentials' "$iceberg$1"; }
got="$(vend /v1/namespaces/dev/tables/lineitem)"
if grep -q "is a clone" <<<"$got"; then pass "engines get no storage credentials for a clone"; else fail "credentials for a clone: $got"; fi
code="$(catalog_get "$walt_bearer" /v1/namespaces/dev/tables/lineitem)"
if [ "$code" = 200 ]; then pass "but may load it without"; else fail "loading a clone without credentials: $code"; fi
got="$(curl -s -X DELETE -H "Authorization: Bearer $walt_bearer" "$iceberg/v1/namespaces/public/tables/lineitem?purgeRequested=true")"
if grep -q '"code":409' <<<"$got" && grep -q "dev.lineitem" <<<"$got"; then
  pass "engines may not purge a table that clones read"
else
  fail "purge of a clone's source: $got"
fi
expect "which keeps every row" "600572" walt walt-password-1 "SELECT count(*) FROM lineitem"
# s3_as LOADTABLE CURL_ARGS... sends a request for the object e2e-junk under
# the table's prefix, signed with the credentials of a loadTable response,
# and prints the status.
field() { sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p" <<<"$2" | sed -n 1p; }
s3_as() {
  local creds=$1 prefix
  shift
  prefix="$(field prefix "$creds")"
  docker run --rm --network "$network" curlimages/curl:8.22.0 -s -o /dev/null -w '%{http_code}' "$@" \
    --aws-sigv4 "aws:amz:$(grep '^S3_REGION=' "$env_file" | cut -d= -f2):s3" \
    --user "$(field s3.access-key-id "$creds"):$(field s3.secret-access-key "$creds")" \
    -H "x-amz-security-token: $(field s3.session-token "$creds")" "http://storage:9000/${prefix#s3://}/e2e-junk"
}
shared="$(vend /v1/namespaces/public/tables/lineitem)"
alone="$(vend /v1/namespaces/public/tables/contended)"
got="$(s3_as "$alone" -X PUT --data-binary junk) $(s3_as "$alone" -X DELETE)"
got="$got $(s3_as "$shared" -X PUT --data-binary junk) $(s3_as "$shared" -X DELETE)"
if [ "$got" = "200 204 200 403" ]; then
  pass "write credentials for a table that clones read delete nothing"
else
  fail "PUT and DELETE with write credentials, without and with clones: $got"
fi

# A clone of a table maintenance compacts and expires, and the other way
# round: neither loses a row. Once the clone's maintenance has replaced
# all it read of the source, the clone stops sharing the source's files,
# and dropping the source, which then purges its files, leaves the clone
# whole. The clone is made while the source has too few files to compact
# (5), so that it reads no manifest of the source's compaction, which
# lists only files it removed: the clone would keep that manifest, and
# share the source's files, for good.
psql_admin -c "CREATE TABLE clone_src (id INTEGER, v VARCHAR)" >/dev/null
for i in 1 2 3 4; do psql_admin -c "INSERT INTO clone_src VALUES ($i, 'v$i')" >/dev/null; done
psql_admin -c "CREATE TABLE dev.clone_dst CLONE clone_src" >/dev/null
for i in 5 6; do psql_admin -c "INSERT INTO clone_src VALUES ($i, 'v$i')" >/dev/null; done
psql_admin -c "INSERT INTO dev.clone_dst VALUES (7, 'v7')" >/dev/null
psql_admin -c "DELETE FROM clone_src WHERE id = 2" >/dev/null
expect "source and clone diverge" "1,3,4,5,6|1,2,3,4,7" walt walt-password-1 \
  "SELECT (SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM clone_src), (SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM dev.clone_dst)"
psql_admin -c "SELECT * FROM set_iceberg_table_properties(lake.public.clone_src, {'history.expire.max-snapshot-age-ms': '0'})" >/dev/null
compacted="SELECT (SELECT count(*) FROM iceberg_metadata(lake.public.clone_src) WHERE status <> 'DELETED' AND manifest_content = 'DATA') <= 2
  AND (SELECT count(*) FROM iceberg_snapshots(lake.public.clone_src)) = 1"
expect_soon "the source is compacted and expired" "t" igloo "$admin_password" "$compacted"
expect "the clone still reads every row" "1,2,3,4,7" walt walt-password-1 "SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM dev.clone_dst"
psql_admin -c "SELECT * FROM set_iceberg_table_properties(lake.dev.clone_dst, {'history.expire.max-snapshot-age-ms': '0'})" >/dev/null
for i in 8 9 10 11; do psql_admin -c "INSERT INTO dev.clone_dst VALUES ($i, 'v$i')" >/dev/null; done
expect_soon "the clone is compacted and expired" "t" igloo "$admin_password" \
  "SELECT (SELECT count(*) FROM iceberg_snapshots(lake.dev.clone_dst)) = 1 AND (SELECT count(*) FROM dev.clone_dst) = 9"
expect "the source still reads every row" "1,3,4,5,6" walt walt-password-1 "SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM clone_src"
clone_shares() {
  compose exec -T postgres psql -U lakekeeper -d lakekeeper -XAt \
    -c "SELECT count(*) FROM igloo_clones.shares WHERE clone_schema = 'dev' AND clone_name = 'clone_dst'" 2>&1
}
deadline=$((SECONDS + 60))
until [ "$(clone_shares)" = 0 ] || [ "$SECONDS" -ge "$deadline" ]; do sleep 2; done
if [ "$(clone_shares)" = 0 ]; then pass "the clone stops sharing the source's files"; else fail "the clone still shares: $(clone_shares)"; fi
expect "drop the source" "DROP TABLE" walt walt-password-1 "DROP TABLE clone_src"
expect "the clone outlives it" "9" walt walt-password-1 "SELECT count(*) FROM dev.clone_dst"

# A clone carries its source's policies and tags: nora, who may read the
# contacts and create tables in team, sees no more of her clone than of the
# source.
expect "a user with SELECT and CREATE grants clones a protected table" "CREATE TABLE" nora nora-password-1 \
  "CREATE TABLE team.contacts_dev CLONE crm.contacts"
expect "and sees only her rows, masked" "1|acme|***@acme.com|$device" nora nora-password-1 "SELECT * FROM team.contacts_dev"
expect "the clone has the source's policies and tags" "client_rows|ROW ACCESS POLICY||
mask_email|MASKING POLICY|email|
pii_hash|MASKING POLICY|device_id|pii" nora nora-password-1 \
  "SELECT policy_name, policy_kind, ref_column_name, tag_name FROM igloo.policy_references
   WHERE ref_schema_name = 'team' AND ref_entity_name = 'contacts_dev' ORDER BY policy_name"
expect "admins too" "2|***@acme.com" igloo "$admin_password" "SELECT count(*), min(email) FROM team.contacts_dev"
expect "cloning it again under the name is quick" "CREATE TABLE" nora nora-password-1 \
  "CREATE OR REPLACE TABLE team.contacts_dev CLONE crm.contacts AT (TIMESTAMP => now())"
expect "and keeps the policies" "1|***@acme.com" nora nora-password-1 "SELECT count(*), min(email) FROM team.contacts_dev"
got="$(curl -s -H "Authorization: Bearer $(catalog_token -d client_id=nora -d client_secret=nora-password-1)" \
  "$iceberg/v1/namespaces/team/tables/contacts_dev")"
if grep -q "data policies" <<<"$got"; then pass "so engines may not load it either"; else fail "protected clone through the catalog: $got"; fi
expect_error "a user cannot clone what she cannot read" 42501 nora nora-password-1 "CREATE TABLE team.contended CLONE contended"
expect_error "nor where she cannot create tables" 42501 nora nora-password-1 "CREATE TABLE public.contacts_dev CLONE crm.contacts"

psql_admin -c "CREATE TABLE tagged AS SELECT range AS n FROM range(3)" >/dev/null
expect "tag a snapshot" "ALTER TABLE" walt walt-password-1 "ALTER TABLE tagged CREATE TAG v1"
psql_admin -c "INSERT INTO tagged VALUES (3)" >/dev/null
expect "branch at the tag" "ALTER TABLE" walt walt-password-1 "ALTER TABLE tagged CREATE BRANCH audit AT (TAG => 'v1')"
expect "read a tag and a branch" "3|3|4" rita rita-password-1 \
  "SELECT (SELECT count(*) FROM tagged AT (TAG => 'v1')), (SELECT count(*) FROM tagged t AT (BRANCH => 'audit')), (SELECT count(*) FROM tagged)"
got="$(psql_as rita rita-password-1 -c "SHOW BRANCHES ON tagged" | cut -d'|' -f1,2 | tr '\n' ' ')" || true
if [ "$got" = "main|branch audit|branch v1|tag " ]; then pass "SHOW BRANCHES"; else fail "SHOW BRANCHES: $got"; fi
expect_error "readers cannot tag" 42501 rita rita-password-1 "ALTER TABLE tagged CREATE TAG v2"
expect "clone at a tag" "CREATE TABLE" walt walt-password-1 "CREATE TABLE dev.tagged_v1 CLONE tagged AT (TAG => 'v1')"
expect "which has the tag's rows" "3" walt walt-password-1 "SELECT count(*) FROM dev.tagged_v1"
expect_error "a branch moves only ahead" 55000 walt walt-password-1 "ALTER TABLE tagged FAST FORWARD main TO audit"
expect "fast-forward a branch" "ALTER TABLE" walt walt-password-1 "ALTER TABLE tagged FAST FORWARD audit TO main"
expect "to the head of the other" "4" walt walt-password-1 "SELECT count(*) FROM tagged AT (BRANCH => 'audit')"
expect "drop a tag" "ALTER TABLE" walt walt-password-1 "ALTER TABLE tagged DROP TAG v1"
expect_error "a dropped tag is gone" 42704 rita rita-password-1 "SELECT count(*) FROM tagged AT (TAG => 'v1')"

psql_admin -c "CREATE SCHEMA shop" -c "CREATE TABLE shop.daily AS SELECT range AS n FROM range(3)" \
  -c "CREATE VIEW shop.daily_big AS SELECT n FROM lake.shop.daily WHERE n > 1" -c "GRANT SELECT ON SCHEMA shop TO analysts" >/dev/null
expect "clone a schema" "CREATE SCHEMA" walt walt-password-1 "CREATE SCHEMA shop_dev CLONE shop"
expect "with its tables, views and schema grants" "3|1" nora nora-password-1 \
  "SELECT (SELECT count(*) FROM shop_dev.daily), (SELECT count(*) FROM lake.shop_dev.daily_big)"
expect_error "readers cannot clone schemas" 42501 rita rita-password-1 "CREATE SCHEMA mine CLONE shop"

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
expect "a task that rebuilds a table" "CREATE TASK" walt walt-password-1 \
  "CREATE TASK e2e_rebuild SCHEDULE '1 hour' AS CREATE OR REPLACE TABLE e2e_rebuilt AS SELECT * FROM range(6)"
expect "EXECUTE TASK" "EXECUTE TASK" walt walt-password-1 "EXECUTE TASK e2e_rebuild"
expect_soon "records the rows its CREATE TABLE AS wrote" "succeeded|6" walt walt-password-1 \
  "SELECT outcome, row_count FROM igloo.task_history WHERE name = 'e2e_rebuild'"

expect "a materialized view" "CREATE MATERIALIZED VIEW" walt walt-password-1 \
  "CREATE MATERIALIZED VIEW e2e_sales_totals AS SELECT count(*) AS orders, sum(amount) AS total FROM e2e_sales"
psql_as walt walt-password-1 -c "INSERT INTO e2e_sales VALUES (12, 'late', 1)" >/dev/null
expect "keeps its rows until refreshed" "7" rita rita-password-1 "SELECT orders FROM e2e_sales_totals"
expect "REFRESH MATERIALIZED VIEW" "REFRESH MATERIALIZED VIEW" walt walt-password-1 "REFRESH MATERIALIZED VIEW e2e_sales_totals"
expect "which replaces them" "8|44.50" rita rita-password-1 "SELECT orders, total FROM e2e_sales_totals"
expect_soon "and the catalog counts its rows once" "1" igloo "$admin_password" \
  "SELECT row_count FROM igloo.tables WHERE table_schema = 'public' AND table_name = 'e2e_sales_totals'"
psql_admin -c "GRANT SELECT ON TABLE e2e_sales_totals TO nora" >/dev/null
expect "reading it needs SELECT on it" "8" nora nora-password-1 "SELECT orders FROM e2e_sales_totals"
expect_error "not on what it reads" 42501 nora nora-password-1 "SELECT count(*) FROM e2e_sales"

echo "alerts and notifications"
# The receiver prints each request: its path, whether its signature checks
# out with the integration's SECRET, and its body.
hooks_py=$(cat <<'EOF'
import hashlib, hmac, http.server
SECRET = b"e2e-signing-secret-123"
class Hook(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        ts = self.headers.get("X-Igloo-Timestamp", "")
        want = "v1=" + hmac.new(SECRET, ts.encode() + b"." + body, hashlib.sha256).hexdigest()
        signed = hmac.compare_digest(self.headers.get("X-Igloo-Signature", ""), want)
        print(self.path, "signed" if signed else "unsigned", body.decode(), flush=True)
        self.send_response(204)
        self.end_headers()
    def log_message(self, *args):
        pass
http.server.ThreadingHTTPServer(("", 8080), Hook).serve_forever()
EOF
)
docker run -d --name "$project-hooks" --network "$network" --network-alias hooks python:3.12-slim \
  python -u -c "$hooks_py" >/dev/null
for _ in $(seq 1 30); do
  docker exec "$project-hooks" python -c "import socket; socket.create_connection(('localhost', 8080))" >/dev/null 2>&1 && break
  sleep 1
done
hooks() { docker logs "$project-hooks" 2>&1; }
# expect_hook NAME TEXT...: waits up to a minute for a request with every TEXT.
expect_hook() {
  local name=$1 deadline=$((SECONDS + 60)) line
  shift
  while [ "$SECONDS" -lt "$deadline" ]; do
    while IFS= read -r line; do
      local all=1 text
      for text in "$@"; do grep -qF -- "$text" <<<"$line" || all=0; done
      if [ "$all" = 1 ]; then pass "$name"; return; fi
    done < <(hooks)
    sleep 1
  done
  fail "$name: no request with $*; got: $(hooks | tail -5)"
}

expect "an admin creates a webhook integration" "CREATE NOTIFICATION INTEGRATION" igloo "$admin_password" \
  "CREATE NOTIFICATION INTEGRATION e2e_hook TYPE webhook URL 'http://hooks:8080/hook/e2e-s3cret-token' SECRET 'e2e-signing-secret-123'"
got="$(psql_admin -c 'SHOW INTEGRATIONS')" || true
if grep -q '^e2e_hook|webhook|t|http://hooks:8080|' <<<"$got" && ! grep -q 's3cret' <<<"$got"; then
  pass "SHOW INTEGRATIONS hides the URL's token"
else
  fail "SHOW INTEGRATIONS: $got"
fi
expect "so does the query history" "0" igloo "$admin_password" "SELECT count(*) FROM igloo.query_history WHERE query_text LIKE '%s3cret%'"
expect_error "integrations reach only public addresses" 42501 igloo "$admin_password" \
  "CREATE NOTIFICATION INTEGRATION e2e_meta TYPE webhook URL 'https://169.254.169.254/latest/meta-data/'"
expect_error "not the private network" 42501 igloo "$admin_password" \
  "CREATE NOTIFICATION INTEGRATION e2e_db TYPE webhook URL 'https://postgres:5432/'"
expect_error "only admins create integrations" 42501 walt walt-password-1 \
  "CREATE NOTIFICATION INTEGRATION e2e_mine TYPE webhook URL 'https://example.com/hook'"
alert="CREATE ALERT e2e_big_sales SCHEDULE '* * * * *' IF (EXISTS (SELECT id, name, amount FROM e2e_sales WHERE amount >= 10 ORDER BY id))
  THEN NOTIFY e2e_hook MESSAGE 'Big sale: {{name}}' WITH RESULT"
expect_error "alerts need USAGE on the integration" 42501 walt walt-password-1 "$alert"
expect "GRANT USAGE ON INTEGRATION" "GRANT" igloo "$admin_password" "GRANT USAGE ON INTEGRATION e2e_hook TO walt"
expect "an alert" "CREATE ALERT" walt walt-password-1 "$alert"
got="$(psql_as walt walt-password-1 -c 'SHOW ALERTS')" || true
if grep -q "^e2e_big_sales|walt|suspended|\* \* \* \* \*|UTC|" <<<"$got"; then pass "SHOW ALERTS: suspended at first"; else fail "SHOW ALERTS: $got"; fi
expect_error "alerts run at most once a minute" 22023 walt walt-password-1 "ALTER ALERT e2e_big_sales SET SCHEDULE '2 seconds'"
expect "EXECUTE ALERT runs it now, as its owner" "EXECUTE ALERT" igloo "$admin_password" "EXECUTE ALERT e2e_big_sales"
expect_hook "the webhook gets the alert, signed, with its rows" "/hook/e2e-s3cret-token signed" '"name":"e2e_big_sales"' \
  '"message":"Big sale: piped"' '"rows":[["10","piped","10.00"],["11","piped","11.00"]]'
expect "the alert history records it" "alert|walt|manual|triggered|2|e2e_hook" walt walt-password-1 \
  "SELECT alert_type, user_name, trigger, state, row_count, integration FROM igloo.alert_history WHERE alert_name = 'e2e_big_sales'"
expect "others do not see it" "0" rita rita-password-1 "SELECT count(*) FROM igloo.alert_history"
expect "ALTER ALERT RESUME" "ALTER ALERT" walt walt-password-1 "ALTER ALERT e2e_big_sales RESUME"
expect_soon "the alert runs on its schedule" "t" walt walt-password-1 \
  "SELECT count(*) >= 1 FROM igloo.alert_history WHERE alert_name = 'e2e_big_sales' AND trigger = 'schedule' AND state = 'triggered'" 100
expect "ALTER ALERT SUSPEND" "ALTER ALERT" walt walt-password-1 "ALTER ALERT e2e_big_sales SUSPEND"
expect "SYSTEM\$SEND_NOTIFICATION" "CALL" walt walt-password-1 "CALL SYSTEM\$SEND_NOTIFICATION('e2e_hook', 'hello from walt')"
expect_hook "sends the message" '"type":"call"' '"message":"hello from walt"' '"user":"walt"'
expect "built-in alerts for an integration" "ALTER NOTIFICATION INTEGRATION" igloo "$admin_password" \
  "ALTER NOTIFICATION INTEGRATION e2e_hook SET OPERATIONAL_ALERTS = (FAILED_RUNS, LOAD_ERRORS)"
psql_as walt walt-password-1 -c "CREATE TASK e2e_broken SCHEDULE '2 seconds' AS INSERT INTO e2e_missing VALUES (1)" >/dev/null
psql_as walt walt-password-1 -c "ALTER TASK e2e_broken RESUME" >/dev/null
expect_hook "a failed scheduled run is reported" '"type":"builtin"' '"name":"failed_runs"' '"name":"e2e_broken"'
psql_as walt walt-password-1 -c "DROP TASK e2e_broken" >/dev/null

echo "erasure and data retention"
# A person's email and device ID in a table, written over several
# snapshots, the email's SHA-256 in another table, and a copy made from the
# first. A privacy officer, with ERASE ON ACCOUNT and no role of their own,
# erases them. scan_bucket NEEDLES CONTROL proves the result: it reads every
# object in the bucket, Avro manifests decompressed and Parquet files
# decoded, and succeeds when no needle is anywhere and CONTROL, a row that
# stays, is.
erase_email=Erase.Me@Subject.example
erase_device=DEV-ERASE-ME-0001
s3_access_key="$(grep '^S3_ACCESS_KEY_ID=' "$env_file" | cut -d= -f2)"
s3_secret_key="$(grep '^S3_SECRET_ACCESS_KEY=' "$env_file" | cut -d= -f2)"
scan_bucket() {
  docker run --rm --network "$network" -e S3_ENDPOINT=http://storage:9000 -e S3_BUCKET="$bucket" \
    -e S3_ACCESS_KEY_ID="$s3_access_key" -e S3_SECRET_ACCESS_KEY="$s3_secret_key" -e NEEDLES="$1" -e CONTROL="$2" \
    -v "$root/scripts/clients:/clients:ro" python:3.12-slim sh -c "
      pip install -q boto3==1.40.61 pyarrow==26.0.0 >/dev/null 2>&1 && python /clients/scan_bucket.py" 2>&1
}
psql_admin -c "CREATE TABLE crm.people (id INTEGER, email VARCHAR, device_id VARCHAR, note VARCHAR)" \
  -c "INSERT INTO crm.people VALUES (1, 'ann@other.example', 'dev-ann', 'stays'), (2, 'bob@other.example', 'dev-bob', 'stays')" >/dev/null
people_before="$(psql_admin -c "SELECT max_by(snapshot_id, timestamp_ms) FROM iceberg_snapshots(lake.crm.people)")"
psql_admin \
  -c "INSERT INTO crm.people VALUES (3, '$erase_email', 'dev-x', 'goes'), (4, 'cy@other.example', '$erase_device', 'goes'),
        (5, 'di@other.example', 'dev-di', 'stays')" \
  -c "INSERT INTO crm.people VALUES (6, 'eve@other.example', 'dev-eve', 'stays')" \
  -c "CREATE TABLE crm.audience (segment VARCHAR, email_sha256 VARCHAR)" \
  -c "INSERT INTO crm.audience VALUES ('sports', sha256(lower('$erase_email'))), ('sports', sha256('ann@other.example'))" \
  -c "CREATE TABLE crm.people_copy AS SELECT * FROM crm.people" \
  -c "CREATE TABLE crm.untagged (id INTEGER)" >/dev/null
email_hash="$(psql_admin -c "SELECT sha256(lower('$erase_email'))")"
if got="$(psql_admin \
  -c "CREATE TAG person ALLOWED_VALUES 'email', 'email:sha256', 'device_id'" \
  -c "ALTER TABLE crm.people ALTER COLUMN email SET TAG person = 'email'" \
  -c "ALTER TABLE crm.people ALTER COLUMN device_id SET TAG person = 'device_id'" \
  -c "ALTER TABLE crm.audience ALTER COLUMN email_sha256 SET TAG person = 'email:sha256'" \
  -c "ALTER TABLE crm.people_copy ALTER COLUMN email SET TAG person = 'email'" \
  -c "ALTER TABLE crm.people_copy ALTER COLUMN device_id SET TAG person = 'device_id'" \
  -c "CREATE USER pat WITH PASSWORD 'pat-password-1' ROLE none" -c "CREATE ROLE privacy_officer" \
  -c "GRANT ERASE ON ACCOUNT TO privacy_officer" -c "GRANT privacy_officer TO pat")"; then
  pass "tag the columns that hold identifiers, and make a privacy officer"
else
  fail "erasure setup: $got"
fi
# Igloo has the catalog refuse tables of Iceberg format version 3, which
# erasure requests cannot rewrite yet, whatever their names.
if got="$(psql_admin -c "CREATE TABLE crm.v3_probe (id INTEGER) WITH ('format-version' = '3')")"; then
  fail "a table of format version 3 was created"
elif grep -q "not allowed in this warehouse" <<<"$got"; then
  pass "the catalog refuses tables of format version 3"
else
  fail "a table of format version 3: $got"
fi
# A statement that names the person, for the history to forget, and one
# that reads their rows into the read cache.
psql_admin -c "SELECT count(*) FROM crm.people WHERE email = '$erase_email'" -c "SELECT count(*) FROM crm.people WHERE note = 'goes'" >/dev/null
people_files="$(psql_admin -c "SELECT string_agg(chr(39) || file_path || chr(39), ',') FROM iceberg_metadata(lake.crm.people)")"
cache_on=1
curl -s "http://127.0.0.1:$http_port/metrics" | grep -q '^igloo_cache_limit_bytes 0$' && cache_on=0
if [ "$cache_on" = 1 ]; then
  expect "the read cache holds the person's files" "t" igloo "$admin_password" \
    "SELECT count(*) > 0 FROM cache_httpfs_cache_status_query() WHERE original_remote_path IN ($people_files)"
fi
lower_email="$(tr '[:upper:]' '[:lower:]' <<<"$erase_email")"
lower_device="$(tr '[:upper:]' '[:lower:]' <<<"$erase_device")"
# Manifest bounds keep 16 characters of a string.
needles="$(printf '%s\n' "$erase_email" "$erase_device" "${erase_device:0:16}" "$email_hash")"
if got="$(scan_bucket "$needles" di@other.example)"; then
  fail "before erasing, the scan finds nothing: $got"
elif grep -q "[.]parquet (decoded): $lower_email" <<<"$got" && grep -q "[.]avro (.*): $(tr '[:upper:]' '[:lower:]' <<<"${erase_device:0:16}")" <<<"$got"; then
  pass "before erasing, the scan finds the person in data files and manifest bounds"
else
  fail "the scan before erasing: $got"
fi

expect_error "readers cannot erase" 42501 rita rita-password-1 "ERASE SUBJECT WHERE person.email = 'someone@else.example'"
expect_error "nor see the requests" 42501 rita rita-password-1 "SHOW ERASURE REQUESTS"
expect_error "a tag value no column has is refused" 22023 pat pat-password-1 "ERASE SUBJECT WHERE pii.email = 'someone@else.example'"
expect_error "a placeholder identifier is refused" 22023 pat pat-password-1 "ERASE SUBJECT WHERE person.device_id = '00000000-0000-0000-0000-000000000000'"
got="$(psql_as pat pat-password-1 -c "ERASE SUBJECT WHERE person.email = '$lower_email' OR person.device_id = '$lower_device' DRY RUN" |
  awk -F'|' '{printf "%s%s:%s", sep, $1, $4; sep=","}')" || true
requests="$(psql_as pat pat-password-1 -c "SELECT count(*) FROM igloo.erasure_requests")" || true
if [ "$got|$requests" = "crm.audience:1,crm.people:2,crm.people_copy:2|0" ]; then
  pass "a dry run counts the rows it would erase, and makes no request"
else
  fail "dry run: got [$got|$requests]"
fi
request="$(psql_as pat pat-password-1 -c "ERASE SUBJECT WHERE person.email = '  $lower_email' OR person.device_id = '$lower_device'" | cut -d'|' -f1)"
status=""
deadline=$((SECONDS + 240))
while [ "$SECONDS" -lt "$deadline" ]; do
  status="$(psql_as pat pat-password-1 -c "SELECT status FROM igloo.erasure_requests WHERE request_id = '$request'")" || true
  [ "$status" = completed ] && break
  sleep 2
done
if [ "$status" = completed ]; then
  pass "the erasure request completes"
else
  fail "the erasure request: $(psql_admin -c "SELECT status, step, error FROM igloo.erasure_requests WHERE request_id = '$request'")"
fi
expect "its verification finds nothing" "0|0|0|0|0|0|0|0|1" pat pat-password-1 \
  "SELECT v->>'rows', v->>'files', v->>'bounds', v->>'statistics', v->>'storage', v->>'orphans', v->>'sharing', v->>'history', v->>'caches'
   FROM (SELECT verification::JSON AS v FROM igloo.erasure_requests WHERE request_id = '$request')"
expect "it erased from the tagged tables" "crm.audience:1,crm.people:2,crm.people_copy:2" pat pat-password-1 \
  "SELECT string_agg((t->>'table') || ':' || (t->>'rows_deleted'), ',' ORDER BY t->>'table')
   FROM (SELECT unnest(json_extract(tables::JSON, '\$[*]')) AS t FROM igloo.erasure_requests WHERE request_id = '$request')"
expect "and reports the copy as written from one" "crm.people_copy|crm.people|true" pat pat-password-1 \
  "SELECT d->>'table', d->'from'->>0, d->>'erased'
   FROM (SELECT unnest(json_extract(downstream::JSON, '\$[*]')) AS d FROM igloo.erasure_requests WHERE request_id = '$request')"
got="$(psql_as pat pat-password-1 -c "SHOW ERASURE REQUESTS")" || true
if grep -q "^$request|pat|completed|done|" <<<"$got" && ! grep -qi "$lower_email\|$lower_device\|$email_hash" <<<"$got"; then
  pass "SHOW ERASURE REQUESTS, without the identifiers"
else
  fail "SHOW ERASURE REQUESTS: $got"
fi
expect "others see no requests" "0" rita rita-password-1 "SELECT count(*) FROM igloo.erasure_requests"
expect "every other row stays" "1,2,5,6|1,2,5,6|1" igloo "$admin_password" \
  "SELECT (SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM crm.people), (SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM crm.people_copy),
     (SELECT count(*) FROM crm.audience)"
expect "time travel to before the person's data still works" "2" igloo "$admin_password" \
  "SELECT count(*) FROM crm.people AT (VERSION => $people_before)"
expect "the history no longer holds the identifiers" "0" igloo "$admin_password" \
  "SELECT count(*) FROM igloo.query_history WHERE query_text ILIKE '%subject.example%' OR query_text ILIKE '%dev-erase-me%'"
expect "the request is recorded without them" "ERASE SUBJECT WHERE \"person\".\"email\" IN ('***') OR \"person\".\"device_id\" IN ('***')" \
  igloo "$admin_password" "SELECT query_text FROM igloo.query_history WHERE user_name = 'pat' AND statement_type = 'ERASE' AND outcome = 'ok' AND query_text NOT LIKE '%DRY RUN'"
if [ "$cache_on" = 1 ]; then
  expect "the read cache holds none of the deleted files" "0" igloo "$admin_password" \
    "SELECT count(*) FROM cache_httpfs_cache_status_query() WHERE original_remote_path IN ($people_files)
       AND original_remote_path NOT IN (SELECT file FROM glob('s3://$bucket/warehouse/**'))"
fi
if got="$(scan_bucket "$needles" di@other.example)"; then
  pass "no trace of the person in any object of the bucket ($(tail -1 <<<"$got"))"
else
  fail "the bucket after erasing: $got"
fi

# Refreshing a dev clone with CREATE OR REPLACE ... CLONE (A), or a
# schema of clones with CREATE OR REPLACE SCHEMA ... CLONE (F), deletes the
# files of the clones replaced, as a drop does, and only those: their
# sources keep theirs and read in full. The replaced clones held a second
# person's row; nothing of it is left, and a request for that person
# completes.
second="second.person@subject.example"
data_dirs() { psql_admin -c "SELECT DISTINCT regexp_extract(file_path, '^(.*)/data/', 1) FROM iceberg_metadata(lake.$1)" | sort; }
own_dir() { comm -23 <(data_dirs "$1") <(data_dirs "$2") | head -1; } # own_dir CLONE SOURCE
psql_admin -c "CREATE SCHEMA left_a" -c "CREATE SCHEMA left_b" -c "CREATE SCHEMA left_f" >/dev/null
psql_as walt walt-password-1 -c "CREATE TABLE left_a.src (id INTEGER, email VARCHAR)" -c "INSERT INTO left_a.src VALUES (1, 'ann@other.example')" \
  -c "CREATE TABLE left_f.t (id INTEGER, email VARCHAR)" -c "INSERT INTO left_f.t VALUES (1, 'ann@other.example')" >/dev/null
psql_admin -c "ALTER TABLE left_a.src ALTER COLUMN email SET TAG person = 'email'" -c "ALTER TABLE left_f.t ALTER COLUMN email SET TAG person = 'email'" >/dev/null
psql_as walt walt-password-1 -c "CREATE TABLE left_a.dev CLONE left_a.src" -c "INSERT INTO left_a.dev VALUES (2, '$second')" \
  -c "CREATE SCHEMA left_f2 CLONE left_f" -c "INSERT INTO left_f2.t VALUES (2, '$second')" >/dev/null
left_a="$(own_dir left_a.dev left_a.src)"
left_f="$(own_dir left_f2.t left_f.t)"
src_files="SELECT (SELECT count(*) FROM glob('$(data_dirs left_a.src | head -1)/data/**')) || '|' || (SELECT count(*) FROM glob('$(data_dirs left_f.t | head -1)/data/**'))"
src_before="$(psql_admin -c "$src_files")"
expect "the clones' own files hold the second person" "1|1" igloo "$admin_password" \
  "SELECT (SELECT count(*) FROM read_parquet('$left_a/data/*.parquet') WHERE email = '$second') || '|' ||
     (SELECT count(*) FROM read_parquet('$left_f/data/*.parquet') WHERE email = '$second')"
psql_as walt walt-password-1 -c "CREATE OR REPLACE TABLE left_a.dev CLONE left_a.src" -c "CREATE OR REPLACE SCHEMA left_f2 CLONE left_f" >/dev/null
expect "a refresh deletes the files of the clone it replaces" "0|0" igloo "$admin_password" \
  "SELECT (SELECT count(*) FROM glob('$left_a/**')) || '|' || (SELECT count(*) FROM glob('$left_f/**'))"
if got="$(scan_bucket "$second" ann@other.example)"; then
  pass "no trace of the second person in any object of the bucket ($(tail -1 <<<"$got"))"
else
  fail "the bucket after the refreshes: $got"
fi
expect "and none of its source's data files" "$src_before" igloo "$admin_password" "$src_files"
expect "the sources and the new clones read in full" "1|1|1|1" igloo "$admin_password" \
  "SELECT (SELECT count(*) FROM left_a.src) || '|' || (SELECT count(*) FROM left_a.dev) || '|' ||
     (SELECT count(*) FROM left_f.t) || '|' || (SELECT count(*) FROM left_f2.t)"
second_request="$(psql_as pat pat-password-1 -c "ERASE SUBJECT WHERE person.email = '$second'" | cut -d'|' -f1)"
expect_soon "a request for the person they held completes" "completed" pat pat-password-1 \
  "SELECT status FROM igloo.erasure_requests WHERE request_id = '$second_request'" 180

# A table dropped while a clone reads it keeps its files, and the clone
# dropped after leaves them under no table's location (B). They hold a
# third person's row. A request for that person sweeps the warehouse:
# those files are recent, so they may be a table's being made, and it
# waits until they are three days old rather than complete while they
# hold the person.
third="third.person@subject.example"
psql_as walt walt-password-1 -c "CREATE TABLE left_b.src (id INTEGER, email VARCHAR)" \
  -c "INSERT INTO left_b.src VALUES (1, 'ann@other.example'), (2, '$third')" >/dev/null
psql_admin -c "ALTER TABLE left_b.src ALTER COLUMN email SET TAG person = 'email'" >/dev/null
left_b="$(data_dirs left_b.src | head -1)"
psql_as walt walt-password-1 -c "CREATE TABLE left_b.cl CLONE left_b.src" -c "DROP TABLE left_b.src" -c "DROP TABLE left_b.cl" >/dev/null
# Tags stay with dropped tables' names, and refuse requests until unset.
psql_admin -c "ALTER TABLE left_b.src ALTER COLUMN email UNSET TAG person" -c "ALTER TABLE left_b.cl ALTER COLUMN email UNSET TAG person" >/dev/null
third_rows="SELECT count(*) FROM read_parquet('$left_b/data/*.parquet') WHERE email = '$third'"
expect "what the drops leave holds the third person" "1" igloo "$admin_password" "$third_rows"
third_request="$(psql_as pat pat-password-1 -c "ERASE SUBJECT WHERE person.email = '$third'" | cut -d'|' -f1)"
expect_soon "a request waits for files no table owns that hold the person, rather than complete" "waiting|purge" pat pat-password-1 \
  "SELECT status || '|' || step FROM igloo.erasure_requests WHERE request_id = '$third_request'" 180
expect "while it waits, those files are still there" "1" igloo "$admin_password" "$third_rows"

expect_error "writers cannot set a data retention" 42501 walt walt-password-1 "ALTER TABLE crm.people SET (data_retention_days = 1)"
expect_error "a data retention is a day at least" 42601 pat pat-password-1 "ALTER TABLE crm.people SET (data_retention_days = 0)"
expect "a privacy officer sets one on a table with tags" "ALTER TABLE" pat pat-password-1 "ALTER TABLE crm.people SET (data_retention_days = 1)"
expect_error "but not on one without" 42501 pat pat-password-1 "ALTER TABLE crm.untagged SET (data_retention_days = 1)"
expect "everyone sees it" "people|1|pat" rita rita-password-1 "SELECT table_name, data_retention_days, set_by FROM igloo.data_retention"
expect_error "a table with a data retention keeps its name" 42501 igloo "$admin_password" "ALTER TABLE crm.people RENAME TO folks"
expect_error "and a tagged column can be neither renamed nor dropped" 42501 igloo "$admin_password" "ALTER TABLE crm.people DROP COLUMN email"

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

echo "connections to other databases"
# A client's CRM in PostgreSQL and an operational MySQL, on the stack's
# network. Connections are attached when Igloo starts, so they are created
# before the restart below and checked after it.
crm_password=crm-password-1
ops_password=ops-password-1
docker run -d --name "$project-crm" --network "$network" -e POSTGRES_USER=crm -e POSTGRES_PASSWORD="$crm_password" \
  -e POSTGRES_DB=crm postgres:17 >/dev/null
docker run -d --name "$project-ops" --network "$network" -e MYSQL_ROOT_PASSWORD=root-password-1 -e MYSQL_DATABASE=ops \
  -e MYSQL_USER=ops -e MYSQL_PASSWORD="$ops_password" mysql:8.4 >/dev/null
until docker exec "$project-crm" pg_isready -U crm -d crm >/dev/null 2>&1; do sleep 1; done
until docker exec "$project-ops" mysql -h127.0.0.1 -uops -p"$ops_password" -e 'SELECT 1' ops >/dev/null 2>&1; do sleep 2; done
crm_sql() { docker exec -i "$project-crm" psql -U crm -d crm -v ON_ERROR_STOP=1 -qAt "$@"; }
crm_sql -c "CREATE SCHEMA sales;
  CREATE TABLE sales.customers (id int PRIMARY KEY, email text, region text);
  INSERT INTO sales.customers SELECT g, 'c' || g || '@example.com', CASE WHEN g % 2 = 0 THEN 'east' ELSE 'west' END
    FROM generate_series(1, 1000) g;
  CREATE TABLE sales.pii (id int, ssn text);
  INSERT INTO sales.pii VALUES (1, '123-45-6789');"
docker exec "$project-ops" mysql -h127.0.0.1 -uops -p"$ops_password" ops -e "
  CREATE TABLE campaigns (id INT PRIMARY KEY, customer_id INT, budget INT);
  INSERT INTO campaigns VALUES (1, 1, 100), (2, 2, 200), (3, 3, 300);" 2>/dev/null
expect_error "a connection may not be named like a schema of the catalog" 22023 igloo "$admin_password" \
  "CREATE CONNECTION crm TYPE postgres WITH (host '$project-crm', database 'crm', user 'crm', password '$crm_password')"
# The databases here have no TLS, or a self-signed certificate.
expect_error "a connection verifies its server by default" 08001 igloo "$admin_password" \
  "CREATE CONNECTION crmdb TYPE postgres WITH (host '$project-crm', database 'crm', user 'crm', password '$crm_password')"
expect_error "MySQL's too" 08001 igloo "$admin_password" \
  "CREATE CONNECTION ops TYPE mysql WITH (host '$project-ops', port 3306, database 'ops', user 'ops', password '$ops_password')"
# warned NAME CONNECTION SQL: SQL must succeed with a warning that the
# connection does not verify its server.
warned() {
  local got
  got="$(psql_admin -c "$3")" || true
  if grep -q "^WARNING:  01000: connection $2 does not" <<<"$got" && grep -q "^${3%% *} CONNECTION$" <<<"$got"; then
    pass "$1"
  else
    fail "$1: $got"
  fi
}
warned "admins register a PostgreSQL connection without TLS, warned" crmdb \
  "CREATE CONNECTION crmdb TYPE postgres WITH (host '$project-crm', database 'crm', user 'crm', password '$crm_password', tls 'disable')"
warned "and a MySQL one" ops \
  "CREATE CONNECTION ops TYPE mysql WITH (host '$project-ops', port 3306, database 'ops', user 'ops', password '$ops_password', tls 'disable')"
expect_error "only admins do" 42501 walt walt-password-1 \
  "CREATE CONNECTION evil TYPE postgres WITH (host 'postgres', database 'igloo', user 'igloo')"
expect_error "and never to Igloo's own metadata database" 22023 igloo "$admin_password" \
  "CREATE CONNECTION meta TYPE postgres WITH (host 'postgres', port 5432, database 'igloo', user 'igloo')"
expect_error "however its name is written" 22023 igloo "$admin_password" \
  "CREATE CONNECTION meta TYPE postgres WITH (host 'POSTGRES.', port 5432, database 'igloo', user 'igloo', tls 'disable')"
# connections prints each connection's name, state and password column.
connections() { psql_admin -c "SHOW CONNECTIONS" | cut -d'|' -f1,12,7 | awk -F'|' '{print $1 "|" $3 "|" $2}' | sort; }
got="$(connections)"
if [ "$got" = "$(printf 'crmdb|not attached|stored\nops|not attached|stored')" ]; then
  pass "connections attach when Igloo next starts"
else
  fail "SHOW CONNECTIONS before the restart: $got"
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
# The integration's URL and secret were stored encrypted; after a restart
# they still decrypt, and the alert still sends.
expect "alerts survive a restart" "EXECUTE ALERT" walt walt-password-1 "EXECUTE ALERT e2e_big_sales"
expect "and so do integrations" "triggered" walt walt-password-1 \
  "SELECT state FROM igloo.alert_history WHERE alert_name = 'e2e_big_sales' ORDER BY start_time DESC LIMIT 1"
expect "drop an alert" "DROP ALERT" walt walt-password-1 "DROP ALERT e2e_big_sales"
expect "drop an integration" "DROP INTEGRATION" igloo "$admin_password" "DROP INTEGRATION e2e_hook"
expect "policies survive a restart" "1|***@acme.com" nora nora-password-1 "SELECT id, email FROM crm.contacts"
expect "erasure requests and data retentions survive a restart" "completed|1" pat pat-password-1 \
  "SELECT status, (SELECT data_retention_days FROM igloo.data_retention) FROM igloo.erasure_requests WHERE request_id = '$request'"

echo "connections after the restart"
got="$(connections)"
if [ "$got" = "$(printf 'crmdb|attached|stored\nops|attached|stored')" ]; then
  pass "both connections are attached, and their passwords never shown again"
else
  fail "SHOW CONNECTIONS after the restart: $got"
fi
expect "admins join PostgreSQL, MySQL and Iceberg tables" "3|600" igloo "$admin_password" \
  "SELECT count(*), sum(o.budget) FROM e2e i JOIN crmdb.sales.customers c ON c.id = i.id JOIN ops.ops.campaigns o ON o.customer_id = c.id"
expect "filters reach the remote database" "500" igloo "$admin_password" \
  "SELECT count(*) FROM crmdb.sales.customers WHERE region = 'east'"
got="$(psql_admin -c "EXPLAIN SELECT email FROM crmdb.sales.customers WHERE region = 'east'")"
if grep -q "POSTGRES_SCAN" <<<"$got" && grep -q "region='east'" <<<"$got"; then pass "the filter is pushed down"; else fail "pushdown: $got"; fi
expect_error "readers need a grant on a connection" 42501 rita rita-password-1 "SELECT count(*) FROM crmdb.sales.customers"
expect_error "so do users with no role" 42501 nora nora-password-1 "SELECT count(*) FROM crmdb.sales.customers"
expect "the system catalogs list connections' tables" "2" nora nora-password-1 \
  "SELECT count(*) FROM information_schema.tables WHERE table_catalog = 'crmdb' AND table_schema = 'sales'"
expect "a grant on a table of a connection" "GRANT" igloo "$admin_password" "GRANT SELECT ON TABLE crmdb.sales.customers TO analysts"
expect "lets a user with no role join it with Iceberg tables" "3" nora nora-password-1 \
  "SELECT count(*) FROM e2e i JOIN crmdb.sales.customers c ON c.id = i.id"
expect "a masking policy on a connection's table" "ALTER TABLE" igloo "$admin_password" \
  "ALTER TABLE crmdb.sales.customers ALTER COLUMN email SET MASKING POLICY mask_email"
expect "masks it in joins with Iceberg tables" "3|***@example.com" nora nora-password-1 \
  "SELECT count(*), min(c.email) FROM e2e i JOIN crmdb.sales.customers c ON c.id = i.id"
expect "for admins too" "***@example.com" igloo "$admin_password" "SELECT min(email) FROM crmdb.sales.customers"
expect "and is listed with its connection" "crmdb|sales|email" igloo "$admin_password" \
  "SELECT ref_database_name, ref_schema_name, ref_column_name FROM igloo.policy_references WHERE ref_entity_name = 'customers'"
expect "unset it" "ALTER TABLE" igloo "$admin_password" \
  "ALTER TABLE crmdb.sales.customers ALTER COLUMN email UNSET MASKING POLICY"
expect_error "but no other table of the connection" 42501 nora nora-password-1 "SELECT * FROM crmdb.sales.pii"
expect_error "nor the other connection" 42501 nora nora-password-1 "SELECT * FROM ops.ops.campaigns"
expect_error "a writer may not make a view of a connection's table it may not read" 42501 walt walt-password-1 \
  "CREATE VIEW e2e_pii AS SELECT p.* FROM lake.public.e2e e JOIN crmdb.sales.pii p USING (id)"
expect "but may once granted" "GRANT" igloo "$admin_password" "GRANT SELECT ON TABLE crmdb.sales.pii TO walt"
expect "a writer makes an Iceberg view of a connection's table" "CREATE VIEW" walt walt-password-1 \
  "CREATE VIEW e2e_pii AS SELECT p.* FROM lake.public.e2e e JOIN crmdb.sales.pii p USING (id)"
expect_error "which reads with the reader's grants" 42501 nora nora-password-1 "SELECT * FROM e2e_pii"
expect "a grant on the whole connection" "GRANT" igloo "$admin_password" "GRANT SELECT ON CONNECTION ops TO rita"
expect "covers its tables" "3" rita rita-password-1 "SELECT count(*) FROM ops.ops.campaigns"
expect_error "readers still need a grant on the other connection" 42501 rita rita-password-1 "SELECT * FROM crmdb.sales.pii"
expect_error "connections are read-only" 0LP01 igloo "$admin_password" "GRANT INSERT ON TABLE crmdb.sales.customers TO walt"
if got="$(psql_admin -c "INSERT INTO crmdb.sales.customers VALUES (5000, 'x', 'y')")"; then
  fail "an admin wrote to a connection"
elif grep -qi "read-only" <<<"$got"; then
  pass "even for admins"
else
  fail "write to a connection: $got"
fi
expect_error "no SQL runs on the remote server from strings" 42501 igloo "$admin_password" \
  "SELECT * FROM postgres_query('crmdb', 'SELECT * FROM sales.pii')"
expect_error "nor statements" 42501 igloo "$admin_password" "CALL postgres_execute('crmdb', 'DROP TABLE sales.pii')"
expect_error "nor through MySQL" 42501 igloo "$admin_password" "SELECT * FROM mysql_query('ops', 'SELECT 1')"
expect_error "nor to other servers" 42501 igloo "$admin_password" \
  "SELECT * FROM postgres_scan('host=postgres user=igloo dbname=igloo', 'igloo_auth', 'users')"
expect_error "a PIVOT reading a connection's data while preparing" 42501 nora nora-password-1 \
  "PIVOT crmdb.sales.customers ON region USING count(*)"
expect_soon "the history names what was read from connections" '["lake.public.e2e","crmdb.sales.customers"]' igloo "$admin_password" \
  "SELECT tables_read FROM igloo.query_history WHERE user_name = 'nora' AND query_text LIKE '%JOIN crmdb.sales.customers%' AND outcome = 'ok' LIMIT 1"
expect "the catalog lists a connection's tables to those who may read them" "crmdb|sales|customers|FOREIGN|3" nora nora-password-1 \
  "SELECT table_catalog, table_schema, table_name, table_type,
     (SELECT count(*) FROM igloo.columns WHERE table_catalog = 'crmdb' AND table_name = 'customers')
   FROM igloo.tables WHERE table_catalog = 'crmdb'"
psql_admin -c "CREATE TABLE e2e_regions AS SELECT region, count(*) AS customers FROM crmdb.sales.customers GROUP BY 1" >/dev/null
expect_soon "lineage names a connection's tables in full" "crmdb|sales|customers" igloo "$admin_password" \
  "SELECT upstream_database, upstream_schema, upstream_name FROM igloo.lineage WHERE downstream_name = 'e2e_regions'"
expect "and leaves them unnamed to readers without a grant on them" "0|1" rita rita-password-1 \
  "SELECT (SELECT count(*) FROM igloo.tables WHERE table_catalog = 'crmdb'),
     (SELECT count(*) FROM igloo.lineage WHERE downstream_name = 'e2e_regions' AND upstream_name IS NULL)"
expect "connection passwords are never recorded" "0" igloo "$admin_password" \
  "SELECT count(*) FROM igloo.query_history WHERE query_text LIKE '%$crm_password%' OR query_text LIKE '%$ops_password%'"
if compose logs igloo 2>&1 | grep -q -e "$crm_password" -e "$ops_password"; then fail "a connection password is in the logs"; else pass "nor logged"; fi
# Rotating the password: on the remote database, then in Igloo, no restart.
crm_sql -c "ALTER ROLE crm PASSWORD 'crm-password-2'" >/dev/null
warned "credentials rotate without a restart" crmdb "ALTER CONNECTION crmdb SET (password 'crm-password-2')"
crm_sql -c "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE usename = 'crm' AND pid <> pg_backend_pid()" >/dev/null
expect "and the connection keeps working" "1000" igloo "$admin_password" "SELECT count(*) FROM crmdb.sales.customers"
# One database going down breaks nothing else.
docker stop -t 1 "$project-crm" >/dev/null
expect "with a database down, catalogs still list its tables" "2" nora nora-password-1 \
  "SELECT count(*) FROM information_schema.tables WHERE table_catalog = 'crmdb' AND table_schema = 'sales'"
expect "and so does the data catalog" "2|t" igloo "$admin_password" \
  "SELECT (SELECT count(*) FROM igloo.tables WHERE table_catalog = 'crmdb'), (SELECT count(*) > 0 FROM igloo.lineage)"
expect "and everything else works" "3|3" rita rita-password-1 \
  "SELECT (SELECT count(*) FROM e2e), (SELECT count(*) FROM ops.ops.campaigns)"
start=$SECONDS
if got="$(psql_as nora nora-password-1 -c "SELECT count(*) FROM crmdb.sales.customers")"; then
  fail "a query of a database that is down succeeded"
elif grep -q "connection crmdb" <<<"$got" && [ $((SECONDS - start)) -lt 30 ]; then
  pass "queries of it fail, quickly"
else
  fail "a query of a database that is down, after $((SECONDS - start))s: $got"
fi
docker start "$project-crm" >/dev/null
until docker exec "$project-crm" pg_isready -U crm -d crm >/dev/null 2>&1; do sleep 1; done
expect_soon "and work again once it is back" "1000" igloo "$admin_password" "SELECT count(*) FROM crmdb.sales.customers"
# The same for MySQL.
docker stop -t 1 "$project-ops" >/dev/null
expect "with a MySQL database down, everything else works" "3|1000" igloo "$admin_password" \
  "SELECT (SELECT count(*) FROM e2e), (SELECT count(*) FROM crmdb.sales.customers)"
start=$SECONDS
if got="$(psql_admin -c "SELECT count(*) FROM ops.ops.campaigns")"; then
  fail "a query of a MySQL database that is down succeeded"
elif grep -q "connection ops" <<<"$got" && [ $((SECONDS - start)) -lt 30 ]; then
  pass "and queries of it fail, quickly"
else
  fail "a query of a MySQL database that is down, after $((SECONDS - start))s: $got"
fi
docker start "$project-ops" >/dev/null
until docker exec "$project-ops" mysql -h127.0.0.1 -uops -p"$ops_password" -e 'SELECT 1' ops >/dev/null 2>&1; do sleep 2; done
expect_soon "and work again once it is back" "3" igloo "$admin_password" "SELECT count(*) FROM ops.ops.campaigns"
expect "dropping a connection detaches it at once" "DROP CONNECTION" igloo "$admin_password" "DROP CONNECTION ops"
if got="$(psql_admin -c "SELECT * FROM ops.ops.campaigns")"; then
  fail "a dropped connection's table is still there"
elif grep -q "does not exist" <<<"$got"; then
  pass "and its tables are gone"
else
  fail "a dropped connection's table: $got"
fi
if psql_admin -c "SHOW GRANTS" | grep -q "|ops"; then fail "a dropped connection's grants remain"; else pass "with its grants"; fi
expect "drop a view" "DROP VIEW" walt walt-password-1 "DROP VIEW e2e_big"
expect_error "a dropped view is gone" 42P01 walt walt-password-1 "DROP VIEW e2e_big"

if [ "$failures" -ne 0 ]; then
  echo "$failures check(s) failed"
  exit 1
fi
echo "all checks passed"
