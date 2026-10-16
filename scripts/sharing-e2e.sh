#!/usr/bin/env bash
# Delta Sharing checks that need more than scripts/e2e.sh's fast ones:
#
#  - the official Python client (delta-sharing, on pandas and pyarrow) reads
#    a share from Igloo: listings, a table as a DataFrame, versions and time
#    travel, and the files of a table with deleted rows;
#  - Igloo reads shares from the reference server (deltaio/delta-sharing-
#    server), a plain and a partitioned Delta table written to the stack's
#    object store.
#
# Starts an isolated copy of deploy/single-vm like scripts/e2e.sh, and
# removes it, its volumes and its image afterwards unless KEEP_E2E=1. The
# Python client runs as linux/amd64, for which its native dependency has
# wheels; on other machines Docker emulates it.
#
#   scripts/sharing-e2e.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
project="${E2E_PROJECT:-igloo-sharing-e2e}"
sql_port="${E2E_SQL_PORT:-25433}"
http_port="${E2E_HTTP_PORT:-28432}"
console_port="${E2E_CONSOLE_PORT:-28443}"
sharing_port="${E2E_SHARING_PORT:-28444}"
reference_image="deltaio/delta-sharing-server:${DELTA_SHARING_SERVER_VERSION:-0.7.8}"
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
    echo "--- reference server logs"
    docker logs --tail 40 "$project-reference" 2>&1 || true
  fi
  if [ "${KEEP_E2E:-}" != 1 ]; then
    docker rm -f "$project-reference" >/dev/null 2>&1 || true
    compose --profile local-storage --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
    docker rmi "$project:dev" >/dev/null 2>&1 || true
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
  -e "s/^IGLOO_CONSOLE_PORT=.*/IGLOO_CONSOLE_PORT=$console_port/" -e "s/^IGLOO_SHARING_PORT=.*/IGLOO_SHARING_PORT=$sharing_port/" \
  -e "s/^IGLOO_IMAGE=.*/IGLOO_IMAGE=$project:dev/" "$env_file"
admin_password="$(grep '^IGLOO_ADMIN_PASSWORD=' "$env_file" | cut -d= -f2)"
s3_key="$(grep '^S3_ACCESS_KEY_ID=' "$env_file" | cut -d= -f2)"
s3_secret="$(grep '^S3_SECRET_ACCESS_KEY=' "$env_file" | cut -d= -f2)"
bucket="$(grep '^S3_BUCKET=' "$env_file" | cut -d= -f2)"

echo "starting the stack"
compose up -d --build --wait --wait-timeout 300 >/dev/null
network="${project}_default"

psql_admin() {
  docker run --rm -i --network "$network" -e PGPASSWORD="$admin_password" postgres:17 \
    psql -h igloo -U igloo -d igloo -X -At -v ON_ERROR_STOP=1 "$@" 2>&1
}
expect() {
  local name=$1 want=$2 got
  got="$(psql_admin -c "$3")" || true
  if [ "$got" = "$want" ]; then pass "$name"; else fail "$name: got [$got], want [$want]"; fi
}

echo "the Python client reads Igloo's share"
sharing="http://127.0.0.1:$sharing_port/delta-sharing"
psql_admin -c "CREATE TABLE campaigns (id INTEGER, client VARCHAR, spend DECIMAL(12,2), ts TIMESTAMP, day DATE,
  tags VARCHAR[], info STRUCT(a INTEGER, b VARCHAR))" >/dev/null
psql_admin -c "INSERT INTO campaigns VALUES
  (1, 'acme', 100.50, TIMESTAMP '2026-10-01 10:00:00.123456', DATE '2026-10-01', ['x', 'y'], {'a': 1, 'b': 'q'})" >/dev/null
psql_admin -c "CREATE SHARE clients; ALTER SHARE clients ADD TABLE campaigns WITH HISTORY" >/dev/null
recipient="$(psql_admin -c "CREATE RECIPIENT acme")"
code="${recipient#*/activate/}"
code="${code%%|*}"
psql_admin -c "GRANT SELECT ON SHARE clients TO RECIPIENT acme" >/dev/null
token="$(curl -sS -X POST "$sharing/activate/$code" | sed -n 's/.*"bearerToken": "\([^"]*\)".*/\1/p')"
old_version="$(curl -sS -o /dev/null -D - -H "Authorization: Bearer $token" "$sharing/shares/clients/schemas/public/tables/campaigns/version" |
  tr -d '\r' | sed -n 's/^Delta-Table-Version: //p')"
psql_admin -c "INSERT INTO campaigns VALUES (2, 'globex', 20.25, TIMESTAMP '2026-10-02 11:00:00', DATE '2026-10-02', [], NULL)" >/dev/null
# The client runs in Igloo's network namespace, where the endpoint is at
# localhost and so needs no TLS.
printf '{"shareCredentialsVersion": 1, "endpoint": "http://localhost:8444/delta-sharing", "bearerToken": "%s"}\n' "$token" >"$work/acme.share"
if docker run --rm --platform linux/amd64 --network "container:$project-igloo-1" \
  -e PROFILE=/work/acme.share -e OLD_VERSION="$old_version" -e EXPECTED_ROWS=1 \
  -v "$root/scripts/clients:/clients:ro" -v "$work:/work" python:3.12-slim sh -c "
    pip install -q --root-user-action=ignore delta-sharing==1.4.1 >/dev/null 2>&1 && python /clients/delta_sharing_test.py"; then
  pass "delta-sharing 1.4.1 (pandas)"
else
  fail "the Python client"
fi
# Rows 1 and 3 deleted: the first file has no rows left and is not listed,
# the third is served as a copy with one row, the second as it is.
psql_admin -c "INSERT INTO campaigns (id, client) VALUES (3, 'initech'), (4, 'hooli')" >/dev/null
psql_admin -c "DELETE FROM campaigns WHERE id IN (1, 3)" >/dev/null
rows="$(curl -sS -X POST -H "Authorization: Bearer $token" -d '{}' "$sharing/shares/clients/schemas/public/tables/campaigns/query" |
  sed -n 's/.*\\"numRecords\\":\([0-9]*\).*/\1/p' | sort | tr '\n' ' ')"
if [ "$rows" = "1 1 " ]; then pass "files with deletes are served without the deleted rows"; else fail "after DELETE: numRecords [$rows]"; fi

echo "Igloo reads shares from the reference server"
reference_token="$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
if docker run --rm --network "$network" -v "$root/scripts/clients:/clients:ro" \
  -e TABLE_ROOT="s3://$bucket/delta" -e S3_ENDPOINT_URL=http://storage:9000 -e S3_REGION=us-east-1 \
  -e S3_ACCESS_KEY_ID="$s3_key" -e S3_SECRET_ACCESS_KEY="$s3_secret" python:3.12-slim sh -c "
    pip install -q --root-user-action=ignore deltalake==1.1.4 pyarrow==21.0.0 >/dev/null 2>&1 && python /clients/write_delta_table.py"; then
  pass "Delta tables written"
else
  fail "write Delta tables"
fi
mkdir -p "$work/reference"
cat >"$work/reference/server.yaml" <<EOF
version: 1
shares:
- name: "partner"
  schemas:
  - name: "data"
    tables:
    - name: "trips"
      location: "s3a://$bucket/delta/trips"
      id: "00000000-0000-0000-0000-000000000001"
    - name: "viewing"
      location: "s3a://$bucket/delta/viewing"
      id: "00000000-0000-0000-0000-000000000002"
authorization:
  bearerToken: "$reference_token"
host: "0.0.0.0"
port: 8080
endpoint: "/delta-sharing"
preSignedUrlTimeoutSeconds: 900
EOF
cat >"$work/reference/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>fs.s3a.access.key</name><value>$s3_key</value></property>
  <property><name>fs.s3a.secret.key</name><value>$s3_secret</value></property>
  <property><name>fs.s3a.endpoint</name><value>http://storage:9000</value></property>
  <property><name>fs.s3a.path.style.access</name><value>true</value></property>
  <property><name>fs.s3a.connection.ssl.enabled</name><value>false</value></property>
</configuration>
EOF
docker run -d --name "$project-reference" --network "$network" --network-alias reference \
  -v "$work/reference/server.yaml:/config/server.yaml:ro" -v "$work/reference/core-site.xml:/opt/docker/conf/core-site.xml:ro" \
  -e AWS_ACCESS_KEY_ID="$s3_key" -e AWS_SECRET_ACCESS_KEY="$s3_secret" \
  "$reference_image" -- --config /config/server.yaml >/dev/null
ready=""
for _ in $(seq 1 60); do
  if docker run --rm --network "$network" curlimages/curl:8.22.0 -fsS -o /dev/null \
    -H "Authorization: Bearer $reference_token" http://reference:8080/delta-sharing/shares 2>/dev/null; then
    ready=1
    break
  fi
  sleep 2
done
if [ -n "$ready" ]; then pass "the reference server is up"; else fail "the reference server did not start"; fi

profile="{\"shareCredentialsVersion\": 1, \"endpoint\": \"http://reference:8080/delta-sharing\", \"bearerToken\": \"$reference_token\"}"
expect "a provider from the reference server's profile" "CREATE PROVIDER" "CREATE PROVIDER reference FROM '$profile'"
expect "its tables" "partner|data|trips
partner|data|viewing" "SHOW SHARES IN PROVIDER reference"
psql_admin -c "CREATE SCHEMA partners" >/dev/null
expect "an external table on a Delta table" "CREATE EXTERNAL TABLE" "CREATE EXTERNAL TABLE partners.trips FROM SHARE reference.partner.data.trips"
expect "reads its rows" "100|2475|25" "SELECT count(*), sum(fare), count(*) FILTER (WHERE city = 'sf') FROM partners.trips"
expect "an external table on a partitioned Delta table" "CREATE EXTERNAL TABLE" \
  "CREATE EXTERNAL TABLE partners.viewing FROM SHARE reference.partner.data.viewing"
expect "puts partition values back, with their types" "30|435|2026-10-01|DATE|10" \
  "SELECT count(*), sum(minutes), min(day), typeof(min(day)), count(*) FILTER (WHERE network = 'cbs') FROM partners.viewing"
expect "and filters by them" "10" "SELECT count(*) FROM partners.viewing WHERE day = DATE '2026-10-02'"

if [ "$failures" -ne 0 ]; then
  echo "$failures check(s) failed"
  exit 1
fi
echo "all checks passed"
