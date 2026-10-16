#!/usr/bin/env bash
# End-to-end test of Igloo's Iceberg REST catalog with heavy engines: Apache
# Spark (with the Iceberg runtime) and Trino read and write Igloo's tables
# through it, under Igloo's grants, and Igloo's SQL sees their changes. It
# starts an isolated copy of deploy/single-vm like scripts/e2e.sh, which
# covers PyIceberg on every run, and removes it afterwards unless
# KEEP_E2E=1. The first run pulls about 3 GB of images and jars.
#
#   scripts/engines-e2e.sh
#
# E2E_PROJECT and the E2E_*_PORT variables let several runs share a Docker
# host; SPARK_IMAGE, TRINO_IMAGE and ICEBERG_VERSION choose the engines.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
project="${E2E_PROJECT:-igloo-engines}"
sql_port="${E2E_SQL_PORT:-15533}"
http_port="${E2E_HTTP_PORT:-18532}"
console_port="${E2E_CONSOLE_PORT:-18543}"
iceberg_port="${E2E_ICEBERG_PORT:-18281}"
spark_image="${SPARK_IMAGE:-apache/spark:3.5.9-java17-python3}"
trino_image="${TRINO_IMAGE:-trinodb/trino:483}"
iceberg_version="${ICEBERG_VERSION:-1.11.0}"
work="$(mktemp -d)"
env_file="$work/e2e.env"
jars="$project-jars"
trino="$project-trino"

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
    docker rm -f "$trino" >/dev/null 2>&1 || true
    compose --profile local-storage --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
    docker volume rm "$jars" >/dev/null 2>&1 || true
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
  -e "s/^IGLOO_CONSOLE_PORT=.*/IGLOO_CONSOLE_PORT=$console_port/" -e "s/^IGLOO_ICEBERG_PORT=.*/IGLOO_ICEBERG_PORT=$iceberg_port/" \
  -e "s/^IGLOO_IMAGE=.*/IGLOO_IMAGE=$project:dev/" "$env_file"
admin_password="$(grep '^IGLOO_ADMIN_PASSWORD=' "$env_file" | cut -d= -f2)"

echo "starting the stack"
compose up -d --build --wait --wait-timeout 300 >/dev/null
network="${project}_default"

psql_admin() {
  docker run --rm -i --network "$network" -e PGPASSWORD="$admin_password" postgres:17 \
    psql -h igloo -U igloo -d igloo -X -At -v ON_ERROR_STOP=1 "$@" 2>&1
}

# expect NAME WANT SQL: Igloo's answer to SQL, as the admin, must be WANT.
# It may take a minute, for history that is written in the background.
expect() {
  local got="" deadline=$((SECONDS + 60))
  while [ "$SECONDS" -lt "$deadline" ]; do
    got="$(psql_admin -c "$3")" || true
    [ "$got" = "$2" ] && break
    sleep 2
  done
  if [ "$got" = "$2" ]; then pass "$1"; else fail "$1: got [$got], want [$2]"; fi
}

echo "tables, users and grants"
psql_admin -c "CREATE SCHEMA engines" -c "CREATE SCHEMA engines_out" \
  -c "CREATE TABLE engines.orders (id INTEGER, customer VARCHAR, amount DECIMAL(10,2));
      INSERT INTO engines.orders VALUES (1, 'acme', 10.00), (2, 'globex', 99.00), (7, 'gone', 1.00);
      UPDATE engines.orders SET amount = 20.00 WHERE id = 2;
      DELETE FROM engines.orders WHERE id = 7;
      CREATE TABLE engines.secret (id INTEGER)" \
  -c "CREATE USER sam WITH PASSWORD 'sam-password-1' ROLE none;
      CREATE USER rae WITH PASSWORD 'rae-password-1' ROLE none;
      CREATE USER tia WITH PASSWORD 'tia-password-1' ROLE none;
      GRANT SELECT, INSERT, UPDATE, DELETE ON engines.orders TO sam;
      GRANT ALL ON SCHEMA engines_out TO sam;
      GRANT SELECT ON engines.orders TO rae;
      GRANT SELECT, INSERT, UPDATE, DELETE ON engines.orders TO tia" >/dev/null
pass "set up (Igloo's UPDATE and DELETE leave position deletes for the engines to apply)"

echo "Apache Spark ($spark_image, Iceberg $iceberg_version)"
docker volume create "$jars" >/dev/null
docker run --rm -u 0 -v "$jars:/jars" -w /jars curlimages/curl:8.22.0 sh -c "
  for jar in iceberg-spark-runtime-3.5_2.12 iceberg-aws-bundle; do
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/\$jar/$iceberg_version/\$jar-$iceberg_version.jar || exit 1
  done; chmod 644 /jars/*.jar"
if docker run --rm --network "$network" -e ICEBERG_URI=http://igloo:8181 -v "$jars:/jars:ro" \
  -v "$root/scripts/clients:/clients:ro" "$spark_image" /opt/spark/bin/spark-submit --driver-memory 1g \
  --jars "/jars/iceberg-spark-runtime-3.5_2.12-$iceberg_version.jar,/jars/iceberg-aws-bundle-$iceberg_version.jar" \
  /clients/spark_test.py >"$work/spark.out" 2>&1; then
  grep '^  spark: ' "$work/spark.out" | sed 's/^  spark: /  ok    spark: /'
else
  grep '^  spark: ' "$work/spark.out" | sed 's/^  spark: /  ok    spark: /' || true
  fail "spark: $(grep -v ' INFO \|WARN\|^\s*at ' "$work/spark.out" | tail -15)"
fi
expect "Igloo sees Spark's append and MERGE" "1:10.00,2:25.00,3:30.00,4:40.00" \
  "SELECT string_agg(id || ':' || amount, ',' ORDER BY id) FROM engines.orders"
expect "and the table Spark created" "4|105.00" "SELECT count(*), sum(total) FROM engines_out.order_totals"

echo "Trino ($trino_image)"
docker run -d --name "$trino" --network "$network" -e ICEBERG_URI=http://igloo:8181 \
  -e IGLOO_CREDENTIAL=tia:tia-password-1 -e READER_CREDENTIAL=rae:rae-password-1 -e S3_ENDPOINT=http://storage:9000 \
  -v "$root/scripts/clients/trino:/etc/trino/catalog:ro" "$trino_image" >/dev/null
deadline=$((SECONDS + 300))
until docker logs "$trino" 2>&1 | grep -q "SERVER STARTED"; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    fail "Trino did not start: $(docker logs "$trino" 2>&1 | tail -5)"
    exit 1
  fi
  sleep 3
done
# trino_sql SQL prints Trino's result as TSV, or its error.
trino_sql() { docker exec "$trino" trino --output-format TSV --execute "$1" 2>&1; }
# trino_expect NAME WANT SQL
trino_expect() {
  local got
  got="$(trino_sql "$3")" || true
  if [ "$got" = "$2" ]; then pass "trino: $1"; else fail "trino: $1: got [$got], want [$2]"; fi
}
# trino_refused NAME SQL NEEDLE: SQL must fail, and --debug output mention
# NEEDLE.
trino_refused() {
  local got
  if got="$(docker exec "$trino" trino --debug --execute "$2" 2>&1)"; then
    fail "trino: $1: succeeded"
  elif grep -q "$3" <<<"$got"; then
    pass "trino: $1"
  else
    fail "trino: $1: $(grep -m3 'Caused by\|failed' <<<"$got")"
  fi
}
trino_expect "reads an Igloo table and Spark's changes" "1	10.00
2	25.00
3	30.00
4	40.00" "SELECT id, CAST(amount AS VARCHAR) FROM igloo.engines.orders ORDER BY id"
trino_expect "lists only granted tables" "orders" "SHOW TABLES FROM igloo.engines"
trino_expect "inserts" "INSERT: 1 row" "INSERT INTO igloo.engines.orders VALUES (5, 'hooli', DECIMAL '50.00')"
trino_expect "deletes" "DELETE: 1 row" "DELETE FROM igloo.engines.orders WHERE id = 1"
trino_expect "updates" "UPDATE: 1 row" "UPDATE igloo.engines.orders SET amount = DECIMAL '35.00' WHERE id = 3"
trino_refused "a table without a grant is refused" "SELECT * FROM igloo.engines.secret" "permission denied for tia"
trino_expect "SELECT alone reads" "4" "SELECT count(*) FROM igloo_reader.engines.orders"
trino_refused "but cannot write" "INSERT INTO igloo_reader.engines.orders VALUES (9, 'nope', DECIMAL '1.00')" "403"
expect "Igloo sees Trino's insert, delete and update" "2:25.00,3:35.00,4:40.00,5:50.00" \
  "SELECT string_agg(id || ':' || amount, ',' ORDER BY id) FROM engines.orders"

echo "history"
expect "every engine's requests are in the query history" "t|t|t|t" "
  SELECT bool_or(user_name = 'sam' AND statement_type = 'updateTable' AND query_text LIKE '%(add-snapshot (overwrite)%'),
         bool_or(user_name = 'sam' AND statement_type = 'createTable'),
         bool_or(user_name = 'tia' AND statement_type = 'loadTable' AND outcome = 'denied'),
         bool_or(user_name = 'rae' AND query_text LIKE '%(read credentials)')
  FROM igloo.query_history WHERE application_name LIKE 'iceberg-rest%'"

if [ "$failures" -ne 0 ]; then
  echo "$failures check(s) failed"
  exit 1
fi
echo "all checks passed"
