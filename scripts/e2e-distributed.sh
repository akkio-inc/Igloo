#!/usr/bin/env bash
# End-to-end test of distributed execution (docs/distributed-execution.md):
# builds the image, starts an isolated copy of deploy/single-vm with worker
# containers, loads TPC-H, and checks that
#   - the 22 TPC-H queries return the same results with and without workers,
#   - the query history records how many workers ran each statement,
#   - grants hold on the workers,
#   - a worker killed in the middle of a statement does not fail it,
#   - statement timeouts reach the workers,
#   - a statement keeps its snapshot while another session commits,
#   - tables that maintenance compacted still run distributed,
#   - statements over tables with policies stay on one replica,
#   - workers accept no one without the worker secret.
# Removes everything afterwards unless KEEP_E2E=1. E2E_PROJECT, the
# E2E_*_PORT variables, E2E_WORKERS and E2E_TPCH_SF change the defaults;
# E2E_IMAGE runs an image already built instead of building one.
#
#   scripts/e2e-distributed.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
project="${E2E_PROJECT:-igloo-e2e-dist}"
sql_port="${E2E_SQL_PORT:-25433}"
http_port="${E2E_HTTP_PORT:-28432}"
console_port="${E2E_CONSOLE_PORT:-28443}"
flight_port="${E2E_FLIGHT_PORT:-23137}"
iceberg_port="${E2E_ICEBERG_PORT:-28181}"
sharing_port="${E2E_SHARING_PORT:-28444}"
workers="${E2E_WORKERS:-3}"
sf="${E2E_TPCH_SF:-0.1}"
image="${E2E_IMAGE:-$project:dev}"
work="$(mktemp -d)"
env_file="$work/e2e.env"

compose() {
  docker compose -p "$project" --project-directory "$root/deploy/single-vm" --env-file "$env_file" "$@"
}

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "--- igloo logs"
    compose logs --no-color --tail 60 igloo || true
    echo "--- worker logs"
    compose logs --no-color --tail 30 worker || true
  fi
  if [ "${KEEP_E2E:-}" != 1 ]; then
    compose --profile local-storage --profile workers --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
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
  -e "s|^IGLOO_IMAGE=.*|IGLOO_IMAGE=$image|" \
  -e "s/^COMPOSE_PROFILES=.*/COMPOSE_PROFILES=local-storage,workers/" -e "s/^IGLOO_WORKERS=.*/IGLOO_WORKERS=worker/" \
  -e "s/^IGLOO_WORKER_REPLICAS=.*/IGLOO_WORKER_REPLICAS=$workers/" -e "s/^IGLOO_WORKER_THREADS=.*/IGLOO_WORKER_THREADS=2/" \
  -e "s/^IGLOO_WORKER_MEMORY_LIMIT=.*/IGLOO_WORKER_MEMORY_LIMIT=1GB/" -e "s/^IGLOO_CACHE_SIZE=.*/IGLOO_CACHE_SIZE=0/" "$env_file"
admin_password="$(grep '^IGLOO_ADMIN_PASSWORD=' "$env_file" | cut -d= -f2)"

echo "starting the stack with $workers workers"
build=--build
[ -n "${E2E_IMAGE:-}" ] && build=--no-build
compose up -d "$build" --wait --wait-timeout 300 >/dev/null

network="${project}_default"

# psql_as USER PASSWORD ARGS... runs psql in a fresh container with
# tuples-only output (errors on stderr, merged).
psql_as() {
  local user=$1 password=$2
  shift 2
  docker run --rm -i --network "$network" -e PGPASSWORD="$password" -v "$root/bench/queries:/queries:ro" postgres:17 \
    psql -h igloo -U "$user" -d igloo -X -At -v ON_ERROR_STOP=1 "$@" 2>&1
}
psql_admin() { psql_as igloo "$admin_password" "$@"; }

# metric_matches PATTERN: whether a line of Igloo's metrics matches. They
# are read whole first: grep -q closing the pipe early would fail curl, and
# with it the pipeline.
metric_matches() {
  local metrics
  metrics="$(curl -fsS "http://127.0.0.1:$http_port/metrics" 2>/dev/null)" || return 1
  grep -q "$1" <<<"$metrics"
}

# A fresh stack runs its first maintenance pass within seconds, which would
# compact the small files loaded below into one per table; load after it.
deadline=$((SECONDS + 120))
until metric_matches '^igloo_maintenance_passes_total{result="ok"} [1-9]'; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "no maintenance pass finished; loading anyway"
    break
  fi
  sleep 2
done

# Several files per table, so that every worker gets some. Rows are divided
# by their first column, not by rowid: with the spatial extension loaded,
# DuckDB 1.5.6 cannot plan a filter on rowid.
echo "loading TPC-H at scale factor $sf"
{
  echo "ATTACH ':memory:' AS tpch_gen; CALL dbgen(sf = $sf, catalog = 'tpch_gen', schema = 'main');"
  for t in region:r_regionkey nation:n_nationkey supplier:s_suppkey customer:c_custkey part:p_partkey \
    partsupp:ps_partkey orders:o_orderkey lineitem:l_orderkey; do
    key=${t#*:} t=${t%%:*}
    echo "CREATE TABLE $t AS SELECT * FROM tpch_gen.main.$t WHERE false;"
    for part in 0 1 2 3 4 5; do
      echo "INSERT INTO $t SELECT * FROM tpch_gen.main.$t WHERE hash($key) % 6 = $part;"
    done
  done
  echo "DETACH tpch_gen;"
} | psql_admin -q >/dev/null

# same FILE_A FILE_B: same rows, numbers equal to 9 significant digits
# (floating-point sums may differ in their last digits, as on one machine
# between runs with different numbers of threads).
same() {
  awk -F'|' '
    NR == FNR { want[FNR] = $0; n = FNR; next }
    {
      got = FNR
      if (!(FNR in want)) { bad = 1; exit }
      split(want[FNR], w, "|")
      if (NF != length(w)) { bad = 1; exit }
      for (i = 1; i <= NF; i++) {
        if ($i == w[i]) continue
        if ($i ~ /^-?[0-9.]+(e[-+]?[0-9]+)?$/ && w[i] ~ /^-?[0-9.]+(e[-+]?[0-9]+)?$/) {
          d = $i - w[i]; if (d < 0) d = -d
          m = w[i]; if (m < 0) m = -m
          if (d <= 1e-9 * m) continue
        }
        bad = 1; exit
      }
    }
    END { if (bad || got != n) exit 1 }' "$1" "$2"
}

echo "TPC-H with and without workers"
for q in $(seq -w 1 22); do
  file="/queries/tpch/q$q.sql"
  psql_admin -c "SET distributed_execution = 'off'" -f "$file" | grep -v '^SET$' >"$work/local-$q" || true
  psql_admin -c "SET distributed_execution = 'on'" -f "$file" | grep -v '^SET$' >"$work/dist-$q" || true
  if grep -q ERROR "$work/dist-$q" "$work/local-$q"; then
    fail "q$q: $(grep -h ERROR "$work/dist-$q" "$work/local-$q" | head -1)"
  elif [ ! -s "$work/local-$q" ]; then
    fail "q$q returned nothing"
  elif same "$work/local-$q" "$work/dist-$q"; then
    pass "q$q ($(wc -l <"$work/local-$q" | tr -d ' ') rows)"
  else
    fail "q$q results differ"
    diff "$work/local-$q" "$work/dist-$q" | head -6
  fi
done

expect() {
  local name=$1 want=$2 got
  got="$(psql_admin -c "$3")" || true
  if [ "$got" = "$want" ]; then pass "$name"; else fail "$name: got [$got], want [$want]"; fi
}

echo "query history"
# History is written in the background, about once a second.
sleep 3
got="$(psql_admin -c "SELECT count(DISTINCT query_text) FROM igloo.query_history WHERE workers = $workers")"
if [ "${got:-0}" -ge 12 ]; then pass "history shows $got statements run by $workers workers"; else fail "history shows $got statements run by $workers workers"; fi
expect "statements run on one machine show 0 workers" "0" \
  "SELECT max(q.workers) FROM igloo.query_history q JOIN igloo.query_history s ON s.session_id = q.session_id
   WHERE s.query_text = 'SET distributed_execution = ''off''' AND q.statement_type = 'SELECT'"

echo "grants"
psql_admin -c "CREATE USER nora WITH PASSWORD 'nora-password-1' ROLE none; GRANT SELECT ON TABLE lineitem TO nora" >/dev/null
got="$(psql_as nora nora-password-1 -c "SET distributed_execution = 'on'" \
  -c "SELECT l_returnflag, count(*) FROM lineitem GROUP BY 1 ORDER BY 1" | grep -v '^SET$' | head -1)"
if [[ "$got" == A\|* ]]; then pass "a granted table runs distributed"; else fail "granted table: $got"; fi
sleep 2
expect "the history shows it ran on workers" "$workers" \
  "SELECT workers FROM igloo.query_history WHERE user_name = 'nora' AND query_text LIKE 'SELECT l_returnflag%' ORDER BY start_time DESC LIMIT 1"
got="$(psql_as nora nora-password-1 -c "SET distributed_execution = 'on'" \
  -c "SELECT count(*) FROM lineitem JOIN orders ON l_orderkey = o_orderkey" || true)"
if grep -q "permission denied" <<<"$got"; then pass "an ungranted table is refused"; else fail "ungranted table: $got"; fi

echo "a worker dies in the middle of a statement"
# Several seconds on the workers, so that the kill a second in lands while
# they work.
slow="SELECT l_returnflag, sum(length(repeat(l_comment, 1500))) FROM lineitem GROUP BY 1 ORDER BY 1"
want="$(psql_admin -c "SET distributed_execution = 'off'" -c "$slow" | grep -v '^SET$')"
start=$SECONDS
psql_admin -c "SET distributed_execution = 'on'" -c "$slow" | grep -v '^SET$' >"$work/slow" &
query=$!
sleep 1
victim="$(docker ps -q --filter "label=com.docker.compose.project=$project" --filter "label=com.docker.compose.service=worker" | head -1)"
docker kill "$victim" >/dev/null
wait "$query" || true
if [ "$(cat "$work/slow")" = "$want" ]; then
  pass "the statement finished with the right result in $((SECONDS - start))s"
else
  fail "after a worker died: [$(cat "$work/slow")], want [$want]"
fi
if compose logs --no-color igloo | grep -q "trying another"; then
  pass "the dead worker's share ran on another worker"
else
  echo "        (the worker finished its share before it was killed)"
fi
compose up -d --wait worker >/dev/null 2>&1 || compose up -d --wait --scale worker="$workers" >/dev/null

echo "statement timeout"
got="$(psql_admin -v VERBOSITY=verbose -c "SET distributed_execution = 'on'" -c "SET statement_timeout = '1s'" \
  -c "SELECT l_returnflag, sum(length(repeat(l_comment, 3000))) FROM lineitem GROUP BY 1" || true)"
if grep -q "57014" <<<"$got"; then pass "statement_timeout cancels a distributed statement"; else fail "timeout: $got"; fi

echo "snapshot isolation"
# A statement reads one snapshot of each table, on the workers and in the
# part the coordinator runs itself (the scalar subquery), while another
# session deletes rows and adds a column.
{
  echo "CREATE TABLE iso AS SELECT * FROM orders WHERE false;"
  for part in 0 1 2 3 4 5; do echo "INSERT INTO iso SELECT * FROM orders WHERE hash(o_orderkey) % 6 = $part;"; done
} | psql_admin -q >/dev/null
iso="SELECT x.o_orderstatus, x.n, x.s, (SELECT count(*) FROM iso) AS total FROM
  (SELECT o_orderstatus, count(*) AS n, sum(length(repeat(o_comment, 5000))) AS s FROM iso GROUP BY 1) AS x ORDER BY 1"
want="$(psql_admin -c "SET distributed_execution = 'off'" -c "$iso" | grep -v '^SET$')"
psql_admin -c "SET distributed_execution = 'on'" -c "/* iso */ $iso" | grep -v '^SET$' >"$work/iso" &
query=$!
sleep 1
psql_admin -c "DELETE FROM iso WHERE o_orderkey % 2 = 0" -c "ALTER TABLE iso ADD COLUMN extra INTEGER" >/dev/null
wait "$query" || true
if [ "$(cat "$work/iso")" = "$want" ]; then
  pass "a statement saw the snapshot it started with"
else
  fail "after a concurrent commit: [$(cat "$work/iso")], want [$want]"
fi
sleep 2
got="$(psql_admin -c "SELECT q.workers, q.end_time > d.end_time FROM igloo.query_history q, igloo.query_history d
  WHERE q.query_text LIKE '/* iso */%' AND d.query_text LIKE 'DELETE FROM iso%' ORDER BY q.start_time DESC LIMIT 1")"
case "$got" in
  [1-9]*\|t) pass "on workers, while the commit landed" ;;
  [1-9]*\|f) echo "        (the statement finished before the commit; inconclusive)" ;;
  *) fail "the snapshot check did not run distributed: [$got]" ;;
esac

echo "after table maintenance"
# Compaction rewrites the small files loaded above, and gives every table
# it compacts a name mapping (for files without field IDs, which these are
# not); the tables must still run distributed.
echo "IGLOO_MAINTENANCE_INTERVAL=5s" >>"$env_file"
compose up -d --no-build --wait igloo >/dev/null
deadline=$((SECONDS + 180))
until metric_matches '^igloo_maintenance_compacted_files_total [1-9]'; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    fail "no table was compacted"
    break
  fi
  sleep 2
done
query="SELECT l_returnflag, count(*), sum(l_quantity) FROM lineitem GROUP BY 1 ORDER BY 1"
want="$(psql_admin -c "SET distributed_execution = 'off'" -c "$query" | grep -v '^SET$')"
got="$(psql_admin -c "SET distributed_execution = 'on'" -c "/* compacted */ $query" | grep -v '^SET$')"
if [ "$got" = "$want" ]; then pass "a compacted table returns the same rows"; else fail "compacted: [$got], want [$want]"; fi
sleep 2
got="$(psql_admin -c "SELECT workers FROM igloo.query_history WHERE query_text LIKE '/* compacted */%' ORDER BY start_time DESC LIMIT 1")"
if [ "${got:-0}" -ge 1 ]; then pass "and runs on workers"; else fail "a compacted table ran on $got workers"; fi

echo "policies"
# Workers read table files and could not apply masking or row access
# policies: a statement over a protected table runs on one replica and
# returns what the policies let through, however it aggregates.
{
  echo "CREATE TABLE contacts AS SELECT c_custkey AS id, c_name AS name, c_nationkey AS nation FROM customer;"
  echo "CREATE MASKING POLICY hide_name AS (val VARCHAR) RETURNS VARCHAR -> '***';"
  echo "ALTER TABLE contacts ALTER COLUMN name SET MASKING POLICY hide_name;"
  echo "CREATE ROW ACCESS POLICY low_nations AS (n INTEGER) RETURNS BOOLEAN -> n < 5;"
  echo "ALTER TABLE contacts ADD ROW ACCESS POLICY low_nations ON (nation);"
} | psql_admin -q >/dev/null
got="$(psql_admin -c "SET distributed_execution = 'on'" \
  -c "/* masked */ SELECT count(DISTINCT name), min(name), max(name), max(nation) FROM contacts" | grep -v '^SET$')"
if [ "$got" = "1|***|***|4" ]; then pass "a protected table returns what its policies let through"; else fail "protected table: [$got]"; fi
sleep 2
expect "and runs on one replica" "0" \
  "SELECT workers FROM igloo.query_history WHERE query_text LIKE '/* masked */%' ORDER BY start_time DESC LIMIT 1"

echo "worker endpoint"
if docker run --rm --network "$network" curlimages/curl:8.22.0 -sk --max-time 10 "https://worker:7433/v1/status" >/dev/null 2>&1; then
  fail "a client without the worker secret reached a worker"
else
  pass "workers refuse clients without the worker secret"
fi

echo
if [ "$failures" -gt 0 ]; then
  echo "$failures check(s) failed"
  exit 1
fi
echo "all checks passed"
