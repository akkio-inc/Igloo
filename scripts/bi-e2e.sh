#!/usr/bin/env bash
# End-to-end test of the BI tools agencies report with, against an isolated
# copy of deploy/single-vm: Metabase and Apache Superset, each connecting
# through its own PostgreSQL driver, syncing the schema and running the
# queries a dashboard runs, and Npgsql, the driver of Power BI's PostgreSQL
# connector, in the release Power BI Desktop bundles and the latest. It pulls
# about 3 GB of images, so CI runs it on a schedule rather than on every pull
# request (.github/workflows/bi-tools.yml). Removes everything afterwards
# unless KEEP_BI_E2E=1. The tools reach Igloo over the stack's network, so
# its ports are published on free ones Docker picks; with BI_E2E_PROJECT,
# several runs can share a Docker host.
#
#   scripts/bi-e2e.sh [metabase] [superset] [npgsql]   (default: all three)
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
project="${BI_E2E_PROJECT:-igloo-bi-e2e}"
metabase_image=metabase/metabase:v0.64.2.7
superset_image=apache/superset:6.1.0
dotnet_image=mcr.microsoft.com/dotnet/sdk:10.0
# Power BI Desktop bundles Npgsql 4.0; applications use the latest.
npgsql_versions="4.0.17 10.0.3"
tools=("$@")
[ ${#tools[@]} -gt 0 ] || tools=(metabase superset npgsql)
work="$(mktemp -d)"
env_file="$work/bi.env"
network="${project}_default"

compose() {
  docker compose -p "$project" --project-directory "$root/deploy/single-vm" --env-file "$env_file" "$@"
}

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "--- igloo logs"
    compose logs --no-color --tail 60 igloo || true
    for tool in metabase superset; do
      docker logs --tail 40 "$project-$tool" 2>&1 | sed "s/^/$tool: /" || true
    done
  fi
  if [ "${KEEP_BI_E2E:-}" != 1 ]; then
    docker rm -f "$project-metabase" "$project-superset" >/dev/null 2>&1 || true
    compose --profile local-storage --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$work"
  else
    echo "kept the stack: docker compose -p $project --env-file $env_file ..."
  fi
  exit "$status"
}
trap cleanup EXIT

wants() { [[ " ${tools[*]} " == *" $1 "* ]]; }

"$root/deploy/single-vm/init-env.sh" "$env_file" >/dev/null
# The ports the stack publishes (not, say, IGLOO_SMTP_PORT, which Igloo reads).
sed -i.bak -E -e "s/^(IGLOO_((HTTP|CONSOLE|FLIGHT|ICEBERG|SHARING)_)?PORT)=.*/\1=0/" \
  -e "s/^IGLOO_IMAGE=.*/IGLOO_IMAGE=$project:dev/" "$env_file"

echo "starting the stack"
compose up -d --build --wait --wait-timeout 300 >/dev/null
# Start the BI tools while the data loads: they take a minute or two.
if wants metabase; then
  docker run -d --name "$project-metabase" --network "$network" --network-alias metabase -e MB_CHECK_FOR_UPDATES=false \
    -e MB_ANON_TRACKING_ENABLED=false "$metabase_image" >/dev/null
fi
if wants superset; then
  # The image has no database drivers; Superset's docs add psycopg2 so.
  docker run -d --name "$project-superset" --network "$network" --network-alias superset -e SUPERSET_SECRET_KEY="$(openssl rand -hex 32)" \
    -e SUPERSET_LOAD_EXAMPLES=no --user root --entrypoint sh "$superset_image" -c '
      set -e
      uv pip install --quiet --python /app/.venv/bin/python psycopg2-binary==2.9.13
      superset db upgrade >/dev/null
      superset fab create-admin --username admin --firstname Ana --lastname Lyst --email admin@example.com --password Igloo-e2e-1 >/dev/null
      superset init >/dev/null
      exec /usr/bin/run-server.sh' >/dev/null
fi

echo "loading data"
compose run --rm -T psql -X -q -v ON_ERROR_STOP=1 -v sf=0.01 -f /sql/load-tpch.sql >/dev/null
# BI tools connect as a reader, as reporting accounts should.
compose run --rm -T psql -X -q -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE USER bi WITH PASSWORD 'bi-password-1' ROLE reader;
CREATE VIEW revenue_by_month AS
  SELECT date_trunc('month', o_orderdate) AS month, sum(o_totalprice) AS revenue FROM lake.public.orders GROUP BY 1;
COMMENT ON TABLE orders IS 'One row per order';
SQL

failures=0
if wants npgsql; then
  echo "Npgsql (Power BI)"
  for version in $npgsql_versions; do
    if docker run --rm --network "$network" -e PGHOST=igloo -e PGUSER=bi -e PGPASSWORD=bi-password-1 \
      -e DOTNET_CLI_TELEMETRY_OPTOUT=1 -e DOTNET_NOLOGO=1 -v "$root/scripts/clients/npgsql:/src:ro" "$dotnet_image" \
      sh -c "cp -r /src /tmp/test && cd /tmp/test && dotnet run -c Release -p:NpgsqlVersion=$version 2>&1"; then
      echo "  ok    Npgsql $version"
    else
      echo "  FAIL  Npgsql $version"
      failures=$((failures + 1))
    fi
  done
fi
for tool in metabase superset; do
  wants "$tool" || continue
  echo "$tool"
  if docker run --rm --network "$network" -e IGLOO_USER=bi -e IGLOO_PASSWORD=bi-password-1 \
    -v "$root/scripts/clients:/clients:ro" python:3.12-slim python /clients/bi_test.py "$tool"; then
    echo "  ok    $tool"
  else
    echo "  FAIL  $tool"
    failures=$((failures + 1))
  fi
done

if [ "$failures" -ne 0 ]; then
  echo "$failures tool(s) failed"
  exit 1
fi
echo "all BI tools passed"
