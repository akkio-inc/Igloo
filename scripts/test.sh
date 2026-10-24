#!/usr/bin/env bash
# Runs the Go tests in a container, with a throwaway PostgreSQL for the
# tests that need one, a throwaway MySQL that the tests of connections to
# other databases read alongside it, DuckDB's extensions (kept in the volume
# igloo-duckdb-extensions) and Igloo's rb extension (igloo-rb-extension) for
# the tests that use them. Only Docker is required.
#
#   scripts/test.sh [go test args...]
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
go_image="${GO_IMAGE:-golang:1.27-bookworm}"
name="igloo-test-$$"

cleanup() {
  # -v: the databases' data volumes too.
  docker rm -f -v "$name-db" "$name-mysql" >/dev/null 2>&1 || true
  docker network rm "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ready CONTAINER COMMAND...: waits until COMMAND succeeds, failing with the
# container's log if it stops or takes more than 3 minutes.
ready() {
  local container=$1
  shift
  for _ in $(seq 180); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null)" != true ]; then
      echo "$container stopped:" >&2
      docker logs --tail 20 "$container" >&2
      return 1
    fi
    sleep 1
  done
  echo "$container was not ready after 3 minutes" >&2
  return 1
}

docker network create "$name" >/dev/null
docker run -d --name "$name-db" --network "$name" -e POSTGRES_PASSWORD=test postgres:17 >/dev/null
docker run -d --name "$name-mysql" --network "$name" -e MYSQL_ROOT_PASSWORD=test -e MYSQL_DATABASE=igloo_test mysql:8.4 >/dev/null
ready "$name-db" docker exec "$name-db" pg_isready -U postgres

mounts=(-v "$root":/src -v igloo-go-mod:/go/pkg/mod -v igloo-go-build:/root/.cache/go-build -v igloo-duckdb-extensions:/ext
  -v igloo-rb-extension:/rb)
# Downloads only what the volume does not have yet.
docker run --rm "${mounts[@]}" -w /src "$go_image" go run -tags duckdb_arrow ./cmd/igloo install-extensions /ext
# Igloo's own rb extension, which the engine tests load (seconds; DuckDB's
# headers stay in the volume).
docker run --rm "${mounts[@]}" -e RB_CACHE=/rb/cache -w /src "$go_image" extensions/rb/build.sh /rb

# MySQL first starts without networking to initialize; wait for TCP.
ready "$name-mysql" docker exec "$name-mysql" mysql -h127.0.0.1 -uroot -ptest -e 'SELECT 1' igloo_test

# IGLOO_TEST_CPUS=n gives the tests n CPUs' time (docker run --cpus), to
# see how they fare on a smaller machine, such as CI's 2-core runners.
cpus=()
if [ -n "${IGLOO_TEST_CPUS:-}" ]; then
  cpus=(--cpus "$IGLOO_TEST_CPUS")
fi

docker run --rm ${cpus[@]+"${cpus[@]}"} --network "$name" \
  -e IGLOO_TEST_DATABASE_URL="postgres://postgres:test@$name-db:5432/postgres" \
  -e IGLOO_TEST_MYSQL_URL="mysql://root:test@$name-mysql:3306/igloo_test" \
  -e IGLOO_TEST_EXTENSION_DIR=/ext \
  -e IGLOO_TEST_RB_EXTENSION=/rb/rb.duckdb_extension \
  "${mounts[@]}" -w /src "$go_image" go test -tags duckdb_arrow "${@:-./...}"
