#!/usr/bin/env bash
# Runs the Go tests in a container, with a throwaway PostgreSQL for the
# tests that need one, a throwaway MySQL that the tests of connections to
# other databases read alongside it, and DuckDB's extensions (kept in the
# volume igloo-duckdb-extensions) for the tests that use them. Only Docker
# is required.
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

docker network create "$name" >/dev/null
docker run -d --name "$name-db" --network "$name" -e POSTGRES_PASSWORD=test postgres:17 >/dev/null
docker run -d --name "$name-mysql" --network "$name" -e MYSQL_ROOT_PASSWORD=test -e MYSQL_DATABASE=igloo_test mysql:8.4 >/dev/null
until docker exec "$name-db" pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done

mounts=(-v "$root":/src -v igloo-go-mod:/go/pkg/mod -v igloo-go-build:/root/.cache/go-build -v igloo-duckdb-extensions:/ext)
# Downloads only what the volume does not have yet.
docker run --rm "${mounts[@]}" -w /src "$go_image" go run -tags duckdb_arrow ./cmd/igloo install-extensions /ext

# MySQL first starts without networking to initialize; wait for TCP.
until docker exec "$name-mysql" mysql -h127.0.0.1 -uroot -ptest -e 'SELECT 1' igloo_test >/dev/null 2>&1; do sleep 1; done

docker run --rm --network "$name" \
  -e IGLOO_TEST_DATABASE_URL="postgres://postgres:test@$name-db:5432/postgres" \
  -e IGLOO_TEST_MYSQL_URL="mysql://root:test@$name-mysql:3306/igloo_test" \
  -e IGLOO_TEST_EXTENSION_DIR=/ext \
  "${mounts[@]}" -w /src "$go_image" go test -tags duckdb_arrow "${@:-./...}"
