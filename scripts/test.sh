#!/usr/bin/env bash
# Runs the Go tests in a container, with a throwaway PostgreSQL for the
# tests that need one, and DuckDB's extensions (kept in the volume
# igloo-duckdb-extensions) for the tests that use them. Only Docker is
# required.
#
#   scripts/test.sh [go test args...]
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
go_image="${GO_IMAGE:-golang:1.27-bookworm}"
name="igloo-test-$$"

cleanup() {
  docker rm -f -v "$name-db" >/dev/null 2>&1 || true
  docker network rm "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$name" >/dev/null
docker run -d --name "$name-db" --network "$name" -e POSTGRES_PASSWORD=test postgres:17 >/dev/null
until docker exec "$name-db" pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done

mounts=(-v "$root":/src -v igloo-go-mod:/go/pkg/mod -v igloo-go-build:/root/.cache/go-build -v igloo-duckdb-extensions:/ext)
# Downloads only what the volume does not have yet.
docker run --rm "${mounts[@]}" -w /src "$go_image" go run -tags duckdb_arrow ./cmd/igloo install-extensions /ext

docker run --rm --network "$name" \
  -e IGLOO_TEST_DATABASE_URL="postgres://postgres:test@$name-db:5432/postgres" \
  -e IGLOO_TEST_EXTENSION_DIR=/ext \
  "${mounts[@]}" -w /src "$go_image" go test -tags duckdb_arrow "${@:-./...}"
