#!/usr/bin/env bash
# Runs the Go tests in a container, with a throwaway PostgreSQL for the
# tests that need one. Only Docker is required.
#
#   scripts/test.sh [go test args...]
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
go_image="${GO_IMAGE:-golang:1.27-bookworm}"
name="igloo-test-$$"

cleanup() {
  docker rm -f "$name-db" >/dev/null 2>&1 || true
  docker network rm "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$name" >/dev/null
docker run -d --name "$name-db" --network "$name" -e POSTGRES_PASSWORD=test postgres:17 >/dev/null
until docker exec "$name-db" pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done

docker run --rm --network "$name" \
  -e IGLOO_TEST_DATABASE_URL="postgres://postgres:test@$name-db:5432/postgres" \
  -v "$root":/src -v igloo-go-mod:/go/pkg/mod -v igloo-go-build:/root/.cache/go-build \
  -w /src "$go_image" go test -tags duckdb_arrow "${@:-./...}"
