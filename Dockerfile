# syntax=docker/dockerfile:1

FROM golang:1.27-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
COPY third_party ./third_party
RUN --mount=type=cache,target=/go/pkg/mod go mod download

ARG VERSION=dev
COPY . .
# duckdb_arrow: DuckDB's Arrow results, for Arrow Flight SQL.
# minicore_disabled: leaves out the native library Snowflake's Go driver
# would otherwise embed, and write out and load at runtime, only to report
# its version to Snowflake (Igloo turns it off at runtime too).
# The rb extension (extensions/rb) is built first, in the same step as the
# binary, which carries the file's SHA-256 and refuses any other file: the
# two cannot come from different builds.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/root/.cache/igloo \
    extensions/rb/build.sh /out/rb && \
    go build -tags duckdb_arrow,minicore_disabled -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X github.com/akkio-inc/igloo/internal/engine.rbSHA256=$(cat /out/rb/rb.duckdb_extension.sha256)" \
      -o /out/igloo ./cmd/igloo

# Bake DuckDB's extensions into the image so the server never downloads code
# at runtime, then check that this binary loads its rb extension.
RUN /out/igloo install-extensions /out/extensions && \
    mkdir -p /out/extensions/igloo /out/tmp /out/cache /out/empty && \
    cp /out/rb/rb.duckdb_extension /out/extensions/igloo/ && \
    /out/igloo check-extensions /out/extensions

# distroless/cc has the glibc DuckDB links against, CA certificates for
# HTTPS object storage, and no shell.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /out/igloo /usr/local/bin/igloo
COPY --from=build /out/extensions /usr/local/share/igloo/extensions
COPY --from=build --chown=nonroot:nonroot /out/tmp /var/lib/igloo/tmp
# The read cache of object storage files. Mount a volume here to keep it
# across container restarts; a new named volume takes this ownership.
COPY --from=build --chown=nonroot:nonroot /out/cache /var/lib/igloo/cache

ENV IGLOO_DUCKDB_EXTENSION_DIR=/usr/local/share/igloo/extensions \
    IGLOO_DUCKDB_TEMP_DIR=/var/lib/igloo/tmp \
    IGLOO_CACHE_DIR=/var/lib/igloo/cache

# DuckDB's Iceberg writer makes sure a relative directory named "data" exists
# before CREATE TABLE AS writes to object storage. It exists here, owned by
# root, so the check passes and nothing can be written to it. The working
# directory itself is not writable either (see internal/engine).
COPY --from=build /out/empty /data
WORKDIR /
# 7433: distributed-execution workers (igloo worker), TLS keyed by
# IGLOO_WORKER_SECRET.
EXPOSE 5432 7433 8080 8181 8443 8444 31337
# Igloo takes up to 3 minutes to start (waiting for its databases, the
# catalog and its connections' databases); the first check that passes ends
# the start period early.
HEALTHCHECK --interval=5s --timeout=3s --start-period=3m --retries=5 \
    CMD ["/usr/local/bin/igloo", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/igloo"]
