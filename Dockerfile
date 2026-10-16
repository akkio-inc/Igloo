# syntax=docker/dockerfile:1

FROM golang:1.27-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
COPY third_party ./third_party
RUN --mount=type=cache,target=/go/pkg/mod go mod download

ARG VERSION=dev
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/igloo ./cmd/igloo

# Bake DuckDB's extensions into the image so the server never downloads code
# at runtime.
RUN /out/igloo install-extensions /out/extensions && mkdir -p /out/tmp /out/empty

# distroless/cc has the glibc DuckDB links against, CA certificates for
# HTTPS object storage, and no shell.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /out/igloo /usr/local/bin/igloo
COPY --from=build /out/extensions /usr/local/share/igloo/extensions
COPY --from=build --chown=nonroot:nonroot /out/tmp /var/lib/igloo/tmp

ENV IGLOO_DUCKDB_EXTENSION_DIR=/usr/local/share/igloo/extensions \
    IGLOO_DUCKDB_TEMP_DIR=/var/lib/igloo/tmp

# DuckDB's Iceberg writer makes sure a relative directory named "data" exists
# before CREATE TABLE AS writes to object storage. It exists here, owned by
# root, so the check passes and nothing can be written to it. The working
# directory itself is not writable either (see internal/engine).
COPY --from=build /out/empty /data
WORKDIR /
EXPOSE 5432 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=5 \
    CMD ["/usr/local/bin/igloo", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/igloo"]
