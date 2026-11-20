#!/usr/bin/env bash
# Builds Igloo's postgres extension, postgres_scanner.duckdb_extension, for
# this machine's system (Linux or macOS) and architecture, and its SHA-256
# beside it:
#
#   extensions/postgres/build.sh OUT_DIR
#
# It is DuckDB's postgres extension (DuckDB-Postgres) at the commit DuckDB
# v1.5.6 ships, with Igloo's patches (patches/, applied in order), built by
# DuckDB's own build with the extension's own configuration. PostgreSQL and
# Snowflake connections are attached with it. Like rb, it links no DuckDB of
# its own (EXTENSION_STATIC_BUILD=0, as DuckDB's build calls it): it loads
# only into a binary that exports v1.5.6's symbols, as igloo does, and the
# build compiles the extension's own sources and nothing of DuckDB's. Its
# libraries (libpq, OpenSSL) come from vcpkg at the commit DuckDB's extension
# tooling builds with.
#
# The sources, vcpkg's packages, the build tree and the compiler cache stay in
# $POSTGRES_CACHE (default ~/.cache/igloo/postgres): the first build takes a
# few minutes, most of them OpenSSL's and libpq's, later ones seconds. It
# needs the tools the Dockerfile's postgres stage installs, and is usually
# run there; on macOS, the Xcode command line tools, CMake, Ninja,
# pkg-config and the autotools (autoconf, automake, libtool,
# autoconf-archive), and ccache if it is there.
set -euo pipefail

# DuckDB-Postgres as DuckDB v1.5.6 builds it
# (.github/config/extensions/postgres_scanner.cmake), the database-connector
# submodule that commit pins, and the vcpkg DuckDB's extension tooling builds
# extensions with (extension-ci-tools at 72e76e99; extensions/iceberg/build.sh
# builds with the same one). Bump them together with extensions/rb's DuckDB.
postgres_commit=318dabb2474fc3789b0199301a2e661e19c1b4fc
connector_commit=0a8505f775dae7bb30edd37f848d5975bf72884e
duckdb_tag=v1.5.6
duckdb_commit=069cc9f9b5be802405797faecc284961b07c70ef
vcpkg_commit=84bab45d415d22042bd0b9081aea57f362da3f35

out=${1:?usage: extensions/postgres/build.sh OUT_DIR}
here=$(dirname "$(readlink -f "$0")")
cache=${POSTGRES_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/igloo/postgres}
mkdir -p "$cache" "$out"
out=$(readlink -f "$out")

# The trees may belong to another user than the one building (a mounted
# cache): git describe, which names the versions in the extension's footer,
# would refuse to read them.
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0='*'

# fetch DIR URL COMMIT: DIR holds URL's tree at COMMIT, fetched once.
fetch() {
	local dir=$1 url=$2 commit=$3
	if [ "$(git -C "$dir" rev-parse HEAD 2>/dev/null)" = "$commit" ]; then
		return
	fi
	mkdir -p "$dir"
	git -C "$dir" init -q
	git -C "$dir" remote remove origin 2>/dev/null || true
	git -C "$dir" remote add origin "$url"
	git -C "$dir" fetch -q --depth 1 origin "$commit"
	git -C "$dir" -c advice.detachedHead=false checkout -q -f FETCH_HEAD
}

fetch "$cache/duckdb-postgres-$postgres_commit" https://github.com/duckdb/duckdb-postgres "$postgres_commit"
fetch "$cache/database-connector-$connector_commit" https://github.com/duckdb/database-connector "$connector_commit"
duckdb="$cache/duckdb-$duckdb_commit"
if [ "$(git -C "$duckdb" rev-parse HEAD 2>/dev/null)" != "$duckdb_commit" ]; then
	git -c advice.detachedHead=false clone -q --depth 1 --branch "$duckdb_tag" https://github.com/duckdb/duckdb "$duckdb.tmp"
	got=$(git -C "$duckdb.tmp" rev-parse HEAD)
	if [ "$got" != "$duckdb_commit" ]; then
		echo "postgres: DuckDB $duckdb_tag is commit $got, not the pinned $duckdb_commit" >&2
		exit 1
	fi
	mv "$duckdb.tmp" "$duckdb"
fi
vcpkg="$cache/vcpkg-$vcpkg_commit"
fetch "$vcpkg" https://github.com/microsoft/vcpkg "$vcpkg_commit"
if [ ! -x "$vcpkg/vcpkg" ]; then
	"$vcpkg/bootstrap-vcpkg.sh" -disableMetrics
fi

# The patched tree. Only files whose content differs are copied, with a new
# modification time, so the build tree recompiles exactly what changed,
# a file a dropped patch puts back included.
src="$cache/src"
mkdir -p "$src"
rsync -rlc --delete --exclude /database-connector "$cache/duckdb-postgres-$postgres_commit/" "$src/"
ln -sfn "$cache/database-connector-$connector_commit" "$src/database-connector"
for p in "$here"/patches/*.patch; do
	(cd "$src" && git apply "$p") || {
		echo "postgres: $(basename "$p") does not apply" >&2
		exit 1
	}
done

export VCPKG_ROOT="$vcpkg"
export VCPKG_DEFAULT_BINARY_CACHE="$cache/vcpkg-binary"
export CCACHE_DIR="$cache/ccache"
# The dependencies' compiles and the extension's, at 2 GiB each.
# shellcheck source=extensions/jobs.sh
. "$here/../jobs.sh"
jobs=$(build_jobs $((2 << 30)))
export CMAKE_BUILD_PARALLEL_LEVEL=$jobs VCPKG_MAX_CONCURRENCY=$jobs
echo "postgres: $jobs compiles at once"
mkdir -p "$VCPKG_DEFAULT_BINARY_CACHE"
# DuckDB's build, of the extension's target alone: with EXTENSION_STATIC_BUILD
# off the extension does not link DuckDB, so nothing of DuckDB's is compiled
# but the program that names the platform in the extension's footer.
build="$cache/build"
cmake -G Ninja -S "$duckdb" -B "$build" -DCMAKE_BUILD_TYPE=Release \
	-DEXTENSION_STATIC_BUILD=0 -DDUCKDB_EXTENSION_CONFIGS="$src/extension_config.cmake" \
	-DBUILD_SHELL=0 -DBUILD_UNITTESTS=0 \
	-DVCPKG_BUILD=1 -DCMAKE_TOOLCHAIN_FILE="$vcpkg/scripts/buildsystems/vcpkg.cmake" -DVCPKG_MANIFEST_DIR="$src"
cmake --build "$build" --target postgres_scanner_loadable_extension

cp "$build/extension/postgres_scanner/postgres_scanner.duckdb_extension" "$out/postgres_scanner.duckdb_extension"
if command -v sha256sum >/dev/null; then sha256=(sha256sum); else sha256=(shasum -a 256); fi
"${sha256[@]}" "$out/postgres_scanner.duckdb_extension" | cut -d' ' -f1 >"$out/postgres_scanner.duckdb_extension.sha256"
echo "built $out/postgres_scanner.duckdb_extension (DuckDB $duckdb_tag): $(cat "$out/postgres_scanner.duckdb_extension.sha256")"
