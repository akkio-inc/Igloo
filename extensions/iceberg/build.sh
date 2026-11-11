#!/usr/bin/env bash
# Builds Igloo's iceberg extension, iceberg.duckdb_extension, for this
# machine's system (Linux or macOS) and architecture, and its SHA-256 beside
# it:
#
#   extensions/iceberg/build.sh OUT_DIR
#
# It is DuckDB's iceberg extension (DuckDB-Iceberg) at the commit DuckDB
# v1.5.6 ships, with Igloo's patches (patches/, applied in order), built as
# DuckDB's extension tooling builds it. Like rb, it links no DuckDB of its
# own: it loads only into a binary that exports v1.5.6's symbols, as igloo
# does. Its libraries (the AWS SDK, curl, OpenSSL, Avro, roaring) come from
# vcpkg at the commit DuckDB-Iceberg names.
#
# The sources, vcpkg's packages, the build tree and the compiler cache stay in
# $ICEBERG_CACHE (default ~/.cache/igloo/iceberg): the first build takes half
# an hour on a few cores, later ones minutes. It needs the tools the
# Dockerfile's iceberg stage installs, and is usually run there; on macOS,
# the Xcode command line tools, CMake, Ninja and pkg-config, and ccache if
# it is there.
set -euo pipefail

# DuckDB-Iceberg as DuckDB v1.5.6 builds it (.github/config/extensions/iceberg.cmake),
# the extension tooling that commit pins, and the vcpkg its vcpkg.json names
# as builtin-baseline. Bump them together with extensions/rb's DuckDB.
iceberg_commit=890b78a9cfae380396b435b033c27cdbdad04e42
ci_tools_commit=72e76e99cd7fee45a99739cd118ec2db64e034ec
duckdb_tag=v1.5.6
duckdb_commit=069cc9f9b5be802405797faecc284961b07c70ef
vcpkg_commit=84bab45d415d22042bd0b9081aea57f362da3f35

out=${1:?usage: extensions/iceberg/build.sh OUT_DIR}
here=$(dirname "$(readlink -f "$0")")
cache=${ICEBERG_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/igloo/iceberg}
mkdir -p "$cache" "$out"
out=$(readlink -f "$out")

# The trees may belong to another user than the one building (a mounted
# cache): git describe, which names the versions in the extension's footer,
# would refuse to read them.
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0='*'

# fetch DIR URL COMMIT [DEPTH]: DIR holds URL's tree at COMMIT, fetched once.
fetch() {
	local dir=$1 url=$2 commit=$3 depth=${4:-1}
	if [ "$(git -C "$dir" rev-parse HEAD 2>/dev/null)" = "$commit" ]; then
		return
	fi
	mkdir -p "$dir"
	git -C "$dir" init -q
	git -C "$dir" remote remove origin 2>/dev/null || true
	git -C "$dir" remote add origin "$url"
	if [ "$depth" = full ]; then
		git -C "$dir" fetch -q origin "$commit"
	else
		git -C "$dir" fetch -q --depth "$depth" origin "$commit"
	fi
	git -C "$dir" -c advice.detachedHead=false checkout -q -f FETCH_HEAD
}

fetch "$cache/duckdb-iceberg-$iceberg_commit" https://github.com/duckdb/duckdb-iceberg "$iceberg_commit"
fetch "$cache/extension-ci-tools-$ci_tools_commit" https://github.com/duckdb/extension-ci-tools "$ci_tools_commit"
duckdb="$cache/duckdb-$duckdb_commit"
if [ "$(git -C "$duckdb" rev-parse HEAD 2>/dev/null)" != "$duckdb_commit" ]; then
	git -c advice.detachedHead=false clone -q --depth 1 --branch "$duckdb_tag" https://github.com/duckdb/duckdb "$duckdb.tmp"
	got=$(git -C "$duckdb.tmp" rev-parse HEAD)
	if [ "$got" != "$duckdb_commit" ]; then
		echo "iceberg: DuckDB $duckdb_tag is commit $got, not the pinned $duckdb_commit" >&2
		exit 1
	fi
	mv "$duckdb.tmp" "$duckdb"
fi
# vcpkg reads the ports' older versions from its history.
vcpkg="$cache/vcpkg-$vcpkg_commit"
fetch "$vcpkg" https://github.com/microsoft/vcpkg "$vcpkg_commit" full
if [ ! -x "$vcpkg/vcpkg" ]; then
	"$vcpkg/bootstrap-vcpkg.sh" -disableMetrics
fi

# The patched tree. Only files whose content differs are copied, with a new
# modification time, so the build tree recompiles exactly what changed,
# a file a dropped patch puts back included.
src="$cache/src"
mkdir -p "$src"
rsync -rlc --delete --exclude /build --exclude /duckdb --exclude /extension-ci-tools \
	"$cache/duckdb-iceberg-$iceberg_commit/" "$src/"
ln -sfn "$duckdb" "$src/duckdb"
ln -sfn "$cache/extension-ci-tools-$ci_tools_commit" "$src/extension-ci-tools"
for p in "$here"/patches/*.patch; do
	(cd "$src" && git apply "$p") || {
		echo "iceberg: $(basename "$p") does not apply" >&2
		exit 1
	}
done

export VCPKG_ROOT="$vcpkg"
export VCPKG_TOOLCHAIN_PATH="$vcpkg/scripts/buildsystems/vcpkg.cmake"
export VCPKG_DEFAULT_BINARY_CACHE="$cache/vcpkg-binary"
export CCACHE_DIR="$cache/ccache"
export GEN=ninja
# DuckDB's and the dependencies' compiles, at 2 GiB each.
# shellcheck source=extensions/jobs.sh
. "$here/../jobs.sh"
jobs=$(build_jobs $((2 << 30)))
export CMAKE_BUILD_PARALLEL_LEVEL=$jobs VCPKG_MAX_CONCURRENCY=$jobs
echo "iceberg: $jobs compiles at once"
mkdir -p "$VCPKG_DEFAULT_BINARY_CACHE"
# DuckDB's CMake caches C++11 before the extension asks for the C++14 it is
# written in. No shell or test runner: only the extension is wanted.
make -C "$src" release EXT_FLAGS="-DCMAKE_CXX_STANDARD=14 -DBUILD_SHELL=0 -DBUILD_UNITTESTS=0"

cp "$src/build/release/extension/iceberg/iceberg.duckdb_extension" "$out/iceberg.duckdb_extension"
if command -v sha256sum >/dev/null; then sha256=(sha256sum); else sha256=(shasum -a 256); fi
"${sha256[@]}" "$out/iceberg.duckdb_extension" | cut -d' ' -f1 >"$out/iceberg.duckdb_extension.sha256"
echo "built $out/iceberg.duckdb_extension (DuckDB $duckdb_tag): $(cat "$out/iceberg.duckdb_extension.sha256")"
