#!/usr/bin/env bash
# Builds the rb extension, rb.duckdb_extension, for this Linux machine's
# architecture, and its SHA-256 beside it:
#
#   extensions/rb/build.sh OUT_DIR
#
# It compiles against DuckDB's headers at the version duckdb-go's bindings
# were built from, and links no DuckDB of its own: duckdb-go links the
# bindings' static libraries with -rdynamic, so the igloo binary (and every Go
# test binary) exports DuckDB's symbols, and the extension's calls resolve to
# the very DuckDB that loads it. It is half a megabyte rather than 34 MB, and
# cannot run a different DuckDB than igloo's; it loads only into such a
# binary. Needs go, gcc, g++, git and sha256sum, as golang:bookworm has.
# DuckDB's headers are fetched once, checked against the pinned commit, into
# $RB_CACHE (default ~/.cache/igloo); DUCKDB_HEADERS points at a src/include
# directory to use instead.
set -euo pipefail

# DuckDB v1.5.6, which duckdb-go-bindings v0.10506.0 is built from. Bump all
# three together with third_party/duckdb-go.
duckdb_tag=v1.5.6
duckdb_commit=069cc9f9b5be802405797faecc284961b07c70ef
bindings_version=v0.10506.0

out=${1:?usage: extensions/rb/build.sh OUT_DIR}
here=$(dirname "$(readlink -f "$0")")
root=$(readlink -f "$here/../..")

arch=$(go env GOARCH)
case "$arch" in
amd64 | arm64) ;;
*)
	echo "rb: unsupported architecture $arch" >&2
	exit 1
	;;
esac
platform="linux_$arch"

lib_module="github.com/duckdb/duckdb-go-bindings/lib/linux-$arch"
have=$(go -C "$root" list -m -f '{{.Version}}' "$lib_module")
if [ "$have" != "$bindings_version" ]; then
	echo "rb: go.mod has $lib_module $have; this script pins DuckDB $duckdb_tag for $bindings_version." >&2
	echo "rb: update duckdb_tag, duckdb_commit and bindings_version in $0" >&2
	exit 1
fi

headers=${DUCKDB_HEADERS:-}
if [ -z "$headers" ]; then
	cache=${RB_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/igloo}
	headers="$cache/duckdb-$duckdb_commit/src/include"
	if [ ! -f "$headers/duckdb.hpp" ]; then
		mkdir -p "$cache"
		tmp=$(mktemp -d "$cache/duckdb-fetch.XXXXXX")
		git -c advice.detachedHead=false clone --quiet --depth 1 --filter=blob:none --sparse \
			--branch "$duckdb_tag" https://github.com/duckdb/duckdb "$tmp/duckdb"
		git -C "$tmp/duckdb" sparse-checkout set src/include
		got=$(git -C "$tmp/duckdb" rev-parse HEAD)
		if [ "$got" != "$duckdb_commit" ]; then
			echo "rb: DuckDB $duckdb_tag is commit $got, not the pinned $duckdb_commit" >&2
			exit 1
		fi
		mv "$tmp/duckdb" "$cache/duckdb-$duckdb_commit"
	fi
fi

# object files go in OUT_DIR/.obj, overwritten by each build
obj="$out/.obj"
mkdir -p "$obj"

src="$here/src"
croaring="$root/third_party/croaring"
gcc -std=c11 -O3 -fPIC -fvisibility=hidden -I"$croaring" -c "$croaring/roaring.c" -o "$obj/roaring.o"
pids=()
for f in rb_extension rb_aggregate rb_scalar rb_memory rb_rewrite; do
	g++ -std=c++17 -O2 -fPIC -DNDEBUG -DDUCKDB_BUILD_LOADABLE_EXTENSION -I"$headers" -I"$croaring" -I"$src" \
		-c "$src/$f.cpp" -o "$obj/$f.o" &
	pids+=($!)
done
for pid in "${pids[@]}"; do
	wait "$pid"
done

g++ -shared -Wl,-s -o "$obj/rb.so" "$obj/roaring.o" "$obj/rb_extension.o" "$obj/rb_aggregate.o" "$obj/rb_scalar.o" \
	"$obj/rb_memory.o" "$obj/rb_rewrite.o" \
	-lstdc++ -lm

# DuckDB's extension footer, unsigned: 22 bytes of WebAssembly custom-section
# header, eight 32-byte fields in reverse order (magic "4", platform, DuckDB
# version, extension version, ABI), then 256 bytes of signature.
field() { printf '%s' "$1"; head -c $((32 - ${#1})) /dev/zero; }
{
	printf '\0\223\004\020duckdb_signature\200\004'
	for f in "" "" "" CPP 1 "$duckdb_tag" "$platform" 4; do field "$f"; done
	head -c 256 /dev/zero
} >>"$obj/rb.so"

mv "$obj/rb.so" "$out/rb.duckdb_extension"
sha256sum "$out/rb.duckdb_extension" | cut -d' ' -f1 >"$out/rb.duckdb_extension.sha256"
echo "built $out/rb.duckdb_extension ($platform, DuckDB $duckdb_tag): $(cat "$out/rb.duckdb_extension.sha256")"
