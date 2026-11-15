#!/usr/bin/env bash
# Builds XGBoost's library for this machine's system (Linux or macOS) and
# architecture, for the igloo binary to link statically (internal/xgboost):
#
#   third_party/xgboost/build.sh OUT_DIR
#
# OUT_DIR gets include/xgboost (the C API's headers), lib/xgboost.o (the
# library, its dmlc-core, and on Linux its OpenMP runtime, in one object)
# and lib/pkgconfig/igloo-xgboost.pc, which tells cgo how to link it on this
# system: point PKG_CONFIG_PATH at OUT_DIR/lib/pkgconfig before building
# Igloo. The .pc file names its directory relative to itself, so OUT_DIR
# can be moved or copied whole.
#
# It is XGBoost at the release below, from its repository, built with CMake
# as its own build does, unchanged. On Linux it is built with OpenMP, with
# GCC's libgomp in the object, so the binary needs no library the runtime
# image lacks. On macOS it is built with OpenMP only when Homebrew's libomp
# is installed; otherwise without, and training and prediction run on one
# thread. The sources, the build tree and the compiler cache stay in
# $XGBOOST_CACHE (default ~/.cache/igloo/xgboost). Needs git, CMake, Ninja,
# a C++17 compiler and ccache if it is there: the tools the Dockerfile's
# xgboost stage installs; on macOS, the Xcode command line tools.
set -euo pipefail

# The XGBoost release Igloo links, and its commit. The tests' reference
# values come from XGBoost's Python package at the same release: make them
# again after a bump (internal/models/testdata/xgboost/make_reference.py).
version=3.4.2
commit=fdf0888bedddbd444d72994d845c59b3ca182c5b

out=${1:?usage: third_party/xgboost/build.sh OUT_DIR}
here=$(dirname "$(readlink -f "$0")")
cache=${XGBOOST_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/igloo/xgboost}
mkdir -p "$cache" "$out"
out=$(readlink -f "$out")

# The trees may belong to another user than the one building (a mounted
# cache).
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0='*'

src="$cache/src-$version"
if [ "$(git -C "$src" rev-parse HEAD 2>/dev/null)" != "$commit" ]; then
	git -c advice.detachedHead=false clone -q --depth 1 --branch "v$version" https://github.com/dmlc/xgboost "$src.tmp.$$"
	got=$(git -C "$src.tmp.$$" rev-parse HEAD)
	if [ "$got" != "$commit" ]; then
		echo "xgboost: v$version is commit $got, not the pinned $commit" >&2
		exit 1
	fi
	# dmlc-core, at the commit the release names; the other submodule is
	# for CUDA builds.
	git -C "$src.tmp.$$" submodule update -q --init --depth 1 dmlc-core
	mv "$src.tmp.$$" "$src"
fi

openmp=ON
libs="-lstdc++ -lm -lpthread -ldl"
case "$(uname -s)" in
Linux) ;;
Darwin)
	libomp=$(brew --prefix libomp 2>/dev/null || true)
	if [ -n "$libomp" ] && [ -e "$libomp/lib/libomp.dylib" ]; then
		libs="-L$libomp/lib -lomp -lc++"
	else
		openmp=OFF
		libs="-lc++"
		echo "xgboost: no libomp (brew install libomp), so XGBoost is built without OpenMP: it trains and predicts on one thread"
	fi
	;;
*)
	echo "xgboost: unsupported system $(uname -s)" >&2
	exit 1
	;;
esac

launcher=()
if command -v ccache >/dev/null; then
	export CCACHE_DIR="$cache/ccache"
	launcher=(-DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_C_COMPILER_LAUNCHER=ccache)
fi
# shellcheck source=extensions/jobs.sh
. "$here/../../extensions/jobs.sh"
jobs=$(build_jobs $((2 << 30)))
echo "xgboost: $jobs compiles at once"

build="$cache/build-$version-openmp-$openmp"
installed="$build/installed"
# Its error messages name the source file: by its path in the release, not
# on the machine that built it.
cmake -S "$src" -B "$build" -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$installed" \
	-DCMAKE_INSTALL_LIBDIR=lib -DBUILD_STATIC_LIB=ON -DUSE_OPENMP="$openmp" -DADD_PKGCONFIG=OFF \
	-DKEEP_BUILD_ARTIFACTS_IN_BINARY_DIR=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
	-DCMAKE_CXX_FLAGS="-fmacro-prefix-map=$src/=" -DCMAKE_C_FLAGS="-fmacro-prefix-map=$src/=" \
	${launcher[@]+"${launcher[@]}"} >/dev/null
cmake --build "$build" --parallel "$jobs"
cmake --install "$build" >/dev/null

# A linker takes from a static archive only the files something calls, and
# XGBoost registers its objectives, metrics and tree updaters in files
# nothing calls. Its link tags keep them for static builds, but miss
# survival:aft's (3.4.2), which then does not exist. So the archive is
# linked whole into one object, as its shared library has it all, with
# what it calls of dmlc-core and of libgomp: cgo takes an object file as it
# is, and refuses the flags that would do the same (--whole-archive,
# -force_load, -l:libgomp.a).
case "$(uname -s)" in
Linux)
	gomp=$("${CXX:-c++}" -print-file-name=libgomp.a)
	if [ ! -e "$gomp" ]; then
		echo "xgboost: no libgomp.a, GCC's OpenMP runtime" >&2
		exit 1
	fi
	parts=("-Wl,--whole-archive" "$installed/lib/libxgboost.a" "-Wl,--no-whole-archive" "$installed/lib/libdmlc.a" "$gomp")
	;;
Darwin) parts=("-Wl,-force_load,$installed/lib/libxgboost.a" "$installed/lib/libdmlc.a") ;;
esac
mkdir -p "$out/include" "$out/lib/pkgconfig"
"${CXX:-c++}" -r -nostdlib "${parts[@]}" -o "$out/lib/xgboost.o"
cp -R "$installed/include/xgboost" "$out/include/"

cat >"$out/lib/pkgconfig/igloo-xgboost.pc" <<EOF
prefix=\${pcfiledir}/../..
includedir=\${prefix}/include
libdir=\${prefix}/lib

Name: igloo-xgboost
Description: XGBoost $version, static, built by Igloo's third_party/xgboost/build.sh (OpenMP $openmp)
Version: $version
Cflags: -I\${includedir}
Libs: \${libdir}/xgboost.o $libs
EOF
echo "built XGBoost $version (OpenMP $openmp) in $out"
