#!/usr/bin/env bash
# Builds XGBoost's library, the Dockerfile's xgboost stage, into the volume
# igloo-xgboost, for the Go builds that run in containers (make lint,
# scripts/test.sh) to link: they mount it at /xgboost, with
# PKG_CONFIG_PATH=/xgboost/lib/pkgconfig. Docker's build cache keeps the
# stage until third_party/xgboost/build.sh changes.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
docker build -q --target xgboost -t igloo-xgboost-build "$root" >/dev/null
docker run --rm -v igloo-xgboost:/xgboost igloo-xgboost-build cp -R /out/xgboost/. /xgboost/
