#!/usr/bin/env bash
# Renders the chart with compute pools and checks the objects it makes,
# and that it refuses pools that cannot work, and admin passwords Igloo
# would refuse, without printing them. Needs only helm; without it,
# run it through a container:
#
#   HELM="docker run --rm -v $PWD/deploy/helm:/w -w /w alpine/helm:4.3.0" deploy/helm/template-test.sh
set -euo pipefail

cd "$(dirname "$0")"
# HELM may be a command with arguments, so it is split into words.
# shellcheck disable=SC2206
helm=(${HELM:-helm})
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

failures=0
pass() { printf '  ok    %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; failures=$((failures + 1)); }

render() { "${helm[@]}" template igloo igloo "$@"; }

# doc KIND NAME prints the rendered object of that kind and name.
doc() {
  awk -v kind="kind: $1" -v name="  name: $2" '
    /^---/ { if (k && n) printf "%s", buf; buf = ""; k = 0; n = 0; next }
    { buf = buf $0 "\n" }
    $0 == kind { k = 1 }
    $0 == name { n = 1 }
    END { if (k && n) printf "%s", buf }' "$work/pools.yaml"
}

# env KIND NAME VAR prints the value of an environment variable.
env_of() { doc "$1" "$2" | grep -A1 -E "^ +- name: $3\$" | sed -n 's/^ *value: //p'; }

# check NAME WANT GOT
check() { if [ "$3" = "$2" ]; then pass "$1"; else fail "$1: got [$3], want [$2]"; fi; }

echo "compute pools"
render -f test-values/pools.yaml >"$work/pools.yaml"
check "a pool's Deployment knows its pool" '"acme"' "$(env_of Deployment igloo-pool-acme IGLOO_POOL)"
check "and its price" '"2.72"' "$(env_of Deployment igloo-pool-acme IGLOO_PRICE_PER_HOUR)"
check "a pool leaves maintenance to the default pool" '"false"' "$(env_of Deployment igloo-pool-acme IGLOO_MAINTENANCE)"
check "DuckDB's memory follows the pool's limit" '"52428MiB"' "$(env_of Deployment igloo-pool-acme IGLOO_DUCKDB_MEMORY_LIMIT)"
check "a pool adds its extraEnv to the release's" 'analytics|2h' \
  "$(env_of Deployment igloo-pool-acme IGLOO_CATALOG_SCHEMA)|$(env_of Deployment igloo-pool-acme IGLOO_STATEMENT_TIMEOUT)"
check "a pool that scales to zero leaves its replicas to its gateway" "" "$(doc Deployment igloo-pool-acme | grep -E '^  replicas:' || true)"
check "a pool's pods are its own" "app.kubernetes.io/component: pool-acme" \
  "$(doc Deployment igloo-pool-acme | sed -n '/^  selector:/,/^  template:/p' | grep -o 'app.kubernetes.io/component: .*')"
check "a pool's scheduling" "node-pool: igloo-large" "$(doc Deployment igloo-pool-acme | grep -o 'node-pool: .*')"
check "another pool's replicas" "  replicas: 2" "$(doc Deployment igloo-pool-reporting | grep -E '^  replicas:')"
check "a pool without a price takes the release's" '"0.68"' "$(env_of Deployment igloo-pool-reporting IGLOO_PRICE_PER_HOUR)"
check "the release's own Deployment is the default pool" "|\"true\"" \
  "$(env_of Deployment igloo IGLOO_POOL)|$(env_of Deployment igloo IGLOO_MAINTENANCE)"
check "a pool's gateway scales the pool's Deployment" '"igloo-pool-acme"|"igloo-pool-acme-server:5432"' \
  "$(env_of Deployment igloo-pool-acme-gateway IGLOO_GATEWAY_K8S_DEPLOYMENT)|$(env_of Deployment igloo-pool-acme-gateway IGLOO_GATEWAY_BACKEND)"
# The names the gateway may scale, whether the list is written inline or one per line.
check "and only it" '"igloo-pool-acme"' \
  "$(doc Role igloo-pool-acme-gateway | sed -n '/resourceNames:/,/verbs:/p' | grep -v 'verbs:' | grep -o '"[^"]*"' | tr '\n' ' ' | sed 's/ $//')"
check "a pool's Service leads to its gateway" "app.kubernetes.io/component: pool-acme-gateway" \
  "$(doc Service igloo-pool-acme | sed -n '/^  selector:/,/^  ports:/p' | grep -o 'app.kubernetes.io/component: .*')"
check "a pool's Service has its own type" "  type: LoadBalancer" "$(doc Service igloo-pool-reporting | grep -E '^  type:')"
check "the catalog accepts every pool's pods" "server pool-acme pool-reporting" \
  "$(doc NetworkPolicy igloo-catalog | grep -o 'component: .*' | sed 's/component: //' | grep -v catalog | tr '\n' ' ' | sed 's/ $//')"
check "a pool's SQL port accepts only its gateway" "pool-acme-gateway" \
  "$(doc NetworkPolicy igloo-pool-acme | sed -n '/from:/,$p' | grep -o 'component: .*' | head -1 | sed 's/component: //')"
check "pools share the release's Secret" "1" "$(grep -c '^kind: Secret' "$work/pools.yaml")"

echo "without pools"
render --set postgresql.enabled=true --set objectStorage.enabled=true >"$work/plain.yaml"
if grep -q 'pool-' "$work/plain.yaml"; then fail "no pool objects without pools"; else pass "no pool objects without pools"; fi

echo "pools that cannot work"
bundled=(--set postgresql.enabled=true --set objectStorage.enabled=true)
for invalid in "pools[0].name=default" "pools[0].name=Acme" "pools[0].name=a,pools[1].name=a" \
  "pools[0].name=a,pools[0].replicaCount=0,pools[0].scaleToZero.enabled=true" "pools[0].name=a,pools[0].colour=blue"; do
  if out=$(render "${bundled[@]}" --set "$invalid" 2>&1); then
    fail "refuses $invalid"
  elif grep -qi 'pool' <<<"$out"; then
    pass "refuses $invalid"
  else
    fail "refuses $invalid with a message about pools: $out"
  fi
done

echo "the admin password"
# A generated password is long enough for Igloo to create the account with.
generated=$(render "${bundled[@]}" | sed -n 's/^  admin-password: //p' | base64 -d | wc -c | tr -d ' ')
check "the chart generates 32 characters" "32" "$generated"
if render "${bundled[@]}" --set admin.password=Kq7vT2mXz9LpR4wN >/dev/null 2>&1; then
  pass "accepts 16 characters"
else
  fail "accepts 16 characters"
fi
# 15 characters fail the schema, the rest the chart's own check; neither
# error may print the password.
for invalid in Kq7vT2mXz9LpR4w change-me please-change-me-0123456789 Change_Me_Now_0123456789 \
  qqqqqqqqqqqqqqqqqqqq "admin.user=warehouse_administrator,admin.password=warehouse_administrator"; do
  case "$invalid" in
    *=*) set_args=(--set "$invalid") password=${invalid##*=} ;;
    *) set_args=(--set "admin.password=$invalid") password=$invalid ;;
  esac
  if out=$(render "${bundled[@]}" "${set_args[@]}" 2>&1); then
    fail "refuses admin password $invalid"
  elif grep -qF -- "$password" <<<"$out"; then
    fail "refuses admin password $invalid without printing it: $out"
  elif grep -q 'admin' <<<"$out"; then
    pass "refuses admin password $invalid"
  else
    fail "refuses admin password $invalid with a message about it: $out"
  fi
done

if [ "$failures" -ne 0 ]; then
  echo "$failures check(s) failed"
  exit 1
fi
echo "all checks passed"
