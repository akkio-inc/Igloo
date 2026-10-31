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

echo "exchanges between workers"
# docf FILE KIND NAME prints the rendered object of that kind and name in FILE.
docf() {
  awk -v kind="kind: $2" -v name="  name: $3" '
    /^---/ { if (k && n) printf "%s", buf; buf = ""; k = 0; n = 0; next }
    { buf = buf $0 "\n" }
    $0 == kind { k = 1 }
    $0 == name { n = 1 }
    END { if (k && n) printf "%s", buf }' "$1"
}
# admitted FILE prints the components the workers' NetworkPolicy admits to their task port.
admitted() {
  docf "$1" NetworkPolicy igloo-workers | sed -n '/ingress:/,/port: http/p' | grep -o 'component: .*' | sed 's/component: //' |
    tr '\n' ' ' | sed 's/ $//'
}
render "${bundled[@]}" --set workers.enabled=true >"$work/object.yaml"
render "${bundled[@]}" --set workers.enabled=true --set workers.exchanges=local >"$work/local.yaml"
check "exchanges through object storage by default" '"object"' \
  "$(docf "$work/object.yaml" Deployment igloo | grep -A1 -E '^ +- name: IGLOO_EXCHANGES$' | sed -n 's/^ *value: //p')"
check "workers take tasks from igloo pods only" "server" "$(admitted "$work/object.yaml")"
check "workers.exchanges local sets IGLOO_EXCHANGES" '"local"' \
  "$(docf "$work/local.yaml" Deployment igloo | grep -A1 -E '^ +- name: IGLOO_EXCHANGES$' | sed -n 's/^ *value: //p')"
check "and lets workers reach each other's task port" "server worker" "$(admitted "$work/local.yaml")"
spill_env() { docf "$1" Deployment igloo-workers | grep -A1 -E "^ +- name: $2\$" | sed -n 's/^ *value: //p'; }
check "workers spill to 90% of their volume" '"92160MiB"|' \
  "$(spill_env "$work/object.yaml" IGLOO_DUCKDB_MAX_TEMP_SIZE)|$(spill_env "$work/object.yaml" IGLOO_EXCHANGE_MAX_SIZE)"
check "with local exchanges, 60% spill and 30% exchanges" '"61440MiB"|"30720MiB"' \
  "$(spill_env "$work/local.yaml" IGLOO_DUCKDB_MAX_TEMP_SIZE)|$(spill_env "$work/local.yaml" IGLOO_EXCHANGE_MAX_SIZE)"
if render "${bundled[@]}" --set workers.enabled=true --set workers.exchanges=nvme >/dev/null 2>&1; then
  fail "refuses workers.exchanges=nvme"
else
  pass "refuses workers.exchanges=nvme"
fi

echo "the large worker"
render -f test-values/large-worker.yaml >"$work/large.yaml"
envf() { docf "$1" "$2" "$3" | grep -A1 -E "^ +- name: $4\$" | sed -n 's/^ *value: //p'; }
lw() { envf "$work/large.yaml" Deployment igloo "$1"; }
check "igloo pods find the large worker" '"igloo-workers-large:7433"|kubernetes|"igloo-workers-large"' \
  "$(lw IGLOO_LARGE_WORKER)|$(lw IGLOO_LARGE_WORKER_COMPUTE)|$(lw IGLOO_LARGE_WORKER_K8S_DEPLOYMENT)"
check "and know its size and price" '"60"|"367001MiB"|"4.35"' \
  "$(lw IGLOO_LARGE_WORKER_CORES)|$(lw IGLOO_LARGE_WORKER_MEMORY)|$(lw IGLOO_LARGE_WORKER_PRICE_PER_HOUR)"
check "and the workers' price" '"0.31"' "$(lw IGLOO_WORKER_PRICE_PER_HOUR)"
check "and the machines, for list prices" '"m7gd.4xlarge"|"true"|"us-east-2"|"r7gd.16xlarge"|' \
  "$(lw IGLOO_WORKER_INSTANCE_TYPE)|$(lw IGLOO_WORKER_SPOT)|$(lw IGLOO_CLOUD_REGION)|$(lw IGLOO_LARGE_WORKER_INSTANCE_TYPE)|$(lw IGLOO_LARGE_WORKER_SPOT)"
check "a pool on demand says so" '' "$(envf "$work/large.yaml" Deployment igloo-pool-reporting IGLOO_WORKER_SPOT)"
check "its DuckDB limit is the one igloo pods estimate with" "$(lw IGLOO_LARGE_WORKER_MEMORY)" \
  "$(envf "$work/large.yaml" Deployment igloo-workers-large IGLOO_DUCKDB_MEMORY_LIMIT)"
check "it starts with no pods and runs igloo worker" '  replicas: 0|args: ["worker"]' \
  "$(docf "$work/large.yaml" Deployment igloo-workers-large | grep -E '^  replicas:')|$(docf "$work/large.yaml" Deployment igloo-workers-large | grep -o 'args: .*')"
check "with the workers' secret" "$(docf "$work/large.yaml" Deployment igloo-workers | grep -A3 'name: IGLOO_WORKER_SECRET' | tr -d ' \n')" \
  "$(docf "$work/large.yaml" Deployment igloo-workers-large | grep -A3 'name: IGLOO_WORKER_SECRET' | tr -d ' \n')"
check "its own resources, volumes and scheduling" 'cpu: "60"|sizeLimit: 2000Gi|sizeLimit: 800Gi|igloo.dev/node: large-worker|key: igloo.dev/large-worker' \
  "$(docf "$work/large.yaml" Deployment igloo-workers-large | grep -o 'cpu: .*')|$(docf "$work/large.yaml" Deployment igloo-workers-large | grep -o 'sizeLimit: [0-9]*Gi' | tr '\n' '|')$(docf "$work/large.yaml" Deployment igloo-workers-large | grep -o 'igloo.dev/node: .*')|$(docf "$work/large.yaml" Deployment igloo-workers-large | grep -o 'key: igloo.dev/.*')"
check "the workers keep theirs" 'igloo.dev/node: workers' "$(docf "$work/large.yaml" Deployment igloo-workers | grep -o 'igloo.dev/node: .*')"
check "a headless Service of its own" "  clusterIP: None|app.kubernetes.io/component: worker-large" \
  "$(docf "$work/large.yaml" Service igloo-workers-large | grep -E '^  clusterIP:')|$(docf "$work/large.yaml" Service igloo-workers-large | sed -n '/^  selector:/,/^  ports:/p' | grep -o 'app.kubernetes.io/component: .*')"
check "igloo pods may scale it, and only it" 'resourceNames: ["igloo-workers-large"]' \
  "$(docf "$work/large.yaml" Role igloo-workers-large | grep -o 'resourceNames: .*')"
check "it takes tasks from igloo pods and exchanges with the workers" "server worker worker-large" \
  "$(docf "$work/large.yaml" NetworkPolicy igloo-workers-large | sed -n '/ingress:/,/port: http/p' | grep -o 'component: .*' | sed 's/component: //' | tr '\n' ' ' | sed 's/ $//')"
check "and the workers exchange with it" "server worker worker-large" \
  "$(docf "$work/large.yaml" NetworkPolicy igloo-workers | sed -n '/ingress:/,/port: http/p' | grep -o 'component: .*' | sed 's/component: //' | tr '\n' ' ' | sed 's/ $//')"
pl() { envf "$work/large.yaml" Deployment igloo-pool-acme "$1"; }
check "a pool has a large worker of its own" '"igloo-pool-acme-workers-large:7433"|"31"|"300GiB"|"3.1"' \
  "$(pl IGLOO_LARGE_WORKER)|$(pl IGLOO_LARGE_WORKER_CORES)|$(pl IGLOO_LARGE_WORKER_MEMORY)|$(pl IGLOO_LARGE_WORKER_PRICE_PER_HOUR)"
check "which the pool's gateway stops with the pool" '"igloo-pool-acme-workers-large"' \
  "$(envf "$work/large.yaml" Deployment igloo-pool-acme-gateway IGLOO_GATEWAY_K8S_LARGE_WORKER_DEPLOYMENT)"
check "and may scale" '"igloo-pool-acme" "igloo-pool-acme-workers" "igloo-pool-acme-workers-large"' \
  "$(docf "$work/large.yaml" Role igloo-pool-acme-gateway | sed -n '/resourceNames:/,/verbs:/p' | grep -v 'verbs:' | grep -o '"[^"]*"' | tr '\n' ' ' | sed 's/ $//')"
check "a pool can turn it off" "0|" \
  "$(grep -c 'name: igloo-pool-reporting-workers-large' "$work/large.yaml")|$(envf "$work/large.yaml" Deployment igloo-pool-reporting IGLOO_LARGE_WORKER)"
if grep -q 'workers-large\|IGLOO_LARGE_WORKER\|IGLOO_WORKER_PRICE' "$work/object.yaml"; then
  fail "no large worker or worker price by default"
else
  pass "no large worker or worker price by default"
fi
if out=$(render "${bundled[@]}" --set workers.large.enabled=true 2>&1); then
  fail "refuses workers.large without workers"
elif grep -q 'workers.large.enabled needs workers.enabled' <<<"$out"; then
  pass "refuses workers.large without workers"
else
  fail "refuses workers.large without workers with a message about it: $out"
fi

echo "the read cache"
cv() { envf "$1" Deployment igloo "$2"; }
volume() { docf "$1" Deployment igloo | sed -n '/^      volumes:/,$p' | sed -n "/- name: $2\$/,/- name: /p"; }
check "igloo pods cache on a 50Gi emptyDir, 90% of it for the cache, which shares the disk" 'sizeLimit: 50Gi|"true"|"46080MiB"|"false"' \
  "$(volume "$work/plain.yaml" cache | grep -o 'sizeLimit: .*')|$(cv "$work/plain.yaml" IGLOO_CACHE_EPHEMERAL)|$(cv "$work/plain.yaml" IGLOO_CACHE_SIZE)|$(cv "$work/plain.yaml" IGLOO_CACHE_DEDICATED)"
check "mounted at the image's cache directory, with a writable /tmp" "mountPath: /var/lib/igloo/cache|mountPath: /tmp" \
  "$(docf "$work/plain.yaml" Deployment igloo | grep -o 'mountPath: /var/lib/igloo/cache')|$(docf "$work/plain.yaml" Deployment igloo | grep -o 'mountPath: /tmp$')"
render "${bundled[@]}" --set cacheVolume.sizeLimit=20Gi >"$work/cache-sized.yaml"
check "a smaller emptyDir" 'sizeLimit: 20Gi|"18432MiB"' \
  "$(volume "$work/cache-sized.yaml" cache | grep -o 'sizeLimit: .*')|$(cv "$work/cache-sized.yaml" IGLOO_CACHE_SIZE)"
render "${bundled[@]}" --set cacheVolume.sizeLimit= >"$work/cache-unsized.yaml"
check "an emptyDir without a limit, the cache sized from its disk" 'emptyDir:|' \
  "$(volume "$work/cache-unsized.yaml" cache | grep -o 'emptyDir:.*')|$(cv "$work/cache-unsized.yaml" IGLOO_CACHE_SIZE)"
render "${bundled[@]}" --set cacheVolume.type=ephemeral --set cacheVolume.sizeLimit=100Gi --set cacheVolume.storageClass=gp3 >"$work/cache-ephemeral.yaml"
check "an ephemeral volume of the pod's own, which the cache has to itself" 'storageClassName: gp3|storage: 100Gi|"92160MiB"|"true"|"true"' \
  "$(volume "$work/cache-ephemeral.yaml" cache | grep -o 'storageClassName: .*')|$(volume "$work/cache-ephemeral.yaml" cache | grep -o 'storage: .*')|$(cv "$work/cache-ephemeral.yaml" IGLOO_CACHE_SIZE)|$(cv "$work/cache-ephemeral.yaml" IGLOO_CACHE_EPHEMERAL)|$(cv "$work/cache-ephemeral.yaml" IGLOO_CACHE_DEDICATED)"
claim=(--set cacheVolume.type=persistentVolumeClaim --set cacheVolume.existingClaim=igloo-cache --set replicaCount=1 --set strategy.type=Recreate)
render "${bundled[@]}" "${claim[@]}" >"$work/cache-claim.yaml"
check "a claim, which outlives the pod, for one pod at a time" 'claimName: igloo-cache|"false"|"true"|type: Recreate|' \
  "$(volume "$work/cache-claim.yaml" cache | grep -o 'claimName: .*')|$(cv "$work/cache-claim.yaml" IGLOO_CACHE_EPHEMERAL)|$(cv "$work/cache-claim.yaml" IGLOO_CACHE_DEDICATED)|$(docf "$work/cache-claim.yaml" Deployment igloo | sed -n '/^  strategy:/,/^  selector:/p' | grep -o 'type: .*')|$(docf "$work/cache-claim.yaml" Deployment igloo | grep -o 'rollingUpdate' || true)"
if render "${bundled[@]}" "${claim[@]}" --set 'pools[0].name=a' --set 'pools[0].replicaCount=1' --set 'pools[0].cacheVolume.existingClaim=a-cache' >/dev/null 2>&1; then
  pass "a pool with a claim of its own"
else
  fail "a pool with a claim of its own"
fi
for invalid in "cacheVolume.type=ephemeral,cacheVolume.sizeLimit=" "cacheVolume.type=persistentVolumeClaim,replicaCount=1,strategy.type=Recreate" \
  "cacheVolume.type=persistentVolumeClaim,cacheVolume.existingClaim=c,strategy.type=Recreate" \
  "cacheVolume.type=persistentVolumeClaim,cacheVolume.existingClaim=c,replicaCount=1" \
  "cacheVolume.type=persistentVolumeClaim,cacheVolume.existingClaim=c,replicaCount=1,strategy.type=Recreate,autoscaling.enabled=true" \
  "cacheVolume.type=persistentVolumeClaim,cacheVolume.existingClaim=c,replicaCount=1,strategy.type=Recreate,pools[0].name=a,pools[0].replicaCount=1" \
  "cacheVolume.type=nfs"; do
  if out=$(render "${bundled[@]}" --set "$invalid" 2>&1); then
    fail "refuses $invalid"
  elif grep -q 'cacheVolume' <<<"$out"; then
    pass "refuses $invalid"
  else
    fail "refuses $invalid with a message about the cache volume: $out"
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
