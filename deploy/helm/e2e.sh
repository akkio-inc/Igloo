#!/usr/bin/env bash
# End-to-end test of the Igloo Helm chart on a throwaway kind cluster.
#
#   deploy/helm/e2e.sh                  build igloo:dev, install, test, delete the cluster
#   KEEP_CLUSTER=1 deploy/helm/e2e.sh   leave the cluster running afterwards
#   SKIP_BUILD=1 deploy/helm/e2e.sh     use the existing igloo:dev image
#
# Only Docker is required. When kind, helm and kubectl are all on PATH they
# are used directly; otherwise the script builds a small toolbox image with
# pinned versions and runs itself inside it, talking to the host's Docker.
#
# What it checks, in bundled mode (PostgreSQL and RustFS in the release):
#   - installing with the chart's defaults is refused with a clear message
#   - all pods become Ready with 2 igloo replicas, read-only root filesystems,
#     in a namespace that enforces the "restricted" Pod Security Standard
#   - SQL through the Service; writes through one pod are read through another
#   - a reader user cannot write (SQLSTATE 42501)
#   - /readyz and /metrics; Lakekeeper, PostgreSQL and RustFS are unreachable
#     from pods outside the release (NetworkPolicy)
#   - the Iceberg REST catalog through the Service: a token, a listing and
#     storage credentials from the bundled store's STS
#   - `helm test`
#   - `helm upgrade` to 3 replicas keeps the generated admin password, leaves
#     the running pods alone and keeps the data
#   - `helm uninstall` removes everything but the generated Secret and the
#     PVCs, and reinstalling finds the data again
#   - scale to zero: with scaleToZero.enabled the SQL Service leads to the
#     gateway and queries work through it; the gateway scales igloo to zero
#     after suspendAfter (20s here), `helm upgrade` leaves it there, and the
#     next connection starts it again; only the gateway reaches igloo's SQL
#     port; turning scale to zero off restores replicaCount pods
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
CHART="$REPO_ROOT/deploy/helm/igloo"
VALUES="$REPO_ROOT/deploy/helm/test-values/e2e.yaml"

CLUSTER=${CLUSTER:-igloo-e2e}
NAMESPACE=${NAMESPACE:-igloo-e2e}
RELEASE=${RELEASE:-igloo}
IMAGE=${IMAGE:-igloo:dev}
SKIP_BUILD=${SKIP_BUILD:-0}
KEEP_CLUSTER=${KEEP_CLUSTER:-0}
TIMEOUT=${TIMEOUT:-10m}

HELM_VERSION=v4.3.0
KIND_VERSION=v0.33.0
KIND_NODE_IMAGE=kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
TOOLBOX_IMAGE=igloo-helm-e2e-toolbox:helm-$HELM_VERSION-kind-$KIND_VERSION

log() { printf '\n==> %s\n' "$*"; }
pass() { printf 'PASS: %s\n' "$*"; }
fail() { printf '\nFAIL: %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Outside the toolbox: build the image, then run here or in the toolbox.
# ---------------------------------------------------------------------------

build_toolbox() {
  log "Building toolbox image $TOOLBOX_IMAGE (helm $HELM_VERSION, kind $KIND_VERSION, kubectl from $KIND_NODE_IMAGE)"
  docker build -q -t "$TOOLBOX_IMAGE" - >/dev/null <<EOF
FROM golang:1.27-bookworm AS tools
RUN CGO_ENABLED=0 GOBIN=/out go install helm.sh/helm/v4/cmd/helm@$HELM_VERSION \
 && CGO_ENABLED=0 GOBIN=/out go install sigs.k8s.io/kind@$KIND_VERSION
FROM $KIND_NODE_IMAGE AS node
FROM docker:29-cli
RUN apk add --no-cache bash
COPY --from=tools /out/ /usr/local/bin/
COPY --from=node /usr/bin/kubectl /usr/local/bin/kubectl
EOF
}

if [[ "${IGLOO_E2E_IN_TOOLBOX:-0}" != 1 ]]; then
  command -v docker >/dev/null || fail "docker is required"
  if [[ "$SKIP_BUILD" != 1 ]]; then
    log "Building $IMAGE from $REPO_ROOT"
    docker build -t "$IMAGE" "$REPO_ROOT"
  fi
  if ! command -v kind >/dev/null || ! command -v helm >/dev/null || ! command -v kubectl >/dev/null; then
    build_toolbox
    log "kind, helm or kubectl is not on PATH; continuing inside $TOOLBOX_IMAGE"
    status=0
    docker run --rm --init \
      -v /var/run/docker.sock:/var/run/docker.sock \
      -v "$REPO_ROOT:$REPO_ROOT:ro" -w "$REPO_ROOT" \
      -e IGLOO_E2E_IN_TOOLBOX=1 -e SKIP_BUILD=1 \
      -e CLUSTER="$CLUSTER" -e NAMESPACE="$NAMESPACE" -e RELEASE="$RELEASE" \
      -e IMAGE="$IMAGE" -e KEEP_CLUSTER="$KEEP_CLUSTER" -e TIMEOUT="$TIMEOUT" \
      "$TOOLBOX_IMAGE" bash "$REPO_ROOT/deploy/helm/e2e.sh" || status=$?
    # The inner run deletes the cluster itself; this covers it being killed.
    if [[ "$KEEP_CLUSTER" != 1 ]]; then
      docker run --rm -v /var/run/docker.sock:/var/run/docker.sock "$TOOLBOX_IMAGE" \
        kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
    fi
    exit "$status"
  fi
fi

# ---------------------------------------------------------------------------
# The test itself.
# ---------------------------------------------------------------------------

WORKDIR=$(mktemp -d)
export KUBECONFIG="$WORKDIR/kubeconfig"
IMAGE_REPOSITORY=${IMAGE%:*}
IMAGE_TAG=${IMAGE##*:}
# The chart's fullname: the release name when it contains "igloo".
if [[ "$RELEASE" == *igloo* ]]; then FULLNAME=$RELEASE; else FULLNAME=$RELEASE-igloo; fi
SERVER_PODS="app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=server"

k() { kubectl --namespace "$NAMESPACE" "$@"; }

diagnostics() {
  log "Diagnostics"
  k get deployments,pods,svc,pvc,networkpolicy -o wide || true
  k get events --sort-by=.lastTimestamp | tail -40 || true
  local pod
  for pod in $(k get pods -o name 2>/dev/null); do
    echo "--- logs of $pod"
    k logs "$pod" --all-containers --prefix --tail=60 2>&1 || true
    k logs "$pod" --all-containers --prefix --previous --tail=30 2>/dev/null || true
  done
}

cleanup() {
  local status=$?
  set +e
  if [[ $status -ne 0 ]]; then
    diagnostics
  fi
  if [[ $status -eq 0 ]]; then
    log "All checks passed"
  else
    printf '\nE2E FAILED (exit %s)\n' "$status" >&2
  fi
  if [[ "$KEEP_CLUSTER" == 1 ]]; then
    log "Keeping cluster $CLUSTER (delete it with: kind delete cluster --name $CLUSTER)"
  else
    log "Deleting cluster $CLUSTER"
    # Docker sometimes times out stopping a busy node; it usually works on a retry.
    local attempt
    for attempt in 1 2 3; do
      kind delete cluster --name "$CLUSTER" && break
      echo "kind delete cluster failed (attempt $attempt of 3)"
      sleep 10
    done
    if docker ps -aq --filter "label=io.x-k8s.kind.cluster=$CLUSTER" | grep -q .; then
      printf '\nCLEANUP FAILED: cluster %s still exists; delete it with: kind delete cluster --name %s\n' "$CLUSTER" "$CLUSTER" >&2
      [[ $status -ne 0 ]] || status=1
    fi
  fi
  rm -rf "$WORKDIR"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

helm_install() {
  local action=$1
  shift
  helm "$action" "$RELEASE" "$CHART" --namespace "$NAMESPACE" \
    --values "$VALUES" \
    --set image.repository="$IMAGE_REPOSITORY" --set image.tag="$IMAGE_TAG" \
    --wait --timeout "$TIMEOUT" "$@"
}

wait_ready() {
  local want=$1
  k wait --for=condition=Ready pod --selector "app.kubernetes.io/instance=$RELEASE" --timeout="$TIMEOUT" >/dev/null
  local ready
  ready=$(k get pods --selector "$SERVER_PODS" \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' | grep -c true || true)
  [[ "$ready" == "$want" ]] || fail "expected $want ready igloo pods, found $ready"
  pass "all pods Ready, $want igloo replicas"
}

# wait_service: wait until the admin logs in through the SQL Service. Ready pods reach
# clients through it a moment later: a reinstall gives the Service a new address, and a
# change of its selector new endpoints, and either refuses connections until kube-proxy
# routes it.
wait_service() {
  local deadline=$((SECONDS + 60))
  until sql "$FULLNAME" igloo "$(admin_password)" "SELECT 1" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || fail "igloo pods are Ready, but the Service refused logins for 60 s"
    sleep 1
  done
}

admin_password() {
  k get secret "$FULLNAME" -o jsonpath='{.data.admin-password}' | base64 -d
}

# sql HOST USER PASSWORD QUERY: run one statement with psql; prints the result.
# CONNECT_TIMEOUT (seconds, default 10) bounds the login, which through the
# scale-to-zero gateway includes waiting for igloo to start.
sql() {
  k exec psql-client -- env PGPASSWORD="$3" \
    psql "host=$1 port=5432 user=$2 dbname=igloo sslmode=disable connect_timeout=${CONNECT_TIMEOUT:-10}" \
    -X -v ON_ERROR_STOP=1 -At -c "$4"
}

expect_eq() {
  [[ "$2" == "$3" ]] || fail "$1: expected '$3', got '$2'"
  pass "$1 ($2)"
}

restart_counts() {
  k get pods --selector "$SERVER_PODS" \
    -o jsonpath='{range .items[*]}{.metadata.name}={.status.containerStatuses[0].restartCount} {end}'
}

# Igloo restarts are not chart failures, but they should be visible.
report_restarts() {
  local pod reason
  for pod in $(k get pods --selector "$SERVER_PODS" \
    -o jsonpath='{range .items[*]}{.metadata.name} {.status.containerStatuses[0].restartCount}{"\n"}{end}' | awk '$2 > 0 {print $1}'); do
    reason=$(k get pod "$pod" -o jsonpath='{.status.containerStatuses[0].lastState.terminated.reason} (exit {.status.containerStatuses[0].lastState.terminated.exitCode})')
    echo "note: igloo pod $pod restarted: $reason. Last lines of the previous container:"
    k logs "$pod" --previous --tail=5 2>&1 | while IFS= read -r line; do echo "    $line"; done
  done
}

server_pod_ips() {
  k get pods --selector "$SERVER_PODS" --field-selector status.phase=Running \
    -o jsonpath='{range .items[*]}{.status.podIP}{"\n"}{end}'
}

# The chart deletes the test pod as soon as it passes (hook-succeeded), which
# can race `helm test --logs`; its logs are read only when it fails, when the
# pod is kept.
run_helm_test() {
  if ! helm test "$RELEASE" --namespace "$NAMESPACE" --timeout "$TIMEOUT"; then
    k logs "$FULLNAME-test" 2>&1 || true
    fail "helm test"
  fi
  pass "helm test"
}

# After `helm uninstall`, only the generated Secret and the PVCs may remain.
check_uninstalled() {
  local left unexpected
  left=$(k get deployments,statefulsets,services,networkpolicies,poddisruptionbudgets,serviceaccounts,roles,rolebindings,configmaps,secrets,pvc \
    --selector "app.kubernetes.io/instance=$RELEASE" -o name | sort)
  printf 'Left behind:\n%s\n' "$left"
  unexpected=$(grep -v -e "^secret/$FULLNAME\$" -e '^persistentvolumeclaim/' <<<"$left" || true)
  [[ -z "$unexpected" ]] || fail "helm uninstall left: $unexpected"
  grep -q "^secret/$FULLNAME\$" <<<"$left" || fail "the generated Secret was deleted"
  pass "only the generated Secret and the PVCs remain"
  k wait --for=delete pod --selector "app.kubernetes.io/instance=$RELEASE" --timeout="$TIMEOUT" >/dev/null 2>&1 || true
}

# Desired and ready replicas of the igloo Deployment, as "desired/ready".
igloo_replicas() {
  local desired ready
  desired=$(k get deployment "$FULLNAME" -o jsonpath='{.spec.replicas}')
  ready=$(k get deployment "$FULLNAME" -o jsonpath='{.status.readyReplicas}')
  echo "${desired:-0}/${ready:-0}"
}

# Waits until the gateway has scaled igloo to zero and its pods are gone.
wait_suspended() {
  local started=$SECONDS deadline=$((SECONDS + 300))
  until [[ "$(igloo_replicas)" == 0/0 && -z "$(k get pods --selector "$SERVER_PODS" -o name)" ]]; do
    ((SECONDS < deadline)) || fail "igloo was not scaled to zero within 300s (desired/ready: $(igloo_replicas))"
    sleep 5
  done
  pass "igloo scaled to zero, its pods gone $((SECONDS - started))s later"
}

gateway_metrics() {
  k exec curl-client -- curl -fsS --max-time 10 "http://$FULLNAME-gateway-metrics:8080/metrics" ||
    fail "could not read the gateway's /metrics"
}

# metric_value METRICS SAMPLE: the sample's value, 0 when it is absent.
metric_value() {
  awk -v sample="$2" '$1 == sample {value = $2} END {print value + 0}' <<<"$1"
}

# --- Cluster ----------------------------------------------------------------

log "Creating kind cluster $CLUSTER"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
# On a busy Docker host the control plane sometimes misses kubeadm's
# deadlines; a second attempt usually works.
for attempt in 1 2 3; do
  kind create cluster --name "$CLUSTER" --image "$KIND_NODE_IMAGE" --kubeconfig "$KUBECONFIG" --wait 120s && break
  [[ $attempt -lt 3 ]] || fail "could not create kind cluster $CLUSTER"
  echo "kind create cluster failed (attempt $attempt of 3)"
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  sleep 10
done
if [[ "${IGLOO_E2E_IN_TOOLBOX:-0}" == 1 ]]; then
  # Reach the API server over the kind network rather than a host port.
  docker network connect kind "$(hostname)" 2>/dev/null || true
  kind get kubeconfig --internal --name "$CLUSTER" >"$KUBECONFIG"
fi
kubectl get nodes

log "Loading images into the cluster"
kind load docker-image "$IMAGE" --name "$CLUSTER"
# Preload the other images when Docker has them, to avoid registry pulls.
for image in $(helm template "$RELEASE" "$CHART" --values "$VALUES" --set image.repository="$IMAGE_REPOSITORY" --set image.tag="$IMAGE_TAG" |
  sed -n 's/^ *image: *"*\([^" ]*\)"*$/\1/p' | sort -u); do
  if [[ "$image" != "$IMAGE" ]] && docker image inspect "$image" >/dev/null 2>&1; then
    kind load docker-image "$image" --name "$CLUSTER" >/dev/null 2>&1 ||
      echo "note: could not preload $image; the node will pull it"
  fi
done

# Every pod in the namespace, the chart's and the test clients, must meet the
# "restricted" Pod Security Standard.
kubectl create namespace "$NAMESPACE"
kubectl label namespace "$NAMESPACE" pod-security.kubernetes.io/enforce=restricted pod-security.kubernetes.io/warn=restricted

# --- Defaults are refused -------------------------------------------------

log "Installing with the chart's defaults must fail"
if out=$(helm install "$RELEASE" "$CHART" --namespace "$NAMESPACE" --create-namespace 2>&1); then
  fail "helm install with default values succeeded"
fi
grep -q "missing required values" <<<"$out" || fail "unexpected error from helm install with defaults: $out"
pass "defaults refused: $(grep -m1 -o 'missing required values' <<<"$out")"

# --- Install ---------------------------------------------------------------

log "Installing $RELEASE in bundled mode (replicaCount=2)"
helm_install install
wait_ready 2
report_restarts
helm status "$RELEASE" --namespace "$NAMESPACE"

ro=$(k get deployment "$FULLNAME" -o jsonpath='{.spec.template.spec.containers[0].securityContext.readOnlyRootFilesystem}')
expect_eq "igloo runs with readOnlyRootFilesystem" "$ro" "true"

log "Starting client pods"
k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: psql-client
spec:
  securityContext: {runAsNonRoot: true, runAsUser: 999, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: psql
      image: postgres:17
      command: ["sleep", "infinity"]
      securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: ["ALL"]}}
---
apiVersion: v1
kind: Pod
metadata:
  name: curl-client
spec:
  securityContext: {runAsNonRoot: true, runAsUser: 100, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: curl
      image: curlimages/curl:8.16.0
      command: ["sleep", "infinity"]
      securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: ["ALL"]}}
EOF
k wait --for=condition=Ready pod/psql-client pod/curl-client --timeout="$TIMEOUT" >/dev/null

ADMIN_PASSWORD=$(admin_password)
[[ -n "$ADMIN_PASSWORD" ]] || fail "no admin password in Secret $FULLNAME"

# --- SQL through the Service ---------------------------------------------------

log "SQL through Service $FULLNAME"
sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "CREATE TABLE e2e_items (id INTEGER, note VARCHAR)" >/dev/null
sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "INSERT INTO e2e_items VALUES (1, 'through the service')" >/dev/null
expect_eq "row count through the Service" "$(sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "SELECT count(*) FROM e2e_items")" "1"
echo "DuckDB memory_limit: $(sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "SELECT current_setting('memory_limit')" || echo unavailable)"

# --- Replicas see each other's writes -----------------------------------------

log "Writes through one igloo pod are visible through the other"
ips=()
while IFS= read -r ip; do ips+=("$ip"); done < <(server_pod_ips)
[[ ${#ips[@]} -eq 2 ]] || fail "expected 2 igloo pod IPs, got ${#ips[@]}: ${ips[*]}"
pod_a=${ips[0]}
pod_b=${ips[1]}
sql "$pod_a" igloo "$ADMIN_PASSWORD" "INSERT INTO e2e_items VALUES (2, 'written via pod A')" >/dev/null
expect_eq "pod B ($pod_b) reads pod A's row" \
  "$(sql "$pod_b" igloo "$ADMIN_PASSWORD" "SELECT note FROM e2e_items WHERE id = 2")" "written via pod A"
sql "$pod_b" igloo "$ADMIN_PASSWORD" "INSERT INTO e2e_items VALUES (3, 'written via pod B')" >/dev/null
expect_eq "pod A ($pod_a) reads pod B's row" \
  "$(sql "$pod_a" igloo "$ADMIN_PASSWORD" "SELECT note FROM e2e_items WHERE id = 3")" "written via pod B"

# --- Users and roles -----------------------------------------------------------

log "A reader cannot write"
sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "CREATE USER r WITH PASSWORD 'reader-password' ROLE reader" >/dev/null
for ip in "$pod_a" "$pod_b"; do
  expect_eq "reader logs in through $ip and reads" "$(sql "$ip" r reader-password "SELECT count(*) FROM e2e_items")" "3"
done
state=$(k exec psql-client -- env PGPASSWORD=reader-password \
  psql "host=$FULLNAME port=5432 user=r dbname=igloo sslmode=disable connect_timeout=10" -X -At \
  -c "INSERT INTO e2e_items VALUES (4, 'by the reader')" -c '\echo SQLSTATE=:LAST_ERROR_SQLSTATE' 2>&1 | grep '^SQLSTATE=' || true)
expect_eq "reader INSERT is rejected" "$state" "SQLSTATE=42501"
expect_eq "row count after the rejected INSERT" "$(sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "SELECT count(*) FROM e2e_items")" "3"

# --- A query that spills ----------------------------------------------------

log "An aggregation larger than DuckDB's memory limit spills to the emptyDir"
restarts_before=$(restart_counts)
expect_eq "large GROUP BY completes" \
  "$(sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "SELECT count(*) FROM (SELECT range AS k, count(*) FROM range(40000000) GROUP BY range)")" "40000000"
expect_eq "igloo restarts during the query (none, so no OOM kill)" "$(restart_counts)" "$restarts_before"

# --- Health, metrics and network policy ---------------------------------------

log "Health and metrics"
expect_eq "/readyz on $pod_a" "$(k exec curl-client -- curl -fsS --max-time 10 "http://$pod_a:8080/readyz")" "ok"
metrics=$(k exec curl-client -- curl -fsS --max-time 10 "http://$pod_b:8080/metrics")
grep -q '^igloo_queries_total' <<<"$metrics" || fail "/metrics on $pod_b has no igloo_queries_total"
pass "/metrics on $pod_b ($(grep -c '^igloo_' <<<"$metrics") igloo_* samples)"
expect_eq "metrics Service" \
  "$(k exec curl-client -- curl -fsS --max-time 10 -o /dev/null -w '%{http_code}' "http://$FULLNAME-metrics:8080/metrics")" "200"
expect_eq "web console through Service $FULLNAME" \
  "$(k exec curl-client -- curl -fsS --max-time 10 -o /dev/null -w '%{http_code}' "http://$FULLNAME:8443/")" "200"

log "Iceberg REST catalog for other engines"
token=$(k exec curl-client -- curl -fsS --max-time 10 -d grant_type=client_credentials -d client_id=r \
  -d client_secret=reader-password "http://$FULLNAME:8181/v1/oauth/tokens" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
[[ -n "$token" ]] || fail "no Iceberg REST token for the reader through Service $FULLNAME"
pass "the reader gets an Iceberg REST token through Service $FULLNAME"
expect_eq "the reader lists the tables" "$(k exec curl-client -- curl -fsS --max-time 10 \
  -H "Authorization: Bearer $token" "http://$FULLNAME:8181/v1/namespaces/public/tables" | grep -o '"e2e_items"')" '"e2e_items"'
table=$(k exec curl-client -- curl -fsS --max-time 30 -H "Authorization: Bearer $token" \
  -H 'X-Iceberg-Access-Delegation: vended-credentials' "http://$FULLNAME:8181/v1/namespaces/public/tables/e2e_items")
grep -q '"s3.session-token":"[^"]' <<<"$table" || fail "no storage credentials for e2e_items: $(head -c 300 <<<"$table")"
pass "the reader gets storage credentials for e2e_items from the bundled store's STS"

log "Only igloo pods may reach Lakekeeper; only igloo and Lakekeeper reach PostgreSQL and RustFS"
if k exec curl-client -- curl -fsS --max-time 5 -o /dev/null "http://$FULLNAME-catalog:8181/health" 2>/dev/null; then
  fail "a pod outside the release reached Lakekeeper"
fi
pass "Lakekeeper is unreachable from other pods"
if k exec curl-client -- curl -fsS --max-time 5 -o /dev/null "http://$FULLNAME-object-storage:9000/health" 2>/dev/null; then
  fail "a pod outside the release reached the object store"
fi
pass "RustFS is unreachable from other pods"
if k exec psql-client -- pg_isready -h "$FULLNAME-postgresql" -t 5 >/dev/null 2>&1; then
  fail "a pod outside the release reached PostgreSQL"
fi
pass "PostgreSQL is unreachable from other pods"

# --- Workers ---------------------------------------------------------------------

WORKER_PODS="app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=worker"
worker_replicas() { k get deployment "$FULLNAME-workers" -o jsonpath='{.spec.replicas}'; }
# exchanges WHERE: statements that exchanged rows there, on every igloo pod.
exchanges() {
  local pod total=0 n
  for pod in $(k get pods --selector "$SERVER_PODS" -o jsonpath='{.items[*].status.podIP}'); do
    n=$(k exec curl-client -- curl -fsS --max-time 10 "http://$pod:8080/metrics" |
      sed -n "s/^igloo_distributed_exchanging_queries_total{exchanges=\"$1\"} //p")
    total=$((total + ${n:-0}))
  done
  echo "$total"
}
# in_mode MODE QUERY: QUERY's rows with distributed execution in MODE.
in_mode() {
  k exec psql-client -- env PGPASSWORD="$ADMIN_PASSWORD" \
    psql "host=$FULLNAME port=5432 user=igloo dbname=igloo sslmode=disable connect_timeout=10" \
    -X -v ON_ERROR_STOP=1 -At -c "SET distributed_execution = '$1'" -c "$2" | grep -v '^SET$'
}

log "No worker runs until a statement is worth one"
expect_eq "worker pods after the statements above" "$(worker_replicas)" "0"
expect_eq "worker pods that exist" "$(k get pods --selector "$WORKER_PODS" -o name | wc -l | tr -d ' ')" "0"

log "Workers start for a forced statement, and exchange its rows on their disks"
sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "CREATE TABLE e2e_big AS SELECT i AS id, i % 1000 AS g FROM range(1500000) t(i)" >/dev/null
sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "INSERT INTO e2e_big SELECT i AS id, i % 1000 AS g FROM range(1500000, 3000000) t(i)" >/dev/null
join="WITH s AS (SELECT id % 200000 AS k, sum(g) AS q FROM e2e_big GROUP BY 1),
  u AS (SELECT id AS k FROM e2e_big WHERE g < 100 UNION ALL SELECT id FROM e2e_big WHERE g BETWEEN 100 AND 200)
  SELECT count(*), sum(q) FROM s JOIN u ON s.k = u.k"
want="$(in_mode off "$join")"
before=$(exchanges local)
expect_eq "a forced join on workers returns the same rows" "$(in_mode on "$join")" "$want"
[[ "$(worker_replicas)" -ge 1 ]] || fail "the forced statement started no worker"
after=$(exchanges local)
[[ "$after" -gt "$before" ]] || fail "its exchanges were not on the workers' disks: $before, then $after local, $(exchanges object) through object storage"
pass "workers exchanged its rows on their disks, past the NetworkPolicy ($before, then $after)"

log "Nothing but igloo pods and workers reaches a worker's task port"
for ip in $(k get pods --selector "$WORKER_PODS" -o jsonpath='{.items[*].status.podIP}'); do
  if k exec curl-client -- curl -ksS --max-time 5 -o /dev/null "https://$ip:7433/" 2>/dev/null; then
    fail "a pod outside the release reached a worker's task port"
  fi
done
pass "a worker's task port is unreachable from other pods"

log "A worker lost holding exchanged rows: the statement's parts run again on the other"
slow="WITH s AS (SELECT id % 200000 AS k, sum(length(repeat(md5(id::VARCHAR), 200))) AS q FROM e2e_big GROUP BY 1),
  u AS (SELECT id AS k FROM e2e_big WHERE g < 100 UNION ALL SELECT id FROM e2e_big WHERE g BETWEEN 100 AND 200)
  SELECT count(*), sum(q) FROM s JOIN u ON s.k = u.k"
want="$(in_mode off "$slow")"
out="$(mktemp)"
in_mode on "$slow" >"$out" 2>&1 &
query=$!
sleep 3
victim=$(k get pods --selector "$WORKER_PODS" --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
[[ -n "$victim" ]] || fail "no worker runs to lose"
k delete pod "$victim" --grace-period=0 --force >/dev/null 2>&1
wait "$query" || true
got="$(cat "$out")"
find "$out" -delete
if [[ "$got" == "$want" ]]; then
  pass "the statement that lost $victim returns the same rows ($got)"
elif grep -q "a worker holding exchanged rows on its disk was lost" <<<"$got"; then
  pass "the statement that lost $victim fails saying so, with no wrong rows"
else
  fail "after losing $victim: [$got], want [$want]"
fi

log "Idle workers stop"
deadline=$((SECONDS + 240))
until [[ "$(worker_replicas)" == 0 ]]; do
  [[ "$SECONDS" -lt "$deadline" ]] || fail "workers still run, $(worker_replicas) of them, 4 minutes after their last task"
  sleep 5
done
pass "no worker pod is asked for once they are idle"

log "helm test"
run_helm_test

# --- Upgrade -------------------------------------------------------------------

log "Upgrading to replicaCount=3"
pods_before=$(k get pods --selector "$SERVER_PODS" -o jsonpath='{.items[*].metadata.name}')
helm_install upgrade --set replicaCount=3
wait_ready 3
[[ "$(admin_password)" == "$ADMIN_PASSWORD" ]] || fail "the admin password changed on upgrade"
pass "admin password unchanged after upgrade"
pods_after=$(k get pods --selector "$SERVER_PODS" -o jsonpath='{.items[*].metadata.name}')
for pod in $pods_before; do
  [[ " $pods_after " == *" $pod "* ]] || fail "pod $pod was replaced by an upgrade that only changed replicaCount"
done
pass "existing igloo pods kept running (generated secrets unchanged)"
expect_eq "rows after upgrade" "$(sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "SELECT count(*) FROM e2e_items")" "3"
ips=()
while IFS= read -r ip; do ips+=("$ip"); done < <(server_pod_ips)
for ip in "${ips[@]}"; do
  expect_eq "admin and reader log in through $ip" \
    "$(sql "$ip" igloo "$ADMIN_PASSWORD" "SELECT 1")/$(sql "$ip" r reader-password "SELECT max(id) FROM e2e_items")" "1/3"
done

# --- Uninstall and reinstall -----------------------------------------------------

log "Uninstalling"
helm uninstall "$RELEASE" --namespace "$NAMESPACE" --wait --timeout "$TIMEOUT"
check_uninstalled

log "Reinstalling finds the data again"
helm_install install
wait_ready 2
wait_service
[[ "$(admin_password)" == "$ADMIN_PASSWORD" ]] || fail "the admin password changed on reinstall"
pass "admin password unchanged after reinstall"
expect_eq "rows after reinstall" "$(sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "SELECT count(*) FROM e2e_items")" "3"
expect_eq "reader after reinstall" "$(sql "$FULLNAME" r reader-password "SELECT note FROM e2e_items WHERE id = 1")" "through the service"
helm uninstall "$RELEASE" --namespace "$NAMESPACE" --wait --timeout "$TIMEOUT"
check_uninstalled

# --- Scale to zero ------------------------------------------------------------------

SUSPEND_AFTER=20s
# A generous resumeTimeout: igloo starts slowly on busy CI machines.
SCALE_TO_ZERO=(--set scaleToZero.enabled=true --set scaleToZero.suspendAfter="$SUSPEND_AFTER"
  --set scaleToZero.resumeTimeout=10m)
# Logins through the gateway may wait that long for igloo to start.
RESUME_WAIT=600

log "Installing with scaleToZero.enabled=true, suspendAfter=$SUSPEND_AFTER"
helm_install install "${SCALE_TO_ZERO[@]}"
expect_eq "the SQL Service selects" \
  "$(k get service "$FULLNAME" -o jsonpath='{.spec.selector.app\.kubernetes\.io/component}')" "gateway"
echo "igloo replicas (desired/ready) after install: $(igloo_replicas)"

log "SQL through the gateway"
started=$SECONDS
expect_eq "rows through the gateway" \
  "$(CONNECT_TIMEOUT=$RESUME_WAIT sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "SELECT count(*) FROM e2e_items")" "3"
echo "the first query took $((SECONDS - started))s"
CONNECT_TIMEOUT=$RESUME_WAIT sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "INSERT INTO e2e_items VALUES (4, 'through the gateway')" >/dev/null

log "The gateway scales igloo to zero after $SUSPEND_AFTER without traffic"
wait_suspended
# The gateway counts a suspend once igloo's SQL port refuses connections,
# which can trail the pods' deletion by a few seconds.
deadline=$((SECONDS + 60))
until metrics=$(gateway_metrics) && suspends=$(metric_value "$metrics" igloo_gateway_suspends_total) && ((suspends >= 1)); do
  ((SECONDS < deadline)) || fail "igloo_gateway_suspends_total is still $suspends 60s after igloo's pods went"
  sleep 2
done
pass "igloo_gateway_suspends_total = $suspends"
resumes_before=$(metric_value "$metrics" 'igloo_gateway_resumes_total{result="success"}')

log "helm upgrade leaves igloo at zero"
helm_install upgrade "${SCALE_TO_ZERO[@]}"
expect_eq "igloo replicas (desired/ready) after helm upgrade" "$(igloo_replicas)" "0/0"

log "A client connection starts igloo again"
started=$SECONDS
expect_eq "rows after the resume" \
  "$(CONNECT_TIMEOUT=$RESUME_WAIT sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "SELECT count(*) FROM e2e_items")" "4"
pass "igloo answered $((SECONDS - started))s after the connection"
expect_eq "igloo Deployment's desired replicas" "$(k get deployment "$FULLNAME" -o jsonpath='{.spec.replicas}')" "2"
# Right after the query, while igloo stays up for suspendAfter: one ready pod.
ready_ip=$(k get pods --selector "$SERVER_PODS" \
  -o jsonpath='{range .items[*]}{.status.podIP} {.status.containerStatuses[0].ready}{"\n"}{end}' |
  awk '$2 == "true" && !found {print $1; found = 1}')
[[ -n "$ready_ip" ]] || fail "no ready igloo pod after the resume"
expect_eq "igloo /healthz on $ready_ip from another pod" \
  "$(k exec curl-client -- curl -fsS --max-time 10 "http://$ready_ip:8080/healthz")" "ok"
if k exec psql-client -- pg_isready -h "$ready_ip" -p 5432 -t 5 >/dev/null 2>&1; then
  fail "a pod other than the gateway reached igloo's SQL port at $ready_ip"
fi
pass "igloo's SQL port is unreachable from pods other than the gateway"
metrics=$(gateway_metrics)
expect_eq "igloo_gateway_resumes_total{result=\"success\"} increase" \
  "$(($(metric_value "$metrics" 'igloo_gateway_resumes_total{result="success"}') - resumes_before))" "1"
expect_eq "igloo_gateway_resumes_total{result=\"failure\"}" \
  "$(metric_value "$metrics" 'igloo_gateway_resumes_total{result="failure"}')" "0"
expect_eq "reader through the gateway" \
  "$(CONNECT_TIMEOUT=$RESUME_WAIT sql "$FULLNAME" r reader-password "SELECT note FROM e2e_items WHERE id = 4")" "through the gateway"

log "helm test through the gateway"
run_helm_test

log "Turning scale to zero off brings back replicaCount pods"
# Start from a stopped igloo, so no pod is shutting down during the upgrade.
wait_suspended
helm_install upgrade
k wait --for=delete pod --selector "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=gateway" \
  --timeout="$TIMEOUT" >/dev/null 2>&1 || true
wait_ready 2
wait_service
expect_eq "the SQL Service selects" \
  "$(k get service "$FULLNAME" -o jsonpath='{.spec.selector.app\.kubernetes\.io/component}')" "server"
left=$(k get "deployment/$FULLNAME-gateway" "serviceaccount/$FULLNAME-gateway" "role/$FULLNAME-gateway" \
  "rolebinding/$FULLNAME-gateway" "networkpolicy/$FULLNAME-gateway" "service/$FULLNAME-gateway-metrics" \
  "service/$FULLNAME-server" "service/$FULLNAME-headless" --ignore-not-found -o name)
[[ -z "$left" ]] || fail "turning scale to zero off left: $left"
pass "the gateway and its Services, RBAC and NetworkPolicy are gone"
expect_eq "rows through the Service without the gateway" \
  "$(sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "SELECT count(*) FROM e2e_items")" "4"

log "Uninstalling"
helm uninstall "$RELEASE" --namespace "$NAMESPACE" --wait --timeout "$TIMEOUT"
check_uninstalled
