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
#   - `helm test`
#   - `helm upgrade` to 3 replicas keeps the generated admin password, leaves
#     the running pods alone and keeps the data
#   - `helm uninstall` removes everything but the generated Secret and the
#     PVCs, and reinstalling finds the data again
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
  k get pods,svc,pvc,networkpolicy -o wide || true
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

admin_password() {
  k get secret "$FULLNAME" -o jsonpath='{.data.admin-password}' | base64 -d
}

# sql HOST USER PASSWORD QUERY: run one statement with psql; prints the result.
sql() {
  k exec psql-client -- env PGPASSWORD="$3" \
    psql "host=$1 port=5432 user=$2 dbname=igloo sslmode=disable connect_timeout=10" \
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

# --- Cluster ----------------------------------------------------------------

log "Creating kind cluster $CLUSTER"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --image "$KIND_NODE_IMAGE" --kubeconfig "$KUBECONFIG" --wait 120s
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

log "helm test"
helm test "$RELEASE" --namespace "$NAMESPACE" --logs

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
left=$(k get deployments,statefulsets,services,networkpolicies,poddisruptionbudgets,serviceaccounts,configmaps,secrets,pvc \
  --selector "app.kubernetes.io/instance=$RELEASE" -o name | sort)
printf 'Left behind:\n%s\n' "$left"
unexpected=$(grep -v -e "^secret/$FULLNAME\$" -e '^persistentvolumeclaim/' <<<"$left" || true)
[[ -z "$unexpected" ]] || fail "helm uninstall left: $unexpected"
grep -q "^secret/$FULLNAME\$" <<<"$left" || fail "the generated Secret was deleted"
pass "only the generated Secret and the PVCs remain"
k wait --for=delete pod --selector "app.kubernetes.io/instance=$RELEASE" --timeout="$TIMEOUT" >/dev/null 2>&1 || true

log "Reinstalling finds the data again"
helm_install install
wait_ready 2
[[ "$(admin_password)" == "$ADMIN_PASSWORD" ]] || fail "the admin password changed on reinstall"
pass "admin password unchanged after reinstall"
expect_eq "rows after reinstall" "$(sql "$FULLNAME" igloo "$ADMIN_PASSWORD" "SELECT count(*) FROM e2e_items")" "3"
expect_eq "reader after reinstall" "$(sql "$FULLNAME" r reader-password "SELECT note FROM e2e_items WHERE id = 1")" "through the service"
helm uninstall "$RELEASE" --namespace "$NAMESPACE" --wait --timeout "$TIMEOUT"
