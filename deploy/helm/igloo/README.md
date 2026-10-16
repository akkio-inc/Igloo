# Igloo Helm chart

Deploys [Igloo](https://github.com/akkio-inc/Igloo), an open-source SQL
warehouse: a PostgreSQL-protocol endpoint (port 5432) running DuckDB over
Apache Iceberg tables in object storage, with the
[Lakekeeper](https://github.com/lakekeeper/lakekeeper) Iceberg REST catalog
bundled in the release.

```
SQL clients ──5432──> igloo pods (Deployment, N replicas) ──8181──> Lakekeeper (catalog)
                         │  users: PostgreSQL schema "igloo_auth"     │  catalog: PostgreSQL schema "public"
                         └──────── table data ──> S3 / GCS / ADLS <───┘
```

Igloo pods are stateless apart from DuckDB spill files (an `emptyDir`), so you
can run several behind one Service; they share the catalog and the users
database. Lakekeeper runs without authentication and is reachable only from
igloo pods.

The OpenTofu modules in `deploy/aws`, `deploy/gcp` and `deploy/azure` install
this chart for you, wired to managed PostgreSQL, object storage and cloud
identities. Use the chart directly for other clusters, on-prem, or development.

## Requirements

- Kubernetes 1.25+ and Helm 3.8+ or Helm 4
- Igloo needs two things, from your infrastructure or bundled in the release:
  - **a warehouse location**: `storage.warehouseLocation` (`s3://`, `gs://` or
    `abfss://`), or `objectStorage.enabled=true` for a bundled RustFS
  - **a PostgreSQL database**: `metadataDatabase` / `catalog.database`, or
    `postgresql.enabled=true` for a bundled PostgreSQL 17

`helm install` with neither refuses to start and says what is missing. (So
that `helm lint` and `helm template` work on the bare chart, rendering the
defaults offline only prints a warning comment.)

## Install

### Everything bundled (development, small on-prem)

```sh
helm install igloo deploy/helm/igloo --namespace igloo --create-namespace \
  --set postgresql.enabled=true \
  --set objectStorage.enabled=true
```

The release then also runs a PostgreSQL StatefulSet and a RustFS
(S3-compatible) StatefulSet, each with one PersistentVolumeClaim, and creates
the bucket before Igloo creates its warehouse. Passwords and keys are
generated.

Connect:

```sh
export PGPASSWORD="$(kubectl -n igloo get secret igloo -o jsonpath='{.data.admin-password}' | base64 --decode)"
kubectl -n igloo port-forward service/igloo 5433:5432 &
psql "host=127.0.0.1 port=5433 user=igloo dbname=igloo"
```

### External PostgreSQL and Amazon S3

Create the Secrets, then install with a values file:

```sh
kubectl -n igloo create secret generic igloo-admin --from-literal=password="$(openssl rand -hex 16)"
kubectl -n igloo create secret generic igloo-database \
  --from-literal=url='postgresql://igloo:PASSWORD@db.example.com:5432/igloo?sslmode=require'
kubectl -n igloo create secret generic igloo-catalog-encryption --from-literal=encryption-key="$(openssl rand -hex 24)"
kubectl -n igloo create secret tls igloo-tls --cert=tls.crt --key=tls.key
```

```yaml
# values.yaml
admin:
  existingSecret: igloo-admin
metadataDatabase:
  existingSecret: igloo-database   # Lakekeeper uses it too unless catalog.database is set
storage:
  warehouseLocation: s3://my-bucket/warehouse
  s3:
    region: eu-west-1
serviceAccount:
  name: igloo
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/igloo
catalog:
  encryptionKey:
    existingSecret: igloo-catalog-encryption
  systemCredentials: aws            # Lakekeeper uses its own IAM role
  serviceAccount:
    name: igloo-catalog
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/igloo-catalog
tls:
  existingSecret: igloo-tls
service:
  type: LoadBalancer
  loadBalancerSourceRanges: ["203.0.113.0/24"]
```

```sh
helm install igloo deploy/helm/igloo --namespace igloo --values values.yaml
```

With static keys instead of IAM roles, create a Secret with keys
`access-key-id` and `secret-access-key`, set `storage.s3.existingSecret`, and
leave `catalog.systemCredentials` at `none`: Lakekeeper then uses the same keys.
For S3-compatible stores also set `storage.s3.endpoint` (`host:port`),
`storage.s3.urlStyle: path`, and `storage.s3.useSSL` as needed.

### Google Cloud Storage and Azure Data Lake Storage

- **GCS**: `storage.warehouseLocation: gs://bucket/warehouse`, an HMAC key in
  `storage.gcs.existingSecret` (keys `hmac-key-id`, `hmac-secret`) for DuckDB,
  and `catalog.systemCredentials: gcp` with Workload Identity on
  `catalog.serviceAccount`.
- **ADLS Gen2**: `storage.warehouseLocation:
  abfss://filesystem@account.dfs.core.windows.net/warehouse`. Either an account
  key in `storage.azure.existingSecret` (key `account-key`), or workload
  identity: `catalog.systemCredentials: azure`, the
  `azure.workload.identity/client-id` annotation on both service accounts and
  `azure.workload.identity/use: "true"` in `podLabels` and `catalog.podLabels`.

The test values in `deploy/helm/test-values/` mirror what the OpenTofu modules
pass for each cloud.

## Operating

- **Secrets the chart generates** (admin password, catalog encryption key,
  bundled PostgreSQL password, bundled object store keys) are stored in the
  Secret named after the release (`igloo`), are reused on every upgrade, and are
  kept by `helm uninstall` (`helm.sh/resource-policy: keep`), as are the
  PersistentVolumeClaims, so reinstalling finds the data again. To remove
  everything:
  `kubectl -n igloo delete secret igloo && kubectl -n igloo delete pvc -l app.kubernetes.io/instance=igloo`.
  Never change the catalog encryption key once the catalog holds data.
- **Values from `existingSecret`** always win over inline values. Pods do not
  restart when an existing Secret changes; run
  `kubectl -n igloo rollout restart deployment` after rotating one. The admin
  password is used only to create the admin on first start; change it later
  with `ALTER USER`.
- **Memory**: unless `duckdb.memoryLimit` is set, DuckDB gets 80% of
  `resources.limits.memory`, leaving at least 512Mi for the rest of the process
  (1Gi gives DuckDB 512MiB, 8Gi gives 6553MiB), and spills larger queries to
  `/var/lib/igloo/tmp` (`duckdb.spillVolume`). Keep a memory limit set: DuckDB
  allocates somewhat beyond its own limit, and spill files are written through
  the page cache, which counts against the container.
- **Startup**: on first start Igloo waits up to 3 minutes for PostgreSQL and
  the catalog, then bootstraps Lakekeeper and creates the warehouse; the
  startup probe allows 5 minutes.
- **Scaling**: `replicaCount`, or `autoscaling.enabled` for a CPU-based HPA,
  and `scaleToZero.enabled` to stop igloo pods when idle (see
  [below](#scale-to-zero)). Replicas prefer different nodes.
  `podDisruptionBudget` keeps `minAvailable` pods during node drains; disable
  it with a single replica.
- **Metrics**: Prometheus metrics at `:8080/metrics` through the
  `<release>-metrics` ClusterIP Service (never through the SQL Service), and
  the scale-to-zero gateway's through `<release>-gateway-metrics`;
  `metrics.serviceMonitor.enabled=true` creates a ServiceMonitor for both.
- **NetworkPolicy** (on by default) restricts ingress only: Lakekeeper accepts
  igloo pods; the bundled PostgreSQL and object store accept igloo and
  Lakekeeper pods; igloo pods accept the SQL and metrics ports (narrow them with
  `networkPolicy.sqlFrom` / `metricsFrom`). With scale to zero, those ports
  apply to the gateway, and igloo's SQL port accepts only the gateway. Egress
  is not restricted.
- **Security**: every pod runs as non-root with a read-only root filesystem,
  all capabilities dropped and the `RuntimeDefault` seccomp profile, which
  satisfies the `restricted` Pod Security Standard. Service account tokens are
  not mounted, except in the scale-to-zero gateway, which may only change the
  igloo Deployment's replica count.
- **One database for both**: Igloo keeps its users in schema `igloo_auth`,
  Lakekeeper its catalog in schema `public`. Lakekeeper's tables are
  unqualified, so the chart also pins its `search_path` to `public`
  (`PGOPTIONS=-c search_path=public`); otherwise a database role named like
  another schema could redirect them. If a connection pooler rejects startup
  options, override `PGOPTIONS` in `catalog.extraEnv`; just avoid naming the
  role `igloo_auth`.
- **GitOps tools that render with `helm template`** (Argo CD) cannot read the
  live Secret, so generated values would change on every sync: set
  `admin.existingSecret` and `catalog.encryptionKey.existingSecret` there.
- `helm test igloo` logs in through the SQL Service with psql.

## Scale to zero

With `scaleToZero.enabled=true` the release also runs Igloo's
[scale-to-zero gateway](../../../docs/scale-to-zero.md), and Igloo stops
when nobody uses it:

```
SQL clients ──5432──> Service igloo ──> gateway (1 pod) ──> Service igloo-server ──> igloo pods (0 to N)
                                            └── scales Deployment igloo through the Kubernetes API
```

- The SQL Service keeps its name, type and load balancer settings but leads
  to the gateway, so clients keep their address. The gateway copies bytes:
  TLS and passwords stay end to end between clients and Igloo.
- After `scaleToZero.suspendAfter` without client traffic, running
  statements or table maintenance, the gateway scales the igloo Deployment to
  zero. The next connection scales it back to `scaleToZero.replicas` (by
  default `replicaCount`) and waits until Igloo accepts it, at most
  `scaleToZero.resumeTimeout`. Clients need a connect timeout that allows for
  this (libpq's `connect_timeout`, for example). Open sessions end when Igloo
  stops; connection pools should check connections before use.
- The chart no longer sets the Deployment's replica count, so `helm upgrade`
  leaves it where the gateway put it. A new release starts with one igloo pod
  until the gateway first stops it.
- The saving comes from nodes: put igloo on a node pool the cluster
  autoscaler can shrink to zero (`nodeSelector`, `tolerations`), and the
  gateway on an always-on one (`scaleToZero.nodeSelector`, `tolerations`,
  `affinity`).
- With `autoscaling.enabled`, the HorizontalPodAutoscaler pauses while the
  Deployment is at zero and takes over after a start. `scaleToZero.replicas`
  then defaults to `autoscaling.minReplicas` and must lie between it and
  `maxReplicas`. The PodDisruptionBudget does not hold the gateway back:
  scaling is not an eviction.
- The gateway's service account may only get and patch the scale
  subresource of the igloo Deployment. Alert on
  `igloo_gateway_resumes_total{result="failure"}`: clients cannot connect
  while starts fail.

## Values

| Key | Default | Description |
|---|---|---|
| `image.repository` | `ghcr.io/akkio-inc/igloo` | Igloo image |
| `image.tag` | `""` | Image tag; empty uses the chart's `appVersion`. A `sha256:` digest also works. |
| `image.pullPolicy` | `IfNotPresent` | |
| `imagePullSecrets` | `[]` | Pull secrets for every pod of the chart |
| `nameOverride`, `fullnameOverride` | `""` | Resource names. Release `igloo` gives Service `igloo`. |
| `replicaCount` | `2` | Igloo pods (ignored with `autoscaling.enabled`; with `scaleToZero.enabled`, what the gateway starts) |
| `admin.user` | `igloo` | `IGLOO_ADMIN_USER` |
| `admin.existingSecret` | `""` | Secret with key `password` for `IGLOO_ADMIN_PASSWORD` |
| `admin.password` | `""` | Inline password (8+ characters); generated and kept when both are empty |
| `metadataDatabase.existingSecret` | `""` | Secret with key `url` for `IGLOO_METADATA_DATABASE_URL` |
| `metadataDatabase.url` | `""` | Inline `postgresql://` URL. Empty: `catalog.database`, or the bundled PostgreSQL. |
| `storage.warehouseLocation` | `""` | `IGLOO_WAREHOUSE_LOCATION`; defaults to `s3://<objectStorage.bucket>/warehouse` with `objectStorage.enabled` |
| `storage.allowedLocations` | `[]` | `IGLOO_ALLOWED_LOCATIONS` |
| `storage.s3.endpoint` | `""` | `IGLOO_S3_ENDPOINT` (`host:port`); empty means AWS |
| `storage.s3.region` | `us-east-1` | `IGLOO_S3_REGION` |
| `storage.s3.urlStyle` | `vhost` | `IGLOO_S3_URL_STYLE`: `vhost` or `path` |
| `storage.s3.useSSL` | `true` | `IGLOO_S3_USE_SSL` |
| `storage.s3.existingSecret` | `""` | Keys `access-key-id`, `secret-access-key`. Empty: AWS credential chain (IRSA) and `catalog.systemCredentials=aws`. |
| `storage.gcs.existingSecret` | `""` | Keys `hmac-key-id`, `hmac-secret` for `IGLOO_GCS_HMAC_*`; required for `gs://` |
| `storage.azure.existingSecret` | `""` | Key `account-key` for `IGLOO_AZURE_ACCOUNT_KEY`. Empty: workload identity. |
| `duckdb.memoryLimit` | `""` | `IGLOO_DUCKDB_MEMORY_LIMIT`, e.g. `6GB`. Empty: 80% of `resources.limits.memory`, leaving at least 512Mi. |
| `duckdb.threads` | `null` | `IGLOO_DUCKDB_THREADS`; null: one per core |
| `duckdb.maxTempSize` | `""` | `IGLOO_DUCKDB_MAX_TEMP_SIZE`; empty: 90% of `duckdb.spillVolume.sizeLimit`, so an oversized query fails instead of the pod being evicted |
| `duckdb.spillVolume.sizeLimit` | `50Gi` | Size of the spill `emptyDir` |
| `duckdb.spillVolume.medium` | `""` | `""` (node disk) or `Memory` |
| `tls.existingSecret` | `""` | `kubernetes.io/tls` Secret; sets `IGLOO_TLS_CERT_FILE` / `IGLOO_TLS_KEY_FILE` |
| `statementTimeout` | `""` | `IGLOO_STATEMENT_TIMEOUT`, e.g. `30m`; empty: no limit |
| `maxConnections` | `100` | `IGLOO_MAX_CONNECTIONS` per pod |
| `shutdownDelay` | `5s` | `IGLOO_SHUTDOWN_DELAY`: how long a stopping pod keeps accepting connections while reporting not ready |
| `maintenance.enabled` | `true` | `IGLOO_MAINTENANCE`: automatic compaction, snapshot expiry and orphan file cleanup |
| `maintenance.snapshotRetention` | `7d` | `IGLOO_SNAPSHOT_RETENTION`: how long snapshots, and so time travel, are kept |
| `log.format` | `text` | `IGLOO_LOG_FORMAT`: `text` or `json` |
| `log.level` | `info` | `IGLOO_LOG_LEVEL` |
| `extraEnv` | `[]` | Extra `EnvVar`s for the igloo container (any `IGLOO_*` setting) |
| `serviceAccount.create` | `true` | |
| `serviceAccount.name` | `""` | Defaults to the release fullname |
| `serviceAccount.annotations` | `{}` | e.g. `eks.amazonaws.com/role-arn`, `iam.gke.io/gcp-service-account`, `azure.workload.identity/client-id` |
| `podLabels`, `podAnnotations` | `{}` | Added to igloo pods |
| `nodeSelector`, `tolerations`, `topologySpreadConstraints`, `priorityClassName` | | Igloo pod scheduling |
| `affinity` | `{}` | Empty: prefer one igloo pod per node |
| `podSecurityContext`, `securityContext` | non-root 65532, read-only root, no capabilities | Igloo pod and container security contexts |
| `resources` | requests `2` CPU / `8Gi`, limit `8Gi` | Passed through unchanged (including `ephemeral-storage`) |
| `terminationGracePeriodSeconds` | `45` | Igloo gives running queries 30 s after SIGTERM |
| `lifecycle` | `{}` | e.g. `{preStop: {sleep: {seconds: 5}}}` on Kubernetes 1.30+ |
| `strategy` | RollingUpdate, maxSurge 1, maxUnavailable 0 | |
| `startupProbe`, `livenessProbe`, `readinessProbe` | `/healthz`, `/healthz`, `/readyz` on port 8080 | |
| `service.type` | `ClusterIP` | `LoadBalancer` exposes SQL outside the cluster; set `tls.existingSecret` first |
| `service.port` | `5432` | |
| `service.annotations` | `{}` | |
| `service.loadBalancerSourceRanges` | `[]` | Client CIDRs allowed through a LoadBalancer |
| `service.loadBalancerClass`, `service.externalTrafficPolicy` | `""` | |
| `autoscaling.enabled` | `false` | CPU-based HorizontalPodAutoscaler |
| `autoscaling.minReplicas` / `maxReplicas` | `2` / `10` | |
| `autoscaling.targetCPUUtilizationPercentage` | `70` | |
| `podDisruptionBudget.enabled` | `true` | |
| `podDisruptionBudget.minAvailable` | `1` | |
| `scaleToZero.enabled` | `false` | Run the [scale-to-zero](#scale-to-zero) gateway behind the SQL Service |
| `scaleToZero.suspendAfter` | `10m` | `IGLOO_GATEWAY_SUSPEND_AFTER`: stop igloo after this long without client traffic or running work |
| `scaleToZero.resumeTimeout` | `5m` | `IGLOO_GATEWAY_RESUME_TIMEOUT`: how long a connecting client waits for igloo to start |
| `scaleToZero.replicas` | `null` | Igloo pods the gateway starts; null: `replicaCount`, or `autoscaling.minReplicas` with `autoscaling.enabled` |
| `scaleToZero.resources` | `50m` / `64Mi`, limit `256Mi` | Gateway container |
| `scaleToZero.nodeSelector`, `tolerations`, `affinity` | | Gateway pod scheduling, separate from igloo's |
| `metrics.serviceMonitor.enabled` | `false` | Needs the Prometheus Operator CRDs |
| `metrics.serviceMonitor.interval`, `scrapeTimeout`, `labels` | `30s`, `""`, `{}` | |
| `networkPolicy.enabled` | `true` | Ingress policies described above |
| `networkPolicy.sqlFrom`, `networkPolicy.metricsFrom` | `[]` | NetworkPolicy peers for ports 5432 and 8080; empty: everyone |
| `catalog.image.repository` / `tag` / `pullPolicy` | `quay.io/lakekeeper/catalog` / `v0.14.0` / `IfNotPresent` | |
| `catalog.replicaCount` | `1` | Lakekeeper pods (a PodDisruptionBudget is added above 1) |
| `catalog.warehouse` | `igloo` | `IGLOO_CATALOG_WAREHOUSE` |
| `catalog.database.existingSecret` | `""` | Secret with key `url` for `LAKEKEEPER__PG_DATABASE_URL_READ/WRITE` |
| `catalog.database.url` | `""` | Inline URL. Empty: `metadataDatabase`, or the bundled PostgreSQL. |
| `catalog.encryptionKey.existingSecret` | `""` | Secret with key `encryption-key`; generated and kept when empty |
| `catalog.systemCredentials` | `none` | `none`, `aws`, `gcp` or `azure`: Lakekeeper uses its own cloud identity |
| `catalog.serviceAccount.create` / `name` / `annotations` | `true` / `<fullname>-catalog` / `{}` | |
| `catalog.podLabels`, `podAnnotations`, `nodeSelector`, `tolerations`, `affinity` | | Lakekeeper pods |
| `catalog.extraEnv` | `[]` | Extra `LAKEKEEPER__*` settings |
| `catalog.resources` | `250m` / `256Mi`, limit `512Mi` | |
| `postgresql.enabled` | `false` | Bundled PostgreSQL (development and on-prem) |
| `postgresql.image` | `postgres:17` | |
| `postgresql.persistence.size` / `storageClass` | `10Gi` / `""` | |
| `postgresql.resources` | `250m` / `256Mi`, limit `1Gi` | |
| `objectStorage.enabled` | `false` | Bundled RustFS (development and on-prem) |
| `objectStorage.image` | `rustfs/rustfs:1.0.1` | |
| `objectStorage.bucket` | `igloo` | Bucket created before Igloo starts |
| `objectStorage.persistence.size` / `storageClass` | `50Gi` / `""` | |
| `objectStorage.resources` | `250m` / `512Mi`, limit `2Gi` | |
| `objectStorage.initImage` | `curlimages/curl:8.16.0` | Image of the bucket-creating init container |
| `tests.image` | `postgres:17` | Image of the `helm test` pod |

`values.schema.json` checks types and enumerations, so typos in keys fail
early.

## Testing the chart

`deploy/helm/e2e.sh` creates a kind cluster named `igloo-e2e`, builds and loads
`igloo:dev`, installs the chart in bundled mode with two replicas and checks
SQL through the Service and through each pod, roles, health and metrics, the
network policies, `helm test`, an upgrade, uninstall/reinstall, and scale to
zero: the gateway stops igloo when idle, `helm upgrade` leaves it stopped, and
a client connection starts it again. It needs only Docker and deletes the
cluster when done (`KEEP_CLUSTER=1` keeps it).
