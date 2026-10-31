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

### Single sign-on

Register Igloo at the identity provider with the redirect URI
`https://<console host>:8443/sso/callback`, put the client secret in a
Secret, and add the `sso` values ([details](../../../docs/sso.md)):

```sh
kubectl -n igloo create secret generic igloo-oidc --from-literal=client-secret='<client secret>'
```

```yaml
sso:
  issuer: https://agency.okta.com
  clientId: 0oa1b2c3d4
  existingSecret: igloo-oidc
  allowedDomains: [agency.com]
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
  erasure key, bundled PostgreSQL password, bundled object store keys) are stored in the
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
  with `ALTER USER`. Igloo creates the admin only with a password of 16 or
  more characters that is not a placeholder or easily guessed, and the chart
  refuses any other `admin.password`. Once the admin exists, a weaker
  password from `admin.existingSecret` only logs a warning. To upgrade a
  release whose `admin.password` is shorter than 16 characters, move it into
  such a Secret, or give the admin a long password with `ALTER USER` and set
  `admin.password` to it.
- **Memory**: unless `duckdb.memoryLimit` is set, DuckDB gets 80% of
  `resources.limits.memory`, leaving at least 512Mi for the rest of the process
  (1Gi gives DuckDB 512MiB, 8Gi gives 6553MiB), and spills larger queries to
  `/var/lib/igloo/tmp` (`duckdb.spillVolume`). Keep a memory limit set: DuckDB
  allocates somewhat beyond its own limit, and spill files are written through
  the page cache, which counts against the container.
- **Spill disk**: the spill `emptyDir` lives in kubelet's directory on the
  node, so queries that spill run at the speed of the disk under it. Put
  that on local NVMe where the cloud offers it: on EKS, instance-store types
  (`r7gd`, `m7gd`, `c7gd`) with nodeadm's `localStorage` strategy `RAID0`;
  on GKE, Local SSD-backed ephemeral storage; on AKS, an ephemeral OS disk on
  a size with a local disk (`Standard_E4pds_v6`). The OpenTofu modules set
  this up ([spill on local NVMe](../../../docs/operations.md#spill-on-local-nvme)).
  Keep `duckdb.spillVolume.sizeLimit`, times the igloo pods per node, within
  what that disk holds.
- **Startup**: on first start Igloo waits up to 3 minutes for PostgreSQL and
  the catalog, then bootstraps Lakekeeper and creates the warehouse; the
  startup probe allows 5 minutes. On every start it also limits the
  warehouse to Iceberg format versions 1 and 2, which [erasure
  requests](../../../docs/privacy.md) can rewrite. With
  `IGLOO_CATALOG_SETUP=false` (in `extraEnv`) it does neither and logs a
  warning: set the warehouse's format-version policy in Lakekeeper
  yourself.
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
  Lakekeeper pods; igloo pods accept the SQL, console, Flight SQL, Iceberg REST
  and metrics ports (narrow them with `networkPolicy.sqlFrom` / `metricsFrom`).
  Engines in the cluster that use the bundled object store with the
  credentials the Iceberg REST catalog hands them need
  `networkPolicy.objectStorageFrom`. With scale to zero, those ports apply to
  the gateway, and igloo's ports accept only the gateway. Egress is not
  restricted. Lakekeeper has no authentication: its management API, which
  can lift the warehouse's limit on format versions, and its Iceberg REST
  API, which does not check what Igloo checks, are closed to everyone but
  igloo pods only by this policy. Keep it on, on a CNI that enforces
  NetworkPolicies; otherwise any pod in the cluster can change the
  catalog without Igloo, and what [erasure
  requests](../../../docs/privacy.md) rely on no longer holds.
- **Other engines**: Spark, Trino, PyIceberg and others use Igloo's tables
  through its [Iceberg REST catalog](../../../docs/external-engines.md) on
  port 8181 of the SQL Service (`iceberg.enabled`). On AWS, set
  `storage.s3.stsRoleArn` to a role Igloo's own role may assume, so that
  engines get credentials for one table at a time; S3-compatible stores need
  no role, and the bundled one only `networkPolicy.objectStorageFrom` for the
  engines' pods.
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
- [Tasks, pipes and materialized views](../../../docs/loading.md) that are
  due start Igloo too, with nobody connected
  ([details](../../../docs/scale-to-zero.md#scheduled-work)). A gateway pod
  that starts while Igloo is at zero scales it up once to learn when work
  is due.
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

## Compute pools

`pools` adds compute pools, "virtual warehouses": more igloo Deployments
that share the release's catalog, metadata database (users, grants, query
history) and warehouse, each with its own size, replicas and scale-to-zero
settings, so a client or team gets compute, and a bill, of its own
([workload management](../../../docs/workload-management.md#compute-pools)).

```yaml
pricePerHour: 0.68                 # the release's own pods: the "default" pool
pools:
  - name: acme
    replicaCount: 1
    pricePerHour: 2.72
    resources:
      requests: { cpu: "16", memory: 64Gi }
      limits: { memory: 64Gi }
    scaleToZero: { enabled: true, suspendAfter: 15m }
    nodeSelector: { node-pool: igloo-large }
```

```
clients of acme ──> Service igloo-pool-acme ──> gateway ──> Deployment igloo-pool-acme (0 to N pods) ─┐
other clients   ──> Service igloo ─────────────────────────> Deployment igloo (the default pool) ─────┴─> catalog, metadata database, warehouse
```

- Each pool gets a Deployment and SQL Service named `<release>-pool-<name>`,
  a `<release>-pool-<name>-metrics` Service (scraped by the ServiceMonitor
  too), NetworkPolicies, and with `scaleToZero.enabled` its own gateway,
  Services and RBAC, which start and stop only that pool. Its pods have the
  component label `pool-<name>`, so no Service or Deployment of another pool
  selects them; the catalog, bundled PostgreSQL and object store accept
  them.
- A pool takes every value it does not set from the release's: `image`,
  `admin`, storage, databases, TLS, `console`, `networkPolicy`, `service`,
  `duckdb`, `resources`, scheduling and the rest. `extraEnv` adds to the
  release's. Pools can set `replicaCount`, `pricePerHour`, `resources`,
  `duckdb`, `statementTimeout`, `maxConnections`, `extraEnv`, `podLabels`,
  `podAnnotations`, `nodeSelector`, `tolerations`, `affinity`,
  `topologySpreadConstraints`, `priorityClassName`, `service` and
  `scaleToZero`.
- Pods know their pool from `IGLOO_POOL`, and record it with every
  statement in `igloo.query_history` and `igloo.usage`. Set `pricePerHour`
  per pool to what its nodes cost, for the cost estimates.
- Pools never run table maintenance, which stays with the release's own
  Deployment, and have no HorizontalPodAutoscaler or PodDisruptionBudget.
- Names are 1 to 20 lowercase letters, digits and hyphens; `default` is the
  release's own Deployment.

## Distributed execution

With `workers.enabled=true`, statements that read very large tables run in
parts on worker pods ([distributed execution](../../../docs/distributed-execution.md)):

```
igloo pods ──TLS, port 7433──> Service igloo-workers (headless) ──> worker pods (0 to workers.maxReplicas)
     └── scale Deployment igloo-workers through the Kubernetes API
```

- The worker Deployment starts with no pods. An igloo pod scales it up when
  a statement is worth distributing (never down while another igloo pod uses
  the workers), and to zero once every worker has been idle for
  `workers.idleAfter`. A statement waits for workers at most
  `workers.startTimeout`, then runs on its igloo pod instead.
- Give workers their own node pool with a minimum size of zero
  (`workers.nodeSelector`, `workers.tolerations`), so idle workers cost
  nothing; by default they prefer one per node.
- Workers use the igloo pods' service account (and so their cloud identity
  for object storage) without a Kubernetes API token. The igloo pods get a
  token, for a Role that may only get and patch the workers' `scale`
  subresource.
- Igloo pods and workers authenticate each other with the worker secret
  (generated and kept, or `workers.existingSecret`); the NetworkPolicy also
  admits only igloo pods to the task port. Port 8080 serves health checks
  and metrics through `<fullname>-workers-metrics`, which the ServiceMonitor
  scrapes too; the NetworkPolicy admits pods of the namespace to it, or
  `networkPolicy.metricsFrom` if set.
- Give the warehouse's bucket a lifecycle rule that expires
  `<warehouse>/_igloo_shuffle/` after a day
  ([how](../../../docs/distributed-execution.md#deploying-workers)).
- Every [compute pool](#compute-pools) gets workers of its own,
  `<fullname>-pool-<name>-workers`, used only by its igloo pods and admitted
  only from them; a pool can set `workers` (`maxReplicas`, `resources`,
  `nodeSelector` and the rest), or `workers.enabled: false` to run without.
  Each pool's workers have a secret of their own (generated as
  `worker-secret-<name>`, or the pool's `workers.existingSecret`), so a
  pool's igloo pods cannot use another pool's workers; with several pools,
  run a CNI that enforces NetworkPolicies, which keep them apart too.
- With `scaleToZero.enabled`, the gateway stops the workers along with the
  igloo pods.
- `workers.pricePerHour` is what one worker's machine costs an hour
  (`IGLOO_WORKER_PRICE_PER_HOUR`), for auto mode's cost estimates.

### A large worker

With `workers.large.enabled=true` (and `workers.enabled`), the chart adds a
second worker Deployment, `<fullname>-workers-large`, of one pod on a large
machine. Auto mode may run a whole statement on it instead of on many
small workers or an igloo pod, when its estimate (fits in memory, no
spill, price) is about as fast as the fastest option and cheaper:

```yaml
workers:
  enabled: true
  pricePerHour: 0.31             # one small worker's machine
  large:
    enabled: true
    pricePerHour: 4.35           # the large worker's machine
    resources:
      requests: { cpu: "60", memory: 448Gi }
      limits: { memory: 448Gi }
    duckdb:
      spillVolume: { sizeLimit: 1900Gi }
    cacheVolume: { sizeLimit: 850Gi }
    nodeSelector: { pool: igloo-large-worker }   # a node pool that scales to zero
```

- It runs `igloo worker` from the same image, with the workers' secret,
  storage settings and exchanges, and its own resources, volumes,
  scheduling, labels and grace period (nothing of `workers.*` but those is
  taken). It starts at zero pods; igloo starts it when it picks it and
  stops it when idle, as it does the other workers.
- The chart adds its headless Service, metrics Service, a Role that lets
  igloo pods scale only it, and a NetworkPolicy like the workers'. The
  igloo pods get `IGLOO_LARGE_WORKER` (`<fullname>-workers-large:7433`),
  `IGLOO_LARGE_WORKER_COMPUTE=kubernetes`,
  `IGLOO_LARGE_WORKER_K8S_DEPLOYMENT`, `IGLOO_LARGE_WORKER_CORES` (whole
  cores of `requests.cpu`), `IGLOO_LARGE_WORKER_MEMORY` (its DuckDB memory
  limit, `workers.large.duckdb.memoryLimit` or 80% of
  `resources.limits.memory`, for estimates while it is stopped) and
  `IGLOO_LARGE_WORKER_PRICE_PER_HOUR`.
- Each compute pool with workers gets a large worker of its own,
  `<fullname>-pool-<name>-workers-large`, from the release's
  `workers.large` with the pool's `workers.large` on top; a pool sets
  `workers.large.enabled: false` to run without. With
  `scaleToZero.enabled`, the gateway stops it with the pool
  (`IGLOO_GATEWAY_K8S_LARGE_WORKER_DEPLOYMENT`).
- Give it a node pool of its own with a minimum size of zero, so it costs
  nothing while stopped. The [OpenTofu modules](../../) provision one with
  `large_worker_enabled = true`.

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
| `admin.password` | `""` | Inline password: 16+ characters, not a placeholder such as `change-me` or easily guessed. Generated (32 characters) and kept when both are empty |
| `metadataDatabase.existingSecret` | `""` | Secret with key `url` for `IGLOO_METADATA_DATABASE_URL` |
| `metadataDatabase.url` | `""` | Inline `postgresql://` URL. Empty: `catalog.database`, or the bundled PostgreSQL. |
| `storage.warehouseLocation` | `""` | `IGLOO_WAREHOUSE_LOCATION`; defaults to `s3://<objectStorage.bucket>/warehouse` with `objectStorage.enabled` |
| `storage.allowedLocations` | `[]` | `IGLOO_ALLOWED_LOCATIONS` |
| `storage.s3.endpoint` | `""` | `IGLOO_S3_ENDPOINT` (`host:port`); empty means AWS |
| `storage.s3.region` | `us-east-1` | `IGLOO_S3_REGION` |
| `storage.s3.urlStyle` | `vhost` | `IGLOO_S3_URL_STYLE`: `vhost` or `path` |
| `storage.s3.useSSL` | `true` | `IGLOO_S3_USE_SSL` |
| `storage.s3.existingSecret` | `""` | Keys `access-key-id`, `secret-access-key`. Empty: AWS credential chain (IRSA) and `catalog.systemCredentials=aws`. |
| `storage.s3.stsRoleArn` | `""` | `IGLOO_S3_STS_ROLE_ARN`: role Igloo assumes to hand other engines credentials limited to one table. Empty on AWS: engines get metadata but no credentials. |
| `storage.gcs.existingSecret` | `""` | Keys `hmac-key-id`, `hmac-secret` for `IGLOO_GCS_HMAC_*`; required for `gs://` |
| `storage.azure.existingSecret` | `""` | Key `account-key` for `IGLOO_AZURE_ACCOUNT_KEY`. Empty: workload identity. |
| `duckdb.memoryLimit` | `""` | `IGLOO_DUCKDB_MEMORY_LIMIT`, e.g. `6GB`. Empty: 80% of `resources.limits.memory`, leaving at least 512Mi. |
| `duckdb.threads` | `null` | `IGLOO_DUCKDB_THREADS`; null: two per core, as memory allows |
| `duckdb.maxTempSize` | `""` | `IGLOO_DUCKDB_MAX_TEMP_SIZE`; empty: 90% of `duckdb.spillVolume.sizeLimit`, so an oversized query fails instead of the pod being evicted |
| `duckdb.spillVolume.sizeLimit` | `50Gi` | Size of the spill `emptyDir` |
| `duckdb.spillVolume.medium` | `""` | `""` (node disk, kubelet's directory; see **Spill disk** above) or `Memory` |
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
| `console.enabled` | `true` | [Web console](../../../docs/console.md) on the SQL Service; it signs users in only over TLS or through `kubectl port-forward` |
| `console.port` | `8443` | Console port on the SQL Service |
| `flightSQL.enabled` | `true` | [Arrow Flight SQL](../../../docs/flight-sql.md) on the SQL Service; it serves clients only over TLS or through `kubectl port-forward` |
| `flightSQL.port` | `31337` | Flight SQL port on the SQL Service |
| `sso.issuer` | `""` | [Single sign-on](../../../docs/sso.md): the OpenID Connect provider's issuer URL (`IGLOO_OIDC_ISSUER`). Empty: off. |
| `sso.clientId` | `""` | `IGLOO_OIDC_CLIENT_ID`; required with `sso.issuer` |
| `sso.existingSecret` | `""` | Secret with key `client-secret`, mounted as `IGLOO_OIDC_CLIENT_SECRET_FILE`. Empty for a public client. |
| `sso.redirectUrl` | `""` | `https://<console host>/sso/callback` as registered at the provider; empty: from each request |
| `sso.scopes` | `[openid, profile, email]` | Add `groups` where the provider needs it |
| `sso.usernameClaim`, `sso.groupsClaim` | `email`, `groups` | `preferred_username` for Microsoft Entra ID |
| `sso.allowedDomains`, `sso.allowedGroups` | `[]` | Only these email domains, and members of these groups, may sign in. Google needs `allowedDomains`. |
| `sso.createUsers` | `true` | Create users, role `none`, at their first sign-in |
| `sso.passwordSignIn` | `true` | `false`: users linked to the provider may not use passwords |
| `sso.displayName` | `SSO` | Label of the sign-in button |
| `sso.accessTokenAudience` | `""` | Accept the provider's access tokens for this audience as SQL passwords, inside TLS |
| `sso.accessTokenIdClaim` | `""` | Claim holding the account's immutable ID in access tokens whose subject is not the ID token's: `uid` for Okta, `oid` for Entra ID |
| `tokenMaxLifetime` | `365d` | Longest lifetime of an access token (`IGLOO_TOKEN_MAX_LIFETIME`) |
| `iceberg.enabled` | `true` | [Iceberg REST catalog](../../../docs/external-engines.md) for other engines on the SQL Service; without TLS it serves clients on private networks only |
| `iceberg.port` | `8181` | Iceberg REST port on the SQL Service |
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
| `scaleToZero.firstByteTimeout` | `30s` | `IGLOO_GATEWAY_FIRST_BYTE_TIMEOUT`: how long a client has to send its first bytes before the gateway closes it, starting nothing |
| `scaleToZero.maxWaiting` | `200` | `IGLOO_GATEWAY_MAX_WAITING`: connections that may wait at once for their first bytes or for igloo to start |
| `scaleToZero.replicas` | `null` | Igloo pods the gateway starts; null: `replicaCount`, or `autoscaling.minReplicas` with `autoscaling.enabled` |
| `scaleToZero.resources` | `50m` / `64Mi`, limit `256Mi` | Gateway container |
| `scaleToZero.nodeSelector`, `tolerations`, `affinity` | | Gateway pod scheduling, separate from igloo's |
| `pricePerHour` | `0` | `IGLOO_PRICE_PER_HOUR`: what one igloo pod's machine costs an hour, for `igloo.usage`; `0` is unknown |
| `pools` | `[]` | [Compute pools](#compute-pools), each with `name` and the overrides listed there |
| `workers.enabled` | `false` | [Distributed execution](#distributed-execution) on worker pods |
| `workers.maxReplicas` | `4` | `IGLOO_WORKER_MAX`: the most worker pods igloo starts |
| `workers.idleAfter` | `auto` | `IGLOO_WORKER_IDLE_AFTER`: stop workers after this long without tasks; `auto` for three times their last start, from 2 to 10 minutes |
| `workers.startTimeout` | `3m` | `IGLOO_WORKER_START_TIMEOUT`: how long a statement waits for workers to start |
| `workers.exchanges` | `object` | `IGLOO_EXCHANGES`: where workers exchange rows; `local` lets workers reach each other's task port and splits the spill volume 60% spill, 30% exchanges (`IGLOO_EXCHANGE_MAX_SIZE`) |
| `workers.existingSecret` | `""` | Secret with key `secret` (32+ characters) for `IGLOO_WORKER_SECRET`; generated and kept when empty |
| `workers.duckdb.memoryLimit`, `threads` | `""`, `null` | As `duckdb.*`, from `workers.resources`; null threads: four per core |
| `workers.duckdb.spillVolume.sizeLimit` / `medium` | `100Gi` / `""` | Workers' spill `emptyDir`; `IGLOO_DUCKDB_MAX_TEMP_SIZE` is 90% of it |
| `workers.cacheVolume.sizeLimit` | `50Gi` | Workers' read cache `emptyDir`; the cache may use 90% of it |
| `workers.resources` | requests `4` CPU / `16Gi`, limit `16Gi` | Worker container |
| `workers.podLabels`, `podAnnotations`, `nodeSelector`, `tolerations` | | Worker pods |
| `workers.affinity` | `{}` | Empty: prefer one worker per node |
| `workers.terminationGracePeriodSeconds` | `45` | A stopping worker finishes its running tasks for up to 30s, as when a Spot node is drained before it is reclaimed |
| `workers.pricePerHour` | `0` | `IGLOO_WORKER_PRICE_PER_HOUR`: what one worker's machine costs an hour, for auto mode's estimates; `0` is unknown |
| `workers.large.enabled` | `false` | [A large worker](#a-large-worker): `<fullname>-workers-large`, one pod; needs `workers.enabled` |
| `workers.large.pricePerHour` | `0` | `IGLOO_LARGE_WORKER_PRICE_PER_HOUR`: what the large worker's machine costs an hour |
| `workers.large.resources` | requests `60` CPU / `448Gi`, limit `448Gi` | Large worker container; `requests.cpu` gives `IGLOO_LARGE_WORKER_CORES` |
| `workers.large.duckdb.memoryLimit`, `threads`, `spillVolume` | `""`, `null`, `1000Gi` | As `workers.duckdb.*`; the memory limit is also `IGLOO_LARGE_WORKER_MEMORY` |
| `workers.large.cacheVolume.sizeLimit` | `500Gi` | Large worker's read cache `emptyDir` |
| `workers.large.podLabels`, `podAnnotations`, `nodeSelector`, `tolerations`, `affinity`, `terminationGracePeriodSeconds` | | Large worker pod; its own, not the workers' |
| `metrics.serviceMonitor.enabled` | `false` | Needs the Prometheus Operator CRDs |
| `metrics.serviceMonitor.interval`, `scrapeTimeout`, `labels` | `30s`, `""`, `{}` | |
| `networkPolicy.enabled` | `true` | Ingress policies described above |
| `networkPolicy.sqlFrom`, `networkPolicy.metricsFrom` | `[]` | NetworkPolicy peers for the SQL, console, Flight SQL and Iceberg REST ports (5432, 8443, 31337, 8181) and for 8080; empty: everyone |
| `networkPolicy.objectStorageFrom` | `[]` | NetworkPolicy peers, besides igloo and Lakekeeper, for the bundled object store: engines using the Iceberg REST catalog's credentials; empty: none |
| `catalog.image.repository` / `tag` / `pullPolicy` | `quay.io/lakekeeper/catalog` / `v0.14.0` / `IfNotPresent` | |
| `catalog.replicaCount` | `1` | Lakekeeper pods (a PodDisruptionBudget is added above 1) |
| `catalog.warehouse` | `igloo` | `IGLOO_CATALOG_WAREHOUSE` |
| `catalog.database.existingSecret` | `""` | Secret with key `url` for `LAKEKEEPER__PG_DATABASE_URL_READ/WRITE` |
| `catalog.database.url` | `""` | Inline URL. Empty: `metadataDatabase`, or the bundled PostgreSQL. |
| `catalog.encryptionKey.existingSecret` | `""` | Secret with key `encryption-key`; generated and kept when empty |
| `erasure.existingSecret` | `""` | Secret with key `key` for `IGLOO_ERASURE_KEY`, which seals erasure requests' identifiers; generated and kept when empty |
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

`deploy/helm/template-test.sh` renders the chart with compute pools
(`test-values/pools.yaml`) and with workers and a large worker
(`test-values/large-worker.yaml`) and checks the objects it makes, and that
it refuses pools and large workers that cannot work; it needs only helm.

`deploy/helm/e2e.sh` creates a kind cluster named `igloo-e2e`, builds and loads
`igloo:dev`, installs the chart in bundled mode with two replicas and checks
SQL through the Service and through each pod, roles, health and metrics, the
network policies, `helm test`, an upgrade, uninstall/reinstall, and scale to
zero: the gateway stops igloo when idle, `helm upgrade` leaves it stopped, and
a client connection starts it again. It needs only Docker and deletes the
cluster when done (`KEEP_CLUSTER=1` keeps it).
