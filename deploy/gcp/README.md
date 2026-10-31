# Igloo on Google Cloud

An [OpenTofu](https://opentofu.org/) module that deploys Igloo to a Google
Cloud project with one `tofu apply`: a private GKE Autopilot cluster, a Cloud
Storage bucket for table data, Cloud SQL for PostgreSQL for the catalog, and
the Igloo Helm chart (igloo pods plus the bundled Lakekeeper catalog).

Run it as a root module: it configures its own providers.

## What gets created

| Resource | Details |
|---|---|
| VPC and subnet | One regional subnet with secondary ranges for pods and services (VPC-native), Private Google Access, flow logs |
| Cloud Router and Cloud NAT | Outbound internet for the private nodes, which pull images from `ghcr.io` and `quay.io` |
| GKE Autopilot cluster | Regional, private nodes, Workload Identity, DNS-based control-plane endpoint authorized by IAM, dedicated node service account |
| GCS bucket | Uniform bucket-level access, public access prevention enforced, soft delete (7 days), optional versioning. Tables live under `gs://<bucket>/warehouse` |
| Service accounts | `<name>-igloo` (query engine) and `<name>-catalog` (Lakekeeper), each with object read/write (`roles/storage.objectAdmin`) and bucket metadata read (`roles/storage.legacyBucketReader`) on the warehouse bucket only; `<name>-nodes` for the nodes (logging and monitoring only) |
| Workload Identity bindings | Kubernetes service account `igloo` (in the namespace) acts as `<name>-igloo`; `igloo-catalog` acts as `<name>-catalog` |
| HMAC key | For `<name>-igloo`; the query engine's GCS credential (see [Storage access](#storage-access)) |
| Cloud SQL for PostgreSQL 17 | Private IP only (private services access), TLS required, daily backups and point-in-time recovery, generated password. One database, `igloo`: the catalog uses schema `public`, Igloo's users and roles use schema `igloo_auth` |
| Kubernetes namespace and Secrets | `igloo-admin` (`password`), `igloo-database` (`url`), `igloo-catalog-encryption` (`encryption-key`), `igloo-gcs-hmac` (`hmac-key-id`, `hmac-secret`), and `igloo-tls` when you supply a certificate |
| Helm release `igloo` | The chart at `../helm/igloo`, wired to everything above |
| Compute classes (optional) | With `workers_enabled`, `igloo-workers` (Spot first, then on-demand) for [distributed-execution workers](#distributed-execution-workers), and with `large_worker_enabled`, `igloo-large-worker`, installed by the Helm release `igloo-compute-classes` from `compute-classes/` |
| Google APIs | Compute, GKE, Cloud SQL Admin, Service Networking, IAM, IAM Credentials, STS, Storage, Resource Manager. Left enabled on destroy |

## Why Autopilot

Autopilot bills for the CPU, memory and disk that pods request. Standard bills
for whole VMs, whether or not pods fill them. Igloo's pods are large and
uniform (2 vCPU and 8 GiB each by default), which suits per-pod billing:

- **Bin-packing waste on Standard.** After GKE's reservations and its
  DaemonSets, an e2-standard-4 (4 vCPU, 16 GB) has room for one igloo pod, not
  two. Two replicas need two such nodes: about $196 a month on demand, plus
  about $20 of boot disks, to run about $191 of Autopilot pod requests.
- **Cluster fee.** Both modes charge $0.10 per cluster-hour (about $73 a month),
  and the GKE free tier credits $74.40 a month per billing account against
  Autopilot and zonal Standard clusters. A regional Standard cluster, which you
  want for availability, is not covered. Autopilot clusters are always regional.
- **Spot is a scheduling choice, not a node pool.** With `spot = true` the igloo
  pods get the `cloud.google.com/gke-spot` node selector and toleration through
  the chart's `nodeSelector` and `tolerations` values, and Autopilot provisions
  Spot capacity for them at roughly 60-70% off.
- **Less to run.** No node pools to size or upgrade, and Google's hardening
  (Workload Identity, shielded nodes, no SSH) is always on.

For the defaults, the igloo and catalog pods cost about $200 a month on
Autopilot and about $290 on an equivalent regional Standard cluster.

Standard becomes the cheaper choice for a large, steady fleet packed above
roughly 75% utilization. It also allows shapes Autopilot's general-purpose
class does not, such as very high memory per vCPU (Autopilot allows 1 to 6.5
GiB per vCPU). Before switching modes, try Autopilot compute classes:
`node_selector = { "cloud.google.com/compute-class" = "Balanced" }` (or a
custom compute class) selects other machine families while keeping per-pod
billing. Local NVMe SSD for DuckDB's spill directory is available on Autopilot
too, with `local_ssd_machine_family` ([Spill on Local SSD](#spill-on-local-ssd)).

## Prerequisites

- A Google Cloud project with billing enabled.
- [OpenTofu](https://opentofu.org/docs/intro/install/) 1.11 or later.
- The [gcloud CLI](https://cloud.google.com/sdk/docs/install), recent enough for
  `gcloud container clusters get-credentials --dns-endpoint`, with the
  `gke-gcloud-auth-plugin` component, and `kubectl`.
- Credentials for OpenTofu: `gcloud auth application-default login`.
- Permissions in the project: Owner, or Kubernetes Engine Admin, Cloud SQL
  Admin, Compute Network Admin, Service Networking Admin, Service Account Admin,
  Service Account User, Project IAM Admin, Storage Admin and Service Usage Admin.
- These organization policies must allow the deployment: service account HMAC
  key creation (`constraints/storage.disableServiceAccountHmacKeyCreation` must
  not be enforced) and Google-managed encryption keys
  (`constraints/gcp.restrictNonCmekServices` must not cover Storage, SQL or GKE).
- The Helm chart at `deploy/helm/igloo`. Without it, set `install_chart = false`
  to create everything except the release.

## Quick start

```sh
cd deploy/gcp
cp terraform.tfvars.example terraform.tfvars   # set project_id, review the rest
tofu init
tofu apply
```

A minimal `terraform.tfvars`:

```hcl
project_id = "my-gcp-project"
region     = "us-central1"
```

A first apply takes 20 to 40 minutes, most of it creating Cloud SQL and the
cluster, then letting Autopilot provision nodes for the first pods. Igloo pods
may restart once while new IAM bindings propagate. Then:

```sh
tofu output next_steps
```

## Connecting

By default the SQL endpoint is reachable only inside the cluster.

```sh
$(tofu output -raw get_credentials_command)
export PGPASSWORD="$(kubectl -n igloo get secret igloo-admin -o jsonpath='{.data.password}' | base64 --decode)"
kubectl -n igloo port-forward svc/igloo 5432:5432 &
psql "host=127.0.0.1 port=5432 user=igloo dbname=igloo"
```

`tofu output admin_password_command` prints the password command for your
namespace and Secret name.

### Other engines

Spark, Trino, PyIceberg and other engines use the tables through Igloo's
[Iceberg REST catalog](../../docs/external-engines.md) on port 8181 of the same
Service (`kubectl -n igloo port-forward svc/igloo 8181:8181`, or
`http://igloo.igloo.svc:8181` inside the cluster), signed in as Igloo users.
Igloo does not hand out credentials for Google Cloud Storage yet, so engines
get table metadata only: to read or write the files they need a service
account of their own with access to the bucket, whose reach Igloo's grants do
not limit.

### Exposing SQL through a load balancer

```hcl
expose_sql        = true
sql_allowed_cidrs = ["203.0.113.0/24"]   # required; there is no default
```

The chart's Service becomes `type: LoadBalancer` with
`loadBalancerSourceRanges` set to `sql_allowed_cidrs`, and GKE creates an
external TCP load balancer whose firewall admits only those ranges. After
`tofu apply`, `tofu output sql_endpoint` shows its IP.

Give Igloo a certificate before exposing it; without TLS, queries and results
cross the internet unencrypted. Supply the PEM files through the environment
rather than a tfvars file, and point a DNS name in the certificate at the
`sql_endpoint` address:

```sh
export TF_VAR_tls_certificate="$(cat igloo.crt)" TF_VAR_tls_private_key="$(cat igloo.key)"
tofu apply
psql "host=sql.example.com port=5432 user=igloo dbname=igloo sslmode=verify-full"
```

## Main variables

| Variable | Default | Meaning |
|---|---|---|
| `project_id` | (required) | Project to deploy into |
| `region` | `us-central1` | Region for everything |
| `name` | `igloo` | Resource name prefix and cluster name |
| `install_chart` | `true` | Install `../helm/igloo` |
| `image_tag` | `""` | Igloo image tag; empty uses the chart's appVersion |
| `image_repository` | `""` | Igloo image repository, for a mirror or private registry; empty uses the chart's default |
| `replica_count` | `2` | igloo pods |
| `igloo_cpu`, `igloo_memory` | `2`, `8Gi` | Requests per igloo pod (memory is also the limit) |
| `spill_size` | `50Gi` | Local disk per pod for larger-than-memory queries |
| `local_ssd_machine_family` | `""` | Machine series, e.g. `c3`, whose Local SSD holds the spill directory; empty: the general-purpose class's boot disk |
| `duckdb_memory_limit` | `""` | Empty: about 80% of `igloo_memory` |
| `spot` | `false` | Run igloo pods on Spot capacity |
| `node_selector` | `{}` | Extra node selector, e.g. an Autopilot compute class |
| `expose_sql`, `sql_allowed_cidrs` | `false`, `[]` | External load balancer, and who may reach it |
| `tls_certificate`, `tls_private_key` | `null` | PEM certificate and key for the SQL endpoint |
| `db_tier` | `db-custom-1-3840` | Cloud SQL tier (1 vCPU, 3.75 GB) |
| `db_high_availability` | `false` | Standby in a second zone |
| `bucket_versioning` | `false` | Keep old object versions (expired after 30 days) |
| `bucket_soft_delete_days` | `7` | Deleted objects stay recoverable this long |
| `deletion_protection` | `true` | Protect the cluster and database from deletion |
| `control_plane_authorized_cidrs` | `[]` | CIDRs allowed on the control plane's public IP endpoint (none needed; see below) |
| `workers_enabled`, `workers_spot`, `workers_machine_family`, `workers_max` | `false`, `true`, `n4`, `4` | [Distributed-execution workers](#distributed-execution-workers): Spot first, on-demand when Spot has no capacity |
| `workers_cpu`, `workers_memory`, `workers_spill_size`, `workers_cache_size`, `workers_price_per_hour` | `14`, `54Gi`, `80Gi`, `28Gi`, `0` | Worker pod size, volumes and price |
| `large_worker_enabled`, `large_worker_spot`, `large_worker_machine_type`, `large_worker_price_per_hour` | `false`, `false`, `n4-highmem-64`, `0` | One large worker, on-demand by default |

See `variables.tf` for the network ranges and the rest.

## Cost estimate

Defaults in us-central1, at on-demand list prices, per month. Prices change;
check the [pricing calculator](https://cloud.google.com/products/calculator).

| Item | Approx. per month |
|---|---|
| GKE Autopilot cluster fee ($0.10/hour) | $73, offset by the $74.40 free-tier credit (one per billing account) |
| 2 igloo pods (2 vCPU, 8 GiB, 50 GiB ephemeral storage each) | $191 ($60 with `spot = true`) |
| Lakekeeper pod (0.25 vCPU, 0.25 GiB) | $9 |
| Cloud SQL, db-custom-1-3840, zonal, 10 GB SSD, backups | $52 ($100 with `db_high_availability`) |
| Cloud NAT | $3, plus $0.045 per GiB processed (mostly image pulls) |
| Cloud Storage, Standard, regional | $0.020 per GiB stored, plus operations ($2 for 100 GiB) |
| **Total, before storage growth** | **about $255 on demand; about $125 with `spot = true`** |

Workers and the large worker cost nothing while stopped; while they run,
their nodes are billed by the hour.

Traffic between the pods and a bucket in the same region is free. Logging
(including VPC flow logs) usually stays within the free 50 GiB per project.
Turning on `bucket_versioning` or keeping soft delete means deleted table data
is billed until it expires.

## Spill on Local SSD

DuckDB writes queries larger than memory (big joins, aggregations and sorts)
to its spill directory, an `emptyDir` of `spill_size` on the node, and reads
them back; heavy reports spill tens to hundreds of GiB. On Autopilot's
general-purpose class that is the node's boot disk, a network persistent disk
whose throughput is a fraction of a local NVMe SSD's.

For workloads that spill a lot, put it on Local SSD:

```hcl
local_ssd_machine_family = "c3"     # or c3d, c4, c4a, c4d: series with Local SSD
spill_size               = "120Gi"  # DuckDB stops at 90% of it
```

The igloo pods then get the node selectors
`cloud.google.com/machine-family: c3` and
`cloud.google.com/gke-ephemeral-storage-local-ssd: "true"`, and Autopilot
runs them on nodes of that series whose ephemeral storage, and so every
`emptyDir`, is on Local SSD. The pods' ephemeral-storage request
(`spill_size`) may then exceed the general-purpose class's 122 GiB, up to the
machine type's Local SSD less what GKE reserves; Autopilot picks a machine
type with enough. It works with `spot = true`.

It is not the default because it changes how the pods are billed: Autopilot
charges for the whole VM of a selected machine series, plus a premium, rather
than for the pods' requests, so size `igloo_cpu` and `igloo_memory` to fill
the machine types of that series. Local SSD is emptied when a node is
deleted or repaired; spill files only live for a query, so nothing is lost.
Check that the series is offered in your region with Local SSD, and that your
GKE version supports it (C4D needs 1.32.9-gke.1711000 or later).

## Distributed-execution workers

`workers_enabled = true` runs statements that read very large tables in
parts on worker pods ([distributed execution](../../docs/distributed-execution.md)).
It is off by default, so applying this version to an existing deployment
changes nothing. Autopilot needs no node pool for them: the module creates
a [custom compute
class](https://cloud.google.com/kubernetes-engine/docs/concepts/about-custom-compute-classes),
`igloo-workers`, and the chart's worker pods select it
(`cloud.google.com/compute-class: igloo-workers`):

```yaml
apiVersion: cloud.google.com/v1
kind: ComputeClass
metadata:
  name: igloo-workers
spec:
  priorities:
    - { machineFamily: n4, spot: true }    # workers_machine_family; dropped with workers_spot = false
    - { machineFamily: n4, spot: false }
  whenUnsatisfiable: DoNotScaleUp
```

When Igloo starts workers, Autopilot creates nodes for them from the first
rule it can satisfy: Spot when Google has Spot capacity for the series, an
on-demand node of the same series when it does not. A worker that fits no
rule waits; a statement waits for workers at most `workers.startTimeout` (3
minutes) and then runs on its igloo pod. Igloo pods keep their own
scheduling (`spot`, on-demand by default): they hold client sessions. When
Google preempts a Spot node, its worker gets about 25 seconds to finish its
tasks, and what it has not finished runs again on other workers. Worker
nodes go away once Igloo stops idle workers; with a machine series named,
Autopilot bills them per node.

Each worker requests 14 vCPUs and 54 GiB (`workers_cpu`, `workers_memory`),
which Autopilot runs on a 16-vCPU machine of the series, and ephemeral
storage for its spill (`workers_spill_size`, 80Gi) and read cache
(`workers_cache_size`, 28Gi), plus 1Gi: Autopilot caps `emptyDir` volumes
at that request, and the boot disk that holds them at about 122 GiB.

The large worker (`large_worker_enabled = true`) is one pod on an
`n4-highmem-64` (64 vCPUs, 512 GB), through a class of its own,
`igloo-large-worker`, which auto mode may run a whole statement on (see the
chart's [large worker](../helm/igloo/README.md#a-large-worker)). It is
on-demand by default; `large_worker_spot = true` puts a Spot rule first.

The classes are installed by a small chart of the module's own,
`compute-classes/`, rather than with `kubernetes_manifest`, which needs the
cluster's API to plan and so cannot be created in the same apply as the
cluster. Set `workers_price_per_hour` and `large_worker_price_per_hour` to
what a node costs, so auto mode can weigh price; Spot is typically 60 to 91%
below on-demand. Left at 0, the price is unknown.

## Storage access

The two components reach the bucket differently:

- **igloo (DuckDB) uses an HMAC key.** DuckDB reads and writes GCS through the
  S3-compatible XML API, which accepts only HMAC keys, not Google OAuth tokens
  or Workload Identity. The module creates an HMAC key for the `<name>-igloo`
  service account and stores it in the `igloo-gcs-hmac` Secret. The key can do
  exactly what that service account can: read and write objects in this bucket.
- **The catalog (Lakekeeper) uses Workload Identity.** It runs as the
  `<name>-catalog` service account with `LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS=true`
  (chart value `catalog.systemCredentials: gcp`), with no stored key.

The igloo pods also run as `<name>-igloo` through Workload Identity, so
nothing changes here if a future DuckDB release accepts OAuth tokens.

To rotate the HMAC key:

```sh
tofu apply -replace=google_storage_hmac_key.igloo
kubectl -n igloo rollout restart deployment
```

## Security notes

- **State holds secrets**: the database password, administrator password,
  catalog encryption key, HMAC secret and any TLS private key. Keep state in an
  encrypted remote backend with restricted access, never in git. Add a backend
  before the first apply, for example in a `backend.tf`:

  ```hcl
  # terraform {
  #   backend "gcs" {
  #     bucket = "my-tofu-state-bucket"   # versioned, uniform access, not public
  #     prefix = "igloo/gcp"
  #   }
  #
  #   # Optional: OpenTofu client-side state encryption with Cloud KMS.
  #   # Example only; see https://opentofu.org/docs/language/state/encryption/
  #   encryption {
  #     key_provider "gcp_kms" "state" {
  #       kms_encryption_key = "projects/my-project/locations/us-central1/keyRings/tofu/cryptoKeys/state"
  #       key_length         = 32
  #     }
  #     method "aes_gcm" "state" {
  #       keys = key_provider.gcp_kms.state
  #     }
  #     state {
  #       method   = method.aes_gcm.state
  #       enforced = true
  #     }
  #     plan {
  #       method   = method.aes_gcm.state
  #       enforced = true
  #     }
  #   }
  # }
  ```

- **Control plane.** Nodes have no public IPs. OpenTofu, `kubectl` and `gcloud`
  reach the API through the DNS-based endpoint, which accepts only requests
  carrying valid Google IAM credentials (`container.clusters.connect`), so it
  needs no IP allowlist. The public IP endpoint admits no addresses unless you
  list them in `control_plane_authorized_cidrs`.
- **Database.** Cloud SQL has no public IP and requires TLS. Connections use
  `sslmode=require`, which encrypts traffic but does not verify the server
  certificate: Cloud SQL signs it with a per-instance CA that is not in the
  images' trust stores. Traffic stays on the VPC's private peering.
- **Bucket.** Public access prevention is enforced and access is granted only
  through IAM (uniform bucket-level access) to the two Igloo service accounts.
- **Accepted Trivy findings**, each marked `trivy:ignore` in `storage.tf`:
  - GCP-0066, customer-managed encryption key. Data is encrypted at rest with
    Google-managed keys. CMEK is an organization-wide decision that also
    involves Cloud SQL and GKE; add an `encryption` block if you need it.
  - GCP-0077, bucket usage logs. Google recommends Cloud Audit Logs instead.
    Data Access audit logs for Cloud Storage are a project-wide setting, and
    with a query engine reading many objects they can cost more than the data.
    Turn them on at the project level if you need per-object audit trails.
  - GCP-0078, versioning. Available as `bucket_versioning`. Soft delete,
    on by default, already keeps deleted objects recoverable for 7 days.
- **Lakekeeper's management API** is not exposed outside the cluster; the
  chart's NetworkPolicy admits only igloo pods.

## Upgrading

Change `image_tag` (or pull a newer chart) and run `tofu apply`. GKE upgrades
itself within `gke_release_channel`; Cloud SQL applies maintenance on Sundays
at 04:00 UTC.

## Teardown

Deletion protection is on by default, and the bucket is never emptied unless
you ask.

```sh
# 1. Turn protection off. bucket_force_destroy=true deletes all table data;
#    leave it out to keep the bucket.
tofu apply   -var deletion_protection=false -var bucket_force_destroy=true

# 2. Destroy.
tofu destroy -var deletion_protection=false -var bucket_force_destroy=true
```

If you kept the bucket (`bucket_force_destroy = false` with data in it),
`tofu destroy` stops at the bucket after removing everything else. Empty it
yourself, or remove it from state with `tofu state rm google_storage_bucket.warehouse`
to keep it. Cloud SQL instance names cannot be reused for a week, which is why
the instance name carries a random suffix.

## Troubleshooting

- **`Unauthorized` from Kubernetes or Helm late in an apply.** The access token
  read at plan time lasts an hour. Run `tofu apply` again.
- **Plan fails on `helm_release.igloo` because the chart path is missing.** The
  chart is not in this checkout yet; set `install_chart = false`.
- **HMAC key creation is denied.** An organization policy forbids service
  account HMAC keys; see [Prerequisites](#prerequisites).
- **Pods stay `Pending` for a few minutes.** Autopilot is provisioning nodes.
  `kubectl -n igloo get events` shows progress.

## Tests

`tests/` holds `tofu test` suites that mock every provider and only plan, so
they need no credentials and make no cloud calls:

```sh
tofu init -backend=false
tofu validate
tofu test
```

Or with Docker:

```sh
docker run --rm -v "$PWD/../..:/repo" -w /repo/deploy/gcp ghcr.io/opentofu/opentofu:1.13.1 init -backend=false
docker run --rm -v "$PWD/../..:/repo" -w /repo/deploy/gcp ghcr.io/opentofu/opentofu:1.13.1 test
```
