# Igloo on Azure

An OpenTofu module that deploys a complete Igloo warehouse into one Azure
subscription with a single `tofu apply`: an AKS cluster running Igloo and the
Lakekeeper catalog, table data in Azure Data Lake Storage Gen2, and catalog
metadata in Azure Database for PostgreSQL. Nothing uses long-lived Azure
credentials: Igloo and Lakekeeper each sign in to Azure with their own
Kubernetes workload identity.

```
 SQL clients ──▶ (port-forward, or a load balancer limited to sql_allowed_cidrs)
                  │
 ┌─ AKS ──────────▼───────────────────────────────────────────┐
 │  igloo pods (Igloo node pool)   lakekeeper pod (system pool)│
 │   workload identity "igloo"      workload identity "catalog"│
 └───────┬───────────────────────────────┬─────────────────────┘
         │ abfss:// (Entra ID auth,      │ postgresql://...sslmode=require
         │ storage firewall: AKS subnet) │ (private subnet, private DNS)
         ▼                               ▼
 ADLS Gen2 filesystem            PostgreSQL 17 Flexible Server
 (table data)                    database "igloo": Lakekeeper (schema public),
                                 Igloo users and roles (schema igloo_auth)
```

## Prerequisites

- [OpenTofu](https://opentofu.org/docs/intro/install/) 1.9 or later (or Docker, see [Testing](#testing)).
- The [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli), signed in with `az login`.
- `kubectl` and `psql` to connect afterwards.
- On the target subscription: **Owner**, or **Contributor** plus **Role Based
  Access Control Administrator** (the module creates role assignments).
- vCPU quota in the region for the VM families you use. The defaults need the
  `Standard DPSv6 Family` (2 to 6 vCPUs) and `Standard EPSv6 Family` (8 to 16
  vCPUs); Spot nodes draw from the separate low-priority quota. Check with:
  ```sh
  az vm list-usage --location eastus2 --output table | grep -i -E 'psv6|low-priority'
  ```
- The Igloo chart at `deploy/helm/igloo` in this repository (or set `chart_path`).

## Quick start

```sh
cd deploy/azure
export ARM_SUBSCRIPTION_ID=$(az account show --query id --output tsv)
cat > terraform.tfvars <<'EOF'
name     = "igloo"
location = "eastus2"

# Optional: allow only your address to reach the Kubernetes API.
# api_server_authorized_ip_ranges = ["203.0.113.7/32"]

# Optional: a public SQL endpoint. Requires an allow list; give it TLS
# (see below).
# expose_sql        = true
# sql_allowed_cidrs = ["203.0.113.0/24"]
# sql_dns_label     = "acme-igloo"

# Optional: cheaper, interruptible Igloo nodes.
# spot_enabled = true
EOF
# With expose_sql, pass the certificate and key through the environment
# (tfvars files cannot read files):
#   export TF_VAR_tls_certificate_pem="$(cat fullchain.pem)"
#   export TF_VAR_tls_private_key_pem="$(cat privkey.pem)"
tofu init
tofu apply          # about 15-25 minutes
tofu output next_steps
```

`terraform.tfvars` is ignored by git (see `.gitignore`). Every input is
documented in [variables.tf](variables.tf).

## What gets created

| Resource | Default | Notes |
|---|---|---|
| Resource group | `<name>-rg` | AKS keeps its VMs in `<name>-rg-aks-nodes`. |
| Virtual network | `10.42.0.0/16` | Subnet `aks-nodes` (`/20`, service endpoint for Storage) and subnet `postgres` (`/24`, delegated to PostgreSQL). |
| AKS cluster | Free tier, Azure CNI Overlay with Cilium | OIDC issuer and workload identity on, automatic patch upgrades, Kubernetes RBAC. |
| System node pool | `Standard_D2ps_v6` × 1-3 | Cluster add-ons and the Lakekeeper catalog. |
| Igloo node pool | `Standard_E4ps_v6` × 2-4 | Label `igloo/pool=query`; Igloo pods select it. Spot when `spot_enabled`. |
| Storage account | StorageV2, ZRS, hierarchical namespace | TLS 1.2, HTTPS only, no public blob access, shared keys off, firewall default deny, 7-day soft delete. |
| ADLS Gen2 filesystem | `igloo` | Warehouse at `abfss://igloo@<account>.dfs.core.windows.net/warehouse`. |
| Managed identities | `<name>-igloo`, `<name>-catalog`, `<name>-aks` | Federated credentials bind the first two to the `igloo` and `igloo-catalog` service accounts. |
| PostgreSQL Flexible Server 17 | `B_Standard_B2s`, 32 GiB, 7-day backups, no HA | Private access only; TLS required; database `igloo`. |
| Private DNS zone | `<name>.private.postgres.database.azure.com` | Linked to the VNet. |
| Kubernetes namespace | `igloo` | Pod Security: `baseline` enforced, `restricted` warned. |
| Kubernetes Secrets | `igloo-admin`, `igloo-database`, `igloo-catalog-encryption` (+ `igloo-tls`) | Generated passwords and keys; the chart reads them by name. |
| Helm release | `igloo` from `deploy/helm/igloo` | Skipped with `install_chart = false`; the `helm_values` output holds the values. |
| Public IP | only with `expose_sql` | Static, so the address survives chart reinstalls. |

### Role assignments

| Identity | Role | Scope | Why |
|---|---|---|---|
| igloo | Storage Blob Data Contributor | the filesystem | DuckDB reads and writes table files. |
| catalog | Storage Blob Data Contributor | the filesystem | Lakekeeper validates the warehouse and manages metadata files. |
| catalog | Storage Blob Delegator | the storage account | Lakekeeper vends short-lived user-delegation SAS tokens. Azure accepts this permission only at account scope; it grants no data access by itself. |
| aks (control plane) | Network Contributor | the resource group | AKS joins nodes to the subnet and attaches the static SQL IP to its load balancer. |

## Connecting

First point `kubectl` at the cluster (the exact command is in the
`get_credentials_command` output):

```sh
az aks get-credentials --resource-group igloo-rg --name igloo-aks --overwrite-existing
```

If you set `admin_group_object_ids`, also run `kubelogin convert-kubeconfig -l azurecli`.

### Administrator password

The first administrator is `igloo` (`admin_user`). Its password is generated
and stored in the Secret named by the `admin_password_secret_name` output:

```sh
export PGPASSWORD="$(kubectl --namespace igloo get secret igloo-admin \
  --output jsonpath='{.data.password}' | base64 --decode)"
```

Change it after the first login with `ALTER USER`; the Secret is only used to
create the account on first start.

### Through a port-forward (default)

Igloo has no public endpoint unless you ask for one:

```sh
kubectl --namespace igloo port-forward service/igloo 5433:5432
psql "host=127.0.0.1 port=5433 user=igloo dbname=igloo"
```

### Through a load balancer (`expose_sql = true`)

The SQL port is published on a static public IP, and Azure's network security
group admits only `sql_allowed_cidrs`:

```sh
psql "host=$(tofu output -raw sql_endpoint_ip) port=5432 user=igloo dbname=igloo sslmode=require"
```

Without `tls_certificate_pem` and `tls_private_key_pem`, passwords are still
protected by SCRAM, but queries and results cross the internet unencrypted;
`tofu plan` warns about this. With `sql_dns_label`, the endpoint is also
`<label>.<location>.cloudapp.azure.com`, a name you can get a certificate for.

## Sizing

Igloo images are multi-arch (amd64 and arm64), so both node pools default to
Arm64 VMs on Azure Cobalt 100, which cost less than comparable x86 sizes. Each
Igloo pod is a complete DuckDB engine and benefits more from memory than from
cores, so Igloo nodes are memory-optimized: one `Standard_E4ps_v6` (4 vCPU,
32 GiB) per pod, with the pod requesting 2 CPUs and 24 GiB (`igloo_cpu`,
`igloo_memory`) and free to use idle cores. DuckDB uses about 80% of the
memory limit before spilling to `igloo_spill_size` of the node's OS disk.

To change sizes, keep the pod's memory a few GiB below the node's: AKS reserves
about 2.5 GiB per node and the system daemons need a little more. Examples:

| Igloo node | `igloo_memory` | Notes |
|---|---|---|
| `Standard_E4ps_v6` (4 vCPU, 32 GiB) | `24Gi` | Default |
| `Standard_E8ps_v6` (8 vCPU, 64 GiB) | `52Gi` | Larger queries |
| `Standard_E4ps_v5` (4 vCPU, 32 GiB) | `24Gi` | Ampere Altra; wider regional availability |
| `Standard_E4as_v5` (4 vCPU, 32 GiB) | `24Gi` | x86 (AMD) fallback |

If a size is not offered in your region, `tofu apply` fails creating the node
pool. Check with `az vm list-skus --location <region> --size Standard_E4ps --output table`.

With `spot_enabled = true` the Igloo pool runs on Spot VMs. Azure can evict
them at any time, which interrupts running queries and briefly reduces capacity
until the autoscaler replaces them; the catalog stays on regular nodes.

## Cost of the defaults

Approximate pay-as-you-go list prices in East US 2, 730 hours a month. Check
the [Azure pricing calculator](https://azure.microsoft.com/pricing/calculator/)
for your region and any discounts.

| Item | Monthly |
|---|---|
| AKS control plane, Free tier | $0 (Standard tier with SLA: ~$73) |
| System node: 1 × `Standard_D2ps_v6` + 64 GiB Premium SSD | ~$65 |
| Igloo nodes: 2 × `Standard_E4ps_v6` + 128 GiB Premium SSD each | ~$330 |
| Standard load balancer and its outbound IP | ~$22 |
| PostgreSQL `B_Standard_B2s` + 32 GiB storage (backups included up to that size) | ~$30 |
| Private DNS zone | ~$0.50 |
| **Total before data** | **~$450** |
| ADLS Gen2 (Hot, ZRS) | ~$23 per TB stored, plus operations |
| SQL public IP (`expose_sql`) | ~$4 |

Spot Igloo nodes typically cut the node line by 60-90%. During rolling
updates the autoscaler briefly adds an Igloo node (up to `igloo_node_max_count`).

## Security notes

- **No keys for storage.** Shared-key access is disabled on the storage
  account (`storage_shared_key_enabled = false`) because both Igloo and
  Lakekeeper authenticate with Entra ID workload identities; there is no
  account key to leak or rotate. The storage firewall denies everything except
  the AKS subnet (via a service endpoint) and `storage_allowed_ip_ranges`.
  This module manages the filesystem through Azure Resource Manager, so
  `tofu` itself never needs to reach the storage data plane.
- **Narrow data roles.** Data roles are scoped to the filesystem, not the
  account. Lakekeeper's warehouse is created with its Azure system identity, so
  Lakekeeper can reach anything the catalog identity can, and nothing more.
- **Private database.** PostgreSQL has no public endpoint, accepts only TLS
  connections, and logs connections and checkpoints.
- **The state file contains secrets**: the PostgreSQL, administrator and
  catalog-encryption passwords, and the cluster's admin kubeconfig. Keep state
  in an access-controlled, encrypted remote backend, for example an Azure
  storage account with Entra ID auth (create it beforehand, separately from
  this module). Add a `backend.tf` like:

  ```hcl
  # terraform {
  #   backend "azurerm" {
  #     resource_group_name  = "tfstate-rg"
  #     storage_account_name = "tfstateacme123"
  #     container_name       = "tfstate"
  #     key                  = "igloo-azure.tfstate"
  #     use_azuread_auth     = true
  #   }
  # }
  ```

  OpenTofu can also encrypt state client-side
  ([state encryption](https://opentofu.org/docs/language/state/encryption/));
  store the passphrase outside the state backend.
- **Kubernetes API.** The API server is public by default so `tofu` can install
  the chart from your machine; requests need cluster credentials. Restrict it
  with `api_server_authorized_ip_ranges`, and set `admin_group_object_ids` to
  use Entra ID sign-in with Azure RBAC for people.
- **Network policies.** The cluster runs Cilium, which enforces the chart's
  NetworkPolicies (only Igloo pods may reach Lakekeeper).

### Scanner findings left open

`trivy config` reports these on purpose:

| Check | Why it stays |
|---|---|
| AZU-0041 API server authorized IP ranges | The module cannot know your address; set `api_server_authorized_ip_ranges`. |
| AZU-0065 private cluster | `tofu` would then need network access into the VNet to install the chart. |
| AZU-0040 Azure Monitor logging, AZU-0057 storage logging | Log Analytics ingestion is billed per GB; add diagnostic settings or Container Insights if you need them. |
| AZU-0066 Azure Policy add-on | Governance is usually applied per organization, not per workload. |
| AZU-0067 disk encryption set, AZU-0060 customer-managed keys | Disks and storage are encrypted with platform keys (storage also with infrastructure encryption). Customer-managed keys need a Key Vault you manage. |
| AZU-0058 geo-redundant storage | ZRS survives a zone outage; set `storage_replication_type = "GZRS"` for regional disaster recovery at higher cost. |
| AZU-0021 PostgreSQL connection throttling | The server has no public endpoint, and all Igloo traffic shares node addresses, so throttling by IP would hurt more than it protects. |

## Operations

- **Upgrade Igloo**: set `image_tag` and run `tofu apply`.
- **Scale**: `replica_count` adds Igloo pods; the node pool autoscales between
  `igloo_node_min_count` and `igloo_node_max_count`. Keep the maximum above
  `replica_count` so rolling updates can add a node.
- **Install the chart yourself**: set `install_chart = false`, then
  `tofu output -raw helm_values > values.yaml` and
  `helm install igloo ../helm/igloo --namespace igloo --values values.yaml`.
- **Lakekeeper's encryption key** (`igloo-catalog-encryption`) must never change
  once the catalog holds data; do not taint `random_password.catalog_encryption_key`.

## Teardown

```sh
tofu destroy
```

This deletes everything, **including all table data and the catalog
database**. Back up or copy the filesystem first if you need it. Deleted blobs
stay recoverable for `storage_soft_delete_retention_days` only while the
storage account exists.

## Testing

The tests plan the module with every provider mocked, so they need no Azure
account or credentials. With Docker:

```sh
cd deploy/azure
docker run --rm -v "$PWD":/work -w /work ghcr.io/opentofu/opentofu:1.13.1 fmt -check -recursive
docker run --rm -v "$PWD":/work -w /work ghcr.io/opentofu/opentofu:1.13.1 init -backend=false
docker run --rm -v "$PWD":/work -w /work ghcr.io/opentofu/opentofu:1.13.1 validate
docker run --rm -v "$PWD":/work -w /work ghcr.io/opentofu/opentofu:1.13.1 test
```

## Troubleshooting

- **`SkuNotAvailable` or quota errors** creating a node pool: pick a size from
  the [Sizing](#sizing) table that your region offers, or request quota.
- **Igloo or Lakekeeper pods report `AADSTS70021` or 403 from storage** right
  after the first apply: new federated credentials and role assignments can
  take a few minutes to reach every Azure front end. The module waits 60
  seconds; the pods recover on restart.
- **`tofu apply` cannot reach the Kubernetes API** after you set
  `api_server_authorized_ip_ranges`: add the address you run `tofu` from.
