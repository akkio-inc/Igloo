# ---------------------------------------------------------------------------
# Azure basics
# ---------------------------------------------------------------------------

variable "subscription_id" {
  description = "Azure subscription to deploy into. Null uses the ARM_SUBSCRIPTION_ID environment variable."
  type        = string
  default     = null

  validation {
    condition     = var.subscription_id == null || can(regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", var.subscription_id))
    error_message = "subscription_id must be a GUID."
  }
}

variable "name" {
  description = "Prefix for every resource name. 3-16 characters: lowercase letters, digits and hyphens, starting with a letter."
  type        = string
  default     = "igloo"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,14}[a-z0-9]$", var.name))
    error_message = "name must be 3-16 characters of lowercase letters, digits and hyphens, start with a letter and not end with a hyphen."
  }
}

variable "location" {
  description = "Azure region, as a short name (az account list-locations -o table). The region must offer the VM sizes chosen below."
  type        = string
  default     = "eastus2"

  validation {
    condition     = can(regex("^[a-z][a-z0-9]+$", var.location))
    error_message = "location must be a short region name such as eastus2 or westeurope."
  }
}

variable "resource_group_name" {
  description = "Name of the resource group to create. Null uses \"<name>-rg\"."
  type        = string
  default     = null

  validation {
    condition     = var.resource_group_name == null || can(regex("^[-\\w\\.\\(\\)]{1,89}[-\\w\\(\\)]$", var.resource_group_name))
    error_message = "resource_group_name must be 1-90 characters of letters, digits, underscores, hyphens, periods and parentheses, not ending in a period."
  }
}

variable "tags" {
  description = "Tags added to every Azure resource."
  type        = map(string)
  default     = {}
}

# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------

variable "vnet_cidr" {
  description = "Address space of the virtual network. The first 1/16 becomes the AKS node subnet and a 1/256 slice after it the PostgreSQL subnet (10.42.0.0/20 and 10.42.16.0/24 for the default)."
  type        = string
  default     = "10.42.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.vnet_cidr)) && tonumber(split("/", var.vnet_cidr)[1]) >= 8 && tonumber(split("/", var.vnet_cidr)[1]) <= 20
    error_message = "vnet_cidr must be an IPv4 CIDR block between /8 and /20."
  }
}

variable "aks_service_cidr" {
  description = "Kubernetes Service address range. Must not overlap vnet_cidr, aks_pod_cidr or any network the cluster needs to reach."
  type        = string
  default     = "10.0.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.aks_service_cidr))
    error_message = "aks_service_cidr must be an IPv4 CIDR block."
  }
}

variable "aks_pod_cidr" {
  description = "Pod address range (Azure CNI Overlay). Pod addresses are not routable outside the cluster, but must not overlap vnet_cidr or aks_service_cidr."
  type        = string
  default     = "10.244.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.aks_pod_cidr))
    error_message = "aks_pod_cidr must be an IPv4 CIDR block."
  }
}

variable "availability_zones" {
  description = "Availability zones for the node pools (and the SQL public IP), e.g. [\"1\", \"2\", \"3\"]. Empty lets Azure place nodes in any zone; use it in regions without zones or where the chosen VM size is not zonal."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for z in var.availability_zones : contains(["1", "2", "3"], z)])
    error_message = "availability_zones entries must be \"1\", \"2\" or \"3\"."
  }
}

# ---------------------------------------------------------------------------
# AKS
# ---------------------------------------------------------------------------

variable "kubernetes_version" {
  description = "Kubernetes minor version, e.g. \"1.34\". Null uses the AKS default. Patch releases are applied automatically."
  type        = string
  default     = null

  validation {
    condition     = var.kubernetes_version == null || can(regex("^[0-9]+\\.[0-9]+(\\.[0-9]+)?$", var.kubernetes_version))
    error_message = "kubernetes_version must look like 1.34 or 1.34.2."
  }
}

variable "aks_sku_tier" {
  description = "AKS control plane tier: Free (no SLA), Standard (financially backed SLA, about $73/month) or Premium."
  type        = string
  default     = "Free"

  validation {
    condition     = contains(["Free", "Standard", "Premium"], var.aks_sku_tier)
    error_message = "aks_sku_tier must be Free, Standard or Premium."
  }
}

variable "api_server_authorized_ip_ranges" {
  description = "CIDR blocks allowed to reach the Kubernetes API server. Empty allows any address (requests still need valid credentials). If set, include the address you run `tofu` from."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for c in var.api_server_authorized_ip_ranges : can(cidrnetmask(c))])
    error_message = "api_server_authorized_ip_ranges entries must be IPv4 CIDR blocks such as 203.0.113.7/32."
  }
}

variable "admin_group_object_ids" {
  description = "Entra ID group object IDs that get cluster-admin. Non-empty enables Entra ID authentication with Azure RBAC for Kubernetes; users then need kubelogin. Empty uses Kubernetes-native accounts only."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for id in var.admin_group_object_ids : can(regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", id))])
    error_message = "admin_group_object_ids entries must be GUIDs."
  }
}

variable "system_node_vm_size" {
  description = "VM size of the system node pool, which runs cluster add-ons and the Lakekeeper catalog. Default: 2 vCPU / 8 GiB Arm64 (Azure Cobalt 100)."
  type        = string
  default     = "Standard_D2ps_v6"

  validation {
    condition     = can(regex("^Standard_[A-Za-z0-9_]+$", var.system_node_vm_size))
    error_message = "system_node_vm_size must be an Azure VM size such as Standard_D2ps_v6."
  }
}

variable "system_node_min_count" {
  description = "Minimum number of system nodes (the pool autoscales)."
  type        = number
  default     = 1

  validation {
    condition     = var.system_node_min_count >= 1 && floor(var.system_node_min_count) == var.system_node_min_count
    error_message = "system_node_min_count must be a whole number of at least 1."
  }
}

variable "system_node_max_count" {
  description = "Maximum number of system nodes."
  type        = number
  default     = 3

  validation {
    condition     = var.system_node_max_count >= var.system_node_min_count && var.system_node_max_count <= 100
    error_message = "system_node_max_count must be between system_node_min_count and 100."
  }
}

variable "igloo_node_vm_size" {
  description = "VM size of the node pool that runs the Igloo query engine. Default: 4 vCPU / 32 GiB memory-optimized Arm64 (Azure Cobalt 100) with a 220 GiB local NVMe disk, one Igloo pod per node. Igloo images are multi-arch, so x86 sizes such as Standard_E4ads_v5 work too. Sizes with a local disk (a \"d\" in the name) get an ephemeral OS disk on it; see igloo_node_os_disk_type."
  type        = string
  default     = "Standard_E4pds_v6"

  validation {
    condition     = can(regex("^Standard_[A-Za-z0-9_]+$", var.igloo_node_vm_size))
    error_message = "igloo_node_vm_size must be an Azure VM size such as Standard_E4ps_v6."
  }
}

variable "igloo_node_min_count" {
  description = "Minimum number of Igloo nodes (the pool autoscales). With the default sizing each node holds one Igloo pod, so keep this at least replica_count."
  type        = number
  default     = 2

  validation {
    condition     = var.igloo_node_min_count >= 0 && floor(var.igloo_node_min_count) == var.igloo_node_min_count
    error_message = "igloo_node_min_count must be a whole number of at least 0."
  }
}

variable "igloo_node_max_count" {
  description = "Maximum number of Igloo nodes. Leave room above replica_count so rolling updates can add a node."
  type        = number
  default     = 4

  validation {
    condition     = var.igloo_node_max_count >= max(var.igloo_node_min_count, 1) && var.igloo_node_max_count <= 1000
    error_message = "igloo_node_max_count must be at least 1, at least igloo_node_min_count, and at most 1000."
  }
}

variable "igloo_node_os_disk_type" {
  description = "OS disk of Igloo nodes: Ephemeral, on the VM's local disk (NVMe on v6 sizes), or Managed, a remote Premium SSD. Query spill (an emptyDir of igloo_spill_size) lives on the OS disk, so Ephemeral puts it on local disk. Null picks Ephemeral for sizes with a local disk (a \"d\" after the vCPU count, as in Standard_E4pds_v6) and Managed for the rest. Ephemeral needs a local disk at least as large as the OS disk, 128 GiB unless igloo_node_os_disk_size_gb says otherwise."
  type        = string
  default     = null

  validation {
    condition     = var.igloo_node_os_disk_type == null || contains(["Ephemeral", "Managed"], coalesce(var.igloo_node_os_disk_type, "Managed"))
    error_message = "igloo_node_os_disk_type must be null, Ephemeral or Managed."
  }
}

variable "igloo_node_os_disk_size_gb" {
  description = "OS disk size of Igloo nodes in GB. Query spill (an emptyDir of igloo_spill_size) lives on this disk. Null lets AKS choose: an ephemeral OS disk takes the whole local disk (220 GiB on Standard_E4pds_v6), a managed one 128 GB on sizes with up to 7 vCPUs."
  type        = number
  default     = null

  validation {
    condition     = var.igloo_node_os_disk_size_gb == null || (coalesce(var.igloo_node_os_disk_size_gb, 64) >= 64 && coalesce(var.igloo_node_os_disk_size_gb, 64) <= 2048)
    error_message = "igloo_node_os_disk_size_gb must be null or between 64 and 2048."
  }
}

variable "spot_enabled" {
  description = "Run the Igloo node pool on Spot VMs (typically 60-90% cheaper, but Azure can evict nodes at any time, interrupting running queries). Igloo pods get the matching toleration and node selector. The catalog stays on the regular system pool."
  type        = bool
  default     = false
}

variable "spot_max_price" {
  description = "Maximum hourly price in USD for Spot nodes: the Igloo pool with spot_enabled, and the Spot worker pools. -1 pays up to the on-demand price and is never evicted for price."
  type        = number
  default     = -1

  validation {
    condition     = var.spot_max_price == -1 || var.spot_max_price > 0
    error_message = "spot_max_price must be -1 or a positive price."
  }
}

# ---------------------------------------------------------------------------
# Storage (ADLS Gen2)
# ---------------------------------------------------------------------------

variable "storage_account_name" {
  description = "Globally unique storage account name (3-24 lowercase letters and digits). Null derives one from name plus a random suffix."
  type        = string
  default     = null

  validation {
    condition     = var.storage_account_name == null || can(regex("^[a-z0-9]{3,24}$", var.storage_account_name))
    error_message = "storage_account_name must be 3-24 lowercase letters and digits."
  }
}

variable "storage_replication_type" {
  description = "Storage account redundancy. ZRS keeps three copies across availability zones; use LRS in regions without zones."
  type        = string
  default     = "ZRS"

  validation {
    condition     = contains(["LRS", "ZRS", "GRS", "GZRS", "RAGRS", "RAGZRS"], var.storage_replication_type)
    error_message = "storage_replication_type must be one of LRS, ZRS, GRS, GZRS, RAGRS, RAGZRS."
  }
}

variable "filesystem_name" {
  description = "Name of the ADLS Gen2 filesystem (blob container) that holds table data."
  type        = string
  default     = "igloo"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.filesystem_name)) && !strcontains(var.filesystem_name, "--")
    error_message = "filesystem_name must be 3-63 characters of lowercase letters, digits and single hyphens, starting and ending with a letter or digit."
  }
}

variable "storage_shared_key_enabled" {
  description = "Allow storage account keys and account SAS. Igloo and Lakekeeper both authenticate with Entra ID workload identities, so this stays off unless another tool needs keys."
  type        = bool
  default     = false
}

variable "storage_allowed_ip_ranges" {
  description = "Public IPv4 addresses or CIDR blocks (prefix /30 or shorter) allowed through the storage firewall in addition to the AKS subnet, e.g. for Spark or Trino outside Azure reading the same tables."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for c in var.storage_allowed_ip_ranges :
      can(regex("^[0-9]{1,3}(\\.[0-9]{1,3}){3}$", c)) || (can(cidrnetmask(c)) && tonumber(split("/", c)[1]) <= 30)
    ])
    error_message = "storage_allowed_ip_ranges entries must be IPv4 addresses or CIDR blocks no longer than /30 (Azure Storage rejects /31 and /32; give a single address without a prefix)."
  }
}

variable "storage_soft_delete_retention_days" {
  description = "Days to keep deleted blobs and filesystems recoverable. 0 turns soft delete off. Iceberg maintenance deletes many files, which are billed while retained."
  type        = number
  default     = 7

  validation {
    condition     = var.storage_soft_delete_retention_days >= 0 && var.storage_soft_delete_retention_days <= 365 && floor(var.storage_soft_delete_retention_days) == var.storage_soft_delete_retention_days
    error_message = "storage_soft_delete_retention_days must be a whole number from 0 to 365."
  }
}

# ---------------------------------------------------------------------------
# PostgreSQL (catalog and user metadata)
# ---------------------------------------------------------------------------

variable "postgres_sku_name" {
  description = "PostgreSQL Flexible Server SKU. The default Burstable B2s (2 vCore, 4 GiB) allows about 430 connections; Burstable does not support high availability."
  type        = string
  default     = "B_Standard_B2s"

  validation {
    condition     = can(regex("^(B|GP|MO)_Standard_[A-Za-z0-9_]+$", var.postgres_sku_name))
    error_message = "postgres_sku_name must look like B_Standard_B2s, GP_Standard_D2ds_v5 or MO_Standard_E2ds_v5."
  }
}

variable "postgres_storage_mb" {
  description = "PostgreSQL storage in MB. It grows automatically when nearly full."
  type        = number
  default     = 32768

  validation {
    condition     = contains([32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4193280, 4194304, 8388608, 16777216, 33553408], var.postgres_storage_mb)
    error_message = "postgres_storage_mb must be one of the sizes Azure offers: 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4193280, 4194304, 8388608, 16777216 or 33553408."
  }
}

variable "postgres_backup_retention_days" {
  description = "Days of point-in-time-restore backups for PostgreSQL."
  type        = number
  default     = 7

  validation {
    condition     = var.postgres_backup_retention_days >= 7 && var.postgres_backup_retention_days <= 35
    error_message = "postgres_backup_retention_days must be between 7 and 35."
  }
}

variable "postgres_geo_redundant_backup_enabled" {
  description = "Copy PostgreSQL backups to the paired region. Can only be set when the server is created."
  type        = bool
  default     = false
}

variable "postgres_high_availability" {
  description = "Run a zone-redundant standby PostgreSQL server. Doubles the database cost and needs a General Purpose or Memory Optimized SKU in a region with availability zones."
  type        = bool
  default     = false

  validation {
    condition     = !var.postgres_high_availability || !startswith(var.postgres_sku_name, "B_")
    error_message = "postgres_high_availability needs a General Purpose (GP_) or Memory Optimized (MO_) postgres_sku_name; Burstable servers do not support high availability."
  }
}

variable "postgres_admin_username" {
  description = "PostgreSQL administrator login. Igloo and Lakekeeper both connect as this user."
  type        = string
  default     = "iglooadmin"

  validation {
    condition = (
      can(regex("^[a-z][a-z0-9_]{0,62}$", var.postgres_admin_username)) &&
      !startswith(var.postgres_admin_username, "pg_") &&
      !contains(["admin", "administrator", "azure_pg_admin", "azure_superuser", "guest", "postgres", "public", "root", "sa"], var.postgres_admin_username)
    )
    error_message = "postgres_admin_username must be lowercase letters, digits and underscores starting with a letter, must not start with pg_, and must not be a name Azure reserves (admin, administrator, azure_pg_admin, azure_superuser, guest, postgres, public, root, sa)."
  }
}

# ---------------------------------------------------------------------------
# Kubernetes and the Igloo chart
# ---------------------------------------------------------------------------

variable "namespace" {
  description = "Kubernetes namespace for Igloo."
  type        = string
  default     = "igloo"

  validation {
    condition     = can(regex("^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$", var.namespace))
    error_message = "namespace must be a DNS label: up to 63 lowercase letters, digits and hyphens."
  }
}

variable "release_name" {
  description = "Helm release name. Also prefixes the Secrets this module creates."
  type        = string
  default     = "igloo"

  validation {
    condition     = can(regex("^[a-z0-9]([-a-z0-9]{0,51}[a-z0-9])?$", var.release_name))
    error_message = "release_name must be up to 53 lowercase letters, digits and hyphens."
  }
}

variable "igloo_service_account_name" {
  description = "Kubernetes service account for Igloo pods. Its Azure workload identity is bound to exactly this name."
  type        = string
  default     = "igloo"

  validation {
    condition     = can(regex("^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$", var.igloo_service_account_name))
    error_message = "igloo_service_account_name must be a DNS label: up to 63 lowercase letters, digits and hyphens."
  }
}

variable "catalog_service_account_name" {
  description = "Kubernetes service account for the Lakekeeper catalog pods. Its Azure workload identity is bound to exactly this name."
  type        = string
  default     = "igloo-catalog"

  validation {
    condition     = can(regex("^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$", var.catalog_service_account_name)) && var.catalog_service_account_name != var.igloo_service_account_name
    error_message = "catalog_service_account_name must be a DNS label and differ from igloo_service_account_name."
  }
}

variable "install_chart" {
  description = "Install the Igloo Helm chart. Set to false to create only the infrastructure, namespace and Secrets, then install the chart yourself with the helm_values output."
  type        = bool
  default     = true
}

variable "chart_path" {
  description = "Path to the Igloo chart. Empty uses the chart in this repository (deploy/helm/igloo)."
  type        = string
  default     = ""
}

variable "helm_timeout_seconds" {
  description = "How long Helm waits for Igloo to become ready, including Spot or autoscaled nodes joining."
  type        = number
  default     = 900

  validation {
    condition     = var.helm_timeout_seconds >= 60 && var.helm_timeout_seconds <= 3600
    error_message = "helm_timeout_seconds must be between 60 and 3600."
  }
}

variable "image_repository" {
  description = "Igloo image repository, for a mirror or private registry. Empty uses the chart's default."
  type        = string
  default     = ""
}

variable "image_tag" {
  description = "Igloo image tag. Empty uses the chart's appVersion."
  type        = string
  default     = ""
}

variable "replica_count" {
  description = "Number of Igloo pods. Each is a complete query engine; clients are spread across them."
  type        = number
  default     = 2

  validation {
    condition     = var.replica_count >= 1 && floor(var.replica_count) == var.replica_count
    error_message = "replica_count must be a whole number of at least 1."
  }
}

variable "catalog_replica_count" {
  description = "Number of Lakekeeper catalog pods."
  type        = number
  default     = 1

  validation {
    condition     = var.catalog_replica_count >= 1 && floor(var.catalog_replica_count) == var.catalog_replica_count
    error_message = "catalog_replica_count must be a whole number of at least 1."
  }
}

variable "admin_user" {
  description = "Name of the first Igloo administrator. Its password is generated and stored in a Kubernetes Secret."
  type        = string
  default     = "igloo"

  validation {
    condition     = can(regex("^[a-z_][a-z0-9_]{0,62}$", var.admin_user))
    error_message = "admin_user must be lowercase letters, digits and underscores, starting with a letter or underscore."
  }
}

variable "igloo_cpu" {
  description = "CPU request of each Igloo pod. There is no CPU limit, so a pod can use idle cores on its node."
  type        = string
  default     = "2"

  validation {
    condition     = can(regex("^([0-9]+(\\.[0-9]+)?|[0-9]+m)$", var.igloo_cpu))
    error_message = "igloo_cpu must be a Kubernetes CPU quantity such as 2, 1.5 or 500m."
  }
}

variable "igloo_memory" {
  description = "Memory request and limit of each Igloo pod. DuckDB uses about 80% of it before spilling to disk. The default fits one pod on each 32 GiB default node."
  type        = string
  default     = "24Gi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi)$", var.igloo_memory))
    error_message = "igloo_memory must be a quantity in Mi or Gi, such as 24Gi."
  }
}

variable "igloo_spill_size" {
  description = "Size limit of each Igloo pod's spill directory (an emptyDir on the node's OS disk, which is the local NVMe disk on the default nodes)."
  type        = string
  default     = "50Gi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi|Ti)$", var.igloo_spill_size))
    error_message = "igloo_spill_size must be a quantity in Mi, Gi or Ti, such as 50Gi."
  }
}

# ---------------------------------------------------------------------------
# SQL endpoint exposure
# ---------------------------------------------------------------------------

variable "expose_sql" {
  description = "Expose the SQL port (5432) on a static public IP through an Azure load balancer. False keeps Igloo reachable only inside the cluster (use kubectl port-forward)."
  type        = bool
  default     = false
}

variable "sql_allowed_cidrs" {
  description = "IPv4 CIDR blocks allowed to reach the SQL endpoint when expose_sql is true. Required then; there is deliberately no open default."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for c in var.sql_allowed_cidrs : can(cidrnetmask(c))])
    error_message = "sql_allowed_cidrs entries must be IPv4 CIDR blocks such as 203.0.113.0/24."
  }

  validation {
    condition     = !var.expose_sql || length(var.sql_allowed_cidrs) > 0
    error_message = "expose_sql = true needs at least one entry in sql_allowed_cidrs."
  }
}

variable "sql_dns_label" {
  description = "Optional DNS label for the SQL public IP, giving <label>.<location>.cloudapp.azure.com. Must be unique in the region."
  type        = string
  default     = null

  validation {
    condition     = var.sql_dns_label == null || can(regex("^[a-z][a-z0-9-]{1,61}[a-z0-9]$", var.sql_dns_label))
    error_message = "sql_dns_label must be 3-63 lowercase letters, digits and hyphens, starting with a letter."
  }
}

variable "tls_certificate_pem" {
  description = "PEM certificate (with chain) for the SQL endpoint. Set together with tls_private_key_pem. Strongly recommended when expose_sql is true."
  type        = string
  default     = null
  sensitive   = true
}

variable "tls_private_key_pem" {
  description = "PEM private key matching tls_certificate_pem."
  type        = string
  default     = null
  sensitive   = true

  validation {
    condition     = (var.tls_certificate_pem == null) == (var.tls_private_key_pem == null)
    error_message = "Set both tls_certificate_pem and tls_private_key_pem, or neither."
  }
}

# ---------------------------------------------------------------------------
# Distributed-execution workers (docs/distributed-execution.md)
# ---------------------------------------------------------------------------

variable "workers_enabled" {
  description = "Add autoscaling node pools for distributed-execution workers, from zero nodes, and turn workers on in the chart. Also sets the cluster autoscaler's expander to priority. Off by default, so existing deployments are unchanged."
  type        = bool
  default     = false
}

variable "workers_spot" {
  description = "Run workers on Spot VMs, falling back to the regular worker pool when Spot capacity is not available. Workers hold no state and a lost worker's tasks run again on another, so Spot suits them; Igloo pods stay on the Igloo pool."
  type        = bool
  default     = true
}

variable "workers_vm_size" {
  description = "VM size of worker nodes, one worker per node. Default: 16 vCPU / 64 GiB Arm64 (Azure Cobalt 100) with an 880 GiB local NVMe disk, which holds the ephemeral OS disk and with it the workers' spill and read cache. Change workers_cpu, workers_memory, workers_spill_size and workers_cache_size with it."
  type        = string
  default     = "Standard_D16pds_v6"

  validation {
    condition     = can(regex("^Standard_[A-Za-z0-9_]+$", var.workers_vm_size))
    error_message = "workers_vm_size must be an Azure VM size such as Standard_D16pds_v6."
  }
}

variable "workers_max" {
  description = "The most worker pods, and so worker nodes, Igloo starts (the chart's workers.maxReplicas, and each worker pool's maximum)."
  type        = number
  default     = 4

  validation {
    condition     = var.workers_max >= 1 && var.workers_max <= 1000 && floor(var.workers_max) == var.workers_max
    error_message = "workers_max must be a whole number from 1 to 1000."
  }
}

variable "workers_cpu" {
  description = "CPU request of each worker pod. The default leaves two of the default size's 16 vCPUs for AKS's reservation and DaemonSets."
  type        = string
  default     = "14"

  validation {
    condition     = can(regex("^([0-9]+(\\.[0-9]+)?|[0-9]+m)$", var.workers_cpu))
    error_message = "workers_cpu must be a Kubernetes CPU quantity such as 14 or 500m."
  }
}

variable "workers_memory" {
  description = "Memory request and limit of each worker pod: 85% of the default size's 64 GiB."
  type        = string
  default     = "54Gi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi)$", var.workers_memory))
    error_message = "workers_memory must be a quantity in Mi or Gi, such as 54Gi."
  }
}

variable "workers_spill_size" {
  description = "Size limit of each worker's spill emptyDir, on the node's OS disk. Spill and read cache together fit the default size's 880 GiB local disk with room for the OS and images."
  type        = string
  default     = "480Gi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi|Ti)$", var.workers_spill_size))
    error_message = "workers_spill_size must be a quantity in Mi, Gi or Ti, such as 480Gi."
  }
}

variable "workers_cache_size" {
  description = "Size limit of each worker's read cache emptyDir, on the node's OS disk."
  type        = string
  default     = "220Gi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi|Ti)$", var.workers_cache_size))
    error_message = "workers_cache_size must be a quantity in Mi, Gi or Ti, such as 220Gi."
  }
}

variable "workers_price_per_hour" {
  description = "What one worker node costs an hour in dollars (the chart's workers.pricePerHour), for auto mode's cost estimates. 0 is unknown."
  type        = number
  default     = 0

  validation {
    condition     = var.workers_price_per_hour >= 0
    error_message = "workers_price_per_hour must be 0 or more."
  }
}

variable "large_worker_enabled" {
  description = "Add an autoscaling node pool, from zero nodes, for one large worker that auto mode may run a whole statement on, and turn it on in the chart (workers.large). Needs workers_enabled."
  type        = bool
  default     = false

  validation {
    condition     = !var.large_worker_enabled || var.workers_enabled
    error_message = "large_worker_enabled needs workers_enabled: the large worker is one of the workers."
  }
}

variable "large_worker_spot" {
  description = "Run the large worker on a Spot VM, falling back to its regular pool when there is none. Off by default: a large Spot VM is evicted more often, and the large worker runs whole statements."
  type        = bool
  default     = false
}

variable "large_worker_vm_size" {
  description = "VM size of the large worker's node. Default: 64 vCPU / 512 GiB Arm64 (Azure Cobalt 100) with local NVMe. Change large_worker_cpu and large_worker_memory with it."
  type        = string
  default     = "Standard_E64pds_v6"

  validation {
    condition     = can(regex("^Standard_[A-Za-z0-9_]+$", var.large_worker_vm_size))
    error_message = "large_worker_vm_size must be an Azure VM size such as Standard_E64pds_v6."
  }
}

variable "large_worker_cpu" {
  description = "CPU request of the large worker pod: 90% of the default size's 64 vCPUs."
  type        = string
  default     = "57"

  validation {
    condition     = can(regex("^([0-9]+(\\.[0-9]+)?|[0-9]+m)$", var.large_worker_cpu))
    error_message = "large_worker_cpu must be a Kubernetes CPU quantity such as 57."
  }
}

variable "large_worker_memory" {
  description = "Memory request and limit of the large worker pod: 85% of the default size's 512 GiB."
  type        = string
  default     = "435Gi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi)$", var.large_worker_memory))
    error_message = "large_worker_memory must be a quantity in Mi or Gi, such as 435Gi."
  }
}

variable "large_worker_spill_size" {
  description = "Size limit of the large worker's spill emptyDir, on its node's OS disk. The default fits one 880 GiB local NVMe disk; a statement auto mode sends to the large worker is one that fits in its memory."
  type        = string
  default     = "480Gi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi|Ti)$", var.large_worker_spill_size))
    error_message = "large_worker_spill_size must be a quantity in Mi, Gi or Ti, such as 480Gi."
  }
}

variable "large_worker_cache_size" {
  description = "Size limit of the large worker's read cache emptyDir, on its node's OS disk."
  type        = string
  default     = "220Gi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi|Ti)$", var.large_worker_cache_size))
    error_message = "large_worker_cache_size must be a quantity in Mi, Gi or Ti, such as 220Gi."
  }
}

variable "large_worker_price_per_hour" {
  description = "What the large worker's node costs an hour in dollars (the chart's workers.large.pricePerHour), for auto mode's cost estimates. 0 is unknown."
  type        = number
  default     = 0

  validation {
    condition     = var.large_worker_price_per_hour >= 0
    error_message = "large_worker_price_per_hour must be 0 or more."
  }
}
