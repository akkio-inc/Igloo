# ---------------------------------------------------------------------------
# Project and naming
# ---------------------------------------------------------------------------

variable "project_id" {
  description = "ID of the Google Cloud project to deploy into. The project must already exist and have billing enabled."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must be a Google Cloud project ID: 6-30 characters, lowercase letters, digits and hyphens, starting with a letter."
  }
}

variable "region" {
  description = "Region for the cluster, database, bucket and network, e.g. us-central1."
  type        = string
  default     = "us-central1"

  validation {
    condition     = can(regex("^[a-z]+-[a-z]+[0-9]+$", var.region))
    error_message = "region must be a Google Cloud region such as us-central1 or europe-west4 (not a zone)."
  }
}

variable "name" {
  description = "Prefix for every resource this module creates. Also the GKE cluster name."
  type        = string
  default     = "igloo"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,18}[a-z0-9]$", var.name))
    error_message = "name must be 2-20 characters of lowercase letters, digits and hyphens, starting with a letter and not ending with a hyphen."
  }
}

variable "labels" {
  description = "Extra labels for the resources that support them."
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for k, v in var.labels : can(regex("^[a-z][a-z0-9_-]{0,62}$", k)) && can(regex("^[a-z0-9_-]{0,63}$", v))
    ])
    error_message = "Label keys must start with a lowercase letter; keys and values may contain only lowercase letters, digits, '_' and '-' (63 characters at most)."
  }
}

variable "deletion_protection" {
  description = "Protect the GKE cluster and the Cloud SQL instance from deletion. Set to false and apply before `tofu destroy`."
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------

variable "subnet_cidr" {
  description = "Primary IPv4 range of the node subnet."
  type        = string
  default     = "10.10.0.0/20"

  validation {
    condition     = can(cidrsubnet(var.subnet_cidr, 0, 0)) && try(cidrsubnet(var.subnet_cidr, 0, 0) == var.subnet_cidr, false) && can(regex("^[0-9.]+/[0-9]+$", var.subnet_cidr))
    error_message = "subnet_cidr must be an IPv4 network in CIDR notation with no host bits set, e.g. 10.10.0.0/20."
  }
}

variable "pods_cidr" {
  description = "Secondary range for pod IPs (VPC-native networking). Autopilot gives each node a /26, so a /16 holds about 1,000 nodes."
  type        = string
  default     = "10.20.0.0/16"

  validation {
    condition     = can(cidrsubnet(var.pods_cidr, 0, 0)) && try(cidrsubnet(var.pods_cidr, 0, 0) == var.pods_cidr, false) && can(regex("^[0-9.]+/[0-9]+$", var.pods_cidr))
    error_message = "pods_cidr must be an IPv4 network in CIDR notation with no host bits set, e.g. 10.20.0.0/16."
  }
}

variable "services_cidr" {
  description = "Secondary range for Kubernetes Service IPs."
  type        = string
  default     = "10.30.0.0/20"

  validation {
    condition     = can(cidrsubnet(var.services_cidr, 0, 0)) && try(cidrsubnet(var.services_cidr, 0, 0) == var.services_cidr, false) && can(regex("^[0-9.]+/[0-9]+$", var.services_cidr))
    error_message = "services_cidr must be an IPv4 network in CIDR notation with no host bits set, e.g. 10.30.0.0/20."
  }
}

variable "sql_private_services_cidr" {
  description = "Range reserved for private services access (the Cloud SQL private IP comes from here). Must not overlap the other ranges."
  type        = string
  default     = "10.40.0.0/20"

  validation {
    condition = (
      can(cidrsubnet(var.sql_private_services_cidr, 0, 0)) &&
      try(cidrsubnet(var.sql_private_services_cidr, 0, 0) == var.sql_private_services_cidr, false) &&
      can(regex("^[0-9.]+/([0-9]+)$", var.sql_private_services_cidr)) &&
      try(tonumber(split("/", var.sql_private_services_cidr)[1]) <= 24, false)
    )
    error_message = "sql_private_services_cidr must be an IPv4 network with no host bits set and a prefix of /24 or larger, e.g. 10.40.0.0/20."
  }
}

variable "enable_flow_logs" {
  description = "Record VPC flow logs for the node subnet (sampled at 50%, 10-minute aggregation)."
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# GKE
# ---------------------------------------------------------------------------

variable "gke_release_channel" {
  description = "GKE release channel: RAPID, REGULAR, STABLE or EXTENDED (EXTENDED adds a fee once a version leaves standard support)."
  type        = string
  default     = "REGULAR"

  validation {
    condition     = contains(["RAPID", "REGULAR", "STABLE", "EXTENDED"], var.gke_release_channel)
    error_message = "gke_release_channel must be RAPID, REGULAR, STABLE or EXTENDED."
  }
}

variable "control_plane_authorized_cidrs" {
  description = <<-EOT
    IPv4 CIDRs allowed to reach the control plane's public IP endpoint. Empty (the
    default) blocks the public IP endpoint entirely; tofu, kubectl and gcloud then use
    the DNS-based endpoint, which accepts only requests carrying valid Google IAM
    credentials. Set this only for tools that cannot use the DNS endpoint.
  EOT
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for c in var.control_plane_authorized_cidrs : can(cidrhost(c, 0)) && can(regex("^[0-9.]+/[0-9]+$", c))])
    error_message = "control_plane_authorized_cidrs must contain IPv4 CIDRs such as 203.0.113.7/32."
  }
}

variable "spot" {
  description = "Run igloo pods on Spot capacity (roughly 60-70% cheaper; pods can be preempted with 25 seconds' notice, which cancels their running queries)."
  type        = bool
  default     = false
}

variable "node_selector" {
  description = "Extra nodeSelector labels for igloo pods, e.g. { \"cloud.google.com/compute-class\" = \"Balanced\" } to pick an Autopilot compute class."
  type        = map(string)
  default     = {}
}

# ---------------------------------------------------------------------------
# Igloo
# ---------------------------------------------------------------------------

variable "install_chart" {
  description = "Install the Igloo Helm chart from ../helm/igloo. Set to false to create only the infrastructure."
  type        = bool
  default     = true
}

variable "namespace" {
  description = "Kubernetes namespace for Igloo."
  type        = string
  default     = "igloo"

  validation {
    condition     = can(regex("^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$", var.namespace))
    error_message = "namespace must be a DNS-1123 label: lowercase letters, digits and '-', at most 63 characters."
  }
}

variable "image_tag" {
  description = "Igloo image tag. Empty uses the chart's appVersion."
  type        = string
  default     = ""

  validation {
    condition     = var.image_tag == "" || can(regex("^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$", var.image_tag))
    error_message = "image_tag must be empty or a valid container image tag."
  }
}

variable "admin_user" {
  description = "Name of the first Igloo administrator. Its password is generated and stored in a Kubernetes Secret."
  type        = string
  default     = "igloo"

  validation {
    condition     = can(regex("^[a-z_][a-z0-9_]{0,62}$", var.admin_user))
    error_message = "admin_user must be a lowercase SQL identifier (letters, digits, '_'), at most 63 characters."
  }
}

variable "replica_count" {
  description = "Number of igloo pods. Each one is a full query engine; clients are spread across them."
  type        = number
  default     = 2

  validation {
    condition     = var.replica_count >= 1 && var.replica_count <= 50 && floor(var.replica_count) == var.replica_count
    error_message = "replica_count must be a whole number from 1 to 50."
  }
}

variable "igloo_cpu" {
  description = "CPU request per igloo pod, in Kubernetes units (\"2\", \"500m\")."
  type        = string
  default     = "2"

  validation {
    condition     = can(regex("^([0-9]+(\\.[0-9]+)?|[0-9]+m)$", var.igloo_cpu))
    error_message = "igloo_cpu must be a Kubernetes CPU quantity such as \"2\", \"1.5\" or \"500m\"."
  }
}

variable "igloo_memory" {
  description = "Memory request and limit per igloo pod (\"8Gi\"). DuckDB uses about 80% of it unless duckdb_memory_limit is set. On Autopilot keep memory between 1 and 6.5 GiB per vCPU, or Autopilot raises the CPU request."
  type        = string
  default     = "8Gi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi)$", var.igloo_memory))
    error_message = "igloo_memory must be a quantity in Mi or Gi, e.g. \"8Gi\"."
  }
}

variable "spill_size" {
  description = "Local disk per igloo pod for queries larger than memory. Requested as ephemeral storage, which Autopilot provisions per pod (122 GiB at most on the general-purpose class)."
  type        = string
  default     = "50Gi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi)$", var.spill_size))
    error_message = "spill_size must be a quantity in Mi or Gi, e.g. \"50Gi\"."
  }
}

variable "duckdb_memory_limit" {
  description = "DuckDB memory limit per pod, e.g. \"6GB\". Empty lets the chart use about 80% of igloo_memory."
  type        = string
  default     = ""

  validation {
    condition     = var.duckdb_memory_limit == "" || can(regex("^[0-9]+(\\.[0-9]+)?(KB|MB|GB|TB|KiB|MiB|GiB|TiB)$", var.duckdb_memory_limit))
    error_message = "duckdb_memory_limit must be empty or a size such as \"6GB\" or \"6GiB\"."
  }
}

variable "catalog_replica_count" {
  description = "Number of Lakekeeper catalog pods. The catalog is stateless; use 2 for availability during node upgrades."
  type        = number
  default     = 1

  validation {
    condition     = var.catalog_replica_count >= 1 && var.catalog_replica_count <= 10 && floor(var.catalog_replica_count) == var.catalog_replica_count
    error_message = "catalog_replica_count must be a whole number from 1 to 10."
  }
}

# ---------------------------------------------------------------------------
# SQL endpoint exposure
# ---------------------------------------------------------------------------

variable "expose_sql" {
  description = "Expose the SQL port (5432) through an external TCP load balancer. When false (the default) Igloo is reachable only inside the cluster; connect with `kubectl port-forward`."
  type        = bool
  default     = false
}

variable "sql_allowed_cidrs" {
  description = "Client CIDRs allowed through the load balancer when expose_sql is true. Required in that case; there is deliberately no default."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for c in var.sql_allowed_cidrs : can(cidrhost(c, 0))])
    error_message = "sql_allowed_cidrs must contain CIDRs such as 203.0.113.0/24."
  }

  validation {
    condition     = !var.expose_sql || length(var.sql_allowed_cidrs) > 0
    error_message = "sql_allowed_cidrs must list at least one client CIDR when expose_sql is true."
  }
}

variable "tls_certificate" {
  description = "PEM certificate (chain) for the SQL endpoint. Strongly recommended with expose_sql. Leave null to serve SQL without TLS."
  type        = string
  default     = null
  sensitive   = true

  validation {
    condition     = var.tls_certificate == null || can(regex("-----BEGIN CERTIFICATE-----", var.tls_certificate))
    error_message = "tls_certificate must be a PEM-encoded certificate."
  }
}

variable "tls_private_key" {
  description = "PEM private key matching tls_certificate."
  type        = string
  default     = null
  sensitive   = true

  validation {
    condition     = var.tls_private_key == null || can(regex("-----BEGIN [A-Z ]*PRIVATE KEY-----", var.tls_private_key))
    error_message = "tls_private_key must be a PEM-encoded private key."
  }

  validation {
    condition     = (var.tls_private_key == null) == (var.tls_certificate == null)
    error_message = "Set tls_certificate and tls_private_key together, or neither."
  }
}

# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

variable "bucket_name" {
  description = "Name of the GCS bucket for table data. Null generates \"<name>-warehouse-<random>\"."
  type        = string
  default     = null

  validation {
    condition     = var.bucket_name == null || can(regex("^[a-z0-9][a-z0-9_-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "bucket_name must be 3-63 characters of lowercase letters, digits, '-' and '_' (no dots), starting and ending with a letter or digit."
  }
}

variable "bucket_location" {
  description = "Bucket location. Null uses the region, which keeps traffic between Igloo and its data free and fast."
  type        = string
  default     = null
}

variable "bucket_versioning" {
  description = "Keep previous versions of overwritten or deleted objects. Iceberg already keeps old snapshots, so this mainly guards against deleting data outside Igloo."
  type        = bool
  default     = false
}

variable "bucket_noncurrent_version_retention_days" {
  description = "With bucket_versioning, delete previous object versions after this many days."
  type        = number
  default     = 30

  validation {
    condition     = var.bucket_noncurrent_version_retention_days >= 1 && floor(var.bucket_noncurrent_version_retention_days) == var.bucket_noncurrent_version_retention_days
    error_message = "bucket_noncurrent_version_retention_days must be a whole number of at least 1."
  }
}

variable "bucket_soft_delete_days" {
  description = "Keep deleted or overwritten objects recoverable for this many days (GCS soft delete). 0 turns it off; otherwise 7-90. Soft-deleted data is billed as storage."
  type        = number
  default     = 7

  validation {
    condition     = var.bucket_soft_delete_days == 0 || (var.bucket_soft_delete_days >= 7 && var.bucket_soft_delete_days <= 90 && floor(var.bucket_soft_delete_days) == var.bucket_soft_delete_days)
    error_message = "bucket_soft_delete_days must be 0 or a whole number from 7 to 90."
  }
}

variable "bucket_force_destroy" {
  description = "Let `tofu destroy` delete the bucket even if it still holds table data. Leave false unless you mean to throw the data away."
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# Cloud SQL (catalog and user metadata)
# ---------------------------------------------------------------------------

variable "db_tier" {
  description = "Cloud SQL machine tier (Enterprise edition). The catalog's load is light; db-custom-1-3840 is 1 vCPU and 3.75 GB."
  type        = string
  default     = "db-custom-1-3840"

  validation {
    condition     = can(regex("^db-(f1-micro|g1-small|custom-[0-9]+-[0-9]+|n1-(standard|highmem)-[0-9]+)$", var.db_tier))
    error_message = "db_tier must be a Cloud SQL Enterprise edition tier such as db-custom-1-3840 or db-g1-small."
  }
}

variable "db_disk_size_gb" {
  description = "Initial Cloud SQL disk size in GB. The disk grows automatically."
  type        = number
  default     = 10

  validation {
    condition     = var.db_disk_size_gb >= 10 && floor(var.db_disk_size_gb) == var.db_disk_size_gb
    error_message = "db_disk_size_gb must be a whole number of at least 10."
  }
}

variable "db_high_availability" {
  description = "Run Cloud SQL with a standby in a second zone (REGIONAL availability). Doubles the database cost."
  type        = bool
  default     = false
}

variable "db_backup_retention_count" {
  description = "Number of daily Cloud SQL backups to keep. Point-in-time recovery covers up to the last 7 days."
  type        = number
  default     = 7

  validation {
    condition     = var.db_backup_retention_count >= 1 && var.db_backup_retention_count <= 365 && floor(var.db_backup_retention_count) == var.db_backup_retention_count
    error_message = "db_backup_retention_count must be a whole number from 1 to 365."
  }
}
