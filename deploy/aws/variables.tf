################################################################################
# General
################################################################################

variable "name_prefix" {
  description = "Prefix for every resource name (VPC, EKS cluster, RDS instance, IAM roles, bucket). 2-20 characters: lowercase letters, digits and hyphens, starting with a letter."
  type        = string
  default     = "igloo"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,18}[a-z0-9]$", var.name_prefix)) && !strcontains(var.name_prefix, "--")
    error_message = "name_prefix must be 2-20 characters of lowercase letters, digits and single hyphens, start with a letter and not end with a hyphen."
  }
}

variable "region" {
  description = "AWS region to deploy into, e.g. us-east-1."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = can(regex("^[a-z]{2}(-gov|-iso[a-z]*)?-[a-z]+-[0-9]+$", var.region))
    error_message = "region must be an AWS region name such as us-east-1 or eu-west-2."
  }
}

variable "tags" {
  description = "Extra tags applied to every AWS resource this module creates."
  type        = map(string)
  default     = {}
}

################################################################################
# Network
################################################################################

variable "vpc_cidr" {
  description = "IPv4 CIDR for the new VPC. Must be a /16 to /20. Private (node) subnets get 1/16 of it each; public and database subnets get 1/256 each."
  type        = string
  default     = "10.0.0.0/16"

  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0)) && can(regex("^[0-9.]+/(1[6-9]|20)$", var.vpc_cidr))
    error_message = "vpc_cidr must be an IPv4 CIDR with a prefix length between /16 and /20."
  }
}

variable "single_nat_gateway" {
  description = "Use one NAT gateway for the whole VPC (cheaper; outbound internet stops if that AZ fails). Set false for one NAT gateway per availability zone."
  type        = bool
  default     = true
}

variable "enable_vpc_flow_logs" {
  description = "Send VPC flow logs to CloudWatch Logs. Off by default because ingestion is billed per GB."
  type        = bool
  default     = false
}

################################################################################
# EKS
################################################################################

variable "kubernetes_version" {
  description = "EKS Kubernetes minor version, e.g. 1.35."
  type        = string
  default     = "1.35"

  validation {
    condition     = can(regex("^1\\.[0-9]{2}$", var.kubernetes_version))
    error_message = "kubernetes_version must look like 1.35."
  }
}

variable "eks_support_type" {
  description = "EKS upgrade policy. STANDARD auto-upgrades the control plane when standard support for its version ends; EXTENDED keeps the version for up to 12 more months at an extra $0.50 per cluster-hour."
  type        = string
  default     = "STANDARD"

  validation {
    condition     = contains(["STANDARD", "EXTENDED"], var.eks_support_type)
    error_message = "eks_support_type must be STANDARD or EXTENDED."
  }
}

variable "cluster_endpoint_public_access_cidrs" {
  description = "CIDRs allowed to reach the public Kubernetes API endpoint. OpenTofu, kubectl and helm on your machine use it. Requests still need valid AWS IAM credentials; narrow this to your network where you can."
  type        = list(string)
  default     = ["0.0.0.0/0"]

  validation {
    condition     = length(var.cluster_endpoint_public_access_cidrs) > 0 && alltrue([for c in var.cluster_endpoint_public_access_cidrs : can(cidrhost(c, 0))])
    error_message = "cluster_endpoint_public_access_cidrs must be a non-empty list of valid CIDRs."
  }
}

variable "cluster_admin_arns" {
  description = "IAM role or user ARNs (besides the identity running OpenTofu, which is added automatically) to grant Kubernetes cluster-admin, e.g. teammates' SSO roles or a CI role."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for a in var.cluster_admin_arns : can(regex("^arn:aws[a-z-]*:iam::[0-9]{12}:(role|user)/.+$", a))])
    error_message = "cluster_admin_arns must be IAM role or user ARNs (arn:aws:iam::<account>:role/<name>), not assumed-role session ARNs."
  }
}

variable "node_instance_types" {
  description = "EC2 instance types for the node group. All must share one CPU architecture; Graviton (arm64) types such as r7gd.xlarge select the arm64 AMI automatically. Types with local NVMe instance store (the \"d\" types such as r7gd, m7gd, c7gd) get it RAID0ed under kubelet, so DuckDB spills to it instead of the root EBS volume. On-demand nodes use the first type; Spot nodes may use any of them, so list several when node_use_spot is true."
  type        = list(string)
  default     = ["r7gd.xlarge"]

  validation {
    condition     = length(var.node_instance_types) > 0 && alltrue([for t in var.node_instance_types : can(regex("^[a-z][a-z0-9-]*\\.[a-z0-9]+$", t))])
    error_message = "node_instance_types must be a non-empty list of instance type names such as r7g.xlarge."
  }

  validation {
    condition     = length(distinct([for t in var.node_instance_types : can(regex("^[a-z]+[0-9]+[a-z]*g[a-z]*\\.", t))])) == 1
    error_message = "node_instance_types must all be Graviton (arm64, e.g. r7g, m7g) or all x86_64 (e.g. r7i, m7a); a node group runs one AMI."
  }
}

variable "node_use_spot" {
  description = "Run nodes on Spot capacity (typically 60-70% cheaper; nodes can be reclaimed with two minutes' notice, which ends queries running on them)."
  type        = bool
  default     = false
}

variable "node_min_size" {
  description = "Minimum number of nodes."
  type        = number
  default     = 2

  validation {
    condition     = var.node_min_size >= 1 && floor(var.node_min_size) == var.node_min_size
    error_message = "node_min_size must be a whole number of at least 1."
  }
}

variable "node_max_size" {
  description = "Maximum number of nodes. No cluster autoscaler is installed, so this is an upper bound for manual scaling."
  type        = number
  default     = 4

  validation {
    condition     = var.node_max_size >= var.node_min_size && floor(var.node_max_size) == var.node_max_size
    error_message = "node_max_size must be a whole number no smaller than node_min_size."
  }
}

variable "node_desired_size" {
  description = "Number of nodes at creation. EKS ignores later changes to this value; change node_min_size or scale the node group directly instead."
  type        = number
  default     = 2

  validation {
    condition     = var.node_desired_size >= var.node_min_size && var.node_desired_size <= var.node_max_size && floor(var.node_desired_size) == var.node_desired_size
    error_message = "node_desired_size must be a whole number between node_min_size and node_max_size."
  }
}

variable "node_disk_size_gib" {
  description = "Root EBS volume (gp3, encrypted) per node in GiB. On types without instance store, DuckDB spills large queries to it, so leave room for duckdb_spill_size times the igloo pods per node. On types with instance store, pods and images live on the instance store and this volume holds only the operating system, so 30 is enough."
  type        = number
  default     = 150

  validation {
    condition     = var.node_disk_size_gib >= 30 && var.node_disk_size_gib <= 16384
    error_message = "node_disk_size_gib must be between 30 and 16384."
  }
}

variable "enable_network_policy" {
  description = "Turn on Kubernetes NetworkPolicy enforcement in the Amazon VPC CNI, so the chart's policies (only igloo pods may reach the catalog) take effect."
  type        = bool
  default     = true
}

################################################################################
# Object storage
################################################################################

variable "bucket_name" {
  description = "Name of the S3 bucket for table data. Empty generates <name_prefix>-warehouse-<random>. The bucket is always created by this module."
  type        = string
  default     = ""

  validation {
    condition = var.bucket_name == "" || (
      can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_name)) &&
      !startswith(var.bucket_name, "xn--") &&
      !startswith(var.bucket_name, "sthree-") &&
      !endswith(var.bucket_name, "-s3alias") &&
      !endswith(var.bucket_name, "--ol-s3") &&
      !endswith(var.bucket_name, "--x-s3")
    )
    error_message = "bucket_name must be empty or a valid S3 bucket name of 3-63 lowercase letters, digits and hyphens (no dots, which break virtual-hosted TLS)."
  }
}

variable "bucket_versioning" {
  description = "Enable S3 versioning on the table bucket. Iceberg keeps its own snapshots, so this mainly protects against accidental deletes, at the cost of storing old object versions."
  type        = bool
  default     = false
}

variable "bucket_noncurrent_version_retention_days" {
  description = "With bucket_versioning, delete non-current object versions after this many days."
  type        = number
  default     = 30

  validation {
    condition     = var.bucket_noncurrent_version_retention_days >= 1
    error_message = "bucket_noncurrent_version_retention_days must be at least 1."
  }
}

variable "bucket_force_destroy" {
  description = "Let `tofu destroy` delete the bucket even when it still holds table data. Leave false unless you mean to delete every table."
  type        = bool
  default     = false
}

variable "bucket_kms_key_arn" {
  description = "ARN of a customer-managed KMS key for bucket encryption. Empty uses S3-managed keys (SSE-S3). When set, both service roles are granted use of this key."
  type        = string
  default     = ""

  validation {
    condition     = var.bucket_kms_key_arn == "" || can(regex("^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key/.+$", var.bucket_kms_key_arn))
    error_message = "bucket_kms_key_arn must be empty or a KMS key ARN (arn:aws:kms:<region>:<account>:key/<id>)."
  }
}

################################################################################
# Database (RDS PostgreSQL)
################################################################################

variable "db_instance_class" {
  description = "RDS instance class for the catalog and user metadata database."
  type        = string
  default     = "db.t4g.small"

  validation {
    condition     = can(regex("^db\\.[a-z0-9-]+\\.[a-z0-9]+$", var.db_instance_class))
    error_message = "db_instance_class must be an RDS instance class such as db.t4g.small."
  }
}

variable "db_allocated_storage_gib" {
  description = "Initial RDS storage in GiB (gp3). Only metadata lives here; table data is in S3."
  type        = number
  default     = 20

  validation {
    condition     = var.db_allocated_storage_gib >= 20 && var.db_allocated_storage_gib <= 65536
    error_message = "db_allocated_storage_gib must be between 20 and 65536."
  }
}

variable "db_max_allocated_storage_gib" {
  description = "Upper limit for RDS storage autoscaling in GiB. 0 disables autoscaling."
  type        = number
  default     = 100

  validation {
    condition     = var.db_max_allocated_storage_gib == 0 || var.db_max_allocated_storage_gib >= var.db_allocated_storage_gib
    error_message = "db_max_allocated_storage_gib must be 0 or at least db_allocated_storage_gib."
  }
}

variable "db_multi_az" {
  description = "Run RDS with a standby in a second availability zone (roughly doubles database cost)."
  type        = bool
  default     = false
}

variable "db_backup_retention_days" {
  description = "Days of automated RDS backups to keep (0-35). 0 turns automated backups off."
  type        = number
  default     = 7

  validation {
    condition     = var.db_backup_retention_days >= 0 && var.db_backup_retention_days <= 35
    error_message = "db_backup_retention_days must be between 0 and 35."
  }
}

variable "db_deletion_protection" {
  description = "Block deletion of the RDS instance. Set to false and apply before `tofu destroy`."
  type        = bool
  default     = true
}

variable "db_skip_final_snapshot" {
  description = "Skip the final RDS snapshot when the database is destroyed."
  type        = bool
  default     = false
}

################################################################################
# Igloo
################################################################################

variable "namespace" {
  description = "Kubernetes namespace for Igloo and its catalog."
  type        = string
  default     = "igloo"

  validation {
    condition     = can(regex("^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$", var.namespace))
    error_message = "namespace must be a valid Kubernetes namespace name (lowercase letters, digits and hyphens, at most 63 characters)."
  }
}

variable "install_chart" {
  description = "Install the Igloo Helm chart from deploy/helm/igloo. Set false to create only the infrastructure, namespace and secrets, and install the chart some other way."
  type        = bool
  default     = true
}

variable "igloo_image_repository" {
  description = "Igloo image repository, for a mirror or private registry. Empty uses the chart's default."
  type        = string
  default     = ""
}

variable "igloo_image_tag" {
  description = "Igloo image tag. Empty uses the chart's appVersion."
  type        = string
  default     = ""

  validation {
    condition     = can(regex("^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$", var.igloo_image_tag)) || var.igloo_image_tag == ""
    error_message = "igloo_image_tag must be empty or a valid image tag."
  }
}

variable "igloo_replicas" {
  description = "Number of igloo pods. Each is a complete query engine; clients are spread across them."
  type        = number
  default     = 2

  validation {
    condition     = var.igloo_replicas >= 1 && floor(var.igloo_replicas) == var.igloo_replicas
    error_message = "igloo_replicas must be a whole number of at least 1."
  }
}

variable "igloo_cpu_request" {
  description = "CPU request per igloo pod. There is no CPU limit, so pods use idle cores on their node. The default fits two pods, plus room for a rolling update, on two r7gd.xlarge nodes."
  type        = string
  default     = "1500m"

  validation {
    condition     = can(regex("^([0-9]+m|[0-9]+(\\.[0-9]+)?)$", var.igloo_cpu_request))
    error_message = "igloo_cpu_request must be a Kubernetes CPU quantity such as 1500m or 2."
  }
}

variable "igloo_memory" {
  description = "Memory request and limit per igloo pod. DuckDB uses about 80% of it unless duckdb_memory_limit is set."
  type        = string
  default     = "12Gi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi)$", var.igloo_memory))
    error_message = "igloo_memory must be a quantity in Mi or Gi, such as 12Gi."
  }
}

variable "duckdb_memory_limit" {
  description = "DuckDB memory limit per pod, e.g. 10GB. Empty lets the chart use about 80% of igloo_memory."
  type        = string
  default     = ""

  validation {
    condition     = var.duckdb_memory_limit == "" || can(regex("^[0-9]+(\\.[0-9]+)?(KB|MB|GB|TB|KiB|MiB|GiB|TiB)$", var.duckdb_memory_limit))
    error_message = "duckdb_memory_limit must be empty or a size such as 10GB."
  }
}

variable "duckdb_threads" {
  description = "DuckDB worker threads per pod. Null uses two per CPU core on the node."
  type        = number
  default     = null

  validation {
    condition     = try(var.duckdb_threads >= 1 && floor(var.duckdb_threads) == var.duckdb_threads, var.duckdb_threads == null)
    error_message = "duckdb_threads must be null or a whole number of at least 1."
  }
}

variable "duckdb_spill_size" {
  description = "Size limit of each igloo pod's spill directory, an emptyDir: on the node's instance store with types that have one, else on its root volume. The igloo pods on a node share that disk (237 GB of NVMe on r7gd.xlarge)."
  type        = string
  default     = "50Gi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi|Ti)$", var.duckdb_spill_size))
    error_message = "duckdb_spill_size must be a quantity such as 50Gi."
  }
}

variable "admin_user" {
  description = "Name of the first Igloo administrator. Its password is generated and stored in a Kubernetes Secret."
  type        = string
  default     = "igloo"

  validation {
    condition     = can(regex("^[a-z_][a-z0-9_]{0,62}$", var.admin_user))
    error_message = "admin_user must be 1-63 lowercase letters, digits or underscores, not starting with a digit."
  }
}

variable "allowed_locations" {
  description = "Extra S3 prefixes users may read and write besides the warehouse, e.g. [\"s3://landing-bucket/exports\"] for COPY. The igloo role must also be granted access to them; see igloo_additional_policy_arns."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for l in var.allowed_locations : startswith(l, "s3://")])
    error_message = "allowed_locations entries must start with s3://."
  }
}

variable "igloo_additional_policy_arns" {
  description = "IAM policy ARNs to attach to the igloo pods' role, e.g. read access to the buckets in allowed_locations."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for a in var.igloo_additional_policy_arns : can(regex("^arn:aws[a-z-]*:iam::([0-9]{12}|aws):policy/.+$", a))])
    error_message = "igloo_additional_policy_arns must be IAM policy ARNs."
  }
}

variable "expose_sql" {
  description = "Expose the SQL port outside the cluster through an AWS Network Load Balancer. When false, connect with kubectl port-forward."
  type        = bool
  default     = false
}

variable "sql_allowed_cidrs" {
  description = "Client CIDRs allowed through the load balancer. Required when expose_sql is true; there is deliberately no default."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for c in var.sql_allowed_cidrs : can(cidrhost(c, 0))])
    error_message = "sql_allowed_cidrs must contain valid CIDRs such as 203.0.113.0/24."
  }

  validation {
    condition     = !var.expose_sql || length(var.sql_allowed_cidrs) > 0
    error_message = "sql_allowed_cidrs must list at least one CIDR when expose_sql is true."
  }
}

variable "sql_load_balancer_internal" {
  description = "With expose_sql, create an internal load balancer reachable only from the VPC and networks connected to it (VPN, peering, Transit Gateway)."
  type        = bool
  default     = false
}

variable "tls_certificate_pem" {
  description = "PEM certificate (chain) for SQL client TLS. Empty with expose_sql generates a self-signed certificate, which encrypts traffic for clients using sslmode=require but cannot be verified."
  type        = string
  default     = ""
}

variable "tls_private_key_pem" {
  description = "PEM private key for tls_certificate_pem."
  type        = string
  default     = ""
  sensitive   = true

  validation {
    condition     = (var.tls_private_key_pem == "") == (var.tls_certificate_pem == "")
    error_message = "Set both tls_certificate_pem and tls_private_key_pem, or neither."
  }
}

variable "helm_values_override" {
  description = "Extra YAML merged over the values this module sets on the chart, for chart settings without a variable here (nodeSelector, autoscaling, ...)."
  type        = string
  default     = ""

  validation {
    condition     = var.helm_values_override == "" || can(yamldecode(var.helm_values_override))
    error_message = "helm_values_override must be valid YAML."
  }
}
