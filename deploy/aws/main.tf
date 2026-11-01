locals {
  cluster_name = var.name_prefix

  tags = merge(
    {
      Application = "igloo"
      Deployment  = var.name_prefix
      ManagedBy   = "opentofu"
    },
    var.tags,
  )

  # Graviton families have a "g" after the generation digit: r7g, m7gd, c7gn, t4g.
  node_arm64 = can(regex("^[a-z]+[0-9]+[a-z]*g[a-z]*\\.", var.node_instance_types[0]))

  # Types with local NVMe instance store (r7gd, m7gd, c7gd, i4i, ...). On
  # them nodeadm puts the disks in a RAID0 array and moves kubelet's and
  # containerd's directories onto it, so pods' emptyDir volumes, DuckDB's
  # spill directory among them, are on local NVMe rather than the root EBS
  # volume. Nodes of a listed type without instance store skip it.
  node_instance_store = anytrue([for t in data.aws_ec2_instance_type.node : t.instance_storage_supported])

  node_group = {
    ami_type       = local.node_arm64 ? "AL2023_ARM_64_STANDARD" : "AL2023_x86_64_STANDARD"
    instance_types = var.node_instance_types
    capacity_type  = var.node_use_spot ? "SPOT" : "ON_DEMAND"
    min_size       = var.node_min_size
    max_size       = var.node_max_size
    desired_size   = var.node_desired_size
    disk_size_gib  = var.node_disk_size_gib
    # nodeadm's spec.instance.localStorage.strategy, or "none".
    local_storage = local.node_instance_store ? "RAID0" : "none"
  }

  # Up to three availability zones. EKS needs at least two; so does RDS.
  azs = slice(data.aws_availability_zones.available.names, 0, min(3, length(data.aws_availability_zones.available.names)))

  bucket_name        = var.bucket_name != "" ? var.bucket_name : "${var.name_prefix}-warehouse-${random_id.suffix.hex}"
  warehouse_prefix   = "warehouse"
  warehouse_location = "s3://${local.bucket_name}/${local.warehouse_prefix}"

  # Kubernetes object names. The chart's service account names are set
  # explicitly so the IAM trust policies below can name them.
  igloo_service_account   = "igloo"
  catalog_service_account = "igloo-catalog"
  worker_service_account  = "igloo-worker"
  release_name            = "igloo"
  sql_service_name        = local.release_name

  tls_enabled     = var.expose_sql || var.tls_certificate_pem != ""
  tls_self_signed = local.tls_enabled && var.tls_certificate_pem == ""
}

data "aws_availability_zones" "available" {
  state = "available"

  # Local Zones and Wavelength Zones cannot host EKS nodes or RDS.
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }

  # Zones where EKS cannot place a control plane.
  exclude_zone_ids = ["use1-az3", "usw1-az2", "cac1-az3"]
}

data "aws_partition" "current" {}

data "aws_ec2_instance_type" "node" {
  for_each = toset(var.node_instance_types)

  instance_type = each.value
}

resource "random_id" "suffix" {
  byte_length = 4
}

################################################################################
# VPC: public subnets for the NAT gateway and any load balancer, private
# subnets for nodes, isolated database subnets for RDS.
################################################################################

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 6.7"

  name = var.name_prefix
  cidr = var.vpc_cidr
  azs  = local.azs

  private_subnets  = [for i in range(length(local.azs)) : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets   = [for i in range(length(local.azs)) : cidrsubnet(var.vpc_cidr, 8, i + 48)]
  database_subnets = [for i in range(length(local.azs)) : cidrsubnet(var.vpc_cidr, 8, i + 52)]

  create_database_subnet_group = true

  enable_nat_gateway     = true
  single_nat_gateway     = var.single_nat_gateway
  one_nat_gateway_per_az = !var.single_nat_gateway

  enable_dns_hostnames = true
  enable_dns_support   = true

  # Subnet discovery for Kubernetes Services of type LoadBalancer.
  public_subnet_tags = {
    "kubernetes.io/role/elb"                      = "1"
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"             = "1"
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
  }

  enable_flow_log                                 = var.enable_vpc_flow_logs
  create_flow_log_cloudwatch_log_group            = var.enable_vpc_flow_logs
  create_flow_log_cloudwatch_iam_role             = var.enable_vpc_flow_logs
  flow_log_cloudwatch_log_group_retention_in_days = 30

  tags = local.tags
}

# S3 traffic from the nodes goes through this free gateway endpoint instead
# of the NAT gateway, which would charge for every GB of table data read.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids

  tags = { Name = "${var.name_prefix}-s3" }

  lifecycle {
    precondition {
      condition     = length(local.azs) >= 2
      error_message = "The region must offer at least two availability zones that support EKS; EKS and RDS both need two."
    }
  }
}

################################################################################
# EKS cluster with one managed node group
################################################################################

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.29"

  name               = local.cluster_name
  kubernetes_version = var.kubernetes_version
  upgrade_policy     = { support_type = var.eks_support_type }

  # API access only through EKS access entries. The identity running
  # `tofu apply` becomes cluster admin so the Kubernetes and Helm providers
  # can create the namespace, secrets and release.
  authentication_mode                      = "API"
  enable_cluster_creator_admin_permissions = true
  access_entries = {
    for arn in var.cluster_admin_arns : arn => {
      principal_arn = arn
      policy_associations = {
        admin = {
          policy_arn   = "arn:${data.aws_partition.current.partition}:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
          access_scope = { type = "cluster" }
        }
      }
    }
  }

  endpoint_public_access       = true
  endpoint_public_access_cidrs = var.cluster_endpoint_public_access_cidrs
  endpoint_private_access      = true

  # IAM roles for service accounts (IRSA): creates the cluster's OIDC provider.
  enable_irsa = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  addons = {
    vpc-cni = {
      before_compute       = true
      configuration_values = jsonencode({ enableNetworkPolicy = tostring(var.enable_network_policy) })
    }
    kube-proxy = {}
    coredns    = {}
  }

  # The default node group, plus the worker node groups of workers.tf.
  eks_managed_node_groups = merge({
    default = {
      ami_type       = local.node_group.ami_type
      instance_types = local.node_group.instance_types
      capacity_type  = local.node_group.capacity_type

      min_size     = local.node_group.min_size
      max_size     = local.node_group.max_size
      desired_size = local.node_group.desired_size

      # Merged with the NodeConfig EKS supplies. Without instance store
      # there is no user data, as before.
      cloudinit_pre_nodeadm = local.node_group.local_storage == "RAID0" ? [{
        content_type = "application/node.eks.aws"
        content      = <<-EOT
          ---
          apiVersion: node.eks.aws/v1alpha1
          kind: NodeConfig
          spec:
            instance:
              localStorage:
                strategy: RAID0
        EOT
      }] : null

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = local.node_group.disk_size_gib
            volume_type           = "gp3"
            encrypted             = true
            delete_on_termination = true
          }
        }
      }
    }
    }, {
    for key, g in local.worker_node_groups : key => {
      ami_type       = g.ami_type
      instance_types = g.instance_types
      capacity_type  = g.capacity_type

      # From zero: the cluster autoscaler adds nodes for waiting worker pods.
      min_size     = g.min_size
      max_size     = g.max_size
      desired_size = 0

      # Only worker pods (or only the large worker) run here.
      labels = g.labels
      taints = {
        worker = { key = g.taint_key, value = "true", effect = "NO_SCHEDULE" }
      }

      cloudinit_pre_nodeadm = g.local_storage == "RAID0" ? [{
        content_type = "application/node.eks.aws"
        content      = <<-EOT
          ---
          apiVersion: node.eks.aws/v1alpha1
          kind: NodeConfig
          spec:
            instance:
              localStorage:
                strategy: RAID0
        EOT
      }] : null

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.workers_disk_size_gib
            volume_type           = "gp3"
            encrypted             = true
            delete_on_termination = true
          }
        }
      }
    }
  })

  tags = local.tags
}
