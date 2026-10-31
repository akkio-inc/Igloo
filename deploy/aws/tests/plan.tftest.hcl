# Plan-only tests. Every provider is mocked, so no AWS or Kubernetes API is
# called and no credentials are needed:
#
#   tofu init -backend=false && tofu test
#
# Mocked providers still run the real providers' argument validation, so the
# computed values below are shaped like real ones (ARNs, IDs, JSON).

mock_provider "aws" {
  mock_data "aws_availability_zones" {
    defaults = {
      names    = ["us-east-1a", "us-east-1b", "us-east-1c"]
      zone_ids = ["use1-az1", "use1-az2", "use1-az4"]
    }
  }
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:user/test"
      user_id    = "AIDATEST"
    }
  }
  mock_data "aws_iam_session_context" {
    defaults = {
      issuer_arn = "arn:aws:iam::123456789012:user/test"
    }
  }
  mock_data "aws_partition" {
    defaults = {
      partition  = "aws"
      dns_suffix = "amazonaws.com"
    }
  }
  # Each run that looks at node local storage overrides this.
  mock_data "aws_ec2_instance_type" {
    defaults = {
      instance_storage_supported = false
    }
  }
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::123456789012:role/mock"
    }
  }
  mock_resource "aws_iam_policy" {
    defaults = {
      arn = "arn:aws:iam::123456789012:policy/mock"
    }
  }
  mock_resource "aws_kms_key" {
    defaults = {
      arn = "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000000"
    }
  }
  mock_resource "aws_iam_openid_connect_provider" {
    defaults = {
      arn = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE"
    }
  }
  mock_resource "aws_eks_cluster" {
    defaults = {
      arn                   = "arn:aws:eks:us-east-1:123456789012:cluster/igloo"
      endpoint              = "https://EXAMPLE.gr7.us-east-1.eks.amazonaws.com"
      certificate_authority = [{ data = "Y2VydA==" }]
      identity              = [{ oidc = [{ issuer = "https://oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE" }] }]
    }
  }
  mock_resource "aws_launch_template" {
    defaults = {
      id              = "lt-0123456789abcdef0"
      arn             = "arn:aws:ec2:us-east-1:123456789012:launch-template/lt-0123456789abcdef0"
      default_version = 1
      latest_version  = 1
    }
  }
  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::igloo-test-warehouse"
    }
  }
  mock_resource "aws_cloudwatch_log_group" {
    defaults = {
      arn = "arn:aws:logs:us-east-1:123456789012:log-group:mock"
    }
  }
  mock_resource "aws_db_instance" {
    defaults = {
      address  = "igloo-db.abcdefghijkl.us-east-1.rds.amazonaws.com"
      endpoint = "igloo-db.abcdefghijkl.us-east-1.rds.amazonaws.com:5432"
    }
  }
}

mock_provider "kubernetes" {}
mock_provider "helm" {}
mock_provider "random" {}
mock_provider "tls" {}
mock_provider "time" {}
mock_provider "cloudinit" {}
mock_provider "null" {}

variables {
  bucket_name = "igloo-test-warehouse"
}

################################################################################
# Defaults
################################################################################

run "defaults" {
  command = plan

  # r7gd.xlarge has a 237 GB NVMe instance store.
  override_data {
    target = data.aws_ec2_instance_type.node
    values = { instance_storage_supported = true }
  }

  # Distinct role ARNs, so the test can tell which role each service account gets.
  override_resource {
    target = aws_iam_role.service["igloo"]
    values = { arn = "arn:aws:iam::123456789012:role/igloo-igloo" }
  }
  override_resource {
    target = aws_iam_role.service["catalog"]
    values = { arn = "arn:aws:iam::123456789012:role/igloo-catalog" }
  }
  override_resource {
    target = aws_iam_role.engines
    values = { arn = "arn:aws:iam::123456789012:role/igloo-engines" }
  }

  # --- S3 -------------------------------------------------------------------

  assert {
    condition = (
      aws_s3_bucket_public_access_block.warehouse.block_public_acls &&
      aws_s3_bucket_public_access_block.warehouse.block_public_policy &&
      aws_s3_bucket_public_access_block.warehouse.ignore_public_acls &&
      aws_s3_bucket_public_access_block.warehouse.restrict_public_buckets
    )
    error_message = "All four S3 public access block settings must be on."
  }

  assert {
    condition     = one([for r in aws_s3_bucket_server_side_encryption_configuration.warehouse.rule : one(r.apply_server_side_encryption_by_default).sse_algorithm]) == "AES256"
    error_message = "Without a KMS key the bucket must use SSE-S3 (AES256)."
  }

  assert {
    condition = anytrue([
      for s in jsondecode(aws_s3_bucket_policy.warehouse.policy).Statement :
      s.Effect == "Deny" && s.Principal == "*" && s.Action == "s3:*" && s.Condition.Bool["aws:SecureTransport"] == "false"
    ])
    error_message = "The bucket policy must deny every request made without TLS."
  }

  assert {
    condition     = length(aws_s3_bucket_versioning.warehouse) == 0
    error_message = "Versioning must be off by default."
  }

  assert {
    condition     = aws_s3_bucket.warehouse.force_destroy == false
    error_message = "The bucket must not be force-destroyable by default."
  }

  assert {
    condition     = aws_vpc_endpoint.s3.vpc_endpoint_type == "Gateway" && aws_vpc_endpoint.s3.service_name == "com.amazonaws.us-east-1.s3"
    error_message = "S3 traffic must use a gateway endpoint, not the NAT gateway."
  }

  # --- RDS ------------------------------------------------------------------

  assert {
    condition     = aws_db_instance.this.publicly_accessible == false
    error_message = "RDS must not be publicly accessible."
  }

  assert {
    condition     = aws_db_instance.this.storage_encrypted == true
    error_message = "RDS storage must be encrypted."
  }

  assert {
    condition     = aws_db_instance.this.engine == "postgres" && aws_db_instance.this.engine_version == "17"
    error_message = "RDS must run PostgreSQL 17."
  }

  assert {
    condition     = aws_db_instance.this.deletion_protection == true && aws_db_instance.this.db_name == "igloo"
    error_message = "RDS must have deletion protection on by default and a database named igloo."
  }

  assert {
    condition = (
      aws_vpc_security_group_ingress_rule.db_from_nodes.referenced_security_group_id == module.eks.node_security_group_id &&
      aws_vpc_security_group_ingress_rule.db_from_nodes.cidr_ipv4 == null &&
      aws_vpc_security_group_ingress_rule.db_from_cluster.referenced_security_group_id == module.eks.cluster_primary_security_group_id &&
      aws_vpc_security_group_ingress_rule.db_from_cluster.cidr_ipv4 == null &&
      aws_vpc_security_group_ingress_rule.db_from_nodes.from_port == 5432
    )
    error_message = "RDS may be reached on 5432 only from the EKS node and cluster security groups."
  }

  # --- IRSA -----------------------------------------------------------------

  assert {
    condition = (
      jsondecode(aws_iam_role.service["igloo"].assume_role_policy).Statement[0].Condition.StringEquals["oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE:sub"] == "system:serviceaccount:igloo:igloo" &&
      jsondecode(aws_iam_role.service["igloo"].assume_role_policy).Statement[0].Condition.StringEquals["oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE:aud"] == "sts.amazonaws.com"
    )
    error_message = "The igloo role must be assumable only by service account igloo/igloo."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role.service["catalog"].assume_role_policy).Statement[0].Condition.StringEquals["oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE:sub"] == "system:serviceaccount:igloo:igloo-catalog" &&
      jsondecode(aws_iam_role.service["catalog"].assume_role_policy).Statement[0].Condition.StringEquals["oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE:aud"] == "sts.amazonaws.com"
    )
    error_message = "The catalog role must be assumable only by service account igloo/igloo-catalog."
  }

  assert {
    condition = alltrue([
      for k in ["igloo", "catalog"] :
      jsondecode(aws_iam_role.service[k].assume_role_policy).Statement[0].Principal.Federated == module.eks.oidc_provider_arn &&
      jsondecode(aws_iam_role.service[k].assume_role_policy).Statement[0].Action == "sts:AssumeRoleWithWebIdentity" &&
      length(jsondecode(aws_iam_role.service[k].assume_role_policy).Statement) == 1
    ])
    error_message = "Both roles must trust only the cluster's OIDC provider via AssumeRoleWithWebIdentity."
  }

  assert {
    condition = alltrue(flatten([
      for k in ["igloo", "catalog"] : [
        for s in jsondecode(aws_iam_role_policy.warehouse[k].policy).Statement :
        s.Effect == "Allow" && alltrue([
          for r in flatten([s.Resource]) :
          contains(["arn:aws:s3:::igloo-test-warehouse", "arn:aws:s3:::igloo-test-warehouse/warehouse/*"], r)
        ])
      ]
    ]))
    error_message = "Service roles may only touch the warehouse bucket and its warehouse/ prefix."
  }

  assert {
    condition = alltrue([
      for k in ["igloo", "catalog"] :
      one([for s in jsondecode(aws_iam_role_policy.warehouse[k].policy).Statement : s if s.Action == "s3:ListBucket"]).Condition.StringLike["s3:prefix"] == ["warehouse", "warehouse/*"]
    ])
    error_message = "Listing must be limited to the warehouse/ prefix."
  }

  assert {
    condition = alltrue([
      for k in ["igloo", "catalog"] :
      length(setsubtract(
        ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
        flatten([for s in jsondecode(aws_iam_role_policy.warehouse[k].policy).Statement : s.Action if s.Resource == "arn:aws:s3:::igloo-test-warehouse/warehouse/*"]),
      )) == 0
    ])
    error_message = "Service roles need get, put, delete and multipart access under warehouse/."
  }

  # --- Kubernetes secrets ---------------------------------------------------

  assert {
    condition     = kubernetes_namespace_v1.igloo.metadata[0].name == "igloo"
    error_message = "Namespace must default to igloo."
  }

  assert {
    condition     = toset(keys(kubernetes_secret_v1.admin.data)) == toset(["password"])
    error_message = "Admin secret must hold exactly the key \"password\"."
  }

  assert {
    condition = (
      toset(keys(kubernetes_secret_v1.database.data)) == toset(["url"]) &&
      startswith(nonsensitive(kubernetes_secret_v1.database.data.url), "postgresql://igloo:") &&
      endswith(nonsensitive(kubernetes_secret_v1.database.data.url), "@igloo-db.abcdefghijkl.us-east-1.rds.amazonaws.com:5432/igloo?sslmode=require")
    )
    error_message = "Database secret must hold key \"url\" in the form postgresql://igloo:<password>@<host>:5432/igloo?sslmode=require."
  }

  assert {
    condition     = toset(keys(kubernetes_secret_v1.catalog_encryption.data)) == toset(["encryption-key"])
    error_message = "Catalog encryption secret must hold exactly the key \"encryption-key\"."
  }

  assert {
    condition     = length(kubernetes_secret_v1.tls) == 0 && length(tls_self_signed_cert.sql) == 0
    error_message = "No TLS secret should be created when SQL is not exposed and no certificate is given."
  }

  # --- Helm values ----------------------------------------------------------

  assert {
    condition     = helm_release.igloo[0].chart == "./../helm/igloo" || endswith(helm_release.igloo[0].chart, "/../helm/igloo")
    error_message = "The chart must come from deploy/helm/igloo."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).storage.warehouseLocation == "s3://igloo-test-warehouse/warehouse"
    error_message = "storage.warehouseLocation must be s3://<bucket>/warehouse."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).storage.s3.region == "us-east-1" &&
      yamldecode(helm_release.igloo[0].values[0]).storage.s3.existingSecret == "" &&
      yamldecode(helm_release.igloo[0].values[0]).storage.s3.endpoint == ""
    )
    error_message = "storage.s3 must set the region and no static keys or endpoint."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).catalog.systemCredentials == "aws"
    error_message = "catalog.systemCredentials must be aws."
  }

  # --- Storage credentials for other engines --------------------------------

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).storage.s3.stsRoleArn == "arn:aws:iam::123456789012:role/igloo-engines"
    error_message = "storage.s3.stsRoleArn must be the engines role, so Igloo can vend table-scoped credentials."
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role.engines.assume_role_policy).Statement) == 1 &&
      jsondecode(aws_iam_role.engines.assume_role_policy).Statement[0].Principal.AWS == "arn:aws:iam::123456789012:role/igloo-igloo" &&
      jsondecode(aws_iam_role.engines.assume_role_policy).Statement[0].Action == "sts:AssumeRole" &&
      aws_iam_role.engines.max_session_duration == 3600
    )
    error_message = "Only the igloo pods' role may assume the engines role, for at most an hour."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.engines_warehouse.policy).Statement == jsondecode(aws_iam_role_policy.warehouse["igloo"].policy).Statement &&
      jsondecode(aws_iam_role_policy.igloo_assume_engines.policy).Statement[0].Resource == "arn:aws:iam::123456789012:role/igloo-engines"
    )
    error_message = "The engines role must reach the warehouse prefix only, and the igloo role may assume it."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).serviceAccount.annotations["eks.amazonaws.com/role-arn"] == "arn:aws:iam::123456789012:role/igloo-igloo" &&
      yamldecode(helm_release.igloo[0].values[0]).catalog.serviceAccount.annotations["eks.amazonaws.com/role-arn"] == "arn:aws:iam::123456789012:role/igloo-catalog"
    )
    error_message = "Each service account must be annotated with its own IRSA role."
  }

  assert {
    condition = (
      "system:serviceaccount:igloo:${yamldecode(helm_release.igloo[0].values[0]).serviceAccount.name}" == jsondecode(aws_iam_role.service["igloo"].assume_role_policy).Statement[0].Condition.StringEquals["oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE:sub"] &&
      "system:serviceaccount:igloo:${yamldecode(helm_release.igloo[0].values[0]).catalog.serviceAccount.name}" == jsondecode(aws_iam_role.service["catalog"].assume_role_policy).Statement[0].Condition.StringEquals["oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE:sub"]
    )
    error_message = "Service account names in the chart values must match the IAM trust policies."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).metadataDatabase.existingSecret == "igloo-database" &&
      yamldecode(helm_release.igloo[0].values[0]).catalog.database.existingSecret == "igloo-database" &&
      yamldecode(helm_release.igloo[0].values[0]).catalog.encryptionKey.existingSecret == "igloo-catalog-encryption" &&
      yamldecode(helm_release.igloo[0].values[0]).admin.existingSecret == "igloo-admin"
    )
    error_message = "The chart must read the database URL, encryption key and admin password from the module's secrets."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).service.type == "ClusterIP" &&
      length(yamldecode(helm_release.igloo[0].values[0]).service.annotations) == 0 &&
      length(yamldecode(helm_release.igloo[0].values[0]).service.loadBalancerSourceRanges) == 0
    )
    error_message = "With expose_sql=false the SQL service must be ClusterIP with no load balancer settings."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).replicaCount == 2 &&
      yamldecode(helm_release.igloo[0].values[0]).image.tag == "" &&
      yamldecode(helm_release.igloo[0].values[0]).resources.limits.memory == "12Gi" &&
      yamldecode(helm_release.igloo[0].values[0]).tls.existingSecret == "" &&
      yamldecode(helm_release.igloo[0].values[0]).postgresql.enabled == false &&
      yamldecode(helm_release.igloo[0].values[0]).objectStorage.enabled == false
    )
    error_message = "Unexpected default chart values."
  }

  assert {
    condition     = length(time_sleep.load_balancer_cleanup) == 0 && length(data.kubernetes_service_v1.sql) == 0
    error_message = "Load balancer helpers must not exist when SQL is not exposed."
  }

  # --- Nodes and outputs ----------------------------------------------------

  assert {
    condition = (
      output.node_group.ami_type == "AL2023_ARM_64_STANDARD" &&
      output.node_group.capacity_type == "ON_DEMAND" &&
      output.node_group.instance_types == tolist(["r7gd.xlarge"])
    )
    error_message = "Default nodes must be on-demand Graviton (r7gd.xlarge) on the arm64 AMI."
  }

  assert {
    condition     = output.node_group.local_storage == "RAID0" && output.node_group.disk_size_gib == 150
    error_message = "Nodes with instance store must RAID0 it under kubelet, so DuckDB spills to local NVMe."
  }

  assert {
    condition     = output.warehouse_location == "s3://igloo-test-warehouse/warehouse"
    error_message = "warehouse_location output is wrong."
  }

  assert {
    condition     = output.kubeconfig_command == "aws eks update-kubeconfig --region us-east-1 --name igloo"
    error_message = "kubeconfig_command output is wrong."
  }

  assert {
    condition     = output.sql_endpoint == null && strcontains(output.next_steps, "port-forward svc/igloo 5433:5432")
    error_message = "Without expose_sql, there is no SQL endpoint and next steps use port-forward."
  }

  # --- No workers by default ------------------------------------------------

  assert {
    condition = (
      length(output.worker_node_groups) == 0 &&
      keys(module.eks.eks_managed_node_groups) == ["default"] &&
      length(helm_release.cluster_autoscaler) == 0 &&
      length(aws_iam_role.cluster_autoscaler) == 0 &&
      !contains(keys(yamldecode(helm_release.igloo[0].values[0])), "workers")
    )
    error_message = "Without workers_enabled there must be no worker node groups, no cluster autoscaler and no workers values, so existing deployments are unchanged."
  }
}

################################################################################
# Distributed-execution workers
################################################################################

run "workers_on_spot_with_on_demand_fallback" {
  command = plan

  variables {
    workers_enabled        = true
    workers_price_per_hour = 0.35
  }

  # m7gd.4xlarge and m6gd.4xlarge: 16 vCPUs, 64 GiB, 950 GB of NVMe.
  override_data {
    target = data.aws_ec2_instance_type.worker
    values = { default_vcpus = 16, memory_size = 65536, instance_storage_supported = true, total_instance_storage = 950 }
  }
  override_resource {
    target = module.eks.module.eks_managed_node_group["workers-spot"].aws_eks_node_group.this
    values = { resources = [{ autoscaling_groups = [{ name = "eks-workers-spot-0123" }], remote_access_security_group_id = "" }] }
  }
  override_resource {
    target = module.eks.module.eks_managed_node_group["workers-ondemand"].aws_eks_node_group.this
    values = { resources = [{ autoscaling_groups = [{ name = "eks-workers-ondemand-4567" }], remote_access_security_group_id = "" }] }
  }
  override_resource {
    target = aws_iam_role.cluster_autoscaler
    values = { arn = "arn:aws:iam::123456789012:role/igloo-cluster-autoscaler" }
  }

  assert {
    condition = (
      keys(output.worker_node_groups) == ["workers-ondemand", "workers-spot"] &&
      output.worker_node_groups["workers-spot"].capacity_type == "SPOT" &&
      output.worker_node_groups["workers-ondemand"].capacity_type == "ON_DEMAND" &&
      alltrue([for g in output.worker_node_groups : g.min_size == 0 && g.max_size == 4 && g.instance_types == tolist(["m7gd.4xlarge", "m6gd.4xlarge"])])
    )
    error_message = "Workers must get a Spot node group and an on-demand one to fall back to, both from zero to workers_max nodes."
  }

  assert {
    condition = alltrue([
      for g in output.worker_node_groups :
      g.labels == { "igloo.dev/node" = "workers" } && g.taint_key == "igloo.dev/workers" &&
      g.ami_type == "AL2023_ARM_64_STANDARD" && g.local_storage == "RAID0"
    ])
    error_message = "Worker node groups must be labelled and tainted for workers, on the arm64 AMI, with their instance store RAID0ed under kubelet."
  }

  assert {
    condition     = output.node_group.capacity_type == "ON_DEMAND" && output.node_group.min_size == 2 && contains(keys(module.eks.eks_managed_node_groups), "default")
    error_message = "The default node group, where igloo pods run, must stay as it was."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).workers.enabled == true &&
      yamldecode(helm_release.igloo[0].values[0]).workers.maxReplicas == 4 &&
      yamldecode(helm_release.igloo[0].values[0]).workers.pricePerHour == 0.35 &&
      yamldecode(helm_release.igloo[0].values[0]).workers.large.enabled == false
    )
    error_message = "The chart must turn workers on, with workers_max and the workers' price, and no large worker."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).workers.resources.requests.cpu == "14" &&
      yamldecode(helm_release.igloo[0].values[0]).workers.resources.requests.memory == "54Gi" &&
      yamldecode(helm_release.igloo[0].values[0]).workers.resources.limits.memory == "54Gi" &&
      yamldecode(helm_release.igloo[0].values[0]).workers.duckdb.spillVolume.sizeLimit == "486Gi" &&
      yamldecode(helm_release.igloo[0].values[0]).workers.cacheVolume.sizeLimit == "221Gi"
    )
    error_message = "Worker pods must fill one node (90% of its vCPUs, 85% of its memory), with spill and cache sized to 55% and 25% of its instance store."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).workers.nodeSelector == { "igloo.dev/node" = "workers" } &&
      yamldecode(helm_release.igloo[0].values[0]).workers.tolerations == [{ key = "igloo.dev/workers", operator = "Equal", value = "true", effect = "NoSchedule" }]
    )
    error_message = "Worker pods must select and tolerate the worker node groups, Spot and on-demand alike."
  }

  assert {
    condition = (
      yamldecode(helm_release.cluster_autoscaler[0].values[0]).autoscalingGroups == [
        { name = "eks-workers-ondemand-4567", minSize = 0, maxSize = 4 },
        { name = "eks-workers-spot-0123", minSize = 0, maxSize = 4 },
      ] &&
      helm_release.cluster_autoscaler[0].namespace == "kube-system" &&
      helm_release.cluster_autoscaler[0].version == "9.59.0"
    )
    error_message = "The cluster autoscaler must scale the worker node groups' autoscaling groups, and only them."
  }

  assert {
    condition = (
      yamldecode(helm_release.cluster_autoscaler[0].values[0]).extraArgs.expander == "priority" &&
      yamldecode(helm_release.cluster_autoscaler[0].values[0]).extraArgs["max-node-provision-time"] == "5m" &&
      contains(yamldecode(helm_release.cluster_autoscaler[0].values[0]).expanderPriorities["50"], "^eks-workers-spot-.*") &&
      yamldecode(helm_release.cluster_autoscaler[0].values[0]).expanderPriorities["10"] == [".*"]
    )
    error_message = "The cluster autoscaler must prefer the Spot group and back it off quickly when it gets no capacity."
  }

  assert {
    condition = (
      yamldecode(helm_release.cluster_autoscaler[0].values[0]).rbac.serviceAccount.name == "cluster-autoscaler" &&
      yamldecode(helm_release.cluster_autoscaler[0].values[0]).rbac.serviceAccount.annotations["eks.amazonaws.com/role-arn"] == "arn:aws:iam::123456789012:role/igloo-cluster-autoscaler" &&
      jsondecode(aws_iam_role.cluster_autoscaler[0].assume_role_policy).Statement[0].Condition.StringEquals["oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE:sub"] == "system:serviceaccount:kube-system:cluster-autoscaler"
    )
    error_message = "The cluster autoscaler's role must be assumable only by kube-system/cluster-autoscaler, which the chart annotates with it."
  }

  assert {
    condition = (
      one([for s in jsondecode(aws_iam_role_policy.cluster_autoscaler[0].policy).Statement : s.Resource if s.Sid == "ScaleWorkerNodeGroups"]) == [
        "arn:aws:autoscaling:us-east-1:123456789012:autoScalingGroup:*:autoScalingGroupName/eks-workers-ondemand-4567",
        "arn:aws:autoscaling:us-east-1:123456789012:autoScalingGroup:*:autoScalingGroupName/eks-workers-spot-0123",
      ] &&
      contains(one([for s in jsondecode(aws_iam_role_policy.cluster_autoscaler[0].policy).Statement : s.Action if s.Sid == "Describe"]), "eks:DescribeNodegroup")
    )
    error_message = "The cluster autoscaler may resize only the worker node groups, and must read managed node groups to scale them from zero."
  }
}

run "workers_on_demand_without_instance_store" {
  command = plan

  variables {
    workers_enabled        = true
    workers_spot           = false
    workers_instance_types = ["m7i.4xlarge"]
    workers_memory         = "56Gi"
  }

  override_data {
    target = data.aws_ec2_instance_type.worker
    values = { default_vcpus = 16, memory_size = 65536, instance_storage_supported = false, total_instance_storage = 0 }
  }

  assert {
    condition = (
      keys(output.worker_node_groups) == ["workers-ondemand"] &&
      output.worker_node_groups["workers-ondemand"].ami_type == "AL2023_x86_64_STANDARD" &&
      output.worker_node_groups["workers-ondemand"].local_storage == "none"
    )
    error_message = "workers_spot=false must leave only the on-demand group; x86 types without instance store get the x86 AMI and no localStorage setting."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).workers.resources.requests.memory == "56Gi" &&
      yamldecode(helm_release.igloo[0].values[0]).workers.resources.requests.cpu == "14" &&
      yamldecode(helm_release.igloo[0].values[0]).workers.duckdb.spillVolume.sizeLimit == "50Gi" &&
      yamldecode(helm_release.igloo[0].values[0]).workers.cacheVolume.sizeLimit == "20Gi"
    )
    error_message = "workers_memory must override the derived size, and without instance store spill and cache take half and a fifth of the root volume."
  }
}

run "large_worker" {
  command = plan

  variables {
    workers_enabled             = true
    large_worker_enabled        = true
    large_worker_price_per_hour = 4.355
  }

  override_data {
    target = data.aws_ec2_instance_type.worker["m7gd.4xlarge"]
    values = { default_vcpus = 16, memory_size = 65536, instance_storage_supported = true, total_instance_storage = 950 }
  }
  override_data {
    target = data.aws_ec2_instance_type.worker["m6gd.4xlarge"]
    values = { default_vcpus = 16, memory_size = 65536, instance_storage_supported = true, total_instance_storage = 950 }
  }
  # r7gd.16xlarge: 64 vCPUs, 512 GiB, 2 x 1900 GB of NVMe.
  override_data {
    target = data.aws_ec2_instance_type.worker["r7gd.16xlarge"]
    values = { default_vcpus = 64, memory_size = 524288, instance_storage_supported = true, total_instance_storage = 3800 }
  }
  override_resource {
    target = module.eks.module.eks_managed_node_group["large-worker-ondemand"].aws_eks_node_group.this
    values = { resources = [{ autoscaling_groups = [{ name = "eks-large-worker-ondemand-89ab" }], remote_access_security_group_id = "" }] }
  }

  assert {
    condition = (
      keys(output.worker_node_groups) == ["large-worker-ondemand", "workers-ondemand", "workers-spot"] &&
      output.worker_node_groups["large-worker-ondemand"].capacity_type == "ON_DEMAND" &&
      output.worker_node_groups["large-worker-ondemand"].instance_types == tolist(["r7gd.16xlarge"]) &&
      output.worker_node_groups["large-worker-ondemand"].min_size == 0 &&
      output.worker_node_groups["large-worker-ondemand"].max_size == 1 &&
      output.worker_node_groups["large-worker-ondemand"].labels == { "igloo.dev/node" = "large-worker" } &&
      output.worker_node_groups["large-worker-ondemand"].taint_key == "igloo.dev/large-worker" &&
      output.worker_node_groups["large-worker-ondemand"].local_storage == "RAID0"
    )
    error_message = "The large worker must get an on-demand r7gd.16xlarge node group of its own, from zero to one node, labelled and tainted for it."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).workers.large.enabled == true &&
      yamldecode(helm_release.igloo[0].values[0]).workers.large.pricePerHour == 4.355 &&
      yamldecode(helm_release.igloo[0].values[0]).workers.large.resources.requests.cpu == "57" &&
      yamldecode(helm_release.igloo[0].values[0]).workers.large.resources.limits.memory == "435Gi" &&
      yamldecode(helm_release.igloo[0].values[0]).workers.large.duckdb.spillVolume.sizeLimit == "1946Gi" &&
      yamldecode(helm_release.igloo[0].values[0]).workers.large.cacheVolume.sizeLimit == "884Gi" &&
      yamldecode(helm_release.igloo[0].values[0]).workers.large.nodeSelector == { "igloo.dev/node" = "large-worker" } &&
      yamldecode(helm_release.igloo[0].values[0]).workers.large.tolerations[0].key == "igloo.dev/large-worker"
    )
    error_message = "The chart's workers.large must fill the large worker's node and select it."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).workers.resources.requests.cpu == "14" && yamldecode(helm_release.igloo[0].values[0]).workers.nodeSelector == { "igloo.dev/node" = "workers" }
    error_message = "The small workers must keep their own size and nodes."
  }

  assert {
    condition     = contains([for g in yamldecode(helm_release.cluster_autoscaler[0].values[0]).autoscalingGroups : g.name], "eks-large-worker-ondemand-89ab")
    error_message = "The cluster autoscaler must scale the large worker's node group too."
  }
}

run "large_worker_on_spot" {
  command = plan

  variables {
    workers_enabled      = true
    large_worker_enabled = true
    large_worker_spot    = true
  }

  override_data {
    target = data.aws_ec2_instance_type.worker
    values = { default_vcpus = 64, memory_size = 524288, instance_storage_supported = true, total_instance_storage = 3800 }
  }

  assert {
    condition = (
      output.worker_node_groups["large-worker-spot"].capacity_type == "SPOT" &&
      output.worker_node_groups["large-worker-ondemand"].capacity_type == "ON_DEMAND" &&
      contains(yamldecode(helm_release.cluster_autoscaler[0].values[0]).expanderPriorities["50"], "^eks-large-worker-spot-.*")
    )
    error_message = "large_worker_spot must add a Spot group the autoscaler prefers, keeping the on-demand one to fall back to."
  }
}

################################################################################
# Exposed SQL endpoint
################################################################################

run "expose_sql" {
  command = plan

  variables {
    expose_sql        = true
    sql_allowed_cidrs = ["203.0.113.0/24", "198.51.100.7/32"]
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).service.type == "LoadBalancer"
    error_message = "expose_sql must make the SQL service a LoadBalancer."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).service.annotations["service.beta.kubernetes.io/aws-load-balancer-type"] == "nlb" &&
      !contains(keys(yamldecode(helm_release.igloo[0].values[0]).service.annotations), "service.beta.kubernetes.io/aws-load-balancer-internal")
    )
    error_message = "The load balancer must be an internet-facing NLB by default."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).service.loadBalancerSourceRanges == ["203.0.113.0/24", "198.51.100.7/32"]
    error_message = "loadBalancerSourceRanges must equal sql_allowed_cidrs."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).tls.existingSecret == "igloo-tls" &&
      kubernetes_secret_v1.tls[0].type == "kubernetes.io/tls" &&
      length(tls_self_signed_cert.sql) == 1
    )
    error_message = "Exposing SQL must turn on TLS, with a self-signed certificate when none is given."
  }

  assert {
    condition     = length(time_sleep.load_balancer_cleanup) == 1 && length(data.kubernetes_service_v1.sql) == 1
    error_message = "Exposing SQL must add the load balancer cleanup delay and read the service's hostname."
  }

  assert {
    condition     = strcontains(output.next_steps, "sslmode=require") && strcontains(output.next_steps, "tofu output -raw sql_endpoint")
    error_message = "Next steps must connect with TLS when SQL is exposed."
  }
}

run "internal_load_balancer" {
  command = plan

  variables {
    expose_sql                 = true
    sql_allowed_cidrs          = ["10.20.0.0/16"]
    sql_load_balancer_internal = true
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).service.annotations["service.beta.kubernetes.io/aws-load-balancer-internal"] == "true"
    error_message = "sql_load_balancer_internal must request an internal NLB."
  }
}

################################################################################
# Other options
################################################################################

run "generated_bucket_name_kms_and_versioning" {
  command = plan

  variables {
    name_prefix        = "acme-prod"
    bucket_name        = ""
    bucket_versioning  = true
    bucket_kms_key_arn = "arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555"
  }

  # The mocked random_id would return arbitrary text; real ones are 8 hex digits.
  override_resource {
    target = random_id.suffix
    values = { hex = "a1b2c3d4" }
  }

  assert {
    condition     = aws_s3_bucket.warehouse.bucket == "acme-prod-warehouse-a1b2c3d4"
    error_message = "An empty bucket_name must generate <name_prefix>-warehouse-<random hex>."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).storage.warehouseLocation == "s3://acme-prod-warehouse-a1b2c3d4/warehouse"
    error_message = "The generated bucket name must flow into storage.warehouseLocation."
  }

  assert {
    condition     = length(aws_s3_bucket_versioning.warehouse) == 1 && length(aws_s3_bucket_lifecycle_configuration.warehouse.rule) == 2
    error_message = "Versioning must add the versioning resource and a non-current version expiry rule."
  }

  assert {
    condition = (
      one([for r in aws_s3_bucket_server_side_encryption_configuration.warehouse.rule : one(r.apply_server_side_encryption_by_default).sse_algorithm]) == "aws:kms" &&
      one([for r in aws_s3_bucket_server_side_encryption_configuration.warehouse.rule : one(r.apply_server_side_encryption_by_default).kms_master_key_id]) == "arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555"
    )
    error_message = "bucket_kms_key_arn must switch the bucket to SSE-KMS with that key."
  }

  assert {
    condition = alltrue([
      for k in ["igloo", "catalog"] :
      anytrue([
        for s in jsondecode(aws_iam_role_policy.warehouse[k].policy).Statement :
        s.Resource == "arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555" && contains(s.Action, "kms:GenerateDataKey")
      ])
    ])
    error_message = "Both service roles must be allowed to use the bucket's KMS key."
  }

  assert {
    condition     = aws_db_instance.this.identifier == "acme-prod-db" && aws_iam_role.service["igloo"].name == "acme-prod-igloo"
    error_message = "Resource names must use name_prefix."
  }
}

run "spot_x86_without_chart" {
  command = plan

  variables {
    node_instance_types = ["m7i.2xlarge", "m6i.2xlarge"]
    node_use_spot       = true
    install_chart       = false
  }

  # Neither type has instance store.
  override_data {
    target = data.aws_ec2_instance_type.node
    values = { instance_storage_supported = false }
  }

  assert {
    condition     = output.node_group.ami_type == "AL2023_x86_64_STANDARD" && output.node_group.capacity_type == "SPOT"
    error_message = "x86 Spot instance types must select the x86_64 AMI and SPOT capacity."
  }

  assert {
    condition     = output.node_group.local_storage == "none"
    error_message = "Types without instance store must keep the node group's user data as it was (no localStorage setting)."
  }

  assert {
    condition     = length(helm_release.igloo) == 0
    error_message = "install_chart=false must skip the Helm release."
  }

  assert {
    condition     = kubernetes_secret_v1.admin.metadata[0].name == "igloo-admin" && kubernetes_secret_v1.database.metadata[0].name == "igloo-database"
    error_message = "Secrets must still be created without the chart, for a chart installed separately."
  }
}

run "extra_cluster_admins" {
  command = plan

  variables {
    cluster_admin_arns = ["arn:aws:iam::123456789012:role/data-team"]
  }

  assert {
    condition     = contains(keys(module.eks.access_entries), "arn:aws:iam::123456789012:role/data-team")
    error_message = "cluster_admin_arns must become EKS access entries."
  }
}

run "user_supplied_tls_certificate" {
  command = plan

  variables {
    tls_certificate_pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
    tls_private_key_pem = "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n"
  }

  assert {
    condition = (
      length(tls_self_signed_cert.sql) == 0 &&
      length(kubernetes_secret_v1.tls) == 1 &&
      yamldecode(helm_release.igloo[0].values[0]).tls.existingSecret == "igloo-tls" &&
      yamldecode(helm_release.igloo[0].values[0]).service.type == "ClusterIP"
    )
    error_message = "A supplied certificate must be used as-is, without exposing SQL."
  }
}

################################################################################
# Input validation
################################################################################

run "rejects_bad_name_prefix" {
  command = plan

  variables {
    name_prefix = "Igloo_Prod"
  }

  expect_failures = [var.name_prefix]
}

run "rejects_long_name_prefix" {
  command = plan

  variables {
    name_prefix = "a-very-long-name-prefix-indeed"
  }

  expect_failures = [var.name_prefix]
}

run "rejects_bad_region" {
  command = plan

  variables {
    region = "useast1"
  }

  expect_failures = [var.region]
}

run "rejects_bad_kubernetes_version" {
  command = plan

  variables {
    kubernetes_version = "1.35.2"
  }

  expect_failures = [var.kubernetes_version]
}

run "rejects_large_vpc_cidr" {
  command = plan

  variables {
    vpc_cidr = "10.0.0.0/8"
  }

  expect_failures = [var.vpc_cidr]
}

run "rejects_expose_sql_without_cidrs" {
  command = plan

  variables {
    expose_sql = true
  }

  expect_failures = [var.sql_allowed_cidrs]
}

run "rejects_invalid_sql_cidr" {
  command = plan

  variables {
    expose_sql        = true
    sql_allowed_cidrs = ["my-office"]
  }

  expect_failures = [var.sql_allowed_cidrs]
}

run "rejects_mixed_architectures" {
  command = plan

  variables {
    node_instance_types = ["r7g.xlarge", "r7i.xlarge"]
  }

  expect_failures = [var.node_instance_types]
}

run "rejects_desired_size_above_max" {
  command = plan

  variables {
    node_desired_size = 5
  }

  expect_failures = [var.node_desired_size]
}

run "rejects_bad_bucket_name" {
  command = plan

  variables {
    bucket_name = "My_Bucket"
  }

  expect_failures = [var.bucket_name]
}

run "rejects_certificate_without_key" {
  command = plan

  variables {
    tls_certificate_pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
  }

  expect_failures = [var.tls_private_key_pem]
}

run "rejects_bad_memory_quantity" {
  command = plan

  variables {
    igloo_memory = "12GB"
  }

  expect_failures = [var.igloo_memory]
}

run "rejects_large_worker_without_workers" {
  command = plan

  variables {
    large_worker_enabled = true
  }

  expect_failures = [var.large_worker_enabled]
}

run "rejects_mixed_worker_architectures" {
  command = plan

  variables {
    workers_enabled        = true
    workers_instance_types = ["m7gd.4xlarge", "m7i.4xlarge"]
  }

  expect_failures = [var.workers_instance_types]
}

run "rejects_session_arn_as_cluster_admin" {
  command = plan

  variables {
    cluster_admin_arns = ["arn:aws:sts::123456789012:assumed-role/Admin/alice"]
  }

  expect_failures = [var.cluster_admin_arns]
}
