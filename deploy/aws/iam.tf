################################################################################
# IAM roles for service accounts (IRSA)
#
# One role for the igloo pods (DuckDB reads and writes table data through the
# AWS credential chain, which picks up the web identity token EKS mounts) and
# one for the Lakekeeper catalog (LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS).
# Each role can be assumed only by its own Kubernetes service account and can
# touch only s3://<bucket>/warehouse/.
################################################################################

locals {
  service_roles = {
    igloo = {
      name            = "${var.name_prefix}-igloo"
      service_account = local.igloo_service_account
      description     = "Igloo SQL engine pods: read and write table data"
    }
    catalog = {
      name            = "${var.name_prefix}-catalog"
      service_account = local.catalog_service_account
      description     = "Lakekeeper Iceberg catalog: read and write table metadata"
    }
  }

  warehouse_access_statements = concat(
    [
      {
        Sid      = "ListWarehousePrefix"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.warehouse.arn
        Condition = {
          StringLike = {
            "s3:prefix" = [local.warehouse_prefix, "${local.warehouse_prefix}/*"]
          }
        }
      },
      {
        Sid      = "BucketLocation"
        Effect   = "Allow"
        Action   = "s3:GetBucketLocation"
        Resource = aws_s3_bucket.warehouse.arn
      },
      {
        Sid    = "ReadWriteWarehouseObjects"
        Effect = "Allow"
        # PutObject also covers CreateMultipartUpload, UploadPart and
        # CompleteMultipartUpload.
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = "${aws_s3_bucket.warehouse.arn}/${local.warehouse_prefix}/*"
      },
    ],
    var.bucket_kms_key_arn == "" ? [] : [
      {
        Sid      = "UseBucketKey"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
        Resource = var.bucket_kms_key_arn
      },
    ],
  )
}

resource "aws_iam_role" "service" {
  for_each = local.service_roles

  name        = each.value.name
  description = each.value.description

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Federated = module.eks.oidc_provider_arn }
        Action    = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "${module.eks.oidc_provider}:sub" = "system:serviceaccount:${var.namespace}:${each.value.service_account}"
            "${module.eks.oidc_provider}:aud" = "sts.amazonaws.com"
          }
        }
      },
    ]
  })
}

resource "aws_iam_role_policy" "warehouse" {
  for_each = local.service_roles

  name = "warehouse-access"
  role = aws_iam_role.service[each.key].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = local.warehouse_access_statements
  })
}

resource "aws_iam_role_policy_attachment" "igloo_additional" {
  for_each = toset(var.igloo_additional_policy_arns)

  role       = aws_iam_role.service["igloo"].name
  policy_arn = each.value
}

################################################################################
# Storage credentials for other engines
#
# Spark, Trino and other engines that read and write tables through Igloo's
# Iceberg REST catalog get credentials for one table at a time
# (docs/external-engines.md). Igloo makes them by assuming this role with a
# session policy that allows only that table's prefix, read-only unless the
# user may change the table, for an hour. Only the igloo pods' role may
# assume it, and it can do no more than they can.
################################################################################

resource "aws_iam_role" "engines" {
  name        = "${var.name_prefix}-engines"
  description = "Assumed by Igloo to hand other engines credentials limited to one table"

  # Role chaining (IRSA role -> this one) caps sessions at an hour.
  max_session_duration = 3600

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = aws_iam_role.service["igloo"].arn }
        Action    = "sts:AssumeRole"
      },
    ]
  })
}

resource "aws_iam_role_policy" "engines_warehouse" {
  name = "warehouse-access"
  role = aws_iam_role.engines.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = local.warehouse_access_statements
  })
}

resource "aws_iam_role_policy" "igloo_assume_engines" {
  name = "assume-engines-role"
  role = aws_iam_role.service["igloo"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "VendTableCredentials"
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Resource = aws_iam_role.engines.arn
      },
    ]
  })
}

################################################################################
# Workers (with workers_enabled)
#
# Workers read the warehouse and write the rows they exchange between a
# statement's stages, nothing else. They write a table's files with the
# credentials a statement's tasks carry: the igloo pods assume the engines role
# with a session policy for each task's directory, with no right to delete,
# for an hour, and check every file before they commit it
# (docs/distributed-execution.md#security). Nothing here lets a worker write
# to a table's location by itself; a worker cannot assume the engines role.
################################################################################

resource "aws_iam_role" "worker" {
  count = var.workers_enabled ? 1 : 0

  name        = "${var.name_prefix}-worker"
  description = "Igloo workers: read table data, write the rows they exchange"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Federated = module.eks.oidc_provider_arn }
        Action    = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "${module.eks.oidc_provider}:sub" = "system:serviceaccount:${var.namespace}:${local.worker_service_account}"
            "${module.eks.oidc_provider}:aud" = "sts.amazonaws.com"
          }
        }
      },
    ]
  })
}

resource "aws_iam_role_policy" "worker_warehouse" {
  count = var.workers_enabled ? 1 : 0

  name = "warehouse-read-exchanges-write"
  role = aws_iam_role.worker[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        {
          Sid      = "ListWarehousePrefix"
          Effect   = "Allow"
          Action   = "s3:ListBucket"
          Resource = aws_s3_bucket.warehouse.arn
          Condition = {
            StringLike = {
              "s3:prefix" = [local.warehouse_prefix, "${local.warehouse_prefix}/*"]
            }
          }
        },
        {
          Sid      = "BucketLocation"
          Effect   = "Allow"
          Action   = "s3:GetBucketLocation"
          Resource = aws_s3_bucket.warehouse.arn
        },
        {
          Sid      = "ReadWarehouseObjects"
          Effect   = "Allow"
          Action   = "s3:GetObject"
          Resource = "${aws_s3_bucket.warehouse.arn}/${local.warehouse_prefix}/*"
        },
        {
          Sid    = "WriteExchanges"
          Effect = "Allow"
          # PutObject also covers multipart uploads. Igloo deletes exchanges.
          Action = [
            "s3:PutObject",
            "s3:AbortMultipartUpload",
            "s3:ListMultipartUploadParts",
          ]
          Resource = "${aws_s3_bucket.warehouse.arn}/${local.warehouse_prefix}/_igloo_shuffle/*"
        },
      ],
      var.bucket_kms_key_arn == "" ? [] : [
        {
          Sid      = "UseBucketKey"
          Effect   = "Allow"
          Action   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
          Resource = var.bucket_kms_key_arn
        },
      ],
    )
  })
}

################################################################################
# Cluster autoscaler (with workers_enabled, see workers.tf)
#
# Assumable only by kube-system/cluster-autoscaler. It may read autoscaling
# groups, launch templates, instance types and managed node groups (labels
# and taints, to scale a group from zero), and change the size of the worker
# node groups' autoscaling groups only, not the default node group's.
################################################################################

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "cluster_autoscaler" {
  count = var.workers_enabled ? 1 : 0

  name        = "${var.name_prefix}-cluster-autoscaler"
  description = "Kubernetes cluster autoscaler: scales the worker node groups"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Federated = module.eks.oidc_provider_arn }
        Action    = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "${module.eks.oidc_provider}:sub" = "system:serviceaccount:kube-system:${local.cluster_autoscaler_service_account}"
            "${module.eks.oidc_provider}:aud" = "sts.amazonaws.com"
          }
        }
      },
    ]
  })
}

resource "aws_iam_role_policy" "cluster_autoscaler" {
  count = var.workers_enabled ? 1 : 0

  name = "scale-worker-node-groups"
  role = aws_iam_role.cluster_autoscaler[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Describe"
        Effect = "Allow"
        Action = [
          "autoscaling:DescribeAutoScalingGroups",
          "autoscaling:DescribeAutoScalingInstances",
          "autoscaling:DescribeLaunchConfigurations",
          "autoscaling:DescribeScalingActivities",
          "autoscaling:DescribeTags",
          "ec2:DescribeImages",
          "ec2:DescribeInstanceTypes",
          "ec2:DescribeLaunchTemplateVersions",
          "ec2:GetInstanceTypesFromInstanceRequirements",
          "eks:DescribeNodegroup",
        ]
        Resource = "*"
      },
      {
        Sid    = "ScaleWorkerNodeGroups"
        Effect = "Allow"
        Action = [
          "autoscaling:SetDesiredCapacity",
          "autoscaling:TerminateInstanceInAutoScalingGroup",
        ]
        Resource = [
          for g in local.worker_autoscaling_groups :
          "arn:${data.aws_partition.current.partition}:autoscaling:${var.region}:${data.aws_caller_identity.current.account_id}:autoScalingGroup:*:autoScalingGroupName/${g.name}"
        ]
      },
    ]
  })
}
