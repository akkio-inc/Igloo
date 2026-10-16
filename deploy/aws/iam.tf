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
