################################################################################
# S3 bucket for Iceberg table data. Tables live under s3://<bucket>/warehouse.
################################################################################

resource "aws_s3_bucket" "warehouse" {
  bucket        = local.bucket_name
  force_destroy = var.bucket_force_destroy
}

resource "aws_s3_bucket_ownership_controls" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.bucket_kms_key_arn != "" ? "aws:kms" : "AES256"
      kms_master_key_id = var.bucket_kms_key_arn != "" ? var.bucket_kms_key_arn : null
    }
    # S3 Bucket Keys cut KMS request costs; ignored for SSE-S3.
    bucket_key_enabled = var.bucket_kms_key_arn != ""
  }
}

resource "aws_s3_bucket_versioning" "warehouse" {
  count  = var.bucket_versioning ? 1 : 0
  bucket = aws_s3_bucket.warehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  # Failed or abandoned multipart uploads are billed until removed.
  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    # What a stopped write had uploaded can be recovered, and so holds its rows:
    # Igloo aborts what it can see to, and this the rest.
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  dynamic "rule" {
    for_each = var.bucket_versioning ? [1] : []

    content {
      id     = "expire-noncurrent-versions"
      status = "Enabled"

      filter {}

      noncurrent_version_expiration {
        noncurrent_days = var.bucket_noncurrent_version_retention_days
      }

      expiration {
        expired_object_delete_marker = true
      }
    }
  }

  depends_on = [aws_s3_bucket_versioning.warehouse]
}

resource "aws_s3_bucket_policy" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.warehouse.arn,
          "${aws_s3_bucket.warehouse.arn}/*",
        ]
        Condition = {
          Bool = { "aws:SecureTransport" = "false" }
        }
      },
    ]
  })

  # Apply the public access block first so the bucket is never briefly open.
  depends_on = [aws_s3_bucket_public_access_block.warehouse]
}
