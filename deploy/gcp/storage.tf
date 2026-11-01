# Table data (Parquet files and Iceberg metadata) lives under
# gs://<bucket>/warehouse.
#
# Trivy findings accepted on purpose (see README.md, "Security notes"):
# - GCP-0066 customer-managed key: data is encrypted with Google-managed keys;
#   CMEK is an organization policy decision that also involves Cloud SQL and GKE.
# - GCP-0077 bucket usage logs: Google recommends Cloud Audit Logs instead, which
#   are a project-wide setting and costly with a query engine reading many objects.
# - GCP-0078 versioning: optional (bucket_versioning). Soft delete keeps deleted
#   objects recoverable for bucket_soft_delete_days without doubling storage.
#trivy:ignore:GCP-0066
#trivy:ignore:GCP-0077
#trivy:ignore:GCP-0078
resource "google_storage_bucket" "warehouse" {
  name          = local.bucket_name
  location      = upper(coalesce(var.bucket_location, var.region))
  storage_class = "STANDARD"
  labels        = local.labels
  force_destroy = var.bucket_force_destroy

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  soft_delete_policy {
    retention_duration_seconds = var.bucket_soft_delete_days * 86400
  }

  versioning {
    enabled = var.bucket_versioning
  }

  # What a stopped write had uploaded can be recovered, and so holds its rows:
  # Igloo aborts what it can see to, and this the rest.
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }

  dynamic "lifecycle_rule" {
    for_each = var.bucket_versioning ? [1] : []
    content {
      condition {
        days_since_noncurrent_time = var.bucket_noncurrent_version_retention_days
        with_state                 = "ARCHIVED"
      }
      action {
        type = "Delete"
      }
    }
  }

  depends_on = [google_project_service.this]
}

# ---------------------------------------------------------------------------
# Service accounts: one for the query engine, one for the catalog. Each can
# read and write objects in the warehouse bucket and nothing else.
# ---------------------------------------------------------------------------

resource "google_service_account" "igloo" {
  account_id   = "${var.name}-igloo"
  display_name = "Igloo query engine (${var.name})"
  description  = "Reads and writes table data in gs://${local.bucket_name}. Its HMAC key is what DuckDB uses."

  depends_on = [google_project_service.this]
}

resource "google_service_account" "catalog" {
  account_id   = "${var.name}-catalog"
  display_name = "Igloo catalog / Lakekeeper (${var.name})"
  description  = "Lakekeeper's identity for gs://${local.bucket_name}, through Workload Identity."

  depends_on = [google_project_service.this]
}

locals {
  bucket_members = {
    igloo   = google_service_account.igloo.member
    catalog = google_service_account.catalog.member
  }

  # objectAdmin: create, read, list, overwrite and delete objects.
  # legacyBucketReader: adds storage.buckets.get (read bucket metadata, e.g. to
  # detect hierarchical namespace or check the bucket exists). Neither role can
  # change the bucket's settings or IAM policy.
  bucket_roles = ["roles/storage.objectAdmin", "roles/storage.legacyBucketReader"]

  bucket_grants = {
    for pair in setproduct(keys(local.bucket_members), local.bucket_roles) :
    "${pair[0]} ${pair[1]}" => { member = local.bucket_members[pair[0]], role = pair[1] }
  }
}

resource "google_storage_bucket_iam_member" "warehouse" {
  for_each = local.bucket_grants

  bucket = google_storage_bucket.warehouse.name
  role   = each.value.role
  member = each.value.member
}

# DuckDB reads and writes GCS through its S3-compatible XML API, which accepts
# only HMAC keys. The key belongs to the igloo service account, so it carries
# exactly that account's bucket permissions.
resource "google_storage_hmac_key" "igloo" {
  service_account_email = google_service_account.igloo.email
}

# ---------------------------------------------------------------------------
# Workers (with workers_enabled): an account of their own, which reads the
# warehouse and may only create objects in the two places workers write: the
# rows they exchange, and statements' directories of table files, named
# igloo-write-* under a table's data/. It can overwrite and delete nothing:
# Google Cloud has no credentials limited to one statement's directory that
# DuckDB could use, so Igloo checks every file a worker says it wrote before it
# commits it (docs/distributed-execution.md#security).
#
# IAM Conditions have startsWith, endsWith and extract on a resource's name, and
# no regular expressions, so this cannot say that what follows igloo-write- is
# hexadecimal, as Igloo's directories are. That gives a worker nothing new: it
# could always choose hex digits of its own. It is Igloo that keeps to one
# statement's directory, which is why the files it commits are its own copies.
# ---------------------------------------------------------------------------

resource "google_service_account" "worker" {
  count = var.workers_enabled ? 1 : 0

  account_id   = "${var.name}-worker"
  display_name = "Igloo workers (${var.name})"
  description  = "Reads table data in gs://${local.bucket_name}, and creates objects only in exchanges and statements' directories. Its HMAC key is what workers' DuckDB uses."

  depends_on = [google_project_service.this]
}

resource "google_storage_bucket_iam_member" "worker_read" {
  for_each = var.workers_enabled ? toset(["roles/storage.objectViewer", "roles/storage.legacyBucketReader"]) : toset([])

  bucket = google_storage_bucket.warehouse.name
  role   = each.value
  member = google_service_account.worker[0].member
}

resource "google_storage_bucket_iam_member" "worker_write" {
  count = var.workers_enabled ? 1 : 0

  bucket = google_storage_bucket.warehouse.name
  # objects.create and multipart uploads: no overwrite, no delete.
  role   = "roles/storage.objectCreator"
  member = google_service_account.worker[0].member

  condition {
    title       = "exchanges-and-statement-directories"
    description = "Only the rows workers exchange and statements' directories of table files."
    expression  = "resource.name.startsWith('projects/_/buckets/${local.bucket_name}/objects/warehouse/_igloo_shuffle/') || (resource.name.startsWith('projects/_/buckets/${local.bucket_name}/objects/warehouse/') && resource.name.extract('/data/igloo-write-{dir}/') != '')"
  }
}

resource "google_storage_hmac_key" "worker" {
  count = var.workers_enabled ? 1 : 0

  service_account_email = google_service_account.worker[0].email
}

resource "google_service_account_iam_member" "worker_workload_identity" {
  count = var.workers_enabled ? 1 : 0

  service_account_id = google_service_account.worker[0].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.workload_pool}[${var.namespace}/${local.worker_ksa}]"

  depends_on = [google_container_cluster.this]
}

# ---------------------------------------------------------------------------
# Workload Identity: the Kubernetes service accounts created by the chart act
# as the Google service accounts above.
# ---------------------------------------------------------------------------

resource "google_service_account_iam_member" "igloo_workload_identity" {
  service_account_id = google_service_account.igloo.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.workload_pool}[${var.namespace}/${local.igloo_ksa}]"

  # The workload identity pool exists once the first cluster in the project does.
  depends_on = [google_container_cluster.this]
}

resource "google_service_account_iam_member" "catalog_workload_identity" {
  service_account_id = google_service_account.catalog.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.workload_pool}[${var.namespace}/${local.catalog_ksa}]"

  depends_on = [google_container_cluster.this]
}
