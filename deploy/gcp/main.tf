locals {
  labels = merge({ app = "igloo", managed-by = "opentofu" }, var.labels)

  # Short random suffix for names that are global (bucket) or cannot be reused
  # for a week after deletion (Cloud SQL instance).
  suffix = random_id.suffix.hex

  bucket_name        = coalesce(var.bucket_name, "${var.name}-warehouse-${local.suffix}")
  warehouse_location = "gs://${google_storage_bucket.warehouse.name}/warehouse"

  pods_range_name     = "pods"
  services_range_name = "services"

  workload_pool   = "${var.project_id}.svc.id.goog"
  cluster_api_url = "https://${google_container_cluster.this.control_plane_endpoints_config[0].dns_endpoint_config[0].endpoint}"

  # Names inside the cluster. The chart is told to use exactly these service
  # account names so the Workload Identity bindings below match them.
  release_name = "igloo"
  igloo_ksa    = "igloo"
  catalog_ksa  = "igloo-catalog"
  worker_ksa   = "igloo-worker"
  service_name = "igloo" # Service the chart creates for the SQL port (release fullname).

  secret_names = {
    admin          = "igloo-admin"
    database       = "igloo-database"
    encryption_key = "igloo-catalog-encryption"
    gcs_hmac       = "igloo-gcs-hmac"
    worker_hmac    = "igloo-gcs-hmac-worker"
    tls            = "igloo-tls"
  }

  # Whether a certificate was supplied is not secret; the certificate is.
  tls_enabled = nonsensitive(var.tls_certificate != null)
}

resource "random_id" "suffix" {
  byte_length = 3
}

# ---------------------------------------------------------------------------
# Google APIs
# ---------------------------------------------------------------------------

resource "google_project_service" "this" {
  for_each = toset([
    "cloudresourcemanager.googleapis.com",
    "compute.googleapis.com",
    "container.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com", # Workload Identity token exchange
    "servicenetworking.googleapis.com",
    "sqladmin.googleapis.com",
    "storage.googleapis.com",
    "sts.googleapis.com", # Workload Identity, and the catalog's downscoped tokens
  ])

  service = each.value

  # Other workloads in the project may rely on these APIs.
  disable_on_destroy = false
}
