# Namespace and Secrets the chart reads through its existingSecret settings.

resource "kubernetes_namespace_v1" "igloo" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/part-of"    = "igloo"
      "app.kubernetes.io/managed-by" = "opentofu"
    }
  }

  # The Kubernetes provider is configured from the cluster; this makes the
  # ordering explicit (create after the cluster, destroy before it).
  depends_on = [google_container_cluster.this]
}

resource "random_password" "admin" {
  length  = 32
  special = false
}

# Key for the secrets Lakekeeper stores in Postgres. Changing it makes stored
# warehouse credentials unreadable, so it is generated once and kept.
resource "random_password" "catalog_encryption_key" {
  length  = 48
  special = false
}

resource "kubernetes_secret_v1" "admin" {
  metadata {
    name      = local.secret_names.admin
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  data = {
    password = random_password.admin.result
  }
}

# Same database for igloo's users (schema igloo) and the catalog (schema public).
resource "kubernetes_secret_v1" "database" {
  metadata {
    name      = local.secret_names.database
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  data = {
    url = local.database_url
  }
}

resource "kubernetes_secret_v1" "catalog_encryption_key" {
  metadata {
    name      = local.secret_names.encryption_key
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  data = {
    encryption-key = random_password.catalog_encryption_key.result
  }
}

resource "kubernetes_secret_v1" "gcs_hmac" {
  metadata {
    name      = local.secret_names.gcs_hmac
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  data = {
    hmac-key-id = google_storage_hmac_key.igloo.access_id
    hmac-secret = google_storage_hmac_key.igloo.secret
  }
}

resource "kubernetes_secret_v1" "tls" {
  count = local.tls_enabled ? 1 : 0

  metadata {
    name      = local.secret_names.tls
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  type = "kubernetes.io/tls"
  data = {
    "tls.crt" = var.tls_certificate
    "tls.key" = var.tls_private_key
  }
}
