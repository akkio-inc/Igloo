# Namespace and the Secrets the chart reads through its existingSecret values.
# They are created even when install_chart is false, so the chart can be
# installed by hand against the same names.

locals {
  admin_secret_name      = "${var.release_name}-admin"
  database_secret_name   = "${var.release_name}-database"
  encryption_secret_name = "${var.release_name}-catalog-encryption"
  tls_secret_name        = "${var.release_name}-tls"
  tls_enabled            = nonsensitive(var.tls_certificate_pem != null)
}

resource "kubernetes_namespace_v1" "igloo" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/part-of" = "igloo"
      # Reject privileged pods; report anything short of the restricted profile.
      "pod-security.kubernetes.io/enforce" = "baseline"
      "pod-security.kubernetes.io/warn"    = "restricted"
      "pod-security.kubernetes.io/audit"   = "restricted"
    }
  }
}

resource "random_password" "admin" {
  length  = 32
  special = false
}

# Lakekeeper encrypts the secrets it stores in PostgreSQL with this key. It
# must never change once the catalog holds data.
resource "random_password" "catalog_encryption_key" {
  length  = 48
  special = false
}

resource "kubernetes_secret_v1" "admin" {
  metadata {
    name      = local.admin_secret_name
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  type = "Opaque"
  data = {
    password = random_password.admin.result
  }
}

# One URL for both Igloo's metadata (schema igloo) and Lakekeeper (schema public).
resource "kubernetes_secret_v1" "database" {
  metadata {
    name      = local.database_secret_name
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  type = "Opaque"
  data = {
    url = local.database_url
  }
}

resource "kubernetes_secret_v1" "catalog_encryption" {
  metadata {
    name      = local.encryption_secret_name
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  type = "Opaque"
  data = {
    "encryption-key" = random_password.catalog_encryption_key.result
  }
}

resource "kubernetes_secret_v1" "tls" {
  count = local.tls_enabled ? 1 : 0

  metadata {
    name      = local.tls_secret_name
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  type = "kubernetes.io/tls"
  data = {
    "tls.crt" = var.tls_certificate_pem
    "tls.key" = var.tls_private_key_pem
  }
}
