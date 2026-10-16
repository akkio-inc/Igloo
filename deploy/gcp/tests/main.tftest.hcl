# Plan-only tests with every provider mocked: no credentials, no cloud calls.
# Run from deploy/gcp:  tofu init -backend=false && tofu test

# OpenTofu still runs each provider's own input validation on mocked values,
# so computed values that feed other resources must look real.
mock_provider "google" {
  mock_resource "google_compute_network" {
    defaults = {
      id        = "projects/igloo-test-project/global/networks/igloo"
      self_link = "https://www.googleapis.com/compute/v1/projects/igloo-test-project/global/networks/igloo"
    }
  }

  mock_resource "google_service_account" {
    defaults = {
      email  = "igloo-nodes@igloo-test-project.iam.gserviceaccount.com"
      member = "serviceAccount:igloo-nodes@igloo-test-project.iam.gserviceaccount.com"
      name   = "projects/igloo-test-project/serviceAccounts/igloo-nodes@igloo-test-project.iam.gserviceaccount.com"
    }
  }

  mock_resource "google_sql_database_instance" {
    defaults = {
      private_ip_address = "10.40.0.3"
    }
  }

  mock_resource "google_storage_hmac_key" {
    defaults = {
      access_id = "GOOG1EMOCKACCESSID"
      secret    = "mock-hmac-secret"
    }
  }
}

mock_provider "kubernetes" {}
mock_provider "helm" {}
mock_provider "random" {}

override_resource {
  target = google_service_account.igloo
  values = {
    email  = "igloo-igloo@igloo-test-project.iam.gserviceaccount.com"
    member = "serviceAccount:igloo-igloo@igloo-test-project.iam.gserviceaccount.com"
    name   = "projects/igloo-test-project/serviceAccounts/igloo-igloo@igloo-test-project.iam.gserviceaccount.com"
  }
}

override_resource {
  target = google_service_account.catalog
  values = {
    email  = "igloo-catalog@igloo-test-project.iam.gserviceaccount.com"
    member = "serviceAccount:igloo-catalog@igloo-test-project.iam.gserviceaccount.com"
    name   = "projects/igloo-test-project/serviceAccounts/igloo-catalog@igloo-test-project.iam.gserviceaccount.com"
  }
}

variables {
  project_id  = "igloo-test-project"
  bucket_name = "igloo-test-warehouse"
}

run "defaults" {
  command = plan

  # --- Bucket ---------------------------------------------------------------

  assert {
    condition     = google_storage_bucket.warehouse.public_access_prevention == "enforced"
    error_message = "The warehouse bucket must enforce public access prevention."
  }

  assert {
    condition     = google_storage_bucket.warehouse.uniform_bucket_level_access == true
    error_message = "The warehouse bucket must use uniform bucket-level access."
  }

  assert {
    condition     = google_storage_bucket.warehouse.versioning[0].enabled == false && length(google_storage_bucket.warehouse.lifecycle_rule) == 0
    error_message = "Versioning is off by default, with no lifecycle rule."
  }

  assert {
    condition     = google_storage_bucket.warehouse.soft_delete_policy[0].retention_duration_seconds == 604800
    error_message = "Deleted objects stay recoverable for 7 days by default."
  }

  assert {
    condition     = google_storage_bucket.warehouse.force_destroy == false
    error_message = "The bucket must not be force-destroyed by default."
  }

  assert {
    condition     = google_storage_bucket.warehouse.location == "US-CENTRAL1"
    error_message = "The bucket defaults to the deployment region."
  }

  # --- Bucket IAM: only the two app identities, only object-level roles ------

  assert {
    condition     = length(google_storage_bucket_iam_member.warehouse) == 4
    error_message = "Expected two roles for each of the two service accounts."
  }

  assert {
    condition = alltrue([
      for g in google_storage_bucket_iam_member.warehouse :
      g.bucket == "igloo-test-warehouse" &&
      contains(["roles/storage.objectAdmin", "roles/storage.legacyBucketReader"], g.role) &&
      contains([
        "serviceAccount:igloo-igloo@igloo-test-project.iam.gserviceaccount.com",
        "serviceAccount:igloo-catalog@igloo-test-project.iam.gserviceaccount.com",
      ], g.member)
    ])
    error_message = "Bucket grants must be objectAdmin/legacyBucketReader on the warehouse bucket for the igloo and catalog accounts only."
  }

  # --- Cloud SQL ------------------------------------------------------------

  assert {
    condition     = google_sql_database_instance.this.settings[0].ip_configuration[0].ipv4_enabled == false
    error_message = "Cloud SQL must not have a public IP."
  }

  assert {
    condition     = google_sql_database_instance.this.settings[0].ip_configuration[0].private_network == google_compute_network.this.id
    error_message = "Cloud SQL must be attached to the module's VPC."
  }

  assert {
    condition     = google_sql_database_instance.this.settings[0].ip_configuration[0].ssl_mode == "ENCRYPTED_ONLY"
    error_message = "Cloud SQL must require TLS."
  }

  assert {
    condition     = google_sql_database_instance.this.database_version == "POSTGRES_17"
    error_message = "Cloud SQL must run PostgreSQL 17."
  }

  assert {
    condition = (
      google_sql_database_instance.this.settings[0].backup_configuration[0].enabled &&
      google_sql_database_instance.this.settings[0].backup_configuration[0].point_in_time_recovery_enabled
    )
    error_message = "Automated backups and point-in-time recovery must be on."
  }

  assert {
    condition = (
      google_sql_database_instance.this.deletion_protection &&
      google_sql_database_instance.this.settings[0].deletion_protection_enabled &&
      google_container_cluster.this.deletion_protection
    )
    error_message = "Deletion protection is on by default."
  }

  assert {
    condition     = google_sql_database_instance.this.settings[0].availability_type == "ZONAL"
    error_message = "The database is zonal unless db_high_availability is set."
  }

  assert {
    condition     = nonsensitive(kubernetes_secret_v1.database.data.url) == "postgresql://igloo:${nonsensitive(random_password.db.result)}@10.40.0.3:5432/igloo?sslmode=require"
    error_message = "Database URL must point at the private IP, database igloo, with sslmode=require."
  }

  # --- GKE ------------------------------------------------------------------

  assert {
    condition     = google_container_cluster.this.enable_autopilot == true
    error_message = "The cluster must be Autopilot."
  }

  assert {
    condition     = google_container_cluster.this.private_cluster_config[0].enable_private_nodes == true
    error_message = "Nodes must be private."
  }

  assert {
    condition     = google_container_cluster.this.workload_identity_config[0].workload_pool == "igloo-test-project.svc.id.goog"
    error_message = "Workload Identity must use the project's pool."
  }

  assert {
    condition     = length(google_container_cluster.this.master_authorized_networks_config[0].cidr_blocks) == 0
    error_message = "The public IP endpoint admits no CIDRs by default."
  }

  assert {
    condition     = google_container_cluster.this.cluster_autoscaling[0].auto_provisioning_defaults[0].service_account == "igloo-nodes@igloo-test-project.iam.gserviceaccount.com"
    error_message = "Nodes must run as the dedicated node service account."
  }

  assert {
    condition     = google_compute_router_nat.this.source_subnetwork_ip_ranges_to_nat == "ALL_SUBNETWORKS_ALL_IP_RANGES"
    error_message = "Private nodes need Cloud NAT."
  }

  assert {
    condition     = alltrue([for s in google_project_service.this : s.disable_on_destroy == false])
    error_message = "APIs must not be disabled on destroy."
  }

  # --- Workload Identity ----------------------------------------------------

  assert {
    condition = (
      google_service_account_iam_member.igloo_workload_identity.member == "serviceAccount:igloo-test-project.svc.id.goog[igloo/igloo]" &&
      google_service_account_iam_member.igloo_workload_identity.role == "roles/iam.workloadIdentityUser" &&
      google_service_account_iam_member.igloo_workload_identity.service_account_id == google_service_account.igloo.name
    )
    error_message = "igloo GSA must trust KSA igloo/igloo."
  }

  assert {
    condition = (
      google_service_account_iam_member.catalog_workload_identity.member == "serviceAccount:igloo-test-project.svc.id.goog[igloo/igloo-catalog]" &&
      google_service_account_iam_member.catalog_workload_identity.role == "roles/iam.workloadIdentityUser" &&
      google_service_account_iam_member.catalog_workload_identity.service_account_id == google_service_account.catalog.name
    )
    error_message = "catalog GSA must trust KSA igloo/igloo-catalog."
  }

  # The KSA names the chart is told to create must be the ones bound above.
  assert {
    condition = (
      google_service_account_iam_member.igloo_workload_identity.member ==
      "serviceAccount:igloo-test-project.svc.id.goog[${helm_release.igloo[0].namespace}/${yamldecode(helm_release.igloo[0].values[0]).serviceAccount.name}]"
    )
    error_message = "igloo Workload Identity binding does not match the chart's service account."
  }

  assert {
    condition = (
      google_service_account_iam_member.catalog_workload_identity.member ==
      "serviceAccount:igloo-test-project.svc.id.goog[${helm_release.igloo[0].namespace}/${yamldecode(helm_release.igloo[0].values[0]).catalog.serviceAccount.name}]"
    )
    error_message = "catalog Workload Identity binding does not match the chart's catalog service account."
  }

  # --- HMAC key -------------------------------------------------------------

  assert {
    condition     = google_storage_hmac_key.igloo.service_account_email == google_service_account.igloo.email
    error_message = "The HMAC key must belong to the igloo service account."
  }

  assert {
    condition     = google_storage_hmac_key.igloo.service_account_email != google_service_account.catalog.email
    error_message = "The HMAC key must not belong to the catalog service account."
  }

  assert {
    condition = (
      toset(keys(nonsensitive(kubernetes_secret_v1.gcs_hmac.data))) == toset(["hmac-key-id", "hmac-secret"]) &&
      nonsensitive(kubernetes_secret_v1.gcs_hmac.data["hmac-key-id"]) == google_storage_hmac_key.igloo.access_id &&
      nonsensitive(kubernetes_secret_v1.gcs_hmac.data["hmac-secret"] == google_storage_hmac_key.igloo.secret)
    )
    error_message = "The GCS Secret must hold hmac-key-id and hmac-secret from the HMAC key."
  }

  # --- Other Secrets --------------------------------------------------------

  assert {
    condition     = toset(keys(nonsensitive(kubernetes_secret_v1.admin.data))) == toset(["password"])
    error_message = "Admin Secret must have the key \"password\"."
  }

  assert {
    condition     = toset(keys(nonsensitive(kubernetes_secret_v1.catalog_encryption_key.data))) == toset(["encryption-key"])
    error_message = "Encryption key Secret must have the key \"encryption-key\"."
  }

  assert {
    condition     = length(kubernetes_secret_v1.tls) == 0
    error_message = "No TLS Secret without a certificate."
  }

  # --- Helm values ----------------------------------------------------------

  assert {
    condition     = length(helm_release.igloo) == 1 && endswith(helm_release.igloo[0].chart, "/../helm/igloo")
    error_message = "The chart is installed from ../helm/igloo by default."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).storage.warehouseLocation == "gs://igloo-test-warehouse/warehouse"
    error_message = "warehouseLocation must be gs://<bucket>/warehouse."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).catalog.systemCredentials == "gcp"
    error_message = "The catalog must use GCP system credentials."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).storage.gcs.existingSecret == kubernetes_secret_v1.gcs_hmac.metadata[0].name
    error_message = "storage.gcs.existingSecret must name the HMAC Secret."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).serviceAccount.annotations["iam.gke.io/gcp-service-account"] == "igloo-igloo@igloo-test-project.iam.gserviceaccount.com" &&
      yamldecode(helm_release.igloo[0].values[0]).catalog.serviceAccount.annotations["iam.gke.io/gcp-service-account"] == "igloo-catalog@igloo-test-project.iam.gserviceaccount.com"
    )
    error_message = "Each Kubernetes service account must be annotated with its own Google service account."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).metadataDatabase.existingSecret == kubernetes_secret_v1.database.metadata[0].name &&
      yamldecode(helm_release.igloo[0].values[0]).catalog.database.existingSecret == kubernetes_secret_v1.database.metadata[0].name &&
      yamldecode(helm_release.igloo[0].values[0]).catalog.encryptionKey.existingSecret == kubernetes_secret_v1.catalog_encryption_key.metadata[0].name &&
      yamldecode(helm_release.igloo[0].values[0]).admin.existingSecret == kubernetes_secret_v1.admin.metadata[0].name
    )
    error_message = "Chart values must reference the Secrets this module creates."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).service.type == "ClusterIP"
    error_message = "SQL must not be exposed by default."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).service.loadBalancerSourceRanges == []
    error_message = "No load balancer source ranges when SQL is not exposed."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).replicaCount == 2 &&
      yamldecode(helm_release.igloo[0].values[0]).resources.requests.cpu == "2" &&
      yamldecode(helm_release.igloo[0].values[0]).resources.requests.memory == "8Gi" &&
      yamldecode(helm_release.igloo[0].values[0]).resources.requests["ephemeral-storage"] == "50Gi" &&
      yamldecode(helm_release.igloo[0].values[0]).duckdb.spillVolume.sizeLimit == "50Gi"
    )
    error_message = "Default sizing is 2 replicas of 2 CPU / 8Gi with a 50Gi spill volume."
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).nodeSelector == {} &&
      yamldecode(helm_release.igloo[0].values[0]).tolerations == []
    )
    error_message = "No Spot scheduling by default."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).podDisruptionBudget.enabled == true
    error_message = "Two replicas keep the PodDisruptionBudget."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).tls.existingSecret == ""
    error_message = "No TLS Secret is referenced without a certificate."
  }

  assert {
    condition     = output.sql_endpoint == null && length(data.kubernetes_service_v1.sql) == 0
    error_message = "No SQL endpoint when SQL is not exposed."
  }

  assert {
    condition     = strcontains(output.next_steps, "port-forward svc/igloo 5432:5432")
    error_message = "Next steps must explain port-forwarding."
  }
}

run "expose_sql" {
  command = plan

  variables {
    expose_sql        = true
    sql_allowed_cidrs = ["203.0.113.0/24", "198.51.100.7/32"]
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).service.type == "LoadBalancer"
    error_message = "expose_sql must create a LoadBalancer Service."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).service.loadBalancerSourceRanges == ["203.0.113.0/24", "198.51.100.7/32"]
    error_message = "The load balancer must admit only sql_allowed_cidrs."
  }

  assert {
    condition     = length(data.kubernetes_service_v1.sql) == 1
    error_message = "The module must read the Service to report the SQL endpoint."
  }
}

run "spot" {
  command = plan

  variables {
    spot          = true
    node_selector = { "cloud.google.com/compute-class" = "Balanced" }
  }

  assert {
    condition = yamldecode(helm_release.igloo[0].values[0]).nodeSelector == {
      "cloud.google.com/gke-spot"      = "true"
      "cloud.google.com/compute-class" = "Balanced"
    }
    error_message = "spot must add the gke-spot node selector, merged with node_selector."
  }

  assert {
    condition = yamldecode(helm_release.igloo[0].values[0]).tolerations == [{
      key      = "cloud.google.com/gke-spot"
      operator = "Equal"
      value    = "true"
      effect   = "NoSchedule"
    }]
    error_message = "spot must tolerate the gke-spot taint."
  }
}

run "tls" {
  command = plan

  variables {
    tls_certificate = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
    tls_private_key = "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----\n"
  }

  assert {
    condition     = length(kubernetes_secret_v1.tls) == 1 && kubernetes_secret_v1.tls[0].type == "kubernetes.io/tls"
    error_message = "A kubernetes.io/tls Secret must be created from the certificate."
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).tls.existingSecret == kubernetes_secret_v1.tls[0].metadata[0].name
    error_message = "tls.existingSecret must name the TLS Secret."
  }
}

run "custom_namespace_and_names" {
  command = plan

  variables {
    name        = "lake"
    namespace   = "analytics"
    bucket_name = null
  }

  assert {
    condition     = google_service_account_iam_member.igloo_workload_identity.member == "serviceAccount:igloo-test-project.svc.id.goog[analytics/igloo]"
    error_message = "Workload Identity must follow the namespace."
  }

  assert {
    condition     = helm_release.igloo[0].namespace == "analytics" && kubernetes_namespace_v1.igloo.metadata[0].name == "analytics"
    error_message = "The release must go into the chosen namespace."
  }

  assert {
    condition     = startswith(google_storage_bucket.warehouse.name, "lake-warehouse-")
    error_message = "The generated bucket name must start with <name>-warehouse-."
  }

  assert {
    condition     = google_container_cluster.this.name == "lake" && google_service_account.igloo.account_id == "lake-igloo"
    error_message = "Resource names must use the name prefix."
  }
}

run "production_options" {
  command = plan

  variables {
    db_high_availability           = true
    bucket_versioning              = true
    deletion_protection            = false
    control_plane_authorized_cidrs = ["198.51.100.0/24"]
  }

  assert {
    condition     = google_sql_database_instance.this.settings[0].availability_type == "REGIONAL"
    error_message = "db_high_availability must make the database regional."
  }

  assert {
    condition = (
      google_storage_bucket.warehouse.versioning[0].enabled &&
      length(google_storage_bucket.warehouse.lifecycle_rule) == 1
    )
    error_message = "Versioning must be on, with a rule expiring old versions."
  }

  assert {
    condition = (
      !google_sql_database_instance.this.deletion_protection &&
      !google_sql_database_instance.this.settings[0].deletion_protection_enabled &&
      !google_container_cluster.this.deletion_protection
    )
    error_message = "deletion_protection = false must turn protection off everywhere."
  }

  assert {
    condition     = [for b in google_container_cluster.this.master_authorized_networks_config[0].cidr_blocks : b.cidr_block] == ["198.51.100.0/24"]
    error_message = "control_plane_authorized_cidrs must reach master authorized networks."
  }
}

run "infrastructure_only" {
  command = plan

  variables {
    install_chart = false
  }

  assert {
    condition     = length(helm_release.igloo) == 0
    error_message = "install_chart = false must skip the Helm release."
  }

  assert {
    condition     = kubernetes_secret_v1.database.metadata[0].name == "igloo-database"
    error_message = "Secrets are still created for a later chart install."
  }

  assert {
    condition     = strcontains(output.next_steps, "install_chart = false")
    error_message = "Next steps must say the chart was not installed."
  }
}

run "single_replica" {
  command = plan

  variables {
    replica_count = 1
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).podDisruptionBudget.enabled == false
    error_message = "A single replica must not get a PodDisruptionBudget that blocks node drains."
  }
}
