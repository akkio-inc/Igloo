output "project_id" {
  description = "Project the deployment lives in."
  value       = var.project_id
}

output "region" {
  description = "Region of the cluster, database and bucket."
  value       = var.region
}

output "cluster_name" {
  description = "GKE Autopilot cluster name."
  value       = google_container_cluster.this.name
}

output "get_credentials_command" {
  description = "Configures kubectl for the cluster through its IAM-authorized DNS endpoint."
  value       = local.get_credentials_command
}

output "bucket_name" {
  description = "GCS bucket that holds table data."
  value       = google_storage_bucket.warehouse.name
}

output "warehouse_location" {
  description = "Iceberg warehouse location (IGLOO_WAREHOUSE_LOCATION)."
  value       = local.warehouse_location
}

output "database_instance" {
  description = "Cloud SQL instance name."
  value       = google_sql_database_instance.this.name
}

output "database_private_ip" {
  description = "Private IP of the Cloud SQL instance (reachable only from the VPC)."
  value       = google_sql_database_instance.this.private_ip_address
}

output "namespace" {
  description = "Kubernetes namespace Igloo runs in."
  value       = kubernetes_namespace_v1.igloo.metadata[0].name
}

output "igloo_service_account" {
  description = "Google service account of the igloo pods (owner of the HMAC key)."
  value       = google_service_account.igloo.email
}

output "catalog_service_account" {
  description = "Google service account of the Lakekeeper catalog (Workload Identity)."
  value       = google_service_account.catalog.email
}

output "admin_user" {
  description = "Igloo administrator user name."
  value       = var.admin_user
}

output "admin_password_secret" {
  description = "Kubernetes Secret (key \"password\") with the administrator password."
  value       = kubernetes_secret_v1.admin.metadata[0].name
}

output "admin_password_command" {
  description = "Prints the administrator password."
  value       = local.admin_password_command
}

output "sql_endpoint" {
  description = "Public IP of the SQL load balancer when expose_sql is true; null otherwise."
  value       = local.sql_endpoint
}

locals {
  sql_endpoint = try(data.kubernetes_service_v1.sql[0].status[0].load_balancer[0].ingress[0].ip, null)

  get_credentials_command = "gcloud container clusters get-credentials ${google_container_cluster.this.name} --region ${var.region} --project ${var.project_id} --dns-endpoint"
  admin_password_command  = "kubectl -n ${var.namespace} get secret ${local.secret_names.admin} -o jsonpath='{.data.password}' | base64 --decode"

  connect_steps = var.expose_sql ? join("\n", [
    "     psql \"host=${coalesce(local.sql_endpoint, "<sql_endpoint output>")} port=5432 user=${var.admin_user} dbname=igloo${local.tls_enabled ? " sslmode=require" : ""}\"",
    "   Only clients in ${join(", ", var.sql_allowed_cidrs)} can reach it.",
    ]) : join("\n", [
    "     kubectl -n ${var.namespace} port-forward svc/${local.service_name} 5432:5432",
    "     psql \"host=127.0.0.1 port=5432 user=${var.admin_user} dbname=igloo\"",
  ])

  chart_note = var.install_chart ? "" : <<-EOT

    The chart was not installed (install_chart = false). Set install_chart = true
    and apply again once deploy/helm/igloo is available.
  EOT
}

output "next_steps" {
  description = "How to connect."
  value       = <<-EOT
    1. Point kubectl at the cluster:
         ${local.get_credentials_command}
    2. Read the administrator password:
         export PGPASSWORD="$(${local.admin_password_command})"
    3. Connect:
    ${local.connect_steps}
    ${local.chart_note}
  EOT
}
