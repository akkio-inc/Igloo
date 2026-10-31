locals {
  kubeconfig_command     = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
  admin_password_command = "kubectl -n ${var.namespace} get secret ${kubernetes_secret_v1.admin.metadata[0].name} -o jsonpath='{.data.password}' | base64 -d"
  port_forward_command   = "kubectl -n ${var.namespace} port-forward svc/${local.sql_service_name} 5433:5432"

  sql_hostname = var.install_chart && var.expose_sql ? try(
    coalesce(
      data.kubernetes_service_v1.sql[0].status[0].load_balancer[0].ingress[0].hostname,
      data.kubernetes_service_v1.sql[0].status[0].load_balancer[0].ingress[0].ip,
    ),
    null,
  ) : null

  # Refers to the sql_endpoint output rather than embedding the hostname, so
  # this text is known at plan time.
  connect_step = var.expose_sql ? join("\n", [
    "3. Connect through the load balancer (TLS is on; the default certificate is self-signed):",
    "     psql \"host=$(tofu output -raw sql_endpoint) port=5432 user=${var.admin_user} dbname=igloo sslmode=require\"",
    "   A new load balancer's DNS name can take a few minutes to resolve.",
    ]) : join("\n", [
    "3. Forward the SQL port to your machine, then connect from another terminal:",
    "     ${local.port_forward_command}",
    "     psql \"host=127.0.0.1 port=5433 user=${var.admin_user} dbname=igloo\"",
  ])
}

output "region" {
  description = "AWS region."
  value       = var.region
}

output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "kubeconfig_command" {
  description = "Adds the cluster to your kubeconfig."
  value       = local.kubeconfig_command
}

output "node_group" {
  description = "Settings of the EKS managed node group: instance types, capacity type (ON_DEMAND or SPOT), AMI type, sizes, and local_storage: RAID0 when the instance store holds pods' emptyDir volumes (and so DuckDB's spill files), none when they are on the root volume."
  value       = local.node_group
}

output "worker_node_groups" {
  description = "With workers_enabled: the worker and large-worker node groups by name (workers-spot, workers-ondemand, large-worker-spot, large-worker-ondemand), each with its capacity type, instance types, sizes, node label, taint key and local_storage. Empty without workers."
  value       = local.worker_node_groups
}

output "vpc_id" {
  description = "ID of the VPC."
  value       = module.vpc.vpc_id
}

output "bucket_name" {
  description = "S3 bucket holding table data."
  value       = aws_s3_bucket.warehouse.bucket
}

output "warehouse_location" {
  description = "Iceberg warehouse location (IGLOO_WAREHOUSE_LOCATION)."
  value       = local.warehouse_location
}

output "database_endpoint" {
  description = "RDS PostgreSQL endpoint (host:port). Reachable only from inside the VPC."
  value       = aws_db_instance.this.endpoint
}

output "database_url_secret_name" {
  description = "Kubernetes Secret (key \"url\") with the PostgreSQL connection URL."
  value       = kubernetes_secret_v1.database.metadata[0].name
}

output "namespace" {
  description = "Kubernetes namespace Igloo runs in."
  value       = kubernetes_namespace_v1.igloo.metadata[0].name
}

output "igloo_role_arn" {
  description = "IAM role assumed by the igloo pods."
  value       = aws_iam_role.service["igloo"].arn
}

output "engines_role_arn" {
  description = "IAM role Igloo assumes to hand other engines credentials limited to one table."
  value       = aws_iam_role.engines.arn
}

output "catalog_role_arn" {
  description = "IAM role assumed by the Lakekeeper catalog pods."
  value       = aws_iam_role.service["catalog"].arn
}

output "admin_user" {
  description = "Igloo administrator user name."
  value       = var.admin_user
}

output "admin_password_secret_name" {
  description = "Kubernetes Secret (key \"password\") holding the administrator password."
  value       = kubernetes_secret_v1.admin.metadata[0].name
}

output "admin_password_command" {
  description = "Prints the administrator password."
  value       = local.admin_password_command
}

output "port_forward_command" {
  description = "Forwards local port 5433 to the Igloo SQL service."
  value       = local.port_forward_command
}

output "sql_endpoint" {
  description = "Load balancer hostname for SQL clients (port 5432), when expose_sql is true. Null otherwise."
  value       = local.sql_hostname
}

output "next_steps" {
  description = "How to reach Igloo after apply."
  value = join("\n", [
    "1. Point kubectl at the cluster:",
    "     ${local.kubeconfig_command}",
    "2. Read the administrator password:",
    "     ${local.admin_password_command}",
    local.connect_step,
    "",
  ])
}
