locals {
  get_credentials_command = "az aks get-credentials --resource-group ${azurerm_resource_group.this.name} --name ${azurerm_kubernetes_cluster.this.name} --overwrite-existing"

  admin_password_command = "kubectl --namespace ${var.namespace} get secret ${local.admin_secret_name} --output jsonpath='{.data.password}' | base64 --decode"

  sql_host = var.expose_sql ? coalesce(azurerm_public_ip.sql[0].fqdn, azurerm_public_ip.sql[0].ip_address) : "127.0.0.1"
  sql_port = var.expose_sql ? 5432 : 5433
}

output "resource_group_name" {
  description = "Resource group holding everything this module created (AKS keeps its VMs in <resource group>-aks-nodes)."
  value       = azurerm_resource_group.this.name
}

output "cluster_name" {
  description = "AKS cluster name."
  value       = azurerm_kubernetes_cluster.this.name
}

output "get_credentials_command" {
  description = "Command that writes a kubeconfig entry for the cluster."
  value       = local.get_credentials_command
}

output "storage_account_name" {
  description = "ADLS Gen2 storage account holding table data."
  value       = azurerm_storage_account.this.name
}

output "filesystem_name" {
  description = "ADLS Gen2 filesystem (blob container) holding table data."
  value       = azurerm_storage_container.warehouse.name
}

output "warehouse_location" {
  description = "Iceberg warehouse root (IGLOO_WAREHOUSE_LOCATION)."
  value       = local.warehouse_location
}

output "postgres_fqdn" {
  description = "Private host name of the PostgreSQL server; resolvable only inside the VNet."
  value       = azurerm_postgresql_flexible_server.this.fqdn
}

output "namespace" {
  description = "Kubernetes namespace of the Igloo release."
  value       = var.namespace
}

output "igloo_identity_client_id" {
  description = "Client ID of the managed identity Igloo pods use for storage."
  value       = azurerm_user_assigned_identity.igloo.client_id
}

output "catalog_identity_client_id" {
  description = "Client ID of the managed identity the Lakekeeper catalog uses for storage."
  value       = azurerm_user_assigned_identity.catalog.client_id
}

output "admin_user" {
  description = "Igloo administrator user name."
  value       = var.admin_user
}

output "admin_password_secret_name" {
  description = "Kubernetes Secret (key \"password\") holding the Igloo administrator password."
  value       = local.admin_secret_name
}

output "admin_password_command" {
  description = "Command that prints the Igloo administrator password."
  value       = local.admin_password_command
}

output "sql_endpoint_ip" {
  description = "Public IP of the SQL endpoint, or null when expose_sql is false."
  value       = var.expose_sql ? azurerm_public_ip.sql[0].ip_address : null
}

output "sql_endpoint_fqdn" {
  description = "DNS name of the SQL endpoint when sql_dns_label is set, otherwise null."
  value       = var.expose_sql && var.sql_dns_label != null ? azurerm_public_ip.sql[0].fqdn : null
}

output "helm_values" {
  description = "Values passed to the Igloo chart (no secrets; those are referenced by Secret name). Useful with install_chart = false."
  value       = yamlencode(local.helm_values)
}

output "next_steps" {
  description = "How to connect."
  value       = <<-EOT
    1. Point kubectl at the cluster:
         ${local.get_credentials_command}%{if length(var.admin_group_object_ids) > 0}
         kubelogin convert-kubeconfig -l azurecli%{endif}

    2. Read the administrator password:
         export PGPASSWORD="$(${local.admin_password_command})"
    %{if var.expose_sql}
    3. Connect (only from ${join(", ", var.sql_allowed_cidrs)}):
         psql "host=${local.sql_host} port=${local.sql_port} user=${var.admin_user} dbname=igloo%{if local.tls_enabled} sslmode=require%{endif}"
    %{else}
    3. Forward the SQL port (Igloo is not exposed outside the cluster) and connect:
         kubectl --namespace ${var.namespace} port-forward service/${var.release_name} ${local.sql_port}:5432
         psql "host=${local.sql_host} port=${local.sql_port} user=${var.admin_user} dbname=igloo"
    %{endif}
  EOT
}
