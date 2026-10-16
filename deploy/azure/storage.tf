# ADLS Gen2 storage for table data. Igloo (DuckDB) and Lakekeeper reach it
# with their own Entra ID workload identities, so account keys are disabled
# and the firewall admits only the AKS subnet (plus storage_allowed_ip_ranges).
resource "azurerm_storage_account" "this" {
  name                     = local.storage_account_name
  location                 = azurerm_resource_group.this.location
  resource_group_name      = azurerm_resource_group.this.name
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = var.storage_replication_type
  access_tier              = "Hot"
  is_hns_enabled           = true
  tags                     = local.tags

  min_tls_version                   = "TLS1_2"
  https_traffic_only_enabled        = true
  allow_nested_items_to_be_public   = false
  shared_access_key_enabled         = var.storage_shared_key_enabled
  default_to_oauth_authentication   = true
  cross_tenant_replication_enabled  = false
  infrastructure_encryption_enabled = true
  local_user_enabled                = false
  sftp_enabled                      = false
  nfsv3_enabled                     = false

  # The public endpoint stays enabled but only answers the networks below.
  public_network_access = "Enabled"

  network_rules {
    default_action             = "Deny"
    bypass                     = ["AzureServices"]
    virtual_network_subnet_ids = [azurerm_subnet.aks.id]
    ip_rules                   = var.storage_allowed_ip_ranges
  }

  dynamic "blob_properties" {
    for_each = var.storage_soft_delete_retention_days > 0 ? [1] : []
    content {
      delete_retention_policy {
        days = var.storage_soft_delete_retention_days
      }
      container_delete_retention_policy {
        days = var.storage_soft_delete_retention_days
      }
    }
  }
}

# The ADLS Gen2 filesystem. With hierarchical namespace enabled a blob
# container is a filesystem; creating it by storage_account_id goes through
# Azure Resource Manager, so it works with keys disabled and the firewall closed.
resource "azurerm_storage_container" "warehouse" {
  name                  = var.filesystem_name
  storage_account_id    = azurerm_storage_account.this.id
  container_access_type = "private"
}
