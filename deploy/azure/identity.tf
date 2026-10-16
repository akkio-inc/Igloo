# Two workload identities, one per Kubernetes service account:
#   igloo   - DuckDB reads and writes table data with it.
#   catalog - Lakekeeper validates the warehouse with it and vends
#             short-lived SAS tokens to query engines.

locals {
  oidc_audience = ["api://AzureADTokenExchange"]

  igloo_subject   = "system:serviceaccount:${var.namespace}:${var.igloo_service_account_name}"
  catalog_subject = "system:serviceaccount:${var.namespace}:${var.catalog_service_account_name}"

  # Data-plane roles are scoped to the filesystem, not the account.
  filesystem_scope = "${azurerm_storage_account.this.id}/blobServices/default/containers/${azurerm_storage_container.warehouse.name}"
}

resource "azurerm_user_assigned_identity" "igloo" {
  name                = "${var.name}-igloo"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  tags                = local.tags
}

resource "azurerm_user_assigned_identity" "catalog" {
  name                = "${var.name}-catalog"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  tags                = local.tags
}

resource "azurerm_federated_identity_credential" "igloo" {
  name                      = "aks-${var.namespace}-${var.igloo_service_account_name}"
  user_assigned_identity_id = azurerm_user_assigned_identity.igloo.id
  issuer                    = azurerm_kubernetes_cluster.this.oidc_issuer_url
  audience                  = local.oidc_audience
  subject                   = local.igloo_subject
}

resource "azurerm_federated_identity_credential" "catalog" {
  name                      = "aks-${var.namespace}-${var.catalog_service_account_name}"
  user_assigned_identity_id = azurerm_user_assigned_identity.catalog.id
  issuer                    = azurerm_kubernetes_cluster.this.oidc_issuer_url
  audience                  = local.oidc_audience
  subject                   = local.catalog_subject
}

resource "azurerm_role_assignment" "igloo_storage" {
  scope                = local.filesystem_scope
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.igloo.principal_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_role_assignment" "catalog_storage" {
  scope                = local.filesystem_scope
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.catalog.principal_id
  principal_type       = "ServicePrincipal"
}

# Lakekeeper vends user-delegation SAS tokens, which requires requesting a
# user delegation key. Azure only accepts that permission at storage account
# scope or above. It grants no data access by itself: a SAS can never exceed
# the signer's filesystem-scoped role above.
resource "azurerm_role_assignment" "catalog_delegator" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Delegator"
  principal_id         = azurerm_user_assigned_identity.catalog.principal_id
  principal_type       = "ServicePrincipal"
}

# New federated credentials and role assignments take a short while to reach
# every Entra ID and Azure Storage front end. Without this pause, the first
# token exchange or storage request from a new pod can fail.
resource "time_sleep" "identity_propagation" {
  create_duration = "60s"

  triggers = {
    igloo_credential   = azurerm_federated_identity_credential.igloo.id
    catalog_credential = azurerm_federated_identity_credential.catalog.id
    igloo_storage      = azurerm_role_assignment.igloo_storage.id
    catalog_storage    = azurerm_role_assignment.catalog_storage.id
    catalog_delegator  = azurerm_role_assignment.catalog_delegator.id
  }
}
