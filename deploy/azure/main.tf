locals {
  resource_group_name = coalesce(var.resource_group_name, "${var.name}-rg")

  tags = merge({ "app" = "igloo", "managed-by" = "opentofu" }, var.tags)

  zones = length(var.availability_zones) > 0 ? var.availability_zones : null

  aks_subnet_cidr      = cidrsubnet(var.vnet_cidr, 4, 0)
  postgres_subnet_cidr = cidrsubnet(var.vnet_cidr, 8, 16)

  # Names that must be globally unique get a random suffix.
  suffix               = random_string.suffix.result
  storage_account_name = coalesce(var.storage_account_name, substr("${replace(var.name, "-", "")}${local.suffix}", 0, 24))

  warehouse_location = "abfss://${azurerm_storage_container.warehouse.name}@${azurerm_storage_account.this.name}.dfs.core.windows.net/warehouse"
}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "azurerm_resource_group" "this" {
  name     = local.resource_group_name
  location = var.location
  tags     = local.tags
}

# ---------------------------------------------------------------------------
# Network: one VNet with a subnet for AKS nodes and a delegated subnet for
# PostgreSQL, which is reachable only from inside the VNet.
# ---------------------------------------------------------------------------

resource "azurerm_virtual_network" "this" {
  name                = "${var.name}-vnet"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  address_space       = [var.vnet_cidr]
  tags                = local.tags
}

resource "azurerm_subnet" "aks" {
  name                 = "aks-nodes"
  resource_group_name  = azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = [local.aks_subnet_cidr]

  # Nodes reach the storage account through this service endpoint; the
  # account's firewall admits this subnet and nothing else by default.
  service_endpoints = ["Microsoft.Storage"]

  # Outbound traffic leaves through the AKS load balancer, never through
  # Azure's implicit default outbound address.
  default_outbound_access_enabled = false
}

resource "azurerm_subnet" "postgres" {
  name                 = "postgres"
  resource_group_name  = azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = [local.postgres_subnet_cidr]

  delegation {
    name = "postgres-flexible-server"
    service_delegation {
      name    = "Microsoft.DBforPostgreSQL/flexibleServers"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

resource "azurerm_private_dns_zone" "postgres" {
  name                = "${var.name}.private.postgres.database.azure.com"
  resource_group_name = azurerm_resource_group.this.name
  tags                = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "postgres" {
  name                  = "${var.name}-vnet"
  resource_group_name   = azurerm_resource_group.this.name
  private_dns_zone_name = azurerm_private_dns_zone.postgres.name
  virtual_network_id    = azurerm_virtual_network.this.id
  registration_enabled  = false
  tags                  = local.tags
}

# Static address for the SQL endpoint, so it survives chart reinstalls. AKS
# attaches it to its load balancer (see the Service annotations in helm.tf).
resource "azurerm_public_ip" "sql" {
  count = var.expose_sql ? 1 : 0

  name                = "${var.name}-sql"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  allocation_method   = "Static"
  sku                 = "Standard"
  zones               = local.zones
  domain_name_label   = var.sql_dns_label
  tags                = local.tags
}
