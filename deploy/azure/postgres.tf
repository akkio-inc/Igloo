# One PostgreSQL database, "igloo", shared by Lakekeeper (schema public) and
# Igloo's user and role metadata (schema igloo). Reachable only from the VNet.

locals {
  database_name = "igloo"

  database_url = format(
    "postgresql://%s:%s@%s:5432/%s?sslmode=require",
    urlencode(var.postgres_admin_username),
    urlencode(random_password.postgres.result),
    azurerm_postgresql_flexible_server.this.fqdn,
    azurerm_postgresql_flexible_server_database.igloo.name,
  )
}

# Letters and digits only, so the password never needs escaping in a URL.
resource "random_password" "postgres" {
  length      = 32
  special     = false
  min_upper   = 2
  min_lower   = 2
  min_numeric = 2
}

resource "azurerm_postgresql_flexible_server" "this" {
  name                = "${var.name}-pg-${local.suffix}"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  version             = "17"
  sku_name            = var.postgres_sku_name
  storage_mb          = var.postgres_storage_mb
  auto_grow_enabled   = true
  tags                = local.tags

  backup_retention_days        = var.postgres_backup_retention_days
  geo_redundant_backup_enabled = var.postgres_geo_redundant_backup_enabled

  # Private access (VNet integration): the server gets an address in the
  # delegated subnet and a name in the private DNS zone, and nothing else.
  delegated_subnet_id           = azurerm_subnet.postgres.id
  private_dns_zone_id           = azurerm_private_dns_zone.postgres.id
  public_network_access_enabled = false

  administrator_login    = var.postgres_admin_username
  administrator_password = random_password.postgres.result

  authentication {
    password_auth_enabled         = true
    active_directory_auth_enabled = false
  }

  dynamic "high_availability" {
    for_each = var.postgres_high_availability ? [1] : []
    content {
      mode = "ZoneRedundant"
    }
  }

  lifecycle {
    # Azure picks the zones; after a failover the primary and standby swap.
    ignore_changes = [zone, high_availability[0].standby_availability_zone]
  }

  depends_on = [azurerm_private_dns_zone_virtual_network_link.postgres]
}

# Azure only lets users create extensions that are on this allow list.
# Lakekeeper's migrations create these five.
resource "azurerm_postgresql_flexible_server_configuration" "extensions" {
  name      = "azure.extensions"
  server_id = azurerm_postgresql_flexible_server.this.id
  value     = "BTREE_GIN,BTREE_GIST,PG_TRGM,PGCRYPTO,UUID-OSSP"
}

# Clients must use TLS. This is the Azure default; it is pinned here so it
# cannot be switched off unnoticed.
resource "azurerm_postgresql_flexible_server_configuration" "require_tls" {
  name      = "require_secure_transport"
  server_id = azurerm_postgresql_flexible_server.this.id
  value     = "on"

  # Azure rejects concurrent operations on one server; go one at a time.
  depends_on = [azurerm_postgresql_flexible_server_configuration.extensions]
}

# Connection and checkpoint logging, for auditing and troubleshooting. The
# logs stay in the server's own log files unless diagnostic settings export them.
resource "azurerm_postgresql_flexible_server_configuration" "log_connections" {
  name      = "log_connections"
  server_id = azurerm_postgresql_flexible_server.this.id
  value     = "on"

  depends_on = [azurerm_postgresql_flexible_server_configuration.require_tls]
}

resource "azurerm_postgresql_flexible_server_configuration" "log_checkpoints" {
  name      = "log_checkpoints"
  server_id = azurerm_postgresql_flexible_server.this.id
  value     = "on"

  depends_on = [azurerm_postgresql_flexible_server_configuration.log_connections]
}

resource "azurerm_postgresql_flexible_server_database" "igloo" {
  name      = local.database_name
  server_id = azurerm_postgresql_flexible_server.this.id
  charset   = "UTF8"
  collation = "en_US.utf8"

  depends_on = [azurerm_postgresql_flexible_server_configuration.log_checkpoints]
}
