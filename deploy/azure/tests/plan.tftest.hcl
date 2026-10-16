# Plan-only tests with every provider mocked: no Azure or Kubernetes access
# and no credentials needed. From deploy/azure:
#   tofu init -backend=false && tofu test

mock_provider "azurerm" {
  # Mocked providers still run azurerm's own argument validation, which parses
  # Azure resource IDs. Every ID that feeds another resource therefore gets a
  # realistic shape; client IDs and the kubeconfig get fixed values so chart
  # values and provider configuration are known at plan time.
  mock_resource "azurerm_resource_group" {
    defaults = {
      id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/igloo-rg"
    }
  }

  mock_resource "azurerm_virtual_network" {
    defaults = {
      id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/igloo-rg/providers/Microsoft.Network/virtualNetworks/igloo-vnet"
    }
  }

  mock_resource "azurerm_subnet" {
    defaults = {
      id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/igloo-rg/providers/Microsoft.Network/virtualNetworks/igloo-vnet/subnets/aks-nodes"
    }
  }

  mock_resource "azurerm_private_dns_zone" {
    defaults = {
      id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/igloo-rg/providers/Microsoft.Network/privateDnsZones/igloo.private.postgres.database.azure.com"
    }
  }

  mock_resource "azurerm_public_ip" {
    defaults = {
      id         = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/igloo-rg/providers/Microsoft.Network/publicIPAddresses/igloo-sql"
      ip_address = "198.51.100.10"
      fqdn       = ""
    }
  }

  mock_resource "azurerm_kubernetes_cluster" {
    defaults = {
      id              = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/igloo-rg/providers/Microsoft.ContainerService/managedClusters/igloo-aks"
      oidc_issuer_url = "https://eastus2.oic.prod-aks.azure.com/00000000-0000-0000-0000-000000000000/11111111-1111-1111-1111-111111111111/"
      kube_config = [{
        host                   = "https://igloo-aks.hcp.eastus2.azmk8s.io:443"
        client_certificate     = "Y2VydA=="
        client_key             = "a2V5"
        cluster_ca_certificate = "Y2E="
        username               = "clusterUser"
        password               = ""
      }]
      kube_admin_config = [{
        host                   = "https://igloo-aks.hcp.eastus2.azmk8s.io:443"
        client_certificate     = "Y2VydA=="
        client_key             = "a2V5"
        cluster_ca_certificate = "Y2E="
        username               = "clusterAdmin"
        password               = ""
      }]
    }
  }

  mock_resource "azurerm_storage_account" {
    defaults = {
      id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/igloo-rg/providers/Microsoft.Storage/storageAccounts/igloot3st00"
    }
  }

  mock_resource "azurerm_user_assigned_identity" {
    defaults = {
      id           = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/igloo-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/igloo-igloo"
      client_id    = "22222222-2222-2222-2222-222222222222"
      principal_id = "33333333-3333-3333-3333-333333333333"
    }
  }

  mock_resource "azurerm_postgresql_flexible_server" {
    defaults = {
      id   = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/igloo-rg/providers/Microsoft.DBforPostgreSQL/flexibleServers/igloo-pg-t3st00"
      fqdn = "igloo-pg-t3st00.igloo.private.postgres.database.azure.com"
    }
  }
}

mock_provider "kubernetes" {}
mock_provider "helm" {}
mock_provider "time" {}

mock_provider "random" {
  mock_resource "random_string" {
    defaults = {
      result = "t3st00"
    }
  }
}

# The two subnets share the mock default above; give PostgreSQL its own ID.
override_resource {
  target = azurerm_subnet.postgres
  values = {
    id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/igloo-rg/providers/Microsoft.Network/virtualNetworks/igloo-vnet/subnets/postgres"
  }
}

# Distinct identity values, to prove each service account gets its own identity.
override_resource {
  target = azurerm_user_assigned_identity.catalog
  values = {
    id           = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/igloo-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/igloo-catalog"
    client_id    = "44444444-4444-4444-4444-444444444444"
    principal_id = "55555555-5555-5555-5555-555555555555"
  }
}

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

run "storage_is_adls_gen2_and_locked_down" {
  command = plan

  assert {
    condition     = azurerm_storage_account.this.name == "igloot3st00"
    error_message = "storage account name should be derived from name and the random suffix"
  }

  assert {
    condition     = azurerm_storage_account.this.is_hns_enabled == true && azurerm_storage_account.this.account_kind == "StorageV2"
    error_message = "storage account must be StorageV2 with hierarchical namespace (ADLS Gen2)"
  }

  assert {
    condition     = azurerm_storage_account.this.min_tls_version == "TLS1_2" && azurerm_storage_account.this.https_traffic_only_enabled == true
    error_message = "storage account must require HTTPS with TLS 1.2"
  }

  assert {
    condition     = azurerm_storage_account.this.allow_nested_items_to_be_public == false
    error_message = "public blob access must be disabled"
  }

  assert {
    condition     = azurerm_storage_account.this.shared_access_key_enabled == false
    error_message = "shared key access must be disabled by default"
  }

  assert {
    condition     = azurerm_storage_account.this.network_rules[0].default_action == "Deny"
    error_message = "storage firewall must deny by default"
  }

  assert {
    condition = (
      length(azurerm_subnet.aks.service_endpoint) == 1 &&
      azurerm_subnet.aks.service_endpoint[0].service == "Microsoft.Storage" &&
      azurerm_storage_account.this.network_rules[0].virtual_network_subnet_ids == toset([azurerm_subnet.aks.id])
    )
    error_message = "the storage firewall must admit the AKS subnet, which reaches storage through a Microsoft.Storage service endpoint"
  }

  assert {
    condition     = azurerm_storage_container.warehouse.container_access_type == "private"
    error_message = "the filesystem must be private"
  }

  assert {
    condition     = output.warehouse_location == "abfss://igloo@igloot3st00.dfs.core.windows.net/warehouse"
    error_message = "unexpected warehouse location: ${output.warehouse_location}"
  }
}

run "postgres_is_private" {
  command = plan

  assert {
    condition     = azurerm_postgresql_flexible_server.this.public_network_access_enabled == false
    error_message = "PostgreSQL must not be publicly accessible"
  }

  assert {
    condition     = azurerm_postgresql_flexible_server.this.version == "17"
    error_message = "PostgreSQL must be version 17"
  }

  assert {
    condition     = endswith(azurerm_postgresql_flexible_server.this.delegated_subnet_id, "/subnets/postgres")
    error_message = "PostgreSQL must sit in the delegated subnet"
  }

  assert {
    condition = (
      azurerm_private_dns_zone_virtual_network_link.postgres.private_dns_zone_id == azurerm_private_dns_zone.postgres.id &&
      azurerm_private_dns_zone_virtual_network_link.postgres.virtual_network_id == azurerm_virtual_network.this.id &&
      azurerm_postgresql_flexible_server.this.private_dns_zone_id == azurerm_private_dns_zone.postgres.id
    )
    error_message = "PostgreSQL's private DNS zone must be linked to the VNet"
  }

  assert {
    condition     = length(azurerm_postgresql_flexible_server.this.high_availability) == 0
    error_message = "high availability must be off by default"
  }

  assert {
    condition     = azurerm_postgresql_flexible_server_configuration.require_tls.value == "on"
    error_message = "PostgreSQL must require TLS"
  }

  assert {
    condition     = alltrue([for e in ["BTREE_GIN", "BTREE_GIST", "PG_TRGM", "PGCRYPTO", "UUID-OSSP"] : contains(split(",", azurerm_postgresql_flexible_server_configuration.extensions.value), e)])
    error_message = "every extension Lakekeeper needs must be allow-listed"
  }

  assert {
    condition     = nonsensitive(startswith(kubernetes_secret_v1.database.data["url"], "postgresql://iglooadmin:") && endswith(kubernetes_secret_v1.database.data["url"], "@igloo-pg-t3st00.igloo.private.postgres.database.azure.com:5432/igloo?sslmode=require"))
    error_message = "database URL must be postgresql://user:pass@host:5432/igloo?sslmode=require"
  }
}

run "workload_identity_bindings" {
  command = plan

  assert {
    condition     = azurerm_kubernetes_cluster.this.oidc_issuer_enabled && azurerm_kubernetes_cluster.this.workload_identity_enabled
    error_message = "AKS must have the OIDC issuer and workload identity enabled"
  }

  assert {
    condition     = azurerm_federated_identity_credential.igloo.subject == "system:serviceaccount:igloo:igloo"
    error_message = "igloo federated credential has subject ${azurerm_federated_identity_credential.igloo.subject}"
  }

  assert {
    condition     = azurerm_federated_identity_credential.catalog.subject == "system:serviceaccount:igloo:igloo-catalog"
    error_message = "catalog federated credential has subject ${azurerm_federated_identity_credential.catalog.subject}"
  }

  assert {
    condition = (
      azurerm_federated_identity_credential.igloo.issuer == azurerm_kubernetes_cluster.this.oidc_issuer_url &&
      azurerm_federated_identity_credential.catalog.issuer == azurerm_kubernetes_cluster.this.oidc_issuer_url &&
      azurerm_federated_identity_credential.igloo.audience == tolist(["api://AzureADTokenExchange"])
    )
    error_message = "federated credentials must trust the cluster's OIDC issuer with the Entra token-exchange audience"
  }

  assert {
    condition     = endswith(azurerm_federated_identity_credential.catalog.user_assigned_identity_id, "/igloo-catalog")
    error_message = "the catalog credential must belong to the catalog identity"
  }

  assert {
    condition = (
      azurerm_role_assignment.igloo_storage.role_definition_name == "Storage Blob Data Contributor" &&
      azurerm_role_assignment.catalog_storage.role_definition_name == "Storage Blob Data Contributor" &&
      endswith(azurerm_role_assignment.igloo_storage.scope, "/storageAccounts/igloot3st00/blobServices/default/containers/igloo") &&
      azurerm_role_assignment.catalog_storage.scope == azurerm_role_assignment.igloo_storage.scope
    )
    error_message = "both identities need Storage Blob Data Contributor scoped to the filesystem"
  }

  assert {
    condition     = azurerm_role_assignment.catalog_storage.principal_id == "55555555-5555-5555-5555-555555555555"
    error_message = "the catalog role assignment must target the catalog identity"
  }
}

run "chart_values_for_azure" {
  command = plan

  variables {
    image_tag = "0.1.0"
  }

  assert {
    condition     = helm_release.igloo[0].chart == "./../helm/igloo" && helm_release.igloo[0].namespace == "igloo"
    error_message = "the chart must come from deploy/helm/igloo and install into the igloo namespace"
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).storage.warehouseLocation == "abfss://igloo@igloot3st00.dfs.core.windows.net/warehouse"
    error_message = "storage.warehouseLocation must be the abfss:// warehouse URL"
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).storage.azure.existingSecret == ""
    error_message = "no account key: DuckDB must use workload identity"
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).catalog.systemCredentials == "azure"
    error_message = "catalog.systemCredentials must be azure"
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).podLabels["azure.workload.identity/use"] == "true" &&
      yamldecode(helm_release.igloo[0].values[0]).catalog.podLabels["azure.workload.identity/use"] == "true"
    )
    error_message = "Igloo and catalog pods must carry the workload identity label"
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).serviceAccount.name == "igloo" &&
      yamldecode(helm_release.igloo[0].values[0]).serviceAccount.annotations["azure.workload.identity/client-id"] == "22222222-2222-2222-2222-222222222222" &&
      yamldecode(helm_release.igloo[0].values[0]).catalog.serviceAccount.name == "igloo-catalog" &&
      yamldecode(helm_release.igloo[0].values[0]).catalog.serviceAccount.annotations["azure.workload.identity/client-id"] == "44444444-4444-4444-4444-444444444444"
    )
    error_message = "each service account must name the federated subject and carry its own identity's client ID"
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).admin.existingSecret == "igloo-admin" &&
      yamldecode(helm_release.igloo[0].values[0]).metadataDatabase.existingSecret == "igloo-database" &&
      yamldecode(helm_release.igloo[0].values[0]).catalog.database.existingSecret == "igloo-database" &&
      yamldecode(helm_release.igloo[0].values[0]).catalog.encryptionKey.existingSecret == "igloo-catalog-encryption"
    )
    error_message = "the chart must read every credential from the module's Secrets"
  }

  assert {
    condition     = kubernetes_secret_v1.admin.metadata[0].name == "igloo-admin" && kubernetes_secret_v1.catalog_encryption.metadata[0].name == "igloo-catalog-encryption"
    error_message = "Secret names must match the chart values"
  }

  assert {
    condition     = !strcontains(helm_release.igloo[0].values[0], "postgresql://")
    error_message = "chart values must not contain the database URL"
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).image.tag == "0.1.0" &&
      yamldecode(helm_release.igloo[0].values[0]).replicaCount == 2 &&
      yamldecode(helm_release.igloo[0].values[0]).resources.requests.memory == "24Gi" &&
      yamldecode(helm_release.igloo[0].values[0]).resources.limits.memory == "24Gi"
    )
    error_message = "image tag, replicas and sizing must be passed through"
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).nodeSelector == { "igloo/pool" = "query" } &&
      length(yamldecode(helm_release.igloo[0].values[0]).tolerations) == 0 &&
      azurerm_kubernetes_cluster_node_pool.igloo.node_labels["igloo/pool"] == "query" &&
      azurerm_kubernetes_cluster_node_pool.igloo.priority == "Regular"
    )
    error_message = "Igloo pods must be pinned to the regular Igloo node pool"
  }
}

run "sql_not_exposed_by_default" {
  command = plan

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).service.type == "ClusterIP"
    error_message = "expose_sql = false must give a ClusterIP service"
  }

  assert {
    condition     = length(yamldecode(helm_release.igloo[0].values[0]).service.loadBalancerSourceRanges) == 0 && length(yamldecode(helm_release.igloo[0].values[0]).service.annotations) == 0
    error_message = "a ClusterIP service needs no load balancer settings"
  }

  assert {
    condition     = length(azurerm_public_ip.sql) == 0 && output.sql_endpoint_ip == null
    error_message = "no public IP without expose_sql"
  }
}

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------

run "sql_exposed_with_tls" {
  command = plan

  variables {
    expose_sql          = true
    sql_allowed_cidrs   = ["203.0.113.0/24", "198.51.100.7/32"]
    sql_dns_label       = "igloo-test"
    tls_certificate_pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
    tls_private_key_pem = "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----\n"
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).service.type == "LoadBalancer"
    error_message = "expose_sql = true must give a LoadBalancer service"
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).service.loadBalancerSourceRanges == ["203.0.113.0/24", "198.51.100.7/32"]
    error_message = "loadBalancerSourceRanges must equal sql_allowed_cidrs"
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).service.annotations["service.beta.kubernetes.io/azure-pip-name"] == "igloo-sql" &&
      yamldecode(helm_release.igloo[0].values[0]).service.annotations["service.beta.kubernetes.io/azure-load-balancer-resource-group"] == "igloo-rg"
    )
    error_message = "the Service must claim the module's static public IP"
  }

  assert {
    condition     = length(azurerm_public_ip.sql) == 1 && azurerm_public_ip.sql[0].domain_name_label == "igloo-test" && output.sql_endpoint_ip == "198.51.100.10"
    error_message = "a static public IP must be created and reported"
  }

  assert {
    condition     = yamldecode(helm_release.igloo[0].values[0]).tls.existingSecret == "igloo-tls" && kubernetes_secret_v1.tls[0].type == "kubernetes.io/tls"
    error_message = "the TLS certificate must reach the chart as a kubernetes.io/tls Secret"
  }
}

run "sql_exposed_without_tls_warns" {
  command = plan

  variables {
    expose_sql        = true
    sql_allowed_cidrs = ["203.0.113.0/24"]
  }

  expect_failures = [check.sql_endpoint_tls]
}

run "spot_node_pool" {
  command = plan

  variables {
    spot_enabled = true
  }

  assert {
    condition = (
      azurerm_kubernetes_cluster_node_pool.igloo.priority == "Spot" &&
      azurerm_kubernetes_cluster_node_pool.igloo.eviction_policy == "Delete" &&
      azurerm_kubernetes_cluster_node_pool.igloo.spot_max_price == -1 &&
      azurerm_kubernetes_cluster_node_pool.igloo.node_taints == tolist(["kubernetes.azure.com/scalesetpriority=spot:NoSchedule"])
    )
    error_message = "spot_enabled must make the Igloo pool a tainted Spot pool"
  }

  assert {
    condition = (
      yamldecode(helm_release.igloo[0].values[0]).tolerations == [{
        key      = "kubernetes.azure.com/scalesetpriority"
        operator = "Equal"
        value    = "spot"
        effect   = "NoSchedule"
      }] &&
      yamldecode(helm_release.igloo[0].values[0]).nodeSelector["kubernetes.azure.com/scalesetpriority"] == "spot" &&
      yamldecode(helm_release.igloo[0].values[0]).nodeSelector["igloo/pool"] == "query"
    )
    error_message = "Igloo pods must tolerate and select the Spot pool"
  }
}

run "infrastructure_only" {
  command = plan

  variables {
    install_chart = false
  }

  assert {
    condition     = length(helm_release.igloo) == 0
    error_message = "install_chart = false must skip the Helm release"
  }

  assert {
    condition     = kubernetes_namespace_v1.igloo.metadata[0].name == "igloo" && kubernetes_secret_v1.database.metadata[0].name == "igloo-database"
    error_message = "the namespace and Secrets are still created"
  }

  assert {
    condition     = yamldecode(output.helm_values).catalog.systemCredentials == "azure"
    error_message = "the helm_values output must hold the chart values"
  }
}

run "entra_id_and_private_api" {
  command = plan

  variables {
    admin_group_object_ids          = ["66666666-6666-6666-6666-666666666666"]
    api_server_authorized_ip_ranges = ["203.0.113.7/32"]
    availability_zones              = ["1", "2", "3"]
    postgres_sku_name               = "GP_Standard_D2ds_v5"
    postgres_high_availability      = true
  }

  assert {
    condition     = azurerm_kubernetes_cluster.this.azure_active_directory_role_based_access_control[0].azure_rbac_enabled
    error_message = "admin groups must enable Entra ID with Azure RBAC"
  }

  assert {
    condition     = azurerm_kubernetes_cluster.this.api_server_access_profile[0].authorized_ip_ranges == toset(["203.0.113.7/32"])
    error_message = "API server ranges must be applied"
  }

  assert {
    condition     = azurerm_kubernetes_cluster_node_pool.igloo.zones == toset(["1", "2", "3"])
    error_message = "zones must apply to the Igloo pool"
  }

  assert {
    condition     = azurerm_postgresql_flexible_server.this.high_availability[0].mode == "ZoneRedundant"
    error_message = "HA must be zone-redundant"
  }
}

# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

run "rejects_exposed_sql_without_cidrs" {
  command = plan

  variables {
    expose_sql = true
  }

  # No TLS either, so the TLS check also fires.
  expect_failures = [var.sql_allowed_cidrs, check.sql_endpoint_tls]
}

run "rejects_invalid_sql_cidr" {
  command = plan

  variables {
    sql_allowed_cidrs = ["everyone"]
  }

  expect_failures = [var.sql_allowed_cidrs]
}

run "rejects_bad_name" {
  command = plan

  variables {
    name = "Igloo_Prod"
  }

  expect_failures = [var.name]
}

run "rejects_ha_on_burstable" {
  command = plan

  variables {
    postgres_high_availability = true
  }

  expect_failures = [var.postgres_high_availability]
}

run "rejects_reserved_postgres_admin" {
  command = plan

  variables {
    postgres_admin_username = "postgres"
  }

  expect_failures = [var.postgres_admin_username]
}

run "rejects_bad_filesystem_name" {
  command = plan

  variables {
    filesystem_name = "My--Lake"
  }

  expect_failures = [var.filesystem_name]
}

run "rejects_single_address_cidr_in_storage_firewall" {
  command = plan

  variables {
    storage_allowed_ip_ranges = ["203.0.113.7/32"]
  }

  expect_failures = [var.storage_allowed_ip_ranges]
}

run "rejects_small_vnet" {
  command = plan

  variables {
    vnet_cidr = "10.42.0.0/24"
  }

  expect_failures = [var.vnet_cidr]
}

run "rejects_inverted_node_counts" {
  command = plan

  variables {
    igloo_node_min_count = 5
    igloo_node_max_count = 4
  }

  expect_failures = [var.igloo_node_max_count]
}

run "rejects_shared_service_account" {
  command = plan

  variables {
    catalog_service_account_name = "igloo"
  }

  expect_failures = [var.catalog_service_account_name]
}

run "rejects_half_tls_config" {
  command = plan

  variables {
    tls_certificate_pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
  }

  expect_failures = [var.tls_private_key_pem]
}

run "rejects_bad_memory_quantity" {
  command = plan

  variables {
    igloo_memory = "24GB"
  }

  expect_failures = [var.igloo_memory]
}
