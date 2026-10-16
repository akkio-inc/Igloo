# Control-plane identity. It is created before the cluster so its network
# permissions are in place when AKS first joins nodes to the subnet.
resource "azurerm_user_assigned_identity" "aks" {
  name                = "${var.name}-aks"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  tags                = local.tags
}

# AKS needs to join nodes to the subnet and, when SQL is exposed, attach the
# static public IP to its load balancer. The Azure cloud provider looks the IP
# up by listing the resource group's public IPs, so the role is scoped to this
# resource group, as the AKS documentation prescribes. It grants nothing on
# storage or databases.
resource "azurerm_role_assignment" "aks_network" {
  scope                = azurerm_resource_group.this.id
  role_definition_name = "Network Contributor"
  principal_id         = azurerm_user_assigned_identity.aks.principal_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_kubernetes_cluster" "this" {
  name                = "${var.name}-aks"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  node_resource_group = "${local.resource_group_name}-aks-nodes"
  dns_prefix          = var.name
  kubernetes_version  = var.kubernetes_version
  sku_tier            = var.aks_sku_tier
  tags                = local.tags

  automatic_upgrade_channel = "patch"
  node_os_upgrade_channel   = "NodeImage"

  # Pods get Azure tokens through federated service-account tokens; no
  # secrets for Azure access exist anywhere in the cluster.
  oidc_issuer_enabled       = true
  workload_identity_enabled = true

  role_based_access_control_enabled = true
  image_cleaner_enabled             = true
  image_cleaner_interval_hours      = 48

  default_node_pool {
    name                        = "system"
    temporary_name_for_rotation = "systemtmp"
    vm_size                     = var.system_node_vm_size
    vnet_subnet_id              = azurerm_subnet.aks.id
    zones                       = local.zones
    auto_scaling_enabled        = true
    min_count                   = var.system_node_min_count
    max_count                   = var.system_node_max_count
    max_pods                    = 110
    os_disk_size_gb             = 64
    tags                        = local.tags

    upgrade_settings {
      max_surge = "10%"
    }
  }

  # Node auto-provisioning stays off: the cluster autoscaler scales the
  # node pools defined here, and nothing else creates nodes.
  node_provisioning_profile {
    mode = "Manual"
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.aks.id]
  }

  # Azure CNI Overlay with the Cilium dataplane, which enforces the chart's
  # NetworkPolicies. Nodes take addresses from the subnet; pods from aks_pod_cidr.
  network_profile {
    network_plugin      = "azure"
    network_plugin_mode = "overlay"
    network_data_plane  = "cilium"
    network_policy      = "cilium"
    load_balancer_sku   = "standard"
    outbound_type       = "loadBalancer"
    pod_cidr            = var.aks_pod_cidr
    service_cidr        = var.aks_service_cidr
    dns_service_ip      = cidrhost(var.aks_service_cidr, 10)
  }

  dynamic "api_server_access_profile" {
    for_each = length(var.api_server_authorized_ip_ranges) > 0 ? [1] : []
    content {
      authorized_ip_ranges = var.api_server_authorized_ip_ranges
    }
  }

  dynamic "azure_active_directory_role_based_access_control" {
    for_each = length(var.admin_group_object_ids) > 0 ? [1] : []
    content {
      azure_rbac_enabled     = true
      admin_group_object_ids = var.admin_group_object_ids
    }
  }

  lifecycle {
    # The cluster autoscaler owns node counts.
    ignore_changes = [default_node_pool[0].node_count]
  }

  depends_on = [azurerm_role_assignment.aks_network]
}

# Igloo's query engines run here. Memory-optimized Arm64 nodes by default;
# optionally Spot. AKS adds the Spot label and taint itself; they are repeated
# below so plans stay clean.
resource "azurerm_kubernetes_cluster_node_pool" "igloo" {
  # Switching spot_enabled replaces the pool (Azure cannot change priority in
  # place); the name changes with it.
  name                        = var.spot_enabled ? "iglspot" : "igloo"
  temporary_name_for_rotation = "iglootmp"
  kubernetes_cluster_id       = azurerm_kubernetes_cluster.this.id
  mode                        = "User"
  vm_size                     = var.igloo_node_vm_size
  vnet_subnet_id              = azurerm_subnet.aks.id
  zones                       = local.zones
  auto_scaling_enabled        = true
  min_count                   = var.igloo_node_min_count
  max_count                   = var.igloo_node_max_count
  max_pods                    = 110
  os_disk_size_gb             = var.igloo_node_os_disk_size_gb
  tags                        = local.tags

  node_labels = merge(local.igloo_node_selector, var.spot_enabled ? local.spot_node_label : {})
  node_taints = var.spot_enabled ? ["${local.spot_key}=spot:NoSchedule"] : []

  priority        = var.spot_enabled ? "Spot" : "Regular"
  eviction_policy = var.spot_enabled ? "Delete" : null
  spot_max_price  = var.spot_enabled ? var.spot_max_price : null

  # Surge upgrades are not available for Spot pools.
  dynamic "upgrade_settings" {
    for_each = var.spot_enabled ? [] : [1]
    content {
      max_surge = "10%"
    }
  }

  lifecycle {
    ignore_changes = [node_count]
  }
}

locals {
  spot_key        = "kubernetes.azure.com/scalesetpriority"
  spot_node_label = { (local.spot_key) = "spot" }

  # Igloo pods are pinned to the Igloo pool by this label.
  igloo_node_selector = { "igloo/pool" = "query" }
}
