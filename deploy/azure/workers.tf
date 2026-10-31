# Distributed-execution workers (docs/distributed-execution.md).
#
# With workers_enabled: a Spot node pool and a Regular one for workers, and
# with large_worker_enabled a Regular one (and a Spot one with
# large_worker_spot) for the large worker, all autoscaling from zero nodes.
# Both pools of a kind carry the same node label and taint, so the chart's
# worker pods may land on either; Spot pools also carry the taint AKS gives
# Spot nodes, which worker pods tolerate. The cluster autoscaler's priority
# expander (auto_scaler_profile in aks.tf, and the ConfigMap below) tries the
# Spot pools first; a Spot pool that gets no VMs, or none within
# max_node_provisioning_time, is backed off and the Regular pool gets the pod.

locals {
  worker_node_label = "igloo.dev/node"

  worker_kinds = {
    workers = {
      enabled  = var.workers_enabled
      spot     = var.workers_spot
      vm_size  = var.workers_vm_size
      max      = var.workers_max
      label    = "workers"
      taint    = "igloo.dev/workers"
      cpu      = var.workers_cpu
      memory   = var.workers_memory
      spill    = var.workers_spill_size
      cache    = var.workers_cache_size
      pool     = "worker"
      priority = "workerspot"
    }
    large = {
      enabled  = var.large_worker_enabled
      spot     = var.large_worker_spot
      vm_size  = var.large_worker_vm_size
      max      = 1
      label    = "large-worker"
      taint    = "igloo.dev/large-worker"
      cpu      = var.large_worker_cpu
      memory   = var.large_worker_memory
      spill    = var.large_worker_spill_size
      cache    = var.large_worker_cache_size
      pool     = "large"
      priority = "largespot"
    }
  }

  # Pool names are at most 12 lowercase letters and digits: workerspot,
  # workerreg, largespot, largereg.
  worker_pools = merge([
    for kind, k in local.worker_kinds : {
      for spot in(k.spot ? [true, false] : [false]) : "${k.pool}${spot ? "spot" : "reg"}" => {
        kind    = kind
        spot    = spot
        vm_size = k.vm_size
        max     = k.max
        # Sizes with a local disk get an ephemeral OS disk on it, as the
        # Igloo pool does (aks.tf), so spill and read cache are on local disk.
        os_disk_type = can(regex("^Standard_[A-Z]+[0-9]+[a-z]*d[a-z]*(_v[0-9]+)?$", k.vm_size)) ? "Ephemeral" : "Managed"
        labels       = merge({ (local.worker_node_label) = k.label }, spot ? local.spot_node_label : {})
        taints       = concat(["${k.taint}=true:NoSchedule"], spot ? ["${local.spot_key}=spot:NoSchedule"] : [])
      }
    } if k.enabled
  ]...)

  # The chart's values for each kind of worker: it selects its kind's label,
  # on both pools, and tolerates its taint and Spot's.
  worker_chart_values = {
    for kind, k in local.worker_kinds : kind => {
      resources = {
        requests = { cpu = k.cpu, memory = k.memory }
        limits   = { memory = k.memory }
      }
      duckdb       = { spillVolume = { sizeLimit = k.spill } }
      cacheVolume  = { sizeLimit = k.cache }
      nodeSelector = { (local.worker_node_label) = k.label }
      tolerations = concat(
        [{ key = k.taint, operator = "Equal", value = "true", effect = "NoSchedule" }],
        k.spot ? [{ key = local.spot_key, operator = "Equal", value = "spot", effect = "NoSchedule" }] : [],
      )
    }
  }

  # Merged into helm_values in helm.tf; nothing without workers_enabled, so
  # the chart's values stay as before.
  workers_helm_values = {
    for key, value in {
      workers = merge(local.worker_chart_values["workers"], {
        enabled      = true
        maxReplicas  = var.workers_max
        pricePerHour = var.workers_price_per_hour
        large = merge(
          { enabled = var.large_worker_enabled },
          { for k, v in merge(local.worker_chart_values["large"], { pricePerHour = var.large_worker_price_per_hour }) : k => v if var.large_worker_enabled },
        )
      })
    } : key => value if var.workers_enabled
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "worker" {
  for_each = local.worker_pools

  # Switching a pool between Spot and Regular replaces it under another name.
  name                        = each.key
  temporary_name_for_rotation = "${substr(each.key, 0, 9)}tmp"
  kubernetes_cluster_id       = azurerm_kubernetes_cluster.this.id
  mode                        = "User"
  vm_size                     = each.value.vm_size
  vnet_subnet_id              = azurerm_subnet.aks.id
  zones                       = local.zones
  auto_scaling_enabled        = true
  min_count                   = 0
  max_count                   = each.value.max
  max_pods                    = 110
  os_disk_type                = each.value.os_disk_type
  tags                        = local.tags

  node_labels = each.value.labels
  node_taints = each.value.taints

  priority        = each.value.spot ? "Spot" : "Regular"
  eviction_policy = each.value.spot ? "Delete" : null
  spot_max_price  = each.value.spot ? var.spot_max_price : null

  # Surge upgrades are not available for Spot pools.
  dynamic "upgrade_settings" {
    for_each = each.value.spot ? [] : [1]
    content {
      max_surge = "10%"
    }
  }

  lifecycle {
    ignore_changes = [node_count]
  }
}

# The priority expander's priorities, by node group (the pools' scale sets,
# aks-<pool>-<id>-vmss): Spot worker pools first, everything else after,
# so the system and Igloo pools still scale as before.
resource "kubernetes_config_map_v1" "cluster_autoscaler_priority_expander" {
  count = var.workers_enabled ? 1 : 0

  metadata {
    name      = "cluster-autoscaler-priority-expander"
    namespace = "kube-system"
  }

  data = {
    priorities = <<-EOT
      50:
        - .*${local.worker_kinds.workers.priority}.*
        - .*${local.worker_kinds.large.priority}.*
      10:
        - .*
    EOT
  }
}
