# The Igloo chart (deploy/helm/igloo): Igloo plus the bundled Lakekeeper
# catalog, wired to the Azure resources above. No secret values appear here;
# the chart reads them from the Secrets in kubernetes.tf.

locals {
  chart_path = var.chart_path != "" ? var.chart_path : "${path.module}/../helm/igloo"

  workload_identity_label = { "azure.workload.identity/use" = "true" }

  igloo_tolerations = var.spot_enabled ? [{
    key      = local.spot_key
    operator = "Equal"
    value    = "spot"
    effect   = "NoSchedule"
  }] : []

  sql_service_annotations = var.expose_sql ? {
    "service.beta.kubernetes.io/azure-pip-name"                     = azurerm_public_ip.sql[0].name
    "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.this.name
  } : {}

  helm_values = {
    image = merge({ tag = var.image_tag }, var.image_repository == "" ? {} : { repository = var.image_repository })

    replicaCount = var.replica_count

    admin = {
      user           = var.admin_user
      existingSecret = kubernetes_secret_v1.admin.metadata[0].name
    }

    metadataDatabase = {
      existingSecret = kubernetes_secret_v1.database.metadata[0].name
    }

    storage = {
      warehouseLocation = local.warehouse_location
      # No account key: DuckDB uses the pod's workload identity.
      azure = { existingSecret = "" }
    }

    duckdb = {
      spillVolume = { sizeLimit = var.igloo_spill_size }
    }

    tls = {
      existingSecret = local.tls_enabled ? kubernetes_secret_v1.tls[0].metadata[0].name : ""
    }

    serviceAccount = {
      create = true
      name   = var.igloo_service_account_name
      annotations = {
        "azure.workload.identity/client-id" = azurerm_user_assigned_identity.igloo.client_id
      }
    }

    podLabels = local.workload_identity_label

    nodeSelector = merge(local.igloo_node_selector, var.spot_enabled ? local.spot_node_label : {})
    tolerations  = local.igloo_tolerations

    resources = {
      requests = { cpu = var.igloo_cpu, memory = var.igloo_memory }
      limits   = { memory = var.igloo_memory }
    }

    service = {
      type                     = var.expose_sql ? "LoadBalancer" : "ClusterIP"
      port                     = 5432
      annotations              = local.sql_service_annotations
      loadBalancerSourceRanges = var.expose_sql ? var.sql_allowed_cidrs : []
    }

    catalog = {
      replicaCount      = var.catalog_replica_count
      systemCredentials = "azure"
      database = {
        existingSecret = kubernetes_secret_v1.database.metadata[0].name
      }
      encryptionKey = {
        existingSecret = kubernetes_secret_v1.catalog_encryption.metadata[0].name
      }
      serviceAccount = {
        create = true
        name   = var.catalog_service_account_name
        annotations = {
          "azure.workload.identity/client-id" = azurerm_user_assigned_identity.catalog.client_id
        }
      }
      podLabels = local.workload_identity_label
    }
  }
}

resource "helm_release" "igloo" {
  count = var.install_chart ? 1 : 0

  name             = var.release_name
  chart            = local.chart_path
  namespace        = kubernetes_namespace_v1.igloo.metadata[0].name
  create_namespace = false
  wait             = true
  timeout          = var.helm_timeout_seconds
  max_history      = 10

  values = [yamlencode(local.helm_values)]

  depends_on = [
    # Nodes for Igloo pods.
    azurerm_kubernetes_cluster_node_pool.igloo,
    # Lakekeeper migrates the schema on start; it needs the extensions allowed.
    azurerm_postgresql_flexible_server_configuration.extensions,
    azurerm_postgresql_flexible_server_configuration.require_tls,
    # Identities usable before the first pod asks for a token.
    time_sleep.identity_propagation,
    # The static IP must be usable by AKS before the Service claims it.
    azurerm_role_assignment.aks_network,
  ]
}

check "sql_endpoint_tls" {
  assert {
    condition     = !var.expose_sql || local.tls_enabled
    error_message = "expose_sql is true but no TLS certificate is configured: SQL traffic, including query results, crosses the internet unencrypted. Set tls_certificate_pem and tls_private_key_pem."
  }
}
