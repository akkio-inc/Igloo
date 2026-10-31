# The Igloo chart: igloo pods plus the bundled Lakekeeper catalog. Values use
# the keys of deploy/helm/igloo/values.yaml.

locals {
  spot_node_selector = var.spot ? { "cloud.google.com/gke-spot" = "true" } : {}

  # Nodes of this series with Local SSD-backed ephemeral storage, so the
  # spill emptyDir is on local NVMe rather than the boot disk.
  local_ssd_node_selector = var.local_ssd_machine_family == "" ? {} : {
    "cloud.google.com/machine-family"                  = var.local_ssd_machine_family
    "cloud.google.com/gke-ephemeral-storage-local-ssd" = "true"
  }
  spot_tolerations = var.spot ? [{
    key      = "cloud.google.com/gke-spot"
    operator = "Equal"
    value    = "true"
    effect   = "NoSchedule"
  }] : []

  helm_values = merge(local.helm_base_values, local.workers_helm_values)

  helm_base_values = {
    image = merge(
      { tag = var.image_tag },
      var.image_repository == "" ? {} : { repository = var.image_repository },
    )

    replicaCount = var.replica_count

    # With one replica, a PodDisruptionBudget of minAvailable 1 would block
    # every node drain, including GKE's automatic upgrades.
    podDisruptionBudget = {
      enabled = var.replica_count > 1
    }

    admin = {
      user           = var.admin_user
      existingSecret = local.secret_names.admin
    }

    metadataDatabase = {
      existingSecret = local.secret_names.database
    }

    storage = {
      warehouseLocation = local.warehouse_location
      gcs = {
        existingSecret = local.secret_names.gcs_hmac
      }
    }

    duckdb = {
      memoryLimit = var.duckdb_memory_limit
      spillVolume = {
        sizeLimit = var.spill_size
      }
    }

    tls = {
      existingSecret = local.tls_enabled ? local.secret_names.tls : ""
    }

    serviceAccount = {
      create = true
      name   = local.igloo_ksa
      annotations = {
        "iam.gke.io/gcp-service-account" = google_service_account.igloo.email
      }
    }

    nodeSelector = merge(local.local_ssd_node_selector, var.node_selector, local.spot_node_selector)
    tolerations  = local.spot_tolerations

    # Autopilot sizes each pod from its requests and caps emptyDir volumes at
    # the pod's ephemeral-storage request, so the spill volume is requested here.
    resources = {
      requests = {
        cpu                 = var.igloo_cpu
        memory              = var.igloo_memory
        "ephemeral-storage" = var.spill_size
      }
      limits = {
        memory              = var.igloo_memory
        "ephemeral-storage" = var.spill_size
      }
    }

    service = {
      type                     = var.expose_sql ? "LoadBalancer" : "ClusterIP"
      loadBalancerSourceRanges = var.expose_sql ? var.sql_allowed_cidrs : []
    }

    catalog = {
      replicaCount = var.catalog_replica_count
      database = {
        existingSecret = local.secret_names.database
      }
      encryptionKey = {
        existingSecret = local.secret_names.encryption_key
      }
      # Lakekeeper reaches GCS as its own service account (Workload Identity).
      systemCredentials = "gcp"
      serviceAccount = {
        create = true
        name   = local.catalog_ksa
        annotations = {
          "iam.gke.io/gcp-service-account" = google_service_account.catalog.email
        }
      }
    }
  }
}

resource "helm_release" "igloo" {
  count = var.install_chart ? 1 : 0

  name      = local.release_name
  chart     = "${path.module}/../helm/igloo"
  namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  values    = [yamlencode(local.helm_values)]

  # Autopilot provisions nodes for new pods, which takes a few minutes.
  wait    = true
  timeout = 1200

  depends_on = [
    kubernetes_secret_v1.admin,
    kubernetes_secret_v1.database,
    kubernetes_secret_v1.catalog_encryption_key,
    kubernetes_secret_v1.gcs_hmac,
    kubernetes_secret_v1.tls,
    google_storage_bucket_iam_member.warehouse,
    google_service_account_iam_member.igloo_workload_identity,
    google_service_account_iam_member.catalog_workload_identity,
    google_sql_user.igloo,
    google_sql_database.igloo,
    # Worker pods select their compute class.
    helm_release.compute_classes,
  ]
}

# The load balancer address, once the chart has created the Service.
data "kubernetes_service_v1" "sql" {
  count = var.install_chart && var.expose_sql ? 1 : 0

  metadata {
    name      = local.service_name
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }

  depends_on = [helm_release.igloo]
}
