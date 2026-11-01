# Distributed-execution workers (docs/distributed-execution.md).
#
# Autopilot creates nodes for pods by itself, so workers need no node pool:
# with workers_enabled, a GKE compute class (cloud.google.com/v1) lists what
# their nodes may be, in order, Spot first and on-demand after it, and worker
# pods select it. When GKE cannot get Spot capacity for a worker, it creates
# the node from the next priority, on-demand. The large worker, with
# large_worker_enabled, has a class of its own: its machine type, on-demand
# unless large_worker_spot.

locals {
  worker_kinds = {
    workers = {
      enabled = var.workers_enabled
      class   = "igloo-workers"
      spot    = var.workers_spot
      rule    = { machineFamily = var.workers_machine_family }
      cpu     = var.workers_cpu
      memory  = var.workers_memory
      spill   = var.workers_spill_size
      cache   = var.workers_cache_size
    }
    large = {
      enabled = var.large_worker_enabled
      class   = "igloo-large-worker"
      spot    = var.large_worker_spot
      rule    = { machineType = var.large_worker_machine_type }
      cpu     = var.large_worker_cpu
      memory  = var.large_worker_memory
      spill   = var.large_worker_spill_size
      cache   = var.large_worker_cache_size
    }
  }

  # The classes: Spot first where asked, always on-demand after. A worker
  # that fits no rule waits rather than land on another machine.
  compute_classes = [
    for kind, k in local.worker_kinds : {
      name = k.class
      spec = {
        priorities        = [for spot in(k.spot ? [true, false] : [false]) : merge(k.rule, { spot = spot })]
        whenUnsatisfiable = "DoNotScaleUp"
      }
    } if k.enabled
  ]

  # The chart's values for each kind of worker. Autopilot caps emptyDir
  # volumes at the pod's ephemeral-storage request, so spill, read cache and
  # 1Gi for /tmp and logs are requested.
  worker_chart_values = {
    for kind, k in local.worker_kinds : kind => {
      resources = {
        requests = {
          cpu                 = k.cpu
          memory              = k.memory
          "ephemeral-storage" = "${trimsuffix(k.spill, "Gi") + trimsuffix(k.cache, "Gi") + 1}Gi"
        }
        limits = {
          memory              = k.memory
          "ephemeral-storage" = "${trimsuffix(k.spill, "Gi") + trimsuffix(k.cache, "Gi") + 1}Gi"
        }
      }
      duckdb       = { spillVolume = { sizeLimit = k.spill } }
      cacheVolume  = { sizeLimit = k.cache }
      nodeSelector = { "cloud.google.com/compute-class" = k.class }
      # GKE adds these to pods that select the class; they are spelled out
      # so the values say where the pods may run.
      tolerations = concat(
        [{ key = "cloud.google.com/compute-class", operator = "Equal", value = k.class, effect = "NoSchedule" }],
        k.spot ? [{ key = "cloud.google.com/gke-spot", operator = "Equal", value = "true", effect = "NoSchedule" }] : [],
      )
    }
  }

  # The machine type Autopilot gives a worker pod, for its list price: the
  # family's next size up from the pod's CPU, highmem where the pod asks
  # for more than 6 GiB a vCPU; empty where the requests do not parse.
  workers_cpus         = try(ceil(endswith(var.workers_cpu, "m") ? tonumber(trimsuffix(var.workers_cpu, "m")) / 1000 : tonumber(var.workers_cpu)), 0)
  workers_machine_cpus = try([for n in [2, 4, 8, 16, 32, 48, 64, 80] : n if n >= local.workers_cpus][0], 0)
  workers_machine_type = try(format("%s-%s-%d", var.workers_machine_family,
  tonumber(trimsuffix(var.workers_memory, "Gi")) / local.workers_machine_cpus > 6 ? "highmem" : "standard", local.workers_machine_cpus), "")

  # Merged into helm_values in helm.tf; nothing without workers_enabled, so
  # the chart's values stay as before.
  workers_helm_values = {
    for key, value in {
      workers = merge(local.worker_chart_values["workers"], {
        enabled      = true
        maxReplicas  = var.workers_max
        pricePerHour = var.workers_price_per_hour
        # Where no price is set, igloo prices the machines from its table of
        # list prices (docs/distributed-execution.md#prices).
        instanceType = local.workers_machine_type
        spot         = var.workers_spot
        region       = var.region
        # An identity of their own (see storage.tf).
        serviceAccount = {
          create = true
          name   = local.worker_ksa
          annotations = {
            "iam.gke.io/gcp-service-account" = one(google_service_account.worker[*].email)
          }
        }
        storage = { gcs = { existingSecret = local.secret_names.worker_hmac } }
        large = merge(
          { enabled = var.large_worker_enabled },
          { for k, v in merge(local.worker_chart_values["large"], { pricePerHour = var.large_worker_price_per_hour, instanceType = var.large_worker_machine_type, spot = var.large_worker_spot }) : k => v if var.large_worker_enabled },
        )
      })
    } : key => value if var.workers_enabled
  }
}

# The compute classes, through a chart of their own (compute-classes/):
# custom resources made with kubernetes_manifest would need the cluster's
# API at plan time, before the cluster exists.
resource "helm_release" "compute_classes" {
  count = var.workers_enabled ? 1 : 0

  name      = "igloo-compute-classes"
  chart     = "${path.module}/compute-classes"
  namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  values    = [yamlencode({ classes = local.compute_classes })]
}
