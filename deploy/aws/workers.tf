################################################################################
# Distributed-execution workers (docs/distributed-execution.md)
#
# With workers_enabled: managed node groups for workers, Spot first and
# on-demand, and for the large worker with large_worker_enabled, all with a
# minimum of zero nodes, labelled and tainted so only their own pods run on
# them. The Kubernetes cluster autoscaler adds their nodes when worker pods
# wait for one and removes them when the pods are gone; its priority
# expander tries the Spot group first, and a Spot group that cannot get
# capacity within max-node-provision-time is backed off, so the on-demand
# group gets the pod. The default node group (igloo pods, Lakekeeper, system
# pods) stays as it is, outside the autoscaler.
################################################################################

data "aws_ec2_instance_type" "worker" {
  for_each = toset(concat(
    var.workers_enabled ? var.workers_instance_types : [],
    var.large_worker_enabled ? var.large_worker_instance_types : [],
  ))

  instance_type = each.value
}

locals {
  # Node label and taint key of each kind of worker node.
  worker_node_label = "igloo.dev/node"

  worker_kind_settings = {
    workers = {
      enabled = var.workers_enabled
      types   = var.workers_instance_types
      cpu     = var.workers_cpu
      memory  = var.workers_memory
      max     = var.workers_max
      label   = "workers"
      taint   = "igloo.dev/workers"
    }
    large = {
      enabled = var.large_worker_enabled
      types   = var.large_worker_instance_types
      cpu     = var.large_worker_cpu
      memory  = var.large_worker_memory
      max     = 1
      label   = "large-worker"
      taint   = "igloo.dev/large-worker"
    }
  }

  # Each kind's node and pod shape, from its first instance type: pods take
  # 90% of the vCPUs and 85% of the memory, so one fills a node with room for
  # the kubelet's reservation and the DaemonSets; spill and read cache take
  # 55% and 25% of the smallest instance store, or half and a fifth of the
  # root volume when a listed type has none. A kind that is off has no
  # instance type data, and gets nulls and zeros.
  worker_kinds = {
    for kind, s in local.worker_kind_settings : kind => merge(s, {
      arm64          = can(regex("^[a-z]+[0-9]+[a-z]*g[a-z]*\\.", s.types[0]))
      instance_store = try(anytrue([for t in s.types : data.aws_ec2_instance_type.worker[t].instance_storage_supported]), false)
      store_gib = try(floor(min([
        for t in s.types : data.aws_ec2_instance_type.worker[t].instance_storage_supported ? data.aws_ec2_instance_type.worker[t].total_instance_storage : 0
      ]...) * 1000000000 / 1073741824), 0)
      pod_cpu    = try(coalesce(s.cpu, tostring(max(1, floor(data.aws_ec2_instance_type.worker[s.types[0]].default_vcpus * 0.9)))), s.cpu)
      pod_memory = try(coalesce(s.memory, "${max(1, floor(data.aws_ec2_instance_type.worker[s.types[0]].memory_size * 0.85 / 1024))}Gi"), s.memory)
    })
  }

  worker_volumes = {
    for kind, k in local.worker_kinds : kind => k.store_gib > 0 ? {
      spill = "${floor(k.store_gib * 0.55)}Gi"
      cache = "${floor(k.store_gib * 0.25)}Gi"
      } : {
      spill = "${floor(var.workers_disk_size_gib * 0.5)}Gi"
      cache = "${floor(var.workers_disk_size_gib * 0.2)}Gi"
    }
  }

  # The node groups: a Spot one when its kind asks for Spot, and always an
  # on-demand one to fall back to.
  worker_node_group_list = [
    { key = "workers-spot", kind = "workers", capacity_type = "SPOT", enabled = var.workers_enabled && var.workers_spot },
    { key = "workers-ondemand", kind = "workers", capacity_type = "ON_DEMAND", enabled = var.workers_enabled },
    { key = "large-worker-spot", kind = "large", capacity_type = "SPOT", enabled = var.large_worker_enabled && var.large_worker_spot },
    { key = "large-worker-ondemand", kind = "large", capacity_type = "ON_DEMAND", enabled = var.large_worker_enabled },
  ]

  worker_node_groups = {
    for g in local.worker_node_group_list : g.key => {
      kind           = g.kind
      capacity_type  = g.capacity_type
      ami_type       = local.worker_kinds[g.kind].arm64 ? "AL2023_ARM_64_STANDARD" : "AL2023_x86_64_STANDARD"
      instance_types = local.worker_kinds[g.kind].types
      min_size       = 0
      max_size       = local.worker_kinds[g.kind].max
      local_storage  = local.worker_kinds[g.kind].instance_store ? "RAID0" : "none"
      labels         = { (local.worker_node_label) = local.worker_kinds[g.kind].label }
      taint_key      = local.worker_kinds[g.kind].taint
    } if g.enabled
  }

  # What the chart's worker pods select and tolerate.
  worker_scheduling = {
    for kind, k in local.worker_kinds : kind => {
      nodeSelector = { (local.worker_node_label) = k.label }
      tolerations  = [{ key = k.taint, operator = "Equal", value = "true", effect = "NoSchedule" }]
    }
  }

  # The chart's values for each kind of worker.
  worker_chart_values = {
    for kind, k in local.worker_kinds : kind => {
      resources = {
        requests = { cpu = k.pod_cpu, memory = k.pod_memory }
        limits   = { memory = k.pod_memory }
      }
      duckdb       = { spillVolume = { sizeLimit = local.worker_volumes[kind].spill } }
      cacheVolume  = { sizeLimit = local.worker_volumes[kind].cache }
      nodeSelector = local.worker_scheduling[kind].nodeSelector
      tolerations  = local.worker_scheduling[kind].tolerations
    }
  }

  # The chart's workers values, merged into helm_values in kubernetes.tf;
  # nothing without workers_enabled, so the chart's values stay as before.
  workers_helm_values = {
    for key, value in {
      workers = merge(local.worker_chart_values["workers"], {
        enabled      = true
        maxReplicas  = var.workers_max
        pricePerHour = var.workers_price_per_hour
        # Where no price is set, igloo prices the machines from its table of
        # list prices (docs/distributed-execution.md#prices).
        instanceType = var.workers_instance_types[0]
        spot         = var.workers_spot
        region       = var.region
        # A role of their own, which writes no table's files (see iam.tf).
        serviceAccount = {
          create = true
          name   = local.worker_service_account
          annotations = {
            "eks.amazonaws.com/role-arn" = one(aws_iam_role.worker[*].arn)
          }
        }
        large = merge(
          { enabled = var.large_worker_enabled },
          { for k, v in merge(local.worker_chart_values["large"], { pricePerHour = var.large_worker_price_per_hour, instanceType = var.large_worker_instance_types[0], spot = var.large_worker_spot }) : k => v if var.large_worker_enabled },
        )
      })
    } : key => value if var.workers_enabled
  }

  # The chart runs workers by default: without capacity for them here, it must not.
  no_workers_helm_values = { for key, value in { workers = { enabled = false } } : key => value if !var.workers_enabled }

  # The worker node groups' autoscaling groups, which the cluster autoscaler
  # scales; it leaves the default node group alone.
  worker_autoscaling_groups = flatten([
    for key, g in local.worker_node_groups : [
      for name in module.eks.eks_managed_node_groups[key].node_group_autoscaling_group_names : {
        name    = name
        minSize = g.min_size
        maxSize = g.max_size
      }
    ]
  ])

  cluster_autoscaler_service_account = "cluster-autoscaler"
}

resource "helm_release" "cluster_autoscaler" {
  count = var.workers_enabled ? 1 : 0

  name       = "cluster-autoscaler"
  repository = "https://kubernetes.github.io/autoscaler"
  chart      = "cluster-autoscaler"
  version    = var.cluster_autoscaler_chart_version
  namespace  = "kube-system"

  values = [yamlencode({
    cloudProvider     = "aws"
    awsRegion         = var.region
    autoscalingGroups = local.worker_autoscaling_groups
    # Spot node groups first (EKS names a managed node group's autoscaling
    # group eks-<node group name>-<id>); on-demand when a Spot group is
    # backed off.
    expanderPriorities = {
      "50" = ["^eks-workers-spot-.*", "^eks-large-worker-spot-.*"]
      "10" = [".*"]
    }
    extraArgs = {
      expander = "priority"
      # A Spot group whose node has not joined by then is backed off, and
      # the on-demand group gets the pod. AWS usually reports a Spot
      # shortage sooner, and the autoscaler backs off at once then.
      max-node-provision-time = "5m"
      # Workers keep spill and read cache in emptyDir volumes; their nodes
      # may still be removed once the workers are gone.
      skip-nodes-with-local-storage = false
      scale-down-unneeded-time      = "5m"
      scale-down-delay-after-add    = "5m"
    }
    rbac = {
      serviceAccount = {
        create = true
        name   = local.cluster_autoscaler_service_account
        annotations = {
          "eks.amazonaws.com/role-arn" = aws_iam_role.cluster_autoscaler[0].arn
        }
      }
    }
    resources = {
      requests = { cpu = "100m", memory = "300Mi" }
      limits   = { memory = "600Mi" }
    }
  })]

  wait    = true
  timeout = 600

  depends_on = [
    module.eks,
    aws_iam_role_policy.cluster_autoscaler,
  ]
}
