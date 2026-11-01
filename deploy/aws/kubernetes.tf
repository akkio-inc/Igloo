################################################################################
# Namespace and secrets. These are created even when install_chart is false,
# so a chart installed another way can reference them.
################################################################################

resource "kubernetes_namespace_v1" "igloo" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/part-of"          = "igloo"
      "pod-security.kubernetes.io/enforce" = "baseline"
      "pod-security.kubernetes.io/warn"    = "restricted"
      "pod-security.kubernetes.io/audit"   = "restricted"
    }
  }
}

resource "random_password" "admin" {
  length  = 32
  special = false
}

# Lakekeeper encrypts stored storage credentials with this key. Losing it
# makes those credentials unreadable, so it lives in state and in the Secret.
resource "random_password" "catalog_encryption_key" {
  length  = 48
  special = false
}

resource "kubernetes_secret_v1" "admin" {
  metadata {
    name      = "igloo-admin"
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  data = {
    password = random_password.admin.result
  }
}

# One URL for both igloo (metadataDatabase) and Lakekeeper (catalog.database).
resource "kubernetes_secret_v1" "database" {
  metadata {
    name      = "igloo-database"
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  data = {
    url = local.database_url
  }
}

resource "kubernetes_secret_v1" "catalog_encryption" {
  metadata {
    name      = "igloo-catalog-encryption"
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  data = {
    "encryption-key" = random_password.catalog_encryption_key.result
  }
}

################################################################################
# TLS for SQL clients: on whenever the SQL port is exposed, or when a
# certificate is supplied.
################################################################################

resource "tls_private_key" "sql" {
  count     = local.tls_self_signed ? 1 : 0
  algorithm = "ECDSA"

  ecdsa_curve = "P256"
}

resource "tls_self_signed_cert" "sql" {
  count           = local.tls_self_signed ? 1 : 0
  private_key_pem = tls_private_key.sql[0].private_key_pem

  subject {
    common_name  = "igloo"
    organization = "Igloo (self-signed)"
  }
  dns_names = [
    local.sql_service_name,
    "${local.sql_service_name}.${var.namespace}.svc",
    "${local.sql_service_name}.${var.namespace}.svc.cluster.local",
  ]

  validity_period_hours = 24 * 365 * 5
  allowed_uses          = ["digital_signature", "key_encipherment", "server_auth"]
}

resource "kubernetes_secret_v1" "tls" {
  count = local.tls_enabled ? 1 : 0

  metadata {
    name      = "igloo-tls"
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }
  type = "kubernetes.io/tls"
  data = {
    "tls.crt" = local.tls_self_signed ? tls_self_signed_cert.sql[0].cert_pem : var.tls_certificate_pem
    "tls.key" = local.tls_self_signed ? tls_private_key.sql[0].private_key_pem : var.tls_private_key_pem
  }
}

################################################################################
# Helm release
################################################################################

locals {
  helm_values = merge(local.helm_base_values, local.workers_helm_values, local.no_workers_helm_values)

  helm_base_values = {
    image = merge(
      { tag = var.igloo_image_tag },
      var.igloo_image_repository == "" ? {} : { repository = var.igloo_image_repository },
    )
    replicaCount = var.igloo_replicas

    admin = {
      user           = var.admin_user
      existingSecret = kubernetes_secret_v1.admin.metadata[0].name
    }

    metadataDatabase = {
      existingSecret = kubernetes_secret_v1.database.metadata[0].name
    }

    storage = {
      warehouseLocation = local.warehouse_location
      allowedLocations  = var.allowed_locations
      s3 = {
        endpoint = ""
        region   = var.region
        urlStyle = "vhost"
        useSSL   = true
        # No static keys: pods get credentials from IRSA.
        existingSecret = ""
        # Credentials for other engines, limited to one table each.
        stsRoleArn = aws_iam_role.engines.arn
      }
    }

    duckdb = {
      memoryLimit = var.duckdb_memory_limit
      threads     = var.duckdb_threads
      spillVolume = {
        sizeLimit = var.duckdb_spill_size
      }
    }

    tls = {
      existingSecret = local.tls_enabled ? kubernetes_secret_v1.tls[0].metadata[0].name : ""
    }

    serviceAccount = {
      create = true
      name   = local.igloo_service_account
      annotations = {
        "eks.amazonaws.com/role-arn" = aws_iam_role.service["igloo"].arn
      }
    }

    resources = {
      requests = {
        cpu    = var.igloo_cpu_request
        memory = var.igloo_memory
      }
      limits = {
        memory = var.igloo_memory
      }
    }

    service = {
      type        = var.expose_sql ? "LoadBalancer" : "ClusterIP"
      port        = 5432
      annotations = var.expose_sql ? local.load_balancer_annotations : {}
      # Enforced by the AWS cloud controller as security group rules.
      loadBalancerSourceRanges = var.expose_sql ? var.sql_allowed_cidrs : []
    }

    catalog = {
      database = {
        existingSecret = kubernetes_secret_v1.database.metadata[0].name
      }
      encryptionKey = {
        existingSecret = kubernetes_secret_v1.catalog_encryption.metadata[0].name
      }
      systemCredentials = "aws"
      serviceAccount = {
        create = true
        name   = local.catalog_service_account
        annotations = {
          "eks.amazonaws.com/role-arn" = aws_iam_role.service["catalog"].arn
        }
      }
    }

    # Managed services replace the chart's bundled Postgres and object store.
    postgresql = {
      enabled = false
    }
    objectStorage = {
      enabled = false
    }
  }

  # Network Load Balancer created by the AWS cloud controller built into EKS.
  load_balancer_annotations = merge(
    { "service.beta.kubernetes.io/aws-load-balancer-type" = "nlb" },
    var.sql_load_balancer_internal ? { "service.beta.kubernetes.io/aws-load-balancer-internal" = "true" } : {},
  )
}

# On destroy, gives the cloud controller time to delete the load balancer
# after the Service is removed and before the cluster and VPC are deleted;
# a leftover load balancer would block deleting the VPC.
resource "time_sleep" "load_balancer_cleanup" {
  count = var.install_chart && var.expose_sql ? 1 : 0

  destroy_duration = "120s"

  depends_on = [module.eks, module.vpc]
}

resource "helm_release" "igloo" {
  count = var.install_chart ? 1 : 0

  name             = local.release_name
  chart            = "${path.module}/../helm/igloo"
  namespace        = kubernetes_namespace_v1.igloo.metadata[0].name
  create_namespace = false

  values = compact([
    yamlencode(local.helm_values),
    var.helm_values_override,
  ])

  # First start pulls images, migrates the catalog database and creates the
  # warehouse, which can take several minutes.
  wait    = true
  timeout = 900

  depends_on = [
    module.eks, # nodes and add-ons must be up before pods can schedule
    aws_iam_role_policy.warehouse,
    aws_iam_role_policy.engines_warehouse,
    aws_iam_role_policy.igloo_assume_engines,
    aws_iam_role_policy_attachment.igloo_additional,
    aws_vpc_endpoint.s3,
    aws_s3_bucket_policy.warehouse,
    time_sleep.load_balancer_cleanup,
  ]
}

data "kubernetes_service_v1" "sql" {
  count = var.install_chart && var.expose_sql ? 1 : 0

  metadata {
    name      = local.sql_service_name
    namespace = kubernetes_namespace_v1.igloo.metadata[0].name
  }

  depends_on = [helm_release.igloo]
}
