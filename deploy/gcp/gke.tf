# GKE Autopilot: Google runs the nodes and bills for the CPU, memory and disk
# that pods request. See README.md ("Why Autopilot") for the cost reasoning.

# Nodes run as a dedicated, minimally privileged service account instead of
# the Compute Engine default account (which has the project Editor role).
resource "google_service_account" "nodes" {
  account_id   = "${var.name}-nodes"
  display_name = "Igloo GKE nodes (${var.name})"
  description  = "Identity of the GKE Autopilot nodes. Logging and monitoring only."

  depends_on = [google_project_service.this]
}

resource "google_project_iam_member" "nodes" {
  project = var.project_id
  role    = "roles/container.defaultNodeServiceAccount"
  member  = google_service_account.nodes.member
}

resource "google_container_cluster" "this" {
  name                = var.name
  location            = var.region
  enable_autopilot    = true
  deletion_protection = var.deletion_protection
  resource_labels     = local.labels

  network    = google_compute_network.this.id
  subnetwork = google_compute_subnetwork.nodes.id

  release_channel {
    channel = var.gke_release_channel
  }

  # VPC-native networking with the subnet's secondary ranges.
  ip_allocation_policy {
    cluster_secondary_range_name  = local.pods_range_name
    services_secondary_range_name = local.services_range_name
  }

  # Nodes get no public IPs.
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
  }

  # Operators and tofu reach the API through the DNS-based endpoint, which
  # authorizes every request with Google IAM.
  control_plane_endpoints_config {
    dns_endpoint_config {
      allow_external_traffic = true
    }
  }

  # The public IP endpoint accepts only the listed CIDRs (none by default).
  master_authorized_networks_config {
    gcp_public_cidrs_access_enabled = false

    dynamic "cidr_blocks" {
      for_each = var.control_plane_authorized_cidrs
      content {
        cidr_block   = cidr_blocks.value
        display_name = "authorized-${cidr_blocks.key}"
      }
    }
  }

  workload_identity_config {
    workload_pool = local.workload_pool
  }

  cluster_autoscaling {
    auto_provisioning_defaults {
      service_account = google_service_account.nodes.email
      oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    }
  }

  depends_on = [
    google_project_iam_member.nodes,
    google_compute_router_nat.this,
  ]
}
