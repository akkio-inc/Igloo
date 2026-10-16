# VPC with one regional subnet. Nodes have private IPs only; they reach Google
# APIs through Private Google Access and the internet (container registries)
# through Cloud NAT.

resource "google_compute_network" "this" {
  name                    = var.name
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"

  depends_on = [google_project_service.this]
}

resource "google_compute_subnetwork" "nodes" {
  name                     = "${var.name}-nodes"
  network                  = google_compute_network.this.id
  region                   = var.region
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = local.pods_range_name
    ip_cidr_range = var.pods_cidr
  }

  secondary_ip_range {
    range_name    = local.services_range_name
    ip_cidr_range = var.services_cidr
  }

  dynamic "log_config" {
    for_each = var.enable_flow_logs ? [1] : []
    content {
      aggregation_interval = "INTERVAL_10_MIN"
      flow_sampling        = 0.5
      metadata             = "INCLUDE_ALL_METADATA"
    }
  }
}

resource "google_compute_router" "this" {
  name    = "${var.name}-router"
  network = google_compute_network.this.id
  region  = var.region
}

# Outbound internet for private nodes: image pulls from ghcr.io and quay.io.
resource "google_compute_router_nat" "this" {
  name                               = "${var.name}-nat"
  router                             = google_compute_router.this.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# Private services access: a peered range for Cloud SQL's private IP.
resource "google_compute_global_address" "sql" {
  name          = "${var.name}-sql-private-services"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  address       = split("/", var.sql_private_services_cidr)[0]
  prefix_length = tonumber(split("/", var.sql_private_services_cidr)[1])
  network       = google_compute_network.this.id
}

resource "google_service_networking_connection" "sql" {
  network                 = google_compute_network.this.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.sql.name]

  # On destroy the Cloud SQL instance is already gone, but its producer-side
  # resources can linger and make the API refuse to delete the connection.
  # REMOVE_PEERING then drops the peering so the network can be deleted.
  deletion_policy = "REMOVE_PEERING"
}
