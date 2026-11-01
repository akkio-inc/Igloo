# Each run feeds one bad input and expects that variable's validation to fail.

mock_provider "google" {
  mock_resource "google_compute_network" {
    defaults = {
      id        = "projects/igloo-test-project/global/networks/igloo"
      self_link = "https://www.googleapis.com/compute/v1/projects/igloo-test-project/global/networks/igloo"
    }
  }

  mock_resource "google_service_account" {
    defaults = {
      email  = "igloo-mock@igloo-test-project.iam.gserviceaccount.com"
      member = "serviceAccount:igloo-mock@igloo-test-project.iam.gserviceaccount.com"
      name   = "projects/igloo-test-project/serviceAccounts/igloo-mock@igloo-test-project.iam.gserviceaccount.com"
    }
  }
}

mock_provider "kubernetes" {}
mock_provider "helm" {}
mock_provider "random" {}

variables {
  project_id = "igloo-test-project"
}

run "rejects_bad_project_id" {
  command = plan
  variables {
    project_id = "My_Project"
  }
  expect_failures = [var.project_id]
}

run "rejects_zone_as_region" {
  command = plan
  variables {
    region = "us-central1-a"
  }
  expect_failures = [var.region]
}

run "rejects_bad_name" {
  command = plan
  variables {
    name = "Igloo-Production-Warehouse"
  }
  expect_failures = [var.name]
}

run "rejects_expose_sql_without_cidrs" {
  command = plan
  variables {
    expose_sql = true
  }
  expect_failures = [var.sql_allowed_cidrs]
}

run "rejects_malformed_sql_cidr" {
  command = plan
  variables {
    expose_sql        = true
    sql_allowed_cidrs = ["203.0.113.300/24"]
  }
  expect_failures = [var.sql_allowed_cidrs]
}

run "rejects_zero_replicas" {
  command = plan
  variables {
    replica_count = 0
  }
  expect_failures = [var.replica_count]
}

run "rejects_fractional_replicas" {
  command = plan
  variables {
    replica_count = 1.5
  }
  expect_failures = [var.replica_count]
}

run "rejects_bad_memory_quantity" {
  command = plan
  variables {
    igloo_memory = "8GB"
  }
  expect_failures = [var.igloo_memory]
}

run "rejects_bad_cpu_quantity" {
  command = plan
  variables {
    igloo_cpu = "two"
  }
  expect_failures = [var.igloo_cpu]
}

run "rejects_bad_duckdb_memory_limit" {
  command = plan
  variables {
    duckdb_memory_limit = "lots"
  }
  expect_failures = [var.duckdb_memory_limit]
}

run "rejects_certificate_without_key" {
  command = plan
  variables {
    tls_certificate = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
  }
  expect_failures = [var.tls_private_key]
}

run "rejects_enterprise_plus_tier" {
  command = plan
  variables {
    db_tier = "db-perf-optimized-N-2"
  }
  expect_failures = [var.db_tier]
}

run "rejects_small_disk" {
  command = plan
  variables {
    db_disk_size_gb = 5
  }
  expect_failures = [var.db_disk_size_gb]
}

run "rejects_cidr_with_host_bits" {
  command = plan
  variables {
    subnet_cidr = "10.10.0.1/20"
  }
  expect_failures = [var.subnet_cidr]
}

run "rejects_tiny_private_services_range" {
  command = plan
  variables {
    sql_private_services_cidr = "10.40.0.0/28"
  }
  expect_failures = [var.sql_private_services_cidr]
}

run "rejects_ipv6_control_plane_cidr" {
  command = plan
  variables {
    control_plane_authorized_cidrs = ["::/0"]
  }
  expect_failures = [var.control_plane_authorized_cidrs]
}

run "rejects_bad_namespace" {
  command = plan
  variables {
    namespace = "Igloo_NS"
  }
  expect_failures = [var.namespace]
}

run "rejects_bad_release_channel" {
  command = plan
  variables {
    gke_release_channel = "NIGHTLY"
  }
  expect_failures = [var.gke_release_channel]
}

run "rejects_bad_bucket_name" {
  command = plan
  variables {
    bucket_name = "My.Bucket"
  }
  expect_failures = [var.bucket_name]
}

run "rejects_bad_label" {
  command = plan
  variables {
    labels = { "Team" = "data" }
  }
  expect_failures = [var.labels]
}

run "rejects_bad_image_tag" {
  command = plan
  variables {
    image_tag = "v1.0 beta"
  }
  expect_failures = [var.image_tag]
}

run "rejects_short_soft_delete" {
  command = plan
  variables {
    bucket_soft_delete_days = 3
  }
  expect_failures = [var.bucket_soft_delete_days]
}

run "rejects_bad_local_ssd_machine_family" {
  command = plan
  variables {
    local_ssd_machine_family = "c3-standard-4-lssd"
  }
  expect_failures = [var.local_ssd_machine_family]
}

run "rejects_large_worker_without_workers" {
  command = plan
  variables {
    large_worker_enabled = true
  }
  expect_failures = [var.large_worker_enabled]
}

run "rejects_worker_spill_outside_gi" {
  command = plan
  variables {
    workers_enabled    = true
    workers_spill_size = "1Ti"
  }
  expect_failures = [var.workers_spill_size]
}
