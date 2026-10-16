# Cloud SQL for PostgreSQL 17, private IP only. One database, "igloo", holds
# the Lakekeeper catalog (schema public) and Igloo's users and roles (schema
# igloo).

resource "google_sql_database_instance" "this" {
  name                = "${var.name}-${local.suffix}"
  region              = var.region
  database_version    = "POSTGRES_17"
  deletion_protection = var.deletion_protection

  settings {
    # Enterprise Plus (the default for new PostgreSQL 16+ instances) starts at
    # 2 vCPUs; the catalog does not need it.
    edition                     = "ENTERPRISE"
    tier                        = var.db_tier
    availability_type           = var.db_high_availability ? "REGIONAL" : "ZONAL"
    disk_type                   = "PD_SSD"
    disk_size                   = var.db_disk_size_gb
    disk_autoresize             = true
    deletion_protection_enabled = var.deletion_protection
    user_labels                 = local.labels

    ip_configuration {
      ipv4_enabled       = false
      private_network    = google_compute_network.this.id
      allocated_ip_range = google_compute_global_address.sql.name
      # Clients must use TLS; no client certificate is required. Connection
      # URLs therefore carry sslmode=require.
      ssl_mode = "ENCRYPTED_ONLY"
    }

    backup_configuration {
      enabled                        = true
      start_time                     = "03:00"
      point_in_time_recovery_enabled = true
      transaction_log_retention_days = min(7, var.db_backup_retention_count)

      backup_retention_settings {
        retained_backups = var.db_backup_retention_count
        retention_unit   = "COUNT"
      }
    }

    maintenance_window {
      day          = 7 # Sunday
      hour         = 4 # UTC
      update_track = "stable"
    }

    database_flags {
      name  = "log_checkpoints"
      value = "on"
    }
    database_flags {
      name  = "log_connections"
      value = "on"
    }
    database_flags {
      name  = "log_disconnections"
      value = "on"
    }
    database_flags {
      name  = "log_lock_waits"
      value = "on"
    }
    database_flags {
      name  = "log_temp_files"
      value = "0"
    }
  }

  depends_on = [google_service_networking_connection.sql]
}

resource "google_sql_database" "igloo" {
  name     = "igloo"
  instance = google_sql_database_instance.this.name

  # Postgres refuses to drop a database that still has connections; the
  # instance's own deletion removes it anyway.
  deletion_policy = "ABANDON"
}

resource "random_password" "db" {
  length  = 32
  special = false # embedded in a URL
}

resource "google_sql_user" "igloo" {
  name     = "igloo"
  instance = google_sql_database_instance.this.name
  password = random_password.db.result

  # Postgres refuses to drop a role that owns objects; the instance's own
  # deletion removes it anyway.
  deletion_policy = "ABANDON"
}

locals {
  database_url = format(
    "postgresql://%s:%s@%s:5432/%s?sslmode=require",
    google_sql_user.igloo.name,
    random_password.db.result,
    google_sql_database_instance.this.private_ip_address,
    google_sql_database.igloo.name,
  )
}
