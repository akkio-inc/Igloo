################################################################################
# RDS PostgreSQL 17: one database ("igloo") shared by the Lakekeeper catalog
# (schema public) and igloo's user store (schema igloo).
################################################################################

locals {
  db_identifier = "${var.name_prefix}-db"
  db_name       = "igloo"
  db_username   = "igloo"

  # sslmode=require: RDS for PostgreSQL 15+ rejects unencrypted connections
  # (rds.force_ssl defaults to 1).
  database_url = format(
    "postgresql://%s:%s@%s:%d/%s?sslmode=require",
    local.db_username,
    random_password.db.result,
    aws_db_instance.this.address,
    aws_db_instance.this.port,
    local.db_name,
  )
}

# Letters and digits only, so the password needs no escaping in the URL.
resource "random_password" "db" {
  length  = 40
  special = false
}

resource "aws_security_group" "db" {
  name_prefix = "${var.name_prefix}-db-"
  description = "PostgreSQL access for the ${local.cluster_name} EKS cluster only"
  vpc_id      = module.vpc.vpc_id

  tags = { Name = "${var.name_prefix}-db" }

  lifecycle {
    create_before_destroy = true
  }
}

# Pods use their node's security group (Amazon VPC CNI default).
resource "aws_vpc_security_group_ingress_rule" "db_from_nodes" {
  security_group_id            = aws_security_group.db.id
  description                  = "PostgreSQL from EKS nodes and pods"
  referenced_security_group_id = module.eks.node_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

resource "aws_vpc_security_group_ingress_rule" "db_from_cluster" {
  security_group_id            = aws_security_group.db.id
  description                  = "PostgreSQL from the EKS cluster security group"
  referenced_security_group_id = module.eks.cluster_primary_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

resource "aws_db_instance" "this" {
  identifier = local.db_identifier

  engine                     = "postgres"
  engine_version             = "17"
  auto_minor_version_upgrade = true
  instance_class             = var.db_instance_class

  db_name  = local.db_name
  username = local.db_username
  password = random_password.db.result
  port     = 5432

  allocated_storage     = var.db_allocated_storage_gib
  max_allocated_storage = var.db_max_allocated_storage_gib
  storage_type          = "gp3"
  storage_encrypted     = true

  db_subnet_group_name   = module.vpc.database_subnet_group_name
  vpc_security_group_ids = [aws_security_group.db.id]
  publicly_accessible    = false
  multi_az               = var.db_multi_az

  backup_retention_period   = var.db_backup_retention_days
  copy_tags_to_snapshot     = true
  deletion_protection       = var.db_deletion_protection
  skip_final_snapshot       = var.db_skip_final_snapshot
  final_snapshot_identifier = var.db_skip_final_snapshot ? null : "${local.db_identifier}-final-${random_id.suffix.hex}"

  # Maintenance and minor upgrades happen in the weekly window, not on apply.
  apply_immediately = false
}
