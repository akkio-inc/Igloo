terraform {
  # 1.9 is the first OpenTofu release that lets a variable's validation refer
  # to other variables, which variables.tf relies on.
  required_version = ">= 1.9.0"

  # Every provider below is MPL-2.0. The community modules used in this
  # directory (terraform-aws-modules/vpc, terraform-aws-modules/eks) are
  # Apache-2.0.
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.59, < 7.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 3.0, < 4.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = ">= 3.0, < 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.6, < 4.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.0, < 5.0"
    }
    time = {
      source  = "hashicorp/time"
      version = ">= 0.9, < 1.0"
    }
    # Used inside the EKS module (node group user data). Pinned here so that
    # every provider in the lock file has an upper bound.
    cloudinit = {
      source  = "hashicorp/cloudinit"
      version = ">= 2.0, < 3.0"
    }
    null = {
      source  = "hashicorp/null"
      version = ">= 3.0, < 4.0"
    }
  }
}

# State holds the database, admin and catalog-encryption passwords. Keep it in
# an encrypted remote backend rather than a local file, for example:
#
# terraform {
#   backend "s3" {
#     bucket       = "my-org-tofu-state"   # versioned, private, SSE-KMS
#     key          = "igloo/aws/terraform.tfstate"
#     region       = "us-east-1"
#     encrypt      = true
#     kms_key_id   = "arn:aws:kms:us-east-1:111122223333:key/..."
#     use_lockfile = true                  # S3-native locking (OpenTofu 1.10+)
#   }
# }
