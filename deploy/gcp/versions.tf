terraform {
  # OpenTofu. Verified with 1.11.0 and 1.13.1. Releases before 1.9 lack
  # cross-variable validation; 1.9 and 1.10 crash in this module's test suite.
  required_version = ">= 1.11.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.7"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 3.3"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 3.3"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.9"
    }
  }
}
