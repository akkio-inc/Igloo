terraform {
  # OpenTofu. Tested with 1.13; 1.9 is the oldest release with every
  # language feature used here (cross-variable validation, mock providers in tests).
  required_version = ">= 1.9.0"

  # All providers are MPL-2.0.
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.10"
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
    time = {
      source  = "hashicorp/time"
      version = "~> 0.14"
    }
  }
}
