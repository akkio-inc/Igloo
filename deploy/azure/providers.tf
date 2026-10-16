provider "azurerm" {
  # Null falls back to the ARM_SUBSCRIPTION_ID environment variable.
  subscription_id = var.subscription_id

  # Shared-key access to the storage account is off, so any storage data-plane
  # request the provider makes must use Entra ID.
  storage_use_azuread = true

  features {
    storage {
      # Everything this module creates in the storage account, the ADLS
      # filesystem included, goes through Azure Resource Manager. The storage
      # data plane is closed by the account firewall, so the provider must not
      # try to reach it.
      data_plane_available = false
    }
  }
}

locals {
  # With Entra ID integration the user kubeconfig needs kubelogin; the admin
  # kubeconfig authenticates with a client certificate and needs no extra tools.
  kube = length(var.admin_group_object_ids) > 0 ? azurerm_kubernetes_cluster.this.kube_admin_config[0] : azurerm_kubernetes_cluster.this.kube_config[0]
}

# Both providers reach the new cluster's public API endpoint with the
# credentials AKS returns. If api_server_authorized_ip_ranges is set, it must
# include the address `tofu` runs from.
provider "kubernetes" {
  host                   = local.kube.host
  client_certificate     = base64decode(local.kube.client_certificate)
  client_key             = base64decode(local.kube.client_key)
  cluster_ca_certificate = base64decode(local.kube.cluster_ca_certificate)
}

provider "helm" {
  kubernetes = {
    host                   = local.kube.host
    client_certificate     = base64decode(local.kube.client_certificate)
    client_key             = base64decode(local.kube.client_key)
    cluster_ca_certificate = base64decode(local.kube.cluster_ca_certificate)
  }
}
