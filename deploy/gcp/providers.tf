provider "google" {
  project = var.project_id
  region  = var.region
}

# Credentials of whoever runs `tofu`, used to reach the new cluster's API.
data "google_client_config" "current" {}

# The Kubernetes and Helm providers reach the cluster through its DNS-based
# control-plane endpoint. Every request is authorized by Google IAM, so the
# endpoint needs no IP allowlist and the operator needs no VPN. The endpoint
# serves a publicly trusted certificate, so no CA bundle is configured.
#
# The access token is read at plan time and lasts one hour; a first apply
# takes 20-40 minutes. If it ever expires mid-apply, run `tofu apply` again.
provider "kubernetes" {
  host  = local.cluster_api_url
  token = data.google_client_config.current.access_token
}

provider "helm" {
  kubernetes = {
    host  = local.cluster_api_url
    token = data.google_client_config.current.access_token
  }
}
