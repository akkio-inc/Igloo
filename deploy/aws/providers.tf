provider "aws" {
  region = var.region

  default_tags {
    tags = local.tags
  }
}

# The Kubernetes and Helm providers authenticate to the new cluster with
# `aws eks get-token`, so the AWS CLI must be installed and must resolve the
# same AWS identity that runs `tofu apply` (that identity is made a cluster
# administrator below). A token from the aws_eks_cluster_auth data source
# would expire after 15 minutes, which is shorter than a first apply.
provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
  }
}

provider "helm" {
  kubernetes = {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

    exec = {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
    }
  }
}
