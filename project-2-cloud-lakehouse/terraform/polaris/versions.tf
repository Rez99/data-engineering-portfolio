terraform {
  required_version = ">= 1.15.0, < 2.0.0"

  required_providers {
    polaris = {
      source  = "tsukubatexas/polaris"
      version = "0.1.1"
    }
  }
}

provider "polaris" {
  alias    = "management"
  endpoint = "${var.polaris_base_url}/api/management/v1"
  realm    = var.polaris_realm

  oauth_token_url = "${var.polaris_base_url}/api/catalog/v1/oauth/tokens"
  oauth_scope     = "PRINCIPAL_ROLE:ALL"
  client_id       = var.polaris_root_client_id
  client_secret   = var.polaris_root_client_secret
}

provider "polaris" {
  alias    = "catalog"
  endpoint = "${var.polaris_base_url}/api/catalog"
  realm    = var.polaris_realm

  oauth_token_url = "${var.polaris_base_url}/api/catalog/v1/oauth/tokens"
  oauth_scope     = "PRINCIPAL_ROLE:ALL"
  client_id       = var.polaris_root_client_id
  client_secret   = var.polaris_root_client_secret
}
