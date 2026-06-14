variable "project_id" {
  description = "GCP project used for the Project 2 validation environment."
  type        = string
  default     = "rez-cloud-lakehouse"
}

variable "region" {
  description = "Primary GCP region for Project 2 resources."
  type        = string
  default     = "us-central1"
}

variable "clickstream_source_url" {
  description = "Public source for the October 2019 ecommerce clickstream dataset."
  type        = string
  default     = "https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz"
}

variable "ingestion_image" {
  description = "Container image used by the ingestion Cloud Run Job."
  type        = string
  default     = "us-docker.pkg.dev/cloudrun/container/job:latest"
}

variable "polaris_image" {
  description = "Apache Polaris image deployed to Cloud Run."
  type        = string
  default     = "apache/polaris:1.5.0"
}

variable "polaris_admin_image" {
  description = "Apache Polaris admin tool image used to bootstrap PostgreSQL."
  type        = string
  default     = "apache/polaris-admin-tool:1.5.0"
}

variable "cloud_sql_proxy_image" {
  description = "Cloud SQL Auth Proxy image used as a Polaris sidecar."
  type        = string
  default     = "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.18.2"
}

variable "polaris_database_name" {
  description = "PostgreSQL database used by Polaris."
  type        = string
  default     = "polaris"
}

variable "polaris_database_user" {
  description = "PostgreSQL user used by Polaris."
  type        = string
  default     = "polaris"
}

variable "polaris_database_password" {
  description = "PostgreSQL password used by Polaris. Supply this with a tfvars file or -var."
  type        = string
  sensitive   = true
}

variable "polaris_realm" {
  description = "Polaris realm made available by the Cloud Run service."
  type        = string
  default     = "POLARIS"
}

variable "polaris_root_client_id" {
  description = "Root client ID created during Polaris bootstrap."
  type        = string
  default     = "admin"
}

variable "polaris_root_client_secret" {
  description = "Root client secret created during Polaris bootstrap. Supply this with a tfvars file or -var."
  type        = string
  sensitive   = true
}
