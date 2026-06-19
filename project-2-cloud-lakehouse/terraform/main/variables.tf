variable "project_id" {
  description = "GCP project used for the Project 2 cloud lakehouse environment."
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

variable "superset_image" {
  description = "Container image used by the Superset Cloud Run service and bootstrap job."
  type        = string
  default     = "us-docker.pkg.dev/cloudrun/container/hello:latest"
}

variable "superset_database_password" {
  description = "PostgreSQL password used by Superset."
  type        = string
  default     = "superset_pass"
  sensitive   = true
}

variable "superset_secret_key" {
  description = "Secret key used to sign Superset sessions."
  type        = string
  default     = "superset_secret_key_for_demo_only"
  sensitive   = true
}

variable "superset_admin_password" {
  description = "Password for the initial Superset administrator."
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "superset_admin_username" {
  description = "Username for the initial Superset administrator."
  type        = string
  default     = "admin"
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
  description = "PostgreSQL password used by Polaris."
  type        = string
  default     = "polaris_pass"
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
  description = "Root client secret created during Polaris bootstrap."
  type        = string
  default     = "polaris_root"
  sensitive   = true
}

variable "dataproc_operator_member" {
  description = "IAM member allowed to create Dataproc clusters and attach the Spark service account."
  type        = string
  default     = "user:rezwan.islam99@gmail.com"
}
