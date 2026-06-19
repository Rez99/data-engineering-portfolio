variable "polaris_base_url" {
  description = "Polaris Cloud Run service URL without a trailing slash."
  type        = string
}

variable "polaris_realm" {
  description = "Polaris realm sent as the Polaris-Realm request header."
  type        = string
  default     = "POLARIS"
}

variable "polaris_root_client_id" {
  description = "Root client ID created by the minimal Polaris bootstrap job."
  type        = string
  default     = "admin"
}

variable "polaris_root_client_secret" {
  description = "Root client secret created by the minimal Polaris bootstrap job."
  type        = string
  default     = "polaris_root"
  sensitive   = true
}

variable "catalog_name" {
  description = "Polaris catalog to create for the lakehouse."
  type        = string
  default     = "lakehouse"
}

variable "warehouse_location" {
  description = "GCS base location for the Polaris-backed Iceberg warehouse."
  type        = string
  default     = "gs://rez-cloud-lakehouse-lakehouse/warehouse/"
}

variable "gcs_service_account" {
  description = "Service account Polaris should use for GCS table storage."
  type        = string
  default     = "polaris@rez-cloud-lakehouse.iam.gserviceaccount.com"
}

variable "namespace" {
  description = "Initial Iceberg namespace to create."
  type        = list(string)
  default     = ["bronze"]
}
