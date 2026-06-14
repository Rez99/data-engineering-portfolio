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
