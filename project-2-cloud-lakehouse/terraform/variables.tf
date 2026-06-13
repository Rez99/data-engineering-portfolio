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
