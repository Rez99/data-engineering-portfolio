variable "superset_endpoint" {
  description = "Base URL of the Superset Cloud Run service."
  type        = string
}

variable "superset_username" {
  description = "Superset administrator username used by the provider."
  type        = string
  default     = "admin"
}

variable "superset_password" {
  description = "Superset administrator password used by the provider."
  type        = string
  default     = "admin"
  sensitive   = true
}
