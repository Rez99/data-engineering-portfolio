output "catalog_name" {
  description = "Polaris catalog managed by this Terraform root."
  value       = polaris_rest_resource.catalog.id
}

output "namespace" {
  description = "Initial Iceberg namespace managed by this Terraform root."
  value       = var.namespace
}
