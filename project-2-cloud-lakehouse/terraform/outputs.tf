output "validation_bucket_name" {
  description = "Name of the Cloud Storage bucket used by the validation spike."
  value       = google_storage_bucket.validation.name
}

output "validation_bucket_url" {
  description = "Cloud Storage URL of the validation bucket."
  value       = google_storage_bucket.validation.url
}
