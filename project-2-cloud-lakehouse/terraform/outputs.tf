output "validation_bucket_name" {
  description = "Name of the Cloud Storage bucket used by the validation spike."
  value       = google_storage_bucket.validation.name
}

output "validation_bucket_url" {
  description = "Cloud Storage URL of the validation bucket."
  value       = google_storage_bucket.validation.url
}

output "artifact_registry_repository" {
  description = "Artifact Registry repository that stores pipeline images."
  value       = google_artifact_registry_repository.pipeline.name
}

output "ingestion_image_repository" {
  description = "Docker repository path for the ingestion image."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.pipeline.repository_id}"
}

output "ingestion_job_name" {
  description = "Name of the extraction Cloud Run Job."
  value       = google_cloud_run_v2_job.ingestion.name
}

output "ingestion_service_account" {
  description = "Runtime service account used by the extraction job."
  value       = google_service_account.ingestion.email
}

output "extract_workflow_name" {
  description = "Name of the workflow that runs the extraction job."
  value       = google_workflows_workflow.extract.name
}

output "workflow_service_account" {
  description = "Runtime service account used by Workflows."
  value       = google_service_account.workflow.email
}
