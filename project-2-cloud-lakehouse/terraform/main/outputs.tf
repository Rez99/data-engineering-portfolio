output "lakehouse_bucket_name" {
  description = "Name of the Cloud Storage bucket used by the lakehouse environment."
  value       = google_storage_bucket.lakehouse.name
}

output "lakehouse_bucket_url" {
  description = "Cloud Storage URL of the lakehouse bucket."
  value       = google_storage_bucket.lakehouse.url
}

output "artifact_registry_repository" {
  description = "Artifact Registry repository that stores the Superset image."
  value       = google_artifact_registry_repository.superset.name
}

output "workflow_service_account" {
  description = "Runtime service account used by Workflows."
  value       = google_service_account.workflow.email
}

output "metadata_database_instance" {
  description = "Cloud SQL instance that stores shared Polaris and Superset metadata."
  value       = google_sql_database_instance.metadata.name
}

output "polaris_service_url" {
  description = "URL of the Polaris Cloud Run service."
  value       = google_cloud_run_v2_service.polaris.uri
}

output "polaris_bootstrap_job_name" {
  description = "Cloud Run Job that initializes the Polaris schema and realm."
  value       = google_cloud_run_v2_job.polaris_bootstrap.name
}

output "spark_service_account" {
  description = "Runtime service account used by temporary Dataproc clusters."
  value       = google_service_account.spark.email
}

output "spark_load_script_uri" {
  description = "GCS URI of the Spark CSV-to-Iceberg load job."
  value       = "gs://${google_storage_bucket_object.load_events.bucket}/${google_storage_bucket_object.load_events.name}"
}

output "pipeline_workflow_name" {
  description = "Name of the parent workflow that runs the end-to-end pipeline."
  value       = google_workflows_workflow.pipeline.name
}

output "superset_service_url" {
  description = "Public URL of the Superset Cloud Run service."
  value       = google_cloud_run_v2_service.superset.uri
}

output "superset_bootstrap_job_name" {
  description = "Cloud Run Job that initializes Superset metadata and the administrator."
  value       = google_cloud_run_v2_job.superset_bootstrap.name
}
