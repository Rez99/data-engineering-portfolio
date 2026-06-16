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

output "ml_job_name" {
  description = "Name of the machine-learning Cloud Run Job."
  value       = google_cloud_run_v2_job.ml.name
}

output "ml_service_account" {
  description = "Runtime service account used by the machine-learning job."
  value       = google_service_account.ml.email
}

output "train_workflow_name" {
  description = "Name of the workflow that runs XGBoost training."
  value       = google_workflows_workflow.train.name
}

output "extract_workflow_name" {
  description = "Name of the workflow that runs the extraction job."
  value       = google_workflows_workflow.extract.name
}

output "workflow_service_account" {
  description = "Runtime service account used by Workflows."
  value       = google_service_account.workflow.email
}

output "polaris_database_instance" {
  description = "Cloud SQL instance that stores Polaris state."
  value       = google_sql_database_instance.polaris.name
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

output "load_workflow_name" {
  description = "Name of the workflow that runs the temporary Spark load."
  value       = google_workflows_workflow.load.name
}

output "dbt_smoke_workflow_name" {
  description = "Name of the workflow that verifies dbt-to-Spark-to-Polaris integration."
  value       = google_workflows_workflow.dbt_smoke.name
}

output "transform_workflow_name" {
  description = "Name of the workflow that builds the session-level feature table."
  value       = google_workflows_workflow.transform.name
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
