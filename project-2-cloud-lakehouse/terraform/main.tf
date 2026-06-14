locals {
  required_services = toset([
    "artifactregistry.googleapis.com",
    "run.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
    "workflows.googleapis.com",
  ])
}

resource "google_project_service" "required" {
  for_each = local.required_services

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "validation" {
  name     = "${var.project_id}-validation"
  project  = var.project_id
  location = upper(var.region)

  force_destroy               = true
  public_access_prevention    = "enforced"
  uniform_bucket_level_access = true

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    condition {
      age = 1
    }

    action {
      type = "Delete"
    }
  }

  labels = {
    environment = "validation"
    project     = "cloud-lakehouse"
  }

  depends_on = [google_project_service.required]
}

resource "google_artifact_registry_repository" "pipeline" {
  project       = var.project_id
  location      = var.region
  repository_id = "pipeline"
  description   = "Container images for the cloud lakehouse pipeline."
  format        = "DOCKER"

  labels = {
    environment = "demo"
    project     = "cloud-lakehouse"
  }

  depends_on = [google_project_service.required]
}

resource "google_service_account" "ingestion" {
  project      = var.project_id
  account_id   = "lakehouse-ingestion"
  display_name = "Lakehouse ingestion job"
  description  = "Runtime identity for extracting clickstream data into Cloud Storage."
}

resource "google_storage_bucket_iam_member" "ingestion_object_user" {
  bucket = google_storage_bucket.validation.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.ingestion.email}"
}

resource "google_cloud_run_v2_job" "ingestion" {
  project  = var.project_id
  name     = "lakehouse-ingestion"
  location = var.region

  deletion_protection = false

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = google_service_account.ingestion.email
      max_retries     = 1
      timeout         = "600s"

      containers {
        image = var.ingestion_image

        env {
          name  = "SOURCE_URL"
          value = var.clickstream_source_url
        }

        env {
          name  = "DESTINATION_BUCKET"
          value = google_storage_bucket.validation.name
        }

        env {
          name  = "DESTINATION_OBJECT"
          value = "raw/2019-Oct-10000.csv.gz"
        }

        env {
          name  = "MAX_ROWS"
          value = "10000"
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }
    }
  }

  labels = {
    environment = "demo"
    project     = "cloud-lakehouse"
    stage       = "extract"
  }

  depends_on = [
    google_artifact_registry_repository.pipeline,
    google_project_service.required,
    google_storage_bucket_iam_member.ingestion_object_user,
  ]
}

resource "google_service_account" "workflow" {
  project      = var.project_id
  account_id   = "lakehouse-workflow"
  display_name = "Lakehouse extraction workflow"
  description  = "Runtime identity used by Workflows to execute pipeline jobs."
}

resource "google_cloud_run_v2_job_iam_member" "workflow_ingestion_invoker" {
  project  = google_cloud_run_v2_job.ingestion.project
  location = google_cloud_run_v2_job.ingestion.location
  name     = google_cloud_run_v2_job.ingestion.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.workflow.email}"
}

resource "google_project_iam_member" "workflow_run_viewer" {
  project = var.project_id
  role    = "roles/run.viewer"
  member  = "serviceAccount:${google_service_account.workflow.email}"
}

resource "google_project_service_identity" "workflows" {
  provider = google-beta

  project = var.project_id
  service = "workflows.googleapis.com"

  depends_on = [google_project_service.required]
}

resource "google_workflows_workflow" "extract" {
  project             = var.project_id
  name                = "lakehouse-extract"
  region              = var.region
  description         = "Runs and monitors the ecommerce clickstream ingestion job."
  service_account     = google_service_account.workflow.id
  deletion_protection = false

  source_contents = templatefile("${path.module}/../workflows/extract.yaml", {
    project_id = var.project_id
    region     = var.region
    job_name   = google_cloud_run_v2_job.ingestion.name
  })

  labels = {
    environment = "demo"
    project     = "cloud-lakehouse"
    stage       = "extract"
  }

  depends_on = [
    google_cloud_run_v2_job_iam_member.workflow_ingestion_invoker,
    google_project_service_identity.workflows,
    google_project_iam_member.workflow_run_viewer,
    google_project_service.required,
  ]
}
