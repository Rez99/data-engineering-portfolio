locals {
  required_services = toset([
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
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
