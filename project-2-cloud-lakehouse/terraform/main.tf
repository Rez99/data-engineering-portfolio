locals {
  required_services = toset([
    "artifactregistry.googleapis.com",
    "compute.googleapis.com",
    "dataproc.googleapis.com",
    "iamcredentials.googleapis.com",
    "sqladmin.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
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

  depends_on = [google_project_service.required["storage.googleapis.com"]]
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

  depends_on = [google_project_service.required["artifactregistry.googleapis.com"]]
}

resource "google_service_account" "workflow" {
  project      = var.project_id
  account_id   = "lakehouse-workflow"
  display_name = "Lakehouse extraction workflow"
  description  = "Runtime identity used by Workflows to execute pipeline jobs."
}

resource "google_project_iam_member" "workflow_run_viewer" {
  project = var.project_id
  role    = "roles/run.viewer"
  member  = "serviceAccount:${google_service_account.workflow.email}"
}

resource "google_project_iam_member" "workflow_dataproc_editor" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.workflow.email}"
}

resource "google_project_service_identity" "workflows" {
  provider = google-beta

  project = var.project_id
  service = "workflows.googleapis.com"

  depends_on = [google_project_service.required]
}

resource "google_workflows_workflow" "pipeline" {
  project             = var.project_id
  name                = "lakehouse-pipeline"
  region              = var.region
  description         = "Runs the complete lakehouse pipeline with one temporary Spark cluster for load and transform."
  service_account     = google_service_account.workflow.id
  deletion_protection = false

  source_contents = templatefile("${path.module}/../deployment/workflows/pipeline.yaml", {
    project_id              = var.project_id
    region                  = var.region
    cluster_name            = "lakehouse-spark-pipeline"
    spark_service_account   = google_service_account.spark.email
    staging_bucket          = google_storage_bucket.validation.name
    extract_script_uri      = "gs://${google_storage_bucket_object.extract.bucket}/${google_storage_bucket_object.extract.name}"
    load_script_uri         = "gs://${google_storage_bucket_object.load_events.bucket}/${google_storage_bucket_object.load_events.name}"
    runner_script_uri       = "gs://${google_storage_bucket_object.run_dbt.bucket}/${google_storage_bucket_object.run_dbt.name}"
    train_script_uri        = "gs://${google_storage_bucket_object.train_model.bucket}/${google_storage_bucket_object.train_model.name}"
    train_library_uri       = "gs://${google_storage_bucket_object.train.bucket}/${google_storage_bucket_object.train.name}"
    dbt_project_archive_uri = "gs://${google_storage_bucket_object.dbt_project.bucket}/${google_storage_bucket_object.dbt_project.name}"
    source_url              = var.clickstream_source_url
    raw_bucket              = google_storage_bucket.validation.name
    raw_object              = "raw/2019-Oct-1000000.csv.gz"
    raw_csv_uri             = "gs://${google_storage_bucket.validation.name}/raw/2019-Oct-1000000.csv.gz"
    expected_rows           = "1000000"
    polaris_url             = google_cloud_run_v2_service.polaris.uri
    polaris_secret          = google_secret_manager_secret.polaris_root_client_secret.id
    artifact_bucket         = google_storage_bucket.validation.name
    artifact_prefix         = "ml/xgboost_conversion"
    feature_export_uri      = "gs://${google_storage_bucket.validation.name}/ml/features/"
  })

  labels = {
    environment = "demo"
    project     = "cloud-lakehouse"
    stage       = "pipeline"
  }

  depends_on = [
    google_project_iam_member.workflow_dataproc_editor,
    google_project_iam_member.workflow_run_viewer,
    google_project_service.required,
    google_project_service_identity.workflows,
    google_secret_manager_secret_iam_member.spark_polaris_secret_accessor,
    google_service_account_iam_member.workflow_spark_user,
    google_storage_bucket_object.dbt_project,
    google_storage_bucket_object.extract,
    google_storage_bucket_object.load_events,
    google_storage_bucket_object.run_dbt,
    google_storage_bucket_object.train,
    google_storage_bucket_object.train_model,
  ]
}

resource "google_sql_database_instance" "polaris" {
  project          = var.project_id
  name             = "lakehouse-polaris"
  region           = var.region
  database_version = "POSTGRES_16"

  deletion_protection = false

  settings {
    tier              = "db-f1-micro"
    edition           = "ENTERPRISE"
    availability_type = "ZONAL"
    disk_size         = 10
    disk_type         = "PD_SSD"

    backup_configuration {
      enabled = false
    }

    ip_configuration {
      ipv4_enabled = true
    }
  }

  depends_on = [google_project_service.required]
}

resource "google_sql_database" "polaris" {
  project  = var.project_id
  name     = var.polaris_database_name
  instance = google_sql_database_instance.polaris.name
}

resource "google_sql_user" "polaris" {
  project         = var.project_id
  name            = var.polaris_database_user
  instance        = google_sql_database_instance.polaris.name
  password        = var.polaris_database_password
  deletion_policy = "ABANDON"
}

resource "google_sql_database" "superset" {
  project  = var.project_id
  name     = "superset"
  instance = google_sql_database_instance.polaris.name
}

resource "google_sql_user" "superset" {
  project         = var.project_id
  name            = "superset"
  instance        = google_sql_database_instance.polaris.name
  password        = var.superset_database_password
  deletion_policy = "ABANDON"
}

resource "google_service_account" "superset" {
  project      = var.project_id
  account_id   = "lakehouse-superset"
  display_name = "Lakehouse Superset service"
  description  = "Runtime identity for the Superset service and bootstrap job."
}

resource "google_project_iam_member" "superset_cloud_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.superset.email}"
}

resource "google_storage_bucket_iam_member" "superset_metrics_viewer" {
  bucket = google_storage_bucket.validation.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.superset.email}"
}

resource "google_cloud_run_v2_job" "superset_bootstrap" {
  project  = var.project_id
  name     = "lakehouse-superset-bootstrap"
  location = var.region

  deletion_protection = false
  launch_stage        = "BETA"

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = google_service_account.superset.email
      max_retries     = 0
      timeout         = "900s"

      containers {
        name  = "cloud-sql-proxy"
        image = var.cloud_sql_proxy_image

        args = [
          "--address=0.0.0.0",
          "--port=5432",
          google_sql_database_instance.polaris.connection_name,
        ]

        startup_probe {
          initial_delay_seconds = 1
          timeout_seconds       = 1
          period_seconds        = 1
          failure_threshold     = 30

          tcp_socket {
            port = 5432
          }
        }

        resources {
          limits = {
            cpu    = "0.25"
            memory = "256Mi"
          }
        }
      }

      containers {
        name       = "superset-bootstrap"
        image      = var.superset_image
        depends_on = ["cloud-sql-proxy"]

        command = ["/bin/bash", "-c"]
        args = [<<-EOT
          set -e
          superset db upgrade
          if ! superset fab list-users | grep -q "${var.superset_admin_username}"; then
            superset fab create-admin \
              --username "${var.superset_admin_username}" \
              --firstname "Superset" \
              --lastname "Admin" \
              --email "admin@example.com" \
              --password "${var.superset_admin_password}"
          fi
          superset init
        EOT
        ]

        env {
          name  = "SUPERSET_DATABASE_URI"
          value = "postgresql+psycopg2://superset:${var.superset_database_password}@127.0.0.1:5432/superset"
        }

        env {
          name  = "SUPERSET_SECRET_KEY"
          value = var.superset_secret_key
        }

        env {
          name  = "METRICS_BUCKET"
          value = google_storage_bucket.validation.name
        }

        env {
          name  = "METRICS_PREFIX"
          value = "ml/xgboost_conversion/metrics"
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }
      }
    }
  }

  labels = {
    environment = "demo"
    project     = "cloud-lakehouse"
    stage       = "superset-bootstrap"
  }

  depends_on = [
    google_artifact_registry_repository.pipeline,
    google_project_iam_member.superset_cloud_sql_client,
    google_storage_bucket_iam_member.superset_metrics_viewer,
    google_sql_database.superset,
    google_sql_user.superset,
  ]
}

resource "google_cloud_run_v2_service" "superset" {
  project  = var.project_id
  name     = "lakehouse-superset"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  deletion_protection = false

  template {
    service_account = google_service_account.superset.email

    scaling {
      min_instance_count = 0
      max_instance_count = 1
    }

    containers {
      name  = "cloud-sql-proxy"
      image = var.cloud_sql_proxy_image

      args = [
        "--address=0.0.0.0",
        "--port=5432",
        google_sql_database_instance.polaris.connection_name,
      ]

      startup_probe {
        initial_delay_seconds = 1
        timeout_seconds       = 1
        period_seconds        = 1
        failure_threshold     = 30

        tcp_socket {
          port = 5432
        }
      }

      resources {
        limits = {
          cpu    = "0.25"
          memory = "256Mi"
        }
      }
    }

    containers {
      name       = "superset"
      image      = var.superset_image
      depends_on = ["cloud-sql-proxy"]

      ports {
        container_port = 8080
      }

      env {
        name  = "SUPERSET_DATABASE_URI"
        value = "postgresql+psycopg2://superset:${var.superset_database_password}@127.0.0.1:5432/superset"
      }

      env {
        name  = "SUPERSET_SECRET_KEY"
        value = var.superset_secret_key
      }

      env {
        name  = "METRICS_BUCKET"
        value = google_storage_bucket.validation.name
      }

      env {
        name  = "METRICS_PREFIX"
        value = "ml/xgboost_conversion/metrics"
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "2Gi"
        }
      }
    }
  }

  labels = {
    environment = "demo"
    project     = "cloud-lakehouse"
    stage       = "consume"
  }

  depends_on = [
    google_artifact_registry_repository.pipeline,
    google_project_iam_member.superset_cloud_sql_client,
    google_storage_bucket_iam_member.superset_metrics_viewer,
    google_sql_database.superset,
    google_sql_user.superset,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "superset_public" {
  project  = google_cloud_run_v2_service.superset.project
  location = google_cloud_run_v2_service.superset.location
  name     = google_cloud_run_v2_service.superset.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_service_account" "polaris" {
  project      = var.project_id
  account_id   = "lakehouse-polaris"
  display_name = "Lakehouse Polaris service"
  description  = "Runtime identity for Polaris and its Cloud SQL connection."
}

resource "google_project_iam_member" "polaris_cloud_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.polaris.email}"
}

resource "google_storage_bucket_iam_member" "polaris_warehouse_object_admin" {
  bucket = google_storage_bucket.validation.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.polaris.email}"
}

resource "google_service_account_iam_member" "polaris_self_token_creator" {
  service_account_id = google_service_account.polaris.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.polaris.email}"
}

resource "google_service_account" "spark" {
  project      = var.project_id
  account_id   = "lakehouse-spark"
  display_name = "Lakehouse Spark cluster"
  description  = "Runtime identity for temporary Dataproc clusters."
}

resource "google_project_iam_member" "spark_dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.spark.email}"
}

resource "google_storage_bucket_iam_member" "spark_bucket_object_admin" {
  bucket = google_storage_bucket.validation.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.spark.email}"
}

resource "google_cloud_run_v2_service_iam_member" "spark_polaris_invoker" {
  project  = google_cloud_run_v2_service.polaris.project
  location = google_cloud_run_v2_service.polaris.location
  name     = google_cloud_run_v2_service.polaris.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.spark.email}"
}

resource "google_cloud_run_v2_service_iam_member" "polaris_public" {
  project  = google_cloud_run_v2_service.polaris.project
  location = google_cloud_run_v2_service.polaris.location
  name     = google_cloud_run_v2_service.polaris.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_project_iam_member" "dataproc_operator" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = var.dataproc_operator_member
}

resource "google_service_account_iam_member" "dataproc_operator_spark_user" {
  service_account_id = google_service_account.spark.name
  role               = "roles/iam.serviceAccountUser"
  member             = var.dataproc_operator_member
}

resource "google_service_account_iam_member" "workflow_spark_user" {
  service_account_id = google_service_account.spark.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.workflow.email}"
}

resource "google_secret_manager_secret" "polaris_root_client_secret" {
  project   = var.project_id
  secret_id = "polaris-root-client-secret"

  replication {
    auto {}
  }

  depends_on = [google_project_service.required]
}

resource "google_secret_manager_secret_version" "polaris_root_client_secret" {
  secret      = google_secret_manager_secret.polaris_root_client_secret.id
  secret_data = var.polaris_root_client_secret
}

resource "google_secret_manager_secret_iam_member" "spark_polaris_secret_accessor" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.polaris_root_client_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.spark.email}"
}

resource "google_storage_bucket_object" "extract" {
  name   = "spark/extract.py"
  bucket = google_storage_bucket.validation.name
  source = "${path.module}/../deployment/spark/extract.py"

  depends_on = [google_project_service.required]
}

resource "google_storage_bucket_object" "load_events" {
  name   = "spark/load_events.py"
  bucket = google_storage_bucket.validation.name
  source = "${path.module}/../deployment/spark/load_events.py"

  depends_on = [google_project_service.required]
}

data "archive_file" "dbt_project" {
  type        = "zip"
  source_dir  = "${path.module}/../deployment/dbt"
  output_path = "${path.module}/dbt-project.zip"

  excludes = [
    ".user.yml",
    "logs",
    "logs/*",
    "target",
    "target/*",
  ]
}

resource "google_storage_bucket_object" "dbt_project" {
  name   = "dbt/dbt-project.zip"
  bucket = google_storage_bucket.validation.name
  source = data.archive_file.dbt_project.output_path
}

resource "google_storage_bucket_object" "run_dbt" {
  name   = "spark/run_dbt.py"
  bucket = google_storage_bucket.validation.name
  source = "${path.module}/../deployment/spark/run_dbt.py"

  depends_on = [google_project_service.required]
}

resource "google_storage_bucket_object" "train_model" {
  name   = "spark/train_model.py"
  bucket = google_storage_bucket.validation.name
  source = "${path.module}/../deployment/spark/train_model.py"

  depends_on = [google_project_service.required]
}

resource "google_storage_bucket_object" "train" {
  name   = "spark/train.py"
  bucket = google_storage_bucket.validation.name
  source = "${path.module}/../deployment/spark/train.py"

  depends_on = [google_project_service.required]
}

resource "google_cloud_run_v2_service" "polaris" {
  project  = var.project_id
  name     = "lakehouse-polaris"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  deletion_protection = false

  template {
    service_account = google_service_account.polaris.email

    scaling {
      min_instance_count = 0
      max_instance_count = 1
    }

    containers {
      name  = "cloud-sql-proxy"
      image = var.cloud_sql_proxy_image

      args = [
        "--address=0.0.0.0",
        "--port=5432",
        google_sql_database_instance.polaris.connection_name,
      ]

      resources {
        limits = {
          cpu    = "0.25"
          memory = "256Mi"
        }
      }
    }

    containers {
      name  = "polaris"
      image = var.polaris_image

      ports {
        container_port = 8080
      }

      env {
        name  = "QUARKUS_HTTP_PORT"
        value = "8080"
      }

      env {
        name  = "POLARIS_PERSISTENCE_TYPE"
        value = "relational-jdbc"
      }

      env {
        name  = "QUARKUS_DATASOURCE_JDBC_URL"
        value = "jdbc:postgresql://127.0.0.1:5432/${google_sql_database.polaris.name}"
      }

      env {
        name  = "QUARKUS_DATASOURCE_USERNAME"
        value = google_sql_user.polaris.name
      }

      env {
        name  = "QUARKUS_DATASOURCE_PASSWORD"
        value = var.polaris_database_password
      }

      env {
        name  = "POLARIS_REALM_CONTEXT_REALMS"
        value = var.polaris_realm
      }

      env {
        name  = "QUARKUS_OTEL_SDK_DISABLED"
        value = "true"
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
      }
    }
  }

  depends_on = [
    google_project_iam_member.polaris_cloud_sql_client,
    google_service_account_iam_member.polaris_self_token_creator,
    google_storage_bucket_iam_member.polaris_warehouse_object_admin,
    google_sql_database.polaris,
    google_sql_user.polaris,
  ]
}

resource "google_cloud_run_v2_job" "polaris_bootstrap" {
  project  = var.project_id
  name     = "lakehouse-polaris-bootstrap"
  location = var.region

  deletion_protection = false
  launch_stage        = "BETA"

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = google_service_account.polaris.email
      max_retries     = 0
      timeout         = "600s"

      containers {
        name  = "cloud-sql-proxy"
        image = var.cloud_sql_proxy_image

        args = [
          "--address=0.0.0.0",
          "--port=5432",
          google_sql_database_instance.polaris.connection_name,
        ]

        startup_probe {
          initial_delay_seconds = 1
          timeout_seconds       = 1
          period_seconds        = 1
          failure_threshold     = 30

          tcp_socket {
            port = 5432
          }
        }

        resources {
          limits = {
            cpu    = "0.25"
            memory = "256Mi"
          }
        }
      }

      containers {
        name       = "polaris-bootstrap"
        image      = var.polaris_admin_image
        depends_on = ["cloud-sql-proxy"]

        args = [
          "bootstrap",
          "-r",
          var.polaris_realm,
          "-c",
          "${var.polaris_realm},${var.polaris_root_client_id},${var.polaris_root_client_secret}",
        ]

        env {
          name  = "POLARIS_PERSISTENCE_TYPE"
          value = "relational-jdbc"
        }

        env {
          name  = "QUARKUS_DATASOURCE_JDBC_URL"
          value = "jdbc:postgresql://127.0.0.1:5432/${google_sql_database.polaris.name}"
        }

        env {
          name  = "QUARKUS_DATASOURCE_USERNAME"
          value = google_sql_user.polaris.name
        }

        env {
          name  = "QUARKUS_DATASOURCE_PASSWORD"
          value = var.polaris_database_password
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }
      }
    }
  }

  labels = {
    environment = "demo"
    project     = "cloud-lakehouse"
    stage       = "polaris-bootstrap"
  }

  depends_on = [
    google_project_iam_member.polaris_cloud_sql_client,
    google_sql_database.polaris,
    google_sql_user.polaris,
  ]
}
