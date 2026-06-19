# Project 2: Cloud Lakehouse

*Project 2 extends the local lakehouse platform into a cloud-native data platform, demonstrating modern data engineering using infrastructure-as-code while prioritizing open standards and portability over vendor-specific services.*

---

## Table of Contents

1. Project Overview
2. Planning
3. Provisioning
4. Deployment
5. Reflections and Next Steps

---

## 2. Planning
## 3. Provisioning

```                                                                                         
 ┌─ APIs ────────────────┐  ┌─ Cloud Run ───────────┐  ┌─ Artifact Registry ────────────┐   
 │ Artifact Registry     │  │ Services              │  │ superset                       │   
 │ Compute Engine        │  │  └ polaris            │  └────────────────────────────────┘   
 │ Dataproc              │  │  └ superset           │                                       
 │ IAM Credentials       │  │ Jobs                  │                                       
 │ Cloud SQL Admin       │  │  └ polaris-bootstrap  │  ┌─ Secret Manager ───────────────┐   
 │ Cloud Run             │  │  └ superset-bootstrap │  │ polaris                        │   
 │ Secret Manager        │  └───────────────────────┘  └────────────────────────────────┘   
 │ Service Usage         │                                                                  
 │ Cloud Storage         │                                                                  
 │ Workflows             │  ┌─ Workflows ───────────┐  ┌─ IAM ──────────────────────────┐   
 └───────────────────────┘  │ pipeline              │  │ google_*_iam_member (Bindings) │   
                            └───────────────────────┘  │  └ Who? (Service Accounts)     │   
 ┌─ Cloud Storage ───────┐                             │  └ What? (Roles)               │   
 │ spark                 │                             │  └ Where? (Resources)          │   
 │  └ extract.py         │  ┌─ Spark* ──────────────┐  └────────────────────────────────┘   
 │  └ load_events.py     │  │ Cluster               │                                       
 │  └ run_dbt.py         │  │  └ spark-pipeline     │  ┌─ Cloud SQL ────────────────────┐             
 │  └ train.py           │  │                       │  │ PostgreSQL Instance            │             
 │  └ train_model.py     │  │ Created by workflow   │  │  └ metadata                    │             
 │ dbt                   │  │ at runtime            │  │      └ polaris DB              │             
 │  └ dbt-project.zip    │  │ * not Terraform       │  │      └ superset DB             │             
 └───────────────────────┘  └───────────────────────┘  └────────────────────────────────┘             
                                                                                        
```

```mermaid
gantt
    title Terraform apply
    dateFormat mm:ss
    axisFormat %M:%S
    section refresh
    archive_file.dbt_project :00:00, 00:00
    section other
    google_artifact_registry_repository.superset :00:03, 00:04
    google_project_iam_member.dataproc_operator :00:04, 00:12
    google_service_account.workflow :00:04, 00:17
    google_service_account.superset :00:04, 00:16
    google_service_account.polaris :00:04, 00:17
    google_service_account.spark :00:04, 00:19
    google_storage_bucket.lakehouse :00:08, 00:09
    google_storage_bucket_object.dbt_project :00:09, 00:10
    google_storage_bucket_object.train_model :00:12, 00:12
    google_storage_bucket_object.train :00:12, 00:12
    google_storage_bucket_object.extract :00:12, 00:12
    google_sql_database_instance.metadata :00:12, 11:43
    google_project_service_identity.workflows :00:12, 00:13
    google_storage_bucket_object.run_dbt :00:12, 00:12
    google_storage_bucket_object.load_events :00:12, 00:13
    google_secret_manager_secret.polaris_root_client_secret :00:12, 00:13
    google_secret_manager_secret_version.polaris_root_client_secret :00:13, 00:14
    google_storage_bucket_iam_member.superset_metrics_viewer :00:16, 00:21
    google_project_iam_member.superset_cloud_sql_client :00:16, 00:24
    google_project_iam_member.workflow_dataproc_editor :00:17, 00:24
    google_project_iam_member.workflow_run_viewer :00:17, 00:25
    google_project_iam_member.polaris_cloud_sql_client :00:17, 00:24
    google_storage_bucket_iam_member.polaris_warehouse_object_admin :00:17, 00:29
    google_service_account_iam_member.polaris_self_token_creator :00:17, 00:21
    google_storage_bucket_iam_member.spark_bucket_object_admin :00:19, 00:29
    google_service_account_iam_member.workflow_spark_user :00:19, 00:23
    google_secret_manager_secret_iam_member.spark_polaris_secret_accessor :00:21, 00:25
    google_project_iam_member.spark_dataproc_worker :00:21, 00:29
    google_service_account_iam_member.dataproc_operator_spark_user :00:23, 00:27
    google_sql_user.superset :11:43, 11:50
    google_sql_database.polaris :11:43, 11:52
    google_sql_database.superset :11:43, 11:48
    google_sql_user.polaris :11:43, 11:55
    google_cloud_run_v2_job.superset_bootstrap :11:50, 12:01
    google_cloud_run_v2_service.superset :11:50, 12:11
    google_cloud_run_v2_service.polaris :11:55, 12:26
    google_cloud_run_v2_job.polaris_bootstrap :11:55, 11:56
    google_cloud_run_v2_service_iam_member.superset_public :12:11, 12:16
    google_cloud_run_v2_service_iam_member.polaris_public :12:26, 12:35
    google_cloud_run_v2_service_iam_member.spark_polaris_invoker :12:26, 12:31
    google_workflows_workflow.pipeline :12:26, 12:37
```

google_*_iam_member (Bindings)
 └ Who? (Service Accounts)
 └ What? (Roles) 
 └ Where? (Resources)
---------------------------------------------------
## old

Project 2 migrates the local lakehouse from Project 1 to Google Cloud while
retaining Apache Iceberg, Apache Polaris, dbt Core, and XGBoost.

The core data-flow milestones are complete: extract, load, transform, train,
and consume. The current pipeline runs through one parent Google Workflow that
creates a temporary Spark cluster, runs the data pipeline, deletes the cluster,
and refreshes Superset.
See [docs/PROJECT_2_SPEC.md](docs/PROJECT_2_SPEC.md) for the delivery plan and
[docs/architecture.md](docs/architecture.md) for the selected architecture.

## Repository Structure

The project is organized around a simple deployment lifecycle:

```text
terraform/      # Stage 1: PROVISION and manage Terraform-owned state
  main/         # Stage 1a: PROVISION cloud infrastructure
  polaris/      # Stage 2b: MANAGE Polaris catalog state
  superset/     # Stage 2c: MANAGE Superset dashboards/charts/datasets
scripts/        # Stage runners: setup, teardown, bootstrap, pipeline trigger
deployment/     # Stage 3: PUBLISH deployable project artifacts
```

```text
project-2-cloud-lakehouse/
├── deployment/           # Stage 3: PUBLISH deployable project artifacts
│   ├── containers/       # Cloud Run image build contexts for platform services
│   ├── dbt/              # dbt project adapted for Spark
│   ├── spark/            # Spark job entrypoints uploaded to GCS
│   ├── workflows/        # Google Cloud Workflows source definitions
│   └── manifest.example.json
├── docs/                 # Architecture documentation, ADRs, and project guidance
│   ├── AGENTS.md
│   ├── PROJECT_2_SPEC.md
│   ├── architecture.md
│   └── adr/
├── scripts/              # Local shell entrypoints
│   ├── bootstrap-polaris.sh
│   ├── bootstrap-superset.sh
│   ├── destroy.sh
│   ├── run-pipeline.sh
│   └── setup.sh
├── terraform/            # Terraform roots grouped by ownership boundary
│   ├── main/             # Stage 1a: PROVISION cloud infrastructure
│   ├── polaris/          # Stage 2b: MANAGE Polaris catalog state
│   └── superset/         # Stage 2c: MANAGE Superset dashboards/charts/datasets
├── tests/
│   ├── ingestion/        # Unit tests for ingestion code
│   ├── integration/      # Cross-service validation and smoke tests
│   └── ml/               # Unit tests for ML code
```

Implementation is organized so infrastructure, platform bootstrap, deployable
artifacts, and pipeline execution remain visible as separate stages.

## Local Tooling

Docker is the only local runtime required. Google Cloud CLI and Terraform run
from pinned container images, so contributors do not need to install either
tool locally.

The login and Application Default Credentials are stored in the gitignored
`.credentials/gcloud/` directory. The commands below mount those credentials
into the Terraform and Google Cloud CLI containers.

For an automated end-to-end run:

```bash
./scripts/setup.sh
```

To destroy all Terraform-managed resources:

```bash
./scripts/destroy.sh
```

To trigger the deployed end-to-end pipeline without rebuilding or reprovisioning:

```bash
./scripts/run-pipeline.sh
```

Credential files, Terraform state, plans, and local variable files are ignored
by Git.

## How to run

1. Start the Terraform container:

```bash
docker run -d \
  --name lakehouse-terraform \
  --entrypoint /bin/sh \
  -v "$PWD:/workspace" \
  -v "$PWD/.credentials/gcloud/application_default_credentials.json:/credentials/gcp.json:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/gcp.json \
  -w /workspace/terraform/main \
  hashicorp/terraform:1.15.6 \
  -c "sleep infinity"
```
2. Enter it:
```bash
docker exec -it lakehouse-terraform /bin/sh
```
3. Inside the container, provision Artifact Registry first:
```bash
terraform init
terraform apply \
  -target=google_artifact_registry_repository.superset
```

4. Exit the Terraform container and authenticate Docker with Artifact Registry:

```bash
exit

docker run --rm \
  -v "$PWD/.credentials/gcloud:/config" \
  -e CLOUDSDK_CONFIG=/config \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:572.0.0-stable \
  gcloud auth print-access-token |
docker login \
  --username oauth2accesstoken \
  --password-stdin \
  us-central1-docker.pkg.dev
```

5. Build and push the Superset image:

```bash
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --tag us-central1-docker.pkg.dev/rez-cloud-lakehouse/superset/superset:dev-amd64 \
  --push \
  deployment/containers/superset
```

6. Re-enter the Terraform container:

```bash
docker exec -it lakehouse-terraform /bin/sh
```

7. Provision the full platform and deploy GCS-backed pipeline artifacts:

```bash
terraform apply \
  -var='superset_image=us-central1-docker.pkg.dev/rez-cloud-lakehouse/superset/superset:dev-amd64'
```

8. Bootstrap Polaris and Superset, then configure their managed assets:

```bash
exit

scripts/bootstrap-polaris.sh
scripts/bootstrap-superset.sh
```

9. Trigger the deployed parent workflow:

```bash
./scripts/run-pipeline.sh
```

```mermaid
sequenceDiagram
    autonumber

    participant Host
    box Docker
    participant Terraform@{ "type" : "collections" }
    participant GCloud@{ "type" : "collections" }
    end

    participant GCP
    
    rect rgb(235,245,255)
    Note over Host,GCP: Infrastructure Provisioning
    Host->>Terraform: docker run
    Terraform->>Terraform: Init
    Terraform->>GCP: Provision Artifact Registry
    Host->>GCloud: docker run
    GCloud-->>GCP: Request Access Token
    GCP-->>Host: Receive Access Token
    Host->>GCP: Build & Push Superset Image
    Terraform->>GCP: Provision Remaining Resources
    end

    rect rgb(235,255,235)
    Note over Host,GCP: Platform Initialization
    Host->>GCloud: docker run
    GCloud-->>GCP: Polaris Bootsrap
    GCP->>GCP: Cloud Run Job
    Host->>Terraform: docker run
    Terraform->>Terraform: Init
    Terraform->>GCP: Provision Polaris Catalog
    Host->>GCloud: docker run
    GCloud-->>GCP: Superset Bootsrap
    GCP->>GCP: Cloud Run Job
    end

    rect rgb(255,248,235)
    Note over Host,GCP: Pipeline Execution
    Host->>GCloud: docker run
    GCloud-->>GCP: Start Pipeline
    GCP->>GCP: Spark Jobs
    end 

    rect rgb(255,235,245)
    Note over Host,GCP: Consumption Configuration
    Host->>GCloud: docker run
    GCloud-->>GCP: Refresh Superset Metics
    GCP->>GCP: Cloud Run Job
    Host->>Terraform: docker run
    Terraform->>Terraform: Init
    Terraform->>GCP: Provision Superset Assets
    end
```
