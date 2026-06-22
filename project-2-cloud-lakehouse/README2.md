# Project 2: Cloud Lakehouse

An end-to-end cloud-native data and machine learning platform that provisions its own infrastructure, transforms e-commerce clickstream events into session-level features, trains purchase-conversion models on Spark, and publishes interactive dashboards using open lakehouse technologies on Google Cloud.

---


| Section                           | Contents                                                                                                                                                                                               |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **1. What This Project Does**     | 1.1 Problem Statement<br>1.2 Inputs and Outputs<br>1.3 End-to-End Deployment Flow                                                                                                                      |
| **2. Follow One Deployment**      | 2.1 Infrastructure Provisioning<br>2.2 Platform Initialization<br>2.3 Pipeline Execution<br>2.4 Dashboard                                                                                              |
| **3. Architecture** | 3.1 Local-to-Cloud Component Mapping<br>3.2 End-to-End Pipeline Flow<br>3.3 Cloud Resource Inventory<br>3.4 Deployment Sequence<br>3.5 Provisioning Duration |
| **4. Why These Choices**          | 4.1 Why Not BigQuery?<br>4.2 Why Polaris?<br>4.3 Why Cloud Workflows Instead of Airflow?<br>4.4 Why Ephemeral Dataproc?<br>4.5 Why Terraform?                                                        |
| **5. Deployment**                 | 5.1 Prerequisites<br>5.2 Repository Structure<br>5.3 Setup<br>5.4 Services<br>5.5 Teardown                                                                                                             |
| **6. Results**                    | 6.1 Provisioning<br>6.2 Pipeline Execution<br>6.3 Model Evaluation Dashboard<br>6.4 Cost                                                                                                               |
| **7. Reflections and Next Steps** | 7.1 How can a local lakehouse be migrated to the cloud?<br>7.2 How can cloud infrastructure become reproducible?<br>7.3 How can you adopt cloud-native services without surrendering data portability? |

# 1. What This Project Does

## 1.1 Problem Statement

Project 1 demonstrated that a modern lakehouse can process 42 million e-commerce clickstream events on commodity hardware using open-source technologies.

This project asks a different question:

> How do we migrate the same architecture to the cloud while preserving openness, portability, and reproducibility?

This project extends the local lakehouse into a cloud-native environment using Infrastructure as Code, managed compute, and open data standards — without rebuilding around vendor-specific services.

The project explores three questions:

1. Which local components translate directly to cloud equivalents, and which require rethinking?
2. How can cloud infrastructure become reproducible, auditable, and repeatable?
3. How can cloud-native services be adopted without surrendering data portability?

## 1.2 Inputs and Outputs

### Inputs

The platform combines infrastructure definitions, deployment artifacts, and analytical workloads into a single reproducible system.

| Input                   | Purpose                                                           |
| ----------------------- | ----------------------------------------------------------------- |
| Terraform Configuration | Provision cloud infrastructure and IAM resources                  |
| Container Images        | Deploy Polaris and Superset to Cloud Run                          |
| Spark Jobs              | Execute ingestion, transformation, and machine learning workloads |
| dbt Project             | Build analytical models and feature stores on Spark               |
| Workflow Definitions    | Orchestrate infrastructure and data pipeline execution            |

### Outputs

The platform produces the following artifacts:

| Output                         | Purpose                                                                 |
| ------------------------------ | ----------------------------------------------------------------------- |
| Cloud Lakehouse Platform       | Fully provisioned cloud environment for analytics and machine learning  |
| Iceberg Lakehouse Tables       | Store raw, transformed, and curated datasets using an open table format |
| Session-Level Feature Store    | Provide machine-learning-ready training data                            |
| Model and Evaluation Artifacts | Publish trained models, predictions, and performance metrics            |
| Superset Dashboard             | Visualize model performance through interactive dashboards              |

## 1.3 End-to-End Platform Flow

The workflow below summarizes the major stages involved in provisioning the platform, executing the data pipeline, and publishing analytical outputs.

```mermaid
flowchart LR
    A[Infrastructure as Code]
    --> B[Cloud Platform]
    --> C[Data Pipeline]
    --> D[Analytics & ML]
    --> E[Dashboard & Metrics]
```

# 2. Follow One Deployment

This section follows a single execution of `bash scripts/setup.sh` from an empty Google Cloud project to a fully operational cloud lakehouse platform.

The deployment proceeds through four stages:

1. Infrastructure Provisioning
2. Platform Initialization
3. Pipeline Execution
4. Dashboard Publication

## 2.1 Infrastructure Provisioning

Terraform provisions the cloud resources required by the platform, including Cloud Storage, Cloud SQL, Cloud Run, IAM, Secret Manager, and Workflows.

At the end of this stage, the infrastructure exists but the platform has not yet been configured.

## 2.2 Platform Initialization

Polaris and Superset are bootstrapped using Cloud Run Jobs.

These initialization steps create the catalog, configure authentication, perform database migrations, and prepare the platform for use.

At the end of this stage, the lakehouse platform is operational.

## 2.3 Pipeline Execution

A Cloud Workflow creates an ephemeral Spark cluster and executes the data pipeline.

The pipeline extracts clickstream data, loads Iceberg tables through Polaris, builds session-level features with dbt, trains an XGBoost model, and publishes evaluation metrics.

Once processing completes, the Spark cluster is deleted.

## 2.4 Dashboard Publication

Finally, Superset datasets, charts, and dashboards are refreshed using the newly generated metrics.

The result is a fully provisioned cloud-native lakehouse platform that can be deployed from source code using a single command.

# 3. Architecture

## 3.1 Local-to-Cloud Component Mapping

Project 2 preserves the core architecture from Project 1 while replacing local infrastructure with managed cloud services. Some components translate directly, while others require redesign to take advantage of cloud-native capabilities.

| Capability     | Project 1      | Project 2       |
| -------------- | -------------- | --------------- |
| Orchestration  | Airflow        | Cloud Workflows |
| Compute        | DuckDB         | Dataproc Spark  |
| Storage        | RustFS         | Cloud Storage   |
| Catalog        | Polaris        | Polaris         |
| Table Format   | Iceberg        | Iceberg         |
| Transformation | dbt            | dbt             |
| Visualization  | Superset       | Superset        |
| Infrastructure | Docker Compose | Terraform       |

The migration preserves the separation between storage, metadata, compute, and consumption layers while replacing local services with managed cloud equivalents.

## 3.2 End-to-End Pipeline Flow

The architecture follows the same high-level ELT pattern introduced in Project 1. Infrastructure is provisioned using Terraform, platform services are initialized, and an orchestrated Spark workflow executes the data pipeline before publishing analytical outputs.

```mermaid
flowchart LR
    A[Terraform]
    --> B[Cloud Platform]
    --> C[Workflow]
    --> D[Spark Pipeline]
    --> E[Iceberg Lakehouse]
    --> F[Superset Dashboard]
```

## 3.3 Cloud Resource Inventory

Terraform provisions the persistent cloud services, while the Spark cluster is created temporarily by the workflow at runtime.

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
## 3.4 Deployment Sequence

The deployment flow separates infrastructure provisioning, platform initialization, pipeline execution, and dashboard configuration.

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
    GCloud-->>GCP: Polaris Bootstrap
    GCP->>GCP: Cloud Run Job
    Host->>Terraform: docker run
    Terraform->>Terraform: Init
    Terraform->>GCP: Provision Polaris Catalog
    Host->>GCloud: docker run
    GCloud-->>GCP: Superset Bootstrap
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
    GCloud-->>GCP: Refresh Superset Metrics
    GCP->>GCP: Cloud Run Job
    Host->>Terraform: docker run
    Terraform->>Terraform: Init
    Terraform->>GCP: Provision Superset Assets
    end
```
## 3.5 Provisioning Duration

Provisioning times vary significantly across resources. Most services are created within seconds, while Cloud SQL accounts for the majority of deployment time.

```mermaid
gantt
    title Terraform apply
    dateFormat mm:ss
    axisFormat %M:%S
    archive_file.dbt_project :00:00, 00:00
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