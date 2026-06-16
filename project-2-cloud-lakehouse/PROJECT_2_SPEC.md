# Project 2 Specification (Draft)

## 1. Project Goal

Project 2 extends the local lakehouse platform developed in Project 1 into a cloud-native data platform. The primary objective is to demonstrate the ability to provision, deploy, and operate a modern data engineering stack using infrastructure-as-code and managed cloud services.

This project is not intended to build a production-scale system. Instead, it should showcase sound engineering judgment, reproducibility, automation, and familiarity with common industry tooling.

---

## 2. Portfolio Narrative

Project 1 demonstrated:

* Local data lakehouse architecture.
* Workflow orchestration with Airflow.
* Object storage and open table formats (RustFS + Iceberg).
* Analytical transformations with dbt.
* Machine learning feature engineering and model training.

Project 2 answers the natural follow-up question:

> "How would you take this local architecture and deploy it in the cloud?"

The emphasis shifts from data modeling and local orchestration toward infrastructure provisioning, cloud deployment, automation, and operational concerns.

---

## 3. Design Principles

### Reproducibility

A reviewer should be able to clone the repository and provision the environment using documented commands without manual configuration.

### Simplicity

Prefer the simplest architecture that demonstrates the target concept. Avoid introducing technologies that do not add portfolio value.

### Incremental Development

Build and validate one component at a time. Every milestone should produce a working artifact before moving to the next.

### Infrastructure as Code

Cloud resources should be provisioned and managed through Terraform wherever practical.

### Verification

Every major component should have a clear acceptance criterion and a straightforward way to verify that it is functioning correctly.

### Documented Decisions

Foundational architecture decisions should be captured as Architecture Decision Records (ADRs). Each ADR should explain the context, chosen approach, alternatives considered, consequences, and cost implications.

---

## 4. Success Criteria

By the end of the project, a reviewer should be able to:

1. Clone the repository.
2. Provision the required cloud infrastructure.
3. Deploy the data platform.
4. Trigger the pipeline.
5. Observe data flowing through the system.
6. Inspect outputs (tables, logs, dashboards, or artifacts).
7. Destroy the infrastructure cleanly.

---

## 5. Major Milestones

The milestones follow the movement of data through the pipeline. Infrastructure
is introduced only when the current data stage requires it.

**Status key:** 🔴 Not started · 🟡 Started · 🟢 Completed

| Milestone | Data outcome | Status |
| --- | --- | --- |
| **M1. Extract** | Public ecommerce clickstream → raw Cloud Storage | 🟢 Completed |
| **M2. Load** | Raw CSV → bronze Iceberg table registered in Polaris | 🟢 Completed |
| **M3. Transform** | Bronze events → queryable session-level feature data | 🟢 Completed |
| **M4. Train** | Session features → XGBoost model, metrics, and evaluation artifacts | 🟢 Completed |
| **M5. Consume** | Reporting outputs → model-performance dashboard | 🟢 Completed |

Architecture selection and repository setup are completed prerequisites rather
than data-flow milestones. ADR-0001 through ADR-0003 are accepted, and
`docs/architecture.md` records the target architecture.

### M1: Extract

**Outcome:** Workflows triggers a container that streams the header and first
10,000 ecommerce clickstream rows from the internet into Cloud Storage.

| Mini-milestone | Deliverable | Why it is needed | Status |
| --- | --- | --- | --- |
| **M1.1 Storage foundation** | Terraform provisions the private raw GCS bucket. | Extracted data requires a durable landing location. | 🟢 Completed |
| **M1.2 Extract compute** | Terraform provisions Artifact Registry, IAM, and the ingestion Cloud Run Job. | The extraction code needs a secure image registry, execution environment, and permission to write to GCS. | 🟢 Completed |
| **M1.3 Extraction logic** | The container streams the header and first 10,000 rows directly into GCS without materializing the full source dataset. | This is the actual Extract operation and its memory-safe sampling behavior. | 🟢 Completed |
| **M1.4 Orchestration** | Workflows invokes and monitors the ingestion Cloud Run Job. | It establishes the orchestration pattern that later pipeline stages will reuse. | 🟢 Completed |
| **M1.5 Validation** | Verify the object location, schema, and exact row count. | A successful job alone does not prove that the extracted data is correct. | 🟢 Completed |

M1 intentionally excludes Cloud Scheduler, Polaris, Cloud SQL, Iceberg, Spark,
dbt, XGBoost, and Looker Studio. Those components are introduced only when a
later data-flow milestone requires them.

M1 was verified by a successful `lakehouse-extract` workflow execution. The
ingestion job independently reopened the uploaded object and confirmed valid
gzip-compressed CSV, exactly 10,000 data rows, and the 9 expected columns.
After verification, Terraform destroys the milestone environment to avoid idle
cloud costs; the configuration can recreate it when the next milestone needs
the shared resources.

### M2: Load

**Outcome:** A temporary Spark cluster converts the raw CSV sample into a
bronze Iceberg table stored in Cloud Storage and registered in Polaris.

| Mini-milestone | Deliverable | Acceptance criterion | Status |
| --- | --- | --- | --- |
| **M2.1 Deploy Polaris** | Cloud SQL for PostgreSQL, a one-time bootstrap job, and a Polaris Cloud Run service. | Bootstrap creates the PostgreSQL schema and `POLARIS` realm; Polaris becomes healthy and can issue a root OAuth token. | 🟢 Completed |
| **M2.2 Configure warehouse** | A Polaris catalog backed by the GCS Iceberg warehouse. | A namespace and test table can be created through Polaris. | 🟢 Completed |
| **M2.3 Enable Spark** | Dataproc API, Spark service account, and required IAM. | A minimal temporary Spark cluster can connect to Polaris. | 🟢 Completed |
| **M2.4 Run the load** | Workflow creates Dataproc, loads CSV into Iceberg through Polaris, and deletes the cluster. | The load completes and the temporary cluster is removed. | 🟢 Completed |
| **M2.5 Validate** | Catalog, schema, row-count, and GCS-file checks. | Polaris reports the table with 10,000 rows and the expected Iceberg files exist in GCS. | 🟢 Completed |

### M3: Transform

**Outcome:** dbt transforms the bronze event table into session-level features
that are ready for machine-learning ingestion.

| Mini-milestone | Deliverable | Acceptance criterion | Status |
| --- | --- | --- | --- |
| **M3.1 Migrate dbt code** | Port the Project 1 dbt models, sources, profiles, and SQL dialect from DuckDB to Spark. | `dbt parse` succeeds. | 🟢 Completed |
| **M3.2 Integrate platform** | Connect dbt, Spark, and Polaris into a working execution stack. | A minimal dbt model successfully queries `bronze.events` through Spark and Polaris. | 🟢 Completed |
| **M3.3 Run transformations** | Execute the sessionization and feature models through the orchestration workflow. | The `gold.features` Iceberg table is built successfully and can be queried. | 🟢 Completed |

### M4: Train

**Outcome:** XGBoost trains on the session-level feature data and writes a
portable model plus evaluation artifacts for downstream reporting.

| Mini-milestone | Deliverable | Acceptance criterion | Status |
| --- | --- | --- | --- |
| **M4.1 Migrate training code** | Port the Project 1 external-memory XGBoost training and evaluation logic into a standalone ML service. | Targeted local tests train and evaluate from synthetic Parquet input and produce the expected artifacts. | 🟢 Completed |
| **M4.2 Integrate training platform** | Connect the feature output in GCS to an XGBoost Cloud Run Job and configure artifact storage. | The Cloud Run Job can read the feature dataset and write a test artifact to GCS. | 🟢 Completed |
| **M4.3 Run training workflow** | Orchestrate the production training and evaluation job after transformation. | The workflow succeeds and GCS contains a non-empty model, metrics, baseline comparison, confusion matrix, feature importance, and ROC curve. | 🟢 Completed |

### M5: Consume

**Outcome:** Apache Superset presents the model evaluation artifacts as the
existing Project 1 dashboard in a cloud-hosted consumption layer.

| Mini-milestone | Deliverable | Acceptance criterion | Status |
| --- | --- | --- | --- |
| **M5.1 Deploy Superset** | Superset Cloud Run service, bootstrap job, and isolated metadata database on the existing Cloud SQL instance. | Superset is healthy at its Cloud Run URL and accepts the configured administrator login. | 🟢 Completed |
| **M5.2 Connect metrics** | Queryable Superset datasets backed by the GCS model-evaluation artifacts. | Superset can query all five metric datasets produced by M4. | 🟢 Completed |
| **M5.3 Import dashboard** | Adapt and import the existing Project 1 Superset YAML assets. | The XGBoost model-evaluation dashboard loads with all charts populated. | 🟢 Completed |

---

## 6. Working Rules for AI Assistants

When collaborating on this project:

* Read this specification before making changes.
* If requirements are ambiguous, ask questions instead of making assumptions.
* Prefer the smallest independently verifiable implementation task.
* Do not modify unrelated files.
* Explain significant architectural decisions before implementing them.
* After completing a task, provide a verification checklist and wait for approval before proceeding.

---

## 7. Non-Goals

This project is **not** intended to:

* Minimize cloud cost at all costs.
* Build a highly available production platform.
* Showcase every popular data engineering technology.
* Eliminate all manual verification.
* Maximize code volume.

The goal is to demonstrate practical engineering skills and the ability to reason about architecture, trade-offs, and deployment.
