# ADR-0002: Evaluate Project 1 to GCP Service Mappings

## Status

Accepted

## Context

Project 2 should preserve the architectural intent of Project 1 while demonstrating practical use of managed cloud services. A direct one-for-one migration is not always desirable: retaining every local technology would preserve continuity but also reproduce operational work that GCP can manage.

The candidate services below are evaluated against:

- Portfolio value
- Continuity with Project 1
- Setup and operational complexity
- Idle and usage-based cost
- Terraform support
- Teardown behavior
- Open-stack portability

This ADR documents the available mappings and their trade-offs. It does not select the final architecture. The choices informed by this analysis are recorded in ADR-0003.

Open-stack portability means keeping ownership of the data, table metadata, and transformation logic independent of a single proprietary analytical engine. It does not require every infrastructure service to be portable across cloud providers.

## Candidate Technology Stack

The `Layer` values follow the participants in the Project 1 architecture diagram. `Compute` is added because Project 1 implicitly used the laptop's CPU and memory through Docker.

Containers are not treated as a separate layer. They are a packaging mechanism used by services such as Cloud Run, Google Kubernetes Engine, and Vertex AI.

| Layer | Project 1 | GCP candidate | What it provides and the main trade-off |
| --- | --- | --- | --- |
| Orchestration | Apache Airflow | - Cloud Composer<br>- Workflows + Cloud Scheduler<br>- Self-managed Airflow | - **Cloud Composer:** Managed Airflow and the strongest DAG continuity, but with persistent environment cost and heavier provisioning.<br>- **Workflows + Cloud Scheduler:** Usage-based orchestration and scheduling with little idle cost, but existing Airflow DAGs cannot run unchanged.<br>- **Self-managed Airflow:** Maximum Airflow compatibility and control, but the project must operate the scheduler, workers, metadata database, networking, and upgrades. |
| Object Storage | RustFS | Cloud Storage | Durable managed object storage for source data, Parquet, Iceberg files, model artifacts, and metrics. |
| Query Engine + Catalog | DuckDB + Polaris + Iceberg | - DuckDB + Polaris + Iceberg on Cloud Storage<br>- Managed Spark + Polaris + Iceberg on Cloud Storage<br>- BigQuery-managed Iceberg<br>- Native BigQuery tables | - **DuckDB + Polaris:** Preserves the Project 1 design, but both DuckDB execution and Polaris hosting must be provided.<br>- **Managed Spark + Polaris:** Adds distributed query capacity while retaining an independent catalog, but introduces Spark complexity and cost.<br>- **BigQuery-managed Iceberg:** Provides managed SQL and Iceberg lifecycle operations, but BigQuery becomes the primary table manager and writer.<br>- **Native BigQuery:** Provides the simplest managed analytical platform, but replaces rather than migrates the independent Iceberg catalog architecture. |
| Transformation | dbt Core | - dbt Core with a temporary managed Spark cluster and Thrift endpoint<br>- PySpark batch jobs on Managed Service for Apache Spark serverless<br>- Dataform with BigQuery | - **dbt Core + managed cluster:** Preserves the existing dbt project, tests, model graph, and documentation while using distributed cloud compute, but requires cluster lifecycle, networking, and a Spark Thrift endpoint.<br>- **PySpark serverless batches:** Fit the finite batch execution model, but replace dbt's transformation workflow and development features.<br>- **Dataform:** Provides a managed GCP-native SQL transformation workflow, but replaces dbt and depends on BigQuery. |
| Machine Learning | XGBoost | - XGBoost in a Cloud Run Job<br>- Vertex AI custom training<br>- BigQuery ML boosted trees | - **Cloud Run Job:** Reuses the existing containerized training code with usage-based compute, but provides fewer managed ML capabilities.<br>- **Vertex AI:** Provides managed training jobs, tracking, and configurable compute, but adds platform concepts and cost.<br>- **BigQuery ML:** Keeps training close to BigQuery data and reduces infrastructure, but substantially changes the Project 1 implementation. |
| Visualization | Apache Superset | - Looker Studio<br>- Self-managed Superset<br>- BigQuery console and saved queries | - **Looker Studio:** Provides managed, no-cost self-service dashboards, but replaces the existing Superset assets.<br>- **Self-managed Superset:** Preserves the existing dashboard and BI workflow, but requires application hosting and a persistent metadata database.<br>- **BigQuery console:** Requires the least setup, but offers a weaker dashboard and portfolio demonstration. |
| Compute | Laptop CPU and RAM through Docker | - Cloud Run Jobs<br>- Cloud Run Services<br>- Compute Engine<br>- Google Kubernetes Engine<br>- Google Cloud Batch | - **Cloud Run Jobs:** Run finite container tasks and exit when complete; suitable for stateless ingestion, transformation, and ML.<br>- **Cloud Run Services:** Host request-driven container applications that can scale down when idle; suitable for APIs or web applications.<br>- **Compute Engine:** Provides a persistent VM and full operating-system control, but requires administration and incurs cost while running.<br>- **Google Kubernetes Engine:** Orchestrates long-running or distributed containers, but adds Kubernetes complexity.<br>- **Google Cloud Batch:** Runs finite VM-based batch workloads with more machine flexibility than Cloud Run, but requires more configuration. |

Artifact Registry, IAM service accounts, Secret Manager, Cloud Logging, and Cloud Monitoring are cross-cutting implementation services. They support the selected layers but are not treated as architecture layers themselves.

## Cost and Complexity Comparison

These ratings are directional and must be replaced with a budget estimate after the architecture and region are selected.

| Layer | Lowest complexity | Lowest idle cost | Strongest Project 1 continuity | Strongest open-stack portability |
| --- | --- | --- | --- | --- |
| Query Engine + Catalog | BigQuery-managed Iceberg | BigQuery-managed Iceberg | DuckDB + Polaris + Iceberg on Cloud Storage | Managed Spark + Polaris + Iceberg on Cloud Storage |
| Transformation | Dataform with BigQuery | PySpark serverless batches | dbt Core with a temporary managed Spark cluster | dbt Core with a temporary managed Spark cluster |
| Orchestration | Workflows + Cloud Scheduler | Workflows + Cloud Scheduler | Cloud Composer | Self-managed Airflow |
| Compute | Cloud Run Jobs | Cloud Run Jobs | Cloud Run Jobs using adapted Project 1 containers | Containerized workloads on Cloud Run Jobs |
| Machine Learning | Vertex AI custom training | Cloud Run Job | Cloud Run Job running the existing XGBoost workflow | Containerized XGBoost in a Cloud Run Job |
| Visualization | Looker Studio | Looker Studio | Self-managed Superset | Self-managed Superset |

Important cost observations:

- Cloud Storage includes limited Always Free usage in eligible US regions.
- Cloud Run is usage-based, has a monthly free tier, and Cloud Run Jobs exit when their work is complete.
- Workflows includes a monthly free allowance and charges by workflow steps rather than idle uptime.
- Cloud Composer charges an hourly environment fee in addition to compute, database, and storage costs.
- Managed Service for Apache Spark scales to zero, but distributed Spark may add cost and complexity without providing value at the planned dataset size.
- Managed Spark supports both serverless batches and managed clusters. The cluster model can provide the Spark connection required by `dbt-spark`.
- `dbt-spark` supports Thrift, HTTP, ODBC, and local session connection methods. A temporary cluster with a Spark Thrift endpoint is the most direct GCP mapping for the existing dbt project.
- Managed clusters support Iceberg components, initialization actions, scheduled deletion, and dependency configuration, but cost more while running than serverless batches.
- Looker Studio's self-service tier is available at no charge, although connected services such as BigQuery still incur their own usage charges.

## Official References

- [Apache Iceberg managed tables in BigQuery](https://cloud.google.com/bigquery/docs/iceberg-tables)
- [Cloud Storage pricing](https://cloud.google.com/storage/pricing)
- [Cloud Composer pricing](https://cloud.google.com/composer/pricing)
- [Cloud Run Jobs](https://cloud.google.com/run/docs/create-jobs)
- [Cloud Run pricing](https://cloud.google.com/run/pricing)
- [Workflows pricing](https://cloud.google.com/workflows/pricing)
- [Managed Service for Apache Spark pricing](https://cloud.google.com/dataproc-serverless/pricing)
- [Managed Spark custom containers](https://cloud.google.com/dataproc-serverless/docs/guides/custom-containers)
- [Managed Spark properties](https://cloud.google.com/dataproc-serverless/docs/concepts/properties)
- [Managed Spark cluster components](https://cloud.google.com/dataproc/docs/concepts/components/overview)
- [Managed Spark cluster scheduled deletion](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/scheduled-deletion)
- [dbt Spark setup](https://docs.getdbt.com/docs/core/connect-data-platform/spark-setup)
- [Vertex AI custom training](https://cloud.google.com/vertex-ai/docs/training/create-custom-job)
- [Dataform pricing](https://cloud.google.com/dataform/pricing)
- [Visualize BigQuery data with Looker Studio](https://cloud.google.com/bigquery/docs/visualize-looker-studio)
