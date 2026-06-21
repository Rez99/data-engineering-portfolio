# Project 1: Local Lakehouse

An end-to-end data and machine learning platform that transforms 42 million e-commerce clickstream events into session-level features, purchase-conversion predictions, and interactive dashboards using open-source lakehouse technologies.

---

## Table of Contents

|Section|Contents|
|---|---|
| **[1. What This Project Does](#1-what-this-project-does)**         | 1.1 Problem Statement<br>1.2 Inputs and Outputs<br>1.3 End-to-End Pipeline                                           |
| **[2. Follow One Session](#2-follow-one-session)**                 | 2.1 Raw Clickstream Event<br>2.2 Curated Session-Level Feature Store<br>2.3 Model Prediction<br>2.4 Model Evaluation |
| **[3. Architecture](#3-architecture)**                             | 3.1 Detailed Pipeline Flow<br>3.2 Technology Stack                                                                   |
| **[4. Why These Choices](#4-why-these-choices)**                   | 4.1 Benchmarking the Alternatives<br>4.2 Resource-Aware Engineering                                                  |
| **[5. Deployment](#5-deployment)**                                 | 5.1 Clone<br>5.2 Repository Structure<br>5.3 Start<br>5.4 Services<br>5.5 Stop                                       |
| **[6. Results](#6-results)**                                       | 6.1 Pipeline Execution<br>6.2 Model Evaluation Dashboard                                                             |
| **[7. Reflections and Next Steps](#7-reflections-and-next-steps)** | 7.1 Key Lessons<br>7.2 Trade-offs and Limitations<br>7.3 Future Directions                                           |

# 1. What This Project Does
## 1.1 Problem Statement

This project demonstrates how raw e-commerce clickstream events can be transformed into machine-learning-ready features, purchase-conversion predictions, and interactive dashboards using a modern lakehouse architecture.

The project explores three questions:

1. How can large analytical datasets be processed efficiently on commodity hardware?
2. What advantages does a lakehouse provide over traditional databases and file-based analytics?
3. How can data engineering, analytics engineering, and machine learning be combined into a reproducible end-to-end workflow?

## 1.2 Inputs and Outputs

### Input

The pipeline uses the October 2019 file from [**Kaggle's E-Commerce Behavior Data from Multi-Category Store**](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset. Each row represents a user-product interaction from a large multi-category online store.

| Attribute   | Value                                                      |
| ----------- | ---------------------------------------------------------- |
| Source      | Kaggle: E-Commerce Behavior Data from Multi-Category Store |
| File        | `2019-Oct.csv.gz`                                          |
| Format      | Compressed CSV                                             |
| Size        | 42.4 million events                                        |
| Period      | October 2019                                               |
| Granularity | One row per user event                                     |
| Event Types | `view`, `cart`, `remove_from_cart`, `purchase`             |

### Outputs

The platform produces the following artifacts:

| Output | Purpose |
| --- | --- |
| Iceberg Lakehouse Tables | Store raw events, transformed datasets, and session-level features in an open table format |
| XGBoost Model | Predict whether a user session will result in a purchase |
| Evaluation Metrics | Quantify model performance using ROC AUC, confusion matrix, and related measures |
| Superset Dashboard | Visualize model performance and explain model behavior through interactive dashboards |

## 1.3 End-to-End Pipeline

The workflow below summarizes the movement of data through the platform.

```mermaid
flowchart LR
    A[Raw Clickstream Events]
    --> B[Lakehouse Tables]
    --> C[Session-Level Features]
    --> D[Purchase Conversion Model]
    --> E[Metrics & Dashboard]
```

# 2. Follow One Session

The machine learning workflow operates at the session level rather than the event level. Individual user interactions are first aggregated into session-level features, which are then used to train and evaluate a purchase-conversion model.

## 2.1 Raw Clickstream Event

The source dataset contains one row per user interaction.

```json
{
  "event_time": "2019-10-01 00:00:00 UTC",
  "event_type": "view",
  "product_id": 44600062,
  "category_code": "electronics.smartphone",
  "brand": "shiaomi",
  "price": 489.07,
  "user_id": 541312140,
  "user_session": "72d76fde-8bb3-4e00-8c23-a032dfed738c"
}
```

## 2.2 Curated Session-Level Feature Store

Events belonging to the same `user_session` are aggregated into a curated session-level feature store. This feature store serves as the training dataset for the purchase-conversion model.

```json
{
  "user_session": "72d76fde-8bb3-4e00-8c23-a032dfed738c",
  "user_id": 541312140,
  "brand": "shiaomi",
  "category_code": "electronics.smartphone",
  "view_count": 4,
  "cart_add_count": 1,
  "purchase_count": 0,
  "converted": false,
  "session_duration_seconds": 423,
  "day_of_week": 2,
  "hour_of_day": 14,
  "seconds_to_first_cart": 127
}
```

## 2.3 Model Prediction

The session-level features are used to train an XGBoost classifier that estimates the probability that a session will result in a purchase.

Example prediction:

```json
{
  "user_session": "72d76fde-8bb3-4e00-8c23-a032dfed738c",
  "purchase_probability": 0.18,
  "predicted_conversion": false
}
```

## 2.4 Model Evaluation

Predictions from the test dataset are aggregated into evaluation artifacts that quantify model performance.

Example metrics:

```json
{
  "roc_auc": 0.99,
  "accuracy": 0.94,
  "balanced_accuracy": 0.75,
  "f1_true": 0.41
}
```

These evaluation artifacts are published to the lakehouse and visualized through an interactive dashboard.

# 3. Architecture

## 3.1 Detailed Pipeline Flow

The pipeline runs as four sequential Airflow DAGs. The first downloads the raw 
clickstream data into object storage. The second loads it into cataloged Iceberg 
tables via an intermediate Parquet conversion. The third runs dbt transformation 
models that aggregate raw events into a session-level feature store. The fourth 
trains and evaluates the XGBoost classifier and publishes the model artifacts. 
The diagram below traces these stages and the interactions between components.

```mermaid
sequenceDiagram
    autonumber

    participant Orchestration
    participant ObjectStorage as Object Storage
    participant QueryEngine as Query Engine<br/>+ Catalog
    participant Transformation
    participant ML as Machine Learning
    participant Visualization

    rect rgb(255, 230, 230)
        Orchestration->>ObjectStorage: DAG 1️⃣ > Extract
    end

    rect rgb(242, 255, 230)
        Orchestration->>QueryEngine: DAG 2️⃣ > Load
        QueryEngine->>ObjectStorage: Write Parquet
        QueryEngine->>ObjectStorage: Write Iceberg
    end

    rect rgb(230, 255, 255)
        Orchestration->>Transformation: DAG 3️⃣ > Transform
        Transformation->>QueryEngine: Run staging model
        QueryEngine->>ObjectStorage: Write staging model

        Transformation->>QueryEngine: Run intermediate model
        QueryEngine->>ObjectStorage: Write intermediate model

        Transformation->>QueryEngine: Run mart model
        QueryEngine->>ObjectStorage: Write mart model<br/>(feature store)
    end

    rect rgb(242, 230, 255)
        Orchestration->>ML: DAG 4️⃣ > Train + evaluate model
        Note over ObjectStorage,ML: Read session features
        ML->>ML: Predict purchase conversion<br/>and calculate metrics
        ML->>ObjectStorage: Publish model and metrics
    end

    Note over ObjectStorage,Visualization: Read model metrics
```

## 3.2 Technology Stack

| Layer | Technology | Purpose |
| --- | --- | --- |
| Infrastructure | <img src="assets/logos/docker.svg" alt="Docker" height="30"> | Reproducible local deployment |
| Orchestration | <img src="assets/logos/airflow.png" alt="Airflow" height="30"> | Pipeline scheduling and workflow management |
| Object Storage | <img src="assets/logos/rustfs.svg" alt="RustFS" height="30"> | S3-compatible object storage |
| Query Engine | <img src="assets/logos/duckdb.svg" alt="DuckDB" height="30"> | High-performance analytical processing |
| Catalog | <img src="assets/logos/polaris.png" alt="Polaris" height="30"> | Iceberg REST catalog |
| Table Format | <img src="assets/logos/iceberg.png" alt="Iceberg" height="30"> | Open analytical table format |
| Transformation | <img src="assets/logos/dbt.png" alt="dbt" height="30"> | Declarative data modeling |
| Machine Learning | <img src="assets/logos/xgboost.png" alt="XGBoost" height="30"> | Purchase-conversion prediction |
| Visualization | <img src="assets/logos/superset.png" alt="Superset" height="30"> | Dashboarding and data exploration |

# 4. Why These Choices

## 4.1 Benchmarking the Alternatives

Before building the lakehouse, I wanted to answer two questions:

1. How much faster are modern OLAP databases than traditional OLTP databases for analytical workloads?
2. If OLAP databases already provide excellent performance, what additional problem does a lakehouse solve?

To answer these questions, I benchmarked the same 42-million-row e-commerce clickstream dataset across four architectures: file analytics with Pandas, PostgreSQL, DuckDB, and a local lakehouse built with DuckDB, Iceberg, and Polaris.

```mermaid
xychart-beta
    title "Query Runtime (seconds)"
    x-axis ["File Analytics", "OLTP Database", "OLAP Database", "Lakehouse Architecture"]
    y-axis "Seconds" 0 --> 75
    bar [69.13, 59.32, 0.13, 0.13]
```

| Stage | Architecture           | Stack                      | Storage Cost | Memory Cost | Compute Cost | Notes                                                                                      |
| ----- | ---------------------- | -------------------------- | ------------ | ----------- | ------------ | ------------------------------------------------------------------------------------------ |
| 1     | File Analytics         | Pandas + CSV.GZ            | 🟠 Medium    | 🔴 High     | 🔴 High      | Simple and flexible for exploratory analysis, but limited by available memory.             |
| 2     | OLTP Database          | PostgreSQL                 | 🔴 High      | 🟢 Low      | 🔴 High      | Optimized for transactions and updates, not large analytical scans.                        |
| 3     | OLAP Database          | DuckDB                     | 🟢 Low       | 🟢 Low      | 🟢 Low       | Columnar OLAP systems dramatically reduce storage and query costs for analytics.           |
| 4     | Lakehouse Architecture | DuckDB + Iceberg + Polaris | 🟢 Low       | 🟢 Low      | 🟢 Low       | Lakehouses decouple storage, metadata, and compute while retaining warehouse capabilities. |

The benchmarks validated two important ideas. First, columnar OLAP systems dramatically reduce both storage and compute costs compared with traditional row-oriented databases. Second, lakehouses solve a different problem: they decouple storage, metadata, and compute while introducing capabilities such as schema evolution, governance, and ACID transactions.

## 4.2 Resource-Aware Engineering

Four implementation decisions keep peak memory use manageable on commodity hardware:

* **Columnar, out-of-core processing** using DuckDB and Parquet rather than in-memory Pandas DataFrames.
* **Staged ingestion** using a `CSV → Parquet → Iceberg` workflow to reduce peak memory pressure during ingestion.
* **Independent dbt model execution** to reclaim memory between transformation stages and improve observability.
* **External-memory machine learning** using streamed Parquet batches and XGBoost external-memory training.

Together, these choices trade some intermediate storage and orchestration complexity for lower peak memory consumption.

# 5. Deployment

## 5.1 Clone

### Prerequisites

The platform requires:

* Docker Desktop
* `curl`
* `jq`
* `openssl`

### Clone the Repository

```bash
git clone https://github.com/Rez99/data-engineering-portfolio.git
```

## 5.2 Repository Structure

```text
pipeline/
├── dags/                   # Airflow DAG definitions
├── docker/
│   ├── airflow/            # Airflow, PostgreSQL, and Redis
│   ├── dbt/                # dbt project and DuckDB profile
│   ├── polaris/            # RustFS and Polaris
│   └── superset/           # Superset and its metadata database
├── logs/
│   └── setup.log           # Detailed installer output
└── scripts/
    ├── setup.sh            # Start infrastructure and run the pipeline
    ├── reset.sh            # Delete containers, volumes, and generated artifacts
    ├── superset_config.py
    ├── superset_init_duckdb.py
    └── superset_assets/    # Version-controlled dashboard definitions
```

## 5.3 Start

### Change to the Project Directory

```bash
cd data-engineering-portfolio/project-1-lakehouse/pipeline/scripts
```

### Start the Platform

```bash
bash setup.sh
```

The setup script performs the complete deployment workflow:

1. Starts the required infrastructure services.
2. Configures object storage and catalog components.
3. Executes the end-to-end data pipeline.
4. Trains and evaluates the machine learning model.
5. Imports the Superset dashboard and supporting assets.

For a practical local demonstration, the default pipeline processes a one-million-row sample. The benchmark results presented in this project were produced separately using the full 42-million-row dataset.

<img src="assets/logos/setup.png" alt="Automated deployment in progress" width="700">

## 5.4 Services

After a successful deployment, the following services are available locally:

| Service         | Purpose                      | URL                   | Credentials                     |
| --------------- | ---------------------------- | --------------------- | ------------------------------- |
| Apache Airflow  | Pipeline orchestration       | http://localhost:8080 | `airflow` / `airflow`           |
| RustFS          | S3-compatible object storage | http://localhost:9001 | `polaris_root` / `polaris_pass` |
| Apache Polaris  | Iceberg catalog              | http://localhost:8181 | API only                        |
| Apache Superset | Model evaluation dashboard   | http://localhost:8088 | `admin` / `admin`               |

## 5.5 Stop

### Change to the Project Directory

```bash
cd data-engineering-portfolio/project-1-lakehouse/pipeline/scripts
```

### Stop the Platform

```bash
bash reset.sh
```

The reset script removes:

* Docker containers
* Docker volumes
* Generated credentials
* Airflow runtime files
* Local pipeline artifacts

The next start operation recreates the environment from the version-controlled configuration.

# 6. Results

## 6.1 Pipeline Execution

<img src="assets/logos/airflow_ui.png" alt="Apache Airflow DAG execution" width="1000">

Apache Airflow orchestrates the workflow from data ingestion through model evaluation. The DAG graph above shows the successful execution of the complete pipeline.

## 6.2 Model Evaluation Dashboard

<img src="assets/logos/superset_ui.png" alt="Apache Superset dashboard" width="1000">

The dashboard visualizes the evaluation artifacts produced by the machine learning workflow, including:

- ROC AUC
- Accuracy
- Confusion Matrix
- Feature Importance
- Baseline Model Comparison

These artifacts provide both quantitative measures of model performance and qualitative insight into the factors that influence purchase conversion.

# 7. Reflections and Next Steps

## 7.1 Key Lessons

This project reinforced that modern data engineering is less about individual tools and more about how independent components work together.

One unexpected lesson was the importance of diagrams. As the architecture grew, diagrams became essential for understanding the system and communicating design decisions. Often, the act of drawing a workflow or architecture diagram exposed gaps in my own understanding.

The project also highlighted the value of benchmarking. Measuring performance and resource usage directly led to better architectural decisions than relying on assumptions.

## 7.2 Trade-offs and Limitations

The architecture prioritizes openness, reproducibility, and efficient local execution over operational simplicity.

Benefits include:

* Open table formats and storage interfaces
* Decoupled storage, metadata, and compute
* Reproducible containerized deployment
* Efficient analytical processing on commodity hardware

Trade-offs include:

* Higher architectural complexity than a single database solution
* Additional orchestration overhead
* Longer setup times
* More moving parts to operate and troubleshoot

## 7.3 Future Directions

The next step is to deploy the same architecture in the cloud using Infrastructure as Code and managed services.

Future projects will build on this foundation by exploring distributed processing with Spark, streaming ingestion, automated model retraining, and real-time data products.
