# Project 1: Local Lakehouse

*A fully containerized local lakehouse platform built with open-source technologies, modern data engineering workflows, and efficient analytical processing on commodity hardware.*

---

## Table of Contents

1. Project Overview
2. Architecture
3. Design Decisions
4. Deployment
5. Reflections and Next Steps

---

## 1. Project Overview

This project builds a fully containerized local lakehouse platform using modern open-source data engineering tools. A single setup script provisions and configures the services. Apache Airflow then orchestrates the data and machine learning pipeline:

1. Downloads a publicly available e-commerce clickstream dataset in CSV format.
2. Converts the CSV data to Parquet and loads it into cataloged Apache Iceberg tables.
3. Transforms the event data into a session-level feature store for machine learning.
4. Trains an XGBoost classifier to predict whether a user session contains a purchase.
5. Publishes the model outputs for Apache Superset to read and visualize as performance metrics and feature importance.

```mermaid
flowchart LR
    A[Download Clickstream Data]
    --> B[Load Data into the Lakehouse]
    --> C[Build Session Feature Store]
    --> D[Train XGBoost Classifier]
    --> E[Visualize Model Metrics]
```

## 2. Architecture

#### How the Components Fit Together

The platform follows a layered lakehouse architecture that separates orchestration, storage, transformation, machine learning, and visualization. Apache Airflow orchestrates the workflow, RustFS stores the data, and Polaris catalogs the Iceberg tables. DuckDB and dbt produce session-level features, XGBoost predicts purchase conversion, and Apache Superset presents the evaluation results.

#### High-Level Architecture

*The diagram below illustrates the flow of data through the platform and the interaction between the major components.*

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

#### Technology Stack

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

## 3. Design Decisions

#### Benchmarking the Alternatives

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
| Stage | Architecture | Stack | Storage Cost | Memory Cost | Compute Cost | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | File Analytics | Pandas + CSV.GZ | 🟠 Medium | 🔴 High | 🔴 High | Simple and flexible for exploratory analysis, but limited by available memory. |
| 2 | OLTP Database | PostgreSQL | 🔴 High | 🟢 Low | 🔴 High | Optimized for transactions and updates, not large analytical scans. |
| 3 | OLAP Database | DuckDB | 🟢 Low | 🟢 Low | 🟢 Low | Columnar OLAP systems dramatically reduce storage and query costs for analytics. |
| 4 | Lakehouse Architecture | DuckDB + Iceberg + Polaris | 🟢 Low | 🟢 Low | 🟢 Low | Lakehouses decouple storage, metadata, and compute while retaining warehouse capabilities. |

*Representative benchmark query over the full 42-million-row clickstream dataset.*

The benchmarks validated two important ideas. First, columnar OLAP systems dramatically reduce both storage and compute costs compared with traditional row-oriented databases. Second, lakehouses solve a different problem: they decouple storage, metadata, and compute while introducing capabilities such as schema evolution, governance, and ACID transactions. These observations directly informed the architecture of the final platform.

#### Resource-Aware Engineering

Four implementation decisions keep peak memory use manageable on commodity hardware:

- **Columnar, out-of-core processing**. The core pipeline uses DuckDB and Parquet rather than materializing the full dataset as an in-memory Pandas DataFrame.
- **Staged ingestion**. The ingestion pipeline was redesigned from `CSV → Iceberg` to `CSV → Parquet → Iceberg`. By materializing an intermediate Parquet artifact, expensive CSV parsing is separated from Iceberg table creation, reducing peak memory utilization and improving scalability.
- **Independent dbt model execution**. Large dbt models are orchestrated as separate Airflow tasks, allowing memory to be reclaimed between stages while improving observability and retry granularity.
- **External-memory machine learning**. The original Iceberg → Pandas → XGBoost workflow was replaced with a disk-backed pipeline using Parquet and XGBoost's external-memory mode, reducing peak memory usage without requiring the full training dataset in RAM.

Together, these choices trade some intermediate storage and orchestration complexity for lower peak memory consumption.

## 4. Deployment

#### Setup

Prerequisites: Docker Desktop, `curl`, `jq`, and `openssl`.

Clone the repository and run the setup script:

```bash
git clone https://github.com/Rez99/data-engineering-portfolio.git
cd data-engineering-portfolio/project-1-lakehouse/pipeline/scripts/
bash setup.sh
```

The setup script provisions the lakehouse infrastructure, configures the required services, runs the end-to-end pipeline, and imports the Superset dashboard through a single command.

For a practical local demonstration, the default pipeline processes a
one-million-row sample. The benchmark results above were produced separately
using the full 42-million-row dataset.

<img src="assets/logos/setup.png" alt="Automated deployment in progress" width="700">

#### Resetting the Environment

To stop the platform and return to a clean local environment, run:

```bash
cd data-engineering-portfolio/project-1-lakehouse/pipeline/scripts/
bash reset.sh
```

The reset script removes the local containers, volumes, generated credentials,
and Airflow runtime files. The next setup run rebuilds the environment from the
version-controlled configuration.

#### Platform Services
After a successful deployment, the following services are available locally:
| Service | Purpose | URL | Credentials |
| --- | --- | --- | --- |
| Apache Airflow | Pipeline orchestration | http://localhost:8080 | `airflow` / `airflow` |
| RustFS | S3-compatible object storage | http://localhost:9001 | `polaris_root` / `polaris_pass` |
| Apache Polaris | Iceberg catalog | http://localhost:8181 | API only |
| Apache Superset | Model evaluation dashboard | http://localhost:8088 | `admin` / `admin` |

#### Repository Structure

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

#### Platform in Action

The figures below show the platform after a successful pipeline execution.

**Apache Airflow**

<img src="assets/logos/airflow_ui.png" alt="Apache Airflow UI" width="1000">

**Apache Superset**

<img src="assets/logos/superset_ui.png" alt="Apache Superset dashboard" width="1000">

## 5. Reflections and Next Steps

#### Key Lessons

Building this project reinforced that modern data engineering is less about individual tools and more about how independent components work together. Benchmarking the alternatives and measuring resource usage directly led to better architectural decisions than relying on assumptions.

#### Trade-offs and Limitations

The platform demonstrates a local batch workflow rather than a production deployment. It does not yet address distributed execution, continuous ingestion, automated model retraining, or cloud security and governance.

#### Future Directions

The next natural step is to deploy the same architecture in a cloud environment using managed services and Infrastructure as Code. From there, the platform could evolve toward streaming ingestion and real-time analytics.
