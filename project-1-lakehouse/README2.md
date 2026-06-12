# Project 1: Local Lakehouse

## End-to-End Data Engineering on Commodity Hardware

*A fully containerized local lakehouse platform built with open-source technologies, demonstrating resource-aware architecture, modern data engineering workflows, and scalable analytical processing on commodity hardware.*

---

## Table of Contents

1. Executive Summary
2. Architecture
3. Design Decisions
4. Pipeline Walkthrough
5. Demonstration
6. Running the Project
7. Reflections and Next Steps

---

### 1. Executive Summary

#### What was built?

This project builds a fully containerized local lakehouse platform using modern open-source data engineering tools. It ingests a large public clickstream dataset, processes it through an end-to-end analytical pipeline, and delivers curated analytics and model metadata through Apache Superset.

#### What does this project demonstrate?

The platform brings together the core components of a modern analytical data stack into a reproducible local environment. It demonstrates practical experience with workflow orchestration, object storage, open table formats, data transformation, and business intelligence tooling.

#### Central Design Philosophy

The pipeline was deliberately designed to make large-scale analytical processing practical on commodity hardware. By favoring columnar storage, staged processing, and out-of-core execution over large in-memory workflows, the same architecture that enables local development also reflects scalable and cost-conscious production engineering.


### 2. Architecture

#### How the Components Fit Together

The platform follows a layered lakehouse architecture, separating orchestration, storage, transformation, and visualization into independent services. Apache Airflow orchestrates the ingestion pipeline, loading raw data into Apache Iceberg tables backed by S3-compatible object storage. DuckDB and dbt perform analytical transformations directly against the lakehouse, while Apache Superset exposes curated analytics and model metadata for exploration and visualization.

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
        Transformation->>QueryEngine: Run stg model
        QueryEngine->>ObjectStorage: Write stg model

        Transformation->>QueryEngine: Run int model
        QueryEngine->>ObjectStorage: Write int model

        Transformation->>QueryEngine: Run mart model
        QueryEngine->>ObjectStorage: Write mart model<br/>(feature store)
    end

    rect rgb(242, 230, 255)
        Orchestration->>ML: DAG 4️⃣ > Build ML model
        Note over ObjectStorage,ML: Read training data
        ML->>ML: Build model
        ML->>ObjectStorage: Publish model and metrics
    end

    Note over ObjectStorage,Visualization: Read model metrics
```

#### Technology Stack

| Layer          | Technology      | Purpose                                     |
| -------------- | --------------- | ------------------------------------------- |
| Infrastructure | <img src="assets/logos/docker.svg" alt="Docker" height="30">   | Reproducible local deployment               |
| Orchestration  | <img src="assets/logos/airflow.png" alt="Airflow" height="30">  | Pipeline scheduling and workflow management |
| Object Storage | <img src="assets/logos/rustfs.svg" alt="RustFS" height="30">          | S3-compatible object storage                |
| Query Engine   | <img src="assets/logos/duckdb.svg" alt="DuckDB" height="30">          | High-performance analytical processing      |
| Catalog        | <img src="assets/logos/polaris.png" alt="Polaris" height="30">  | Iceberg REST catalog                        |
| Table Format   | <img src="assets/logos/iceberg.png" alt="Iceberg" height="30">  | Open analytical table format                |
| Transformation | <img src="assets/logos/dbt.png" alt="dbt" height="30">             | Declarative data modeling                   |
| Machine Learning  | <img src="assets/logos/xgboost.png" alt="XGBoost" height="30"> | Memory-efficient model training for large datasets          |
| Visualization  | <img src="assets/logos/superset.png" alt="Superset" height="30"> | Dashboarding and data exploration           |



### 3. Design Decisions

* Benchmarking the alternatives.
* Why DuckDB?
* Why CSV → Parquet → Iceberg?
* Why Airflow, dbt, Polaris, RustFS, and Superset?
* Designing for low memory usage and cost-efficient scaling.

### 4. Pipeline Walkthrough

* End-to-end data flow.
* Pipeline sequence diagram.
* How data moves from raw ingestion to dashboard.

### 5. Demonstration

* Airflow orchestration.
* Iceberg tables and transformed datasets.
* Model metadata and analytical outputs.
* Superset dashboard.

### 6. Running the Project

* Repository structure.
* One-command setup.
* Accessing Airflow and Superset.

### 7. Reflections and Next Steps

* Key lessons learned.
* Trade-offs and limitations.
* Future evolution toward cloud deployment and streaming architectures.
