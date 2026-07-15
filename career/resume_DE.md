# Rezwan Hoppe-Islam

📧 rezwan.islam99@gmail.com  
📍 Brookfield, WI  
GitHub: https://github.com/Rez99/data-engineering-portfolio  
LinkedIn: https://www.linkedin.com/in/rezwan-islam/

---

# Data Engineer

Data engineer and former senior data scientist with 15+ years of experience building analytics platforms, experimentation systems, and machine learning products across e-commerce, healthcare, and technology. Recently completed a production-style data engineering portfolio spanning local lakehouse architecture, cloud infrastructure, and real-time streaming systems. Brings senior analytics judgment, platform thinking, and hands-on engineering ability to build scalable, reliable, and observable data systems.

---

# Technical Skills

## Languages

- Python
- SQL
- R

## Data Platforms

- Apache Kafka / Redpanda
- Apache Flink
- DuckDB
- Apache Spark
- Apache Iceberg
- Apache Parquet
- PostgreSQL
- Airflow

## Cloud & Infrastructure

- Google Cloud Platform
- Terraform
- Docker
- Cloud Workflows
- Cloud Run
- Cloud SQL
- Dataproc

## Data Modeling

- Batch ETL
- Streaming ETL
- dbt
- Event-time processing
- Stateful stream processing
- Data validation
- Data quality
- Schema evolution

## Observability

- Grafana
- Metrics
- Performance profiling
- Memory profiling
- Backpressure analysis

---

# Data Engineering Portfolio

## Streaming Bot Detection Platform

Designed and implemented a real-time clickstream processing platform using Kafka, Flink, PostgreSQL, and Grafana.

Highlights

- Built an event-driven streaming architecture processing millions of clickstream events.
- Implemented event-time processing, watermarks, and keyed Flink state for live session scoring.
- Designed real-time bot scoring from streaming session statistics and persisted operational scores to PostgreSQL.
- Added replay tooling, schema validation, dead-letter queues, checkpointing, and Grafana observability.
- Diagnosed memory pressure through profiling and reduced TaskManager memory footprint by roughly 50%.
- Investigated Flink execution plans, checkpoint state, JVM memory, and Docker architecture to improve stability.

**Technologies**

Kafka, Redpanda, Flink, Java, PostgreSQL, Grafana, DuckDB, Parquet, Docker

---

## Cloud Lakehouse

Designed and deployed a cloud-native lakehouse on Google Cloud using infrastructure-as-code.

Highlights

- Provisioned GCP infrastructure with Terraform, including Cloud Storage, Cloud SQL, Cloud Run, IAM, Secret Manager, Workflows, and Dataproc.
- Built Spark and dbt pipelines that load clickstream data into Iceberg tables, create session-level features, train an XGBoost model, and publish evaluation metrics.
- Deployed Polaris and Superset on Cloud Run with Cloud SQL metadata stores.
- Orchestrated the pipeline with Cloud Workflows and ephemeral Dataproc Spark clusters.
- Implemented reproducible setup, execution, dashboard refresh, and teardown workflows.

**Technologies**

GCP, Terraform, Spark, dbt, Iceberg, Polaris, Cloud Workflows, Cloud Run, Cloud SQL, Dataproc, Superset

---

## Local Lakehouse

Built an end-to-end local lakehouse that transforms 42 million clickstream events into analytical tables, machine-learning features, model predictions, and BI dashboards.

Highlights

- Built Airflow-orchestrated ingestion, transformation, training, and evaluation pipelines.
- Loaded raw clickstream data through staged CSV-to-Parquet-to-Iceberg ingestion.
- Modeled session-level feature stores with dbt and queried analytical datasets with DuckDB.
- Benchmarked Pandas, PostgreSQL, DuckDB, and lakehouse architectures to evaluate storage, memory, and compute tradeoffs.
- Built an external-memory XGBoost purchase prediction model and published model metrics to Superset.

**Technologies**

DuckDB, dbt, Airflow, Iceberg, Polaris, RustFS, Superset, Python, Parquet, XGBoost

---

# Professional Experience

## Senior Data Scientist — Hinge Health

**2024–2025**

- Led product analytics across Exercise Platform, Exercise Experience, and Computer Vision teams, partnering with 20+ engineers, designers, and PMs.
- Built modular experimentation infrastructure that cut analysis turnaround from hours to minutes and shaped team workflows for StatSig onboarding.
- Developed dbt-powered analytical models to monitor personalization engagement and surface high-friction product moments.
- Designed KPI frameworks and product health dashboards, including a Computer Vision dashboard that clarified adoption bottlenecks and influenced roadmap focus.
- Partnered with engineering teams on scalable analytics platforms, experimentation design, and impact measurement.

---

## Senior Data Scientist — Instacart

**2020–2023**

- Led experimentation and analytics across Storefront, Cart, Checkout, and Post-Checkout.
- Delivered insights and A/B test strategy contributing to over $200M in incremental GMV.
- Built large-scale experimentation tooling adopted organization-wide and recognized by leadership as a highly impactful data science initiative.
- Reduced experimentation pipeline runtime by 90%.
- Reduced infrastructure costs by approximately $1M annually.

---

## Consultant — Knowledge Leaps

**2019**

- Built marketing analytics and forecasting models for grocery retail.

---

## Data Scientist — Chegg

**2017–2018**

- Led customer lifetime value and marketing analytics initiatives.

---

## Product Analyst — eBay / PayPal

**2011–2017**

- Worked across Checkout, Marketplace, Gift Cards, StubHub, and Growth Experimentation.
- Selected for eBay Leadership Development Program.

---

## Analyst — Vodafone

**2007–2011**

---

# Education

## Ph.D. — Biochemical Engineering

University College London

---

## B.Eng. — Biochemical Engineering

University College London
