# Data Engineering Portfolio

This portfolio documents my transition from Data Science to Data Engineering through a series of projects that build on one another.

Rather than treating data engineering as a collection of disconnected tools, the projects follow the lifecycle of a modern data platform:

1. **Build a Lakehouse** — transform operational data into an analytical platform.
2. **Productionize the Lakehouse** — deploy, orchestrate, monitor, and operate the platform in the cloud.
3. **AI Data Infrastructure** — extend the platform to support real-time and AI-powered applications.

The focus throughout is not on using as many technologies as possible. The focus is on understanding the architectural decisions, tradeoffs, and patterns that underpin modern data systems.

---

## Project 1: Build a Lakehouse

Starting from a normalized transactional database, build a modern analytical platform using open table formats and analytics engineering principles.

```text
Postgres (3NF)
        ↓
Parquet
        ↓
Iceberg
        ↓
dbt
        ↓
Star Schema
        ↓
Analytics
```

Topics covered:

* OLTP vs OLAP
* ELT
* Parquet
* Apache Iceberg
* Data modeling
* Star schemas
* dbt
* Data quality testing

---

## Project 2: Productionize the Lakehouse

Take the local lakehouse architecture and operate it as a production system.

```text
Sources
    ↓
Cloud Storage
    ↓
Orchestration
    ↓
Transformations
    ↓
Monitoring & Alerting
```

Topics covered:

* Cloud infrastructure
* Object storage
* IAM
* Workflow orchestration
* Scheduling and retries
* Monitoring and observability
* CI/CD
* Infrastructure as Code

---

## Project 3: AI Data Infrastructure

Build a platform that continuously transforms unstructured data into knowledge that can be consumed by AI systems.

```text
Documents & Events
         ↓
Streaming Ingestion
         ↓
Chunking
         ↓
Embeddings
         ↓
Vector Store
         ↓
Retrieval
         ↓
LLM Applications
```

Topics covered:

* Streaming data pipelines
* Event-driven architectures
* Embedding generation
* Vector databases
* Semantic search
* Retrieval-Augmented Generation (RAG)
* AI data platforms
* Real-time knowledge systems

---

## Themes

Across all projects, I focus on a small set of recurring questions:

* Why choose one architecture over another?
* When should data be normalized or denormalized?
* When is batch processing sufficient?
* When does streaming provide value?
* How do modern lakehouse architectures differ from traditional warehouses?
* How do AI applications change the way data platforms are designed?

The objective is to develop practical experience building systems while understanding the tradeoffs that drive architectural decisions.
