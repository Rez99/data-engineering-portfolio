# data-engineering-portfolio

This portfolio documents my transition from 12 years in Data Science to Data Engineering.

Rather than focusing on individual tools, the projects are organized around the evolution of modern data platforms:

1. **Lakehouse Fundamentals** — building an analytical platform from an operational database.
2. **Cloud & Orchestration** — productionizing pipelines in the cloud with scheduling, monitoring, and automation.
3. **AI Data Infrastructure** — building retrieval systems that transform unstructured data into assets consumable by LLMs.

The goal is not to demonstrate familiarity with a collection of technologies. The goal is to demonstrate the ability to reason about data systems, understand architectural tradeoffs, and choose appropriate tools for a given problem.

---

## Project 1 — Lakehouse Fundamentals

Build a modern analytical platform starting from a normalized transactional database.

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

Key concepts:

* OLTP vs OLAP
* ELT pipelines
* Columnar storage
* Parquet
* Apache Iceberg
* Data modeling
* Star schemas
* dbt transformations
* Data quality testing

---

## Project 2 — Cloud & Orchestration

Take the local lakehouse architecture and operate it in a production-style environment.

```text
Sources
    ↓
Cloud Storage
    ↓
Orchestration
    ↓
Transformations
    ↓
Monitoring & Alerts
```

Key concepts:

* Cloud infrastructure
* Object storage
* IAM and permissions
* Workflow orchestration
* Scheduling and retries
* Observability
* Infrastructure as Code
* CI/CD

---

## Project 3 — AI Data Infrastructure

Build a retrieval platform that continuously transforms unstructured information into knowledge that can be consumed by AI applications.

```text
Documents
     ↓
Ingestion
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

Key concepts:

* Unstructured data pipelines
* Embedding generation
* Vector databases
* Semantic search
* Retrieval-Augmented Generation (RAG)
* Event-driven architectures
* Streaming ingestion
* AI infrastructure

---

## Architectural Themes

Across all projects, I focus on the following questions:

* Why choose one architecture over another?
* When should data be modeled, transformed, or denormalized?
* When is batch processing sufficient?
* When does streaming become necessary?
* What are the tradeoffs between cost, complexity, performance, and maintainability?
* How do modern lakehouse and AI systems differ from traditional warehouse architectures?

The emphasis throughout is on understanding systems and tradeoffs, not simply learning tools.
