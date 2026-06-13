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

| Milestone                               | Objective                                                                       | Acceptance Criteria                                                                 |
| --------------------------------------- | ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| **M1. Architecture & Repository Setup** | Define the cloud architecture, project scope, and repository structure.         | Foundational decisions are recorded as ADRs, and the architecture diagram and repository skeleton are complete. |
| **M2. Infrastructure Provisioning**     | Provision core cloud resources using Terraform.                                 | `terraform apply` completes successfully and required resources exist.              |
| **M3. Compute & Orchestration**         | Deploy Airflow (or equivalent orchestration environment).                       | Airflow services are reachable and healthy.                                         |
| **M4. Cloud Storage Layer**             | Configure cloud object storage for pipeline inputs and outputs.                 | Test files can be written to and read from cloud storage.                           |
| **M5. Pipeline Deployment**             | Adapt Project 1 ingestion and transformation pipeline to the cloud environment. | Running the DAG successfully produces the expected downstream artifacts.            |
| **M6. Analytics & Consumption**         | Expose pipeline outputs for downstream consumption.                             | Data products or dashboards can be queried successfully.                            |
| **M7. Automation & Validation**         | Add deployment scripts, validation checks, and basic CI where appropriate.      | Automated checks complete successfully and deployment process is documented.        |
| **M8. Documentation & Demo**            | Finalize documentation and create a portfolio-quality demonstration.            | README and demo materials allow a reviewer to understand and reproduce the project. |

### Milestone 1 Decisions

Milestone 1 must resolve and document the decisions that shape the implementation:

1. **Cloud provider**: Select AWS, Azure, or GCP.
2. **Service mapping**: Decide which cloud services replace or host the Project 1 components.
3. **Orchestration model**: Choose between managed Airflow, self-hosted Airflow, or an appropriate alternative.
4. **Table format and catalog**: Decide whether to retain Apache Iceberg and select its catalog.
5. **Machine learning scope**: Decide whether Project 2 includes the Project 1 training and evaluation workflow.
6. **Analytics and consumption**: Select how users will query data and inspect outputs.
7. **Cost and teardown policy**: Establish a working budget, resource limits, and cleanup expectations.
8. **Dataset scope**: Select the dataset size used for deployment and verification.

These are architecture decisions within Milestone 1 rather than separate implementation milestones. Decisions should be recorded under `docs/adr/` before dependent infrastructure is built.

#### Milestone 1 Acceptance Criteria

- `docs/architecture.md` describes the selected architecture and includes a current architecture diagram.
- The repository skeleton reflects the selected components and deployment workflow.
- Foundational ADRs have an `Accepted` status.
- Each decision above is covered by an ADR, either individually or as part of a closely related decision.
- Expected cloud costs and teardown behavior are documented.

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
