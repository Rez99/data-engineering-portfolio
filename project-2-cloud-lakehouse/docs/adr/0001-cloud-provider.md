# ADR-0001: Select Google Cloud as the Cloud Provider

## Status

Accepted

## Context

Project 2 migrates the local lakehouse from Project 1 to a cloud environment. The selected provider must support infrastructure as code, object storage, managed or containerized compute, orchestration, analytics, and secure identity management.

AWS, Microsoft Azure, and Google Cloud can all support the project. The primary goal, however, is to demonstrate cloud data engineering rather than multi-cloud portability. The project should therefore use one provider and avoid complexity that does not add portfolio value.

An existing Google Cloud account lowers the initial setup burden and allows implementation to focus on architecture, Terraform, deployment, and operations.

## Evaluation Criteria

The providers were evaluated against the criteria most relevant to this portfolio project:

| Criterion | Google Cloud | Amazon Web Services | Microsoft Azure |
| --- | --- | --- | --- |
| Portfolio relevance | Strong data engineering ecosystem and recognizable managed services | Broadest general cloud adoption and strong employer recognition | Strong enterprise adoption, especially in Microsoft environments |
| Managed data services | Cloud Storage, BigQuery, Dataproc, Dataflow, Composer, and Vertex AI | S3, Athena, EMR, Glue, MWAA, and SageMaker | ADLS, Synapse, Data Factory, Databricks, and Azure ML |
| Terraform support | Mature official provider with broad resource coverage | Mature official provider with broad resource coverage | Mature official provider with broad resource coverage |
| Cost and free-tier options | Useful free or low-cost options for storage, querying, and serverless services; continuously running managed services require care | Useful free-tier options, but several managed data services can create persistent costs | Useful free-service allowances, but managed analytics and orchestration can create persistent costs |
| Mapping from Project 1 | Clear mappings from RustFS to Cloud Storage and from local analytics to managed GCP services | Clear mappings from RustFS to S3 and from local analytics to AWS data services | Clear mappings from RustFS to ADLS and from local analytics to Azure data services |
| Existing access and experience | An account already exists, reducing setup friction; prior hands-on experience is not assumed | A new account and initial configuration may be required | A new account and initial configuration may be required |

All three providers satisfy the technical requirements. GCP provides sufficient portfolio relevance, managed services, and Terraform support while presenting the lowest known setup barrier for this project. Exact service costs will be evaluated separately when the service architecture is selected.

## Decision

Project 2 will use Google Cloud Platform (GCP).

Terraform will provision GCP resources wherever practical. The selection of individual services, region, orchestration model, Iceberg catalog, analytics layer, and machine learning runtime will be documented in subsequent ADRs.

Before infrastructure is provisioned, the following account-level prerequisites must be confirmed:

- Billing is enabled for the target GCP project.
- The user can create or configure projects, service accounts, and IAM roles.
- A deployment region has been selected.
- A working budget and billing alerts have been established.

## Alternatives Considered

### Amazon Web Services

AWS has broad industry adoption and a mature set of managed data services. It was not selected because there is no identified project requirement that outweighs the additional account setup and learning overhead.

### Microsoft Azure

Azure provides suitable infrastructure, orchestration, storage, and analytics services. It was not selected because the project has no existing Azure dependency or account advantage.

### Provider-Agnostic or Multi-Cloud Architecture

Limiting the implementation to provider-neutral services could improve portability, but it would reduce the opportunity to demonstrate practical use of managed cloud capabilities. Supporting multiple providers would also add substantial complexity without advancing the project goal.

## Consequences

- Infrastructure modules and deployment documentation will target GCP.
- Cloud service selection can make use of GCP-managed capabilities where they simplify operation or improve portfolio value.
- Project 2 will demonstrate depth with one provider rather than portability across several providers.
- Some Terraform resources and operational knowledge will be GCP-specific.
- Migrating the implementation to another provider would require replacing service integrations and infrastructure definitions.
- The remaining architecture decisions must evaluate GCP services and their cost profiles.

## Cost Implications

Using an existing account removes account creation overhead but does not guarantee free usage. Managed orchestration, continuously running compute, and analytics services may incur charges even when the pipeline is idle.

Before deployment:

- Define a project budget and configure billing alerts.
- Prefer resources that can be stopped, scaled to zero, or destroyed when not in use.
- Document expected costs for each selected service.
- Use `terraform destroy` as part of the documented cleanup workflow.
- Avoid provisioning long-running managed services until their cost and teardown behavior are understood.
