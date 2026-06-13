# Project 2 Architecture

This document will describe the selected cloud architecture after the foundational decisions in Milestone 1 have been accepted.

It should include:

- A mapping from Project 1 components to their Project 2 cloud equivalents.
- The final architecture diagram from ADR-0003.
- Data flow from ingestion through consumption.
- Deployment and operational boundaries.
- Links to the ADRs that explain significant decisions.

## Current Status

Google Cloud has been selected as the cloud provider. ADR-0002 evaluates the candidate service mappings, while ADR-0003 records the selected architecture. The current direction uses Workflows and Cloud Scheduler for orchestration, a temporary Managed Spark cluster with dbt Core, Polaris, and Iceberg for lakehouse processing, Cloud Storage for data, and Cloud Run Jobs for lightweight tasks. The dbt-to-Spark-to-Polaris integration must still pass the validation spike defined in ADR-0003.

## Accepted Decisions

- [ADR-0001: Select Google Cloud as the Cloud Provider](adr/0001-cloud-provider.md)
- [ADR-0002: Evaluate Project 1 to GCP Service Mappings](adr/0002-service-mapping.md)

## Proposed Decisions

- [ADR-0003: Select the Project 2 Cloud Architecture](adr/0003-cloud-architecture.md)
