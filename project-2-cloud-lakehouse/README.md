# Project 2: Cloud Lakehouse

Project 2 migrates the local lakehouse from Project 1 to Google Cloud while
retaining Apache Iceberg, Apache Polaris, dbt Core, and XGBoost.

The project is currently in Milestone 1: architecture and repository setup.
See [PROJECT_2_SPEC.md](PROJECT_2_SPEC.md) for the delivery plan and
[docs/architecture.md](docs/architecture.md) for the selected architecture.

## Repository Structure

```text
project-2-cloud-lakehouse/
├── dbt/                  # dbt project adapted for Spark
├── docs/                 # Architecture documentation and ADRs
├── scripts/              # Human-facing deployment and teardown commands
├── services/
│   ├── ingestion/        # Clickstream sampling and ingestion Cloud Run Job
│   ├── ml/               # XGBoost training and evaluation Cloud Run Job
│   └── polaris/          # Polaris Cloud Run Service packaging and configuration
├── terraform/            # GCP infrastructure as code
├── tests/
│   └── integration/      # Cross-service validation and smoke tests
└── workflows/            # Google Cloud Workflows definitions
```

The directories are placeholders for later milestones. Implementation will be
added incrementally after the architecture validation spike.

## Local Tooling

Docker is the only local runtime required. Google Cloud CLI and Terraform run
from pinned container images, so contributors do not need to install either
tool locally.

Authenticate and set the target project:

```bash
./scripts/gcloud.sh auth login --update-adc
./scripts/gcloud.sh config set project rez-cloud-lakehouse
./scripts/gcloud.sh config set run/region us-central1
```

The login and Application Default Credentials are stored in the gitignored
`.credentials/gcloud/` directory. Terraform detects and mounts those
credentials read-only.

Run Terraform:

```bash
./scripts/terraform.sh version
./scripts/terraform.sh init
./scripts/terraform.sh plan
```

For non-interactive environments, a separate credential file can override the
project-local login:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/path/to/gcp-credentials.json"
./scripts/terraform.sh plan
```

Credential files, Terraform state, plans, and local variable files are ignored
by Git.
