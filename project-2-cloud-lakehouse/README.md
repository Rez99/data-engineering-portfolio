# Project 2: Cloud Lakehouse

Project 2 migrates the local lakehouse from Project 1 to Google Cloud while
retaining Apache Iceberg, Apache Polaris, dbt Core, and XGBoost.

The architecture prerequisites and **M1: Extract** are complete. The next
data-flow milestone is **M2: Load**, which converts the raw clickstream sample
into a bronze Iceberg table registered in Polaris.
See [PROJECT_2_SPEC.md](PROJECT_2_SPEC.md) for the delivery plan and
[docs/architecture.md](docs/architecture.md) for the selected architecture.

## Repository Structure

```text
project-2-cloud-lakehouse/
├── dbt/                  # dbt project adapted for Spark
├── docs/                 # Architecture documentation and ADRs
├── setup.sh              # Optional end-to-end development setup
├── services/
│   ├── ingestion/        # Clickstream sampling and ingestion Cloud Run Job
│   ├── ml/               # XGBoost training and evaluation Cloud Run Job
│   └── polaris/          # Polaris Cloud Run Service packaging and configuration
├── terraform/            # GCP infrastructure as code
├── tests/
│   └── integration/      # Cross-service validation and smoke tests
└── workflows/            # Google Cloud Workflows definitions
```

Implementation is added incrementally as each milestone provisions and
validates its assigned components.

## Local Tooling

Docker is the only local runtime required. Google Cloud CLI and Terraform run
from pinned container images, so contributors do not need to install either
tool locally.

The login and Application Default Credentials are stored in the gitignored
`.credentials/gcloud/` directory. The commands below mount those credentials
into the Terraform and Google Cloud CLI containers.

For an automated end-to-end run:

```bash
./setup.sh
```

To destroy all Terraform-managed resources:

```bash
./destroy.sh
```

Credential files, Terraform state, plans, and local variable files are ignored
by Git.

## How to run

1. Start the Terraform container:

```bash
docker run -d \
  --name lakehouse-terraform \
  --entrypoint /bin/sh \
  -v "$PWD:/workspace" \
  -v "$PWD/.credentials/gcloud/application_default_credentials.json:/credentials/gcp.json:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/gcp.json \
  -w /workspace/terraform \
  hashicorp/terraform:1.15.6 \
  -c "sleep infinity"
```
2. Enter it:
```bash
docker exec -it lakehouse-terraform /bin/sh
```
3. Inside the container:
```bash
terraform init
terraform apply
```

4. Exit the Terraform container and authenticate Docker with Artifact Registry:

```bash
exit

docker run --rm \
  -v "$PWD/.credentials/gcloud:/config" \
  -e CLOUDSDK_CONFIG=/config \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:572.0.0-stable \
  gcloud auth print-access-token |
docker login \
  --username oauth2accesstoken \
  --password-stdin \
  us-central1-docker.pkg.dev
```

5. Build and push the real ingestion image:

```bash
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --target runtime \
  --tag us-central1-docker.pkg.dev/rez-cloud-lakehouse/pipeline/ingestion:dev-amd64 \
  --push \
  services/ingestion
```

6. Re-enter the Terraform container:

```bash
docker exec -it lakehouse-terraform /bin/sh
```

7. Update the Cloud Run Job to use the ingestion image:

```bash
terraform apply -var='ingestion_image=us-central1-docker.pkg.dev/rez-cloud-lakehouse/pipeline/ingestion:dev-amd64'
```

8. Exit the Terraform container and manually start `lakehouse-extract`:

```bash
exit

docker run --rm -it \
  -v "$PWD/.credentials/gcloud:/config" \
  -e CLOUDSDK_CONFIG=/config \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:572.0.0-stable \
  gcloud workflows run lakehouse-extract \
  --location=us-central1 \
  --project=rez-cloud-lakehouse
```
