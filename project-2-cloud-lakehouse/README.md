# Project 2: Cloud Lakehouse

Project 2 migrates the local lakehouse from Project 1 to Google Cloud while
retaining Apache Iceberg, Apache Polaris, dbt Core, and XGBoost.

The core data-flow milestones are complete: extract, load, transform, train,
and consume. The current pipeline runs through one parent Google Workflow that
creates a temporary Spark cluster, runs the data pipeline, deletes the cluster,
and refreshes Superset.
See [PROJECT_2_SPEC.md](PROJECT_2_SPEC.md) for the delivery plan and
[docs/architecture.md](docs/architecture.md) for the selected architecture.

## Repository Structure

The project is organized around a simple deployment lifecycle:

```text
terraform/      # Stage 1: PROVISION cloud resources
bootstrap/      # Stage 2: INITIALIZE platform state
terraform-polaris/ # Stage 2b: MANAGE Polaris catalog state in Terraform
terraform-superset/ # Stage 2c: MANAGE Superset assets in Terraform
deployment/     # Stage 3: PUBLISH deployable project artifacts
run.sh          # Stage 4: RUN the deployed pipeline
```

```text
project-2-cloud-lakehouse/
├── bootstrap/            # Stage 2: INITIALIZE platform state
│   ├── polaris/          # Execute minimal Polaris realm/root bootstrap
│   ├── superset/         # Execute minimal Superset metadata/admin bootstrap
│   └── README.md         # Minimal bootstrap boundary
├── deployment/           # Stage 3: PUBLISH deployable project artifacts
│   ├── containers/       # Cloud Run image build contexts for platform services
│   ├── dbt/              # dbt project adapted for Spark
│   ├── spark/            # Spark job entrypoints uploaded to GCS
│   ├── workflows/        # Google Cloud Workflows source definitions
│   └── manifest.example.json
├── docs/                 # Architecture documentation and ADRs
├── run.sh                # Stage 4: RUN the deployed parent workflow
├── setup.sh              # Optional end-to-end wrapper for all stages
├── terraform/            # Stage 1: PROVISION cloud resources
├── terraform-polaris/    # Stage 2b: MANAGE Polaris catalog state
├── terraform-superset/   # Stage 2c: MANAGE Superset dashboards/charts/datasets
├── tests/
│   ├── ingestion/        # Unit tests for ingestion code
│   ├── integration/      # Cross-service validation and smoke tests
│   └── ml/               # Unit tests for ML code
└── destroy.sh            # Tear down Terraform-managed resources
```

Implementation is organized so infrastructure, platform bootstrap, deployable
artifacts, and pipeline execution remain visible as separate stages.

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

To trigger the deployed end-to-end pipeline without rebuilding or reprovisioning:

```bash
./run.sh
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
3. Inside the container, provision Artifact Registry first:
```bash
terraform init
terraform apply \
  -target=google_artifact_registry_repository.pipeline
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

5. Build and push the Superset image:

```bash
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --tag us-central1-docker.pkg.dev/rez-cloud-lakehouse/pipeline/superset:dev-amd64 \
  --push \
  deployment/containers/superset
```

6. Re-enter the Terraform container:

```bash
docker exec -it lakehouse-terraform /bin/sh
```

7. Provision the full platform and deploy GCS-backed pipeline artifacts:

```bash
terraform apply \
  -var='superset_image=us-central1-docker.pkg.dev/rez-cloud-lakehouse/pipeline/superset:dev-amd64'
```

8. Bootstrap Polaris and Superset, then configure their managed assets:

```bash
exit

bootstrap/polaris/run.sh
bootstrap/superset/run.sh
```

9. Trigger the deployed parent workflow:

```bash
./run.sh
```
