# Ingestion Job

This Cloud Run Job streams and validates a deterministic sample of the public
REES46 clickstream dataset in Cloud Storage.

It decompresses the source response line by line, copies the header and exact
number of configured data rows, recompresses the sample, and writes it directly
to GCS. The full source dataset and sample are never materialized in memory or
on local disk.

After upload, the job independently reopens the object from GCS and verifies:

- the object is valid gzip-compressed CSV;
- the columns match the expected clickstream schema;
- the object contains exactly the configured number of data rows.

## Configuration

| Environment variable | Purpose |
| --- | --- |
| `SOURCE_URL` | Gzip-compressed source CSV |
| `DESTINATION_BUCKET` | Target GCS bucket |
| `DESTINATION_OBJECT` | Target object path |
| `MAX_ROWS` | Exact number of data rows to copy, excluding the header |

## Test

```bash
docker build \
  --platform linux/amd64 \
  --target test \
  --tag lakehouse-ingestion:test \
  services/ingestion
```

The test stage verifies the exact row limit and failure behavior for empty or
undersized sources.

## Runtime Image

The current image is:

```text
us-central1-docker.pkg.dev/rez-cloud-lakehouse/pipeline/ingestion:m1-5-v2
```

## Deployment and Execution Path

The Python file is packaged and executed through this chain:

```text
services/ingestion/extract.py
        |
        | COPY extract.py .
        v
Docker image built from services/ingestion/Dockerfile
        |
        | docker push
        v
Artifact Registry: pipeline/ingestion:m1-5-v2
        |
        | image configured by Terraform
        v
Cloud Run Job: lakehouse-ingestion
        |
        | container ENTRYPOINT
        v
python extract.py
```

`extract.py` is not uploaded to Cloud Run as a standalone file. It is copied
into the Docker image during `docker build`. Cloud Run pulls that image from
Artifact Registry, starts a temporary container, and executes:

```text
python extract.py
```

Terraform supplies the environment variables and assigns the
`lakehouse-ingestion` service account. The container exits after extraction;
the resulting object remains in Cloud Storage.

## Orchestrated Execution

The `lakehouse-extract` Google Cloud Workflow invokes the Cloud Run Job through
the Cloud Run connector and waits for the job operation to finish:

```bash
docker run --rm -it \
  -v "$PWD/.credentials/gcloud:/config" \
  -e CLOUDSDK_CONFIG=/config \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:572.0.0-stable \
  gcloud workflows run lakehouse-extract \
  --location=us-central1 \
  --project=rez-cloud-lakehouse \
  --call-log-level=log-errors-only
```

The workflow uses its own `lakehouse-workflow` service account. That identity
can invoke the ingestion job and read Cloud Run operation status, but the
ingestion container uses the separate `lakehouse-ingestion` identity to write
objects to GCS.
