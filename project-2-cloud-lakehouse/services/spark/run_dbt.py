import argparse
import base64
import importlib
import json
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

from pyspark import SparkContext
from pyspark.sql import SparkSession


METADATA_URL = (
    "http://metadata.google.internal/computeMetadata/v1/instance/"
    "service-accounts/default"
)
DBT_PACKAGE = "dbt-spark==1.10.1"


def metadata_request(path: str) -> str:
    request = urllib.request.Request(
        f"{METADATA_URL}/{path}",
        headers={"Metadata-Flavor": "Google"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8")


def get_access_token() -> str:
    return json.loads(metadata_request("token"))["access_token"]


def get_identity_token(audience: str) -> str:
    query = urllib.parse.urlencode({"audience": audience, "format": "full"})
    return metadata_request(f"identity?{query}")


def get_secret(secret_resource: str, access_token: str) -> str:
    request = urllib.request.Request(
        (
            "https://secretmanager.googleapis.com/v1/"
            f"{secret_resource}/versions/latest:access"
        ),
        headers={"Authorization": f"Bearer {access_token}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)
    return base64.b64decode(payload["payload"]["data"]).decode("utf-8")


def download_gcs_object(uri: str, destination: Path, access_token: str) -> None:
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Expected a GCS URI, received: {uri}")

    object_name = urllib.parse.quote(parsed.path.lstrip("/"), safe="")
    request = urllib.request.Request(
        (
            "https://storage.googleapis.com/download/storage/v1/b/"
            f"{parsed.netloc}/o/{object_name}?alt=media"
        ),
        headers={"Authorization": f"Bearer {access_token}"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        destination.write_bytes(response.read())


def get_polaris_token(
    polaris_url: str,
    identity_token: str,
    root_secret: str,
) -> str:
    credentials = base64.b64encode(
        f"admin:{root_secret}".encode("utf-8")
    ).decode("ascii")
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "scope": "PRINCIPAL_ROLE:ALL",
        }
    ).encode("ascii")
    request = urllib.request.Request(
        f"{polaris_url}/api/catalog/v1/oauth/tokens",
        data=body,
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Serverless-Authorization": f"Bearer {identity_token}",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)["access_token"]


def install_dbt() -> None:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--quiet",
            "--disable-pip-version-check",
            DBT_PACKAGE,
        ],
        check=True,
    )
    importlib.invalidate_caches()


def write_runtime_profile(
    project_dir: Path,
    polaris_url: str,
    polaris_token: str,
    identity_token: str,
) -> None:
    profile = f"""lakehouse:
  target: runtime

  outputs:
    runtime:
      type: spark
      method: session
      host: local-spark-session
      schema: bronze
      threads: 1
      server_side_parameters:
        spark.sql.defaultCatalog: polaris
        spark.sql.catalog.polaris: org.apache.iceberg.spark.SparkCatalog
        spark.sql.catalog.polaris.type: rest
        spark.sql.catalog.polaris.uri: {polaris_url}/api/catalog
        spark.sql.catalog.polaris.warehouse: lakehouse
        spark.sql.catalog.polaris.token: {polaris_token}
        spark.sql.catalog.polaris.header.X-Serverless-Authorization: "Bearer {identity_token}"
"""
    (project_dir / "profiles.yml").write_text(profile)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run selected dbt models through Spark and Polaris."
    )
    parser.add_argument("--project-archive", required=True)
    parser.add_argument("--polaris-url", required=True)
    parser.add_argument("--polaris-secret", required=True)
    parser.add_argument("--selector", required=True)
    parser.add_argument("--verify-table", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    polaris_url = args.polaris_url.rstrip("/")
    access_token = get_access_token()
    identity_token = get_identity_token(polaris_url)
    root_secret = get_secret(args.polaris_secret, access_token)
    polaris_token = get_polaris_token(
        polaris_url,
        identity_token,
        root_secret,
    )

    install_dbt()

    SparkContext.getOrCreate()
    with tempfile.TemporaryDirectory(prefix="lakehouse-dbt-") as temp_dir:
        temp_path = Path(temp_dir)
        archive_path = temp_path / "dbt-project.zip"
        project_dir = temp_path / "project"
        project_dir.mkdir()
        download_gcs_object(args.project_archive, archive_path, access_token)
        with zipfile.ZipFile(archive_path) as archive:
            archive.extractall(project_dir)
        write_runtime_profile(
            project_dir,
            polaris_url,
            polaris_token,
            identity_token,
        )

        from dbt.cli.main import dbtRunner

        result = dbtRunner().invoke(
            [
                "run",
                "--project-dir",
                str(project_dir),
                "--profiles-dir",
                str(project_dir),
                "--select",
                args.selector,
            ]
        )
        if not result.success:
            raise RuntimeError(
                f"dbt run failed for selector: {args.selector}"
            )

        row_count = SparkSession.builder.getOrCreate().table(
            args.verify_table
        ).count()

    print(
        f"dbt run verified: {args.verify_table}, rows={row_count:,}"
    )


if __name__ == "__main__":
    main()
