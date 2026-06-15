import argparse
import base64
import json
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession


METADATA_URL = (
    "http://metadata.google.internal/computeMetadata/v1/instance/"
    "service-accounts/default"
)
EXPECTED_SCHEMA = [
    ("event_time", "timestamp"),
    ("event_type", "string"),
    ("product_id", "bigint"),
    ("category_id", "bigint"),
    ("category_code", "string"),
    ("brand", "string"),
    ("price", "decimal(12,2)"),
    ("user_id", "bigint"),
    ("user_session", "string"),
]


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the bronze ecommerce Iceberg table."
    )
    parser.add_argument("--polaris-url", required=True)
    parser.add_argument("--polaris-secret", required=True)
    parser.add_argument("--catalog", default="lakehouse")
    parser.add_argument("--namespace", default="bronze")
    parser.add_argument("--table", default="events")
    parser.add_argument("--expected-rows", type=int, default=10_000)
    return parser.parse_args()


def create_spark(
    polaris_url: str,
    catalog: str,
    polaris_token: str,
    identity_token: str,
) -> SparkSession:
    return (
        SparkSession.builder.appName("validate-events")
        .config(
            "spark.sql.catalog.polaris",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", f"{polaris_url}/api/catalog")
        .config(
            "spark.sql.catalog.polaris.oauth2-server-uri",
            f"{polaris_url}/api/catalog/v1/oauth/tokens",
        )
        .config("spark.sql.catalog.polaris.warehouse", catalog)
        .config("spark.sql.catalog.polaris.token", polaris_token)
        .config(
            "spark.sql.catalog.polaris.header.X-Serverless-Authorization",
            f"Bearer {identity_token}",
        )
        .getOrCreate()
    )


def validate_files_exist(spark: SparkSession, file_paths: list[str]) -> None:
    hadoop_configuration = spark.sparkContext._jsc.hadoopConfiguration()
    path_class = spark.sparkContext._jvm.org.apache.hadoop.fs.Path

    missing = []
    for file_path in file_paths:
        path = path_class(file_path)
        if not path.getFileSystem(hadoop_configuration).exists(path):
            missing.append(file_path)

    if missing:
        raise ValueError(f"Missing Iceberg data files: {missing}")


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
    spark = create_spark(
        polaris_url,
        args.catalog,
        polaris_token,
        identity_token,
    )

    table = f"polaris.`{args.namespace}`.`{args.table}`"
    events = spark.table(table)
    actual_schema = [
        (field.name, field.dataType.simpleString())
        for field in events.schema.fields
    ]
    if actual_schema != EXPECTED_SCHEMA:
        raise ValueError(
            f"Unexpected schema: {actual_schema}; expected {EXPECTED_SCHEMA}"
        )

    row_count = events.count()
    if row_count != args.expected_rows:
        raise ValueError(
            f"Table contained {row_count:,} rows; "
            f"expected {args.expected_rows:,}"
        )

    metadata_table = f"polaris.{args.namespace}.{args.table}"
    snapshot_count = spark.table(f"{metadata_table}.snapshots").count()
    if snapshot_count < 1:
        raise ValueError("Iceberg table has no snapshots")

    files = spark.table(f"{metadata_table}.files").select(
        "file_path",
        "record_count",
    )
    file_rows = files.collect()
    if not file_rows:
        raise ValueError("Iceberg table has no data files")

    file_record_count = sum(row.record_count for row in file_rows)
    if file_record_count != args.expected_rows:
        raise ValueError(
            f"Iceberg files contain {file_record_count:,} records; "
            f"expected {args.expected_rows:,}"
        )

    file_paths = [row.file_path for row in file_rows]
    validate_files_exist(spark, file_paths)

    print(
        "Validated Iceberg table: "
        f"{table}, rows={row_count:,}, columns={len(actual_schema)}, "
        f"snapshots={snapshot_count}, data_files={len(file_paths)}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
