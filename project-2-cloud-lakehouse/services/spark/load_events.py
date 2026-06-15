import argparse
import base64
import json
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


METADATA_URL = (
    "http://metadata.google.internal/computeMetadata/v1/instance/"
    "service-accounts/default"
)


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
        description="Load the ecommerce CSV sample into an Iceberg table."
    )
    parser.add_argument("--source", required=True)
    parser.add_argument("--polaris-url", required=True)
    parser.add_argument("--polaris-secret", required=True)
    parser.add_argument("--catalog", default="lakehouse")
    parser.add_argument("--namespace", default="bronze")
    parser.add_argument("--table", default="events")
    parser.add_argument("--expected-rows", type=int, default=10_000)
    return parser.parse_args()


def event_schema() -> StructType:
    return StructType(
        [
            StructField("event_time", TimestampType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),
            StructField("product_id", LongType(), nullable=False),
            StructField("category_id", LongType(), nullable=False),
            StructField("category_code", StringType(), nullable=True),
            StructField("brand", StringType(), nullable=True),
            StructField("price", DecimalType(12, 2), nullable=False),
            StructField("user_id", LongType(), nullable=False),
            StructField("user_session", StringType(), nullable=True),
        ]
    )


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

    spark = (
        SparkSession.builder.appName("load-events")
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
        .config("spark.sql.catalog.polaris.warehouse", args.catalog)
        .config("spark.sql.catalog.polaris.token", polaris_token)
        .config(
            "spark.sql.catalog.polaris.header.X-Serverless-Authorization",
            f"Bearer {identity_token}",
        )
        .getOrCreate()
    )

    events = (
        spark.read.option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss 'UTC'")
        .schema(event_schema())
        .csv(args.source)
    )
    row_count = events.count()
    if row_count != args.expected_rows:
        raise ValueError(
            f"Source contained {row_count:,} rows; "
            f"expected {args.expected_rows:,}"
        )

    table = f"polaris.`{args.namespace}`.`{args.table}`"
    events.writeTo(table).using("iceberg").createOrReplace()
    print(f"Loaded {row_count:,} rows into {table}")
    spark.stop()


if __name__ == "__main__":
    main()
