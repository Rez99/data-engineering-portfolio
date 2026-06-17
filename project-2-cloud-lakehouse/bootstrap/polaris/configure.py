import os
from typing import Any

import requests
from pyiceberg.catalog.rest import RestCatalog


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Required environment variable is not set: {name}")
    return value


def request_polaris_token(
    polaris_url: str,
    identity_token: str,
    realm: str,
    client_id: str,
    client_secret: str,
) -> str:
    response = requests.post(
        f"{polaris_url}/api/catalog/v1/oauth/tokens",
        headers={
            "Polaris-Realm": realm,
            "X-Serverless-Authorization": f"Bearer {identity_token}",
        },
        auth=(client_id, client_secret),
        data={
            "grant_type": "client_credentials",
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def create_catalog(
    polaris_url: str,
    identity_token: str,
    polaris_token: str,
    realm: str,
    catalog_name: str,
    warehouse: str,
    service_account: str,
) -> None:
    payload: dict[str, Any] = {
        "catalog": {
            "name": catalog_name,
            "type": "INTERNAL",
            "readOnly": False,
            "properties": {"default-base-location": warehouse},
            "storageConfigInfo": {
                "storageType": "GCS",
                "allowedLocations": [warehouse],
                "gcsServiceAccount": service_account,
            },
        }
    }
    response = requests.post(
        f"{polaris_url}/api/management/v1/catalogs",
        headers={
            "Authorization": f"Bearer {polaris_token}",
            "Content-Type": "application/json",
            "Polaris-Realm": realm,
            "X-Serverless-Authorization": f"Bearer {identity_token}",
        },
        json=payload,
        timeout=30,
    )
    if response.status_code == 409:
        print(f"Catalog already exists: {catalog_name}")
        return
    response.raise_for_status()
    print(f"Created catalog: {catalog_name}")


def grant_table_write_data(
    polaris_url: str,
    identity_token: str,
    polaris_token: str,
    realm: str,
    catalog_name: str,
) -> None:
    grants_url = (
        f"{polaris_url}/api/management/v1/catalogs/{catalog_name}"
        "/catalog-roles/catalog_admin/grants"
    )
    headers = {
        "Authorization": f"Bearer {polaris_token}",
        "Content-Type": "application/json",
        "Polaris-Realm": realm,
        "X-Serverless-Authorization": f"Bearer {identity_token}",
    }
    response = requests.get(grants_url, headers=headers, timeout=30)
    response.raise_for_status()
    if "TABLE_WRITE_DATA" in response.text:
        print(f"Verified TABLE_WRITE_DATA grant: {catalog_name}.catalog_admin")
        return

    response = requests.put(
        grants_url,
        headers=headers,
        json={"type": "catalog", "privilege": "TABLE_WRITE_DATA"},
        timeout=30,
    )
    response.raise_for_status()
    print(f"Verified TABLE_WRITE_DATA grant: {catalog_name}.catalog_admin")


def configure_warehouse() -> None:
    polaris_url = required_env("POLARIS_URL").rstrip("/")
    identity_token = required_env("CLOUD_RUN_IDENTITY_TOKEN")
    root_client_secret = required_env("POLARIS_ROOT_CLIENT_SECRET")
    warehouse = required_env("POLARIS_WAREHOUSE").rstrip("/") + "/"
    service_account = required_env("POLARIS_GCS_SERVICE_ACCOUNT")

    realm = os.getenv("POLARIS_REALM", "POLARIS")
    root_client_id = os.getenv("POLARIS_ROOT_CLIENT_ID", "admin")
    catalog_name = os.getenv("POLARIS_CATALOG_NAME", "lakehouse")
    namespace = os.getenv("POLARIS_NAMESPACE", "bronze")

    polaris_token = request_polaris_token(
        polaris_url,
        identity_token,
        realm,
        root_client_id,
        root_client_secret,
    )
    create_catalog(
        polaris_url,
        identity_token,
        polaris_token,
        realm,
        catalog_name,
        warehouse,
        service_account,
    )
    grant_table_write_data(
        polaris_url,
        identity_token,
        polaris_token,
        realm,
        catalog_name,
    )

    catalog = RestCatalog(
        catalog_name,
        uri=f"{polaris_url}/api/catalog",
        warehouse=catalog_name,
        token=polaris_token,
        **{
            "header.Polaris-Realm": realm,
            "header.X-Serverless-Authorization": f"Bearer {identity_token}",
        },
    )

    if catalog.namespace_exists(namespace):
        print(f"Namespace already exists: {namespace}")
    else:
        catalog.create_namespace(namespace)
        print(f"Created namespace: {namespace}")

    print(
        "Warehouse configuration verified: "
        f"{catalog_name}.{namespace}"
    )


if __name__ == "__main__":
    configure_warehouse()
