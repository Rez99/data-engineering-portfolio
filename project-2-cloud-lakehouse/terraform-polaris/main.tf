locals {
  warehouse_location = trimsuffix(var.warehouse_location, "/")
  namespace_path     = join("\u001F", var.namespace)
}

resource "polaris_rest_resource" "catalog" {
  provider = polaris.management

  create_operation_id = "createCatalog"
  read_operation_id   = "getCatalog"
  delete_operation_id = "deleteCatalog"

  path_params = {
    catalogName = var.catalog_name
  }

  body = jsonencode({
    catalog = {
      name     = var.catalog_name
      type     = "INTERNAL"
      readOnly = false
      properties = {
        "default-base-location" = "${local.warehouse_location}/"
      }
      storageConfigInfo = {
        storageType       = "GCS"
        allowedLocations  = ["${local.warehouse_location}/"]
        gcsServiceAccount = var.gcs_service_account
      }
    }
  })

  id_attribute = "name"
}

resource "polaris_rest_resource" "catalog_admin_table_write" {
  provider = polaris.management

  create_operation_id = "addGrantToCatalogRole"
  read_operation_id   = "listGrantsForCatalogRole"

  path_params = {
    catalogName     = polaris_rest_resource.catalog.id
    catalogRoleName = "catalog_admin"
  }

  body = jsonencode({
    grant = {
      type      = "catalog"
      privilege = "TABLE_WRITE_DATA"
    }
  })

  depends_on = [
    polaris_rest_resource.catalog,
  ]
}

resource "polaris_rest_resource" "namespace" {
  provider = polaris.catalog

  create_operation_id = "createNamespace"
  read_operation_id   = "loadNamespaceMetadata"
  delete_operation_id = "dropNamespace"

  path_params = {
    prefix    = polaris_rest_resource.catalog.id
    namespace = local.namespace_path
  }

  body = jsonencode({
    namespace = var.namespace
  })

  depends_on = [
    polaris_rest_resource.catalog_admin_table_write,
  ]
}
