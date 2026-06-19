provider "superset" {
  endpoint = var.superset_endpoint
  username = var.superset_username
  password = var.superset_password
}

locals {
  asset_root = "${path.module}/../../deployment/containers/superset/assets"

  database_asset = yamldecode(file("${local.asset_root}/databases/XGBoost_Model_Metrics.yaml"))

  dataset_assets = {
    metrics            = yamldecode(file("${local.asset_root}/datasets/XGBoost_Model_Metrics/metrics_1.yaml"))
    confusion_matrix   = yamldecode(file("${local.asset_root}/datasets/XGBoost_Model_Metrics/confusion_matrix_2.yaml"))
    feature_importance = yamldecode(file("${local.asset_root}/datasets/XGBoost_Model_Metrics/feature_importance_3.yaml"))
    roc_curve          = yamldecode(file("${local.asset_root}/datasets/XGBoost_Model_Metrics/roc_curve_4.yaml"))
    model_comparison   = yamldecode(file("${local.asset_root}/datasets/XGBoost_Model_Metrics/model_comparison_5.yaml"))
  }

  dataset_key_by_uuid = {
    for key, asset in local.dataset_assets : asset.uuid => key
  }

  chart_assets = {
    model_comparison   = yamldecode(file("${local.asset_root}/charts/Model_vs_Majority_Class_Baseline_1.yaml"))
    confusion_matrix   = yamldecode(file("${local.asset_root}/charts/Confusion_Matrix_4.yaml"))
    feature_importance = yamldecode(file("${local.asset_root}/charts/Feature_Importance_5.yaml"))
    roc_curve          = yamldecode(file("${local.asset_root}/charts/ROC_Curve_6.yaml"))
  }

  dashboard_asset = yamldecode(file("${local.asset_root}/dashboards/XGBoost_Model_Evaluation_1.yaml"))
}

resource "superset_database" "xgboost_metrics" {
  database_name     = local.database_asset.database_name
  sqlalchemy_uri    = local.database_asset.sqlalchemy_uri
  cache_timeout     = local.database_asset.cache_timeout
  expose_in_sqllab  = local.database_asset.expose_in_sqllab
  allow_run_async   = local.database_asset.allow_run_async
  allow_ctas        = local.database_asset.allow_ctas
  allow_cvas        = local.database_asset.allow_cvas
  allow_dml         = local.database_asset.allow_dml
  allow_file_upload = local.database_asset.allow_file_upload
  extra             = jsonencode(local.database_asset.extra)
  impersonate_user  = local.database_asset.impersonate_user
}

resource "superset_dataset" "model_metrics" {
  for_each = local.dataset_assets

  database_id             = superset_database.xgboost_metrics.id
  table_name              = each.value.table_name
  schema                  = each.value.schema
  description             = each.value.description
  main_dttm_col           = each.value.main_dttm_col
  filter_select_enabled   = each.value.filter_select_enabled
  normalize_columns       = each.value.normalize_columns
  always_filter_main_dttm = each.value.always_filter_main_dttm
  cache_timeout           = each.value.cache_timeout

  columns = [
    for column in each.value.columns : {
      column_name        = column.column_name
      verbose_name       = column.verbose_name
      description        = column.description
      expression         = column.expression
      filterable         = column.filterable
      groupby            = column.groupby
      is_active          = column.is_active
      is_dttm            = column.is_dttm
      type               = column.type
      python_date_format = column.python_date_format
    }
  ]

  metrics = [
    for metric in each.value.metrics : {
      metric_name  = metric.metric_name
      expression   = metric.expression
      metric_type  = metric.metric_type
      verbose_name = metric.verbose_name
      description  = metric.description
      d3format     = metric.d3format
      warning_text = metric.warning_text
    }
  ]
}

resource "superset_chart" "model_metrics" {
  for_each = local.chart_assets

  slice_name    = each.value.slice_name
  description   = each.value.description
  datasource_id = superset_dataset.model_metrics[local.dataset_key_by_uuid[each.value.dataset_uuid]].id
  viz_type      = each.value.viz_type
  cache_timeout = each.value.cache_timeout

  params = jsonencode(merge(each.value.params, {
    datasource = format(
      "%d__table",
      superset_dataset.model_metrics[local.dataset_key_by_uuid[each.value.dataset_uuid]].id
    )
  }))
}

resource "superset_dashboard" "xgboost_model_evaluation" {
  dashboard_title = local.dashboard_asset.dashboard_title
  slug            = local.dashboard_asset.slug
  css             = local.dashboard_asset.css
  published       = local.dashboard_asset.published

  chart_ids = [
    superset_chart.model_metrics["model_comparison"].id,
    superset_chart.model_metrics["confusion_matrix"].id,
    superset_chart.model_metrics["feature_importance"].id,
    superset_chart.model_metrics["roc_curve"].id,
  ]

  native_filter_configuration = jsonencode(local.dashboard_asset.metadata.native_filter_configuration)

  position_json = jsonencode({
    DASHBOARD_VERSION_KEY = "v2"
    ROOT_ID = {
      id       = "ROOT_ID"
      type     = "ROOT"
      children = ["GRID_ID"]
    }
    GRID_ID = {
      id       = "GRID_ID"
      type     = "GRID"
      parents  = ["ROOT_ID"]
      children = ["ROW-1", "ROW-2", "ROW-3"]
    }
    HEADER_ID = {
      id   = "HEADER_ID"
      type = "HEADER"
      meta = {
        text = local.dashboard_asset.dashboard_title
      }
    }
    CHART_MODEL_COMPARISON = {
      id       = "CHART_MODEL_COMPARISON"
      type     = "CHART"
      children = []
      parents  = ["ROOT_ID", "GRID_ID", "ROW-1"]
      meta = {
        chartId   = superset_chart.model_metrics["model_comparison"].id
        height    = 24
        sliceName = superset_chart.model_metrics["model_comparison"].slice_name
        uuid      = superset_chart.model_metrics["model_comparison"].uuid
        width     = 12
      }
    }
    "ROW-1" = {
      id       = "ROW-1"
      type     = "ROW"
      children = ["CHART_MODEL_COMPARISON"]
      parents  = ["ROOT_ID", "GRID_ID"]
      meta = {
        background = "BACKGROUND_TRANSPARENT"
      }
    }
    CHART_CONFUSION_MATRIX = {
      id       = "CHART_CONFUSION_MATRIX"
      type     = "CHART"
      children = []
      parents  = ["ROOT_ID", "GRID_ID", "ROW-2"]
      meta = {
        chartId   = superset_chart.model_metrics["confusion_matrix"].id
        height    = 30
        sliceName = superset_chart.model_metrics["confusion_matrix"].slice_name
        uuid      = superset_chart.model_metrics["confusion_matrix"].uuid
        width     = 12
      }
    }
    "ROW-2" = {
      id       = "ROW-2"
      type     = "ROW"
      children = ["CHART_CONFUSION_MATRIX"]
      parents  = ["ROOT_ID", "GRID_ID"]
      meta = {
        background = "BACKGROUND_TRANSPARENT"
      }
    }
    CHART_FEATURE_IMPORTANCE = {
      id       = "CHART_FEATURE_IMPORTANCE"
      type     = "CHART"
      children = []
      parents  = ["ROOT_ID", "GRID_ID", "ROW-3"]
      meta = {
        chartId   = superset_chart.model_metrics["feature_importance"].id
        height    = 40
        sliceName = superset_chart.model_metrics["feature_importance"].slice_name
        uuid      = superset_chart.model_metrics["feature_importance"].uuid
        width     = 6
      }
    }
    CHART_ROC_CURVE = {
      id       = "CHART_ROC_CURVE"
      type     = "CHART"
      children = []
      parents  = ["ROOT_ID", "GRID_ID", "ROW-3"]
      meta = {
        chartId   = superset_chart.model_metrics["roc_curve"].id
        height    = 40
        sliceName = superset_chart.model_metrics["roc_curve"].slice_name
        uuid      = superset_chart.model_metrics["roc_curve"].uuid
        width     = 6
      }
    }
    "ROW-3" = {
      id       = "ROW-3"
      type     = "ROW"
      children = ["CHART_FEATURE_IMPORTANCE", "CHART_ROC_CURVE"]
      parents  = ["ROOT_ID", "GRID_ID"]
      meta = {
        background = "BACKGROUND_TRANSPARENT"
      }
    }
  })
}
