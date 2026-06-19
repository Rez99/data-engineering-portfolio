# Superset Terraform

This Terraform root manages project-specific Superset state after the Superset
metadata database and Cloud Run service already exist.

It owns:

- the DuckDB database connection
- the model-metrics datasets
- the model-evaluation charts
- the dashboard layout

It intentionally does not provision Cloud Run, Cloud SQL, IAM, secrets, or
container images. Those remain in `../main/`.

## Apply

From the Terraform container:

```bash
terraform -chdir=/workspace/terraform/superset init
terraform -chdir=/workspace/terraform/superset apply \
  -var="superset_endpoint=${SUPERSET_URL}"
```

`SUPERSET_URL` is the `superset_service_url` output from `../main/`.

## Import Existing Assets

Fresh environments do not need imports. If an existing Superset instance was
previously populated with `superset import-directory`, import the assets before
applying this root:

```bash
terraform import superset_database.xgboost_metrics <database_id>
terraform import 'superset_dataset.model_metrics["metrics"]' <dataset_id>
terraform import 'superset_dataset.model_metrics["confusion_matrix"]' <dataset_id>
terraform import 'superset_dataset.model_metrics["feature_importance"]' <dataset_id>
terraform import 'superset_dataset.model_metrics["roc_curve"]' <dataset_id>
terraform import 'superset_dataset.model_metrics["model_comparison"]' <dataset_id>
terraform import 'superset_chart.model_metrics["model_comparison"]' <chart_id>
terraform import 'superset_chart.model_metrics["confusion_matrix"]' <chart_id>
terraform import 'superset_chart.model_metrics["feature_importance"]' <chart_id>
terraform import 'superset_chart.model_metrics["roc_curve"]' <chart_id>
terraform import superset_dashboard.xgboost_model_evaluation <dashboard_id>
```

The numeric IDs are visible in the Superset UI URLs or via the Superset REST API.
