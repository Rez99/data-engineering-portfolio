output "dashboard_url" {
  description = "Resolved Superset URL for the XGBoost model evaluation dashboard."
  value       = superset_dashboard.xgboost_model_evaluation.url
}

output "dashboard_id" {
  description = "Superset dashboard identifier managed by this Terraform root."
  value       = superset_dashboard.xgboost_model_evaluation.id
}

