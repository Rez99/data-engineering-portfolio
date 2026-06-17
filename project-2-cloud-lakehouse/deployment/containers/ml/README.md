# Machine Learning Job

This service ports the Project 1 ecommerce-conversion model to a standalone
runtime. It reads session-level features from Parquet, creates a deterministic
train/test split, and trains XGBoost from streamed Parquet batches using
`ExtMemQuantileDMatrix`.

The production defaults retain the Project 1 feature set, model parameters,
300 boosting rounds, and evaluation outputs:

- `xgboost_model.json`
- `metrics.parquet`
- `model_comparison.parquet`
- `confusion_matrix.parquet`
- `feature_importance.parquet`
- `roc_curve.parquet`

The Cloud Run entrypoint downloads the exported feature Parquet parts from GCS,
merges them as a streamed local Parquet file, invokes the standalone trainer,
and uploads the model and evaluation datasets under
`ml/xgboost_conversion/`.
