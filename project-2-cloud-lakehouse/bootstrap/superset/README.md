# Superset Bootstrap

This stage executes the already-provisioned Superset bootstrap Cloud Run Job.

Terraform defines the job because it is infrastructure:

```text
terraform/ -> google_cloud_run_v2_job.superset_bootstrap
```

This runner executes the job because bootstrap is a platform initialization
action:

```bash
bootstrap/superset/run.sh
```

The job only performs the minimal Superset initialization:

```text
superset db upgrade
create the initial admin user if absent
superset init
```

Project-specific Superset assets are managed separately by
`terraform-superset/`.

