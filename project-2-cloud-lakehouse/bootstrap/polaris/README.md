# Polaris Bootstrap

This stage executes the already-provisioned Polaris bootstrap Cloud Run Job.

Terraform defines the job because it is infrastructure:

```text
terraform/ -> google_cloud_run_v2_job.polaris_bootstrap
```

This runner executes the job because bootstrap is a one-time platform
initialization action:

```bash
bootstrap/polaris/run.sh
```

The job runs the upstream Polaris admin tool image:

```text
apache/polaris-admin-tool:1.5.0
```

with the equivalent command:

```text
bootstrap -r POLARIS -c POLARIS,admin,<root-client-secret>
```

After this minimal bootstrap creates the realm and root credentials,
`terraform-polaris/` manages project-specific Polaris state:

```text
catalog: lakehouse
grant: TABLE_WRITE_DATA on catalog_admin
namespace: bronze
```
