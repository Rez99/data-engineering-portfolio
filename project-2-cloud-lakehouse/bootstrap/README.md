# Bootstrap

Bootstrap is intentionally minimal.

The Polaris bootstrap step only creates the initial realm and root client
credentials that Polaris needs before its management API can be used. It is
executed by `bootstrap/polaris/run.sh`. After that, project-specific Polaris
state is managed by `terraform-polaris/`.

Superset bootstrap is executed by `bootstrap/superset/run.sh`. It initializes
the Superset metadata database and initial admin account only. After that,
project-specific Superset state is managed by `terraform-superset/`.
