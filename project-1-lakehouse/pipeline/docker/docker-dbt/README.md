# Initial set-up
```bash
docker exec -it docker-dbt-dbt-1 bash
```

```bash
cd /app
dbt init lakehouse
```
### profiles.yml
```yaml
lakehouse:
  target: dev

  outputs:
    dev:
      type: duckdb
      path: lakehouse.duckdb

      extensions:
        - iceberg
        - httpfs

      secrets:
        - type: s3
          provider: config
          name: rustfs_secret
          key_id: polaris_root
          secret: polaris_pass
          endpoint: host.docker.internal:9000
          url_style: path
          use_ssl: false

      attach:
        - path: lakehouse
          type: iceberg
          alias: polaris
          endpoint: http://host.docker.internal:8181/api/catalog
          client_id: "{{ env_var('AIRFLOW_CLIENT_ID') }}"
          client_secret: "{{ env_var('AIRFLOW_CLIENT_SECRET') }}"
```

```bash
cp ~/.dbt/profiles.yml ../docker/docker-dbt/profiles.yml
```

```bash
docker exec -it docker-dbt-dbt-1 bash
```
```bash
cd /app
dbt run --select test_polaris_write.sql
```