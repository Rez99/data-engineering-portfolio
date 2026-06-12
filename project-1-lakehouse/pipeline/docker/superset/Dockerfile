FROM apache/superset:6.1.0

USER root

RUN uv pip install --no-cache \
    duckdb \
    duckdb-engine \
    psycopg2-binary

USER superset
