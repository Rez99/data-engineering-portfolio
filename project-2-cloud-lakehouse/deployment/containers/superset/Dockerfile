FROM apache/superset:6.1.0

USER root

RUN uv pip install --no-cache \
    duckdb \
    duckdb-engine \
    google-cloud-storage \
    psycopg2-binary

COPY superset_config.py /app/pythonpath/superset_config.py
COPY init_metrics.py /app/pythonpath/init_metrics.py
COPY assets /app/pythonpath/assets

USER superset

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

CMD ["/bin/bash", "-c", "python /app/pythonpath/init_metrics.py || echo 'Model metrics are not available yet; Superset will start without refreshed metrics.'; exec gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 1 --worker-class gthread --threads 20 --timeout 120 'superset.app:create_app()'"]
