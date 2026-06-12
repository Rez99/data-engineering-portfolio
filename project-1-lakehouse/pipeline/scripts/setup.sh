# Start Docker if necessary
echo 'Start Docker if necessary...'
open -a Docker
echo '✅ Docker ready'


# Clean up any existing containers and volumes
echo 'Clean up environment...'
docker compose -f ../docker/docker-airflow/docker-compose.yaml down -v > /dev/null 2>&1
docker compose -f ../docker/docker-polaris/docker-compose.yaml down -v > /dev/null 2>&1
docker compose -f ../docker/docker-superset/docker-compose.yaml down -v > /dev/null 2>&1
rm -rf '../docker/docker-airflow'
rm -rf '../docker/docker-polaris'
rm -rf '../docker/docker-superset'
echo '✅ Environment clean up complete'


# Polaris
echo 'Start Polaris...'
mkdir '../docker/docker-polaris'
cp ../docker/original_compose_files/polaris-compose-original.yaml ../docker/docker-polaris/docker-compose.yaml
sed -i '' 's|apache/polaris:latest|apache/polaris:1.5.0|g' ../docker/docker-polaris/docker-compose.yaml
sed -i '' 's|bucket123|lakehouse-bucket|g' ../docker/docker-polaris/docker-compose.yaml
sed -i '' 's|"endpoint": "http://localhost:9000"|"endpoint": "http://host.docker.internal:9000"|g' ../docker/docker-polaris/docker-compose.yaml

cat > ../docker/docker-polaris/.env <<'EOF'
# Polaris bootstrap admin credentials
ROOT_CLIENT_ID=admin
ROOT_CLIENT_SECRET=change-me

# Polaris configuration
POLARIS_REALM=POLARIS
CATALOG_NAME=lakehouse

# RustFS
RUSTFS_ACCESS_KEY=polaris_root
RUSTFS_SECRET_KEY=polaris_pass
EOF

docker compose -f ../docker/docker-polaris/docker-compose.yaml up -d  --build > /dev/null 2>&1
echo '✅ Polaris ready'


# Polaris / provision entities
echo 'Start provisioning Polaris entities...'
source ../docker/docker-polaris/.env

## Grab polaris token
export POLARIS_TOKEN=$(curl -s http://polaris:8181/api/catalog/v1/oauth/tokens \
   --resolve polaris:8181:127.0.0.1 \
   --user ${ROOT_CLIENT_ID}:${ROOT_CLIENT_SECRET} \
   -d 'grant_type=client_credentials' \
   -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r .access_token)

## Check catalog created
until CATALOG=$(
  curl -s \
    -H "Authorization: Bearer $POLARIS_TOKEN" \
    http://localhost:8181/api/management/v1/catalogs \
    | jq -r '.catalogs[] | select(.name=="lakehouse") | .name'
)
[ -n "$CATALOG" ]
do
  echo "Waiting for lakehouse catalog..."
  sleep 2
done

## Create principal
PRINCIPAL_RESPONSE=$(
  curl -s -X POST \
    http://localhost:8181/api/management/v1/principals \
    -H "Authorization: Bearer $POLARIS_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"name":"airflow"}'
)
export AIRFLOW_CLIENT_ID=$(echo "$PRINCIPAL_RESPONSE" | jq -r '.credentials.clientId')
export AIRFLOW_CLIENT_SECRET=$(echo "$PRINCIPAL_RESPONSE" | jq -r '.credentials.clientSecret')

## Create principal role
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X POST \
  http://localhost:8181/api/management/v1/principal-roles \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"airflow_role"}'

## Create catalog role
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X POST \
  http://localhost:8181/api/management/v1/catalogs/lakehouse/catalog-roles \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"lakehouse_role"}'

## Grant principal role to principal
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X PUT \
  http://localhost:8181/api/management/v1/principals/airflow/principal-roles \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"principalRole":{"name":"airflow_role"}}'

## Grant catalog role to principal role
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X PUT \
  http://localhost:8181/api/management/v1/principal-roles/airflow_role/catalog-roles/lakehouse \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"lakehouse_role"}}'

## Grant catalog privilege
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X PUT \
  http://localhost:8181/api/management/v1/catalogs/lakehouse/catalog-roles/lakehouse_role/grants \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}'

cat >> ../docker/docker-polaris/.env <<EOF

# Generated during setup
AIRFLOW_CLIENT_ID=${AIRFLOW_CLIENT_ID}
AIRFLOW_CLIENT_SECRET=${AIRFLOW_CLIENT_SECRET}
EOF

echo '✅ Polaris provisioning complete'


# Airflow
echo 'Start Airflow...'
mkdir '../docker/docker-airflow'
cp ../docker/original_compose_files/airflow-compose-original.yaml ../docker/docker-airflow/docker-compose.yaml
cp ../docker/docker_files/airflow-dockerfile ../docker/docker-airflow/Dockerfile
cp -r ../docker/docker-dbt ../docker/docker-airflow/
sed -i '' 's|  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.2.2}|  #image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.2.2}|' ../docker/docker-airflow/docker-compose.yaml && \
sed -i '' 's|  # build: .|  build: .|' ../docker/docker-airflow/docker-compose.yaml
sed -i '' '/plugins:\/opt\/airflow\/plugins/a\
    - ../docker-dbt/lakehouse:/opt/airflow/lakehouse\
    - ../docker-dbt/profiles.yml:/home/airflow/.dbt/profiles.yml\
' ../docker/docker-airflow/docker-compose.yaml
echo -e "AIRFLOW_UID=$(id -u)" > ../docker/docker-airflow/.env
grep '^AIRFLOW_' ../docker/docker-polaris/.env >> ../docker/docker-airflow/.env
grep '^RUSTFS_' ../docker/docker-polaris/.env >> ../docker/docker-airflow/.env
docker compose -f ../docker/docker-airflow/docker-compose.yaml up airflow-init --build  > /dev/null 2>&1
cp dag_* ../docker/docker-airflow/dags/
docker compose -f ../docker/docker-airflow/docker-compose.yaml up -d --build > /dev/null 2>&1
echo '✅ Airflow ready'


# Wait for Airflow DAGs ro be discovered
echo "Waiting for DAG to be discovered..."
while true; do
  if docker exec docker-airflow-airflow-worker-1 \
      airflow dags list 2>/dev/null \
      | grep -q "^${DAG_ID}[[:space:]]"; then
    break
  fi

  sleep 2
done
echo "✅ DAG discovered"


# Airflow start DAGs
echo 'Start Airflow DAG...'

DAG_ID="lakehouse_0_pipeline"
RUN_ID="manual_$(date +%s)"

docker exec docker-airflow-airflow-worker-1 \
  airflow dags trigger "$DAG_ID" \
  --run-id "$RUN_ID" \
  > /dev/null 2>&1

TOKEN=$(
curl -s -X POST \
  http://localhost:8080/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username":"airflow","password":"airflow"}' \
  | jq -r '.access_token'
)

curl -N \
  -H "Authorization: Bearer ${TOKEN}" \
  "http://localhost:8080/api/v2/dags/${DAG_ID}/dagRuns/${RUN_ID}/wait?interval=30"

echo '✅ DAG runs complete'


# Superset
echo 'Start Superset...'
mkdir '../docker/docker-superset'
cp ../docker/docker_files/superset-dockerfile ../docker/docker-superset/Dockerfile
cp superset_config.py ../docker/docker-superset/superset_config.py
cp superset_init_duckdb.py ../docker/docker-superset/superset_init_duckdb.py

cat > ../docker/docker-superset/docker-compose.yaml <<'EOF'
services:
  superset-db:
    image: postgres:17
    environment:
      POSTGRES_DB: superset
      POSTGRES_USER: superset
      POSTGRES_PASSWORD: ${SUPERSET_DATABASE_PASSWORD}
    volumes:
      - superset-db-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U superset -d superset"]
      interval: 5s
      timeout: 5s
      retries: 20
    restart: unless-stopped

  superset-init:
    image: lakehouse-superset:6.1.0
    build: .
    env_file:
      - .env
    environment:
      SUPERSET_CONFIG_PATH: /app/pythonpath/superset_config.py
    volumes:
      - ./superset_config.py:/app/pythonpath/superset_config.py:ro
      - ./superset_init_duckdb.py:/app/pythonpath/superset_init_duckdb.py:ro
      - superset-home:/app/superset_home
    depends_on:
      superset-db:
        condition: service_healthy
    command:
      - /bin/bash
      - -c
      - |
        set -e
        superset db upgrade
        superset fab create-admin \
          --username "$${SUPERSET_ADMIN_USERNAME}" \
          --firstname "$${SUPERSET_ADMIN_FIRSTNAME}" \
          --lastname "$${SUPERSET_ADMIN_LASTNAME}" \
          --email "$${SUPERSET_ADMIN_EMAIL}" \
          --password "$${SUPERSET_ADMIN_PASSWORD}"
        superset init
        python /app/pythonpath/superset_init_duckdb.py
        superset set-database-uri \
          --database_name "XGBoost Model Metrics" \
          --uri "duckdb:////app/superset_home/ml_metrics.duckdb"

  superset:
    image: lakehouse-superset:6.1.0
    build: .
    env_file:
      - .env
    environment:
      SUPERSET_CONFIG_PATH: /app/pythonpath/superset_config.py
    volumes:
      - ./superset_config.py:/app/pythonpath/superset_config.py:ro
      - superset-home:/app/superset_home
    ports:
      - "8088:8088"
    depends_on:
      superset-db:
        condition: service_healthy
      superset-init:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8088/health"]
      interval: 10s
      timeout: 5s
      retries: 20
      start_period: 30s
    restart: unless-stopped

volumes:
  superset-db-data:
  superset-home:
EOF

SUPERSET_SECRET_KEY=$(openssl rand -hex 42)
SUPERSET_DATABASE_PASSWORD=$(openssl rand -hex 24)

cat > ../docker/docker-superset/.env <<EOF
SUPERSET_SECRET_KEY=${SUPERSET_SECRET_KEY}
SUPERSET_DATABASE_PASSWORD=${SUPERSET_DATABASE_PASSWORD}
SUPERSET_DATABASE_URI=postgresql+psycopg2://superset:${SUPERSET_DATABASE_PASSWORD}@superset-db:5432/superset

SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=admin
SUPERSET_ADMIN_FIRSTNAME=Superset
SUPERSET_ADMIN_LASTNAME=Admin
SUPERSET_ADMIN_EMAIL=admin@example.com

RUSTFS_ACCESS_KEY=${RUSTFS_ACCESS_KEY}
RUSTFS_SECRET_KEY=${RUSTFS_SECRET_KEY}
EOF

if ! docker compose \
  -f ../docker/docker-superset/docker-compose.yaml \
  up superset-init --build > /dev/null 2>&1
then
  docker compose \
    -f ../docker/docker-superset/docker-compose.yaml \
    logs --tail=100 superset-init superset-db
  echo 'Superset initialization failed'
  exit 1
fi

if ! docker compose \
  -f ../docker/docker-superset/docker-compose.yaml \
  up -d superset > /dev/null 2>&1
then
  docker compose \
    -f ../docker/docker-superset/docker-compose.yaml \
    logs --tail=100 superset superset-init superset-db
  echo 'Superset startup failed'
  exit 1
fi

SUPERSET_READY=false

for attempt in {1..60}
do
  if curl --fail --silent http://localhost:8088/health > /dev/null
  then
    SUPERSET_READY=true
    break
  fi

  echo "Waiting for Superset (${attempt}/60)..."
  sleep 5
done

if [ "${SUPERSET_READY}" != "true" ]
then
  docker compose \
    -f ../docker/docker-superset/docker-compose.yaml \
    logs --tail=100 superset
  echo 'Superset health check timed out'
  exit 1
fi

SUPERSET_TOKEN=$(
  curl --fail --silent \
    -X POST \
    http://localhost:8088/api/v1/security/login \
    -H 'Content-Type: application/json' \
    -d '{
      "username": "admin",
      "password": "admin",
      "provider": "db",
      "refresh": true
    }' \
    | jq -r '.access_token'
)

SUPERSET_CSRF_TOKEN=$(
  curl --fail --silent \
    --cookie-jar /tmp/superset-cookies \
    -H "Authorization: Bearer ${SUPERSET_TOKEN}" \
    http://localhost:8088/api/v1/security/csrf_token/ \
    | jq -r '.result'
)

SUPERSET_DATABASE_ID=$(
  curl --fail --silent \
    -H "Authorization: Bearer ${SUPERSET_TOKEN}" \
    'http://localhost:8088/api/v1/database/?q=(page_size:100)' \
    | jq -r '
        .result[]
        | select(.database_name == "XGBoost Model Metrics")
        | .id
      '
)

for dataset in metrics confusion_matrix feature_importance roc_curve
do
  DATASET_EXISTS=$(
    curl --fail --silent \
      -H "Authorization: Bearer ${SUPERSET_TOKEN}" \
      'http://localhost:8088/api/v1/dataset/?q=(page_size:100)' \
      | jq -r \
        --arg dataset "${dataset}" \
        '
          any(
            .result[];
            .schema == "ml" and .table_name == $dataset
          )
        '
  )

  if [ "${DATASET_EXISTS}" != "true" ]
  then
    curl --fail --silent \
      --cookie /tmp/superset-cookies \
      -X POST \
      http://localhost:8088/api/v1/dataset/ \
      -H "Authorization: Bearer ${SUPERSET_TOKEN}" \
      -H "X-CSRFToken: ${SUPERSET_CSRF_TOKEN}" \
      -H 'Content-Type: application/json' \
      -d "{
        \"database\": ${SUPERSET_DATABASE_ID},
        \"schema\": \"ml\",
        \"table_name\": \"${dataset}\"
      }" \
      > /dev/null
  fi
done

rm -f /tmp/superset-cookies

echo 'Superset ready at http://localhost:8088'
echo '   Username: admin'
echo '   Password: admin'
