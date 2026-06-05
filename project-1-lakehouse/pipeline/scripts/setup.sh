# Start Docker if necessary
echo 'Start Docker if necessary...'
open -a Docker
echo '✅ Docker ready'


# Clean up any existing containers and volumes
echo 'Clean up environment...'
docker compose -f ../docker/docker-airflow/docker-compose.yaml down -v > /dev/null 2>&1
rm -rf '../docker/docker-airflow'
docker compose -f ../docker/docker-polaris/docker-compose.yaml down -v > /dev/null 2>&1
rm -rf '../docker/docker-polaris'
#docker compose -f ../docker/compose_polaris_edited.yaml down -v
#docker compose -f ../docker/compose_dbt.yaml down -v
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

docker compose -f ../docker/docker-polaris/docker-compose.yaml up -d > /dev/null 2>&1
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
sed -i '' 's|  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.2.2}|  #image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.2.2}|' ../docker/docker-airflow/docker-compose.yaml && \
sed -i '' 's|  # build: .|  build: .|' ../docker/docker-airflow/docker-compose.yaml
echo -e "AIRFLOW_UID=$(id -u)" > ../docker/docker-airflow/.env
grep '^AIRFLOW_' ../docker/docker-polaris/.env >> ../docker/docker-airflow/.env
grep '^RUSTFS_' ../docker/docker-polaris/.env >> ../docker/docker-airflow/.env
docker compose -f ../docker/docker-airflow/docker-compose.yaml up airflow-init  > /dev/null 2>&1
cp dag_* ../docker/docker-airflow/dags/
docker compose -f ../docker/docker-airflow/docker-compose.yaml up -d  > /dev/null 2>&1
echo '✅ Airflow ready'