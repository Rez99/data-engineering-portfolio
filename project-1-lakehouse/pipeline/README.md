# Set-up Python
```bash
echo
echo "====== Clean-up ======"
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline/
docker compose down --volumes --remove-orphans
echo
echo "====== Initialize ======"
docker compose up -d --build
```
```python
import duckdb

con = duckdb.connect()

con.execute("""
ATTACH 'lakehouse' AS polaris (
    TYPE iceberg,
    CLIENT_ID '641bc6eb572b997a',
    CLIENT_SECRET '0f07c437b6fd86e05fbc956a3f8ccf86',
    ENDPOINT 'http://localhost:8181/api/catalog',
    ACCESS_DELEGATION_MODE 'vended_credentials'
)
""")

con.execute("""
CREATE SCHEMA IF NOT EXISTS polaris.raw
""")

con.execute("""
DROP TABLE IF EXISTS polaris.raw.posts_test
""")

con.execute("""
CREATE TABLE polaris.raw.posts_test AS
SELECT
    1 AS id,
    'hello world' AS message
""")

print(
    con.execute("""
    SELECT *
    FROM polaris.raw.posts_test
    """).fetchall()
)
```


# Set-up Apache Airflow
```bash
echo
echo "====== Clean-up ======"
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline/docker-airflow
docker compose down --volumes --remove-orphans
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline
rm -rf 'docker-airflow'


echo
echo "====== Initialize ======"
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline
mkdir -p docker-airflow
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline/docker-airflow

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.2.2/docker-compose.yaml'

mkdir -p ./dags ./logs ./plugins ./config
cp \
/Users/rezwanhoppe-islam/data-engineering-portfolio/project-1-lakehouse/pipeline/scripts/download_toy_data.py \
/Users/rezwanhoppe-islam/data-engineering-portfolio/project-1-lakehouse/pipeline/docker-airflow/dags/
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init


  
echo
echo "====== Running Airflow ======"
docker compose up -d
```

# Set-up Apache Polaris
```bash
echo
echo "====== Clean-up ======"
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline/docker-polaris
docker compose down -v
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline
rm -r docker-polaris

echo
echo "====== Prepare filesystem ======"
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline
mkdir -p docker-polaris
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline/docker-polaris
curl -o docker-compose.yml https://raw.githubusercontent.com/apache/polaris/refs/heads/main/site/content/guides/quickstart/docker-compose.yml
cat > .env.example <<'EOF'
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
cp .env.example .env
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline/docker-polaris

echo
echo "====== Docker compose ======"
docker compose up -d

echo
echo "====== Grab polaris token ======"
source .env
export POLARIS_TOKEN=$(curl -s http://polaris:8181/api/catalog/v1/oauth/tokens \
   --resolve polaris:8181:127.0.0.1 \
   --user ${ROOT_CLIENT_ID}:${ROOT_CLIENT_SECRET} \
   -d 'grant_type=client_credentials' \
   -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r .access_token)

echo
echo "====== Check catalog ======"
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
echo "Found catalog: $CATALOG"

echo
echo "====== Create principal ======"

PRINCIPAL_RESPONSE=$(
  curl -s -X POST \
    http://localhost:8181/api/management/v1/principals \
    -H "Authorization: Bearer $POLARIS_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"name":"airflow"}'
)

echo "$PRINCIPAL_RESPONSE" | jq

export AIRFLOW_CLIENT_ID=$(echo "$PRINCIPAL_RESPONSE" | jq -r '.credentials.clientId')
export AIRFLOW_CLIENT_SECRET=$(echo "$PRINCIPAL_RESPONSE" | jq -r '.credentials.clientSecret')
echo $AIRFLOW_CLIENT_ID
echo $AIRFLOW_CLIENT_SECRET

echo
echo "====== Create principal role ======"

curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X POST \
  http://localhost:8181/api/management/v1/principal-roles \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"airflow_role"}'

echo
echo "====== Create catalog role ======"

curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X POST \
  http://localhost:8181/api/management/v1/catalogs/lakehouse/catalog-roles \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"lakehouse_role"}'

echo
echo "====== Grant principal role to principal ======"

curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X PUT \
  http://localhost:8181/api/management/v1/principals/airflow/principal-roles \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"principalRole":{"name":"airflow_role"}}'

echo
echo "====== Grant catalog role to principal role ======"

curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X PUT \
  http://localhost:8181/api/management/v1/principal-roles/airflow_role/catalog-roles/lakehouse \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"lakehouse_role"}}'

echo
echo "====== Grant catalog privilege ======"

curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X PUT \
  http://localhost:8181/api/management/v1/catalogs/lakehouse/catalog-roles/lakehouse_role/grants \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}'

```