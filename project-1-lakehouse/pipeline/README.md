# Set-up Apache Airflow

## Cleaning-up the environment
```bash
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline/docker-airflow
docker compose down --volumes --remove-orphans
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline
rm -rf 'docker-airflow'
```

## Initialize
```bash
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline
mkdir -p docker-airflow
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline/docker-airflow

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.2.2/docker-compose.yaml'

mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init
  
```
## Running Airflow
```bash
docker compose up -d
```

# Set-up Apache Polaris
```bash
cd ~/data-engineering-portfolio/project-1-lakehouse/pipeline
mkdir -p docker-polaris

curl -s https://raw.githubusercontent.com/apache/polaris/refs/heads/main/site/content/guides/quickstart/docker-compose.yml | docker compose -p polaris-quickstart -f - up -d
```