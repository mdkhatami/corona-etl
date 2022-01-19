rm -rf ./dags
rm -rf ./logs
rm -rf ./plugins
rm -rf ./data

mkdir -p ./dags ./logs ./plugins ./data

cp backup/corona_ETL.py dags/corona_ETL.py
cp backup/sample_ETL.py dags/sample_ETL.py
cp backup/docker-compose.yaml docker-compose.yaml
rm -f .env
echo -e "AIRFLOW_UID=$(id -u)" > .env

#curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.3/docker-compose.yaml'