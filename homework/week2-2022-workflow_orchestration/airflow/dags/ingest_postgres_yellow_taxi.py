import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from scripts.ingest import postgres_ingest

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
URL_TEMPLATE = (
    URL_PREFIX + "/yellow/yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv.gz"
)

DOWNLOAD_FILE_TEMPLATE = "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv.gz"

OUTPUT_FILE_TEMPLATE = (
    AIRFLOW_HOME + "/yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv"
)


TABLE_NAME_TEMPLATE = "yellow_taxi_{{ execution_date.strftime('%Y_%m') }}"

with DAG(
    "ingest_postgres_yellow_taxi_catchup",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    catchup=True,
    max_active_runs=3,
) as dag:

    download_data = BashOperator(
        task_id="dowload_data",
        bash_command=f"wget -q {URL_TEMPLATE} && gunzip -q ./{DOWNLOAD_FILE_TEMPLATE}",
    )

    ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=postgres_ingest,
        op_kwargs={
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST,
            "port": PG_PORT,
            "database": "ny_taxi",
            "table_name": TABLE_NAME_TEMPLATE,
            "csv_file_path": OUTPUT_FILE_TEMPLATE,
        },
    )

    download_data >> ingest_data
