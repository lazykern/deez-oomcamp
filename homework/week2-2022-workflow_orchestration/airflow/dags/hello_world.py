from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "hello_world",
    default_args={
        "depends_on_past": False,
        "email": ["phusitsom@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Hello world DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    show_date = BashOperator(task_id="show_date", bash_command="date")

    sleep = BashOperator(
        task_id="sleep", bash_command="sleep 5", depends_on_past=False, retries=3
    )

    hello = BashOperator(task_id="hello", bash_command="echo 'Hello World!'")

    show_date >> sleep >> hello
