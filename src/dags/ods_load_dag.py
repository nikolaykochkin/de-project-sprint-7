from datetime import datetime

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from utils import init_environ

init_environ()

EVENTS_PATH = Variable.get("MASTER_GEO_EVENTS_PATH")
CITIES_PATH = Variable.get("CITIES_PATH")
OUTPUT_PATH = Variable.get("PRE_PROCESSED_EVENTS_PATH")


@dag(
    schedule="@daily",
    start_date=datetime(2022, 6, 1),
    end_date=datetime(2022, 6, 30),
    catchup=True
)
def ods_load_dag():
    start = EmptyOperator(task_id='start')

    events_nearest_cities = SparkSubmitOperator(
        task_id='events_nearest_cities_task',
        application='../scripts/events_nearest_cities.py',
        conn_id='yarn_spark',
        application_args=[
            "{{ ds }}",
            EVENTS_PATH,
            CITIES_PATH,
            OUTPUT_PATH
        ]
    )

    finish = EmptyOperator(task_id='finish')

    (
            start
            >> events_nearest_cities
            >> finish
    )


_ = ods_load_dag()
