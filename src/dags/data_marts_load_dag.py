from datetime import datetime

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from utils import init_environ

init_environ()

EVENTS_PATH = Variable.get("PRE_PROCESSED_EVENTS_PATH")
CITIES_PATH = Variable.get("CITIES_PATH")
CONSECUTIVE_DAYS_BOUND = Variable.get("CONSECUTIVE_DAYS_BOUND")
USERS_LOCATIONS_OUTPUT_PATH = Variable.get("USERS_LOCATIONS_OUTPUT_PATH")
GEO_ZONES_STATS_OUTPUT_PATH = Variable.get("GEO_ZONES_STATS_OUTPUT_PATH")
CROSS_USERS_OUTPUT_PATH = Variable.get("CROSS_USERS_OUTPUT_PATH")
NEAREST_USERS_OUTPUT_PATH = Variable.get("NEAREST_USERS_OUTPUT_PATH")
FRIENDS_RECOMMENDATION_OUTPUT_PATH = Variable.get("FRIENDS_RECOMMENDATION_OUTPUT_PATH")


@dag(schedule_interval=None, start_date=datetime.now())
def data_marts_load_dag():
    start = EmptyOperator(task_id='start')

    users_locations = SparkSubmitOperator(
        task_id='users_locations_task',
        application='../scripts/users_locations.py',
        conn_id='yarn_spark',
        application_args=[
            EVENTS_PATH,
            CITIES_PATH,
            CONSECUTIVE_DAYS_BOUND,
            USERS_LOCATIONS_OUTPUT_PATH
        ]
    )

    geo_zones_stats = SparkSubmitOperator(
        task_id='geo_zones_stats_task',
        application='../scripts/geo_zones_stats.py',
        conn_id='yarn_spark',
        application_args=[
            EVENTS_PATH,
            GEO_ZONES_STATS_OUTPUT_PATH
        ]
    )

    cross_users_by_last_message = SparkSubmitOperator(
        task_id='cross_users_by_last_message_task',
        application='../scripts/cross_users_by_last_message.py',
        conn_id='yarn_spark',
        application_args=[
            EVENTS_PATH,
            CROSS_USERS_OUTPUT_PATH
        ]
    )

    nearest_users_from_cross_users = SparkSubmitOperator(
        task_id='nearest_users_from_cross_users_task',
        application='../scripts/nearest_users_from_cross_users.py',
        conn_id='yarn_spark',
        application_args=[
            CROSS_USERS_OUTPUT_PATH,
            CITIES_PATH,
            NEAREST_USERS_OUTPUT_PATH
        ]
    )

    friends_recommendation = SparkSubmitOperator(
        task_id='friends_recommendation_task',
        application='../scripts/friends_recommendation.py',
        conn_id='yarn_spark',
        application_args=[
            NEAREST_USERS_OUTPUT_PATH,
            EVENTS_PATH,
            FRIENDS_RECOMMENDATION_OUTPUT_PATH
        ]
    )

    finish = EmptyOperator(task_id='finish')

    (
            start
            >> users_locations
            >> geo_zones_stats
            >> cross_users_by_last_message
            >> nearest_users_from_cross_users
            >> friends_recommendation
            >> finish
    )


_ = data_marts_load_dag()
