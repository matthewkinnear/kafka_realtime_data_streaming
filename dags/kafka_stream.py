import requests
from datetime import datetime, timezone, timedelta
import logging
import json
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    "owner": "matthew_kinnear",
    "start_date": datetime(2024, 3, 27, 17, 30, tzinfo=timezone.utc)
}

logger = logging.getLogger()


def fetch_user_data():
    res = requests.get("https://randomuser.me/api")
    if res.status_code != 200:
        logger.error("Error in response from randomuser.me: {}".format(res))
        return None
    return res.json().get("results", "")[0]


def format_user_data(user_data, formatted_user_data={}):
    formatted_user_data["first_name"] = user_data.get("first_name")
    formatted_user_data["last_name"] = user_data.get("last_name")
    formatted_user_data["gender"] = user_data.get("gender")
    formatted_user_data["email"] = user_data.get("email")
    formatted_user_data["username"] = user_data.get("login")["username"]
    return formatted_user_data


def stream_data(formatted_user_data):
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    current_time = time.time()

    while True:
        if time.time() > current_time + 60:
            break
        try:
            producer.send("users_created", json.dumps(formatted_user_data).encode("utf-8"))
        except Exception as e:
            logger.error(f"an error occurred: {e}")
            continue


def execute_tasks():
    user_data = fetch_user_data()
    formatted_user_data = format_user_data(user_data)
    stream_data(formatted_user_data)


with DAG("user_automation",
         default_args=default_args,
         schedule="@daily",
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id="streaming_data_from_api",
        python_callable=execute_tasks
    )

    execute_tasks()
