import requests
import json
from datetime import datetime, timezone
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

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
    location = user_data.get("location")

    formatted_user_data["first_name"] = user_data.get("first_name")
    formatted_user_data["last_name"] = user_data.get("last_name")
    formatted_user_data["gender"] = user_data.get("gender")
    formatted_user_data["email"] = user_data.get("email")
    formatted_user_data["username"] = user_data.get("login")['username']


def stream_data():
    res = requests.get("https://randomuser.me/api")
    if res.status_code != 200:
        logger.error("Error in response from randomuser.me: {}".format(res))
        return None
    res = res.json().get("results", "")[0]
    return json.dumps(res, indent=3)


with DAG("user_automation",
         default_args=default_args,
         schedule="@daily",
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id="streaming_data_from_api",
        python_callable=stream_data
    )

user_data = fetch_user_data()
