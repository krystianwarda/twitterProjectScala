from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator

import pymongo
import os
import sys
sys.path
sys.path.append('scripts_py/')

script_dir = os.path.dirname(__file__)
rel_path_graph = r"./scripts_py/"
scriptsPath = os.path.join(script_dir, rel_path_graph)


# from scripts_py.mongo_to_mysql import data_download
from mongo_to_mysql import data_download

default_args = {
    'owner': 'kw',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='kw_mongo_mysql_transfer',
    default_args=default_args,
    start_date=datetime(2022, 6, 27),
    schedule_interval='@hourly',
    tags=["kw"],
    description='transfer data from mongo to mysql',
    catchup=False) as dag:

    data_download_script = PythonOperator(\
        task_id='download-data', \
        python_callable=data_download)


