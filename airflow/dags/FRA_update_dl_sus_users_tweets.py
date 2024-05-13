from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator



import sys
sys.path
sys.path.append('scripts_py/')

import os
script_dir = os.path.dirname(__file__)
rel_path_graph = r"./scripts_py/"
scriptsPath = os.path.join(script_dir, rel_path_graph)



# from scripts_py.mongo_to_mysql import data_download
from FRA_update_users_ledger import FRA_update_user_ledger
from FRA_dl_sus_tweets import FRA_dl_sus_tweets

default_args = {
    'owner': 'kw',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='FRA_update_dl_sus_tweets',
    default_args=default_args,
    start_date=datetime(2022, 6, 27),
    schedule_interval='0 9,14,22 * * *',
    tags=["kw"],
    description='update and download tweets of sus accounts',
    catchup=False) as dag:

    update_ledger = PythonOperator(\
        task_id='FRA_update_ledger', \
        python_callable=FRA_update_user_ledger)

    sus_tweets_download = PythonOperator(\
        task_id='FRA_download-sts-tweets', \
        python_callable=FRA_dl_sus_tweets)

update_ledger >> sus_tweets_download
