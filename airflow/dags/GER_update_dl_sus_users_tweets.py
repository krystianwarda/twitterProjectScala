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
from GER_update_users_ledger import GER_update_user_ledger
from GER_dl_sus_tweets import GER_dl_sus_tweets

default_args = {
    'owner': 'kw',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='GER_update_dl_sus_tweets',
    default_args=default_args,
    start_date=datetime(2022, 6, 27),
    schedule_interval='0 7,10,20 * * *',
    tags=["kw"],
    description='update and download tweets of sus accounts',
    catchup=False) as dag:

    update_ledger = PythonOperator(\
        task_id='GER_update_ledger', \
        python_callable=GER_update_user_ledger)

    sus_tweets_download = PythonOperator(\
        task_id='GER_download-sts-tweets', \
        python_callable=GER_dl_sus_tweets)

update_ledger >> sus_tweets_download
