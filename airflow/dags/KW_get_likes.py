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
from POL_get_likes import POL_get_likes
from GER_get_likes import GER_get_likes
from FRA_get_likes import FRA_get_likes


default_args = {
    'owner': 'kw',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='KW_get_likes',
    default_args=default_args,
    start_date=datetime(2022, 6, 27),
    schedule_interval='0 6,8,10,12,14,16,18,20,22 * * *',
    tags=["kw"],
    max_active_runs=1,
    description='get likes',
    catchup=False) as dag:

    POL = PythonOperator(\
        task_id='POL_LIKES', \
        python_callable=POL_get_likes)

    GER = PythonOperator(\
        task_id='GER_LIKES', \
        python_callable=GER_get_likes)

    FRA = PythonOperator(\
        task_id='FRA_LIKES', \
        python_callable=FRA_get_likes)

GER >> FRA >> POL
