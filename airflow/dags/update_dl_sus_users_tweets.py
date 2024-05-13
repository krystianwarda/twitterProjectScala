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
from update_users_ledger import update_user_ledger
from dl_sus_tweets import dl_sus_tweets
from FRA_update_users_ledger import FRA_update_user_ledger
from FRA_dl_sus_tweets import FRA_dl_sus_tweets
from GER_update_users_ledger import GER_update_user_ledger
from GER_dl_sus_tweets import GER_dl_sus_tweets
from POL_clean_duplicates import POL_clean_duplicates
from FRA_clean_duplicates import FRA_clean_duplicates
from GER_clean_duplicates import GER_clean_duplicates

default_args = {
    'owner': 'kw',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='update_dl_sus_tweets_v2',
    default_args=default_args,
    start_date=datetime(2022, 6, 27),
    schedule_interval='0 6,8,10,12,14,16,18,20,22 * * *',
    tags=["kw"],
    max_active_runs=1,
    description='update and download tweets of sus accounts',
    catchup=False) as dag:

    POL_update_ledger = PythonOperator(\
        task_id='update_ledger', \
        python_callable=update_user_ledger)

    POL_sus_tweets_download = PythonOperator(\
        task_id='download-sts-tweets', \
        python_callable=dl_sus_tweets)
    
    # POL_clean_duplicates_all = PythonOperator(\
    #     task_id='POL-clean-duplicates', \
    #     python_callable=POL_clean_duplicates)

    GER_update_ledger = PythonOperator(\
        task_id='GER_update_ledger', \
        python_callable=GER_update_user_ledger)

    GER_sus_tweets_download = PythonOperator(\
        task_id='GER_download-sts-tweets', \
        python_callable=GER_dl_sus_tweets)

    # GER_clean_duplicates_all = PythonOperator(\
    #     task_id='GER-clean-duplicates', \
    #     python_callable=GER_clean_duplicates)
 
    FRA_update_ledger = PythonOperator(\
        task_id='FRA_update_ledger', \
        python_callable=FRA_update_user_ledger)

    FRA_sus_tweets_download = PythonOperator(\
        task_id='FRA_download-sts-tweets', \
        python_callable=FRA_dl_sus_tweets)

    # FRA_clean_duplicates_all = PythonOperator(\
    #     task_id='FRA-clean-duplicates', \
    #     python_callable=FRA_clean_duplicates)

# POL_update_ledger >> POL_sus_tweets_download >> POL_clean_duplicates_all >> GER_update_ledger >> GER_sus_tweets_download >> GER_clean_duplicates_all >> FRA_update_ledger >> FRA_sus_tweets_download >> FRA_clean_duplicates_all
POL_update_ledger >> POL_sus_tweets_download  >> GER_update_ledger >> GER_sus_tweets_download >> FRA_update_ledger >> FRA_sus_tweets_download