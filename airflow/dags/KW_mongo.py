from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
# from airflow.providers.sqlite.operators.mysql import SqliteOperator
# from airflow.providers.mysql.operators.mysql import MySqlOperator
# from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
from airflow.utils.db import provide_session
from datetime import datetime, timedelta


default_args = {
    'owner': 'kw',
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}


doc_raw = {
  'name': "Chaitanya",
  'age': 30,
  'website': "beginnersbook.com"
}

MONGO_CONN_ID = 'mongo_conn'
MONGO_COLLECTION = 'airflowtest'


def print_hello():
    return 'Hello world from first Airflow DAG!'

def insert_to_mongo():
    hook = MongoHook(MONGO_CONN_ID)
    hook.insert_one(mongo_collection = MONGO_COLLECTION, doc=doc_raw)

with DAG(
    dag_id='kw_mongo_test4',
    tags=["kw"],
    default_args=default_args,
    start_date=datetime(2022, 6, 27),
    schedule_interval="@hourly",
    description='use case of mongo operator in airflow',
    catchup=False) as dag:


    task1 = PythonOperator(task_id='hello_task', python_callable=print_hello)

    task2 = PythonOperator(task_id='mongo_insert_test', python_callable=insert_to_mongo)

    # task1 = PythonOperator(task_id='hello_task', python_callable=print_hello)

    # task2 = MongoHook(
    #     task_id="mongo_insert_test",
    #     mongo_conn_id=MONGO_CONN_ID,
    #     mongo_collection=MONGO_COLLECTION,
    #     mongo_db=MONGO_COLLECTION,
    #     default_conn_name = 'mongo_default',
    #     conn_id='mongo_conn',
    #     conn_type = 'mongo_conn',
    #     ).insert_one(
    #     mongo_collection = 'airflowtest',
    #     doc=doc_raw,
    #     )




    # task1 >> task2

        # mongo_method='replace',
        # conn_name_attr="mongo_conn",
        # conn_id='mongo_conn',

        
        # conn_type = 'mongo_conn',
        # hook_name = 'MongoDB',
        # query=insert_one,
        # dag=dag)

