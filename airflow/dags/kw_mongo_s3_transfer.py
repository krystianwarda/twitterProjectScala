from airflow import DAG
from airflow.providers.amazon.aws.transfers.mongo_to_s3 import MongoToS3Operator
from datetime import datetime, timedelta
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
# import pymongo

default_args = {
    'owner': 'kw',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


today = datetime.today()
filename = str(today)[:19].replace(" ", "_")

# exec_time = datetime.today() - timedelta(hours=2, minutes=0).timestamp()

MONGO_CONN_ID = 'mongo_conn'
MONGO_DATABASE = 'twitterDB'
MONGO_COLLECTION = 'tweets'

S3_BUCKET = 'tweetskw'
S3_KEY = str('tweets/') + filename


def delete_collection():
    hook = MongoHook(MONGO_CONN_ID)
    hook.delete_many(
        mongo_collection = MONGO_COLLECTION,\
        filter_doc={})

# 'created_at': 'Tue Jul 19 13:32:41 +0000 2022', 

with DAG(
    dag_id='kw_mong_s3_transfer',
    default_args=default_args,
    start_date=datetime(2022, 6, 27),
    schedule_interval='@daily',
    tags=["kw"],
    description='transfer data from mongo to s3',
    catchup=False) as dag:


    move_collection_to_S3 = MongoToS3Operator(
        task_id="kw_mongo_to_s3_transfer",
        # Mongo query by matching values
        # Here returns all documents which have "OK" as value for the key "status"
        mongo_conn_id = MONGO_CONN_ID,
        mongo_db= MONGO_DATABASE,
        mongo_collection = MONGO_COLLECTION,
        mongo_query={},
        aws_conn_id = 'my_conn_S3',
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY + str(".gz"), 
        replace=True,
        compression ='gzip' 
    )

    delete_collection = PythonOperator(\
        task_id='mongo_delete_collection', \
        python_callable=delete_collection)

move_collection_to_S3 >> delete_collection


