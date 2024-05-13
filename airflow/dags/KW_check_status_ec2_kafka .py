from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
# from airflow.providers.sqlite.operators.mysql import SqliteOperator
# from airflow.providers.mysql.operators.mysql import MySqlOperator
# from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
from airflow.utils.db import provide_session
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.ec2 import EC2StartInstanceOperator, EC2StopInstanceOperator


import boto3
from botocore.config import Config

AWS_CONN_ID =
AWS_INSTANCE_ID=


default_args = {
    'owner': 'kw',
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}


def status_ec2():
    ec2 = EC2Hook(aws_conn_id=AWS_CONN_ID,
    api_type="client_type",
    ).get_conn()
    state = ec2.describe_instance_status(InstanceIds=[AWS_INSTANCE_ID])
    print(state)


with DAG(
    dag_id='kw_ec2_check_status_kafka2',
    default_args=default_args,
    tags=["kw"],
    start_date=datetime(2022, 6, 27),
    schedule_interval="@hourly",
    description='restart ec2 instance',
    catchup=False) as dag:


    task1 = PythonOperator(task_id='status_ec2', python_callable=status_ec2)


































# c = Connection(
#     conn_id='mongo_conn',
#     conn_type='mongo',
#     login='kwadmin',
#     password='mongopassword',
#     host='cluster0.r8sard3.mongodb.net',
#     schema='twitterDB',
# )
# conn = Connection.get_connection_from_secrets("ec2Kafka")

# default_args = {
#     'owner': 'kw',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=15),
# }


# dag = 
    # conn_name_attr="mongo_conn",
    # conn_id='mongo_conn',
    # default_conn_name = 'mongo_default',
    # hook_name='MongoDB',
    # task_id="mongo_sensor")

# dag_mongo =  DAG(dag_id="kw_mongo_test",
#     default_args=default_args,
#     schedule_interval="@hourly",
#     description='use case of mongo operator in airflow',
#     catchup=False) 

# client = boto3.client('ec2', region_name='eu-central-1')
# AWS_CONN_ID = 'ec2Kafka'
# AWS_INSTANCE_ID='i-07fdb1e709cadd23f'


# def print_hello():
#     return 'Hello world from first Airflow DAG!'

# def stop_ec2():
#     # ec2 = EC2Hook(api_type="client_type")
#     ec2 = EC2Hook(aws_conn_id=AWS_CONN_ID, api_type="client_type")
#     # , region_name='eu-central-1'
#     # instance = hook.get_instance('kw_kafka')
#     ec2.stop_instances(instance_ids=[AWS_INSTANCE_ID])

# with DAG(
#     dag_id='kw_ec2_restart_7',
#     default_args=default_args,
#     start_date=datetime(2022, 6, 27),
#     schedule_interval="@hourly",
#     description='restart ec2 instance',
#     catchup=False) as dag:


#     task1 = PythonOperator(task_id='hello_task', python_callable=print_hello)

#     task2 = PythonOperator(task_id='stop_ec2', python_callable=stop_ec2)

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

