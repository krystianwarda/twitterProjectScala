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
from airflow.operators.python import BranchPythonOperator

from datetime import datetime
from airflow.utils.db import provide_session
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.ec2 import EC2StartInstanceOperator, EC2StopInstanceOperator
import time

import boto3
from botocore.config import Config

AWS_CONN_ID =
AWS_INSTANCE_ID=


default_args = {
    'owner': 'kw',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def stop_ec2():
    ec2 = EC2Hook(aws_conn_id=AWS_CONN_ID,
    api_type="client_type",
    ).get_conn()
    ec2.stop_instances(InstanceIds=[AWS_INSTANCE_ID])


def start_ec2():
    ec2 = EC2Hook(aws_conn_id=AWS_CONN_ID,
    api_type="client_type",
    ).get_conn()
    ec2.start_instances(InstanceIds=[AWS_INSTANCE_ID])


with DAG(
    dag_id='kw_force_restart_ec2_2',
    default_args=default_args,
    start_date=datetime(2022, 6, 27),
    schedule_interval='@once',
    tags=["kw"],
    description='force restart ec2 instance',
    catchup=False) as dag:

    task1 = PythonOperator(task_id='stop_ec2', python_callable=stop_ec2)
    task2 = PythonOperator(task_id="delay_python_task2", python_callable=lambda: time.sleep(60))
    task3 = PythonOperator(task_id='start_ec2', python_callable=start_ec2)

task1 >> task2 >> task3