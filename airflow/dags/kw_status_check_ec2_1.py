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

AWS_CONN_ID = 'ec2Kafka'
AWS_INSTANCE_ID='i-07fdb1e709cadd23f'


default_args = {
    'owner': 'kw',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    return 'Hello world from first Airflow DAG!'

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


def check_status():
    ec2 = EC2Hook(aws_conn_id=AWS_CONN_ID,
    api_type="client_type",
    ).get_conn()
    statusLog = ec2.describe_instance_status(InstanceIds=[AWS_INSTANCE_ID])
    try:
        state = statusLog['InstanceStatuses'][0]['InstanceState']['Name']
        status = statusLog['InstanceStatuses'][0]['InstanceStatus']['Details'][0]['Status']

        if (state == 'running') & (status == 'passed'):
            return 'hello_task'
        if (state == 'running') & (status == 'failed'):
            return ['stop_ec2', 'delay_python_task2', 'start_ec2']
    except:
        return ['stop_ec2', 'delay_python_task2', 'start_ec2']

with DAG(
    dag_id='kw_status_check_ec2_1',
    default_args=default_args,
    start_date=datetime(2022, 6, 27),
    schedule_interval=timedelta(minutes=10),
    tags=["kw"],
    description='restart ec2 instance',
    catchup=False) as dag:


    # statusLog = PythonOperator(task_id='status_ec2', python_callable=status_ec2)
    checks_status = BranchPythonOperator(task_id='check_status', python_callable=check_status)
    taskA = PythonOperator(task_id='hello_task', python_callable=print_hello)
    task1 = PythonOperator(task_id='stop_ec2', python_callable=stop_ec2)
    task2 = PythonOperator(task_id="delay_python_task2", python_callable=lambda: time.sleep(60))
    task3 = PythonOperator(task_id='start_ec2', python_callable=start_ec2)

checks_status >> [taskA, task1] 
task1 >> task2 >> task3



