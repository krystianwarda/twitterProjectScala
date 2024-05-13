from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.sqlite.operators.mysql import SqliteOperator
# from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime
from airflow.hooks.mysql_hook import MySqlHook   # MySQL Hook

default_args = {
    'start_date': datetime(2022,6,19),
    'owner': 'kw',
}


import sqlalchemy
database_username = 'admin'
database_password = 
database_ip       = 
database_name     = 'twitter_database'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

dag_mysql =  DAG(dag_id="kw_user_processing6",
    default_args=default_args,
    schedule_interval="@hourly",
    description='use case of mysql operator in airflow',
    catchup=False) 

create_sql_query = """ CREATE TABLE twitter_database.testTableDAG6(empid int, empname VARCHAR(25), salary int); """

create_table = MySqlOperator(sql=create_sql_query, task_id="CreateTable", mysql_conn_id='aws_rds', dag=dag_mysql)

insert_sql_query = """ INSERT INTO twitter_database.testTableDAG6(empid, empname, salary) VALUES(1,'VAMSHI',30000),(2,'chandu',50000); """

insert_data = MySqlOperator(sql=insert_sql_query, task_id="InsertData", mysql_conn_id='aws_rds', dag=dag_mysql)