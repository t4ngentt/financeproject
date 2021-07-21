from airflow import DAG
from airflow.models import dag
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.dagrun import DagRun

from helpers import sqlQueries
import logging

def start():
    """ 
    To log the start of DAG for a given ticker run
    :params: None 
    :returns: None
    """
    logging.info("Starting DAG")

def end():
    """ 
    To log the successful end of DAG for a given ticker run
    :params: None 
    :returns: None
    """
    logging.info("Ending DAG")







fetchCount = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="airflow",
    sql=sqlQueries.fetchCount
)

selectTicker = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="airflow",
    sql=sqlQueries.selectTicker,
    params = {''}
)
    
