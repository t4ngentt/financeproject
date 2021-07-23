from airflow import DAG
from airflow.models import dag
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.dagrun import DagRun

from plugins.helpers import sqlQueries, functions
from plugins.operators import sentimentOperator
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


fetchCount = PythonOperator(
    task_id = 'fetchCount',
    python_callable= functions.fetchCount,
    provide_context = True,
    # op_kwargs={'date': datetime.date.now()}
    dag = dag
)

selectTicker = PythonOperator(
    task_id = 'selectTicker',
    python_callable= functions.selectTicker,
    provide_context = True,
    # op_kwargs={'date': datetime.date.now()}
    dag = dag
)

populateStages = PythonOperator(
    task_id = 'populateStages',
    python_callable= functions.populateStages,
    provide_context = True,
    # op_kwargs={}
    dag = dag
)

initializeNextDag = TriggerDagRunOperator(
    task_id="initializeNextDag",
    trigger_dag_id="******************"
)

extractNews = PythonOperator(
    task_id="extractNews",
    python_callable= functions.extractNews,
    provide_context = True,
    dag = dag
)

extractTweets = PythonOperator(
    task_id="extractTweets",
    python_callable= functions.extractTweets,
    provide_context = True,
    dag = dag
)

sentimentOperatorNews = sentimentOperator(
    task_id = "sentimentNews" ,
    params ={ 'dataset': 'News' },
    provide_context = True,
    dag = dag
)

sentimentOperatorTweets = sentimentOperator(
    task_id = "sentimentTweets" ,
    params ={ 'api': 'Tweets' },
    provide_context = True,
    dag = dag
)