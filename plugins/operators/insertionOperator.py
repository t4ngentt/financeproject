from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import os

class insertionOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 dataset,
                 *args, **kwargs):

        super(insertionOperator, self).__init__(*args, **kwargs)
        self.dataset = dataset  

        def execute(self, context):
            """ 
            Acts as the common postgres insertion point for all tasks 
            :params: datasets (dictionary) e.g. Trends, Tweets, News
            :returns: None
            :xcom: extractDataset (eg. extractTrends, extractTweets, extractNews), selectTicker
            """

            ti = kwargs['task_instance']
            ticker = ti.xcom_pull(task_ids = 'selectTicker')
            postgresHook = PostgresHook(postgres_conn_id=os.getenv('postgres_conn_id'), schema=os.getenv('schema'))
            
            for dataset in self.dataset:
                #automatically iterate to said task xcom
                data = ti.xcom_pull(task_ids = f'extract{dataset}')
                # converting the list of keys input from given dictionary into a string, readable by PostgreSQL
                
                ######### ADD data validation for dictionary

                keysString = ''
                dataKeys = data.keys()
                for key in dataKeys:
                    keysString.append(key + ', ')
                key = dataKeys[0]
                keyString = keyString[:-2]

                for i in range(len(data[f'{key}'])):
                    values = ''
                    for key in dataKeys:
                        values.append(data[f'{key}'][i] + ', ')
                    values = values[:-2]
                    postgresHook.run(f'INSERT INTO {dataset} ({keyString}) VALUES ({values})', autocommit=True)            