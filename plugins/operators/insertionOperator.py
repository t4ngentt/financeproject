from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class insertionOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 params = ""
                 *args, **kwargs):

        super(insertionOperator, self).__init__(*args, **kwargs)
        self.params = params

        def execute(self, context):
            """
            Acts as the common postgres insertion point for all tasks 
            :params: datasets (dictionary) e.g. Trends, Tweets, News
            :returns: None
            :xcom: extractDataset (eg. extractTrends, extractTweets, extractNews), selectTicker
            """

            