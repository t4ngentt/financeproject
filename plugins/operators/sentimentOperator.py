from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import nltk
from nltk.sentiment.vader import  SentimentIntensityAnalyzer

class sentimentOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 params = ""
                 *args, **kwargs):

        super(sentimentOperator, self).__init__(*args, **kwargs)
        self.params = params

        def execute(self, context):
            """
            Applies sentiment analysis for the given dataset 
            :params: dataset (string)
            :returns: sentiments (dictionary)
            :xcom: extractDataset (eg. extractTweet, extractNews)
            """
            sentiments = {}
            ti = kwargs['task_instance']
            dataset = kwargs["params"]["dataset"]
            sentiments[dataset] = ti.xcom_pull(task_ids = 'extract' + dataset)
            sentiments['sentiment'] = []

            logging.info(f'sentimentOperator: execution started for {dataset}')

            nltk.download('vader_lexicon')
            vader = SentimentIntensityAnalyzer()

            for i in range(len(sentiments[dataset])):
                sentiments['sentiment'][i] = vader.polarity_scores(i)['compound']
            
            logging.info(f'Sentiments for the extracted {dataset} are: \n{sentiments}')

            return sentiments