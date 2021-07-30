from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
from dotenv import load_dotenv

from GoogleNews import GoogleNews
import tweepy

import pytrends
from pytrends.request import TrendReq

def fetchCount(**kwargs):
    """
    returns the number of existing tickers in the main table
    :params: None
    :returns: count (int)
    """
    postgresHook = PostgresHook(postgres_conn_id=os.getenv('postgres_conn_id'), schema=os.getenv('schema'))

    count = postgresHook.get_first("SELECT COUNT(*) FROM stages")
    count = count[0]

    return count

def selectTicker(**kwargs):
    """
    returns the next ticker to be performed operations upon
    :params: None
    :returns: ticker (str)
    :xcom: fetchCount
    """
    postgresHook = PostgresHook(postgres_conn_id=os.getenv('postgres_conn_id'), schema=os.getenv('schema'))

    ti = kwargs['task_instance']
    count = ti.xcom_pull(task_ids='fetchCount')

    ticker = postgresHook.get_first(f"SELECT ticker FROM tickerslist WHERE ID = {count+1}")
    ticker = ticker[0]

    return ticker

def populateStages(**kwargs):
    """
    pushes the required ticker into workflow 
    :params: None
    :returns: None
    :xcom: selectTicker
    """
    postgresHook = PostgresHook(postgres_conn_id=os.getenv('postgres_conn_id'), schema=os.getenv('schema'))

    ti = kwargs['task_instance']
    ticker = ti.xcom_pull(task_ids='selectTicker')

    postgresHook.run(f"INSERT INTO stages (ticker) VALUES ('{ticker}')", autocommit = True)

#---------------------------------------------------------------------------------------------------------------------------------

def extractTweets(**kwargs):
    """
    extracts tweets for the given stock ticker
    :params: None
    :returns: tweets (array of strings)
    :xcom: selectTicker
    """

    load_dotenv()  

    auth = tweepy.OAuthHandler(os.getenv('consumerKey'),os.getenv('consumerSecret'))
    auth.set_access_token(os.getenv('accessToken'), os.getenv('accessTokenSecret'))
    api = tweepy.API(auth)  

    ti = kwargs['task_instance']
    ticker = ti.xcom_pull(task_ids='selectTicker')

    ticker = ticker[:ticker.find(".")]
    public_tweets=api.search(ticker)

    tweets = []
    for tweet in public_tweets:
        tweets.append(tweet.text)
    
    return tweets

def extractNews(**kwargs):
    """
    extracts news for the given stock ticker (source: Google News)
    :params: None
    :returns: news (array of strings)
    :xcom: selectTicker
    """
    
    ti = kwargs['task_instance']
    ticker = ti.xcom_pull(task_ids='select_ticker')
    
    postgresHook = PostgresHook(postgres_conn_id=os.getenv('postgres_conn_id'), schema=os.getenv('schema'))

    companyName = postgresHook.get_first("SELECT companyName from tickers1 WHERE keyword = '" + ticker + "'")
    companyName = companyName[0]

    googleNews = GoogleNews()
    googleNews.set_lang('en')
    googleNews.get_news(companyName)

    news = googleNews.get_texts()

    return news

def extractTrends(**kwargs):
    """
    Extracts google trends data for a given comany name
    :params: None
    :returns: None
    :xcom: selectTicker
    """
    postgresHook = PostgresHook(postgres_conn_id=os.getenv('postgres_conn_id'), schema=os.getenv('schema'))
    regionsJson = postgresHook.get_records("SELECT ")

    ti = kwargs['task_instance']
    ticker = ti.xcom_pull(task_ids='select_ticker')
    tickerArray = [ticker]
    
    trendsData = {}
    trendsData['regions'] = ['IN','US','GB','AU','JP']
    trendsData['numberOfSearches'] = []
    
    for region in trendsData['regions']:
        pytrends = TrendReq(hl='en-IN', tz=330, geo='IN', )
        pytrends.build_payload(tickerArray, cat = 0 , timeframe = 'now 1-d' , geo = region , gprop='')
        data = pytrends.interest_over_time()
        mean = data.mean()
        mean = mean.tolist()
        try:
            trendsData['numberOfSearches'].append(mean[0])
        except:
            trendsData['numberOfSearches'].append(0.0)

    return trendsData