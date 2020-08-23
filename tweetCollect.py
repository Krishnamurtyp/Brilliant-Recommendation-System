from tweepy import OAuthHandler
from apscheduler.schedulers.blocking import BlockingScheduler
import sqlalchemy
from collections import Counter
import en_core_web_sm
import tweepy
import re
import pandas as pd

# authorization tokens
movies = pd.read_csv("data/movies.csv", header=0)
consumer_key = "UK8cLFsq19FVMI50jbaRaHIpr"
consumer_secret = "v7czLcFmKFWkMtGoN8lOc6CmsIgZv0DaEr4KIHAGFHi5zB1TS5"
access_key = "1171257510889775106-qV1ZPtqfl1SpGC0Zcf5m8WUyqEs2Vx"
access_secret = "PJmS1lJ55jqeVtcVTJmVjWXO4rvQ8fUO4rouEPjyPRpUe"
DATABASE_URL = 'mysql+pymysql://x6sqgn4tykzeqy5m:ubnd2hkpfg91y65b@nwhazdrp7hdpd4a4.cbetxkdyhwsb.us-east-1.rds.amazonaws.com:3306/qdaig9augsz5or1c'

engine = sqlalchemy.create_engine(DATABASE_URL)
connection = engine.connect()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

# The Twitter user who we want to get tweets from
# These are the most popular accounts on twitter which focus on movies
names = ["flavorpill", "alisonwillmore", "akstanwyck", "erikdavis", "eug", "karinalongworth", "melsil", "NikkiFinke",
         "slashfilm", "petertravers", "ebertchicago"]
# Number of tweets to pull
tweetCount = 200
tweets = []


def scrape():
    # Calling the user_timeline function with our parameters
    for name in names:
        results = api.user_timeline(id=name, count=tweetCount)
        for tweet in results:
            # foreach through all tweets pulled
            # print(tweet)
            tweets.append(tweet.text)

    tweet_df = pd.DataFrame({'tweets': tweets})
    tweet_df['tweets'] = tweet_df['tweets'].apply(lambda x: " ".join(re.split(r'[\n\t]+', x)))
    # preprocessing
    nlp = en_core_web_sm.load()
    tweet_article = nlp('|'.join(tweet_df.tweets))
    # make sure the entities we need are persons
    items = [x.text for x in tweet_article.ents if x.label_ == 'PERSON']
    # exclude the obvious misclassified entities
    items = [celebrity[0] for celebrity in Counter(items).most_common(20) if
             'http' not in celebrity[0] and '@' not in celebrity[0]
             and '#' not in celebrity[0]]

    dummy_movies = movies
    # extract the movies if the director list contains any persons in the item list
    dummy_movies.director = dummy_movies.director.apply(lambda x: x.split(", ") if pd.isnull(x) == False else [])
    # extract the movies if the cast list contains any persons in the item list
    dummy_movies.cast = dummy_movies.cast.apply(lambda x: x.split(", ") if pd.isnull(x) == False else [])
    participant_list = dummy_movies.cast.tolist()
    participant_list.extend(dummy_movies.director.tolist())
    recommendation = movies[pd.DataFrame(participant_list).isin(items).any(1)]

    if recommendation.shape[0] < 10:
        # add some popular movies if there are fewer than 10 movies recommended
        rank = 10 - recommendation.shape[0]
        top = movies.sort_values('num_votes', ascending=False)[:1500].index
        best = movies[movies.index.isin(top)]['average_vote'].sort_values(0, ascending=False)[:rank].index
        extra = movies[movies.index.isin(best)]
        recommendation = pd.concat([recommendation, extra], ignore_index=True)
    elif recommendation.shape[0] > 10:
        # only takes the top 10 recommendation based on the voting rates
        best = recommendation['average_vote'].sort_values(0, ascending=False)[:10].index
        recommendation = recommendation[recommendation.index.isin(best)]

    insertDataFrame('recommendation', recommendation[['id', 'title']])


def insertDataFrame(tableName, df):
    try:
        df.to_sql(tableName, connection, if_exists='replace', index=True)
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("Table %s created successfully." % tableName)


def job():
    # create BlockingScheduler
    scheduler = BlockingScheduler()
    # add the task and set up its interval
    scheduler.add_job(scrape, 'interval', minutes=20, id='collecting_newest_tweets')
    scheduler.start()


if __name__ == '__main__':
    job()