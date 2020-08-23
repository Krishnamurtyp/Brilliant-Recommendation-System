import en_core_web_sm
from collections import Counter
import pandas as pd
import os
import sqlalchemy

movies = pd.read_csv("data/movies.csv", header=0)
DATABASE_URL = os.environ['JAWSDB_URL']
engine = sqlalchemy.create_engine(DATABASE_URL)
connection = engine.connect()


def promote():
    # preprocessing
    tweet_df = pd.read_sql('SELECT * FROM qdaig9augsz5or1c.tweets', connection)
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

    return recommendation[['id', 'title']]
