import pandas as pd
import streamlit as st
import numpy as np
import urllib
import requests
import scrapy
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from twisted.internet import reactor
from PIL import Image
from requests.exceptions import MissingSchema
from best_rated_promotion import promote
from io import BytesIO

list_of_genres = ['animation', 'western', 'fantasy', 'thriller', 'drama', 'history', 'crime', 'comedy', 'tv movie',
                  'documentary', 'mystery', 'adventure', 'family', 'romance', 'action', 'horror', 'war', 'music',
                  'science fiction', 'foreign']

movies = pd.read_csv('movies.csv')
movies.drop_duplicates(inplace=True)
df_predict = pd.read_csv('TFIDF.csv')
ratings = pd.read_csv('ratings_small.csv')
user_profile = pd.read_csv('user_profile.csv')
TFIDF = pd.read_csv('idf.csv').set_index('movieId')
movie_profile = pd.read_csv('movie_profile.csv')


def recommender(user_no):
    # user predicted rating to all films
    user_predicted_rating = df_predict[['movieId', df_predict.columns[user_no]]]
    # combine film rating and film detail
    user_rating_film = pd.merge(user_predicted_rating, movies, left_on='movieId', right_on='id')
    # films already watched by user
    already_watched = ratings[ratings['userId'].isin([user_no])]['movieId']
    # recommendation without films being watched by user
    all_rec = user_rating_film[~user_rating_film.index.isin(already_watched)]
    return all_rec.sort_values(by=str(user_no), ascending=False, axis=0).iloc[0:10][['movieId', 'title']]


def display():
    st.write("""Recommended films for you:""")
    for i in range(len(films)):
        if posters[i] != 404:
            img = Image.open(BytesIO(posters[i].content))
        else:
            img = Image.open('404NF.png')

        film_name = films[i]
        film_ID = movies.loc[movies['title'] == film_name]['id'].values[0]
        try:
            film_link = 'https://www.themoviedb.org/movie/' + str(film_ID)
        except:
            film_link = 'https://www.themoviedb.org/movie/'

        st.write("""#### **[{}]({})**""".format(film_name, film_link))
        try:
            st.image(img, use_column_width=True)
        except:
            st.image(img.convert('RGB'))
        st.write()


class Spider(scrapy.Spider):
    def __init__(self, codes):
        self.name = 'rec'
        self.allowed_domains = ['themoviedb.org']
        self.start_urls = ['https://www.themoviedb.org/movie/']
        self.posters = []
        self.codes = codes

    def start_requests(self):
        for code in self.codes:
            scrapy.Request(url=self.start_urls[0] + str(int(code)),
                           callback=self.parse, dont_filter=True)

    def parse(self, response):
        try:
            link = response.xpath('//div[@class="image_content backdrop"]/img/@data-src').extract_first(default='')
            poster = requests.get(link)
            self.posters.append(poster)
        except IndexError:
            print('Movie\'s title is not found in the database')
        except urllib.error.HTTPError as exception:
            print('Movie\'s URL is broken!')
            poster = 404
            self.posters.append(poster)
        except MissingSchema as exception:
            print('Movie\'s poster is not found!')
            poster = 404
            self.posters.append(poster)


if __name__ == '__main__':
    # streamlit app design
    st.header('Welcome to MovieLens Recommendation System!')
    st.sidebar.header('Please enter your User ID:')
    id_ = st.sidebar.number_input('Your ID')
    st.sidebar.subheader('New User Only!')
    default = st.sidebar.radio("Do you want our default recommendation?", ('Yes', 'No'))
    options = st.sidebar.multiselect('Your Choice of Genres', list_of_genres)
    button = st.sidebar.button('Confirm')

    films, posters = [], []
    if button:
        # existed users
        if int(id_) in ratings.userId.unique():
            recommended_movies = recommender(int(id_))
            films = recommended_movies['title'].values.tolist()
            runner = CrawlerRunner()
            runner.crawl(Spider, codes=recommended_movies['movieId'])
            d = runner.join()
            d.addBoth(lambda _: reactor.stop())
            reactor.run()
            posters = runner.spiders.posters
            display()

        # newcomer
        else:
            # customized choice
            if default == 'No':
                rates = dict()
                for option in list_of_genres:
                    if option in options:
                        rates[option] = [user_profile.mean(axis=0)[option]]
                    else:
                        rates[option] = [0]

                preference = pd.DataFrame(rates, index=[int(id_)]).T.sort_index(axis=0)
                test_list = np.dot(TFIDF, preference)
                movie_ranks = pd.DataFrame(data=test_list, index=movie_profile['movieId'].unique(), columns=[int(id_)])
                recommended_movies = pd.merge(movie_ranks, movies, left_on=movie_ranks.index, right_on='id') \
                                         .sort_values(by=int(id_), ascending=False, axis=0).iloc[0:10][['id', 'title']]

            # default recommendation
            elif default == 'Yes':
                recommended_movies = promote()

            films = recommended_movies['title'].values.tolist()
            runner = CrawlerRunner()
            runner.crawl(Spider, codes=recommended_movies['id'])
            d = runner.join()
            d.addBoth(lambda _: reactor.stop())
            reactor.run()
            posters = runner.spiders.posters
            display()
