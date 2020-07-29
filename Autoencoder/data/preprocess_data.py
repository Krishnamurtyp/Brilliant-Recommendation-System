import pandas as pd
import numpy as np
import gc
import os


ROOT_DIR = os.path.abspath(os.path.join( '..','data', 'raw'))

def moviesid_to_index(movies_lst):
    '''
    maps movies id to a numbers from 0 ...
    :param movies_lst:
    :return:
    '''
    movies_set = set(movies_lst)
    id_to_idx = {}
    counter = 0
    for x in movies_set:
        id_to_idx[x] = counter
        counter += 1
    return id_to_idx

def convert(data, num_users, num_movies,movie_id_to_index):
    ''' Making a User-Movie-Matrix'''

    new_data = []

    for id_user in range(1, num_users + 1):

        id_movie = data[:, 1][data[:, 0] == id_user]
        id_rating = data[:, 2][data[:, 0] == id_user]
        ratings = np.zeros(num_movies, dtype=np.uint32)
        for m in range(0,len(id_movie)):
            try:
                m_id = id_movie[m]
                idx =movie_id_to_index[m_id]

                ratings[idx] = id_rating[m]
            except:
                print(id_movie[m],m,movie_id_to_index[id_movie[m]])


        #ratings[id_movie - 1] = id_rating
        if sum(ratings) == 0:
            continue
        new_data.append(ratings)

        del id_movie
        del id_rating
        del ratings

    return new_data


def get_dataset_1M():
    ''' For each train.csv and test.csv making a User-Movie-Matrix'''

    gc.enable()

    training_set = pd.read_csv(ROOT_DIR + '/train.csv', sep=',', header=0, engine='python',
                               encoding='latin-1')
    training_set = np.array(training_set, dtype=np.uint32)

    test_set = pd.read_csv(ROOT_DIR + '/test.csv', sep=',', header=0, engine='python', encoding='latin-1')
    test_set = np.array(test_set, dtype=np.uint32)

    num_users = len(set(training_set[:, 0]))
    num_movies = len(set(np.concatenate([training_set[:, 1], test_set[:, 1]])))
    #num_users = int(max(max(training_set[:, 0]), max(test_set[:, 0])))
    #num_movies = int(max(max(training_set[:, 1]), max(test_set[:, 1])))
    movie_id_to_index = moviesid_to_index(np.concatenate([training_set[:, 1], test_set[:, 1]]))
    print(num_users)
    print(num_movies)
    training_set = convert(training_set, num_users, num_movies,movie_id_to_index)
    test_set = convert(test_set, num_users, num_movies,movie_id_to_index)

    return training_set, test_set


def _get_dataset():
    return get_dataset_1M()