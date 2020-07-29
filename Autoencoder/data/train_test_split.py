import os
path = os.path.join("..","data","raw")
try:
    os.mkdir(path)
except OSError as error:
    print(error)

#TOTAL_USERS= 700 # using ratings_small.csv | TODO : adapt later
FILE = "ratings_small.csv"
FILE_FULLPATH = os.path.join("..","movielensdataset",FILE)

TRAIN_FILE = os.path.join("..","data","raw","train.csv")
TEST_FILE = os.path.join("..","data","raw","test.csv")

NUM_OF_TESTRatings_per_user = 3

def get_ratings_per_user_dict():
    '''
    gets number of ratings per unique users
    :return:
    dictionary with key= userid and value= number of ratings.
    '''
    user_ratings_dict = {}
    ratings_file = open(FILE_FULLPATH)
    test_uid_counter = {}
    next(ratings_file) # Skipping csv headers
    for line in ratings_file:
        user_id = line.split(",")[0]
        if user_id in user_ratings_dict:
            user_ratings_dict[user_id]+=1
        else:
            test_uid_counter[user_id] = NUM_OF_TESTRatings_per_user
            user_ratings_dict[user_id]=1
    ratings_file.close()
    return user_ratings_dict, test_uid_counter
def split_data():
    '''
    creates train and test data  from the ratings files.
    data set is divided per user.
    :return:
    '''
    ratings_per_user,test_uid_counter = get_ratings_per_user_dict()
    '''for k,v in ratings_per_user.items():

        print("%s:%d"%(k,v))
    '''
    ratings_file = open(FILE_FULLPATH)
    next(ratings_file)
    train_fhandler = open(TRAIN_FILE,"w")
    test_fhandler = open(TEST_FILE,"w")
    for line in ratings_file:
        user_id = line.split(",")[0]
        if test_uid_counter[user_id] > 0:

            if ratings_per_user[user_id] > NUM_OF_TESTRatings_per_user:
                test_fhandler.write(line)
                test_uid_counter[user_id] = test_uid_counter[user_id] - 1
            else:
                #continue
                train_fhandler.write(line)
        else:
            train_fhandler.write(line)



if __name__ == "__main__":
    split_data();