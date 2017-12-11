#encoding=utf-8

from hbase_connection import *

def test_output():
    for movie in all_movies():
        print movie

if __name__ == "__main__":
    insert_movie_data()
    insert_cast_data()
    post_process()
    test_output()
