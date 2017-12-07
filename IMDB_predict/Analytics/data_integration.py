#encoding=utf-8

from hbase_connection import *

CAST_DATA = "../Data/processed_cast_data.dat"
MOVIE_DATA = "../Data/processed_movie_data.dat"

def insert_movie_data():
    f = open(MOVIE_DATA, "rb")

    while True:
        line = f.readline()
        if not line:
            break
        put_movie_data(line)

def insert_cast_data():
    f = open(CAST_DATA, "rb")

    while True:
        line = f.readline()
        if not line:
            break
        put_movie_cast_data(line)

def test_with_movie_data():
    print get_movie_by_id(285)

if __name__ == "__main__":
    insert_movie_data()
    test_with_movie_data()
    # insert_cast_data()
