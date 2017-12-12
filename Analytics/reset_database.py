#encoding=utf-8

from hbase_connection import *

if __name__ == "__main__":
    reset_database()
    insert_movie_data()
    insert_cast_data()
    post_process()
