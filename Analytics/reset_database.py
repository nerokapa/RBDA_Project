#encoding=utf-8

from hbase_connection import reset_database

if __name__ == "__main__":
    insert_movie_data()
    insert_cast_data()
    post_process()
    reset_database()
