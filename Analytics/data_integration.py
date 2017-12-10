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
        lines = f.readline()
        if not lines:
            break
        put_movie_cast_data(lines)

def test_with_movie_data():
    print get_movie_by_id(285)

def movies_in_year(year):
    year = str(year)
    for movie in all_movies():
        if movie["year"] != "2016":
            continue
        print "%s, %s"%(movie["id"], movie["title"])

def cast_analytics():
    pass
    for movie in all_movies():
        # print movie
        revenues = []
        if not movie.has_key("cast"):
            continue
        cast = movie["cast"]
        cast = ast.literal_eval(cast)
        for actor in cast:
            actor_info = get_cast_by_id(actor)
            if not actor_info:
                continue
            revenues.append(int(actor_info["revenue"]))
        revenues.sort()
        cast_impression = sq_average(revenues[-5:])
        print "%s, %s, %s, %d, %s, %s"%(movie["revenue"], movie['budget'], \
                movie['year'], cast_impression,\
                str(movie["genre"])[1:-1], str(movie["lang"])[1:-1])

if __name__ == "__main__":
    # insert_movie_data()
    # insert_cast_data()
    # test_with_movie_data()
    # movies_in_year(2016)
    cast_analytics()
