#encoding=utf-8

from hbase_connection import *

if __name__ == "__main__":
    display_filters = []
    # display_filters.append(DF_discret_WL("year", [2016,2014]))
    # display_filters.append(DF_numeric_WL("year", 1990,2012))
    # display_filters.append(DF_numeric_WL("revenue", 0, 1000000))
    # display_filters.append(DF_numeric_WL("budget", 0, 200000))
    # display_filters.append(DF_lang_BL(["en","zh"]))
    # display_filters.append(DF_genre_WL(["Music"]))
    # display_filters.append(DF_lang_BL(["en","zh"]))
    display_filters.append(DF_genre_BL(["Romance", "Action"]))
    for movie in all_movies(display_filters):
        print movie
