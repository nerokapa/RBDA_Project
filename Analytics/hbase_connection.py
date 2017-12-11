#encoding=utf-8

import happybase
import math, numpy
import sys, os, json, ast

CAST_DATA = "../Data/processed_cast_data.dat"
MOVIE_DATA = "../Data/processed_movie_data.dat"

HBASE_DOMAIN = os.getenv('HBASE_DOMAIN', 'localhost')
MOVIE_DATA_TABLE = os.getenv('MOVIE_DATA_TABLE', 'MovieTable')
CAST_DATA_TABLE = os.getenv('CAST_DATA_TABLE', 'CastTable')

BASIC_MOVIE_ATTR = ["budget", "genre", "revenue", "title", "year", "cast", "lang"]
DISPLAY_ATTR = ["revenue","budget","cast_impression","year","genre","lang"]

GENRES = {'Mystery': 14, 'Romance': 8, 'History': 15, 'Family': 6, 'Fantasy': 10, 'Horror': 16, 'Crime': 0, 'Drama': 7, 'Science Fiction': 4, 'Animation': 5, 'Music': 9, 'Adventure': 2, 'Foreign': 18, 'Action': 3, 'Comedy': 1, 'Documentary': 17, 'War': 12, 'Thriller': 11, 'Western': 13}

LANGS = {'en': 0, 'zh': 3, 'cn': 17, 'af': 9, 'vi': 20, 'is': 25, 'it': 6, 'xx': 22, 'id': 23, 'es': 2, 'ru': 12, 'nl': 16, 'pt': 7, 'no': 18, 'nb': 21, 'th': 15, 'ro': 11, 'pl': 24, 'fr': 5, 'de': 1, 'da': 10, 'fa': 19, 'hi': 13, 'ja': 4, 'he': 14, 'te': 26, 'ko': 8}


def reset_database():
    # delete table
    delete_table(MOVIE_DATA_TABLE)
    delete_table(CAST_DATA_TABLE)

    # rebuild table
    cfs = {"cf":None}
    connection = happybase.Connection(HBASE_DOMAIN)
    connection.create_table(MOVIE_DATA_TABLE, families = cfs)
    connection.create_table(CAST_DATA_TABLE, families = cfs)

def convert_id(id):
    return ("%08d"%(int(id))).encode("ascii")

def get_table(table_name):
    connection = happybase.Connection(HBASE_DOMAIN)
    table_lists = connection.tables()
    if not table_name in table_lists:
        raise Exception("TABLE NOT EXIST")
    # get table instance
    return connection.table(table_name)

def delete_table(table_name):
    connection = happybase.Connection(HBASE_DOMAIN)
    table_lists = connection.tables()
    if table_name in table_lists:
        connection.delete_table(table_name, disable=True)

def get_movie_table():
    return get_table(MOVIE_DATA_TABLE)

def get_cast_table():
    return get_table(CAST_DATA_TABLE)

def get_movie_by_id(id):
    table = get_movie_table()
    query = table.row(convert_id(id)) or {}
    ret = {}
    for (k,v) in query.items():
        ret[k.replace("cf:", "")] = v
    return ret

def get_cast_by_id(id):
    table = get_cast_table()
    query = table.row(convert_id(id)) or {}
    ret = {}
    for (k,v) in query.items():
        ret[k.replace("cf:", "")] = v
    return ret

def insert_movie_data():
    f = open(MOVIE_DATA, "rb")
    table = get_movie_table()
    with table.batch() as b:
        for line in f:
            row_key, msg = line.split("\t")
            info = json.loads(msg)
            for (k, v) in info.items():
                b.put(row_key, {"cf:"+k:json.dumps(v)})

def insert_cast_data():
    f = open(CAST_DATA, "rb")
    counter = 0
    for line in f:
        # update connection
        if counter:
            counter = counter-1
        else:
            movie_table = get_movie_table()
            cast_table = get_cast_table()
            counter = 500
                    
        if line.find("MOVIE") >= 0:
            # data is like 
            # MOVIE_00396152	[1615805, 1615806, 12714, 58793]
            id_msg, cast_msg = line.split("\t")
            MOVIE, movie_id = id_msg.split("_")
            assert MOVIE == "MOVIE"
            if get_movie_by_id(movie_id):
                cast = ast.literal_eval(cast_msg)
                movie_table.put(convert_id(movie_id), {"cf:cast":cast_msg})
        else:
            # data is like
            # CAST_00000001	[1895, 306, 879, 87]
            id_msg, movie_ids = line.split("\t")
            CAST, cast_id = id_msg.split("_")
            assert CAST == "CAST"
            movie_ids = ast.literal_eval(movie_ids)
            revenues = []
            for movie_id in movie_ids:
                movie_info = get_movie_by_id(movie_id)
                if movie_info:
                    revenues.append(int(movie_info["revenue"]))
            mean = 0
            if len(revenues) > 0:
                mean = numpy.mean(revenues)
            cast_table.put(cast_id, {"cf:revenue":str(mean)})

# record filter that make sure every recode has all fields
def RF_has_all_attibute(movie_info, requiared_attr = BASIC_MOVIE_ATTR):
    ret = {}
    for attr in requiared_attr:
        if not movie_info.has_key("cf:"+attr):
            return {}
        ret[attr] = movie_info["cf:"+attr]
    return ret 

# record filter that calculate cast impression
def RF_calculate_CI(movie_info):
    if movie_info == {}:
        return {}
    revenues = []
    cast = ast.literal_eval((movie_info["cast"]))
    for actor in cast:
        actor_info = get_cast_by_id(actor)
        if not actor_info:
            continue
        revenues.append(float(actor_info["revenue"]))
    sqrd_r = map(lambda x: x*x, revenues[-5:]) 
    if len(sqrd_r) < 1:
        mean = 0
    else:
        mean = math.sqrt(numpy.mean(sqrd_r))
    movie_info["cast_impression"] = str(mean)
    return movie_info

# filters is order sensitive
def post_process(RF_filters = [RF_has_all_attibute, RF_calculate_CI]):
    table = get_movie_table()
    cols = []
    scanner = table.scan(columns = cols)
    for res in scanner:
        row_key = res[0]
        movie_info = res[1]
        table.delete(row_key)
        for RF_filter in RF_filters:
            movie_info = RF_filter(movie_info)
        if movie_info:
            for (k, v) in movie_info.items():
                table.put(row_key, {"cf:"+k:v})

# filter = DF_DiscretWL("year", [2012,2016,2017]) will generate a display filter that only accept movies in year 2012, 2016 and 2017
def DF_DiscretWL(data_filed, WL):
    WL = map(str, WL)
    def DF(movie_info):
        return movie_info[data_filed] in WL
    return DF

# filter = DF_Numeric("revenue", [1000,2000]) will generate a display filter that only accept movies whose revenue is [1000, 2000]
def DF_revenue(data_filed, lower_bound, upper_bound):
    def DF(movie_info):
        data = int(movie_info[data_filed])
        return (data >= lower_bound) and (data <= upper_bound)
    return DF

# filter = DF_lang_WL(["en","zh"] will generate a display filter that only accept movies with language of "en" and "zh")
def DF_lang_WL(lang_wl):
    def DF(movie_info):
        data = int(movie_info["lang"])
        for lang in lang_wl:
            idx = LANGS[lang]
            if data[idx]:
                return True
        return False
    return DF

# filter = DF_Lang_BL(["en","zh"] will generate a display filter that will not alow movies with language of "en" and "zh")
def DF_Lang_BL(genre_bl):
    def DF(movie_info):
        data = int(movie_info["lang"])
        for lang in lang_bl:
            idx = LANGS[lang]
            if data[idx]:
                return False
        return True
    return DF

# filter = DF_genre_WL(["en","zh"] will generate a display filter that only accept movies with language of "en" and "zh")
def DF_genre_WL(lang_wl):
    def DF(movie_info):
        data = int(movie_info["lang"])
        for lang in lang_wl:
            idx = GENRES[lang]
            if data[idx]:
                return True
        return False
    return DF

# filter = DF_Lang_BL(["en","zh"] will generate a display filter that will not alow movies with language of "en" and "zh")
def DF_Lang_BL(lang_bl):
    def DF(movie_info):
        data = int(movie_info["lang"])
        for lang in lang_bl:
            idx = SUPPURTED_LANGS[lang]
            if data[idx]:
                return False
        return True
    return DF



def all_movies(display_filters = None):
    table = get_movie_table()
    cols = []
    scanner = table.scan(columns = cols)
    for res in scanner:
        movie_id = res[0]
        movie_info = res[1]
        ret = {"id":res[0]}
        for attr in DISPLAY_ATTR:
            ret[attr] = ast.literal_eval(movie_info["cf:"+attr])
        yield ret
