#encoding=utf-8

import happybase, json, os
import ast, math

HBASE_DOMAIN = os.getenv('HBASE_DOMAIN', 'localhost')
MOVIE_DATA_TABLE = os.getenv('MOVIE_DATA_TABLE', 'MovieTable')
CAST_DATA_TABLE = os.getenv('CAST_DATA_TABLE', 'CastTable')

MOVIE_ATTR = ["budget", "genre", "revenue", "title", "year", "cast"]
CAST_ATTR = ["revenue", "movies"]

def sq_average(l):
    cnt = len(l)
    if cnt is 0:
        return 0
    sq_sum = 0
    for ele in l:
        sq_sum = sq_sum + ele*ele
    return math.sqrt(sq_sum*1.0/cnt)

def average(l):
    cnt = len(l)
    if cnt is 0:
        return 0
    return sum(l)/cnt

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

def put_movie_data(lines):
    table = get_movie_table()
    if not isinstance(lines, list):
        lines = [lines]
    with table.batch() as b:
        for line in lines:
            row_key, msg = line.split("\t")
            info = json.loads(msg)
            for (k, v) in info.items():
                column = ("cf:" + k).encode("ascii")
                value = json.dumps(v)
                b.put(row_key, {column:value})

def put_movie_cast_data(lines):
    movie_table = get_movie_table()
    cast_table = get_cast_table()
    if not isinstance(lines, list):
        lines = [lines]
    with movie_table.batch() as movie_batch, cast_table.batch() as cast_batch:
        for line in lines:
            if line.find("MOVIE") >= 0:
                # data is like 
                # MOVIE_00396152	[1615805, 1615806, 12714, 58793]
                id_msg, cast_msg = line.split("\t")
                MOVIE, movie_id = id_msg.split("_")
                assert MOVIE == "MOVIE"
                movie_info = get_movie_by_id(movie_id)
                if not movie_info:
                    continue
                row_key = convert_id(movie_id)
                column = "cf:cast".encode("ascii")
                movie_batch.put(row_key, {column:cast_msg})
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
                map(convert_id, movie_ids)
                row_key = convert_id(cast_id)
                # put revenue
                column = "cf:revenue".encode("ascii")
                value = json.dumps(average(revenues))
                cast_batch.put(row_key, {column:value})
                # put movies
                column = "cf:movies".encode("ascii")
                value = json.dumps(movie_ids)
                cast_batch.put(row_key, {column:value})

def get_movie_by_id(id):
    table = get_movie_table()
    ret = {}
    row = table.row(convert_id(id))
    if row == None:
        return None
    else:
        for attr in MOVIE_ATTR:
            column = ("cf:" + attr).encode("ascii")
            if column in row:
                ret[attr] = row[column];
        return ret

def get_cast_by_id(id):
    table = get_cast_table()
    ret = {}
    row = table.row(convert_id(id))
    if row == None:
        return None
    else:
        for attr in CAST_ATTR:
            column = ("cf:" + attr).encode("ascii")
            if column in row:
                ret[attr] = row[column];
        return ret

# do a join like process
def cast_movies_generator(id):
    table = get_movie_table()
    cols = []
    cast_info = get_cast_table(id)  
    if not cast_info:
        return

    movies_id = cast_info["movies"]
    for movie_id in movies_id:
        yield get_movie_table(movie_id)

# yield all movies, will casue severe performance problem
def all_movies():
    table = get_movie_table()
    cols = []
    for attr in MOVIE_ATTR:
        cols.append(("cf:" + attr).encode("utf-8"))
    scanner = table.scan(columns = cols)
    for res in scanner:
        movie_id = res[0]
        movie_info = res[1]
        ret = {}
        is_break = False
        ret["id"] = movie_id 
        for attr in MOVIE_ATTR:
            column = ("cf:" + attr)
            if not movie_info.has_key(column):
                is_break = False
            else:
                ret[attr] = movie_info[column];
        if not is_break:
            yield ret
