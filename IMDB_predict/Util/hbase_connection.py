#encoding=utf-8

import happybase, json

HBASE_DOMAIN = os.getenv('HBASE_DOMAIN', 'localhost')
MOVIE_DATA_TABLE = os.getenv('MOVIE_DATA_TABLE', 'MovieTable')
CAST_DATA_TABLE = os.getenv('CAST_DATA_TABLE', 'CastTable')

MOVIE_ATTR = ["budget", "genre", "revenue", "title", "year"]
CAST_ATTR = ["movies"]

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
    return ("%08d"%(int(ts))).encode("ascii")

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

def put_movie_data(line):
    table = get_movie_table()
    with table.batch() as b:
        row_key, msg = line.split("\t")
        info = json.loads(msg)
        for k, v in info:
            cloumn = ("cf:" + k).encode("ascii")
            value = json.dumps(v)
            b.put(row_key, {cloumn:value})

def put_movie_cast_data(line):
    table = get_movie_table()
    with table.batch() as b:
        id_msg, cast_msg = line.split("\t")
        MOVIE, movie_id = id_msg.split("_")
        casts = json.dumps(cast_msg)
        map(convert_id, casts)
        row_key = convert_id(movie_id)
        cloumn = "cf:movies".encode("ascii")
        value = json.dumps(casts)
        b.put(row_key, {cloumn:value})

def get_movie_by_id(id):
    table = get_movie_table()
    ret = {}
    row = table.row(convert_id(id))
    if row == None:
        return None
    else:
        for attr in MOVIE_ATTR:
            cloumn = ("cf:" + attr).encode("ascii")
            if column in res:
                ret[attr] = res[cloumn];
        return ret

def get_cast_by_id(id):
    table = get_cast_table()
    ret = {}
    row = table.row(convert_id(id))
    if row == None:
        return None
    else:
        for attr in CAST_ATTR:
            cloumn = ("cf:" + attr).encode("ascii")
            if column in res:
                ret[attr] = res[cloumn];
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

def is_cast_data_exist(cast_id):
    table = get_cast_table()
    cols = []
    for attr in CAST_ATTR:
        cols.append(("cf:" + cast_id).encode("utf-8"))
    scanner = table.scan(columns = cols)
    return (res in scanner):
