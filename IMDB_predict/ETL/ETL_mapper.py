# -*- coiding: utf-8 -*-

import csv,json,sys

MOVIE_DATASET = "/Users/huodahaha/Documents/BigData/hw9/tmdb_5000_movies.csv"
TEST_DATASET = "/Users/huodahaha/Documents/BigData/hw9/head.csv"
COL_CNT = 20

PROCESS_OK = 0
BAD_COL_CNT = 100
PARSE_FAILURE = 200
DAMAGED_RECORD = 300

recorded_genres = {'Mystery': 14, 'Romance': 8, 'History': 15, 'Family': 6, 'Fantasy': 10, 'Horror': 16, 'Crime': 0, 'Drama': 7, 'Science Fiction': 4, 'Animation': 5, 'Music': 9, 'Adventure': 2, 'Foreign': 18, 'Action': 3, 'Comedy': 1, 'Documentary': 17, 'War': 12, 'Thriller': 11, 'Western': 13}

recorded_langs = {'en': 0, 'zh': 3, 'cn': 17, 'af': 9, 'vi': 20, 'is': 25, 'it': 6, 'xx': 22, 'id': 23, 'es': 2, 'ru': 12, 'nl': 16, 'pt': 7, 'no': 18, 'nb': 21, 'th': 15, 'ro': 11, 'pl': 24, 'fr': 5, 'de': 1, 'da': 10, 'fa': 19, 'hi': 13, 'ja': 4, 'he': 14, 'te': 26, 'ko': 8}

def line_data(filename, skip_first = True):
    f = open(filename)
    if skip_first:
        s = f.readline(); 
    while True:
        s = f.readline()
        if len(s) == 0:
            break;
        else:
            yield s

def data_check(dic):
    # check budget 
    check_errors = []
    ages = 0
    if dic["budget"] == 0:
        check_errors.append("INVALID BUDGET")
    # check genre 
    if len(dic["genre"]) == 0:
        check_errors.append("INVALID GENRES")
    # check revenue
    if dic["revenue"] == 0:
        check_errors.append("INVALID REVENUE")
    return check_errors

def ETL_process(line):
    ret = PROCESS_OK
    errors = []
    check_errors = []
    dic = {}
    output = ""

    # parse a single line
    reader = csv.reader([line])
    parsed_line = reader.next()
    if len(parsed_line) != COL_CNT:
        ret = BAD_COL_CNT
        errors.append("DATA_COL_FAIL")
    else:
        # store the parsed result
        try:
            budget = int(parsed_line[0])
            title = parsed_line[17]
            genre_json = json.loads(parsed_line[1]) 
            genres = [0]* len(recorded_genres)
            for genre in genre_json:
                genre_id = recorded_genres[genre['name']]
                genres[genre_id] = 1
            lang = parsed_line[5]
            lang_vec = [0] * len(recorded_langs)
            lang_id = recorded_langs[lang]
            lang_vec[lang_id] = 1
            movie_id = int(parsed_line[3])
            revenue = int(parsed_line[12])
            released_time = parsed_line[11]
            year = int(released_time.split("-")[0])
            dic = {"title": title,\
                    "genre": genres,\
                    "budget": budget,\
                    "revenue": revenue,\
                    "lang": lang_vec,\
                    "year": year}
            check_errors = data_check(dic)
        except Exception as e:
            ret = PARSE_FAILURE
            errors.append("DATA_FORMAT_FAIL")
        else:
            errors += check_errors
            if len(errors) != 0:
                ret = DAMAGED_RECORD

    # generate return data
    if ret == PROCESS_OK:
        output = "%08d\t%s"%(movie_id, json.dumps(dic))

    return output.strip()

for line in sys.stdin:
    try:
        output = ETL_process(line)
    except Exception as e:
        pass
    else:
        sys.stdout.write(output)
        if len(output):
            sys.stdout.write("\n")

