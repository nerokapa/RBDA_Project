# -*- coiding: utf-8 -*-

import csv,json,sys

MOVIE_DATASET = "/Users/huodahaha/Documents/BigData/hw9/tmdb_5000_movies.csv"
TEST_DATASET = "/Users/huodahaha/Documents/BigData/hw9/head.csv"
COL_CNT = 20

PROCESS_OK = 0
BAD_COL_CNT = 100
PARSE_FAILURE = 200
DAMAGED_RECORD = 300

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
            genres = []
            for genre in genre_json:
                genres.append(genre['name'])
            movie_id = int(parsed_line[3])
            revenue = int(parsed_line[12])
            released_time = parsed_line[11]
            year = int(released_time.split("-")[0])
            dic = {"title": title,\
                    "genre": genres,\
                    "budget": budget,\
                    "revenue": revenue,\
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

