# -*- coiding: utf-8 -*-

import csv,json,sys,ast

TEST_DATASET = "../head.csv"
COMPLETE_DATASET = "../tmdb_5000_credits.csv"

#config
# MAIN_CAST_NUMBER = 5

COL_CNT = 4

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

def cast_etl_process(line):
    ret = PROCESS_OK
    errors = []
    movie_id = 0
    output = ""
    cast_ids = []

    # parse a single line
    reader = csv.reader([line])
    parsed_line = reader.next()
    if len(parsed_line) != COL_CNT:
        ret = BAD_COL_CNT
        errors.append("DATA_COL_FAIL")
    else:
        # store the parsed result
        try:
            movie_id = int(parsed_line[0])
            casts = ast.literal_eval(parsed_line[2])
            for cast in casts:
                name = cast['name']
                cast_id = int(cast['id'])
                cast_ids.append(cast_id)

        except Exception as e:
            ret = PARSE_FAILURE
            errors.append("DATA_FORMAT_FAIL")

        else:
            if len(errors) != 0:
                ret = DAMAGED_RECORD

    # generate return data
    if ret == PROCESS_OK:
        output = "MOVIE_%08d\t%s"%(movie_id, json.dumps(cast_ids))
        for cast_id in cast_ids:
            output += "\nCAST_%08d\t%s"%(cast_id, json.dumps([movie_id]))

    return output.strip()

if __name__ == "__main__":
    for line in sys.stdin:
        try:
            output = cast_etl_process(line)
        except Exception as e:
            pass
        else:
            sys.stdout.write(output)
            if len(output):
                sys.stdout.write("\n")

