# !/usr/bin/python
import sys, ast

# config
CAST_MOVIE_LOWER_CNT = 2

current_cast = None
current_list = []
word = None

for line in sys.stdin:
    line = line.strip()
    parsed = line.split('\t')
    if len(parsed) != 2:
        continue

    word = parsed[0]
    try:
        l = ast.literal_eval(parsed[1])
    except ValueError:
        continue

    if current_cast == word:
        current_list += l
    else:
        if len(current_list) >= CAST_MOVIE_LOWER_CNT:
            print '%s\t%s' % (current_cast, current_list)
        current_list = l
        current_cast = word

if current_cast == word:
    if len(current_list) >= CAST_MOVIE_LOWER_CNT:
        print '%s\t%s' % (current_cast, current_list)
