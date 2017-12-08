#!/usr/bin/env python

import sys, ast

langs = {}

def summerize_lang(line):
    movie_id, info = line.split("\t")
    info = ast.literal_eval(info)
    if info["lang"] in langs:
        return
    else:
        langs[info["lang"]] = len(langs)
        print langs

for line in sys.stdin:
    line = line.strip()
    # summerize_lang(line)
    print line
