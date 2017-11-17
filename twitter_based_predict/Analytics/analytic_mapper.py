#!/usr/bin/env python
import sys
import string, json, re
import unicodedata
import scipy
from fuzzywuzzy import fuzz
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS as stop_words
# import pdb

# used for remove punctition
tbl = dict.fromkeys(i for i in range(sys.maxunicode)
                      if unicodedata.category(unichr(i)).startswith('P'))

# list of movies currently avaliable
movies = ['Justice League', 'Ragnarok', 'Daddy\'s Home',
        'Murder on the Orient Express',
        'Wonder', 'The Star', 'A Bad Moms Christmas',
        'Tumhari Sulu', 'Qarib Qarib Singlle', 'Verna']

def analytic_mapper(input_from=sys.stdin, output_to=sys.stdout):
    for in_key, in_value in clean_tweet_generator(input_from):
        for movie in movies:
            # the fuzz model calculate vague matach score
            # between the movie name and the tweet
            if fuzz.partial_ratio(movie, in_value) > 80:
                mapper_out = '\t'.join([movie, in_value])
                print(mapper_out)

# a function that take unicode string and remove stop words and punctitions
def clean_text(text, stop_word_list=stop_words):
    res = text.lower()
    res = res.translate(tbl)
    res = res.replace('\n', ' ')
    res = res.split()
    # filter out 'numeric' words such as '14th'
    is_alpha = re.compile('^[a-z]+$')
    res = filter(lambda word: is_alpha.match(word), res)
    res = filter(lambda word: word not in stop_word_list, res)
    res = ' '.join(res)
    return res

# read tweet texts
def clean_tweet_generator(input_json):
    for line in input_json:
        tweet = json.loads(line)
        try:
            tweet_id = tweet[0]
            tweet_text = tweet[1]
            yield tweet_id, clean_text(tweet_text)
        except IndexError():
            pass
    raise StopIteration()

if __name__ == '__main__':
    # pdb.set_trace()
    # f = open('test_out.txt' ,'r')
    analytic_mapper()
