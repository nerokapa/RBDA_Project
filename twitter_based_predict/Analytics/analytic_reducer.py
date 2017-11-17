import sys
import json
from collections import defaultdict
from nltk.sentiment.vader import SentimentIntensityAnalyzer

movie_count = {}
movie_count = defaultdict(lambda: {'total': 0, 'positive': 0, 'negative': 0}, movie_count)


sid = SentimentIntensityAnalyzer()
if __name__ == '__main__':
    for line in sys.stdin:
        movie, tweet = line.split('\t', 1)
        movie_count[movie]['total'] += 1
        # print tweet
        ss = sid.polarity_scores(tweet)
        if ss['compound'] > 0.5:
            movie_count[movie]['positive'] += 1
        elif ss['compound'] < -0.5:
            movie_count[movie]['negative'] += 1
    # print movie_count
    for movie, count in movie_count.items():
        print '\t'.join([movie, json.dumps(count)])
