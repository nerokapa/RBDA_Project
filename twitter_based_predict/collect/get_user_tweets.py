#!/usr/bin/env python
# encoding: utf-8

# Modified based on https://gist.github.com/yanofsky/5436496

import tweepy
import redis
import json
from celery import Celery

# configing the celery worker
# app = Celery('get_user_tweets', backend='redis://localhost', broker='pyamqp://')
app = Celery('get_user_tweets', backend='redis://localhost/0', broker='redis://localhost/1')
#Twitter API credentials
CONSUMER_KEY        = "IceR7uq5VJ6ohICoxOZ7yXkqi"
CONSUMER_SECRET     = "EoXhoKIDokcwv39fbADNybqkvSevDhfvHNGXrnxSVnnPCJjiy5"
ACCESS_TOKEN        = "853661430515728384-ba7r6ZI6vDjMaXb0TasvO2aekq87DyR"
ACCESS_TOKEN_SECRET = "6ZlCG3Tzqz7TnHjGUuTvlvrAo92gNM2BBKK503cEWDZtF"

r = redis.StrictRedis(host='localhost', port=6379, db=4)

@app.task
def get_tweets_of(screen_name):
    #Twitter only allows access to a users most recent 3240 tweets with this method
    #authorize twitter, initialize tweepy
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    new_tweets = api.user_timeline(screen_name=screen_name, count=200)
    # store in redis or some other
    for tweet in new_tweets:
        r.lpush(screen_name, tweet.text)
    return
#
# if __name__ == '__main__':
#     res = get_all_tweets('nerokapa')
#     for tweet in res:
#         print(tweet.text)
#         # tweet = json.loads(tweet)
#         # print tweet['text']
