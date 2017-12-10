import sys, os
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from fuzzywuzzy import fuzz
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS as stop_words
from nltk.sentiment.vader import SentimentIntensityAnalyzer


CONSUMER_KEY        = "IceR7uq5VJ6ohICoxOZ7yXkqi"
CONSUMER_SECRET     = "EoXhoKIDokcwv39fbADNybqkvSevDhfvHNGXrnxSVnnPCJjiy5"
ACCESS_TOKEN        = "853661430515728384-ba7r6ZI6vDjMaXb0TasvO2aekq87DyR"
ACCESS_TOKEN_SECRET = "6ZlCG3Tzqz7TnHjGUuTvlvrAo92gNM2BBKK503cEWDZtF"

consumer_key        ="f1EcPHGqaMQyaIrKOxbWMcrtg"
consumer_secret     ="7I0hLDffxmHTIrcwkRzRDcRhV7aUnreSZAd0MTGtcojiZYS041"
access_token        ="3135879643-Dvp2azyD4eSkTuBHZ6plohKsBwb9K6q3dB2p2Kr"
access_token_secret ="Pu4gZIQzfVuNJvKK313BqhmyMypuMm2Hmio3TJqVvmnzy"

auth1 = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth1.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

auth2 = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth2.set_access_token(access_token, access_token_secret)

auths = [auth1, auth2]



if __name__ == "__main__":
    
