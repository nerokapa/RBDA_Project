from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from tweepy.utils import import_simplejson
json = import_simplejson()

import csv

# debugging
# import pdb

# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="f1EcPHGqaMQyaIrKOxbWMcrtg"
consumer_secret="7I0hLDffxmHTIrcwkRzRDcRhV7aUnreSZAd0MTGtcojiZYS041"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token="3135879643-Dvp2azyD4eSkTuBHZ6plohKsBwb9K6q3dB2p2Kr"
access_token_secret="Pu4gZIQzfVuNJvKK313BqhmyMypuMm2Hmio3TJqVvmnzy"

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    # def selecting_twitter_field(self, data):

    def __init__(self, start_index, file_path):
        StreamListener.__init__(self)
        StdOutListener.index = start_index
        StdOutListener.output_file = file_path

    def on_data(self, rawdata):
        # pdb.set_trace()
        data = json.loads(rawdata)
        # print(data.keys())
        try:
            if(data['lang'] == 'en'):
                output = [data['id'], data['text'].encode('utf-8').rstrip(), data['favorite_count'],
                        data['retweet_count']]
                print(output)
        except KeyError:
            pass
            # f.write()
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener(0, 'data/twitters.json')
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # pdb.set_trace()
    stream = Stream(auth, l)
    stream.filter(track=['Justice League', 'Ragnarok', 'Daddy\'s Home', 'Murder on the Orient Express'
                         'Wonder', 'The Star', 'A Bad Moms Christmas', 'Tumhari Sulu', 'Qarib Qarib Singlle', 'Verna'])
