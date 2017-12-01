from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from tweepy.utils import import_simplejson
json = import_simplejson()

from get_user_tweets import get_tweets_of
from celery.exceptions import TimeoutError
from httplib import IncompleteRead
from urllib3.exceptions import ProtocolError
import redis
# debugging
# import pdb
r = redis.StrictRedis(host='localhost', port=6379, db=3)
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
        data = json.loads(rawdata)
        try:
            if(data['lang'] == 'en'):
                # print(data.keys())
                r.hmset(data['id'], data)
                user_tweets = get_tweets_of.delay(data['user']['screen_name'])
                try:
                    res = user_tweets.get(timeout=2)
                except TimeoutError:
                    print('TimeoutError happend')
                    pass
                # print(data['text'])
                # raise RuntimeError
                # with open(self.output_file, "a") as f:
                #     output = [data['id'], data['text'].encode('utf-8').rstrip(), data['favorite_count'],
                #             data['retweet_count']]
                #     f.write(json.dumps(output) + '\n')
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
    while True:
        try:
            stream = Stream(auth, l)
            # stream.filter(track=['the'])
            stream.filter(track=['Justice League', 'Ragnarok', 'Daddy\'s Home',
                    'Murder on the Orient Express',
                    'Wonder', 'The Star', 'A Bad Moms Christmas',
                    'Tumhari Sulu', 'Qarib Qarib Singlle', 'Verna'])
        except IncompleteRead:
            continue
        except ProtocolError:
            print("ProtocolError happened")
            continue
        except KeyboardInterrupt:
            stream.disconnect()
            break
