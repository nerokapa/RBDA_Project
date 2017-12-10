import sys, os
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.error import TweepError
# from tweepy.utils import import_simplejson
# json = import_simplejson()
import json

import csv

from httplib import IncompleteRead
from urllib3.exceptions import ProtocolError

# screen_names of movie related account


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



class No_tweet_collected(Exception):
    pass


def get_all_tweets(screen_name, auth):
    #Twitter only allows access to a users most recent 3240 tweets with this method
    #authorize twitter, initialize tweepy
    api = tweepy.API(auth)
    #initialize a list to hold all the tweepy Tweets
    alltweets = []
    #make initial request for most recent tweets (200 is the maximum allowed count)
    # import pdb; pdb.set_trace()
    try:
        # import pdb; pdb.set_trace()
        new_tweets = api.user_timeline(screen_name = screen_name,count=200)
    except TweepError:
        print(screen_name)
    #save most recent tweets
    alltweets.extend(new_tweets)
    #save the id of the oldest tweet less one
    try:
        oldest = alltweets[-1].id - 1
    except IndexError:
        import pdb; pdb.set_trace()
    #keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print "getting tweets before %s" % (oldest)
        #all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(screen_name = screen_name, count=200, max_id=oldest)
        #save most recent tweets
        alltweets.extend(new_tweets)
        #update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1
        print "...%s tweets downloaded so far" % (len(alltweets))
    return alltweets;

def get_tweets_statistic(tweets, movie_oa):
    offical_account_statistic = {}
    offical_account_statistic['movie_id'] = movie_oa[0]
    offical_account_statistic['movie_name'] = movie_oa[1]
    if(len(tweets) < 1):
        raise No_tweet_collected;
    # import pdb; pdb.set_trace()
    assert movie_oa[2].lower() == tweets[0].author.screen_name.lower()
    offical_account_statistic['oa_screen_name'] = tweets[0].author.screen_name
    offical_account_statistic['oa_follower_count'] = tweets[0].author.followers_count
    retweet_counts = []
    favorite_counts = []
    tweet_ids = []
    for tweet in tweets:
        try:
            retweet_counts.append(tweet.retweet_count)
            favorite_counts.append(tweet.favorite_count)
            tweet_ids.append(tweet.id)
        except AttributeError:
            print("AttributeError: " + str(tweet.id))
            pass
    assert len(retweet_counts) == len(tweets)
    assert len(favorite_counts) == len(tweets)
    sum_mean_max_temp1 = sum_mean_max(retweet_counts)
    # tweet_ids_str = ', '.join(str(tid) for tid in tweet_ids)
    offical_account_statistic['offical_account_retweet_sum'] = sum_mean_max_temp1[0]
    offical_account_statistic['offical_account_retweet_mean'] = sum_mean_max_temp1[1]
    offical_account_statistic['offical_account_retweet_max'] = sum_mean_max_temp1[2]
    sum_mean_max_temp2 = sum_mean_max(favorite_counts)
    offical_account_statistic['offical_account_favorite_sum'] = sum_mean_max_temp2[0]
    offical_account_statistic['offical_account_favorite_mean'] = sum_mean_max_temp2[1]
    offical_account_statistic['offical_account_favorite_max'] = sum_mean_max_temp2[2]
    # offical_account_statistic['list_of_tweet_id'] = tweet_ids_str;
    return offical_account_statistic, tweet_ids

def sum_mean_max(l):
    res_sum = sum(l)
    res_mean = float(res_sum) / float(len(l))
    res_max = max(l)
    return (res_sum, res_mean, res_max)

def load_csv(f):
    movie_offical_accounts = []
    with open(f) as os_list:
        id_name_account_junk = csv.reader(os_list, delimiter=',')
        for row in id_name_account_junk:
            # need to be changed if change
            if not row[2]:
                continue;
            movie_offical_accounts.append( (int(row[0]), str(row[1]).strip(),
                                        str(row[2]).replace("""\xe2\x80\x8f""", '').strip()) )
    # import pdb; pdb.set_trace()
    return movie_offical_accounts

def load_writed_json(f):
    if not os.path.isfile(f):
        return set()
    collected = set()
    with open(f) as json_file:
        for row in json_file:
            # import pdb; pdb.set_trace()
            collected.add(json.loads(row)['oa_screen_name'].lower())
            print("Have already collected: " + json.loads(row)['oa_screen_name'])
    return collected

if __name__ == '__main__':
    movie_oa_list = sys.argv[1];
    output_file = sys.argv[2];
    # import pdb; pdb.set_trace()
    movie_offical_accounts = load_csv(movie_oa_list)
    collceted_movie = load_writed_json(output_file)
    # open()
    ping_pong = 0;
    for id_name_account in movie_offical_accounts:
        ping_pong += 1
        if(id_name_account[2].lower() in collceted_movie):
            continue
        tweets = get_all_tweets(id_name_account[2], auth=auths[ping_pong % 2])
        meta_account_info, tweet_ids = get_tweets_statistic(tweets, id_name_account)
        to_write = json.dumps(meta_account_info)
        with open(output_file, 'a+') as foutput_meta:
            foutput_meta.write(to_write + '\n')
        # os.system("mkdir " + os.path.join('ids',id_name_account[2]));
        with open(os.path.join('ids', id_name_account[2]), 'w') as foutput_ids:
            for tid in tweet_ids:
                foutput_ids.write(str(tid) + '\n')
        # import pdb; pdb.set_trace()
        print("the number of account collected = %s" % str(ping_pong))
