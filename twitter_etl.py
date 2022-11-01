import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():

        access_key = "R1c9oUezExC7log80K0Efg480"
        access_secret = "mMRFl3nDcUZJwuoXZDV8LyTriTmPotKpehElXsnZxzrNrkJLe2"
        consumer_key = "324338786-E8Sfm9ftriStRAAbivLbhvwgI3YWZA64iuhSrS2i"
        consumer_secret = "0qbqtC1D5ndatLgpaRseukSvitLoaEOPhFSyC2zRGG21D"


        # Twitter authentication
        auth = tweepy.OAuthHandler(access_key, access_secret)   
        auth.set_access_token(consumer_key, consumer_secret) 

        # # # Creating an API object 
        api = tweepy.API(auth)
        tweets = api.user_timeline(screen_name='@elonmusk', count=200, include_rts = False, tweet_mode = 'extended')

        print(tweets)

        tweet_list = []

        for tweet in tweets:
                text = tweet._json["full_text"]

                refined_tweet = {"user": tweet.user.screen_name,
                                'text' : text,
                                'favorite_count' : tweet.favorite_count,
                                'retweet_count' : tweet.retweet_count,
                                'created_at' : tweet.created_at}
                
                tweet_list.append(refined_tweet)

        df = pd.DataFrame(tweet_list)
        df.to_csv("s3://andrei-airflow-twitter-bucket/elon_musk.csv")