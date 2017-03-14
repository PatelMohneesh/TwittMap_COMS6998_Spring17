#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 23:22:40 2017

@author: Mohneesh
"""

from elasticsearch import Elasticsearch
import time
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
import json



#Keys to be obtained from Twitter Application

ACCESS_TOKEN ='598322060-85YdYXf17Ecf30GXZdT7hNqypapNUfIbk6JzCHxv'
ACCESS_SECRET = 'M6M1MLGQCNm8Rfb3wUOMx5h9b6i0LJuA25DbDgFMhlIwk'
CONSUMER_KEY = 'pDLnK0jtrrj3TOlj9eKr7yOQt'
CONSUMER_SECRET = '6rZ2NeZq7MjC0lQVA501zAtuBso845njET2TTReENJRrmJTbet'

#ElasticSearch Host URL

ES_HOST_URL = "ES URL"


#Connecting to the ElasticSeacrh 

es = Elasticsearch(
    hosts=[{'host': ES_HOST_URL, 'port': 443}],
    use_ssl=True,
)

#Info. Elastic Search 
print(es.info())

#Listening to Twitter Data. 

class listner(StreamListener):
    count = 0
    def on_data(self, raw_data):
        try:
            tweets_json = json.loads(raw_data)
            try:
                if('coordinates' in tweets_json):
                    if tweets_json["coordinates"] is not None:
                        lon = tweets_json["coordinates"]['coordinates'][0]
                        lat = tweets_json["coordinates"]['coordinates'][1]
                        text = tweets_json["text"]
                        text = text.encode(encoding='utf-8')
                        tweet_json = {
                            'tweet':text,
                            'lat':lat,
                            'lng':lon,
                            'id':tweets_json['id']
                        }
                        es.index(index='final-tweet-index',doc_type='twitter',id = tweet_json['id'],body=tweet_json)
                        print("Received")

            except:
                return True
            return True
        except:
            print('error')
            time.sleep(1)

    def on_error(self, status_code):
        print(status_code)

#Authenticating Credentials 

oauth = OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
oauth.set_access_token(ACCESS_TOKEN,ACCESS_SECRET)

#Streaming Twitter Data
#Filtering Twitter Data

while(True):
    try:
        twitterStream = Stream(oauth,listner())
        twitterStream.filter(languages=["en"],track=['movies', 'technology','sports','life','news', 'travel', 'health', 'awesome','energy','music'])
    except:
        continue
