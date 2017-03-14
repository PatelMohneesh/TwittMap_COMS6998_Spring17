
from flask import Flask, render_template, request
from elasticsearch import Elasticsearch
#import tweepy
import certifi

application = Flask(__name__)

keyword_dict = {'keyword 1':'movies','keyword 2':'technology','keyword 3':'sports','keyword 4':'soccer','keyword 5':'news','keyword 6':'engineering','keyword 7':'twitter','keyword 8':'google','keyword 9':'facebook','keyword 10':'amazon','Nothing here':'','':''}

es = Elasticsearch(hosts = [{"host" : "search-mp3542-1-posiat2uo7jopakbg7bacyvcbi.us-east-1.es.amazonaws.com",
                              "port" : 443}],
                              use_ssl='True')

@application.route ('/',methods = ['POST', 'GET'])
def update_map():
    lat = 0
    long = 0
    key = "Nothing here"
    #tweets = [{'_type': 'twitter', '_source': {'content': "#top3apps for 'cm okram ibobi singh'\n\ntwitter for android 45%\ntwitter web client 17%\ntwitter for iphone 10%", 'user': 'Trendinalia India', 'user_id': 1270150182, 'coordinates': [40.7128, -74.0059]}, '_id': 'AVrHfR1W0V-fCyN2tAXe', '_index': 'cloud_tweet', '_score': 4.2272134},
    #                                           {'_type': 'twitter', '_source': {'content': "#top3apps for 'cm okram ibobi singh'\n\ntwitter for android 45%\ntwitter web client 17%\ntwitter for iphone 10%", 'user': 'Trendinalia India', 'user_id': 1270150182, 'coordinates': [49.7128, -70.0059]}, '_id': 'AVrHfR1W0V-fCyN2tAXe', '_index': 'cloud_tweet', '_score': 4.2272134},
    #                                                                            {'_type': 'twitter', '_source': {'content': "#top3apps for 'cm okram ibobi singh'\n\ntwitter for android 45%\ntwitter web client 17%\ntwitter for iphone 10%", 'user': 'Trendinalia India', 'user_id': 1270150182, 'coordinates': [52.7128, -73.0059]}, '_id': 'AVrHfR1W0V-fCyN2tAXe', '_index': 'cloud_tweet', '_score': 4.2272134}]
    #tweets = ["dddsd","dsdsd","dsdds"]
    tweets = []
    if request.method == 'POST':
        
        key = request.form['keyword']
        tweets = gettweets(keyword_dict[key])
            
    return render_template("index.html",lat = lat, long = long, key = keyword_dict[key], tweets = tweets)
    
    
def gettweets(keyword):
    
    tweets = []
    
    #body2 = {"query":{"match_all":{}}}
    body2 = {"query":{"match":{"_all":keyword}}}

    stream = es.search(index = "cloud_tweet", doc_type = "twitter", body = body2, size = 10000)
    
    tweets = stream["hits"]["hits"]
    
    return tweets

if __name__ == "__main__":
    application.run(host='0.0.0.0', debug = True)
