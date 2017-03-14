
from flask import Flask, render_template, request
from elasticsearch import Elasticsearch
#import thread
import certifi
#import file StreamData

application = Flask(__name__)

keyword_dict = {'keyword 1':'movies','keyword 2':'technology','keyword 3':'sports','keyword 4':'life','keyword 5':'news','keyword 6':'travel','keyword 7':'health','keyword 8':'awesome','keyword 9':'energy','keyword 10':'music','Nothing here':'nothing','':'nothing'}
geo_dict = {'d1':10,'d2':50,'d3':100,'d4':200,'d5':500,'d6':1000,'d7':5000,'Nothing here':0,'': 0}
es = Elasticsearch(hosts = [{"host" : "search-tweetmap-utivo42pdpwisoy7ttbejzf7z4.us-east-1.es.amazonaws.com",
                              "port" : 443}],
                              use_ssl='True')

@application.route ('/',methods = ['POST', 'GET'])
def update_map():
    lat = 0.00
    long = 0.00
    key = "Nothing here"
    dist = "Nothing here"
    tweets = []
    if request.method == 'POST':
        
        key = request.form['keyword']
        dist = request.form['geo_dist']
        tweets = gettweets(keyword_dict[key])
      
        
    #if request.method == 'GET':
        
    #    tweets = [{'lng': 1, 'id': 8, 'tweet': 'A', 'lat': 5}]
            
    return render_template("index.html",lat = lat, long = long, key = keyword_dict[key], dist = geo_dict[dist], tweets = tweets)
    
    
def gettweets(keyword):
    
    tweets = []
    
    if keyword == "nothing":
        keyword = ""

    #body2 = {"query":{"match_all":{}}}
    body2 = {"query":{"match":{"_all":keyword}}}

    stream = es.search(index = "final-tweet-index", doc_type = "twitter", body = body2, size = 10000)
    
    tweets = stream["hits"]["hits"]
    
    return tweets
    
def getgeotweets(latitude, longitude, distance):
    
    distance_string = distance + 'km'
    
    body1 = {"query": {"match_all": {}}, "filter": {"geo_distance": { "distance": distance_string, "distance_type": "sloppy_arc", "location": { "lat": latitude, "lon": longitude 	}}}}

    result = es.search(index = "final-tweet-index", doc_type = "twitter", body = body1, size = 10000)

    return result
    
    

if __name__ == "__main__":
    #thread.start_new_thread(startTwitterRequests, ())
    application.run(host='0.0.0.0', debug = True)
