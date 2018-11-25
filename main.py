# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START gae_python37_app]
from flask import Flask,json,request,Response
from google.cloud import datastore
import concurrent.futures as cf
import csv

import query_util as qu
# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
executor = cf.ThreadPoolExecutor(2)

app = Flask(__name__)

datastore_client = datastore.Client()

@app.route('/')
def hello():

    kind = 'Stock'
    name = 'samplestock1'
    stock_key = datastore_client.key(kind, name)
    stock = datastore.Entity(key=stock_key)
    stock['date']='2018-11-5'


    datastore_client.put(stock)
    return ('saved{}:{}'.format(stock.key.name, stock['date']))


@app.route('/loadcsv', methods=['GET'])
def loadcsv():
    path = './csvdata/' + request.args.get("path")
    kind = request.args.get("kind")
    executor.submit(loadfile, path, kind)
    return "upload job started in the background"

def loadfile(path, kind):
    with open(path) as f:
        reader = csv.reader(f)
        for row in reader:
            name = row[0]
            stock_key = datastore_client.key(kind, name)
            stock = datastore.Entity(key=stock_key)
            stock['day'] = name[0:10]
            stock['minute'] = name[11:]
            stock['open'] = row[1]
            stock['close'] = row[2]
            stock['low'] = row[3]
            stock['high'] = row[4]
            stock['volume'] = row[5]
            datastore_client.put(stock)
    return "data successfully loaded to noSQL datastore"


@app.route('/queryrange', methods=['GET'])
def query():
    kind = 'spy'
    query = datastore_client.query(kind = kind)
    end_time =  request.args.get('end')
    day_range = (int)(request.args.get('range'))
    scale = (int)(request.args.get('scale'))
    
    start_time = qu.shift_day(end_time, day_range - 1) + '-000'
    end_time = end_time + '-960'
    start_key = datastore_client.key(kind, start_time)
    end_key = datastore_client.key(kind, end_time)
    query.key_filter(start_key,'>=')
    query.key_filter(end_key,'<=')
    result = list(query.fetch())

    scaled_result = qu.scale_down(result, scale)   
    #myarray = np.asarray(result)
    js = json.dumps(scaled_result)
    resp = Response(js, status=200, mimetype='application/json')
    resp.headers['Link'] = "http://twitter-stock.com"
    resp.headers['Access-Control-Allow-Origin'] = '*'
    #print(result)
    #print(res)
    return resp

@app.route('/loadTweets', methods = ['GET'])
def loadcsv_tweets():
    path = './csvdata/' + request.args.get("path")
    # Trump tweets
    kind = 'Trump'
    executor.submit(loadfile_tweets, path, kind)
    return "New upload job started in the background"

def loadfile_tweets(path, kind):
    with open(path) as f:
        reader = csv.reader(f)
        i = 0
        for row in reader:
            if i > 0:
                year = row[0]
                month = row[1]
                day = row[2]
                minute = row[3]
                timestamp = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2) + '-' + str(minute).zfill(4)
                tweet_key = datastore_client.key(kind, timestamp)
                tweet = datastore.Entity(key=tweet_key)
                tweet['time'] = timestamp
                tweet['EST'] = row[5]
                tweet['text'] = row[7]
                tweet['retweet_count'] = row[8]
                tweet['favorate_count'] = row[9]
                tweet['id'] = row[4]
                datastore_client.put(tweet)
            i = i + 1
    return "data successfully loaded to noSQL datastore"

@app.route('/queryTweets', methods = ['GET'])
def query_tweets():
    # get all Trump tweets entities
    kind = 'Trump'
    query = datastore_client.query(kind = kind)
    end_time = request.args.get('end')
    day_range = (int)(request.args.get('range'))
    start_time = qu.shift_day(end_time, day_range - 1) + '-0000'
    end_time = end_time + '-' + str(24 * 60)
    start_key = datastore_client.key(kind, start_time)
    end_key = datastore_client.key(kind, end_time)
    query.key_filter(start_key, '>=')
    query.key_filter(end_key, '<=')
    result = list(query.fetch())
    filtered_result = []
    for record in result:
        tmp_entry = {}
        time = record['time']
        minute = int(time[11:15])
        hour = int(minute / 60)
        minute = minute % 60
        tmp_entry['date'] = time[:10]
        tmp_entry['time'] = str(hour).zfill(2) + ':' + str(minute).zfill(2)
        tmp_entry['text'] = record['text']
        tmp_entry['id'] = record['id']
        tmp_entry['retweet'] = record['retweet_count']
        tmp_entry['favorate'] = record['favorate_count']
        filtered_result.append(tmp_entry)
    js = json.dumps(filtered_result)
    resp = Response(js, status = 200, mimetype='application/json')
    resp.headers['link'] = "http://twitter-stock.com"
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python37_app]
