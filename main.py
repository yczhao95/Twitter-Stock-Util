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
        i = 0
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
            i = i + 1
            if i%100 == 0:
                print (i)
    return "data successfully loaded to noSQL datastore"

@app.route('/queryrange', methods=['GET'])
def query():
    kind = request.args.get('kind')
    query = datastore_client.query(kind = kind)
    start_time =  request.args.get('start')
    end_time =  request.args.get('end')
    scale = request.args.get('scale')# in terms of minute
    start_key = datastore_client.key(kind, start_time);
    end_key = datastore_client.key(kind, end_time);
    query.key_filter(start_key,'>')
    query.key_filter(end_key,'<')
    result = list(query.fetch())
    #myarray = np.asarray(result)
    js = json.dumps(result)
    resp = Response(js, status=200, mimetype='application/json')
    resp.headers['Link'] = "http://twitter-stock.com"
    resp.headers['Access-Control-Allow-Origin'] = '*'
    #print(result)
    #print(res)
    return resp


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python37_app]
