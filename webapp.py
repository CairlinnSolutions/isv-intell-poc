# Author: Kam Patel
# Date: Aug 8th 2019
# Disclaimer: Please use at your own risk

# /server.py

import json
from six.moves.urllib.request import urlopen
from functools import wraps

from flask import Flask, request, jsonify, _request_ctx_stack
from flask_cors import cross_origin
from jose import jwt
import sys
from datetime import date, timedelta
from aa import copyandsummarize 
import redis
from rq import Queue
from worker import conn
import os

q = Queue(connection=conn)
APP = Flask(__name__)

# This needs no authentication. Don't use in production
@APP.route("/api/aa", methods=['POST'])
def aa():
    print ("basicaa called")
    content = request.json
    print (content)
    sys.stdout.flush()    

    result = q.enqueue(copyandsummarize, content['AppName'], content['packages'], content['whichDate'], content['filelocation'])

    return jsonify(message="STARTED")


# This does not need authentication
@APP.route('/')
def index():
    return 'App Analytics APIs'


