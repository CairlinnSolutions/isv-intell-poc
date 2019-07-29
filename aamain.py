import requests
import json
from datetime import date, timedelta
import datetime

import pandas as pd
import time
import io
import os
import boto3
from io import StringIO
from sf import getSFToken, sf_api_call
from ea import PushDataToEA, DoesDatasetExist

pboorg = {
    "grant_type": "password",
    "client_id": os.environ['pboclientid'],
    "client_secret": os.environ['pboclientsecret'],
    "username": os.environ['pbousername'],
    "password": os.environ['pbopasswd']
}

eaorg = {
    "grant_type": "password",
    "client_id": os.environ['eaclientid'],
    "client_secret": os.environ['eaclientsecret'],
    "username": os.environ['eausername'],
    "password": os.environ['eapasswd']
}

dsname = os.environ['dsname']
metadataurl = os.environ['metadatajsonurl']
awskey = os.environ['awskey']
awssecret = os.environ['awssecret']
sleepseconds=int(os.environ['sleepseconds'])
packages=os.environ['packages']

bucket = "isvaa"
dailyfolderpath = "daily"
dailysumfolderpath = "dailysum"

def startjobForYesterday(appname, packages):
    today = date.today()
    aday = today - timedelta(days=1)
    whichdate = aday.isoformat()
    startjobByDate(appname, packages, whichdate)

def startjobByDate(appname, packages, whichdate):
    
    print("startjobForADate");

    #get EA metadata
    url = metadataurl
    r = requests.get(url, allow_redirects=False)
    metadata = r.text

    pboorginfo = getSFToken(pboorg)
    print("after pboorginfo info");

    rec = requestAAByDate(pboorginfo, packages, whichdate)
    print("after aa record response");

    sumdf = createsum(pboorginfo, appname, whichdate, rec)
    print("after createsum");
    
    eaorginfo = getSFToken(eaorg)
    print("after eaorginfo info");
    dsexist = DoesDatasetExist(eaorginfo, dsname)
    op = "overwrite"
    if(dsexist): 
        op = "upsert" 
    
    PushDataToEA(eaorginfo, metadata, sumdf, op, dsname)
    print("data pushed to EA");

    return 

def requestAAByDate(orginfo, packages, aday):

    adayfilename = aday + '.csv'
    adaystart = aday + 'T00:00:00'  # Convert to ISO 8601 string
    adayend = aday + 'T23:59:59'  # Convert to ISO 8601 string

    dailyfilepath = dailyfolderpath + '/' + adayfilename
    dailysumfilepath = dailysumfolderpath + '/' + adayfilename

    newreq = {
        'PackageIds': packages,
        'DataType': 'CustomObjectUsageLog',
        'StartTime': adaystart,
        'EndTime': adayend
    }
    # convert into JSON:
    jsonbody = json.dumps(newreq)
    postres = json.dumps(sf_api_call(orginfo,
        '/services/data/v46.0/sobjects/AppAnalyticsQueryRequest/', '', 'post', jsonbody), indent=2)
    postres = json.loads(postres)

    time.sleep(sleepseconds)

    getres = json.dumps(sf_api_call(orginfo,
        '/services/data/v46.0/sobjects/AppAnalyticsQueryRequest/%s' % (postres['id']), '', 'get', {}), indent=2)
    print(getres)

    res = json.loads(getres)
    return res


def createsum(orgdata, appname, aday, rec):
    s = requests.get(rec['DownloadUrl']).content
    data = pd.read_csv(io.StringIO(s.decode('utf-8')))

    print(data.head())

    dailysumfilepath = dailysumfolderpath + '/' + aday  + '.csv'

    s3 = boto3.resource('s3', aws_access_key_id=awskey,
                        aws_secret_access_key=awssecret
                        )

    orgdata = data.groupby('organization_id', as_index=False).agg(
        {
            'session_key': 'nunique',
            'user_id_token': 'nunique',
            'operation_count': ['sum'],
            'url': ['nunique'],
            'custom_entity': ['nunique']
        }
    )

    # Now summary file
    orgdata.columns = ["_".join(x) for x in orgdata.columns.ravel()]
    orgdata.rename(columns={'session_key_nunique': 'sessioncount',
                            'user_id_token_nunique': 'usercount',
                            'operation_count_sum': 'operationssum',
                            'url_nunique': 'urlcount',
                            'custom_entity_nunique': 'customentitycount',
                            'organization_id_': 'orgid'}, inplace=True)

    orgdata = orgdata.reset_index(drop=True)

    orgdata['App'] = appname
    orgdata['dtLog'] = aday
    orgdata['extid'] = orgdata['orgid'] + orgdata['App'] + orgdata['dtLog'] 

    print(orgdata.head())

    csv_buffer = StringIO()
    orgdata.to_csv(csv_buffer, index=False)
    #s3.Object(bucket, dailysumfilepath).put(Body=csv_buffer.getvalue())

    return orgdata



