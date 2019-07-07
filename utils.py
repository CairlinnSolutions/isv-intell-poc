import requests
import json
from datetime import date, timedelta
import datetime
from datetime import date, timedelta

import pandas as pd
import time
import io
import boto3
from io import StringIO

bucket = "isvaa"
dailyfolderpath = "daily"
dailysumfolderpath = "dailysum"

params = {
    "grant_type": "password",
    "client_id": "3MVG9KsVczVNcM8wO8m.fbSJVSWnEtgk5ukFdG65D.NEAVTcCqUuYDgm0FOBCO3b3m5JTR7qnKSXf49K8fF8K",
    "client_secret": "C6DDB888B8C5AF02EBFA87AED78966E168FE8D32B9E41C7CE78A702BE290AB0D",
    "username": "kamlesh.patel@labsapps.com",
    "password": "kam12345"
}


def processaa(url, deltadays):
    print("before token");
    today = date.today()
    aday = today - timedelta(days=deltadays)

    orgdata = getSFToken()
    print("after org info");

    rec = requestAA(orgdata, aday)
    print("after aa record response");

    createsum(orgdata, aday, rec)

    return 1

def createsum(orgdata, aday, rec):
    s = requests.get(rec['DownloadUrl']).content
    data = pd.read_csv(io.StringIO(s.decode('utf-8')))

    print(data.head())

    dailysumfilepath = dailysumfolderpath + '/' + aday.isoformat()  + '.csv'

    s3 = boto3.resource('s3', aws_access_key_id='AKIAITVDBSKAAQTDB3NQ',
                        aws_secret_access_key='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'
                        )

    orgdata = orgdata.groupby('organization_id', as_index=False).agg(
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
                            'custom_entity_nunique': 'customentitycount'
                            }, inplace=True)

    orgdata = orgdata.reset_index(drop=True)

    orgdata['App'] = 'CaseTimer'
    orgdata['dtLog'] = aday.isoformat()

    orgdata.to_csv('./schema_sample.csv', index=None,
                header=True)  # Don't forget to add '.csv' at the end of the path

    print(orgdata.head())

    csv_buffer = StringIO()
    orgdata.to_csv(csv_buffer, index=False)
    s3.Object(bucket, dailysumfilepath).put(Body=csv_buffer.getvalue())


def requestAA(orginfo, aday):

    adayfilename = aday.isoformat() + '.csv'
    adaystart = aday.isoformat() + 'T00:00:00'  # Convert to ISO 8601 string
    adayend = aday.isoformat() + 'T23:59:59'  # Convert to ISO 8601 string

    dailyfilepath = dailyfolderpath + '/' + adayfilename
    dailysumfilepath = dailysumfolderpath + '/' + adayfilename

    print(aday.isoformat())  # Print the string

    newreq = {
        'PackageIds': '0331U000000EHq2',
        'DataType': 'CustomObjectUsageLog',
        'StartTime': adaystart,
        'EndTime': adayend
    }
    # convert into JSON:
    jsonbody = json.dumps(newreq)
    print(jsonbody)
    postres = json.dumps(sf_api_call(orginfo,
        '/services/data/v46.0/sobjects/AppAnalyticsQueryRequest/', '', 'post', jsonbody), indent=2)
    print(postres)
    postres = json.loads(postres)

    time.sleep(60)

    getres = json.dumps(sf_api_call(orginfo,
        '/services/data/v46.0/sobjects/AppAnalyticsQueryRequest/%s' % (postres['id']), '', 'get', {}), indent=2)
    print(getres)

    res = json.loads(getres)
    return res

def getSFToken():
    r = requests.post(
        "https://login.salesforce.com/services/oauth2/token", params=params)
    d = dict();
    d['token'] = r.json().get("access_token")
    d['instanceUrl'] = r.json().get("instance_url")
    return d

def sf_api_call(orginfo, action, parameters={}, method='get', data={}):
    headers = {
        'Content-type': 'application/json',
        'Accept-Encoding': 'gzip',
        'Authorization': 'Bearer %s' % orginfo['token']
    }
    if method == 'get':
        r = requests.request(method, orginfo['instanceUrl']+action,
                             headers=headers, params=parameters, timeout=30)
    elif method in ['post', 'patch']:
        print('instance_url+action= %s' % (orginfo['instanceUrl']+action))
        print('data= %s' % (data))
        r = requests.request(method, orginfo['instanceUrl']+action,
                             headers=headers, data=data, params=parameters, timeout=10)
    else:
        raise ValueError('Method should be get or post or patch.')
        print('Debug: API %s call: %s' % (method, json.dumps(r)))

    if r.status_code < 300:
        if method == 'patch':
            return None
        else:
            return r.json()
    else:
        raise Exception('API error when calling %s : %s' %
                        (r.url, r.content))

processaa("tbd", 12)
