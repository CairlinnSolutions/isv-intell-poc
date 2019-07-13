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
from sf import getSFToken, sf_api_call
from ea import PushDataToEA

pboorg = {
    "grant_type": "password",
    "client_id": "3MVG9KsVczVNcM8wO8m.fbSJVSWnEtgk5ukFdG65D.NEAVTcCqUuYDgm0FOBCO3b3m5JTR7qnKSXf49K8fF8K",
    "client_secret": "C6DDB888B8C5AF02EBFA87AED78966E168FE8D32B9E41C7CE78A702BE290AB0D",
    "username": "kamlesh.patel@labsapps.com",
    "password": "kam12345"
}

eaorg = {
    "grant_type": "password",
    "client_id": "3MVG9KsVczVNcM8wO8m.fbSJVSWnEtgk5ukFdG65D.NEAVTcCqUuYDgm0FOBCO3b3m5JTR7qnKSXf49K8fF8K",
    "client_secret": "C6DDB888B8C5AF02EBFA87AED78966E168FE8D32B9E41C7CE78A702BE290AB0D",
    "username": "dev@kam.ent",
    "password": "kam123456eJjw0Wo5p192sww5bCFh0353F"
}

bucket = "isvaa"
dailyfolderpath = "daily"
dailysumfolderpath = "dailysum"

def processaa(deltadays):

    
    print("before token");
    today = date.today()
    aday = today - timedelta(days=deltadays)

    pboorginfo = getSFToken(pboorg)
    print("after pboorginfo info");

    rec = requestAA(pboorginfo, aday)
    print("after aa record response");

    sumdf = createsum(pboorginfo, aday, rec)
    

    #Testing only
    '''
    sumdf = pd.read_csv("./data.csv")
    sumdf['extid'] = sumdf['App'] + sumdf['dtLog'] + sumdf['dtLog']
    print(sumdf['extid'].head())
    '''

    eaorginfo = getSFToken(eaorg)
    print("after eaorginfo");
    metadata = open("./MetaDataJson.json", "r").read()
    
    PushDataToEA(eaorginfo, metadata, sumdf, "overwrite", "aatest")

    return 1

def createsum(orgdata, aday, rec):
    s = requests.get(rec['DownloadUrl']).content
    data = pd.read_csv(io.StringIO(s.decode('utf-8')))

    print(data.head())

    dailysumfilepath = dailysumfolderpath + '/' + aday.isoformat()  + '.csv'

    s3 = boto3.resource('s3', aws_access_key_id='AKIAITVDBSKAAQTDB3NQ',
                        aws_secret_access_key='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'
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
                            'org_info_': 'orginfo'}, inplace=True)

    orgdata = orgdata.reset_index(drop=True)

    orgdata['App'] = 'CaseTimer'
    orgdata['dtLog'] = aday.isoformat()
    orgdata['extid'] = orgdata['orgid'] + orgdata['App'] + orgdata['dtLog'] 

    orgdata.to_csv('./schema_sample.csv', index=None,
                header=True)  # Don't forget to add '.csv' at the end of the path

    print(orgdata.head())

    csv_buffer = StringIO()
    orgdata.to_csv(csv_buffer, index=False)
    s3.Object(bucket, dailysumfilepath).put(Body=csv_buffer.getvalue())

    return orgdata


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

    time.sleep(90)

    getres = json.dumps(sf_api_call(orginfo,
        '/services/data/v46.0/sobjects/AppAnalyticsQueryRequest/%s' % (postres['id']), '', 'get', {}), indent=2)
    print(getres)

    res = json.loads(getres)
    return res


processaa(1)
