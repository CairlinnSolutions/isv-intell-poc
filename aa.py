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

awskey = os.environ['awskey']
awssecret = os.environ['awssecret']
bucket = os.environ['awsbucket']

#awskey='AKIAITVDBSKAAQTDB3NQ' 
#awssecret='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'

#bucket = "isvaa"
dailyfolderpath = "daily"
dailysumfolderpath = "dailysum"

def copyandsummarize(appname, packages, whichdate, filelocation):
    
    print("copyandsummarize called")

    s = requests.get(filelocation).content
    data = pd.read_csv(io.StringIO(s.decode('utf-8')))

    print(data.head())

    sumdf = createsum(appname, whichdate, data)
    print("after createsum")
    
    #Store the summary file
    csv_buffer = StringIO()
    sumdf.to_csv(csv_buffer, index=False)
    dailysumfilepath = dailysumfolderpath + '/' + whichdate  + '-' + appname + '.csv'
    s3 = boto3.resource('s3', aws_access_key_id=awskey,
                    aws_secret_access_key=awssecret
                    )
    s3.Object(bucket, dailysumfilepath).put(Body=csv_buffer.getvalue())

    #Store the raw file
    csv_buffer2 = StringIO()
    data.to_csv(csv_buffer2, index=False)
    dailyfilepath = dailyfolderpath + '/' + whichdate  + '-' + appname + '.csv'
    s3.Object(bucket, dailyfilepath).put(Body=csv_buffer2.getvalue())

    print("data pushed to S3")

    return 

def createsum(appname, whichdate, data):

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
    orgdata['dtLog'] = whichdate
    orgdata['extid'] = orgdata['orgid'] + orgdata['App'] + orgdata['dtLog'] 

    print(orgdata.head())

    return orgdata


#filelocation = 'https://pa-production-pickup.s3.us-west-2.amazonaws.com/c7e713a5-c35f-11e9-b350-65b3e20997df_results.csv?X-Amz-Security-Token=AgoJb3JpZ2luX2VjEBAaCXVzLXdlc3QtMiJGMEQCIGn12jCVl9LVbAo1DVnBGxJfSYoDSZCiwsTvLmqClNsqAiAfCEmuArTvobs6oEwDzVwftJ5m8nvuuEoY1WphPh73pSqfAgio%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAAaDDY4NjQ0NDE3OTg2MCIMzpf8ehaa3OHIQlhPKvMBIl11e2pPqX76i8SM9R4AlLGuuzdlBoaDFM%2BskwytD%2Fo71wGqtD2eqxPGAFt1YwOPuJnHWyD4%2B40S4pv7KmjDUQbaHuDLuu%2FTpI0qc0wVbgstxfoOHh50lS4sjSUqp8itwTUPn9sZzwm%2BV0ElMFFOrdaUN9yvG7Lj8Oo%2BVMncouDUJ%2FgETaTfuLE484JWA7BBzT%2BgTKGWL9ga37FUSE6%2FTU69%2Buvee4DAlcVRD%2Basoqdd%2Bcihu8DcUlu7I6CyYn7oamkbj%2FIXy9v0HYg7apHpp7vqEVeEy%2BlnVzqsAOatV0QAF2DWkrR51jhkmLIXZy%2BrbO1gMKGe8OoFOrUBzdiJHVUNRF%2FHIUCjQU91HU3S6oLxoX6udgfasMRointjiXUn%2FLb6jsknB%2B23fujKUH4jZHUhTJEXCYAvgjNguOAxHK%2Bi2lAGBKNSkVBVwKnFlwGyRWV9NyJlot9p4Rei4m0lciwEEd5h5ubZTuarTFPjizHWKCmZY8nJwP3HXmYhHD86RY2WtEejfkdfG9AW6hWyaljc4t%2BiClwA3A3hf7Txr6YIf5sjJsHPHPtLHRQHZGtX7A%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20190820T153315Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=ASIAZ7U2C6GKNIUT7KMU%2F20190820%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=3b42e136436ddb009028072e91973e775e5f806d559ba986afe4303959b7001c'
#copyandsummarize('CaseTimer', '0331U000000EHq2', '2019-08-19', filelocation)