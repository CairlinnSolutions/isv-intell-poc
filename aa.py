import pandas as pd

import requests
import json
import base64
import time
import io
import boto3
from io import StringIO
from datetime import date, timedelta
import datetime
import sys

from datetime import date, timedelta
today = date.today()
yesterday = today - timedelta(days=1)

yesterdayfilename = yesterday.isoformat() + '.csv'
yesterdaystart = yesterday.isoformat() + 'T00:00:00'  # Convert to ISO 8601 string
yesterdayend = yesterday.isoformat() + 'T23:59:59'  # Convert to ISO 8601 string

testfile = "./b.csv"
data = pd.read_csv(testfile)


orgdata = data.groupby('organization_id').agg(
    {
        'session_key': 'nunique',
        'user_id_token': 'nunique',
        'operation_count': ['sum'],
        'url': ['nunique'],
        'custom_entity': ['nunique']
    }
)

orgdata.columns = ["_".join(x) for x in orgdata.columns.ravel()]
# Don't forget to add '.csv' at the end of the path
orgdata = orgdata.reset_index()

orgdata.rename(columns={'session_key_nunique': 'sessioncount',
                        'user_id_token_nunique': 'usercount',
                        'operation_count_sum': 'operationssum',
                        'url_nunique': 'urlcount',
                        'custom_entity_nunique': 'customentitycount'
                        }, inplace=True)

orgdata = orgdata.to_csv('./out.csv', index=None, header=True)

print(orgdata)

# uniquesessioncount = data['organization_id'] data['session_key'].value_counts()
#print("uniquesessioncount=%s" % (uniquesessioncount))

print("done")
