import requests
import json
import base64

def getSFToken(parameters):
    r = requests.post(
        "https://login.salesforce.com/services/oauth2/token", params=parameters)
    d = dict();
    d['token'] = r.json().get("access_token")
    d['instanceUrl'] = r.json().get("instance_url")

    baseurl = "https://" + r.json().get("instance_url")
    starturl = baseurl + "/services/data/v46.0/sobjects/"
    d['extdataurl'] = "/services/data/v46.0/sobjects/InsightsExternalData"
    d['extdataparturl'] = "/services/data/v46.0/sobjects/InsightsExternalDataPart"

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
