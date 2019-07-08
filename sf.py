import requests
import json

params = {
    "grant_type": "password",
    "client_id": "3MVG9KsVczVNcM8wO8m.fbSJVSWnEtgk5ukFdG65D.NEAVTcCqUuYDgm0FOBCO3b3m5JTR7qnKSXf49K8fF8K",
    "client_secret": "C6DDB888B8C5AF02EBFA87AED78966E168FE8D32B9E41C7CE78A702BE290AB0D",
    "username": "kamlesh.patel@labsapps.com",
    "password": "kam12345"
}

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