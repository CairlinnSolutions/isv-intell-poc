import requests
import json
import base64

'''
params = {
    "grant_type": "password",
    "client_id": "3MVG9KsVczVNcM8wO8m.fbSJVSWnEtgk5ukFdG65D.NEAVTcCqUuYDgm0FOBCO3b3m5JTR7qnKSXf49K8fF8K",
    "client_secret": "C6DDB888B8C5AF02EBFA87AED78966E168FE8D32B9E41C7CE78A702BE290AB0D",
    "username": "kamlesh.patel@labsapps.com",
    "password": "kam12345"
}
'''

params = {
    "grant_type": "password",
    "client_id": "3MVG9KsVczVNcM8wO8m.fbSJVSWnEtgk5ukFdG65D.NEAVTcCqUuYDgm0FOBCO3b3m5JTR7qnKSXf49K8fF8K",
    "client_secret": "C6DDB888B8C5AF02EBFA87AED78966E168FE8D32B9E41C7CE78A702BE290AB0D",
    "username": "dev@kam.ent",
    "password": "kam123456eJjw0Wo5p192sww5bCFh0353F"
}

def getSFToken():
    r = requests.post(
        "https://login.salesforce.com/services/oauth2/token", params=params)
    d = dict();
    d['token'] = r.json().get("access_token")
    d['instanceUrl'] = r.json().get("instance_url")

    baseurl = "https://" + r.json().get("instance_url")
    starturl = baseurl + "/services/data/v46.0/sobjects/"
    d['extdataurl'] = "/services/data/v46.0/sobjects/InsightsExternalData"
    d['extdataparturl'] = "/services/data/v46.0/sobjects/InsightsExternalDataPart"

    return d

def sf_api_call(orginfo, action, parameters={}, method='get', data={}):

    print(orginfo['instanceUrl']+action)
    print("Correct?")

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
        #print('data= %s' % (data))
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

def getDSInfo():

    orginfo = getSFToken()
    print("after org info");

    data = open("./MetaDataJson.json", "r").read()
    base64encodeofmetaJSON = base64.b64encode(bytes(data, 'utf-8'))
    base64encodeofmetaJSON = base64encodeofmetaJSON.decode('utf-8')

    print("step 1.1")

    rec = {}
    rec["Format"] = "Csv"
    rec["EdgemartAlias"] = "aatest"
    rec["Operation"] = "overwrite"
    rec["Action"] = "none"
    rec["MetadataJson"] = base64encodeofmetaJSON

    q = json.dumps(rec)
    print(q)

    print("step 2")

    res = json.dumps(sf_api_call(orginfo,
        orginfo['extdataurl'], '', 'post', q), indent=2)
    
    print(json.dumps(res))
    ed = json.loads(res)
    print (ed['id'])

    data = open("./data.csv", "r").read()
    base64encodeofds = base64.b64encode(bytes(data, 'utf-8'))
    base64encodeofds = base64encodeofds.decode('utf-8')

    rec = {}
    rec["DataFile"] = base64encodeofds
    rec["InsightsExternalDataId"] = ed['id']
    rec["PartNumber"] = 1

    q = json.dumps(rec)

    res = json.dumps(sf_api_call(orginfo, orginfo['extdataparturl'], '', 'post', q), indent=2)
    print ("Part response")
    print (res)

    extdatauploadurl = orginfo['extdataurl'] + "/" + ed['id']
    rec = {}
    rec["Action"] = "Process"
    q = json.dumps(rec)

    res = json.dumps(sf_api_call(orginfo, extdatauploadurl, '', 'patch', q), indent=2)
    print ("Final response")
    print (res)


getDSInfo()