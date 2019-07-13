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

'''
def PushDataToEA(orginfo, metadata, dataframe, op, dsname):

    base64encodeofmetaJSON = base64.b64encode(bytes(metadata, 'utf-8'))
    base64encodeofmetaJSON = base64encodeofmetaJSON.decode('utf-8')

    print("step 1.1")

    rec = {}
    rec["Format"] = "Csv"
    rec["EdgemartAlias"] = dsname
    rec["Operation"] = op
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


metadata = open("./MetaDataJson.json", "r").read()
data = open("./data.csv", "r").read()
PushDataToEA(metadata, data, "overwrite", "aatest")

'''