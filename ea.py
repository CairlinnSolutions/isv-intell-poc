import requests
import json
import base64
from sf import getSFToken, sf_api_call
from io import StringIO

def PushDataToEA(orginfo, metadata, data, op, dsname):

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
    #print(q)

    print("step 2")

    res = json.dumps(sf_api_call(orginfo,
        orginfo['extdataurl'], '', 'post', q), indent=2)
    
    print(json.dumps(res))
    ed = json.loads(res)
    print (ed['id'])
    
    print("step before df to data***")

    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    base64encodeofds = base64.b64encode(bytes(csv_buffer.getvalue(), 'utf-8'))
    base64encodeofds = base64encodeofds.decode('utf-8')

    print("after before df to data***")

    rec = {}
    rec["DataFile"] = base64encodeofds
    rec["InsightsExternalDataId"] = ed['id']
    rec["PartNumber"] = 1

    q = json.dumps(rec)

    res = json.dumps(sf_api_call(orginfo, orginfo['extdataparturl'], '', 'post', q), indent=2)
    print ("Part response")
    #print (res)

    extdatauploadurl = orginfo['extdataurl'] + "/" + ed['id']
    rec = {}
    rec["Action"] = "Process"
    q = json.dumps(rec)

    res = json.dumps(sf_api_call(orginfo, extdatauploadurl, '', 'patch', q), indent=2)
    print ("Final response")
    #print (res)


