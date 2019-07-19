import requests
import json
import base64
from sf import getSFToken, sf_api_call
from io import StringIO

def DoesDatasetExist(orginfo, dsname):
    getres = json.dumps(sf_api_call(orginfo,
        '/services/data/v46.0/wave/datasets', '', 'get', {}), indent=2)
    loaded_json = json.loads(getres)
    for x in loaded_json['datasets']:
        if(x['label'] == dsname):
            return (True)
    return (False)


def PushDataToEA(orginfo, metadata, data, op, dsname):

    print("PushDataToEA called")

    base64encodeofmetaJSON = base64.b64encode(bytes(metadata, 'utf-8'))
    base64encodeofmetaJSON = base64encodeofmetaJSON.decode('utf-8')

    rec = {}
    rec["Format"] = "Csv"
    rec["EdgemartAlias"] = dsname
    rec["Operation"] = op
    rec["Action"] = "none"
    rec["MetadataJson"] = base64encodeofmetaJSON

    q = json.dumps(rec)

    res = json.dumps(sf_api_call(orginfo,
        orginfo['extdataurl'], '', 'post', q), indent=2)
    
    ed = json.loads(res)

    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    base64encodeofds = base64.b64encode(bytes(csv_buffer.getvalue(), 'utf-8'))
    base64encodeofds = base64encodeofds.decode('utf-8')

    rec = {}
    rec["DataFile"] = base64encodeofds
    rec["InsightsExternalDataId"] = ed['id']
    rec["PartNumber"] = 1

    q = json.dumps(rec)

    res = json.dumps(sf_api_call(orginfo, orginfo['extdataparturl'], '', 'post', q), indent=2)
    print ("Part response")

    extdatauploadurl = orginfo['extdataurl'] + "/" + ed['id']
    rec = {}
    rec["Action"] = "Process"
    q = json.dumps(rec)

    res = json.dumps(sf_api_call(orginfo, extdatauploadurl, '', 'patch', q), indent=2)
    print ("PushDataToEA complete!")
