import requests
import json
import base64 #formata arquivos para mandar no TB


def lambda_handler(event, context):
    post(event['Records'])
    return '200-OK'

def post(stream):
    token = 'fZxsV9R4bpSVWyLw6CHd'
    for i in stream:
        cidadesPE = i['kinesis']['data']
        cidadesPE = base64.b64decode(cidadesPE).decode('iso-8859-1')
        requests.post('https://thingsboard.cloud/api/v1/'+token+'/telemetry',
                      data = cidadesPE)
