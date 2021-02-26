import requests
import json
from datetime import datetime, timezone,timedelta
import boto3

def filtroCidade(valoresPE):
    cidades = []
    for i in valoresPE:
        cidades.append({'UF': i['UF'], 'DC_NOME': i['DC_NOME'], 'TEM_INS': i['TEM_INS'], 'UMD_INS': i['UMD_INS']})
    return cidades

def filtroPE(obj):
    valoresPE = []
    for i in obj:
        if i['UF'] == 'PE':
            valoresPE.append(i)
    return valoresPE
    
def insertStream(cidadesPE):
    client = boto3.client('kinesis')
    resposta = client.put_record(
        StreamName = 'insertAnalytics',
        Data = json.dumps(cidadesPE),
        PartitionKey = 'insertAnalytics'
    )
    return resposta

def lambda_handler(event, context):
    dataHora = datetime.now()
    fuso_horario = timezone(timedelta(hours=-3))
    dataHora = dataHora.astimezone(fuso_horario)
    dataHora = dataHora.strftime('%Y-%m-%d/%H00')
    request = requests.get('https://apitempo.inmet.gov.br/estacao/dados/'+dataHora)
    obj = json.loads(request.text)
    #filtros
    filtra_PE = filtroPE(obj)
    cidadesPE = filtroCidade(filtra_PE)    
    resposta = insertStream(cidadesPE)
    return {
        'statusCode': resposta,
        'body': cidadesPE
    }


    
