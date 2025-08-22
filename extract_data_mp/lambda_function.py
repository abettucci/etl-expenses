import json
import requests
import boto3
from datetime import datetime, timedelta
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Funciona para obtener parametro de parameter store de AWS que contiene el access token a la API de Mercado Pago
def auth_mp():
    # Cliente AWS SSM para Parameter Store
    ssm_client = boto3.client("ssm", region_name="us-east-2")
    PARAMETER_NAME = "/mercado_pago/token"

    # Obtener el parámetro desde AWS Parameter Store
    try:
        response = ssm_client.get_parameter(Name=PARAMETER_NAME, WithDecryption=True)
        access_token = response["Parameter"]["Value"]
        return access_token
    except ssm_client.exceptions.ParameterNotFound:
        raise Exception(f"El parámetro {PARAMETER_NAME} no existe en AWS Parameter Store.")

# En caso de que se modifique la frecuencia de creacion automatica de reportes desde MP, leemos esa frecuencia y ajustamos EventBridge
def get_report_frequency(access_token):
    url = "https://api.mercadopago.com/v1/account/settlement_report/config"
    payload = {}
    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + access_token}
    response = requests.request("GET", url, headers=headers, data=payload).json()
    horizonte_temporal = response["type"] #weekly
    fecha_ejecucion = response["value"]
    file_name_prefix = response["file_name_prefix"]
    return horizonte_temporal, fecha_ejecucion, file_name_prefix

# Funcion para obtener una lista de los reportes de Mercado Pago 
def get_reports(access_token):
    url = "https://api.mercadopago.com/v1/account/settlement_report/list"
    payload = {}
    headers = {'Authorization': 'Bearer ' + access_token}
    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()

# Funcion para convertir objeto pdf a dataframe
def format_string_io_to_df(reader):
    # Convertimos a lista de listas
    rows = list(reader)

    # Primer fila es el header
    header = rows[0]

    # El resto son los datos
    data = rows[1:]

    # Creamos el DataFrame
    report_df = pd.DataFrame(data, columns=header)

    return report_df

# Funcion para guardar el reporte de Mercado Pago en un bucket de S3
def save_report_to_s3(report_file_name, access_token, s3_client, bucket_name, key, file_format, report_id=None, report_date=None):
    url = f"https://api.mercadopago.com/v1/account/settlement_report/{report_file_name}"
    payload = {}
    headers = {'Authorization': 'Bearer ' + access_token}
    response = requests.get(url, headers=headers, data=payload)
    response.raise_for_status()

    if file_format.upper() in ('FILE/CSV','CSV'):
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=response.text.encode('utf-8'),
            ContentType='text/csv'
        )
        print(f'Reporte {report_id} de fecha {report_date}, subido a S3')
    elif file_format.upper() in ('FILE/XLSX','XLSX'):
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=response.content,
            ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        print(f'Reporte {report_id} de fecha {report_date}, subido a S3')
    else:
        raise ValueError("Reporte no subido")
    
def format_report_file_name(s3_filename):
    base = s3_filename.rsplit('_', 1)[0]
    extension = s3_filename.split('.')[-1]
    report_file_name = f"{base}.{extension}"

    report_id = s3_filename.rsplit('_', 1)[-1].rsplit('.', 1)[0]

    parts = s3_filename.rsplit('_', 2)
    report_date = parts[-2]

    return report_file_name, report_id, report_date

# (MODIFICADO POR EL WEBHOOK) Funcion que extrae los reportes de la lista de reportes y analiza cual es el ultimo a ingestar en Redshift
def extract_mercado_pago_reports(event): 
    access_token = auth_mp()

    file_name = event['file_name']
    file_url = event['file_url']
    file_type = event['file_type']

    print('file_name: ', file_name)
    print('file_url: ', file_url)
    print('file_type: ', file_type)
    
    s3_client = boto3.client('s3')
    bucket_name = 'mercadopago-reports'
    folder = 'raw/'
    key = f'{folder}{file_name}'
    print(key)
    save_report_to_s3(file_name, access_token, s3_client, bucket_name, key, file_type, '', '')

    return key

# En el event vienen los parametros enviados por la lambda del webhook de MP
def lambda_handler(event, context):
    try:
        key = extract_mercado_pago_reports(event)
        return {
            "key": key
        }
    except Exception as e:
        print("⚠️ Error:", str(e))
        raise Exception(str(e))