import json
import requests
import boto3
import re
import time
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

    report_date_hour = s3_filename.rsplit('_', 1)[-1].rsplit('.', 1)[0]

    parts = s3_filename.rsplit('_', 2)
    report_date = parts[-2]

    return report_file_name, report_date_hour, report_date

def get_report_id(my_file_name, access_token):
    url = "https://api.mercadopago.com/v1/account/settlement_report/list"
    headers = {"Authorization": "Bearer " + access_token}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return None
    else:
        data = response.json()  # Convertimos la respuesta a JSON
        match = next((item for item in data if item.get("file_name") == my_file_name), None)
        if match:
            return str(match.get('id',None)), 'csv'
        else:
            # Probamos con file format '.xlsx' para los casos en los que archivo original era xlsx y lo convertimos a csv para guardarlo en s3
            my_file_name = my_file_name.replace('.csv','.xlsx')
            match = next((item for item in data if item.get("file_name") == my_file_name), None)
            if match:
                return str(match.get('id',None)), 'xlsx'
            else:
                return None, None

def check_file_name_exists_in_api(file_name, access_token):
    url = f"https://api.mercadopago.com/v1/account/settlement_report/{file_name}"
    payload = {}
    headers = {'Authorization': 'Bearer ' + access_token}
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code != 200:
        return 'no'
    else:
        return 'si'

def fix_file_name(report_file_name):
    patron = r"^(settlement-\d+-\d{4}-\d{2}-\d{2}-\d{6})"
    match = re.match(patron, report_file_name)
    if match:
        # Conserva solo la parte principal + .csv
        return f"{match.group(1)}.csv"
    else:
        # Si no cumple el formato esperado, se devuelve sin cambios
        return report_file_name

def run_step_function_sync(sfn_client, step_function_arn, payload, poll_interval=5):
    response = sfn_client.start_execution(
        stateMachineArn=step_function_arn,
        input=json.dumps(payload)
    )
    execution_arn = response['executionArn']
    print(f"▶️ Step Function iniciada: {execution_arn}")

    # Esperar hasta que termine
    while True:
        desc = sfn_client.describe_execution(executionArn=execution_arn)
        status = desc['status']
        
        if status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
            print(f"✅ Step Function finalizó con estado: {status}")
            return status, desc
        else:
            print(f"⏳ Step Function sigue en {status}... esperando {poll_interval}s")
            time.sleep(poll_interval)

def carga_inicial_de_s3(access_token):
    s3_client = boto3.client('s3')
    folder = 'raw/'
    bucket_name = 'mercadopago-reports'

    sfn_client = boto3.client("stepfunctions")
    step_function_arn = 'arn:aws:states:us-east-2:039434644707:stateMachine:mp-report-etl-flow'  

    crawler_name = 'mp-reports-crawler'

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
    keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
    for key in keys:
        print(key)
        file_type = 'csv'
        if key.endswith(".csv"):     
            report_file_name, report_date_hour, report_date = format_report_file_name(key)
            report_file_name = report_file_name[4:]
            file_name_ok = check_file_name_exists_in_api(report_file_name, access_token)            
            if file_name_ok == 'si':
                pass
            else:
                report_file_name = fix_file_name(report_file_name)
            report_id, file_type = get_report_id(report_file_name, access_token)
            report_file_name_sin_extension = report_file_name[:-4]
            report_file_name_final = report_file_name_sin_extension + '.' + file_type
            file_url_base = "https://www.mercadopago.com.ar/balance/reports/settlement/settlement"
            file_url = file_url_base + '-' + '279729559' + '-' + report_id + '/download?format=' + file_type
            payload = {
                "file_name" : report_file_name_final,
                "file_url": file_url,
                "file_type": file_type.upper()
            }

            status, desc = run_step_function_sync(
                sfn_client,
                step_function_arn,
                payload,
                poll_interval=5  # cada 10 segundos chequea
            )
            if status != "SUCCEEDED":
                print(f"⚠️ Ejecución fallida para s3 file {key}: {status}")

            exit()

# Funcion que extrae los reportes de la lista de reportes y analiza cual es el ultimo a ingestar en Redshift
def extract_mercado_pago_reports(event, access_token): 
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
        access_token = auth_mp()
        key = extract_mercado_pago_reports(event, access_token)
        return {
            "key": key
        }
    except Exception as e:
        print("⚠️ Error:", str(e))
        raise Exception(str(e))

# access_token = auth_mp()
# carga_inicial_de_s3(access_token)