import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import base64
import re
import time
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
import pandas as pd
import os
import io
from io import BytesIO
import requests
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import google.auth.transport.requests
import google.oauth2.id_token
from google.cloud import bigquery
from google.oauth2 import service_account
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

BANK_BUCKET = os.environ['BANK_BUCKET_NAME']
MARKET_BUCKET = os.environ['MARKET_BUCKET_NAME']
MP_TRANSFER_BUCKET = os.environ['MP_TRANSFER_BUCKET_NAME']
BANK_TRANSFER_BUCKET = os.environ.get('BANK_TRANSFER_BUCKET_NAME', BANK_BUCKET)

BANK_STEP_FUNCTION_ARN = os.environ['BANK_STEP_FUNCTION_ARN']
MARKET_STEP_FUNCTION_ARN = os.environ['MARKET_STEP_FUNCTION_ARN']
MP_TRANSFER_STEP_FUNCTION_ARN = os.environ['MP_TRANSFER_STEP_FUNCTION_ARN']
BANK_TRANSFER_STEP_FUNCTION_ARN = os.environ.get('BANK_TRANSFER_STEP_FUNCTION_ARN', BANK_STEP_FUNCTION_ARN)

BANK_EMAIL_SENDER = "mensajesyavisos@mails.santander.com.ar"
BANK_SUBJECTS_PAYMENTS = ["Pagaste", "Aviso de débito automático"]
BANK_SUBJECT_TRANSFER = "Aviso de transferencia"
MARKET_EMAIL_SENDERS = ["atencion_clientes@m.contactocarrefour.com.ar", "contacto@m.tarjetacarrefour.com.ar"]
MARKET_SUBJECT = "Hola, te enviamos el ticket digital de tu compra."
MP_EMAIL_SENDERS = ['info@mercadopago.com']
MP_SUBJECT_TRANSFER = 'Tu transferencia fue enviada' # Pago aprobado en, Pagaste tu tarjeta de crédito
MP_SUBJECT_REPORT = 'Ya podés conciliar todas tus transacciones'

# Senders/subjects que deben IGNORARSE para evitar loops infinitos
# (emails de error, notificaciones del sistema, etc.)
SKIP_SENDERS = [
    'no-reply@sns.amazonaws.com',
    'notifications@amazonaws.com',
    'noreply@',
    'mailer-daemon@',
    'postmaster@',
    'bounce@',
    'aws-notifications',
    'alerts@',
]
SKIP_SUBJECTS = [
    'Fallo en proceso ETL',
    'ETL Error',
    'Lambda Error',
    'Step Function Failed',
    'Delivery Status Notification',
    'Undeliverable:',
    'Mail delivery failed',
    'Exceeded rate limits',
    'AWS Notification',
    'Alarm:',
]

# bank_body_contains = ["Te acercamos el detalle de tu consumo con la Tarjeta Santander", "Te acercamos el detalle del débito con tu Tarjeta Santander"]

# Funcion para obtener la API Key de Google Cloud y consumir la API de Gmail
def get_secret(SECRET_NAME, REGION_NAME):
    client = boto3.client('secretsmanager', region_name=REGION_NAME)
    response = client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(response['SecretString'])

def update_secret(updated_token_json, SECRET_NAME, REGION_NAME):
    client = boto3.client('secretsmanager', region_name=REGION_NAME)
    client.update_secret(
        SecretId=SECRET_NAME,
        SecretString=updated_token_json
    )

def auth_google(SECRET_NAME):
    SCOPES = ["https://www.googleapis.com/auth/gmail.readonly", 
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery"]
    REGION_NAME = 'us-east-2'    
    token_info = get_secret(SECRET_NAME, REGION_NAME)
    creds = Credentials.from_authorized_user_info(token_info, SCOPES)
    
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        print("♻️ Token refrescado")

        # Guardar el token actualizado en Secrets Manager
        update_secret(creds.to_json(), SECRET_NAME, REGION_NAME)

    return creds

def get_bigquery_client():
    """Obtener cliente de BigQuery autenticado con Service Account"""
    try:
        SECRET_NAME = "gcp_sa_api_credentials" #"gcp_service_account"
        REGION_NAME = "us-east-2"
        
        credentials_json = get_secret(SECRET_NAME, REGION_NAME)
        credentials = service_account.Credentials.from_service_account_info(credentials_json)
        
        # project_id = os.environ.get('GCP_PROJECT_ID')
        project_id = 'hazel-pillar-400222'
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        print(f"✅ Cliente de BigQuery autenticado para proyecto: {project_id}")
        return client
    except Exception as e:
        print(f"❌ Error al autenticar con BigQuery: {e}")
        raise

def find_html_part(payload, depth=0, max_depth=10):
    # Prevenir recursión infinita
    if depth > max_depth:
        print(f"⚠️ Máxima profundidad de recursión alcanzada ({max_depth})")
        return None
        
    if payload.get("mimeType") == "text/html":
        return payload["body"].get("data", None)
    elif "parts" in payload:
        for part in payload["parts"]:
            result = find_html_part(part, depth + 1, max_depth)
            if result:
                return result
    return None

def get_message_ids_loaded_in_bigquery(bq_client, table_name, pk):
    """Obtener IDs de mensajes ya cargados en BigQuery"""
    try:
        project_id = os.environ.get('GCP_PROJECT_ID', 'hazel-pillar-400222')
        dataset = os.environ.get('BQ_DATASET_PROD', 'PRD')
        
        query = f"""
        SELECT DISTINCT {pk}
        FROM `{project_id}.{dataset}.{table_name}`
        """
        
        print(f"🔍 Consultando IDs existentes en BigQuery: {project_id}.{dataset}.{table_name}")
        
        query_job = bq_client.query(query)
        results = query_job.result()
        
        ids_existentes = {row[pk] for row in results}
        print(f"✅ Se encontraron {len(ids_existentes)} IDs existentes en BigQuery")
        
        return ids_existentes
        
    except Exception as e:
        error_msg = str(e)
        if "Not found: Table" in error_msg or "404" in error_msg:
            print(f"⚠️ Tabla {table_name} no existe en BigQuery, devolvemos conjunto vacío.")
            return set()
        else:
            print(f"❌ Error al consultar BigQuery: {e}")
            return set()

def get_last_message_loaded(bq_client):
    """Obtener la última fecha de mensaje cargado en BigQuery"""
    try:
        project_id = os.environ.get('GCP_PROJECT_ID')
        dataset = os.environ.get('BQ_DATASET_PROD', 'PRD')
        
        query = f"""
        SELECT MAX(
            PARSE_DATE('%d/%m/%Y',
                CASE 
                    WHEN LENGTH(SPLIT(fecha_pago, '/')[OFFSET(2)]) = 2 THEN 
                        CONCAT(
                            SPLIT(fecha_pago, '/')[OFFSET(0)], '/',
                            SPLIT(fecha_pago, '/')[OFFSET(1)], '/',
                            '20', SPLIT(fecha_pago, '/')[OFFSET(2)]
                        )
                    ELSE fecha_pago
                END
            )
        ) AS max_date 
        FROM `{project_id}.{dataset}.bank_payments`
        """
        
        print(f"🔍 Consultando última fecha en BigQuery: {project_id}.{dataset}.bank_payments")
        
        query_job = bq_client.query(query)
        results = query_job.result()
        
        fecha_ultimo_payment_cargado = None
        for row in results:
            if row['max_date']:
                fecha_ultimo_payment_cargado = row['max_date']
                # Sumar un día para buscar desde el siguiente
                fecha_ultimo_payment_cargado = datetime.combine(
                    fecha_ultimo_payment_cargado, datetime.min.time()
                ) + timedelta(days=1)
                break
        
        if fecha_ultimo_payment_cargado is None:
            date_str = '2024/10/01'
            print(f"⚠️ No se encontró fecha máxima, usando fecha por defecto: {date_str}")
        else:
            date_str = fecha_ultimo_payment_cargado.strftime('%Y/%m/%d')
            print(f"✅ Última fecha encontrada: {date_str}")

        return date_str
        
    except Exception as e:
        error_msg = str(e)
        if "Not found: Table" in error_msg or "404" in error_msg:
            print(f"⚠️ Tabla bank_payments no existe en BigQuery, usando fecha por defecto")
        else:
            print(f"❌ Error al consultar BigQuery: {e}")
        return '2024/10/01'

def process_email(message_id, gmail_service):
    """Procesar email completo desde Gmail API"""
    try:
        message = gmail_service.users().messages().get(
            userId='me', id=message_id, format='full'
        ).execute()
        
        payload = message['payload']
        html_encoded = find_html_part(payload)
        
        if html_encoded:
            html_data = base64.urlsafe_b64decode(html_encoded).decode('utf-8', errors='replace')
            body_text = BeautifulSoup(html_data, 'html.parser').get_text()
        else:
            html_data = None
            body_text = ""
        
        # Extraer headers
        headers = {h['name']: h['value'] for h in payload['headers']}
        subject = headers.get('Subject', '')
        from_email = headers.get('From', '')
        
        mail_data = {
            "message_id": message_id,
            "date": datetime.fromtimestamp(int(message['internalDate']) / 1000).isoformat(),
            "sender": from_email,
            "subject": subject,
            "html_body": html_data,
            "raw_text": body_text,
            "headers": headers
        }
        
        return mail_data     
    except Exception as e:
        print(f"Error procesando mensaje {message_id}: {e}")
        return None    

# Funcion para extraer los PDFs especificos de Gmail
def download_pdf_from_email_urls(mail_data, sender_email, bucket_name, folder, s3_client):
    raw_date = mail_data["date"]
    try:
        # Ejemplo: 2025-09-18T10:45:10  o  2025-09-18
        parsed_date = datetime.fromisoformat(raw_date.replace("Z", ""))
    except ValueError:
        # fallback si no viene en formato ISO estándar
        parsed_date = datetime.strptime(raw_date[:10], "%Y-%m-%d")
    date = parsed_date.strftime("%d-%m-%y")
    print('Analizando mail de fecha: ', date)

    filename = f'Ticket_{date}.pdf'
    s3_key = f'{folder}{filename}'
    soup = BeautifulSoup(mail_data["html_body"], 'html.parser')

    if soup == "":
        print(f"⚠️ No HTML content found in email for date {date}, skipping link extraction.")
        return s3_key

    if sender_email == "atencion_clientes@m.contactocarrefour.com.ar":
        links = [a['href'] for a in soup.find_all('a', href=True) if 'https://m.contactocarrefour.com.ar/x/c/' in a['href']]
    else:
        links = [a['href'] for a in soup.find_all('a', href=True) if 'https://m.tarjetacarrefour.com.ar/x/c/' in a['href']]
    
    for url in links:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers)
            if response.content[:4] == b'%PDF' and len(response.content) > 1024 :
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                    print("⚠️ El archivo ya existe en S3, se omite la subida.")
                except s3_client.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        # Subir archivo PDF a S3
                        s3_client.upload_fileobj(BytesIO(response.content), bucket_name, s3_key)
                        print(f"✅ Archivo subido a S3: {s3_key}")
            else:
                print(f"⚠️ Archivo inválido desde URL: {url}")
        except Exception as e:
            print(f"❌ Error al descargar desde URL {url}: {e}")

    return s3_key

def dispatch_processor(mail_data, folder, MARKET_BUCKET, BANK_BUCKET, s3_client, sender, subject):
    """Dispatch basado en subject y sender"""

    # Transferencias bancarias Santander (Aviso de transferencia)
    if (BANK_EMAIL_SENDER in sender and BANK_SUBJECT_TRANSFER in subject):
        print('Descargando la info del mail de transferencia bancaria Santander...')
        s3_key = f"{folder}{mail_data['date'][:10]}-{mail_data['message_id']}.json"
        s3_client.put_object(
            Body=json.dumps(mail_data),
            Bucket=BANK_TRANSFER_BUCKET,
            Key=s3_key
        )        
        print(f"✅ Archivo subido a S3: {s3_key}")

        return {
            "statusCode": 200,
            "body": {
                "key": s3_key,
                "process": True,
                "etl_flow": "BANK_TRANSFER",
                "bucket": BANK_TRANSFER_BUCKET
            }
        }

    # Pagos con tarjeta Santander (Pagaste, Aviso de débito automático)
    elif (BANK_EMAIL_SENDER in sender and any(keyword in subject for keyword in BANK_SUBJECTS_PAYMENTS)):
        print('Descargando la info del mail del gasto de santander')
        s3_key = f"{folder}{mail_data['date'][:10]}-{mail_data['message_id']}.json"
        s3_client.put_object(
            Body=json.dumps(mail_data),
            Bucket=BANK_BUCKET,
            Key=s3_key
        )        
        print(f"✅ Archivo subido a S3: {s3_key}")

        return {
            "statusCode": 200,
            "body": {
                "key": s3_key,
                "process": True,
                "etl_flow": "BANK"
            }
        }

    elif (sender in MARKET_EMAIL_SENDERS and MARKET_SUBJECT in subject):
        print('Descargando el pdf del mail de carrefour...')
        s3_key = download_pdf_from_email_urls(mail_data, sender, MARKET_BUCKET, folder, s3_client)
        print(f"✅ Archivo subido a S3: {s3_key}")

        return  {
            "statusCode": 200,
            "body": {
                "key": s3_key,
                "process": True,
                "etl_flow": "TICKET"
            }
        }

    elif (sender in MP_EMAIL_SENDERS and MP_SUBJECT_TRANSFER in subject):
        print('Descargando la info del mail de transferencia de Mercado Pago...')
        s3_key = f"{folder}{mail_data['date'][:10]}-{mail_data['message_id']}.json"
        s3_client.put_object(
            Body=json.dumps(mail_data),
            Bucket=MP_TRANSFER_BUCKET,
            Key=s3_key
        )        
        print(f"✅ Archivo subido a S3: {s3_key}")

        return  {
            "statusCode": 200,
            "body": {
                "key": s3_key,
                "process": True,
                "etl_flow": "MP_TRANSFER"
            }
        }

    else:
        print(f"Email no manejado - Subject: {subject}, From: {sender}")
        return {"process": False, "reason": "Evento descartado por filtros"}

def load_last_history_id(dynamo_table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamo_table_name)

    try:
        resp = table.get_item(Key={"PK": "gmail_last_history_id"})
        return resp.get("Item", {}).get("historyId")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFoundException':
            print(f"⚠️ Tabla '{dynamo_table_name}' no existe. Primera ejecución.")
        else:
            print(f"❌ Error de DynamoDB: {e}")
        return None
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return None

def save_last_history_id_in_dynamo(table, history_id):
    table.put_item(Item={
        "PK": "gmail_last_history_id",
        "historyId": str(history_id)
    })

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

def reproceso_historico(table_name):
    try:
        creds = auth_google('gcp_api_credentials')
        gmail_service = build('gmail', 'v1', credentials=creds)
        sfn_client = boto3.client("stepfunctions")
        dynamodb = boto3.resource('dynamodb')
        dynamo_table_name = "gmail-history-tracker"
        bq_client = get_bigquery_client()
        s3_client = boto3.client('s3')
        folder = 'raw/'

        if table_name == 'carrefour_data':
            labels = ['Avisos Compra Carrefour']
            crawler_name = 'market-tickets-crawler'
        elif table_name == 'bank_payments':
            labels = ['Avisos Gastos Santander']
            crawler_name = 'bank-payments-crawler'
        else: # Transferencia MP
            labels = ['Aviso Transferencia MP']
            crawler_name = 'bank-payments-crawler'

        results = gmail_service.users().labels().list(userId="me").execute()
        for label in results['labels']:
            if label['name'] in labels: #'Avisos Gastos Santander'
      
                results = gmail_service.users().messages().list(
                    userId="me",
                    labelIds=[label['id']]
                ).execute()

                messages = results.get("messages", [])
                print(f"🔎 Encontrados {len(messages)} mails históricos con etiqueta {label}")

                for m in messages:
                    msg_id = m["id"]
                    mail_data = process_email(msg_id, gmail_service)
                    if not mail_data:
                        continue
                    
                    match = re.search(r"<([^>]+)>", mail_data['sender'])
                    if match:
                        sender = match.group(1)
                    subject = mail_data['subject']
                    date = mail_data['date']

                    print('sender: ', sender)
                    print('subject: ', subject)
                    print('date: ', date)

                    table_name, pk = None, None
                    if (BANK_EMAIL_SENDER in sender and BANK_SUBJECT_TRANSFER in subject):
                        table_name = 'bank_transfers'
                        pk = 'nro_comprobante'
                    elif (BANK_EMAIL_SENDER in sender and any(keyword in subject for keyword in BANK_SUBJECTS_PAYMENTS)):
                        table_name = 'bank_payments'
                        pk = 'MESSAGE_ID'
                    elif (sender in MARKET_EMAIL_SENDERS and MARKET_SUBJECT in subject):
                        table_name = 'carrefour_data'
                        pk = 'nro_ticket'
                    elif (sender in MP_EMAIL_SENDERS and MP_SUBJECT_REPORT in subject):
                        table_name = 'mp_data'
                        pk = 'REPORT_ID'
                    elif (sender in MP_EMAIL_SENDERS and MP_SUBJECT_TRANSFER in subject):
                        table_name = 'mp_transfer_data'
                        pk = 'message_id'    
                    else:
                        print(f'Email ignorado (no cumple filtros): {sender} - {subject}')
                        continue
                                                    
                    print('table_name: ', table_name)
                    print('pk : ', pk)

                    ids_existentes = get_message_ids_loaded_in_bigquery(bq_client, table_name, pk)

                    print('mail_msg_id: ', msg_id)
                    print('ids_existentes en BigQuery: ', ids_existentes)
                    
                    if msg_id not in ids_existentes:
                        print('Intentamos extraer los datos del mail y cargarlos a S3')
                        response = dispatch_processor(mail_data, folder, MARKET_BUCKET, BANK_BUCKET, s3_client, sender, subject)              
                    else:
                        print(f"⚠️ Mensaje {msg_id} ya existe en BigQuery, se omite procesamiento")
                        continue

                    save_last_history_id_in_dynamo(dynamodb.Table("gmail-history-tracker"), '') #el history_id lo dejamos vacio porque no tenemos ese dato

                    # Parámetros para la Step Function: el bloque de Transform espera un "key" y "process=true"
                    payload = response
                    print(payload)
                    
                    # Si dispatch_processor retornó process=False, no ejecutar Step Function
                    should_process = response.get('process') if isinstance(response, dict) else False
                    if isinstance(response, dict) and 'body' in response and isinstance(response['body'], dict):
                        should_process = response['body'].get('process', False)
                    
                    if not should_process:
                        print(f"⏭️ Mensaje {msg_id} no requiere procesamiento por Step Function (dispatch_processor retornó process=False)")
                        continue

                    # Seleccionar Step Function basado en etl_flow del dispatch_processor
                    etl_flow = response.get('body', {}).get('etl_flow', '')
                    
                    if etl_flow == 'BANK':
                        step_function_arn = BANK_STEP_FUNCTION_ARN
                    elif etl_flow == 'BANK_TRANSFER':
                        step_function_arn = BANK_TRANSFER_STEP_FUNCTION_ARN
                    elif etl_flow == 'TICKET':
                        step_function_arn = MARKET_STEP_FUNCTION_ARN
                    elif etl_flow == 'MP_TRANSFER':
                        step_function_arn = MP_TRANSFER_STEP_FUNCTION_ARN
                    else:
                        print(f"etl_flow '{etl_flow}' no reconocido - continuamos con el siguiente mail")
                        continue
                    
                    status, desc = run_step_function_sync(
                        sfn_client,
                        step_function_arn,
                        payload,
                        poll_interval=10  # cada 10 segundos chequea
                    )

                    if status != "SUCCEEDED":
                        print(f"⚠️ Ejecución fallida para mail {msg_id}: {status}")
                                  
    except Exception as e:
        print("⚠️ Error:", str(e))
        raise Exception(str(e))

def carga_inicial_desde_s3(table_name):
    s3_client = boto3.client('s3')
    sfn_client = boto3.client("stepfunctions")
    folder = 'raw/'

    if table_name == 'carrefour_data':
        step_function_arn = MARKET_STEP_FUNCTION_ARN
        bucket_name = 'market-tickets'
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
        keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.pdf')]
        for key in keys:
            print(key)
            if key.endswith(".pdf"):     
                payload = {
                    "statusCode": 200,
                    "body": {
                        "key": key,
                        "process": True
                    }
                }
                status, desc = run_step_function_sync(
                    sfn_client,
                    step_function_arn,
                    payload,
                    poll_interval=5  # cada 10 segundos chequea
                )
                if status != "SUCCEEDED":
                    print(f"⚠️ Ejecución fallida para s3 file {key}: {status}")
    else: #bank_payments
        step_function_arn = BANK_STEP_FUNCTION_ARN
        bucket_name = 'bank-payments'
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
        keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]
        for key in keys:
            print(key)
            if key.endswith(".json"):     
                payload = {
                    "statusCode": 200,
                    "body": {
                        "key": key,
                        "process": True
                    }
                }
                status, desc = run_step_function_sync(
                    sfn_client,
                    step_function_arn,
                    payload,
                    poll_interval=5  # cada 10 segundos chequea
                )
                if status != "SUCCEEDED":
                    print(f"⚠️ Ejecución fallida para s3 file {key}: {status}")
    
def lambda_handler(event, context):
    print("📨 Event received:", json.dumps(event))
    
    headers = event.get("headers", {})
    auth_header = headers.get("authorization") or headers.get("Authorization")
    
    if not auth_header:
        print("❌ No authorization header")
        return {"statusCode": 401, "body": "Missing Authorization header"}

    # Extraer token
    token_parts = auth_header.split(" ")
    if len(token_parts) != 2 or token_parts[0].lower() != "bearer":
        print("❌ Invalid authorization format")
        return {"statusCode": 401, "body": "Invalid Authorization format"}
    
    token = token_parts[1]
    
    # Verificar token - IMPORTANTE: usar la URL correcta
    request_adapter = google.auth.transport.requests.Request()
    
    try:
        # Obtener el dominio y path completo del evento
        domain = event.get("requestContext", {}).get("domainName", "")
        stage = event.get("requestContext", {}).get("stage", "")
        path = event.get("requestContext", {}).get("path", "")
        
        # Construir audience dinámicamente
        if domain:
            # Si tenemos dominio, construir URL completa
            audience = f"https://{domain}{path}"
            print(f"🔍 Audience construido dinámicamente: {audience}")
        else:
            # Fallback al valor por defecto (sin stage)
            audience = "https://gyu5e47m41.execute-api.us-east-2.amazonaws.com/prod/market_pdf"
            print(f"⚠️ Usando audience por defecto: {audience}")
        
        id_info = google.oauth2.id_token.verify_oauth2_token(
            token,
            request_adapter,
            audience=audience
        )
        print(f"✅ Valid token from: {id_info.get('email', 'Unknown')}")
        print(f"✅ Token audience validated: {id_info.get('aud')}")
        
    except ValueError as e:
        print(f"❌ Token validation failed: {e}")
        print(f"   Expected audience: {audience if 'audience' in locals() else 'N/A'}")
        return {"statusCode": 403, "body": json.dumps({"error": "Forbidden - Invalid token", "details": str(e)})}
    except Exception as e:
        print(f"❌ Unexpected error during token validation: {e}")
        return {"statusCode": 403, "body": json.dumps({"error": "Forbidden", "details": str(e)})}

    try:
        body_message_pubsub = json.loads(event.get("body", "{}"))
        print(f"Body del mensaje Pub/Sub: {body_message_pubsub}")
        
        # =========================================
        # DEDUPLICACIÓN A NIVEL DE PUB/SUB MESSAGE
        # =========================================
        # Evita procesar el mismo mensaje Pub/Sub múltiples veces
        # (puede llegar por múltiples subscriptions o reintentos)
        pubsub_message_id = body_message_pubsub.get('message', {}).get('messageId') or body_message_pubsub.get('message', {}).get('message_id')
        if pubsub_message_id:
            dynamodb_dedup = boto3.resource('dynamodb')
            dedup_table = dynamodb_dedup.Table('gmail-pubsub-dedup')
            
            try:
                # Intentar insertar con ConditionExpression para atomicidad
                dedup_table.put_item(
                    Item={
                        'pubsub_message_id': pubsub_message_id,
                        'processed_at': int(time.time()),
                        'ttl': int(time.time()) + 86400  # TTL de 1 día
                    },
                    ConditionExpression='attribute_not_exists(pubsub_message_id)'
                )
                print(f"🔒 Lock adquirido para Pub/Sub message {pubsub_message_id}")
            except dynamodb_dedup.meta.client.exceptions.ConditionalCheckFailedException:
                print(f"⚠️ Pub/Sub message {pubsub_message_id} ya fue procesado, ignorando duplicado")
                return {'statusCode': 200, 'body': json.dumps({'message': 'Duplicate Pub/Sub message ignored'})}
            except Exception as dedup_error:
                # Si la tabla no existe o hay otro error, continuar (no bloquear el procesamiento)
                print(f"⚠️ Error en deduplicación Pub/Sub (continuando): {dedup_error}")

        creds = auth_google('gcp_api_credentials')
        dynamodb = boto3.resource('dynamodb')
        dynamo_table_name = "gmail-history-tracker"
        sfn_client = boto3.client("stepfunctions")
        gmail_service = build('gmail', 'v1', credentials=creds)
        bq_client = get_bigquery_client()
        s3_client = boto3.client('s3')
        folder = 'raw/'

        # Construir mapa de labels para referencia
        results = gmail_service.users().labels().list(userId="me").execute()
        label_map = {}
        target_label_names = ['Avisos Gastos Santander', 'Avisos Compra Carrefour', 'Aviso Transferencia MP', 'Aviso Transferencia Santander']
        target_label_ids = []
        
        for label in results['labels']:
            label_map[label['id']] = label['name']
            if label['name'] in target_label_names:
                target_label_ids.append(label['id'])

        print(f"\n🔍 Labels objetivo: {target_label_names}")
        print(f"🔍 Label IDs objetivo: {target_label_ids}")
        print(f"🔍 Total de labels en Gmail: {len(label_map)}")

        print('Labels: ', list(label_map.values()))

        if 'message' in body_message_pubsub:
            message = body_message_pubsub['message']       

            print("\n📩 Message: ", message)     
            message_data = json.loads(base64.b64decode(message['data']).decode('utf-8'))

            print("📩 Data decodificada: ", message_data)

            history_id = message_data.get('historyId')
            if not history_id:
                print("⚠️ No se encontró historyId en el evento")
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': 'No historyId en el mensaje'})
                }

            print(f'📍 History ID recibido de Pub/Sub: {history_id}')

            saved_history_id = load_last_history_id(dynamo_table_name)
            if saved_history_id:
                last_history_id = saved_history_id
            else:
                last_history_id = str(int(history_id) - 1)

            print(f"📍 History ID guardado en DynamoDB: {last_history_id}")
            print(f"📍 Diferencia: {int(history_id) - int(last_history_id)} cambios")
            print(f"🔍 CONSULTANDO HISTORIAL DESDE {last_history_id} HASTA {history_id}")
    
            try:
                # CAMBIO IMPORTANTE: Consultar historial UNA SOLA VEZ sin filtro de label
                # para ver TODOS los cambios y luego filtrar por labels en el código
                history = gmail_service.users().history().list(
                    userId='me',
                    startHistoryId=last_history_id,
                    # NO usar labelId aquí - procesamos todos los cambios y filtramos después
                ).execute()

                # Log del historial completo
                history_records = history.get('history', [])

                # print('🔍 DEBUG - history_records completos: ', json.dumps(history_records, indent=2, default=str))
                print(f"📊 Se obtuvieron {len(history_records)} records del historial")
                
                if len(history_records) == 0:
                    print(f"\n⚠️ ⚠️ ⚠️  NO HAY CAMBIOS NUEVOS DESDE historyId={last_history_id} ⚠️ ⚠️ ⚠️")
                    
                    # Guardar el historyId para evitar reprocesar
                    save_last_history_id_in_dynamo(dynamodb.Table("gmail-history-tracker"), history_id)
                    
                    return {
                        'statusCode': 200,
                        'body': json.dumps({'message': 'No hay cambios nuevos'})
                    }
                
                # Set GLOBAL para evitar procesar el mismo mensaje múltiples veces
                # (puede aparecer en múltiples records del historial)
                processed_message_ids = set()
                
                # Ahora procesar cada record del historial
                for record in history_records:
                    
                    print('🔍 DEBUG - record completo: ', json.dumps(record, indent=2, default=str))

                    record_history_id = str(record.get('id'))  # HistoryId de este record
                    print(f"🔄 Procesando record con historyId={record_history_id}")
                    
                    # DEBUG: Analizar TODOS los tipos de cambios en el record
                    print(f"🔍 DEBUG - Tipos de cambios en este record:")
                    print(f"   - messagesAdded: {len(record.get('messagesAdded', []))}")
                    print(f"   - messagesDeleted: {len(record.get('messagesDeleted', []))}")
                    print(f"   - labelsAdded: {len(record.get('labelsAdded', []))}")
                    print(f"   - labelsRemoved: {len(record.get('labelsRemoved', []))}")
                    print(f"   - messages: {len(record.get('messages', []))}")
                        
                    # PASO 1: Procesar mensajes nuevos (messagesAdded)
                    if 'messagesAdded' in record:
                        print(f"📧 Record {record_history_id} tiene {len(record['messagesAdded'])} mensajes agregados")
                        for m in record['messagesAdded']:
                            mail_msg_id = m['message']['id']
                            
                            # Verificar si ya procesamos este mensaje en un record anterior
                            if mail_msg_id in processed_message_ids:
                                print(f"⚠️ Mensaje {mail_msg_id} ya fue procesado en un record anterior, se omite")
                                continue
                            
                            processed_message_ids.add(mail_msg_id)
                            
                            # DEBUGGING: Obtener información completa del mensaje para logging
                            print(f"🔍 DEBUG - Analizando mensaje {mail_msg_id}")
                            
                            try:
                                msg = gmail_service.users().messages().get(
                                    userId="me", id=mail_msg_id, format="metadata"
                                ).execute()
                            except Exception as e:
                                if "404" in str(e) or "notFound" in str(e):
                                    print(f"⚠️ Mensaje {mail_msg_id} ya no existe (fue eliminado). Continuando con el siguiente...")
                                    continue
                                else:
                                    raise

                            labels = msg.get("labelIds", [])
                            labels_names = [label_map.get(lid, lid) for lid in labels]
                            print(f"🏷️  Labels del mensaje: {labels_names}")
                            
                            # DEBUGGING: Extraer y loggear información del email SIN importar el label
                            mail_data_debug = process_email(mail_msg_id, gmail_service)
                            if mail_data_debug:
                                sender_debug = mail_data_debug.get('sender', 'N/A')
                                match = re.search(r"<([^>]+)>", sender_debug)
                                if match:
                                    sender_debug = match.group(1)
                                
                                print(f"📧 SENDER: {sender_debug}")
                                print(f"📧 SUBJECT: {mail_data_debug.get('subject', 'N/A')}")
                                print(f"📧 DATE: {mail_data_debug.get('date', 'N/A')}")
                                
                                # FILTRO ANTI-LOOP: Ignorar emails de error/notificación del sistema
                                subject_debug = mail_data_debug.get('subject', '')
                                sender_lower = sender_debug.lower()
                                
                                # Verificar si es un email que debe ignorarse
                                should_skip = False
                                skip_reason = ""
                                
                                for skip_sender in SKIP_SENDERS:
                                    if skip_sender.lower() in sender_lower:
                                        should_skip = True
                                        skip_reason = f"Sender matches skip pattern: {skip_sender}"
                                        break
                                
                                if not should_skip:
                                    for skip_subject in SKIP_SUBJECTS:
                                        if skip_subject.lower() in subject_debug.lower():
                                            should_skip = True
                                            skip_reason = f"Subject matches skip pattern: {skip_subject}"
                                            break
                                
                                if should_skip:
                                    print(f"⏭️ SKIP: Email de sistema/error detectado. Razón: {skip_reason}")
                                    print(f"⏭️ Ignorando mensaje {mail_msg_id} para evitar loop infinito")
                                    continue
                                
                                # Loggear preview del body (primeros 500 caracteres)
                                body_preview = mail_data_debug.get('raw_text', '')[:500]
                                print(f"📧 BODY PREVIEW (primeros 500 chars):\n{body_preview}")
                            
                            # CAMBIO: Verificar si el mensaje tiene ALGUNO de los labels objetivo
                            has_target_label = any(lid in target_label_ids for lid in labels)
                            
                            if not has_target_label:
                                print(f"⚠️ Mensaje {mail_msg_id} ignorado - no tiene ninguno de los labels objetivo")
                                print(f"   Labels objetivo: {target_label_names}")
                                print(f"   Labels del mensaje: {labels_names}")
                                continue

                            print(f"✅ Mensaje {mail_msg_id} será procesado - tiene labels objetivo: {[n for n in labels_names if n in target_label_names]}")
                            
                            # Reusar mail_data_debug si ya lo tenemos, sino procesarlo
                            mail_data = mail_data_debug if mail_data_debug else process_email(mail_msg_id, gmail_service)
                            
                            if not mail_data:
                                print(f"❌ Error procesando email {mail_msg_id}")
                                continue
                            
                            # Extraer sender
                            sender = mail_data['sender']
                            match = re.search(r"<([^>]+)>", sender)
                            if match:
                                sender = match.group(1)
                            
                            subject = mail_data['subject']
                            date = mail_data['date']

                            print(f'✉️  Procesando: sender={sender}, subject={subject}, date={date}')

                            # Determinar tabla y PK basado en sender/subject
                            table_name, pk = None, None
                            if (BANK_EMAIL_SENDER in sender and BANK_SUBJECT_TRANSFER in subject):
                                table_name = 'bank_transfers'
                                pk = 'nro_comprobante'
                            elif (BANK_EMAIL_SENDER in sender and any(keyword in subject for keyword in BANK_SUBJECTS_PAYMENTS)):
                                table_name = 'bank_payments'
                                pk = 'MESSAGE_ID'
                            elif (sender in MARKET_EMAIL_SENDERS and MARKET_SUBJECT in subject):
                                table_name = 'carrefour_data'
                                pk = 'nro_ticket'
                            elif (sender in MP_EMAIL_SENDERS and MP_SUBJECT_REPORT in subject):
                                table_name = 'mp_data'
                                pk = 'REPORT_ID'
                            elif (sender in MP_EMAIL_SENDERS and MP_SUBJECT_TRANSFER in subject):
                                table_name = 'mp_transfer_data'
                                pk = 'message_id'    
                            else:
                                print(f'⚠️  Email ignorado (no cumple filtros de sender/subject): {sender} - {subject}')
                                continue
                                                            
                            print(f'📊 table_name={table_name}, pk={pk}')

                            # Verificar si ya existe en BigQuery
                            ids_existentes = get_message_ids_loaded_in_bigquery(bq_client, table_name, pk)
                            
                            if mail_msg_id in ids_existentes:
                                print(f"⚠️ Mensaje {mail_msg_id} ya existe en BigQuery, se omite procesamiento")
                                continue
                            
                            # Procesar y subir a S3
                            print(f'💾 Extrayendo datos del mail y cargando a S3...')
                            response = dispatch_processor(mail_data, folder, MARKET_BUCKET, BANK_BUCKET, s3_client, sender, subject)    
                            print(f"✅ Mensaje procesado exitosamente: {mail_msg_id}")
                            
                            # Verificar si dispatch_processor marcó el mensaje para procesar
                            payload = response
                            print(f'🚀 Payload para Step Function: {payload}')
                            
                            # Si dispatch_processor retornó process=False, no ejecutar Step Function
                            should_process = response.get('process') if isinstance(response, dict) else False
                            if isinstance(response, dict) and 'body' in response and isinstance(response['body'], dict):
                                should_process = response['body'].get('process', False)
                            
                            if not should_process:
                                print(f"⏭️ Mensaje {mail_msg_id} no requiere procesamiento por Step Function (dispatch_processor retornó process=False)")
                                continue

                            if 'Avisos Gastos Santander' in labels_names:
                                step_function_arn = BANK_STEP_FUNCTION_ARN 
                            elif 'Avisos Compra Carrefour' in labels_names:
                                step_function_arn = MARKET_STEP_FUNCTION_ARN
                            elif  'Aviso Transferencia MP' in labels_names:
                                step_function_arn = MP_TRANSFER_STEP_FUNCTION_ARN
                            else:
                                print(f"⚠️ No se encontró Step Function para labels: {labels_names}")
                                continue
                            
                            status, desc = run_step_function_sync(
                                sfn_client,
                                step_function_arn,
                                payload,
                                poll_interval=10
                            )

                            if status != "SUCCEEDED":
                                print(f"⚠️ Ejecución fallida para mail {mail_msg_id}: {status}")
                            else:
                                print(f"✅ Step Function completada exitosamente para {mail_msg_id}")
                    
                    # PASO 2: Procesar labels agregados (labelsAdded)
                    # CRÍTICO: Esto captura emails que llegaron en un historyId anterior pero se les
                    # agregó el label objetivo en este historyId
                    if 'labelsAdded' in record:
                        print(f"\n🏷️  Record {record_history_id} tiene {len(record['labelsAdded'])} labels agregados")
                        for label_change in record['labelsAdded']:
                            mail_msg_id = label_change['message']['id']
                            labels_added = label_change.get('labelIds', [])
                            
                            # Verificar si alguno de los labels agregados es de nuestro interés
                            added_target_labels = [lid for lid in labels_added if lid in target_label_ids]
                            
                            if not added_target_labels:
                                print(f"⚠️ Labels agregados al mensaje {mail_msg_id} no son de interés: {labels_added}")
                                continue
                            
                            if mail_msg_id in processed_message_ids:
                                print(f"⚠️ Mensaje {mail_msg_id} ya fue procesado anteriormente, se omite")
                                continue
                            
                            processed_message_ids.add(mail_msg_id)
                            
                            print(f"🏷️  DEBUG - Mensaje {mail_msg_id} recibió labels objetivo: {added_target_labels}")
                            print(f"💡 Este mensaje probablemente llegó en un historyId anterior")
                            
                            # Obtener el mensaje completo
                            try:
                                msg = gmail_service.users().messages().get(
                                    userId="me", id=mail_msg_id, format="metadata"
                                ).execute()
                            except Exception as e:
                                if "404" in str(e) or "notFound" in str(e):
                                    print(f"⚠️ Mensaje {mail_msg_id} ya no existe (fue eliminado). Continuando con el siguiente...")
                                    continue
                                else:
                                    raise

                            all_labels = msg.get("labelIds", [])
                            all_labels_names = [label_map.get(lid, lid) for lid in all_labels]
                            print(f"🏷️  Labels actuales del mensaje: {all_labels_names}")
                            
                            # Procesar el mensaje completo
                            mail_data = process_email(mail_msg_id, gmail_service)
                            if not mail_data:
                                print(f"❌ Error procesando email {mail_msg_id}")
                                continue
                            
                            # Extraer información y loggear
                            sender = mail_data['sender']
                            match = re.search(r"<([^>]+)>", sender)
                            if match:
                                sender = match.group(1)
                            
                            subject = mail_data['subject']
                            date = mail_data['date']

                            print(f"📧 SENDER: {sender}")
                            print(f"📧 SUBJECT: {subject}")
                            print(f"📧 DATE: {date}")
                            
                            body_preview = mail_data.get('raw_text', '')[:500]
                            print(f"📧 BODY PREVIEW:\n{body_preview}")

                            # Determinar tabla y PK
                            table_name, pk = None, None
                            if (BANK_EMAIL_SENDER in sender and BANK_SUBJECT_TRANSFER in subject):
                                table_name = 'bank_transfers'
                                pk = 'nro_comprobante'
                            elif (BANK_EMAIL_SENDER in sender and any(keyword in subject for keyword in BANK_SUBJECTS_PAYMENTS)):
                                table_name = 'bank_payments'
                                pk = 'MESSAGE_ID'
                            elif (sender in MARKET_EMAIL_SENDERS and MARKET_SUBJECT in subject):
                                table_name = 'carrefour_data'
                                pk = 'nro_ticket'
                            elif (sender in MP_EMAIL_SENDERS and MP_SUBJECT_REPORT in subject):
                                table_name = 'mp_data'
                                pk = 'REPORT_ID'
                            elif (sender in MP_EMAIL_SENDERS and MP_SUBJECT_TRANSFER in subject):
                                table_name = 'mp_transfer_data'
                                pk = 'message_id'    
                            else:
                                print(f'⚠️  Email ignorado (no cumple filtros de sender/subject): {sender} - {subject}')
                                continue
                                                            
                            print(f'📊 table_name={table_name}, pk={pk}')

                            # Verificar si ya existe en BigQuery
                            ids_existentes = get_message_ids_loaded_in_bigquery(bq_client, table_name, pk)
                            
                            if mail_msg_id in ids_existentes:
                                print(f"⚠️ Mensaje {mail_msg_id} ya existe en BigQuery, se omite procesamiento")
                                continue
                            
                            # Procesar y subir a S3
                            print(f'💾 Extrayendo datos del mail y cargando a S3...')
                            response = dispatch_processor(mail_data, folder, MARKET_BUCKET, BANK_BUCKET, s3_client, sender, subject)    
                            print(f"✅ Mensaje procesado exitosamente: {mail_msg_id}")
                            
                            # Ejecutar Step Function
                            payload = response
                            print(f'🚀 Payload para Step Function: {payload}')

                            # Determinar Step Function basado en labels actuales
                            if 'Avisos Gastos Santander' in all_labels_names:
                                step_function_arn = BANK_STEP_FUNCTION_ARN   
                            elif 'Avisos Compra Carrefour' in all_labels_names:
                                step_function_arn = MARKET_STEP_FUNCTION_ARN
                            elif  'Aviso Transferencia MP' in labels_names:
                                step_function_arn = MP_TRANSFER_STEP_FUNCTION_ARN
                            else:
                                print(f"⚠️ No se encontró Step Function para labels: {all_labels_names}")
                                continue
                            
                            status, desc = run_step_function_sync(
                                sfn_client,
                                step_function_arn,
                                payload,
                                poll_interval=10
                            )

                            if status != "SUCCEEDED":
                                print(f"⚠️ Ejecución fallida para mail {mail_msg_id}: {status}")
                            else:
                                print(f"✅ Step Function completada exitosamente para {mail_msg_id}")
                    
                    elif 'messages' in record:
                        print(f"📧 Record {record_history_id} tiene {len(record['messages'])} mensajes (campo 'messages', no 'messagesAdded')")
                        for m in record['messages']:
                            mail_msg_id = m['id']
                            
                            if mail_msg_id in processed_message_ids:
                                print(f"⚠️ Mensaje {mail_msg_id} ya fue procesado anteriormente, se omite")
                                continue
                            
                            processed_message_ids.add(mail_msg_id)
                            
                            # DEBUGGING: Obtener información completa del mensaje para logging
                            print(f"🔍 DEBUG - Analizando mensaje {mail_msg_id} (campo 'messages')")
                            
                            try:
                                msg = gmail_service.users().messages().get(
                                    userId="me", id=mail_msg_id, format="metadata"
                                ).execute()
                            except Exception as e:
                                if "404" in str(e) or "notFound" in str(e):
                                    print(f"⚠️ Mensaje {mail_msg_id} ya no existe (fue eliminado). Se omite pero NO es un error de historyId.")
                                    continue
                                else:
                                    raise

                            labels = msg.get("labelIds", [])
                            labels_names = [label_map.get(lid, lid) for lid in labels]
                            print(f"🏷️  Labels del mensaje: {labels_names}")
                            
                            # DEBUGGING: Extraer y loggear información del email SIN importar el label
                            mail_data_debug = process_email(mail_msg_id, gmail_service)
                            if mail_data_debug:
                                sender_debug = mail_data_debug.get('sender', 'N/A')
                                match = re.search(r"<([^>]+)>", sender_debug)
                                if match:
                                    sender_debug = match.group(1)
                                
                                print(f"📧 SENDER: {sender_debug}")
                                print(f"📧 SUBJECT: {mail_data_debug.get('subject', 'N/A')}")
                                print(f"📧 DATE: {mail_data_debug.get('date', 'N/A')}")
                                
                                # Loggear preview del body (primeros 500 caracteres)
                                body_preview = mail_data_debug.get('raw_text', '')[:500]
                                print(f"📧 BODY PREVIEW (primeros 500 chars):\n{body_preview}")
                                print(f"{'='*80}\n")
                            
                            # CAMBIO: Verificar si el mensaje tiene ALGUNO de los labels objetivo
                            has_target_label = any(lid in target_label_ids for lid in labels)
                            
                            if not has_target_label:
                                print(f"⚠️ Mensaje {mail_msg_id} ignorado - no tiene ninguno de los labels objetivo")
                                print(f"   Labels objetivo: {target_label_names}")
                                print(f"   Labels del mensaje: {labels_names}")
                                continue

                            print(f"✅ Mensaje {mail_msg_id} será procesado - tiene labels objetivo: {[n for n in labels_names if n in target_label_names]}")
                        
                            # Reusar mail_data_debug si ya lo tenemos
                            mail_data = mail_data_debug if mail_data_debug else process_email(mail_msg_id, gmail_service)
                            
                            if not mail_data:
                                print(f"❌ Error procesando email {mail_msg_id}")
                                continue
                            
                            # Extraer sender
                            sender = mail_data['sender']
                            match = re.search(r"<([^>]+)>", sender)
                            if match:
                                sender = match.group(1)
                            
                            subject = mail_data['subject']
                            date = mail_data['date']

                            print(f'✉️  Procesando: sender={sender}, subject={subject}, date={date}')

                            # Determinar tabla y PK basado en sender/subject
                            table_name, pk = None, None
                            if (BANK_EMAIL_SENDER in sender and BANK_SUBJECT_TRANSFER in subject):
                                table_name = 'bank_transfers'
                                pk = 'nro_comprobante'
                            elif (BANK_EMAIL_SENDER in sender and any(keyword in subject for keyword in BANK_SUBJECTS_PAYMENTS)):
                                table_name = 'bank_payments'
                                pk = 'MESSAGE_ID'
                            elif (sender in MARKET_EMAIL_SENDERS and MARKET_SUBJECT in subject):
                                table_name = 'carrefour_data'
                                pk = 'nro_ticket'
                            elif (sender in MP_EMAIL_SENDERS and MP_SUBJECT_REPORT in subject):
                                table_name = 'mp_data'
                                pk = 'REPORT_ID'
                            elif (sender in MP_EMAIL_SENDERS and MP_SUBJECT_TRANSFER in subject):
                                table_name = 'mp_transfer_data'
                                pk = 'message_id'    
                            else:
                                print(f'⚠️  Email ignorado (no cumple filtros de sender/subject): {sender} - {subject}')
                                continue
                                                            
                            print(f'📊 table_name={table_name}, pk={pk}')

                            # Verificar si ya existe en BigQuery
                            ids_existentes = get_message_ids_loaded_in_bigquery(bq_client, table_name, pk)
                            
                            if mail_msg_id in ids_existentes:
                                print(f"⚠️ Mensaje {mail_msg_id} ya existe en BigQuery, se omite procesamiento")
                                continue
                            
                            # Procesar y subir a S3
                            print(f'💾 Extrayendo datos del mail y cargando a S3...')
                            response = dispatch_processor(mail_data, folder, MARKET_BUCKET, BANK_BUCKET, s3_client, sender, subject)    
                            print(f"✅ Mensaje procesado exitosamente: {mail_msg_id}")
                            
                            # Ejecutar Step Function
                            payload = response
                            print(f'🚀 Payload para Step Function: {payload}')

                            if 'Avisos Gastos Santander' in labels_names:
                                step_function_arn = BANK_STEP_FUNCTION_ARN 
                            elif 'Avisos Compra Carrefour' in labels_names:
                                step_function_arn = MARKET_STEP_FUNCTION_ARN
                            elif  'Aviso Transferencia MP' in labels_names:
                                step_function_arn = MP_TRANSFER_STEP_FUNCTION_ARN
                            else:
                                print(f"⚠️ No se encontró Step Function para labels: {labels_names}")
                                continue
                            
                            status, desc = run_step_function_sync(
                                sfn_client,
                                step_function_arn,
                                payload,
                                poll_interval=10
                            )

                            if status != "SUCCEEDED":
                                print(f"⚠️ Ejecución fallida para mail {mail_msg_id}: {status}")
                            else:
                                print(f"✅ Step Function completada exitosamente para {mail_msg_id}")
                
                # Después de procesar todos los records, guardar el historyId más reciente
                print(f"\n✅ Procesamiento de historial completado")
                print(f"💾 Guardando historyId={history_id} en DynamoDB...")
                save_last_history_id_in_dynamo(dynamodb.Table("gmail-history-tracker"), history_id)
                print(f"✅ HistoryId guardado exitosamente")
            
            except Exception as e:
                error_str = str(e)
                print('error_str: ', error_str)
                #  and "users.history" in error_str
                if '404' in error_str or 'notFound' in error_str or 'not found' in error_str.lower():
                    print(f"⚠️ HistoryId {last_history_id} no encontrado (muy antiguo o inválido)")
                    print(f"🔄 AUTO-RECUPERACIÓN: Actualizando historyId a {history_id} (valor actual de Pub/Sub)")
                    
                    # AUTO-RECUPERACIÓN: Guardar el historyId actual para que la próxima ejecución funcione
                    save_last_history_id_in_dynamo(dynamodb.Table(dynamo_table_name), history_id)
                    print(f"✅ HistoryId actualizado automáticamente a {history_id}")
                    print(f"💡 La próxima notificación de Gmail se procesará correctamente")
                    
                    # Retornar 200 para que Pub/Sub no reintente
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'message': 'HistoryId inválido - auto-recuperación completada',
                            'old_historyId': last_history_id,
                            'new_historyId': history_id,
                            'action': 'historyId actualizado automáticamente'
                        })
                    }
                else:
                    raise
        
        else:
            print('❌ Error: No viene el campo "message" en el body de Pub/Sub')
            # Devolver 200 para que Pub/Sub no reintente mensajes malformados
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Mensaje malformado descartado', 'error': 'No message field'})
            }

        print("\n Event: ", event)
        
        # IMPORTANTE: Siempre devolver 200 al final para que Pub/Sub marque el mensaje como procesado
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Procesamiento completado exitosamente'})
        }
        
    except Exception as e:
        print("⚠️ Error:", str(e))
        import traceback
        traceback.print_exc()
        
        # Devolver 200 para evitar reintentos infinitos de Pub/Sub
        # Solo fallar con 500 si es un error que realmente puede resolverse con un reintento
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Error procesando mensaje, marcado como procesado para evitar loop infinito',
                'error': str(e)
            })
        }