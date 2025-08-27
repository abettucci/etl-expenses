import json
import boto3
from datetime import datetime, timedelta
import base64
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
import pandas as pd
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

SENDER_EMAIL = "mensajesyavisos@mails.santander.com.ar"
SUBJECT_CONTAINS = "Pagaste"
# body_contains = "Te acercamos el detalle de tu consumo con la Tarjeta Santander"

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
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    REGION_NAME = 'us-east-2'    
    token_info = get_secret(SECRET_NAME, REGION_NAME)
    creds = Credentials.from_authorized_user_info(token_info, SCOPES)
    
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        print("♻️ Token refrescado")

        # Guardar el token actualizado en Secrets Manager
        update_secret(creds.to_json(), SECRET_NAME, REGION_NAME)

    return creds

def find_html_part(payload):
    if payload.get("mimeType") == "text/html":
        return payload["body"].get("data", None)
    elif "parts" in payload:
        for part in payload["parts"]:
            result = find_html_part(part)
            if result:
                return result
    return None

def get_message_ids_loaded(redshift_data):
    # Obtenemos los ids existentes
    id_existentes_query = "SELECT DISTINCT id FROM bank_payments;"

    # Ejecutar consulta
    response = redshift_data.execute_statement(
        Database='dev',
        WorkgroupName='pdf-etl-workgroup',
        Sql=id_existentes_query
    )

    ids_existentes_en_redshift = set()
    while True:
        desc = redshift_data.describe_statement(Id=response['Id'])
        if desc['Status'] == 'FINISHED':
            if desc['HasResultSet']:
                try:
                    result = redshift_data.get_statement_result(Id=response['Id'])
                    ids_existentes_en_redshift = {
                        row[0]['stringValue'] for row in result['Records'] if 'stringValue' in row[0]
                    }
                except Exception as e:
                    ids_existentes_en_redshift = set()
                    print(f"❌ Error al obtener resultados de Redshift: {e}")
            break
        elif desc['Status'] == 'FAILED':
            print("❌ Error al consultar Redshift:", desc['Error'])
            break

    return ids_existentes_en_redshift

def get_last_message_loaded(redshift_data):
    # Obtenemos la ultima fecha de la tabla de tickets ya ingestados de Redshift        
    date_query = """
        SELECT MAX(
            TO_DATE(
                CASE 
                    WHEN LENGTH(SPLIT_PART(fecha_pago, '/', 3)) = 2 THEN 
                        -- convertimos a formato DD/MM/20YY
                        SPLIT_PART(fecha_pago, '/', 1) || '/' || 
                        SPLIT_PART(fecha_pago, '/', 2) || '/' || 
                        '20' || SPLIT_PART(fecha_pago, '/', 3)
                    ELSE fecha_pago -- Asumir que ya está en formato DD/MM/YYYY
                END,
                'DD/MM/YYYY'
            )
        ) AS max_date 
        FROM bank_payments
    """
    
    # Ejecutar consulta
    response = redshift_data.execute_statement(
        Database='dev',
        WorkgroupName='pdf-etl-workgroup',
        Sql=date_query
    )

    # Esperar resultados (puede tomar algunos segundos)
    fecha_ultimo_payment_cargado = None
    while True:
        desc = redshift_data.describe_statement(Id=response['Id'])
        if desc['Status'] == 'FINISHED':
            if desc['HasResultSet']:
                result = redshift_data.get_statement_result(Id=response['Id'])
                try:
                    fecha_ultimo_payment_cargado = result['Records'][0][0]['stringValue']
                    if len(fecha_ultimo_payment_cargado.split('/')[-1]) == 2:
                        day, month, year = fecha_ultimo_payment_cargado.split('/')
                        fecha_ultimo_payment_cargado = f"{day}/{month}/20{year}"
                    fecha_ultimo_payment_cargado = datetime.strptime(fecha_ultimo_payment_cargado, '%Y-%m-%d')
                    fecha_ultimo_payment_cargado += timedelta(days=1)
                except Exception as e:
                    fecha_ultimo_payment_cargado = None
                    print(f"Error: {e}")
            break
        elif desc['Status'] == 'FAILED':
            print("Error al consultar Redshift:", desc['Error'])
            date_str = '2024/10/01'
            break
        
    if fecha_ultimo_payment_cargado is None:
        date_str = '2024/10/01' 
    else:
        date_str = fecha_ultimo_payment_cargado.strftime('%Y/%m/%d')

    return date_str

def extract_by_date_payments_from_gmail(redshift_data, ids_existentes_en_redshift, gmail_service, s3_client, bucket_name, folder):
    date_str = get_last_message_loaded(redshift_data)
    query = f'from:{SENDER_EMAIL} subject:"{SUBJECT_CONTAINS}" after:{date_str}'
    results = gmail_service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])
    print(f"Total de mails de Santander posterior a {date_str}: {len(messages)}")

    for msg in messages:
        message = gmail_service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        msg_id = msg['id']

        if msg_id not in ids_existentes_en_redshift:
            payload = message['payload']
            parts = payload.get('parts', [])
            html_encoded = find_html_part(payload)
            html_data = base64.urlsafe_b64decode(html_encoded).decode('utf-8', errors='replace') if html_encoded else None
            body_text = BeautifulSoup(html_data, 'html.parser').get_text() if html_data else ""

            mail_data = {
                "message_id": msg_id,
                "date": datetime.fromtimestamp(int(message['internalDate']) / 1000).isoformat(),
                "sender": SENDER_EMAIL,
                "subject": next(h['value'] for h in payload['headers'] if h['name'] == 'Subject'),
                "html_body": html_data,
                "raw_text": body_text,
            }

            s3_key = f"{folder}{mail_data['date'][:10]}-{msg_id}.json"
            s3_client.put_object(Body=json.dumps(mail_data), Bucket=bucket_name, Key=s3_key)
            print(f"✅ Archivo subido a S3: {s3_key}")
        else:
            print("⚠️ El archivo ya existe en S3, se omite la subida.")

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

def lambda_handler(event, context):
    try:
        print(f"Mensaje Pub/Sub: {json.dumps(event)}")

        creds = auth_google('gcp_api_credentials')
        gmail_service = build('gmail', 'v1', credentials=creds)
        redshift_data = boto3.client('redshift-data')
        s3_client = boto3.client('s3')
        bucket_name = 'bank-payments'
        folder = 'raw/'
        
        # El mensaje de Pub/Sub viene en el body del request de API Gateway
        if 'body' in event:
            pubsub_message = json.loads(event['body'])

            # Los datos del email están en message.data (base64)
            if 'message' in pubsub_message and 'data' in pubsub_message['message']:

                # Extraer message_id del evento de Pub/Sub
                message_data = json.loads(base64.b64decode(pubsub_message['message']['data']).decode('utf-8'))
                message_id = message_data.get('messageId')  # Ajustar según la estructura real

                ids_existentes_en_redshift = get_message_ids_loaded(redshift_data)
                if message_id not in ids_existentes_en_redshift:
                    # Procesar el email
                    mail_data = process_email(message_id, gmail_service)

                    if not mail_data:
                        return {'statusCode': 500, 'body': 'Error procesando email'}
                    
                    # Aplicar filtros (igual que antes)
                    if (SENDER_EMAIL in mail_data['sender'] and 
                        SUBJECT_CONTAINS in mail_data['subject']):
                        
                        # Guardar en S3
                        s3_key = f"{folder}{mail_data['date'][:10]}-{mail_data['message_id']}.json"
                        s3_client.put_object(
                            Body=json.dumps(mail_data),
                            Bucket=bucket_name,
                            Key=s3_key
                        )
                        
                        print(f"✅ Archivo subido a S3: {s3_key}")
                        return {'statusCode': 200, 'body': 'Email procesado exitosamente'}
                    else:
                        print("⚠️ Email no cumple filtros, descartado")
                        return {'statusCode': 200, 'body': 'Email descartado por filtros'}

    except Exception as e:
        # Si falla la logica de filtrado por ids podria probar con traer los mails recibidos desde la ultima fecha de ingestion
        # extract_by_date_payments_from_gmail(redshift_data, ids_existentes_en_redshift, gmail_service, s3_client, bucket_name, folder)

        print("⚠️ Error:", str(e))
        raise Exception(str(e))