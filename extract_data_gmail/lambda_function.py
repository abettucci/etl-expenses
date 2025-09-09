import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import base64
import re
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
import pandas as pd
import os
from io import BytesIO
import requests
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

BANK_EMAIL_SENDER = "mensajesyavisos@mails.santander.com.ar"
BANK_SUBJECTS = ["Pagaste","Aviso de débito automático"]
MARKET_EMAIL_SENDERS = ["atencion_clientes@m.contactocarrefour.com.ar", "contacto@m.tarjetacarrefour.com.ar"]
MARKET_SUBJECT = "Hola, te enviamos el ticket digital de tu compra."
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

def get_message_ids_loaded(redshift_data, table_name, pk):
    # Obtenemos los ids existentes
    id_existentes_query = f"SELECT DISTINCT {pk} FROM {table_name};"

    print('id_existentes_query: ', id_existentes_query)

    # Ejecutar consulta
    response = redshift_data.execute_statement(
        Database='dev',
        WorkgroupName='pdf-etl-workgroup',
        Sql=id_existentes_query
    )

    print(response)

    ids_existentes_en_redshift = set()
    while True:
        print("Esperando resultado de Redshift...")
        desc = redshift_data.describe_statement(Id=response['Id'])
        if desc['Status'] == 'FINISHED':
            if desc['HasResultSet']:
                try:
                    result = redshift_data.get_statement_result(Id=response['Id'])
                    print('result: ', result)
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
    for subject in SUBJECT_CONTAINS:
        date_str = get_last_message_loaded(redshift_data)
        query = f'from:{SENDER_EMAIL} subject:"{subject}" after:{date_str}'
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

# Funcion para extraer los PDFs especificos de Gmail
def download_pdf_from_email_urls(mail_data, sender_email, bucket_name, folder, s3_client):
    print('Analizando mail de fecha: ', date)
    filename = f'Ticket_{date}.pdf'
    s3_key = f'{folder}{filename}'
    date = mail_data["date"]    
    soup = mail_data["raw_text"]

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

def dispatch_processor(mail_data, folder, market_bucket, bank_bucket, s3_client, sender, subject):
    """Dispatch basado en subject y sender"""

    if (BANK_EMAIL_SENDER in sender and subject in BANK_SUBJECTS): 
        print('Descargando la info del mail del gasto de santander')
        s3_key = f"{folder}{mail_data['date'][:10]}-{mail_data['message_id']}.json"
        s3_client.put_object(
            Body=json.dumps(mail_data),
            Bucket=bank_bucket,
            Key=s3_key
        )        
        print(f"✅ Archivo subido a S3: {s3_key}")

        return  {
            "statusCode": 200,
            "body": {
                "key": s3_key,
                "process": True
            }
        }
    
    elif (sender in MARKET_EMAIL_SENDERS and MARKET_SUBJECT in subject):
        print('Descargando el pdf del mail de carrefour...')
        s3_key = download_pdf_from_email_urls(mail_data, sender, market_bucket, folder, s3_client)
        print(f"✅ Archivo subido a S3: {s3_key}")

        return  {
            "statusCode": 200,
            "body": {
                "key": s3_key,
                "process": True
            }
        }

    else:
        # Default o email no manejado
        print(f"Email no manejado - Subject: {sender}, From: {subject}")
        return {
            "process": False,
            "reason": "Evento descartado por filtros"
        }

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

def save_last_history_id(table, history_id):
    table.put_item(Item={
        "PK": "gmail_last_history_id",
        "historyId": str(history_id)
    })

def lambda_handler(event, context):
    try:
        print(f"Mensaje Pub/Sub: {json.dumps(event)}")

        creds = auth_google('gcp_api_credentials')
        dynamodb = boto3.resource('dynamodb')
        dynamo_table_name = dynamodb.Table("gmail-history-tracker")
        gmail_service = build('gmail', 'v1', credentials=creds)
        redshift_data = boto3.client('redshift-data')
        s3_client = boto3.client('s3')
        bank_bucket = os.environ['BANK_BUCKET_NAME']
        market_bucket = os.environ['MARKET_BUCKET_NAME']
        folder = 'raw/'

        results = gmail_service.users().labels().list(userId="me").execute()
        label_ids = []
        for label in results['labels']:
            if label['name'] in ['Avisos Gastos Santander', 'Avisos Compra Carrefour']:
                label_ids.append(label['id'])

        print('label_ids: ', label_ids)

        if label_ids:
            for label_id in label_ids:
                if 'message' in event:
                    message = event['message']       

                    print("\n Message: ", message)     
                    message_data = json.loads(base64.b64decode(message['data']).decode('utf-8'))

                    print("\n Data decodificada: ", message_data)

                    history_id = message_data.get('historyId')
                    if not history_id:
                        print("⚠️ No se encontró historyId en el evento")
                        return

                    last_history_id = load_last_history_id(dynamo_table_name) or history_id
                    print(f"📩 Procesando desde historyId={last_history_id} hasta {history_id}")
            
                    history = gmail_service.users().history().list(
                        userId='me',
                        startHistoryId=last_history_id,
                        labelId = label_id
                    ).execute()

                    for record in history.get('history', []):
                        if 'messagesAdded' in record:
                            for m in record['messagesAdded']:
                                mail_msg_id = m['message']['id']
                                
                                msg = gmail_service.users().messages().get(
                                    userId="me", id=mail_msg_id, format="metadata"
                                ).execute()

                                labels = msg.get("labelIds", [])
                                if label_id not in labels:
                                    print(f"⚠️ Mensaje {mail_msg_id} ignorado porque no tiene el label {label_id}")
                                    continue

                                print(f" Procesando mensaje {mail_msg_id} porque tiene el label {label_id}")
                                
                                mail_data = process_email(mail_msg_id, gmail_service)
                                match = re.search(r"<([^>]+)>", mail_data['sender'])
                                if match:
                                    sender = match.group(1)
                                subject = mail_data['subject']
                                date = mail_data['date']

                                print('sender: ', sender)
                                print('subject: ', subject)
                                print('date: ', date)

                                if not mail_data:
                                    return {'statusCode': 500, 'body': 'Error procesando email'}

                                table_name, pk = None, None
                                if (BANK_EMAIL_SENDER in sender and any(keyword in subject for keyword in BANK_SUBJECTS)):
                                    table_name = 'bank_payments'
                                    pk = 'id'

                                elif (sender in MARKET_EMAIL_SENDERS and MARKET_SUBJECT in subject):
                                    table_name = 'carrefour_data'
                                    pk = 'nro_ticket'

                                else:
                                    return {"process": False, "reason": "Evento descartado por filtros"}
                                                                
                                print('table_name: ', table_name)
                                print('pk : ', pk)

                                ids_existentes_en_redshift = get_message_ids_loaded(redshift_data, table_name, pk)

                                print('mail_msg_id: ', mail_msg_id)
                                print('ids_existentes_en_redshift: ', ids_existentes_en_redshift)
                                
                                if mail_msg_id not in ids_existentes_en_redshift:
                                    print('Intentamos extraer los datos del mail y cargarlos a S3')
                                    response = dispatch_processor(mail_data, folder, market_bucket, bank_bucket, s3_client, sender, subject)              

                    save_last_history_id(dynamo_table_name, history_id)

                    return response

                else:
                    print('Error al extraer los datos')

        else:
            print("❌ No hay etiquetas configuradas")

        print("\n Event: ", event)
        
            
    except Exception as e:
        # Si falla la logica de filtrado por ids podria probar con traer los mails recibidos desde la ultima fecha de ingestion
        # extract_by_date_payments_from_gmail(redshift_data, ids_existentes_en_redshift, gmail_service, s3_client, bucket_name, folder)

        print("⚠️ Error:", str(e))
        raise Exception(str(e))