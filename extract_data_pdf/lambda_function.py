import json
import requests
from io import BytesIO
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

SENDERS_EMAIL = ["atencion_clientes@m.contactocarrefour.com.ar", "contacto@m.tarjetacarrefour.com.ar"]
SUBJECT_CONTAINS = "Hola, te enviamos el ticket digital de tu compra."

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
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly','https://www.googleapis.com/auth/bigquery']
    REGION_NAME = 'us-east-2'    
    token_info = get_secret(SECRET_NAME, REGION_NAME)
    creds = Credentials.from_authorized_user_info(token_info, SCOPES)
    
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        print("♻️ Token refrescado")

        # Guardar el token actualizado en Secrets Manager
        update_secret(creds.to_json(), SECRET_NAME, REGION_NAME)

    return creds

def get_message_ids_loaded(redshift_data):
    # Obtenemos los ids existentes
    id_existentes_query = "SELECT DISTINCT nro_ticket FROM carrefour_data;"

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
                    WHEN LENGTH(SPLIT_PART(fecha, '/', 3)) = 2 THEN 
                        -- convertimos a formato DD/MM/20YY
                        SPLIT_PART(fecha, '/', 1) || '/' || 
                        SPLIT_PART(fecha, '/', 2) || '/' || 
                        '20' || SPLIT_PART(fecha, '/', 3)
                    ELSE fecha -- Asumir que ya está en formato DD/MM/YYYY
                END,
                'DD/MM/YYYY'
            )
        ) AS max_date 
        FROM carrefour_data
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

def extract_by_date_payments_from_gmail(mail_data, redshift_data, gmail_service, s3_client, bucket_name, folder, senders, subject_contains):
    date_str = get_last_message_loaded(redshift_data)
    
    for sender_email in senders:
        query = f'from:{sender_email} subject:"{subject_contains}" after:{date_str}'
        results = gmail_service.users().messages().list(userId='me', q=query).execute()
        messages = results.get('messages', [])

        print(f"Total de mails de tickets de carrefour posterior a {date_str}: {len(messages)}")

        for msg in messages:
            message = gmail_service.users().messages().get(userId='me', id=msg['id']).execute()
            
            print(message)

            message_id = message.get('messageId')

            ids_existentes_en_redshift = get_message_ids_loaded(redshift_data)
            if message_id not in ids_existentes_en_redshift:
                # Procesar el email
                mail_data = process_email(message_id, gmail_service)
                download_pdf_from_email_urls(mail_data, sender_email, bucket_name, folder, s3_client)

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

def find_html_part(payload):
    if payload.get("mimeType") == "text/html":
        return payload["body"].get("data", None)
    elif "parts" in payload:
        for part in payload["parts"]:
            result = find_html_part(part)
            if result:
                return result
    return None

def lambda_handler(event, context):
    try:
        print(f"Mensaje Pub/Sub: {json.dumps(event)}")

        redshift_data = boto3.client('redshift-data')
        creds = auth_google('gcp_api_credentials')
        gmail_service = build('gmail', 'v1', credentials=creds)
        s3_client = boto3.client('s3')
        bucket_name = 'market-tickets'
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
                    for sender_email in SENDERS_EMAIL:
                        if (sender_email in mail_data['sender'] and 
                            SUBJECT_CONTAINS in mail_data['subject']):
                            download_pdf_from_email_urls(mail_data, sender_email, bucket_name, folder, s3_client)
                            return {'statusCode': 200, 'body': 'Email procesado exitosamente'}
                        else:
                            print("⚠️ Email no cumple filtros, descartado")
                            return {'statusCode': 200, 'body': 'Email descartado por filtros'}

    except Exception as e:
        # if 'body' in event:
        #     pubsub_message = json.loads(event['body'])
        #     if 'message' in pubsub_message and 'data' in pubsub_message['message']:
        #         message_data = json.loads(base64.b64decode(pubsub_message['message']['data']).decode('utf-8'))
        #         message_id = message_data.get('messageId')  # Ajustar según la estructura real
        #         ids_existentes_en_redshift = get_message_ids_loaded(redshift_data)
        #         if message_id not in ids_existentes_en_redshift:
        #             mail_data = process_email(message_id, gmail_service)
        #             extract_by_date_payments_from_gmail(mail_data, redshift_data, ids_existentes_en_redshift, gmail_service, s3_client, bucket_name, folder)

        print("⚠️ Error:", str(e))
        raise Exception(str(e))