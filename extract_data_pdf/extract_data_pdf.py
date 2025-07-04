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

def auth_google():
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    SECRET_NAME = 'gcp_api_credentials'
    REGION_NAME = 'us-east-2'

    token_info = get_secret(SECRET_NAME, REGION_NAME)
    creds = Credentials.from_authorized_user_info(token_info, SCOPES)
    
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        print("♻️ Token refrescado")

        # Guardar el token actualizado en Secrets Manager
        update_secret(creds.to_json(), SECRET_NAME, REGION_NAME)

    return creds

# Funcion para extraer los PDFs especificos de Gmail
def extract_gmail_pdfs(redshift_data):
    creds = auth_google()
    gmail_service = build('gmail', 'v1', credentials=creds)
    s3_client = boto3.client('s3')
    bucket_name = 'market-tickets'
    folder = 'raw/'

    sender_email = "contacto@m.tarjetacarrefour.com.ar"
    subject_contains = "Hola, te enviamos el ticket digital de tu compra."

    # Obtenemos la ultima fecha de la tabla de tickets ya ingestados de Redshift        
    date_query = """
        SELECT MAX(
            TO_DATE(
                CASE 
                    WHEN LENGTH(SPLIT_PART(fecha, '/', 3)) = 2 THEN 
                        -- Formato DD/MM/YY → convertir a DD/MM/20YY
                        SPLIT_PART(fecha, '/', 1) || '/' || 
                        SPLIT_PART(fecha, '/', 2) || '/' || 
                        '20' || SPLIT_PART(fecha, '/', 3)
                    ELSE fecha -- Asumir que ya está en DD/MM/YYYY
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
    fecha_ultimo_ticket_cargado = None
    while True:
        desc = redshift_data.describe_statement(Id=response['Id'])
        if desc['Status'] == 'FINISHED':
            if desc['HasResultSet']:
                result = redshift_data.get_statement_result(Id=response['Id'])
                try:
                    fecha_ultimo_ticket_cargado = result['Records'][0][0]['stringValue']
                    if len(fecha_ultimo_ticket_cargado.split('/')[-1]) == 2:
                        day, month, year = fecha_ultimo_ticket_cargado.split('/')
                        fecha_ultimo_ticket_cargado = f"{day}/{month}/20{year}"
                    fecha_ultimo_ticket_cargado = datetime.strptime(fecha_ultimo_ticket_cargado, '%Y-%m-%d')
                    fecha_ultimo_ticket_cargado += timedelta(days=1)
                except Exception as e:
                    fecha_ultimo_ticket_cargado = None
                    print(f"Error: {e}")
            break
        elif desc['Status'] == 'FAILED':
            print("Error al consultar Redshift:", desc['Error'])
            fecha_ultimo_ticket_cargado = (datetime.now() - timedelta(days=7)) # Fallback: últimos 7 días
            break
        
    if fecha_ultimo_ticket_cargado is None:
        fecha_actual = datetime.now()
        date_str = fecha_actual - timedelta(weeks=1)
        date_str = date_str.strftime('%Y/%m/%d')
    else:
        date_str = fecha_ultimo_ticket_cargado.strftime('%Y/%m/%d')

    query = f'from:{sender_email} subject:"{subject_contains}" after:{date_str}'
    results = gmail_service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])

    print(f"Total de mails de tickets de carrefour posterior a {date_str}: {len(messages)}")

    for msg in messages:
        message = gmail_service.users().messages().get(userId='me', id=msg['id']).execute()
        parts = message['payload'].get('parts', [])

        headers = {h['name']: h['value'] for h in message['payload']['headers']}
        date = datetime.fromtimestamp(int(message['internalDate']) / 1000).strftime('%Y-%m-%d')

        filename = f'Ticket_{date}.pdf'
        s3_key = f'{folder}{filename}'

        for part in parts:
            if part.get('mimeType') == 'text/html':
                data = part['body']['data']
                decoded_data = base64.urlsafe_b64decode(data).decode('utf-8')
                soup = BeautifulSoup(decoded_data, 'html.parser')
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

def lambda_handler(event, context):
    try:
        redshift_data = boto3.client('redshift-data')
        extract_gmail_pdfs(redshift_data)
    except Exception as e:
        print("⚠️ Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }