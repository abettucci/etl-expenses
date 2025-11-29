import os
import json
import boto3
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

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

def lambda_handler(event, context):
    creds = auth_google('gcp_sa_api_credentials')
    gmail_service = build('gmail', 'v1', credentials=creds)
    
    results = gmail_service.users().labels().list(userId="me").execute()
    
    custom_labels = []
    inbox_label_id = None

    for label in results['labels']:
        if label['name'] == 'INBOX':
            inbox_label_id = label['id']
        if label['name'] in ['Avisos Gastos Santander', 'Avisos Compra Carrefour']:
            custom_labels.append(label['id'])

    if inbox_label_id is None:
        raise Exception("❌ No encontré el label INBOX (esto no debería pasar).")

    body = {
        "labelIds": [inbox_label_id] + custom_labels,
        "topicName": f"projects/{os.environ['GCP_PROJECT_ID']}/topics/{os.environ['PUBSUB_TOPIC']}"
    }

    resp = gmail_service.users().watch(userId="me", body=body).execute()

    print("Watcher renewed:", resp)
    return {"status": "ok", "response": resp}