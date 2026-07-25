import json
import boto3
import logging
import os
import psycopg2
from datetime import datetime

# Importamos las variables del github secrets
aws_region = os.environ["REGION_ID"]
aws_account_id = os.environ["ACCOUNT_ID"]
sns_topic = os.environ["SNS_TOPIC"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sns_client = boto3.client('sns')
SNS_TOPIC_ARN = f'arn:aws:sns:{aws_region}:{aws_account_id}:{sns_topic}'

# Iniciamos los servicios de AWS para realizar las operaciones de compensacion
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Funcion para registrar los errores que hayan ocurrido en el flujo
def log_failure_to_dynamo(table_name, error_detail):
    table = dynamodb.Table(table_name)
    table.put_item(
        Item={
            'error_id': str(datetime.utcnow().timestamp()),
            'error_detail': error_detail,
            'timestamp': datetime.utcnow().isoformat()
        }
    )
    logger.info("Failure logged in DynamoDB.")

# Funcion para rollbackear las modificaciones que se hayan hecho en la tabla de Redshift
def rollback_redshift(redshift_params, control_table, file_id):
    try:
        conn = psycopg2.connect(**redshift_params)
        cur = conn.cursor()
        # Ejemplo: marcar como fallido
        cur.execute(f"UPDATE {control_table} SET status='FAILED' WHERE file_id = %s", (file_id,))
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Rollback in Redshift completed successfully.")
    except Exception as e:
        logger.error(f"Error during rollback: {str(e)}")

# Funcion para borrar archivos temporales de S3 que no se terminaron de ingestar o convertir por falla en el flujo
def cleanup_s3_temp_files(bucket_name, prefix):
    logger.info(f"Cleaning up temp files in {bucket_name}/{prefix}")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
            logger.info(f"Deleted {obj['Key']}")
    else:
        logger.info("No temporary files found to delete.")

def lambda_handler(event, context):
    print(event)
    logger.info("Compensation flow triggered due to failure in ETL process.")
    
    # Extraer información del contexto
    body = event.get('body', {})
    etl_flow = body.get('etl_flow', event.get('etl_flow', 'UNKNOWN'))
    bucket = body.get('bucket', event.get('bucket', 'N/A'))
    key = body.get('key', event.get('key', 'N/A'))
    
    # Para MP Reports que tienen estructura diferente
    file_name = event.get('file_name', key)
    
    error_info = event.get('error-info', {})
    error_type = error_info.get('Error', 'Unknown Error')
    error_cause = error_info.get('Cause', 'No details available')
    
    # Parsear el Cause si es JSON
    try:
        cause_parsed = json.loads(error_cause)
        error_message = cause_parsed.get('errorMessage', error_cause)
    except (json.JSONDecodeError, TypeError):
        error_message = error_cause
    
    logger.error(f"Compensation triggered - ETL: {etl_flow}, File: {file_name}, Error: {error_type}")
    
    # Construir mensaje descriptivo
    subject = f"ETL {etl_flow} Failed - {error_type}"
    message = f"""🚨 Fallo en proceso ETL

📋 ETL Flow: {etl_flow}
📁 Archivo: {file_name}
🪣 Bucket: {bucket}

❌ Error: {error_type}
📝 Detalle: {error_message}

---
Evento completo: {json.dumps(event, indent=2, default=str)[:1500]}
"""
    
    # Enviar alerta SNS con detalle del error
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],  # SNS subject max 100 chars
        Message=message
    )
    
    # # Segun el tipo de error ejecutamos una funcion especifica de compensacion
    # if 'GmailDownloadError' in error_detail:
    #     cleanup_s3_temp_files(bucket_name, prefix)
    # elif 'RedshiftUploadError' in error_detail:
    #     rollback_redshift(redshift_params, control_table, file_id)
    # else:
    #     log_failure_to_dynamo(table_name, error_detail)

    return {
        'statusCode': 200,
        'body': json.dumps('Compensation flow executed successfully.')
    }