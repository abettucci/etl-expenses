import json
import boto3
import logging
import os
import psycopg2
from datetime import datetime

# Importamos las variables del github secrets
aws_region = os.environ["AWS_REGION"]
aws_account_id = os.environ["AWS_ACCOUNT_ID"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sns_client = boto3.client('sns')
SNS_TOPIC_ARN = f'arn:aws:sns:{aws_region}:{aws_account_id}:etl_alerts'

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
    logger.info("Compensation flow triggered due to failure in ETL process.")
    error_detail = json.dumps(event.get('error-info', {}))
    logger.error(f"Compensation triggered due to: {error_detail}")
    
    # Enviar alerta SNS con detalle del error
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="ETL PDF Process Failed - Compensation Executed",
        Message=f"Fallo en proceso ETL. Detalle: {error_detail}"
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