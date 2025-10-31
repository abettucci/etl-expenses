import bcrypt
import json
import os
import boto3
import re

def lambda_handler(event, context):
    step_function_arn = os.environ['STEP_FUNCTION_ARN']
    step_functions_client = boto3.client('stepfunctions')
    CIFRADO_SECRET = os.environ["CIFRADO_SECRET_MP"]
    
    if isinstance(event.get("body"), str):
        body_json = json.loads(event["body"])
    else:
        body_json = event.get("body", {})

    try:
        transaction_id = body_json.get("transaction_id", "")
        generation_date = body_json.get("generation_date", "")
        firma_enviada = body_json.get("signature", "")

        if not transaction_id or not generation_date or not firma_enviada:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Faltan campos requeridos"})
            }

        files = body_json.get("files", [])
        if files:
            file = files[0]
            file_name = file.get("name", "")
            file_url = file.get("url", "")
            file_type = file.get("type", "")
        else:
            file_name = file_url = file_type = ""
        
        step_input = {
            "file_name": file_name,
            "file_url": file_url,
            "file_type": file_type
        }

        cadena_para_firma = f"{transaction_id}-{CIFRADO_SECRET}-{generation_date}"
        cadena_para_firma_bytes = cadena_para_firma.encode("utf-8")

        bcrypt_pattern = re.compile(r'^\$2[aby]\$\d{2}\$[A-Za-z0-9./]{53}$')
        
        if not bcrypt_pattern.match(firma_enviada):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Formato de firma inválido"})
            }

        if not bcrypt.checkpw(cadena_para_firma_bytes, firma_enviada.encode("utf-8")):
            return {
                "statusCode": 403,
                "body": json.dumps({"message": "Firma inválida"})
            }

        response = step_functions_client.start_execution(
            stateMachineArn=step_function_arn,
            input=json.dumps(step_input)
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Step Function started successfully!',
                'executionArn': response['executionArn']
            })
        }
            
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Error interno: {str(e)}"})
        }