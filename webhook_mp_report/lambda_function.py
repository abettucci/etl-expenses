import bcrypt
import json
import os
import boto3
import re

def lambda_handler(event, context):
    print('event: ', event)

    step_functions_client = boto3.client('stepfunctions')
    
    # Usar la variable de entorno en lugar del valor hardcodeado
    CIFRADO_SECRET = os.environ.get("CIFRADO_SECRET_MP")
    
    # Parsear el body
    if isinstance(event.get("body"), str):
        try:
            body_json = json.loads(event["body"])
        except json.JSONDecodeError:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Body JSON inválido"})
            }
    else:
        body_json = event.get("body", {})

    try:
        # Obtener datos del body
        transaction_id = body_json.get("transaction_id", "")
        generation_date = body_json.get("generation_date", "")
        
        # IMPORTANTE: La firma viene en los headers, no en el body
        headers = event.get('headers', {})

        print('headers: ', headers)
        
        # Buscar la firma en diferentes formatos de header (case-insensitive)
        firma_enviada = None
        for header_name, header_value in headers.items():
            if header_name.lower() == 'x-signature':
                firma_enviada = header_value
                break
        
        print('firma_enviada: ', firma_enviada)

        if not firma_enviada:
            print("Header x-signature no encontrado")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Falta el header x-signature"})
            }

        if not transaction_id or not generation_date:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Faltan campos requeridos en el body"})
            }

        # Construir la cadena para verificación
        cadena_para_firma = f"{transaction_id}-{CIFRADO_SECRET}-{generation_date}"
        cadena_para_firma_bytes = cadena_para_firma.encode("utf-8")
        
        print(f"Cadena para verificación: {cadena_para_firma}")
        print(f"Firma recibida: {firma_enviada}")

        # Verificar la firma con bcrypt
        # La firma de Mercado Pago ya está en formato bcrypt
        try:
            # bcrypt.checkpw espera que ambos parámetros estén en bytes
            if bcrypt.checkpw(cadena_para_firma_bytes, firma_enviada.encode("utf-8")):
                print("Firma válida")
                
                # Extraer información de files si existe
                files = body_json.get("files", [])
                if files:
                    file = files[0]
                    file_name = file.get("name", "")
                    file_url = file.get("url", "")
                    file_type = file.get("type", "")
                else:
                    file_name = file_url = file_type = ""
                
                # Iniciar Step Function
                step_input = {
                    "file_name": file_name,
                    "file_url": file_url,
                    "file_type": file_type
                }

                response = step_functions_client.start_execution(
                    stateMachineArn=os.environ['STEP_FUNCTION_ARN'],
                    input=json.dumps(step_input)
                )
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Step Function started successfully!',
                        'executionArn': response['executionArn']
                    })
                }
            else:
                print("Firma inválida - no coincide")
                return {
                    "statusCode": 403,
                    "body": json.dumps({"message": "Firma inválida"})
                }
                
        except Exception as bcrypt_error:
            print(f"Error en verificación bcrypt: {str(bcrypt_error)}")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": f"Error en verificación de firma: {str(bcrypt_error)}"})
            }
            
    except Exception as e:
        print(f"Error general: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Error interno: {str(e)}"})
        }