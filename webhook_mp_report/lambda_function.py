import bcrypt
import json
import os
import boto3
import re

def lambda_handler(event, context):
    step_functions_client = boto3.client('stepfunctions')
    
    # Usar la variable de entorno
    CIFRADO_SECRET = os.environ.get("CIFRADO_SECRET_MP")
    
    print('CIFRADO_SECRET: ', CIFRADO_SECRET)
    
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
        # Obtener datos del body - CORREGIDO: la firma está en el body, no en headers
        transaction_id = body_json.get("transaction_id", "")
        generation_date = body_json.get("generation_date", "")
        firma_enviada = body_json.get("signature", "")  # ¡Esto es lo importante!

        if not transaction_id or not generation_date or not firma_enviada:
            print(f"Faltan campos: transaction_id={transaction_id}, generation_date={generation_date}, signature={firma_enviada}")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Faltan campos requeridos en el body"})
            }

        # Agrega esto al inicio de tu lambda_handler después de parsear el body
        print("=== DEBUG INFO ===")
        print(f"Transaction ID: {body_json.get('transaction_id')}")
        print(f"Generation Date: {body_json.get('generation_date')}")
        print(f"Signature: {body_json.get('signature')}")
        print(f"All body keys: {list(body_json.keys())}")

        # Construye y muestra la cadena exacta que se está verificando
        cadena_para_firma = f"{body_json.get('transaction_id')}-{CIFRADO_SECRET}-{body_json.get('generation_date')}"
        print(f"Cadena construida para verificación: '{cadena_para_firma}'")

        # Construir la cadena para verificación
        cadena_para_firma = f"{transaction_id}-{CIFRADO_SECRET}-{generation_date}"
        cadena_para_firma_bytes = cadena_para_firma.encode("utf-8")
        
        print(f"Cadena para verificación: {cadena_para_firma}")
        print(f"Firma recibida: {firma_enviada}")
        print(f"Secret usado: {CIFRADO_SECRET}")

        # Verificar la firma con bcrypt
        try:
            # bcrypt.checkpw espera que ambos parámetros estén en bytes
            if bcrypt.checkpw(cadena_para_firma_bytes, firma_enviada.encode("utf-8")):
                print("✅ Firma válida - Coincide")
                
                # Extraer información de files
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
                print("❌ Firma inválida - no coincide")
                print(f"Se esperaba: {cadena_para_firma}")
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