import bcrypt
import json
import os
import boto3

def lambda_handler(event, context):
    step_function_arn = os.environ['STEP_FUNCTION_ARN']
    step_functions_client = boto3.client('stepfunctions')
    CIFRADO_SECRET = os.environ["CIFRADO_SECRET_MP"]
    
    # 1. Obtener el cuerpo del request
    raw_body = event["body"]
    body_json = json.loads(raw_body)

    print(body_json)

    try:
        # 2. Extraer los campos necesarios para la firma (Extraer parámetros del POST)
        try:
            transaction_id = body_json.get("transaction_id", "")
            generation_date = body_json.get("generation_date", "")
            firma_enviada = body_json.get("signature", "")
        except:
            if not transaction_id:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "Faltan el campo requerido transaction_id"})
                }
            elif not generation_date:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "Faltan el campo requerido generation_date"})
                }
            else: # not firma_enviada
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "Faltan el campo requerido firma_enviada"})
                }

        files = body_json.get("files", [])
        if files:  # me aseguro que no esté vacío
            file = files[0]  # primer elemento de la lista
            file_name = file.get("name", "")
            file_url = file.get("url", "")
            file_type = file.get("type", "")
        else:
            file_name = file_url = file_type = ""
        
        # Input que le pasás a la Step Function
        step_input = {
            "file_name" : file_name,
            "file_url": file_url,
            "file_type" : file_type
        }

        # 3. Construir la cadena exacta que MercadoPago usó para la firma
        cadena_para_firma = f"{transaction_id}-{CIFRADO_SECRET}-{generation_date}"
        cadena_para_firma_bytes = cadena_para_firma.encode("utf-8")

        print("🔑 Cadena para firma:", cadena_para_firma)

        # 4. Verificar con bcrypt.checkpw()
        if not bcrypt.checkpw(cadena_para_firma_bytes, firma_enviada.encode("utf-8")):
            print("❌ Firma inválida")
            return {
                "statusCode": 403,
                "body": json.dumps({"message": "Firma inválida"})
            }

        print("✅ Webhook válido!")
        
        response = step_functions_client.start_execution(
            stateMachineArn=step_function_arn,
            input=json.dumps(step_input)
        )
        
        print(response)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Step Function started successfully!')
        }
    except Exception as e:
        print(f"⚠️ Error al recibir webhook y enviar datos a step function: {e}")
        raise Exception(str(e))

