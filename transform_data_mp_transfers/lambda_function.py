import boto3
import json
import pandas as pd
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime 
import io
import os

MP_TRANSFER_BUCKET_NAME  = os.environ.get('MP_TRANSFER_BUCKET_NAME')

def parse_monto(monto_raw):
    if not monto_raw:
        return None
    limpio = monto_raw.strip()
    for prefijo in ["U$S", "USD", "US$", "ARS$", "AR$", "$"]:
        limpio = limpio.replace(prefijo, "")
    limpio = limpio.replace(".", "").replace(",", ".")
    try:
        return float(limpio)
    except ValueError:
        print(f"❌ Error al convertir monto: '{monto_raw}' -> '{limpio}'")
        return None

def find_val(cuadro, label):
    try:
        idx = cuadro.index(label)
        return cuadro[idx + 1]
    except ValueError:
        return None
            
def parse_mail(json_obj):
    soup = BeautifulSoup(json_obj.get("html_body", ""), "html.parser")
    cuadro = list(soup.stripped_strings)
    
    print('json_obj: ', json_obj)

    date = json_obj.get("date", "")
    monto_raw = find_val(cuadro, "Ya enviamos tu transferencia de")
    monto = parse_monto(monto_raw)
    entidad = find_val(cuadro, "Entidad")
    nro_cuenta = find_val(cuadro, "Número de cuenta")
    receptor = find_val(cuadro, "Nombre y apellido")

    if not (entidad and nro_cuenta and receptor and monto):
        print("❌ Faltan campos requeridos para crear el ID.")
        return None
    
    base_str = f"{date}_{monto}_{receptor}_{nro_cuenta}"
    id_hash = hashlib.md5(base_str.encode('utf-8')).hexdigest()
    
    return {
        "id": id_hash,
        "message_id": json_obj["message_id"],
        "nro_cuenta": nro_cuenta,
        "receptor": receptor,
        "monto": monto,
        "date" : date,
        "extraido_en": datetime.now().isoformat()
    }

def transform_mp_transfers_data(s3_key):
    prefix = 'raw/'
    destination_folder = 'processed/'
    s3_client = boto3.client('s3')

    obj = s3_client.get_object(Bucket=MP_TRANSFER_BUCKET_NAME, Key=s3_key)
    content = json.loads(obj['Body'].read().decode('utf-8'))
    records = parse_mail(content)
    df = pd.DataFrame([records])

    print(f"📄 Procesando: {s3_key}")
    try:
        # Convertir el DataFrame a CSV en memoria (no guardar en disco)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        new_key = f"{destination_folder}{records['date']}-{records['message_id']}.csv"
        s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=MP_TRANSFER_BUCKET_NAME, Key=new_key)
        print(f"✅ Archivo subido como csv a S3/{new_key}")

    except Exception as e:
        print(f"Error al procesar {s3_key}: {str(e)}")

    return new_key

def lambda_handler(event,context):
    try:
        s3_file_to_transform = event['key']
        key = transform_mp_transfers_data(s3_file_to_transform)
        return {
            "statusCode": 200,
            "body": {
                "etl_flow": 'MP_TRANSFER',
                "bucket": MP_TRANSFER_BUCKET_NAME,
                "key": key
            }
        }
    except Exception as e:
        print("⚠️ Error:", str(e))
        raise Exception(str(e))