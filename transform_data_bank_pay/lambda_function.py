import boto3
import json
import pandas as pd
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime 
import io
import os

BANK_BUCKET = os.environ['BANK_BUCKET']
BANK_TRANSFER_BUCKET = os.environ.get('BANK_TRANSFER_BUCKET', BANK_BUCKET)

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

def parse_transfer_mail(json_obj):
    """Parsea emails de 'Aviso de transferencia' de Santander"""
    soup = BeautifulSoup(json_obj.get("html_body", ""), "html.parser")
    cuadro = list(soup.stripped_strings)
    
    date = json_obj.get("date", "")
    subject = json_obj.get("subject", "")
    
    # Buscar los campos específicos de transferencia
    destinatario = find_val(cuadro, "Destinatario")
    cuenta_origen = find_val(cuadro, "Cuenta de origen")
    cbu_destino = find_val(cuadro, "CBU de Destino")
    importe_raw = find_val(cuadro, "Importe")
    nro_comprobante = find_val(cuadro, "Número de comprobante")
    
    # Parsear importe
    importe = parse_monto(importe_raw)
    divisa = "USD" if importe_raw and "U$S" in importe_raw else "ARS" if importe_raw and "$" in importe_raw else None
    
    if not (destinatario and importe and nro_comprobante):
        print(f"❌ Faltan campos requeridos para transferencia. destinatario={destinatario}, importe={importe}, nro_comprobante={nro_comprobante}")
        print(f"Contenido del email: {cuadro[:50]}...")
        return None
    
    # Generar ID único basado en número de comprobante
    base_str = f"{date}_{nro_comprobante}_{importe}_{destinatario}"
    id_hash = hashlib.md5(base_str.encode('utf-8')).hexdigest()
    
    return {
        "id": id_hash,
        "message_id": json_obj["message_id"],
        "destinatario": destinatario,
        "cuenta_origen": cuenta_origen,
        "cbu_destino": cbu_destino,
        "importe": importe,
        "divisa": divisa,
        "nro_comprobante": nro_comprobante,
        "date": date,
        "extraido_en": datetime.now().isoformat()
    }
            
def parse_payment_mail(json_obj):
    """Parsea emails de 'Pagaste' y 'Aviso de débito automático' de Santander"""
    soup = BeautifulSoup(json_obj.get("html_body", ""), "html.parser")
    cuadro = list(soup.stripped_strings)
    
    date = json_obj.get("date", "")
    monto_raw = find_val(cuadro, "Monto")
    divisa = "USD" if monto_raw and "U$S" in monto_raw else "ARS" if monto_raw and "$" in monto_raw else None
    monto = parse_monto(monto_raw)
    fecha = find_val(cuadro, "Fecha")
    hora = find_val(cuadro, "Hora")
    comercio = find_val(cuadro, "Comercio")

    nro_tarjeta = None
    for i, text in enumerate(cuadro):
        if text.startswith("terminada en"):
            try:
                nro_tarjeta = cuadro[i + 1]
            except IndexError:
                nro_tarjeta = None

    if not (fecha and hora and comercio and monto and nro_tarjeta and divisa):
        print("❌ Faltan campos requeridos para crear el ID.")
        return None
    
    base_str = f"{fecha}_{hora}_{monto}_{comercio}_{nro_tarjeta or ''}_{divisa}"
    id_hash = hashlib.md5(base_str.encode('utf-8')).hexdigest()
    
    return {
        "id": id_hash,
        "message_id": json_obj["message_id"],
        "fecha_pago": fecha,
        "hora_pago": hora,
        "tarjeta": next((t for t in cuadro if "Tarjeta Santander" in t), None),
        "nro_tarjeta": nro_tarjeta,
        "comercio": comercio,
        "cuotas": int(find_val(cuadro, "Cuotas") or 1),
        "monto": monto,
        "divisa": divisa,
        "date": date,
        "extraido_en": datetime.now().isoformat()
    }

def transform_bank_payments_data(s3_key, bucket=None):
    """Transforma emails de pagos con tarjeta Santander"""
    destination_folder = 'processed/'
    s3_client = boto3.client('s3')
    bucket = bucket or BANK_BUCKET

    obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
    content = json.loads(obj['Body'].read().decode('utf-8'))
    records = parse_payment_mail(content)
    
    if records is None:
        raise Exception("No se pudieron extraer los campos del email de pago")
    
    df = pd.DataFrame([records])

    print(f"📄 Procesando pago: {s3_key}")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    new_key = f"{destination_folder}{records['date']}-{records['message_id']}.csv"
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=new_key)
    print(f"✅ Archivo subido como csv a S3/{new_key}")

    return new_key

def transform_bank_transfer_data(s3_key, bucket=None):
    """Transforma emails de transferencias bancarias Santander"""
    destination_folder = 'processed/'
    s3_client = boto3.client('s3')
    bucket = bucket or BANK_TRANSFER_BUCKET

    obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
    content = json.loads(obj['Body'].read().decode('utf-8'))
    records = parse_transfer_mail(content)
    
    if records is None:
        raise Exception("No se pudieron extraer los campos del email de transferencia")
    
    df = pd.DataFrame([records])

    print(f"📄 Procesando transferencia: {s3_key}")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    new_key = f"{destination_folder}{records['date']}-{records['message_id']}.csv"
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=new_key)
    print(f"✅ Archivo subido como csv a S3/{new_key}")

    return new_key

def lambda_handler(event, context):
    try:
        s3_file_to_transform = event['key']
        etl_flow = event.get('etl_flow', 'BANK')
        
        if etl_flow == 'BANK_TRANSFER':
            bucket = event.get('bucket', BANK_TRANSFER_BUCKET)
            key = transform_bank_transfer_data(s3_file_to_transform, bucket)
            return {
                "statusCode": 200,
                "body": {
                    "etl_flow": 'BANK_TRANSFER',
                    "bucket": bucket,
                    "key": key
                }
            }
        else:
            bucket = event.get('bucket', BANK_BUCKET)
            key = transform_bank_payments_data(s3_file_to_transform, bucket)
            return {
                "statusCode": 200,
                "body": {
                    "etl_flow": 'BANK',
                    "bucket": bucket,
                    "key": key
                }
            }
    except Exception as e:
        print("⚠️ Error:", str(e))
        raise Exception(str(e))