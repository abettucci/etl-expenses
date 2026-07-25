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
    """Busca un label en el cuadro y retorna el siguiente valor."""
    try:
        idx = cuadro.index(label)
        return cuadro[idx + 1]
    except ValueError:
        return None

def find_val_with_colon(cuadro, label):
    """
    Busca un label que puede estar seguido de ':' en el mismo string o separado.
    Ej: "Nombre y apellido:" seguido de "Bettucci Agustin"
    O: "Nombre y apellido" seguido de ":" seguido de "Bettucci Agustin"
    """
    for i, item in enumerate(cuadro):
        # Caso 1: El label termina con ":" y el valor está en el siguiente item
        if item.strip().lower().startswith(label.lower()):
            # Si el item contiene ":" y hay algo después
            if ':' in item:
                parts = item.split(':', 1)
                if len(parts) > 1 and parts[1].strip():
                    return parts[1].strip()
                # Si no hay nada después del ":", el valor está en el siguiente item
                elif i + 1 < len(cuadro):
                    return cuadro[i + 1].strip()
            # Si no tiene ":", el valor está en el siguiente item
            elif i + 1 < len(cuadro):
                next_item = cuadro[i + 1]
                # Si el siguiente es ":", el valor está en i+2
                if next_item == ':' and i + 2 < len(cuadro):
                    return cuadro[i + 2].strip()
                return next_item.strip()
    return None

def extract_monto_from_text(cuadro):
    """Extrae el monto de la frase 'Ya enviamos tu transferencia de $X'"""
    for i, item in enumerate(cuadro):
        if "Ya enviamos tu transferencia de" in item:
            # El monto puede estar en el mismo string o en el siguiente
            if "$" in item:
                # Extraer todo después de "de"
                parts = item.split("de")
                if len(parts) > 1:
                    return parts[-1].strip()
            # Si no, buscar en el siguiente item
            elif i + 1 < len(cuadro):
                return cuadro[i + 1].strip()
    return None
            
def parse_mail(json_obj):
    soup = BeautifulSoup(json_obj.get("html_body", ""), "html.parser")
    cuadro = list(soup.stripped_strings)
    
    print('json_obj: ', json_obj)
    print(f'🔍 Cuadro extraído ({len(cuadro)} items): {cuadro[:30]}...')  # Debug: mostrar primeros 30 items

    date = json_obj.get("date", "")
    
    # Extraer monto
    monto_raw = extract_monto_from_text(cuadro)
    print(f'🔍 Monto raw extraído: {monto_raw}')
    monto = parse_monto(monto_raw)
    print(f'🔍 Monto parseado: {monto}')
    
    # Extraer otros campos (pueden tener ":" en el label)
    entidad = find_val_with_colon(cuadro, "Entidad")
    print(f'🔍 Entidad: {entidad}')
    
    nro_cuenta = find_val_with_colon(cuadro, "Número de cuenta")
    print(f'🔍 Número de cuenta: {nro_cuenta}')
    
    receptor = find_val_with_colon(cuadro, "Nombre y apellido")
    print(f'🔍 Receptor: {receptor}')

    if not (entidad and nro_cuenta and receptor and monto):
        print(f"❌ Faltan campos requeridos para crear el ID. entidad={entidad}, nro_cuenta={nro_cuenta}, receptor={receptor}, monto={monto}")
        return None
    
    base_str = f"{date}_{monto}_{receptor}_{nro_cuenta}"
    id_hash = hashlib.md5(base_str.encode('utf-8')).hexdigest()
    
    return {
        "id": id_hash,
        "message_id": json_obj["message_id"],
        "nro_cuenta": nro_cuenta,
        "receptor": receptor,
        "entidad": entidad,
        "monto": monto,
        "date" : date,
        "extraido_en": datetime.now().isoformat()
    }

def transform_mp_transfers_data(s3_key):
    destination_folder = 'processed/'
    s3_client = boto3.client('s3')

    print(f"📄 Procesando: {s3_key}")
    
    obj = s3_client.get_object(Bucket=MP_TRANSFER_BUCKET_NAME, Key=s3_key)
    content = json.loads(obj['Body'].read().decode('utf-8'))
    records = parse_mail(content)
    
    if records is None:
        raise Exception(f"No se pudieron extraer los datos del email en {s3_key}. Revisar formato del HTML.")
    
    df = pd.DataFrame([records])

    # Convertir el DataFrame a CSV en memoria
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Usar solo la fecha (YYYY-MM-DD) para el nombre del archivo
    date_part = records['date'][:10] if records.get('date') else 'unknown-date'
    new_key = f"{destination_folder}{date_part}-{records['message_id']}.csv"
    
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=MP_TRANSFER_BUCKET_NAME, Key=new_key)
    print(f"✅ Archivo subido como csv a S3/{new_key}")

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