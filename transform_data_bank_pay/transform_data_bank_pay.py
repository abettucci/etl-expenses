import boto3
import json
import pandas as pd
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime 
import io

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
        "date" : date,
        "extraido_en": datetime.now().isoformat()
    }

def transform_bank_payments_data():
    bucket_name = 'bank-payments'
    prefix = 'raw/'
    destination_folder = 'processed/'
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    raw_json = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]

    for key in raw_json:
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = json.loads(obj['Body'].read().decode('utf-8'))
        records = parse_mail(content)
        df = pd.DataFrame([records])

        print(f"📄 Procesando: {key}")
        try:
            # Convertir el DataFrame a CSV en memoria (no guardar en disco)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            new_key = f"{destination_folder}{records["date"]}-{records["message_id"]}.csv"

            # Subir el CSV a S3
            s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=new_key)
            print(f"✅ Archivo subido como csv a S3/{new_key}")
        except Exception as e:
                print(f"Error al procesar {key}: {str(e)}")
    
    return new_key

def lambda_handler(event,context):
    try:
        key = transform_bank_payments_data()
        return {
            "statusCode": 200,
            "body": {
                "etl_flow": 'BANK',
                "bucket": 'bank-payments',
                "key": key
            }
        }
    except Exception as e:
        print("⚠️ Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }