import boto3
import io
import pandas as pd
import unicodedata
import re
import requests
import os
import csv

MP_REPORTS_BUCKET_NAME = os.environ['MP_REPORTS_BUCKET_NAME']

def auth_mp():
    # Cliente AWS SSM para Parameter Store
    ssm_client = boto3.client("ssm", region_name="us-east-2")
    PARAMETER_NAME = "/mercado_pago/token"

    # Obtener el parámetro desde AWS Parameter Store
    try:
        response = ssm_client.get_parameter(Name=PARAMETER_NAME, WithDecryption=True)
        access_token = response["Parameter"]["Value"]
        return access_token
    except ssm_client.exceptions.ParameterNotFound:
        raise Exception(f"El parámetro {PARAMETER_NAME} no existe en AWS Parameter Store.")

def normalize_columns_auto(column_name):
    # Elimina tildes y convierte a ASCII
    column_name = unicodedata.normalize('NFKD', column_name).encode('ASCII', 'ignore').decode('utf-8')
    # Reemplaza espacios por _
    column_name = column_name.replace(' ', '_')
    return column_name.upper()

def format_report_file_name(s3_filename):
    base = s3_filename.rsplit('_', 1)[0]
    extension = s3_filename.split('.')[-1]

    if "_" in s3_filename:
        # "settlement-<mp_user_id>-2025-04-14-014721_2025-04-13_51102371.csv"
        base = s3_filename.rsplit("_", 1)[0]  # hasta antes del último _
        report_id = s3_filename.rsplit("_", 1)[-1].rsplit(".", 1)[0]
        report_date = s3_filename.split("_")[-2]
    else:
        # Caso 2: formato manual
        # "settlement-<mp_user_id>-manual-2025-08-22-111914.csv"
        base = s3_filename.rsplit("-", 1)[0]  # hasta antes del último "-"
        report_id = s3_filename.rsplit("-", 1)[-1].rsplit(".", 1)[0]

        # Buscar la fecha con regex
        match = re.search(r"\d{4}-\d{2}-\d{2}", s3_filename)
        report_date = match.group(0) if match else None

    report_file_name = f"{base}.{extension}"
    return report_file_name, report_id, report_date

def fix_json_in_csv(content_str):
    """
    Preprocesa el CSV para escapar correctamente los campos JSON embebidos.
    MercadoPago genera CSVs con JSON mal escapado como: "[{"key":"value"}]"
    Esto lo convierte a un formato que pandas pueda parsear.
    
    Estrategia: Reemplazar los campos JSON problemáticos por una versión escapada.
    """
    lines = content_str.split('\n')
    fixed_lines = [lines[0]]  # Header no necesita fix
    
    for line in lines[1:]:
        if not line.strip():
            fixed_lines.append(line)
            continue
            
        # Patrón para encontrar campos JSON: ,"[{...}]", o ,"{...}",
        # El problema es que tienen comillas internas sin escapar
        
        # Reemplazar patrones de JSON array: "[{...}]"
        # Busca: ,"[{ seguido de cualquier cosa hasta }]",
        line = re.sub(
            r',"(\[\{.*?\}\])"(,|$)',
            lambda m: ',"' + m.group(1).replace('"', "'") + '"' + m.group(2),
            line
        )
        
        # Reemplazar patrones de JSON object simple: "{...}"
        line = re.sub(
            r',"(\{.*?\})"(,|$)',
            lambda m: ',"' + m.group(1).replace('"', "'") + '"' + m.group(2),
            line
        )
        
        # Reemplazar patrones de array vacío o simple: "[]"
        line = re.sub(
            r',"(\[\])"(,|$)',
            lambda m: ',"' + m.group(1) + '"' + m.group(2),
            line
        )
        
        fixed_lines.append(line)
    
    return '\n'.join(fixed_lines)
    
def move_to_processed(s3_client, file_key, bucket_name):
    destination_folder = 'processed/'
    try:
        # Leer archivo desde S3
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = obj['Body'].read()

        # Detectar formato
        if file_key.endswith('.csv'):
            # Decodificar el CSV
            content_str = content.decode('utf-8')
            
            # Debug: mostrar primeras líneas del CSV original
            lines = content_str.split('\n')
            print(f"📋 Header CSV: {lines[0]}")
            print(f"📋 Primera fila datos (original): {lines[1][:200] if len(lines) > 1 else 'N/A'}...")
            print(f"📋 Total columnas en header: {len(lines[0].split(','))}")
            
            # Preprocesar para arreglar JSON mal escapado
            content_str = fix_json_in_csv(content_str)
            
            # Debug: mostrar línea después del fix
            fixed_lines = content_str.split('\n')
            print(f"📋 Primera fila datos (fixed): {fixed_lines[1][:200] if len(fixed_lines) > 1 else 'N/A'}...")
            
            # Usar pandas con configuración robusta
            report_df = pd.read_csv(
                io.StringIO(content_str),
                sep=',',
                quotechar='"',
                doublequote=True,
                engine='python',
                dtype=str,
                keep_default_na=False,
                skipinitialspace=True
            )
            
            # Debug: verificar columnas parseadas
            print(f"📊 Columnas parseadas: {len(report_df.columns)}")
            print(f"📊 Filas parseadas: {len(report_df)}")
            if len(report_df) > 0:
                print(f"📊 Valores primera fila - EXTERNAL_REFERENCE: {report_df.iloc[0].get('EXTERNAL_REFERENCE', 'N/A')}")
                print(f"📊 Valores primera fila - SOURCE_ID: {report_df.iloc[0].get('SOURCE_ID', 'N/A')}")
                print(f"📊 Valores primera fila - TRANSACTION_TYPE: {report_df.iloc[0].get('TRANSACTION_TYPE', 'N/A')}")
            
        elif file_key.endswith('.xlsx'):
            report_df = pd.read_excel(io.BytesIO(content), dtype=str)
        else:
            raise Exception("Formato no soportado: debe ser .csv o .xlsx")

        # Normalizar nombres de columnas
        report_df.columns = [normalize_columns_auto(col) for col in report_df.columns]

        # Convertir a CSV en memoria
        csv_buffer = io.BytesIO()
        report_df.to_csv(
            csv_buffer,
            sep=',',
            index=False,
            encoding='utf-8',
            quoting=csv.QUOTE_MINIMAL,
            quotechar='"'
        )

        # Construir nuevo nombre de archivo (convertir a .csv si era .xlsx)
        filename = file_key.split('/')[-1]
        if filename.endswith('.xlsx'):
            filename = filename.rsplit('.', 1)[0] + '.csv'
        new_key = destination_folder + filename

        # Subir a S3 como CSV
        s3_client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=bucket_name,
            Key=new_key,
            ContentType='text/csv'
        )

        print(f"✅ Archivo convertido a CSV y cargado en {destination_folder}: {new_key}")

        return new_key

    except Exception as e:
        print(f"❌ Error al mover {file_key}: {str(e)}")
        raise

def get_report_id(my_file_name, access_token):
    url = "https://api.mercadopago.com/v1/account/settlement_report/list"
    headers = {"Authorization": "Bearer " + access_token}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return None
    else:
        data = response.json()  # Convertimos la respuesta a JSON
        match = next((item for item in data if item.get("file_name") == my_file_name), None)
        if match:
            return str(match.get('id',None)), 'csv'
        else:
            # Probamos con file format '.xlsx' para los casos en los que archivo original era xlsx y lo convertimos a csv para guardarlo en s3
            my_file_name = my_file_name.replace('.csv','.xlsx')
            match = next((item for item in data if item.get("file_name") == my_file_name), None)
            if match:
                return str(match.get('id',None)), 'xlsx'
            else:
                return None, None

def transform_mp_report_data(event):
    key = event['key']  # ya incluye carpeta (raw/)
    s3_client = boto3.client('s3')

    print(f"📄 Procesando archivo: {key}")
    s3_filename = key.split('/')[-1]

    access_token = auth_mp()
    report_id, file_type = get_report_id(s3_filename, access_token)

    print('report_id: ', report_id)

    s3_report_file_name, report_date_hour, report_date = format_report_file_name(s3_filename)
    
    # Mover y convertir archivo
    new_key = move_to_processed(s3_client, key, MP_REPORTS_BUCKET_NAME)

    print(f"🗓️ Fecha del reporte: {report_date}")
    return new_key, report_date, report_id

def lambda_handler(event, context):
    try:
        new_key, report_date, report_id = transform_mp_report_data(event)
        return {
            "etl_flow": 'MP',
            "bucket": MP_REPORTS_BUCKET_NAME,
            "key": new_key,
            "report_date": report_date,
            "report_id": report_id
        }
    except Exception as e:
        print("⚠️ Error en lambda_handler:", str(e))
        raise Exception(str(e))