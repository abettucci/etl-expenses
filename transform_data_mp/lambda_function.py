import boto3
import io
import pandas as pd
import unicodedata
import re
import requests

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
        # "settlement-279729559-2025-04-14-014721_2025-04-13_51102371.csv"
        base = s3_filename.rsplit("_", 1)[0]  # hasta antes del último _
        report_id = s3_filename.rsplit("_", 1)[-1].rsplit(".", 1)[0]
        report_date = s3_filename.split("_")[-2]
    else:
        # Caso 2: formato manual
        # "settlement-279729559-manual-2025-08-22-111914.csv"
        base = s3_filename.rsplit("-", 1)[0]  # hasta antes del último "-"
        report_id = s3_filename.rsplit("-", 1)[-1].rsplit(".", 1)[0]

        # Buscar la fecha con regex
        match = re.search(r"\d{4}-\d{2}-\d{2}", s3_filename)
        report_date = match.group(0) if match else None

    report_file_name = f"{base}.{extension}"
    return report_file_name, report_id, report_date

def move_to_processed(s3_client, file_key, bucket_name):
    destination_folder = 'processed/'
    try:
        # Leer archivo desde S3
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = obj['Body'].read()

        # Detectar formato
        if file_key.endswith('.csv'):
            report_df = pd.read_csv(io.BytesIO(content), encoding='utf-8', delimiter=',')
        elif file_key.endswith('.xlsx'):
            report_df = pd.read_excel(io.BytesIO(content))
        else:
            raise Exception("Formato no soportado: debe ser .csv o .xlsx")

        # Normalizar nombres de columnas
        report_df.columns = [normalize_columns_auto(col) for col in report_df.columns]

        # Convertir a CSV en memoria
        csv_buffer = io.BytesIO()
        report_df.to_csv(csv_buffer, sep=',', index=False, encoding='utf-8')

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

        # Eliminar archivo original (opcional)
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=file_key)
            print(f"🧹 Archivo original eliminado: {file_key}")
        except Exception as e:
            print(f"⚠️ No se pudo eliminar el archivo original: {str(e)}")

        print(f"✅ Archivo convertido a CSV y movido: {file_key} -> {new_key}")

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
    bucket_name = 'mercadopago-reports'

    print(f"📄 Procesando archivo: {key}")
    s3_filename = key.split('/')[-1]

    access_token = auth_mp()
    report_id, file_type = get_report_id(s3_filename, access_token)

    print('report_id: ', report_id)

    s3_report_file_name, report_date_hour, report_date = format_report_file_name(s3_filename)
    
    # Mover y convertir archivo
    new_key = move_to_processed(s3_client, key, bucket_name)

    print(f"🗓️ Fecha del reporte: {report_date}")
    return new_key, report_date, report_id


def lambda_handler(event, context):
    try:
        new_key, report_date, report_id = transform_mp_report_data(event)
        return {
            "etl_flow": 'MP',
            "bucket": 'mercadopago-reports',
            "key": new_key,
            "report_date": report_date,
            "report_id": report_id
        }
    except Exception as e:
        print("⚠️ Error en lambda_handler:", str(e))
        raise Exception(str(e))

event = {
  "key": "raw/settlement-279729559-2024-03-04-011706.xlsx"
}
transform_mp_report_data(event)