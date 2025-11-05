import boto3
import io
import pandas as pd
import unicodedata
import re

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


def transform_mp_report_data(event):
    key = event['key']  # ya incluye carpeta (raw/)
    s3_client = boto3.client('s3')
    bucket_name = 'mercadopago-reports'

    print(f"📄 Procesando archivo: {key}")
    s3_filename = key.split('/')[-1]
    s3_report_file_name, report_date_hour, report_date = format_report_file_name(s3_filename)

    # Mover y convertir archivo
    new_key = move_to_processed(s3_client, key, bucket_name)

    print(f"🗓️ Fecha del reporte: {report_date}")
    return new_key, report_date


def lambda_handler(event, context):
    try:
        new_key, report_date = transform_mp_report_data(event)
        return {
            "etl_flow": 'MP',
            "bucket": 'mercadopago-reports',
            "key": new_key,
            "report_date": report_date
        }
    except Exception as e:
        print("⚠️ Error en lambda_handler:", str(e))
        raise Exception(str(e))
