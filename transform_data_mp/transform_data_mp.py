import boto3
import io
import json
import pandas as pd

def format_report_file_name(s3_filename):
    base = s3_filename.rsplit('_', 1)[0]
    extension = s3_filename.split('.')[-1]
    report_file_name = f"{base}.{extension}"
    
    report_id = s3_filename.rsplit('_', 1)[-1].rsplit('.', 1)[0]

    parts = s3_filename.rsplit('_', 2)
    report_date = parts[-2]

    return report_file_name, report_id, report_date

def move_to_processed(s3_client, file_key, bucket_name):
    # Mover archivo de /Raw a /Processed
    destination_folder = 'processed/'        
    try:
        filename = file_key.split('/')[-1]
        new_key = destination_folder + filename
        
        # Copiar y eliminar en una sola operación (más eficiente)
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': file_key},
            Key=new_key
        )
        s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        
        print(f"PDF movido: {file_key} -> {new_key}")
    except Exception as e:
            print(f"Error al mover {file_key}: {str(e)}")

def transform_mp_report_data():    
    # Conexion a  S3
    s3_client = boto3.client('s3')
    bucket_name = 'mercadopago-reports'
    folder = 'raw/'
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
    csvs = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
    xlsx = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.xlsx')]

    for csv_file in csvs:
        print('Nombre archivo leido: ', csv_file)
        print(f"📄 Procesando: {csv_file}")
        obj = s3_client.get_object(Bucket=bucket_name, Key=csv_file)
        content = obj['Body'].read()
        report_df = pd.read_csv(io.BytesIO(content), encoding='utf-8', delimiter=';')
        s3_filename = csv_file.split('/')[-1]
        s3_report_file_name, report_id, report_date = format_report_file_name(s3_filename)
        move_to_processed(s3_client, csv_file, bucket_name)

    for xlsx_file in xlsx:
        print('Nombre archivo leido: ', xlsx_file)
        print(f"📄 Procesando: {xlsx_file}")
        obj = s3_client.get_object(Bucket=bucket_name, Key=xlsx_file)
        content = obj['Body'].read()
        report_df = pd.read_excel(io.BytesIO(content))
        s3_filename = xlsx_file.split('/')[-1]
        s3_report_file_name, report_id, report_date = format_report_file_name(s3_filename)
        move_to_processed(s3_client, xlsx_file, bucket_name)

def lambda_handler(event,context):
    try:
        transform_mp_report_data()
    except Exception as e:
        print("⚠️ Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }