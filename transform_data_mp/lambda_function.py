import boto3
import io
import json
import pandas as pd
import unicodedata

def normalize_columns_auto(column_name):
    # Elimina tildes y convierte a ASCII
    column_name = unicodedata.normalize('NFKD', column_name).encode('ASCII', 'ignore').decode('utf-8')
    # Reemplaza espacios por _
    column_name = column_name.replace(' ', '_')
    return column_name.upper()

def format_report_file_name(s3_filename):
    base = s3_filename.rsplit('_', 1)[0]
    extension = s3_filename.split('.')[-1]
    report_file_name = f"{base}.{extension}"
    
    report_id = s3_filename.rsplit('_', 1)[-1].rsplit('.', 1)[0]

    parts = s3_filename.rsplit('_', 2)
    report_date = parts[-2]

    return report_file_name, report_id, report_date

def move_to_processed(s3_client, file_key, bucket_name):
    destination_folder = 'processed/'        
    
    try:
        if file_key.endswith('.csv'):
            obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            content = obj['Body'].read()
            report_df = pd.read_csv(io.BytesIO(content), encoding='utf-8', delimiter=',')

        elif file_key.endswith('.xlsx'):
            obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            content = obj['Body'].read()
            report_df = pd.read_excel(io.BytesIO(content))
        
        report_df.columns = [normalize_columns_auto(col) for col in report_df.columns]
        csv_buffer = io.BytesIO()
        report_df.to_csv(csv_buffer, sep=',', index=False, encoding='utf-8')

        filename = file_key.split('/')[-1]
        new_key = destination_folder + filename
        
        s3_client.put_object(
            Body=csv_buffer.getvalue(), 
            Bucket=bucket_name, 
            Key=new_key,
            ContentType='text/csv'
        )
                
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
        s3_filename = csv_file.split('/')[-1]
        s3_report_file_name, report_id, report_date = format_report_file_name(s3_filename)
        move_to_processed(s3_client, csv_file, bucket_name)

    for xlsx_file in xlsx:
        print('Nombre archivo leido: ', xlsx_file)
        print(f"📄 Procesando: {xlsx_file}")
        s3_filename = xlsx_file.split('/')[-1]
        s3_report_file_name, report_id, report_date = format_report_file_name(s3_filename)
        move_to_processed(s3_client, xlsx_file, bucket_name)

    return s3_report_file_name

def lambda_handler(event,context):
    try:
        key = transform_mp_report_data()        
        return {
            "statusCode": 200,
            "body": {
                "etl_flow": 'MP',
                "bucket": 'mercadopago-reports',
                "key": key
            }
        }
    except Exception as e:
        print("⚠️ Error:", str(e))
        raise Exception(str(e))