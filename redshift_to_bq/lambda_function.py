import boto3
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import time
import json
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# Funcion para obtener la API Key de Google Cloud y consumir la API de Gmail
def get_secret(SECRET_NAME, REGION_NAME):
    client = boto3.client('secretsmanager', region_name=REGION_NAME)
    response = client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(response['SecretString'])

def update_secret(updated_token_json, SECRET_NAME, REGION_NAME):
    client = boto3.client('secretsmanager', region_name=REGION_NAME)
    client.update_secret(
        SecretId=SECRET_NAME,
        SecretString=updated_token_json
    )

def auth_google(SECRET_NAME):
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly','https://www.googleapis.com/auth/bigquery']
    REGION_NAME = 'us-east-2'    
    token_info = get_secret(SECRET_NAME, REGION_NAME)
    creds = Credentials.from_authorized_user_info(token_info, SCOPES)
    
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        print("♻️ Token refrescado")

        # Guardar el token actualizado en Secrets Manager
        update_secret(creds.to_json(), SECRET_NAME, REGION_NAME)

    return creds

# Función para inferir y convertir tipos de datos
def convert_column_types(df, table_name):
    """
    Intenta convertir las columnas del dataframe a los tipos de datos apropiados
    basado en el nombre de la tabla y los nombres de las columnas.
    """
    if table_name == 'mp_data':
        # Mapeo de tipos para la tabla mp_data
        type_mapping = {
            'source_id': 'string',
            'report_id': 'string',
            'report_date': 'datetime64[ns]',
            'settlement_date': 'datetime64[ns]',
            'payment_method_type': 'string',
            'transaction_type': 'string',
            'transaction_amount': 'float64',
            'transaction_date': 'datetime64[ns]',
            'real_amount': 'float64',
            'pos_id': 'string',
            'store_id': 'string',
            'store_name': 'string',
            'payer_name': 'string',
            'business_unit': 'string',
            'sub_unit': 'string'
        }
    elif table_name == 'bank_payments':
        # Mapeo de tipos para la tabla bank_payments
        type_mapping = {
            'comercio' : 'string',
            'cuotas': 'int64',
            'extraido_en': 'datetime64[ns]',
            'fecha_pago': 'datetime64[ns]',
            'hora_pago': 'datetime64[ns]',
            'id' : 'string',
            'message_id' : 'string',
            'monto': 'float64',
            'nro_tarjeta' : 'string',
            'tarjeta' : 'string',
        }
    else: # market-tickets
        # Mapeo de tipos para la tabla carrefour_data
        type_mapping = {
            'nro_ticket': 'int64',
            'fecha': 'datetime64[ns]',
            'categ': 'string',
            'prod': 'string',
            'cant': 'int64',
            'peso': 'float64',
            'p_unit': 'float64',
            'p_total': 'float64',
            'total_ticket_bruto': 'float64',
            'total_ticket_meli': 'float64'
        }
    
    for col in df.columns:
        # Si tenemos un mapeo específico, lo usamos
        if col in type_mapping:
            target_type = type_mapping[col]
            try:
                if target_type.startswith('datetime'):
                    if col == 'fecha' and table_name == 'carrefour_data':
                        df[col] = pd.to_datetime(df[col], format='%d/%m/%y')
                    else:
                        df[col] = pd.to_datetime(df[col])
                else:
                    df[col] = df[col].astype(target_type)
            except (ValueError, TypeError) as e:
                print(f"⚠️ No se pudo convertir la columna {col} a {target_type}: {e}")
                # Mantenemos el tipo original si falla la conversión
                continue
        else:
            # Intento de inferencia automática para columnas no mapeadas
            try:
                # Primero intentamos convertir a numérico
                numeric_vals = pd.to_numeric(df[col], errors='raise')
                # Si todos los valores son enteros, usamos int64, sino float64
                if (numeric_vals % 1 == 0).all():
                    df[col] = numeric_vals.astype('int64')
                else:
                    df[col] = numeric_vals.astype('float64')
                print(f"✅ Convertida columna {col} a numérico")
                continue
            except (ValueError, TypeError):
                pass
            
            try:
                # Intentamos convertir a fecha/hora
                datetime_vals = pd.to_datetime(df[col], errors='raise')
                df[col] = datetime_vals
                print(f"✅ Convertida columna {col} a datetime")
                continue
            except (ValueError, TypeError):
                pass
            
            # Si no es numérico ni fecha, lo dejamos como string
            df[col] = df[col].astype('string')
    
    return df

def lambda_handler(event, context):
    try:
        redshift_data = boto3.client('redshift-data')
        tabla = event["tabla"]

        creds = auth_google('gcp_api_credentials')

        response = redshift_data.execute_statement(
            Database='dev',
            WorkgroupName='pdf-etl-workgroup',
            Sql= f"SELECT * FROM {tabla}"
        )

        query_id = response['Id']
        while True:
            status = redshift_data.describe_statement(Id=query_id)
            if status['Status'] == 'FAILED':
                print("❌ Query falló:", status['Error'])
                break
            elif status['Status'] == 'FINISHED':
                if status.get('HasResultSet'):
                    results = redshift_data.get_statement_result(Id=query_id)
                    break
                else:
                    print("⚠️ La query no devuelve resultados.")
                    break
            time.sleep(1)

        column_names = [col['name'] for col in results['ColumnMetadata']]
        parsed_rows = []
        for record in results['Records']:
            parsed_row = []
            for cell in record:
                # Extrae el primer valor del diccionario (stringValue, longValue, etc.)
                value = list(cell.values())[0]
                parsed_row.append(value)
            parsed_rows.append(dict(zip(column_names, parsed_row)))
        df = pd.DataFrame(parsed_rows)

        # Después de obtener tu dataframe df, antes de cargarlo a BigQuery:
        df = convert_column_types(df, tabla)

        project_id = 'hazel-pillar-400222'
        dataset_id = 'etl_expenses_no_redshift'

        client = bigquery.Client(credentials=creds, project=f'{project_id}')
        table_id = f'{project_id}.{dataset_id}.{tabla}'
        staging_table_id = f'{project_id}.{dataset_id}.{tabla}_staging'

        try:
            client.get_table(table_id)
            table_exists = True
        except NotFound:
            table_exists = False

        # Primero vemos si existe la tabla, si no existe la creamos de forma dinamica con las columnas del dataframe
        if not table_exists:
            print("🆕 La tabla no existe. Creándola...")
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_EMPTY",
                autodetect=True
            )
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            print("✅ Tabla creada y datos cargados.")

        # Si ya existe la tabla, hacemos un merge para no repetir registros y reducir el procesamiento innecesario
        else:
            print("🔁 Tabla ya existe. Cargando a staging y haciendo MERGE...")

            # 1. Cargar a tabla staging
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                autodetect=True
            )
            job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
            job.result()

            # 2. Ejecutar MERGE
            # Elegimos una o varias columnas clave para evitar duplicados
            # Ejemplo usando 'report_id' como clave
            if tabla == 'mp_data':
                pk = 'report_id'
            elif tabla == 'bank_payments':
                pk = 'id'
            else: # carrefour_data
                pk = 'nro_ticket'

            merge_sql = f"""
            MERGE `{table_id}` T
            USING `{staging_table_id}` S
            ON T.{pk} = S.{pk}
            WHEN NOT MATCHED THEN
            INSERT ROW
            """

            query_job = client.query(merge_sql)
            query_job.result()
            print("✅ MERGE ejecutado correctamente.")

    except Exception as e:
        print("⚠️ Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }