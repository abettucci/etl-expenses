import boto3
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import GoogleAPICallError
import time
import json
import warnings
import pyarrow
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

def table_merge_staging_to_production_bq(bq_client, update_columns, target_table, source_table, pk, tbl_project_dataset):
    target_table = f'{tbl_project_dataset}.{target_table}'

    # Get the target table schema to identify TIMESTAMP columns
    target_table_ref = bq_client.get_table(target_table)
    timestamp_cols = [field.name for field in target_table_ref.schema 
                     if field.field_type == 'TIMESTAMP']

    print(timestamp_cols)

    # Create the SET clause with CAST for TIMESTAMP columns
    set_clause_parts = []
    timestamp_cols_insert = timestamp_cols

    for col in update_columns:
        if col.upper() in timestamp_cols:
            timestamp_cols_insert -= col.upper()
            set_clause_parts.append(f"{col.upper()} = CAST(S.{col.upper()} AS TIMESTAMP)")
        else:
            set_clause_parts.append(f"{col.upper()} = S.{col.upper()}")
    set_clause = ", ".join(set_clause_parts + ["UPD_DTTM = CURRENT_TIMESTAMP()"])
    
    # Create the INSERT clause with CAST for TIMESTAMP columns
    insert_cols = ", ".join([pk] + update_columns + timestamp_cols_insert)
    insert_vals_parts = [f"S.{pk}"]

    timestamp_vals_insert = ["CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP()"]
    insert_vals_parts = []

    for col in update_columns:
        if col.upper() in timestamp_cols:
            # sacar el primer elemento de timestamp_vals_insert
            ts_val = timestamp_vals_insert.pop(0)
            insert_vals_parts.append(ts_val)  
        else:
            insert_vals_parts.append(f"S.{col.upper()}")

    insert_vals = ", ".join(insert_vals_parts)

    merge_sql = f"""
        MERGE INTO `{target_table}` T
        USING `{source_table}` S
        ON T.{pk} = S.{pk}
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
            """ 
    
    print('\n')
    print(merge_sql)

    try:
        job = bq_client.query(merge_sql)
        result = job.result()
        print(f"✅ Merge completado. Filas afectadas: {result.num_dml_affected_rows}")
        return True
    except GoogleAPICallError as e:
        print(f"❌ Error en MERGE: {str(e)}")
        return False
    except Exception as e:
        print(f"⚠️ Error inesperado: {str(e)}")
        return False

def lambda_handler(event, context):
    try:
        tabla = event["table_name"]

        creds = auth_google('gcp_api_credentials')
        project_id = 'hazel-pillar-400222'
        
        stg_dataset_id = 'STG'
        stg_project_dataset = f'{project_id}.{stg_dataset_id}'

        tbl_dataset_id = 'TBL'
        tbl_project_dataset = f'{project_id}.{tbl_dataset_id}'

        client = bigquery.Client(credentials=creds, project=f'{project_id}')

        redshift_data = boto3.client('redshift-data')
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
        df = convert_column_types(df, tabla)

        for column, dtype in df.dtypes.items():
            print(column, dtype)

        # Primero vemos si existe la tabla
        staging_table_id = f'{stg_project_dataset}.{tabla}'
        try:
            client.get_table(staging_table_id)
            table_exists = True
        except NotFound:
            table_exists = False

        # Si no existe la tabla, la creamos de forma dinamica con las columnas del dataframe y luego transferimos los datos de redshift a bigquery a traves de pandas df
        if not table_exists:
            print(f"🆕 La tabla en staging {staging_table_id} no existe. Creándola...")
            
            # Manejar advertencia de pandas-gbq
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=FutureWarning)
                
                for col in ["INS_DTTM", "UPD_DTTM"]:
                    if col not in df.columns:
                        if col.lower() not in df.columns:
                            df[col] = pd.Timestamp.now(tz='UTC')
                        else:
                            # existe pero en miinuscula
                            df[col] = df[col].upper()

                # Si no se provee schema, inferirlo del DataFrame
                schema = []
                for column, dtype in df.dtypes.items():
                    # Mapear tipos de pandas a BigQuery
                    if pd.api.types.is_integer_dtype(dtype):
                        bq_type = "INTEGER"
                    elif pd.api.types.is_float_dtype(dtype):
                        bq_type = "FLOAT"
                    elif pd.api.types.is_bool_dtype(dtype):
                        bq_type = "BOOLEAN"
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        bq_type = "TIMESTAMP"
                    else:
                        bq_type = "STRING"  # Default para otros tipos
                    
                    schema.append(bigquery.SchemaField(column, bq_type))
                        
            job_config = bigquery.LoadJobConfig(
                schema = schema,
                write_disposition="WRITE_EMPTY",
                autodetect=False
            )

            job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
            job.result()
            print("✅ Tabla creada y datos cargados.")

        # Si la tabla ya existe, solo transferimos los datos de redshift a bigquery a traves de pandas df
        else:
            # Creo que deberia hacer un merge aca?
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                autodetect=True
            )
            job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
            job.result()
            print("✅ Tabla ya existe, datos cargados.")
    
        # Hacemos el merge de la tabla de staging de BQ a la tabla productiva de BQ
        if tabla == 'mp_data':
            pk = 'REPORT_ID'
        elif tabla == 'carrefour_data':
            pk = 'nro_ticket'
        elif tabla == 'dim_producto':
            pk = 'product_id'
        elif tabla == 'archivos_ingestados':
            pk = 'id'
        else: # tabla = bank_payments
            pk = 'id'

        update_columns = [col for col in df.columns if col not in [pk, 'INS_DTTM', 'UPD_DTTM']]

        # Crear tabla productiva si no existe
        prod_table_id = f'{tbl_project_dataset}.{tabla}'
        try:
            client.get_table(prod_table_id)
            prod_table_exists = True
        except NotFound:
            prod_table_exists = False

        if not prod_table_exists:
            print(f"🆕 La tabla en produccion {prod_table_id} no existe. Creándola...")

            for col in ["INS_DTTM", "UPD_DTTM"]:
                if col not in df.columns:
                    if col.lower() not in df.columns:
                        df[col] = pd.Timestamp.now(tz='UTC')
                    else:
                        # existe pero en miinuscula
                        df[col] = df[col].upper()

            # Usar el mismo esquema que la tabla de staging
            schema = []
            for column, dtype in df.dtypes.items():
                if pd.api.types.is_integer_dtype(dtype):
                    bq_type = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    bq_type = "FLOAT"
                elif pd.api.types.is_bool_dtype(dtype):
                    bq_type = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    bq_type = "TIMESTAMP"
                else:
                    bq_type = "STRING"
                schema.append(bigquery.SchemaField(column, bq_type))

            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_EMPTY",
                autodetect=False
            )
            job = client.load_table_from_dataframe(df, prod_table_id, job_config=job_config)
            job.result()
            print(f"✅ Tabla productiva {prod_table_id} creada.")

        result = table_merge_staging_to_production_bq(client, update_columns, tabla, staging_table_id, pk, tbl_project_dataset)

        # Si el merge se ejecutó bien,
        if result:
            # Borramos los datos de la tabla de staging
            try:
                query = f"TRUNCATE TABLE `{staging_table_id}`"
                job = client.query(query)
                job.result()
                print(f"✅ Tabla {staging_table_id} truncada exitosamente de BigQuery")
                
            except GoogleAPICallError as e:
                print(f"❌ Error truncando la tabla {staging_table_id}: {str(e)}")

            # Y tambien borramos los datos de la tabla de redshift que se transfirieron
            try:
                redshift_data.execute_statement(
                    Database='dev',
                    WorkgroupName='pdf-etl-workgroup',
                    Sql=f"TRUNCATE TABLE {tabla}"
                )    
                print(f"✅ Tabla {tabla} truncada exitosamente de redshift")
            except Exception as e:
                print(f"❌ Error truncando la tabla {tabla} de redshift: {str(e)}") 

    except Exception as e:
        print("⚠️ Error:", str(e))
        raise Exception(str(e))

# event = {
#     "body": json.dumps({
#         "tabla": 'carrefour_data'
#     })
# }

# event = {"tabla": 'carrefour_data'}
# event = {"tabla": 'archivos_ingestados'}

# print(lambda_handler(event, ''))