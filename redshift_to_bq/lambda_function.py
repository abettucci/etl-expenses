import boto3
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import GoogleAPICallError
import io
import time
import json
import datetime
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
    print(df.dtypes)

    if table_name == 'mp_data':
        type_mapping = {
            'EXTERNAL_REFERENCE': 'string',
            'SOURCE_ID': 'int64',
            'USER_ID': 'int64',
            'PAYMENT_METHOD_TYPE': 'string',
            'PAYMENT_METHOD': 'string',
            'SITE': 'string',
            'TRANSACTION_TYPE': 'string',
            'TRANSACTION_AMOUNT': 'float64',
            'TRANSACTION_CURRENCY': 'string',
            'SELLER_AMOUNT': 'float64',
            'TRANSACTION_DATE': 'datetime64[ns]',   # estaba como object, asumo que es fecha
            'FEE_AMOUNT': 'float64',
            'SETTLEMENT_NET_AMOUNT': 'float64',
            'SETTLEMENT_CURRENCY': 'string',
            'SETTLEMENT_DATE': 'datetime64[ns]',    # estaba como object, lo paso a fecha
            'REAL_AMOUNT': 'float64',
            'COUPON_AMOUNT': 'float64',
            'METADATA': 'string',
            'MKP_FEE_AMOUNT': 'float64',
            'FINANCING_FEE_AMOUNT': 'float64',
            'SHIPPING_FEE_AMOUNT': 'float64',
            'TAXES_AMOUNT': 'float64',
            'INSTALLMENTS': 'int64',
            'TAX_DETAIL': 'float64',
            'POS_ID': 'float64',
            'STORE_ID': 'float64',
            'STORE_NAME': 'float64',
            'EXTERNAL_POS_ID': 'float64',
            'POS_NAME': 'float64',
            'EXTERNAL_STORE_ID': 'float64',
            'ORDER_ID': 'float64',
            'SHIPPING_ID': 'float64',
            'SHIPMENT_MODE': 'float64',
            'PACK_ID': 'float64',
            'TAXES_DISAGGREGATED': 'string',
            'POI_ID': 'float64',
            'POI_WALLET_NAME': 'float64',
            'POI_BANK_NAME': 'float64',
            'CARD_INITIAL_NUMBER': 'float64',
            'OPERATION_TAGS': 'float64',
            'PAYER_ID_TYPE': 'string',
            'PAYER_ID_NUMBER': 'float64',
            'PAYER_NAME': 'string',
            'BUSINESS_UNIT': 'string',
            'SUB_UNIT': 'string',
            'MONEY_RELEASE_DATE': 'datetime64[ns]',  # estaba como object, lo paso a fecha
            'PRODUCT_SKU': 'float64',
            'SALE_DETAIL': 'float64'
        }

    elif table_name == 'bank_payments':
        type_mapping = {
            'comercio' : 'string',
            'cuotas': 'int64',
            'extraido_en': 'datetime64[ns]',
            'fecha_pago': 'date',            # solo fecha
            'hora_pago': 'string',
            'id' : 'string',
            'message_id' : 'string',
            'monto': 'float64',
            'nro_tarjeta' : 'string',
            'tarjeta' : 'string',
        }
    else: # carrefour_data
        type_mapping = {
            'nro_ticket': 'int64',
            'fecha': 'date',                # solo fecha
            'categ': 'string',
            'prod': 'string',
            'cant': 'int64',
            'peso': 'float64',
            'p_unit': 'float64',
            'p_total': 'float64',
            'total_ticket_bruto': 'float64',
            'total_ticket_meli': 'float64'
        }
    
    audit_cols_mappings = {
        'INS_DTTM': 'datetime64[ns]',
        'UPD_DTTM': 'datetime64[ns]'
    }
    type_mapping.update(audit_cols_mappings)

    for col in df.columns:
        if col in type_mapping:
            target_type = type_mapping[col]
            try:
                if target_type == 'date':
                    # convertir a datetime y luego quedarnos solo con la fecha
                    df[col] = pd.to_datetime(df[col]).dt.date
                elif target_type.startswith('datetime'):
                    if col == 'fecha' and table_name == 'carrefour_data':
                        df[col] = pd.to_datetime(df[col], format='%d/%m/%y')
                    else:
                        df[col] = pd.to_datetime(df[col])
                elif target_type == 'string':
                    df[col] = df[col].astype('str')
                else:
                    df[col] = df[col].astype(target_type)
            except (ValueError, TypeError) as e:
                print(f"⚠️ No se pudo convertir la columna {col} a {target_type}: {e}")
                continue
        else: # si una columna no esta en el mapping, la considero string por default
            df[col] = df[col].astype('str')
    
    return df

def table_merge_staging_to_production_bq(bq_client, update_columns, target_table, source_table, pk, tbl_project_dataset):
    target_table = f'{tbl_project_dataset}.{target_table}'

    # Get the target table schema to identify TIMESTAMP columns
    target_table_ref = bq_client.get_table(target_table)
    timestamp_cols = [field.name for field in target_table_ref.schema 
                     if field.field_type == 'TIMESTAMP']

    # Create the SET clause with CAST for TIMESTAMP columns
    set_clause_parts = []
    for col in update_columns:
        if col.upper() not in timestamp_cols:
            set_clause_parts.append(f"{col.upper()} = S.{col.upper()}")
    set_clause = ", ".join(set_clause_parts + ["UPD_DTTM = CURRENT_TIMESTAMP()"])
    
    # Create the INSERT clause with CAST for TIMESTAMP columns
    insert_cols = ", ".join([pk] + update_columns + timestamp_cols)

    insert_vals_parts = [f"S.{pk}"]
    for col in update_columns:
        if col.upper() not in timestamp_cols:
            insert_vals_parts.append(f"S.{col.upper()}")

    insert_vals_parts.append("CURRENT_TIMESTAMP()")
    insert_vals_parts.append("CURRENT_TIMESTAMP()")
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

def check_exists_and_prepare_schema_for_bq(df, client, tabla, staging_table_id):
    df = convert_column_types(df, tabla)

    table_has_data = False
    table_exists = False
    try:
        table = client.get_table(staging_table_id)
        table_exists = True
        if table.num_rows > 0:
            table_has_data = True
            print(f"La tabla {staging_table_id} tiene {table.num_rows} filas.")
        else:
            table_has_data = False
            print(f"La tabla {staging_table_id} está vacía.")
    except NotFound:
        table_exists = False

    for col in ["INS_DTTM", "UPD_DTTM"]:
        if col not in df.columns:
            if col.lower() not in df.columns:
                df[col] = pd.Timestamp.now(tz='UTC')
            else:
                # existe pero en miinuscula
                df[col] = df[col].upper()

    print(df.dtypes)

    # Si no se provee schema, inferirlo del DataFrame
    schema = build_bq_schema_from_df(df)
    print('schema: ', schema)
    
    return df, schema, table_exists, table_has_data

def build_bq_schema_from_df(df):
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
        elif all(isinstance(x, datetime.date) and not isinstance(x, datetime.datetime) for x in df[column].dropna()):
            # ojo: datetime.date pero NO datetime.datetime
            bq_type = "DATE"
        else:
            bq_type = "STRING"  # default

        schema.append(bigquery.SchemaField(column, bq_type))

    return schema

def get_df_from_redshift_table(redshift_data, table_name):
    response = redshift_data.execute_statement(
        Database='dev',
        WorkgroupName='pdf-etl-workgroup',
        Sql= f"SELECT * FROM {table_name}"
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

    return df

def upload_dataframe_to_bigquery(client, df, table_id, schema=None):    
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Para saltar el header
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True if schema is None else False,
        schema=schema
    )
    
    # Convertir StringIO a BytesIO (load_table_from_file espera bytes)
    bytes_buffer = io.BytesIO(buffer.getvalue().encode('utf-8'))
    
    job = client.load_table_from_file(
        bytes_buffer, 
        table_id, 
        job_config=job_config
    )
    job.result()
    
    print(f"Cargados {df.shape[0]} filas en {table_id}")

def lambda_handler(event, context):
    try:
        tabla = event["table_name"]

        creds = auth_google('gcp_api_credentials')
        project_id = 'hazel-pillar-400222'
        
        stg_dataset_id = 'STG'
        stg_project_dataset = f'{project_id}.{stg_dataset_id}'
        staging_table_id = f'{stg_project_dataset}.{tabla}'

        tbl_dataset_id = 'TBL'
        tbl_project_dataset = f'{project_id}.{tbl_dataset_id}'
        prod_table_id = f'{tbl_project_dataset}.{tabla}'

        client = bigquery.Client(credentials=creds, project=f'{project_id}')
        redshift_data = boto3.client('redshift-data')
        
        df = get_df_from_redshift_table(redshift_data, tabla)
        
        ################# TABLA  STAGING ######################
        df, schema, table_exists, table_has_data = check_exists_and_prepare_schema_for_bq(df, client, tabla, staging_table_id)        

        # Si no existe la tabla, la creamos de forma dinamica con las columnas del dataframe y luego transferimos los datos de redshift a bigquery a traves de pandas df
        if not table_exists:
            print(f"🆕 La tabla en staging {staging_table_id} no existe. Creándola...")
            upload_dataframe_to_bigquery(client, df, staging_table_id, schema)
        # Si la tabla ya existe, no deberia tener datos porque siempre hacemos un TRUNC TABLE despues de mergear la tabla de staging con la productiva
        # Procedemos a cargarle los datos del dataframe
        else:
            print("📊 La tabla existe pero no tiene datos, ejecutamos insert")
            upload_dataframe_to_bigquery(client, df, staging_table_id, schema)
        print("✅ Datos cargados")

        ################# TABLA  PRODUCTIVA ######################
        df, schema, table_exists, table_has_data = check_exists_and_prepare_schema_for_bq(df, client, tabla, prod_table_id)    

        # Crear tabla productiva si no existe
        if not table_exists:
            print(f"🆕 La tabla en prod {prod_table_id} no existe. Creándola...")
            upload_dataframe_to_bigquery(client, df, prod_table_id, schema)
            print(f"🆕 La tabla en prod {prod_table_id} no existe. Creándola...")

        # Si la tabla ya existe, aunque ya tuviera datos, le insertamos los datos de nuevo, luego hacemos un merge y se evitan los duplicados
        # y tambien se trunca la tabla de staging. Aca solo transferimos los datos de redshift a bigquery a traves de pandas df.
        else:
            upload_dataframe_to_bigquery(client, df, prod_table_id, schema)
            print(f"✅ Tabla productiva {prod_table_id} creada.")

        ################# MERGE STAGING => PROD ######################
        # Hacemos el merge de la tabla de staging de BQ a la tabla productiva de BQ
        if tabla == 'mp_data':
            pk = 'report_id'
        elif tabla == 'carrefour_data':
            pk = 'nro_ticket'
        elif tabla == 'dim_producto':
            pk = 'product_id'
        elif tabla == 'archivos_ingestados':
            pk = 'id'
        else: # tabla = bank_payments
            pk = 'id'

        update_columns = [col for col in df.columns if col not in [pk, 'INS_DTTM', 'UPD_DTTM']]
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