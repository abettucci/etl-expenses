import pandas as pd
import boto3
import io
import os
import json
import re
import unicodedata
from rapidfuzz import fuzz
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Configuración de BigQuery desde variables de entorno
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET_STAGING = os.environ.get("BQ_DATASET_STAGING", "STG")
BQ_DATASET_PROD = os.environ.get("BQ_DATASET_PROD", "PRD")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

# --------------------------
# Inicialización de BigQuery Client
# --------------------------
def get_bigquery_client():
    """Inicializa cliente de BigQuery con credenciales de Secrets Manager"""
    secrets_client = boto3.client('secretsmanager')
    secret_response = secrets_client.get_secret_value(SecretId='gcp_sa_api_credentials')
    credentials_json = json.loads(secret_response['SecretString'])
    
    credentials = service_account.Credentials.from_service_account_info(credentials_json)
    client = bigquery.Client(
        credentials=credentials,
        project=GCP_PROJECT_ID,
        location=BQ_LOCATION
    )
    return client

# --------------------------
# Utilidades BigQuery
# --------------------------
def bigquery_type(dtype):
    """Convierte tipo de pandas a tipo de BigQuery"""
    if pd.api.types.is_integer_dtype(dtype):
        return "INT64"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT64"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOL"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "STRING"

def create_bigquery_schema(df):
    """Genera schema de BigQuery desde DataFrame"""
    schema = []
    for col, dtype in zip(df.columns, df.dtypes):
        schema.append(
            bigquery.SchemaField(col, bigquery_type(dtype), mode="NULLABLE")
        )
    return schema

def load_to_staging(client, df, table_name):
    """
    Carga DataFrame a tabla staging en BigQuery
    Returns: número de filas cargadas
    """
    if df.empty:
        print(f"ℹ️ No hay filas para {table_name}")
        return 0
    
    staging_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.stg_{table_name}"
    
    print(f"📤 Cargando {len(df)} filas a staging: {staging_table_id}")
    
    # Verificar si la tabla existe
    try:
        client.get_table(staging_table_id)
        table_exists = True
        print(f"✅ Tabla staging existe: {staging_table_id}")
    except Exception:
        table_exists = False
        print(f"⚠️ Tabla staging no existe, se creará: {staging_table_id}")
    
    if table_exists:
        # Tabla existe: Truncar primero y luego usar WRITE_APPEND con schema_update_options
        truncate_query = f"TRUNCATE TABLE `{staging_table_id}`"
        client.query(truncate_query).result()
        print(f"🧹 Tabla staging truncada")
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ]
        )
    else:
        # Tabla no existe: usar WRITE_TRUNCATE con autodetect (sin schema_update_options)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
    
    # Cargar desde DataFrame
    job = client.load_table_from_dataframe(
        df, staging_table_id, job_config=job_config
    )
    
    # Esperar a que termine
    job.result()
    
    print(f"✅ Staging {table_name}: cargadas {len(df)} filas")
    return len(df)

def merge_to_prod(client, table_name, key_columns, all_columns):
    """
    Ejecuta MERGE desde staging a prod en BigQuery
    Solo inserta registros que no existen (basado en key_columns)
    """
    staging_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.stg_{table_name}"
    prod_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{table_name}"
    
    print(f"🔄 Ejecutando MERGE de staging a prod para {table_name}")
    
    # Construir condición de JOIN para las claves
    if key_columns:
        on_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])
    else:
        # Si no hay claves, insertar todo (no hay deduplicación)
        on_condition = "FALSE"  # Nunca matchea, siempre inserta
    
    # Construir lista de columnas para INSERT
    columns_list = ", ".join(all_columns)
    values_list = ", ".join([f"source.{c}" for c in all_columns])
    
    merge_query = f"""
    MERGE `{prod_table_id}` AS target
    USING `{staging_table_id}` AS source
    ON {on_condition}
    WHEN NOT MATCHED THEN
      INSERT ({columns_list})
      VALUES ({values_list})
    """
    
    print(f"📝 Query MERGE:\n{merge_query}")
    
    try:
        query_job = client.query(merge_query)
        query_job.result()  # Esperar a que termine
        
        print(f"✅ MERGE completado para {table_name}")
        print(f"   Filas modificadas: {query_job.num_dml_affected_rows}")
        
        return query_job.num_dml_affected_rows
    except Exception as e:
        print(f"⚠️ Error en MERGE: {str(e)}")
        # Si la tabla prod no existe, crear desde staging
        print(f"💡 Creando tabla prod desde staging...")
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{prod_table_id}` AS
        SELECT * FROM `{staging_table_id}` WHERE FALSE
        """
        client.query(create_query).result()
        
        # Reintentar MERGE
        query_job = client.query(merge_query)
        query_job.result()
        print(f"✅ MERGE completado después de crear tabla")
        return query_job.num_dml_affected_rows

def verify_table_count(client, dataset, table_name):
    """Verifica el conteo de registros en una tabla"""
    table_id = f"{GCP_PROJECT_ID}.{dataset}.{table_name}"
    
    try:
        query = f"SELECT COUNT(*) as total FROM `{table_id}`"
        result = client.query(query).result()
        count = list(result)[0]['total']
        print(f"🔍 Verificación: tabla {table_name} en {dataset} tiene {count} registros")
        return count
    except Exception as e:
        print(f"⚠️ No se pudo verificar {table_name}: {str(e)}")
        return 0

# --------------------------
# Funciones de utilidad (mantener de la versión anterior)
# --------------------------
def clean_column_name(col):
    """Limpia nombres de columnas para BigQuery"""
    col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode()
    col = re.sub(r'[^A-Za-z0-9_]+', '_', col)
    return col.upper().strip('_')

def generar_diccionario_normalizacion(productos, threshold=85):
    """Agrupa productos similares usando fuzzy matching"""
    grupos = {}
    normalizacion = {}

    for producto in productos:
        encontrado = False
        for canonico in grupos:
            if fuzz.ratio(producto, canonico) >= threshold:
                normalizacion[producto] = normalizacion[canonico]
                encontrado = True
                break
        if not encontrado:
            clave = slugify(producto)
            grupos[producto] = clave
            normalizacion[producto] = clave

    return normalizacion

def slugify(text):
    """Convierte texto a slug"""
    return re.sub(r'[^a-z0-9]+', '', text.lower()).strip('')

def column_name_mapping(df):
    """Mapea nombres de columnas de Mercado Pago a nombres estándar"""
    column_mapping = {
        "NUMERO_DE_IDENTIFICACION": "EXTERNAL_REFERENCE",
        "ID_DE_OPERACION_EN_MERCADO_PAGO": "SOURCE_ID",
        "CODIGO_DE_LA_CUENTA_DEL_VENDEDOR": "USER_ID",
        "TIPO_DE_MEDIO_DE_PAGO": "PAYMENT_METHOD_TYPE",
        "MEDIO_DE_PAGO": "PAYMENT_METHOD",
        "PAIS_DE_ORIGEN_DE_LA_CUENTA_DE_MERCADO_PAGO": "SITE",
        "TIPO_DE_OPERACION": "TRANSACTION_TYPE",
        "VALOR_DE_LA_COMPRA": "TRANSACTION_AMOUNT",
        "MONEDA": "TRANSACTION_CURRENCY",
        "MONTO_RECIBIDO_POR_COMPRAS_POR_SPLIT": "SELLER_AMOUNT",
        "FECHA_DE_ORIGEN": "TRANSACTION_DATE",
        "COMISION_MAS_IVA": "FEE_AMOUNT",
        "MONTO_NETO_DE_LA_OPERACION_QUE_IMPACTO_TU_DINERO": "SETTLEMENT_NET_AMOUNT",
        "MONEDA_DE_LA_LIQUIDACION": "SETTLEMENT_CURRENCY",
        "FECHA_DE_APROBACION": "SETTLEMENT_DATE",
        "MONTO_NETO_DE_OPERACION": "REAL_AMOUNT",
        "CUPON_DE_DESCUENTO": "COUPON_AMOUNT",
        "DATOS_EXTRA": "METADATA",
        "COMISION_DE_MERCADO_LIBRE_MAS_IVA": "MKP_FEE_AMOUNT",
        "COMISION_POR_OFRECER_CUOTAS_SIN_INTERES": "FINANCING_FEE_AMOUNT",
        "COSTO_DE_ENVIO": "SHIPPING_FEE_AMOUNT",
        "IMPUESTOS_COBRADOS_POR_RETENCIONES_IIBB": "TAXES_AMOUNT",
        "CUOTAS": "INSTALLMENTS",
        "DETALLE_DE_IMPUESTOS": "TAX_DETAIL",
        "ID_DE_CAJA": "POS_ID",
        "ID_DE_LA_SUCURSAL": "STORE_ID",
        "NOMBRE_DE_LA_SUCURSAL": "STORE_NAME",
        "ID_DE_CAJA_DEFINIDO_POR_EL_USUARIO": "EXTERNAL_POS_ID",
        "NOMBRE_DE_CAJA": "POS_NAME",
        "ID_DE_SUCURSAL_DEFINIDO_POR_EL_USUARIO": "EXTERNAL_STORE_ID",
        "ID_DE_LA_ORDEN": "ORDER_ID",
        "ID_DEL_ENVIO": "SHIPPING_ID",
        "MODO_DE_ENVIO": "SHIPMENT_MODE",
        "ID_DEL_PAQUETE": "PACK_ID",
        "IMPUESTOS_DESAGREGADOS": "TAXES_DISAGGREGATED",
        "NUMERO_DE_SERIE_DEL_LECTOR_(S/N)": "POI_ID",
        "BILLETERA_VIRTUAL": "POI_WALLET_NAME",
        "BANCO_DE_ORIGEN": "POI_BANK_NAME",
        "NUMERO_INICIAL_DE_TARJETA": "CARD_INITIAL_NUMBER",
        "OPERATION_TAGS": "OPERATION_TAGS",
        "TIPO_DE_IDENTIFICACION_DEL_PAGADOR": "PAYER_ID_TYPE",
        "NUMERO_DE_IDENTIFICACION_DEL_PAGADOR": "PAYER_ID_NUMBER",
        "PAGADOR": "PAYER_NAME",
        "CANAL_DE_VENTA": "BUSINESS_UNIT",
        "PLATAFORMA_DE_COBRO": "SUB_UNIT",
        "FECHA_DE_LIBERACION_DEL_DINERO": "MONEY_RELEASE_DATE",
        "CODIGO_DE_PRODUCTO_SKU": "PRODUCT_SKU",
        "DETALLE_DE_LA_VENTA": "SALE_DETAIL"
    }
    df.rename(columns=column_mapping, inplace=True)
    return df

# --------------------------
# Lambda Handler
# --------------------------
def lambda_handler(event, context):
    try:
        print(f"📥 Event recibido: {json.dumps(event)}")
        
        # Inicializar clientes
        bq_client = get_bigquery_client()
        s3_client = boto3.client('s3')
        
        # Extraer parámetros del event
        etl_flow = event['etl_flow']
        bucket = event['bucket']
        key = event['key']
        
        print(f"🔧 ETL Flow: {etl_flow}")
        print(f"📦 Bucket: {bucket}")
        print(f"📄 Key: {key}")
        
        # Descargar archivo desde S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Determinar tipo de archivo y cargar DataFrame
        if etl_flow == 'MP':
            dtype = {}
            table_name = 'mp_data'
        elif etl_flow == 'TICKET':
            dtype = {
                'ean': str,
                'grupo_producto': str,
                'product_id': 'Int64',
                'nro_ticket': 'Int64',
                'categoria': str,
                'producto': str,
                'fecha': str
            }
            table_name = 'carrefour_data'
        else:
            dtype = {}
            table_name = 'bank_payments'
        
        # Leer archivo
        if key.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(response['Body'].read()), dtype=dtype)
        elif key.endswith(".xlsx"):
            df = pd.read_excel(io.BytesIO(response['Body'].read()))
        else:
            raise Exception("Formato no soportado")
        
        print(f"📊 DataFrame cargado: {len(df)} filas, {len(df.columns)} columnas")
        
        tables_processed = []
        
        # ========================================
        # FLUJO: MERCADO PAGO
        # ========================================
        if etl_flow == 'MP':
            report_id = event['report_id']
            report_date = event['report_date']
            
            df = column_name_mapping(df)
            df.columns = [clean_column_name(c) for c in df.columns]
            df['REPORT_ID'] = report_id
            df['REPORT_DATE'] = report_date
            
            # Cargar a staging
            load_to_staging(bq_client, df, 'mp_data')
            
            # Hacer MERGE a prod (clave: REPORT_ID)
            merge_to_prod(bq_client, 'mp_data', ['REPORT_ID'], list(df.columns))
            
            # Verificar
            verify_table_count(bq_client, BQ_DATASET_PROD, 'mp_data')
            
            tables_processed = ['mp_data']
        
        # ========================================
        # FLUJO: TICKETS CARREFOUR
        # ========================================
        elif etl_flow == 'TICKET':
            # Verificar tickets ya procesados
            tickets_del_archivo = df['nro_ticket'].unique().tolist()
            print(f"🔍 Tickets en el archivo: {tickets_del_archivo}")
            
            # Consultar tickets existentes en BigQuery
            try:
                query = f"""
                SELECT DISTINCT id 
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.archivos_ingestados`
                """
                result = bq_client.query(query).result()
                tickets_existentes = {row['id'] for row in result}
            except Exception as e:
                print(f"⚠️ Tabla archivos_ingestados no existe aún: {str(e)}")
                tickets_existentes = set()
            
            print(f"📦 Tickets ya procesados: {tickets_existentes}")
            
            # Filtrar solo tickets nuevos
            tickets_nuevos = [t for t in tickets_del_archivo if t not in tickets_existentes]
            print(f"🆕 Tickets nuevos a procesar: {tickets_nuevos}")
            
            if not tickets_nuevos:
                print("✅ Todos los tickets ya fueron procesados. No se requiere carga.")
                tables_processed = ['archivos_ingestados', 'dim_producto', 'carrefour_data']
            else:
                # 1) REGISTRAR TICKETS EN archivos_ingestados
                df_uploaded_files = pd.DataFrame(tickets_nuevos, columns=['id'])
                df_uploaded_files['ins_dttm'] = datetime.now()
                
                load_to_staging(bq_client, df_uploaded_files, 'archivos_ingestados')
                merge_to_prod(bq_client, 'archivos_ingestados', ['id'], ['id', 'ins_dttm'])
                verify_table_count(bq_client, BQ_DATASET_PROD, 'archivos_ingestados')
                
                # 2) CARGAR carrefour_data (solo tickets nuevos)
                df_nuevos = df[df['nro_ticket'].isin(tickets_nuevos)].copy()
                
                load_to_staging(bq_client, df_nuevos, 'carrefour_data')
                merge_to_prod(
                    bq_client, 
                    'carrefour_data', 
                    ['nro_ticket'],
                    ['categoria', 'producto', 'cantidad', 'peso', 'precio_unit', 
                     'monto_total', 'ean', 'product_id', 'grupo_producto', 
                     'nro_ticket', 'fecha', 'total_ticket_bruto', 'total_ticket_meli']
                )
                verify_table_count(bq_client, BQ_DATASET_PROD, 'carrefour_data')
                
                # 3) ACTUALIZAR dim_producto
                # Consultar productos únicos desde BigQuery
                query = f"""
                SELECT DISTINCT producto as nombre_producto, ean
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.carrefour_data`
                """
                result = bq_client.query(query).result()
                productos = [(row['nombre_producto'], row['ean']) for row in result]
                
                df_dim = pd.DataFrame(productos, columns=['nombre_producto', 'ean']).drop_duplicates()
                
                # Generar grupo_producto
                normalizacion = generar_diccionario_normalizacion(
                    df_dim['nombre_producto'].fillna('').unique()
                )
                df_dim['grupo_producto'] = df_dim['nombre_producto'].map(normalizacion)
                
                # Obtener max product_id
                try:
                    query = f"""
                    SELECT COALESCE(MAX(product_id), 0) as max_id
                    FROM `{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.dim_producto`
                    """
                    result = bq_client.query(query).result()
                    max_id = list(result)[0]['max_id']
                except:
                    max_id = 0
                
                df_dim = df_dim.reset_index(drop=True)
                df_dim['product_id'] = range(max_id + 1, max_id + 1 + len(df_dim))
                
                load_to_staging(bq_client, df_dim, 'dim_producto')
                merge_to_prod(
                    bq_client,
                    'dim_producto',
                    ['nombre_producto', 'ean'],
                    ['nombre_producto', 'product_id', 'ean', 'grupo_producto']
                )
                verify_table_count(bq_client, BQ_DATASET_PROD, 'dim_producto')
                
                tables_processed = ['archivos_ingestados', 'dim_producto', 'carrefour_data']
        
        # ========================================
        # FLUJO: BANK PAYMENTS
        # ========================================
        else:
            print(f"💳 Procesando pagos bancarios: {table_name}")
            df_bank = df.copy()
            df_bank.columns = [clean_column_name(c) for c in df_bank.columns]
            
            # Si existe columna ID, usarla como clave
            key_cols = ['ID'] if 'ID' in df_bank.columns else []
            
            load_to_staging(bq_client, df_bank, table_name)
            merge_to_prod(bq_client, table_name, key_cols, list(df_bank.columns))
            verify_table_count(bq_client, BQ_DATASET_PROD, table_name)
            
            tables_processed = [table_name]
        
        print(f"✅ Proceso completado exitosamente")
        print(f"📋 Tablas procesadas: {tables_processed}")
        
        return {
            'statusCode': 200,
            'table_name': tables_processed,
            'message': f'Datos cargados exitosamente a BigQuery'
        }
    
    except Exception as e:
        print(f"❌ Error en lambda_handler: {str(e)}")
        import traceback
        traceback.print_exc()
        raise Exception(str(e))
