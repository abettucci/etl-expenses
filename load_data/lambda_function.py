import pandas as pd
import boto3
import io
import os
import json
import csv
import re
import unicodedata
import time
from rapidfuzz import fuzz
from datetime import datetime
from botocore.exceptions import ClientError
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

iam_role = os.environ["IAM_ROLE_REDSHIFT"]

# --------------------------
# Rate Limiter para prevenir throttling
# --------------------------
class RateLimiter:
    """Rate limiter simple para evitar saturar Redshift"""
    def __init__(self, min_interval=0.5):
        self.min_interval = min_interval  # segundos entre operaciones
        self.last_call = 0
    
    def wait_if_needed(self):
        """Espera si es necesario para respetar el rate limit"""
        now = time.time()
        time_since_last = now - self.last_call
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            print(f"⏱️ Rate limiting: esperando {sleep_time:.2f}s...")
            time.sleep(sleep_time)
        self.last_call = time.time()

# Instancia global del rate limiter
rate_limiter = RateLimiter(min_interval=0.5)

# --------------------------
# Utilidades SQL robustas con retry logic
# --------------------------
def exec_sql_wait_with_retry(redshift_data, sql, database, workgroup, max_retries=5):
    """Ejecuta SQL con retry logic para manejar throttling"""
    # Aplicar rate limiting antes de cada operación
    rate_limiter.wait_if_needed()
    
    for attempt in range(max_retries):
        try:
            resp = redshift_data.execute_statement(Database=database, WorkgroupName=workgroup, Sql=sql)
            stmt_id = resp["Id"]
            
            while True:
                try:
                    d = redshift_data.describe_statement(Id=stmt_id)
                    if d["Status"] == "FINISHED":
                        return True
                    if d["Status"] == "FAILED":
                        error_msg = d.get("Error", "SQL failed")
                        # Si es throttling, reintentar
                        if "throttl" in error_msg.lower() or "rate" in error_msg.lower():
                            print(f"⚠️ Throttling detectado en intento {attempt + 1}/{max_retries}: {error_msg}")
                            raise ClientError({"Error": {"Code": "ThrottlingException"}}, "execute_statement")
                        raise RuntimeError(error_msg)
                    time.sleep(3)
                except ClientError as e:
                    if e.response['Error']['Code'] in ['ThrottlingException', 'TooManyRequestsException']:
                        if attempt < max_retries - 1:
                            backoff = (2 ** attempt) + (time.time() % 1)  # Exponential backoff con jitter
                            print(f"🔄 Throttling en describe_statement. Reintentando en {backoff:.2f}s...")
                            time.sleep(backoff)
                            break  # Sale del while interno para reintentar desde execute_statement
                        else:
                            raise
                    raise
                    
        except ClientError as e:
            if e.response['Error']['Code'] in ['ThrottlingException', 'TooManyRequestsException']:
                if attempt < max_retries - 1:
                    backoff = (2 ** attempt) + (time.time() % 1)  # Exponential backoff con jitter
                    print(f"🔄 Throttling detectado. Reintentando en {backoff:.2f}s (intento {attempt + 1}/{max_retries})...")
                    time.sleep(backoff)
                    continue
                else:
                    print(f"❌ Máximo de reintentos alcanzado después de {max_retries} intentos")
                    raise
            raise
    
    raise RuntimeError(f"No se pudo ejecutar SQL después de {max_retries} intentos")

def exec_sql_wait(redshift_data, sql, database, workgroup):
    """Wrapper para mantener compatibilidad"""
    return exec_sql_wait_with_retry(redshift_data, sql, database, workgroup)

def ensure_table_by_sql(redshift_data, create_sql, database, workgroup):
    return exec_sql_wait(redshift_data, create_sql, database, workgroup)

def get_existing_keys(redshift_data, table_name, key_column, database, workgroup):
    keys = set()
    try:
        resp = redshift_data.execute_statement(
            Database=database,
            WorkgroupName=workgroup,
            Sql=f"SELECT DISTINCT {key_column} FROM public.{table_name};"
        )
        while True:
            d = redshift_data.describe_statement(Id=resp['Id'])
            if d['Status'] == 'FINISHED':
                if d.get('HasResultSet'):
                    res = redshift_data.get_statement_result(Id=resp['Id'])
                    for r in res.get('Records', []):
                        v = list(r[0].values())[0] if r and r[0] else None
                        if v is not None:
                            keys.add(int(v) if isinstance(v, (int,)) or 'longValue' in r[0] else v)
                break
            elif d['Status'] == 'FAILED':
                break
            time.sleep(3)
    except Exception:
        pass
    return keys

def verify_table_count(redshift_data, table_name, database, workgroup):
    """Verifica el conteo de registros en una tabla para confirmar persistencia"""
    try:
        resp = redshift_data.execute_statement(
            Database=database,
            WorkgroupName=workgroup,
            Sql=f"SELECT COUNT(*) FROM public.{table_name};"
        )
        while True:
            d = redshift_data.describe_statement(Id=resp['Id'])
            if d['Status'] == 'FINISHED':
                res = redshift_data.get_statement_result(Id=resp['Id'])
                count = res['Records'][0][0].get('longValue', 0) if res['Records'] else 0
                print(f"🔍 Verificación: tabla {table_name} tiene {count} registros")
                return count
            elif d['Status'] == 'FAILED':
                print(f"⚠️ Falló verificación de {table_name}: {d.get('Error', 'Error desconocido')}")
                return 0
            time.sleep(2)
    except Exception as e:
        print(f"⚠️ Error verificando {table_name}: {str(e)}")
        return 0

def s3_put_with_retry(s3_client, bucket_name, key, body, max_retries=3):
    """Sube archivo a S3 con retry logic para manejar throttling"""
    for attempt in range(max_retries):
        try:
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['ThrottlingException', 'RequestLimitExceeded', 'SlowDown']:
                if attempt < max_retries - 1:
                    backoff = (2 ** attempt) + (time.time() % 1)
                    print(f"🔄 S3 throttling. Reintentando en {backoff:.2f}s (intento {attempt + 1}/{max_retries})...")
                    time.sleep(backoff)
                    continue
                else:
                    print(f"❌ No se pudo subir a S3 después de {max_retries} intentos")
                    raise
            raise
    raise RuntimeError(f"No se pudo subir a S3 después de {max_retries} intentos")

def copy_via_staging_and_insert(redshift_data, s3_client, df, table_name, columns_in_order, key_columns, bucket_name, database, workgroup, iam_role):
    if df.empty:
        print(f"ℹ️ No hay filas para {table_name}")
        return 0

    # 1) Crear staging PERMANENTE en public (las TEMP no sobreviven entre statements del API)
    staging = f"stg_{table_name}"
    create_stg_sql = f"CREATE TABLE IF NOT EXISTS public.{staging} (LIKE public.{table_name});"
    exec_sql_wait(redshift_data, create_stg_sql, database, workgroup)
    # Limpiar staging
    exec_sql_wait(redshift_data, f"TRUNCATE TABLE public.{staging};", database, workgroup)

    # 2) Subir CSV con columnas en orden (con retry)
    df_to_save = df[columns_in_order].copy()
    csv_buffer = io.StringIO()
    df_to_save.to_csv(csv_buffer, index=False, sep=',', quoting=csv.QUOTE_MINIMAL)
    s3_key = f"tmp/{table_name}_{int(time.time())}.csv"
    s3_put_with_retry(s3_client, bucket_name, s3_key, csv_buffer.getvalue().encode('utf-8'))
    s3_path = f"s3://{bucket_name}/{s3_key}"
    # ⏰ CRÍTICO: Esperar propagación de S3
    print(f"⏰ Esperando propagación de archivo S3: {s3_key}")
    time.sleep(5)

    cols_clause = "(" + ", ".join(columns_in_order) + ")"
    copy_sql = f"""
        COPY public.{staging} {cols_clause}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        CSV
        IGNOREHEADER 1
        DELIMITER ','
        EMPTYASNULL
        BLANKSASNULL
        TRUNCATECOLUMNS
        MAXERROR 0;
    """
    exec_sql_wait(redshift_data, copy_sql, database, workgroup)

    # 3) Insertar no duplicados si hay claves; si no hay claves, insertar todo
    insert_cols = ", ".join(columns_in_order)
    select_cols = ", ".join([f"s.{c}" for c in columns_in_order])
    if key_columns:
        on_join = " AND ".join([f"s.{k} = d.{k}" for k in key_columns])
        null_checks = " AND ".join([f"d.{k} IS NULL" for k in key_columns])
        insert_sql = f"""
            INSERT INTO public.{table_name} ({insert_cols})
            SELECT {select_cols}
            FROM public.{staging} s
            LEFT JOIN public.{table_name} d
              ON {on_join}
            WHERE {null_checks};
        """
    else:
        insert_sql = f"""
            INSERT INTO public.{table_name} ({insert_cols})
            SELECT {select_cols}
            FROM public.{staging} s;
        """
    exec_sql_wait(redshift_data, insert_sql, database, workgroup)
    
    # ⏰ CRÍTICO: Esperar a que el INSERT se propague completamente
    print(f"⏰ Esperando propagación del INSERT en {table_name}...")
    time.sleep(5)

    # 4) Contar insertados
    cnt_resp = redshift_data.execute_statement(Database=database, WorkgroupName=workgroup, Sql=f"SELECT COUNT(*) FROM public.{staging};")
    while True:
        d = redshift_data.describe_statement(Id=cnt_resp['Id'])
        if d['Status'] == 'FINISHED':
            res = redshift_data.get_statement_result(Id=cnt_resp['Id'])
            staged = res['Records'][0][0]['longValue']
            break
        time.sleep(3)

    # 5) Limpiar staging para futuras cargas
    exec_sql_wait(redshift_data, f"TRUNCATE TABLE public.{staging};", database, workgroup)
    
    # ⏰ CRÍTICO: Esperar después del TRUNCATE para evitar race conditions
    time.sleep(2)
    return staged
    
def format_value(val):
    if val is None or pd.isna(val):
        return 'NULL'
    if isinstance(val, str):
        # Escapar comillas simples en SQL ( ' -> '' )
        return "'" + val.replace("'", "''") + "'"
    if isinstance(val, pd.Timestamp):
        return f"'{val.isoformat(sep=' ')}'"
    return str(val)  # para números

def redshift_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "VARCHAR(500)"  # catch-all for strings, objects

def clean_column_name(col):
    # Eliminar tildes y convertir a ASCII
    col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode()
    # Reemplazar caracteres no alfanuméricos por guion bajo
    col = re.sub(r'[^A-Za-z0-9_]+', '_', col)
    return col.upper().strip('_')

def generar_diccionario_normalizacion(productos, threshold=85):
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
    return re.sub(r'[^a-z0-9]+', '', text.lower()).strip('')
      
def column_name_mapping(df):
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
        "OPERATION_TAGS": "OPERATION_TAGS",  # ya coincide
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
        
def lambda_handler(event,context):
    try:
        # Configurar clientes con retry automático
        from botocore.config import Config
        
        retry_config = Config(
            retries={
                'max_attempts': 5,
                'mode': 'adaptive'  # Se adapta dinámicamente al throttling
            },
            connect_timeout=60,
            read_timeout=60
        )
        
        redshift_data = boto3.client('redshift-data', config=retry_config)
        print(event)

        etl_flow = event['etl_flow']
        bucket = event['bucket']
        key = event['key'] # ya tiene la carpeta en el path
        folder = 'processed/'

        print('etl_flow: ', etl_flow)
        print('bucket: ', bucket)
        print('key: ', key)
        
        # print(f"📥 Descargando archivo desde S3: s3://{bucket}/{key}")
        s3 = boto3.client('s3', config=retry_config)
        response = s3.get_object(Bucket=bucket, Key=key)

        if etl_flow == 'MP':
            table_name = 'mp_data'
            dtype = {}
        elif etl_flow == 'TICKET':
            dtype = {
                'ean': str,  # Leer como string desde el principio
                'grupo_producto': str,
                'product_id': 'Int64',  # Usar Int64 para enteros con NaN
                'nro_ticket': 'Int64',
                'categoria': str,
                'producto': str,
                'fecha': str  # Leer como string y luego convertir a datetime
            }
            table_name = 'carrefour_data'
        else:
            dtype = {}
            table_name = 'bank_payments'

        if key.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(response['Body'].read()),dtype=dtype)
        elif key.endswith(".xlsx"):
            df = pd.read_excel(io.BytesIO(response['Body'].read()))
        else:
            raise Exception("Formato no soportado")

        if etl_flow == 'MP':
            report_id = event['report_id']
            report_date = event['report_date']

            df = column_name_mapping(df)
            # Renombrar columnas a identificadores limpios y agregar claves
            df_mp = df.copy()
            df_mp.columns = [clean_column_name(c) for c in df_mp.columns]
            df_mp['REPORT_ID'] = report_id
            df_mp['REPORT_DATE'] = report_date

            # Crear tabla si no existe (sin constraints)
            cols_create = []
            for col, dtype in zip(df_mp.columns, df_mp.dtypes):
                if col in ('REPORT_DATE',):
                    cols_create.append(f"{col} VARCHAR(500)")
                else:
                    cols_create.append(f"{col} {redshift_type(dtype)}")
            create_mp_sql = "CREATE TABLE IF NOT EXISTS public.mp_data (\n  " + ",\n  ".join(cols_create) + "\n);"
            ensure_table_by_sql(redshift_data, create_mp_sql, 'dev', 'pdf-etl-workgroup')

            inserted_mp = copy_via_staging_and_insert(
                redshift_data, s3, df_mp, 'mp_data', list(df_mp.columns), ['REPORT_ID'],
                bucket, 'dev', 'pdf-etl-workgroup', iam_role
            )
            print(f"✅ mp_data: insertadas {inserted_mp} filas")

            tables = [table_name]

        elif etl_flow == 'TICKET':
            report_id, report_date = '', ''
            
            # 🔍 VERIFICACIÓN SIMPLE: Solo verificar si el ticket ya existe
            tickets_del_archivo = df['nro_ticket'].unique().tolist()
            print(f"🔍 Tickets en el archivo: {tickets_del_archivo}")
            
            # Consultar tickets ya procesados (manejo de error si la tabla no existe)
            tickets_existentes = set()
            try:
                check_tickets_query = "SELECT DISTINCT id FROM archivos_ingestados;"
                check_resp = redshift_data.execute_statement(
                    Database='dev',
                    WorkgroupName='pdf-etl-workgroup',
                    Sql=check_tickets_query
                )
                
                while True:
                    desc = redshift_data.describe_statement(Id=check_resp['Id'])
                    if desc["Status"] == "FINISHED":
                        if desc.get('HasResultSet'):
                            result = redshift_data.get_statement_result(Id=check_resp['Id'])
                            tickets_existentes = {
                                int(record[0]['longValue']) 
                                for record in result.get("Records", [])
                                if record and record[0].get('longValue')
                            }
                        break
                    elif desc["Status"] == "FAILED":
                        print(f"⚠️ Tabla archivos_ingestados no existe aún, se creará")
                        break
                    time.sleep(3)
            except Exception as e:
                print(f"⚠️ No se pudo consultar tickets existentes: {str(e)}")
            
            print(f"📦 Tickets ya procesados: {tickets_existentes}")
            
            # Filtrar solo tickets nuevos
            tickets_nuevos = [t for t in tickets_del_archivo if t not in tickets_existentes]
            print(f"🆕 Tickets nuevos a procesar: {tickets_nuevos}")
            
            if not tickets_nuevos:
                print("✅ Todos los tickets ya fueron procesados. No se requiere carga.")
                tables = ['archivos_ingestados', 'dim_producto', 'carrefour_data']
            else:
                # 1) Registrar primero en archivos_ingestados
                df_uploaded_files = pd.DataFrame(tickets_nuevos, columns=['id'])
                df_uploaded_files['ins_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                create_arch_sql = """
                    CREATE TABLE IF NOT EXISTS public.archivos_ingestados (
                        id BIGINT,
                        ins_dttm TIMESTAMP
                    );
                """
                ensure_table_by_sql(redshift_data, create_arch_sql, 'dev', 'pdf-etl-workgroup')
                inserted_arch = copy_via_staging_and_insert(
                    redshift_data, s3, df_uploaded_files,
                    'archivos_ingestados', ['id','ins_dttm'], ['id'], bucket,
                    'dev', 'pdf-etl-workgroup', iam_role
                )
                print(f"✅ archivos_ingestados: procesados {inserted_arch} tickets")
                
                # 🔍 Verificar persistencia
                verify_table_count(redshift_data, 'archivos_ingestados', 'dev', 'pdf-etl-workgroup')
                
                # ⏰ CRÍTICO: Esperar entre cargas de tablas diferentes
                print(f"⏰ Esperando entre tabla archivos_ingestados y carrefour_data...")
                time.sleep(5)

                # 2) Crear carrefour_data si no existe y cargar SOLO tickets nuevos
                create_car_sql = """
                    CREATE TABLE IF NOT EXISTS public.carrefour_data (
                        categoria VARCHAR(500),
                        producto VARCHAR(500),
                        cantidad DOUBLE PRECISION,
                        peso DOUBLE PRECISION,
                        precio_unit DOUBLE PRECISION,
                        monto_total DOUBLE PRECISION,
                        ean VARCHAR(500),
                        product_id BIGINT,
                        grupo_producto VARCHAR(500),
                        nro_ticket BIGINT,
                        fecha VARCHAR(500),
                        total_ticket_bruto DOUBLE PRECISION,
                        total_ticket_meli DOUBLE PRECISION
                    );
                """
                ensure_table_by_sql(redshift_data, create_car_sql, 'dev', 'pdf-etl-workgroup')
                df_nuevos = df[df['nro_ticket'].isin(tickets_nuevos)].copy()
                inserted_car = copy_via_staging_and_insert(
                    redshift_data, s3, df_nuevos,
                    'carrefour_data',
                    ['categoria','producto','cantidad','peso','precio_unit','monto_total','ean','product_id','grupo_producto','nro_ticket','fecha','total_ticket_bruto','total_ticket_meli'],
                    ['nro_ticket'], bucket,
                    'dev', 'pdf-etl-workgroup', iam_role
                )
                print(f"✅ carrefour_data: insertadas {inserted_car} filas")
                
                # 🔍 Verificar persistencia
                verify_table_count(redshift_data, 'carrefour_data', 'dev', 'pdf-etl-workgroup')
                
                # ⏰ CRÍTICO: Esperar entre carrefour_data y dim_producto
                print(f"⏰ Esperando entre tabla carrefour_data y dim_producto...")
                time.sleep(5)

                # 3) Crear/actualizar dim_producto desde carrefour_data actual
                create_dim_sql = """
                    CREATE TABLE IF NOT EXISTS public.dim_producto (
                        nombre_producto VARCHAR(500),
                        product_id BIGINT,
                        ean VARCHAR(500),
                        grupo_producto VARCHAR(500)
                    );
                """
                ensure_table_by_sql(redshift_data, create_dim_sql, 'dev', 'pdf-etl-workgroup')
                
                # ⏰ CRÍTICO: Esperar antes de consultar carrefour_data para dim_producto
                print(f"⏰ Esperando antes de consultar carrefour_data para dim_producto...")
                time.sleep(5)

                # Productos únicos actuales (nombre, ean)
                resp = redshift_data.execute_statement(
                    Database='dev', WorkgroupName='pdf-etl-workgroup',
                    Sql="SELECT DISTINCT producto, ean FROM public.carrefour_data;"
                )
                prod = []
                while True:
                    d = redshift_data.describe_statement(Id=resp['Id'])
                    if d['Status'] == 'FINISHED':
                        if d.get('HasResultSet'):
                            res = redshift_data.get_statement_result(Id=resp['Id'])
                            for r in res.get('Records', []):
                                nombre = r[0].get('stringValue')
                                ean = r[1].get('stringValue') if r[1].get('stringValue') else None
                                prod.append((nombre, ean))
                        break;
                    time.sleep(3)
                df_dim = pd.DataFrame(prod, columns=['nombre_producto','ean']).drop_duplicates()
                # Generar grupo y product_id incremental
                normalizacion = generar_diccionario_normalizacion(df_dim['nombre_producto'].fillna('').unique())
                df_dim['grupo_producto'] = df_dim['nombre_producto'].map(normalizacion)
                # Max product_id existente
                max_resp = redshift_data.execute_statement(Database='dev', WorkgroupName='pdf-etl-workgroup', Sql="SELECT COALESCE(MAX(product_id),0) FROM public.dim_producto;")
                while True:
                    dd = redshift_data.describe_statement(Id=max_resp['Id'])
                    if dd['Status'] == 'FINISHED':
                        res = redshift_data.get_statement_result(Id=max_resp['Id'])
                        max_id = res['Records'][0][0].get('longValue', 0) if res['Records'][0][0] else 0
                        break
                    time.sleep(1)
                df_dim = df_dim.reset_index(drop=True)
                df_dim['product_id'] = range(max_id + 1, max_id + 1 + len(df_dim))

                inserted_dim = copy_via_staging_and_insert(
                    redshift_data, s3, df_dim[['nombre_producto','product_id','ean','grupo_producto']],
                    'dim_producto', ['nombre_producto','product_id','ean','grupo_producto'], ['nombre_producto','ean'],
                    bucket, 'dev', 'pdf-etl-workgroup', iam_role
                )
                print(f"✅ dim_producto: insertados/asegurados {inserted_dim} productos")
                
                # 🔍 Verificar persistencia
                verify_table_count(redshift_data, 'dim_producto', 'dev', 'pdf-etl-workgroup')
                
                # ⏰ CRÍTICO: Espera final para asegurar persistencia completa
                print(f"⏰ Esperando persistencia final de todas las tablas...")
                time.sleep(8)

                tables = ['archivos_ingestados', 'dim_producto', 'carrefour_data']  
        else: # es un gasto del banco
            print(f'Se lee el mail {key} convertido en csv en S3 y se mergea a la tabla de {table_name}')
            df_bank = df.copy()
            df_bank.columns = [clean_column_name(c) for c in df_bank.columns]

            cols_create = [f"{col} {redshift_type(dtype)}" for col, dtype in zip(df_bank.columns, df_bank.dtypes)]
            create_bank_sql = "CREATE TABLE IF NOT EXISTS public." + table_name + " (\n  " + ",\n  ".join(cols_create) + "\n);"
            ensure_table_by_sql(redshift_data, create_bank_sql, 'dev', 'pdf-etl-workgroup')

            # Si existe columna ID, usarla como clave para evitar duplicados; si no, inserta todo
            key_cols = ['ID'] if 'ID' in df_bank.columns else []
            inserted_bank = copy_via_staging_and_insert(
                redshift_data, s3, df_bank, table_name, list(df_bank.columns), key_cols if key_cols else [],
                bucket, 'dev', 'pdf-etl-workgroup', iam_role
            )
            print(f"✅ {table_name}: insertadas {inserted_bank} filas")

            tables = [table_name]

        return {
            'table_name': tables
        }

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        
        if error_code in ['ThrottlingException', 'TooManyRequestsException']:
            print(f"❌ ERROR DE THROTTLING: {error_code}")
            print(f"   Mensaje: {error_msg}")
            print(f"   💡 SUGERENCIAS:")
            print(f"      1. Aumentar timeout de Lambda (actualmente puede ser insuficiente)")
            print(f"      2. Verificar límites de Redshift en CloudWatch")
            print(f"      3. Considerar espaciar más las operaciones")
        else:
            print(f"❌ ERROR: {error_code} - {error_msg}")
        
        raise Exception(f"{error_code}: {error_msg}")
        
    except Exception as e:
        print("⚠️ Error no esperado:", str(e))
        print(f"   Tipo de error: {type(e).__name__}")
        import traceback
        print("   Stack trace:")
        traceback.print_exc()
        raise Exception(str(e))

# s3_client = boto3.client('s3')
# bucket_name = 'market-tickets'
# folder = 'processed/'
# response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
# csvs = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

# dtype = {}
# for csv_key in csvs:
#     response = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
#     if csv_key.endswith(".csv"):
#         df = pd.read_csv(io.BytesIO(response['Body'].read()),dtype=dtype)

#     event = {
#         "body": json.dumps({
#             "etl_flow": 'TICKET',
#             "bucket": 'market-tickets',
#             "key": csv_key
#         })
#     }

#     lambda_handler(event,'')
#     exit()