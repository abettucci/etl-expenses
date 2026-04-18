import pandas as pd
import boto3
import io
import os
import json
import re
import requests
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
MP_REPORTS_BUCKET = os.environ.get("MP_REPORTS_BUCKET") 
PARAMETER_NAME = "/mercado_pago/token"
MAPPING_TABLE = "dim_comercio_mapping"
UNMAPPED_TABLE = "comercio_unmapped_queue"

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
    Carga DataFrame a tabla staging en BigQuery con timestamp único
    Returns: staging_table_id, número de filas cargadas
    """
    if df.empty:
        print(f"ℹ️ No hay filas para {table_name}")
        return None, 0
    
    # Usar timestamp para evitar colisiones entre ejecuciones paralelas
    import time
    timestamp_suffix = str(int(time.time() * 1000))
    staging_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.stg_{table_name}_{timestamp_suffix}"
    print(f"📤 Cargando {len(df)} filas a staging temporal: {staging_table_id}")
    
    # Siempre crear tabla nueva con WRITE_TRUNCATE
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
    
    print(f"✅ Staging {table_name}: cargadas {len(df)} filas en {staging_table_id}")
    return staging_table_id, len(df)

def merge_to_prod(client, staging_table_id, table_name, key_columns, all_columns):
    """
    Ejecuta MERGE desde staging a prod en BigQuery
    Solo inserta registros que no existen (basado en key_columns)
    Luego elimina la tabla staging temporal
    """
    prod_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{table_name}"
    
    print(f"🔄 Ejecutando MERGE de staging a prod para {table_name}")
    
    # Construir condición de JOIN para las claves
    if key_columns:
        on_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])
        # Deduplica en staging usando DISTINCT para evitar race conditions
        distinct_keys = ", ".join(key_columns)
        source_query = f"(SELECT DISTINCT * FROM `{staging_table_id}`)"
    else:
        # Si no hay claves, insertar todo (no hay deduplicación)
        on_condition = "FALSE"
        source_query = f"`{staging_table_id}`"
    
    # Construir lista de columnas para INSERT
    columns_list = ", ".join(all_columns)
    values_list = ", ".join([f"source.{c}" for c in all_columns])
    
    merge_query = f"""
    MERGE `{prod_table_id}` AS target
    USING {source_query} AS source
    ON {on_condition}
    WHEN NOT MATCHED THEN
      INSERT ({columns_list})
      VALUES ({values_list})
    """
    
    print(f"📝 Query MERGE:\n{merge_query}")
    
    try:
        # Ejecutar MERGE dentro de una transacción para evitar race conditions
        query_job = client.query(merge_query)
        query_job.result()  # Esperar a que termine
        
        print(f"✅ MERGE completado para {table_name}")
        print(f"   Filas modificadas: {query_job.num_dml_affected_rows}")
        
        # Eliminar tabla staging temporal
        if staging_table_id:
            try:
                client.delete_table(staging_table_id)
                print(f"🧹 Tabla staging temporal eliminada: {staging_table_id}")
            except Exception as cleanup_error:
                print(f"⚠️ No se pudo eliminar staging temporal: {cleanup_error}")
        
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
        
        # Eliminar tabla staging temporal
        if staging_table_id:
            try:
                client.delete_table(staging_table_id)
                print(f"🧹 Tabla staging temporal eliminada: {staging_table_id}")
            except Exception as cleanup_error:
                print(f"⚠️ No se pudo eliminar staging temporal: {cleanup_error}")
        
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

def normalize_text(text):
    if text is None:
        return ""
    text = str(text).strip()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Za-z0-9]+", "", text.upper())
    return text

def parse_mapping_file(file_path, flow_name):
    mappings = []
    if not os.path.exists(file_path):
        return mappings
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            parts = [p.strip() for p in raw.split("=>")]
            if not parts:
                continue
            comercio_raw = parts[0]
            comercio_norm = normalize_text(comercio_raw)
            if not comercio_norm:
                continue
            categoria = parts[1].strip() if len(parts) > 1 else "sin_clasificar"
            subcategoria = parts[2].strip() if len(parts) > 2 else ""
            mappings.append(
                {
                    "flow": flow_name,
                    "match_type": "contains",
                    "match_value": comercio_norm,
                    "comercio_depurado": comercio_raw,
                    "categoria": categoria,
                    "subcategoria": subcategoria,
                    "prioridad": 100,
                    "activo": True,
                    "ins_dttm": datetime.utcnow().isoformat(),
                }
            )
    return mappings

def ensure_comercio_tables(client):
    create_mapping = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{MAPPING_TABLE}` (
      flow STRING,
      match_type STRING,
      match_value STRING,
      comercio_depurado STRING,
      categoria STRING,
      subcategoria STRING,
      prioridad INT64,
      activo BOOL,
      ins_dttm TIMESTAMP
    )
    """
    create_unmapped = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{UNMAPPED_TABLE}` (
      flow STRING,
      comercio_raw STRING,
      comercio_normalizado STRING,
      sample_record STRING,
      ins_dttm TIMESTAMP,
      resolved BOOL
    )
    """
    client.query(create_mapping).result()
    client.query(create_unmapped).result()

def upsert_mappings(client, rows):
    if not rows:
        return
    df_map = pd.DataFrame(rows)
    staging_table_id, _ = load_to_staging(client, df_map, "dim_comercio_mapping")
    query = f"""
    MERGE `{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{MAPPING_TABLE}` t
    USING `{staging_table_id}` s
    ON t.flow = s.flow AND t.match_type = s.match_type AND t.match_value = s.match_value
    WHEN MATCHED THEN UPDATE SET
      comercio_depurado = s.comercio_depurado,
      categoria = s.categoria,
      subcategoria = s.subcategoria,
      prioridad = s.prioridad,
      activo = s.activo,
      ins_dttm = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (flow, match_type, match_value, comercio_depurado, categoria, subcategoria, prioridad, activo, ins_dttm)
      VALUES (s.flow, s.match_type, s.match_value, s.comercio_depurado, s.categoria, s.subcategoria, s.prioridad, s.activo, CURRENT_TIMESTAMP())
    """
    client.query(query).result()

def load_mapping_for_flow(client, flow_name):
    query = f"""
    SELECT flow, match_type, match_value, comercio_depurado, categoria, subcategoria, prioridad, comercio_raw
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{MAPPING_TABLE}`
    WHERE activo = TRUE AND (flow = @flow OR flow = 'all')
    ORDER BY prioridad DESC
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("flow", "STRING", flow_name)]
    )
    rows = client.query(query, job_config=cfg).result()
    return [dict(r) for r in rows]

def resolve_comercio_column(df):
    candidates = ["COMERCIO", "STORE_NAME", "PAYER_NAME", "POS_NAME", "EXTERNAL_REFERENCE"]
    for col in candidates:
        if col in df.columns:
            if col != "COMERCIO":
                df["COMERCIO"] = df[col].astype(str)
            return "COMERCIO"
    df["COMERCIO"] = ""
    return "COMERCIO"

def fuzzy_match_comercio(comercio_normalizado: str, match_value: str, threshold: int = 80) -> bool:
    """
    Compara dos strings usando fuzzy matching con rapidfuzz.
    Usa múltiples algoritmos para mayor flexibilidad:
    - ratio: similitud general
    - partial_ratio: para cuando uno es substring del otro
    - token_sort_ratio: ignora orden de palabras
    
    Retorna True si alguno supera el threshold.
    """
    if not comercio_normalizado or not match_value:
        return False
    
    # Calcular diferentes métricas de similitud
    ratio = fuzz.ratio(comercio_normalizado, match_value)
    partial = fuzz.partial_ratio(comercio_normalizado, match_value)
    token_sort = fuzz.token_sort_ratio(comercio_normalizado, match_value)
    
    # Usar el máximo de las tres métricas
    max_score = max(ratio, partial, token_sort)
    
    return max_score >= threshold


def apply_comercio_mapping(df, mapping_rows, flow_name):
    """
    Aplica mapeos de comercio al DataFrame.
    
    Tipos de matching soportados:
    - exact: coincidencia exacta del texto normalizado
    - contains: el texto normalizado contiene el patrón (default)
    - regex: expresión regular (NO se normaliza el patrón)
    - fuzzy: similitud >= threshold usando rapidfuzz (default 80%)
    - fuzzy:90: similitud >= 90% (threshold personalizado)
    
    IMPORTANTE: match_value y comercio_raw se normalizan automáticamente antes de comparar
    (excepto para regex donde se usa el patrón tal cual).
    """
    if "COMERCIO" not in df.columns:
        df["COMERCIO"] = ""
    df["COMERCIO"] = df["COMERCIO"].fillna("").astype(str)
    df["comercio_normalizado"] = df["COMERCIO"].apply(normalize_text)
    df["comercio_depurado"] = df["COMERCIO"]
    df["categoria"] = "sin_clasificar"
    df["subcategoria"] = ""

    mapping_rows = sorted(mapping_rows, key=lambda x: x.get("prioridad", 0), reverse=True)
    for m in mapping_rows:
        mv_original = (m.get("match_value") or "").strip()
        if not mv_original:
            continue
        mt = (m.get("match_type") or "contains").lower()
        
        # Normalizar match_value (excepto para regex donde se usa el patrón original)
        if mt == "regex":
            mv = mv_original
        else:
            mv = normalize_text(mv_original)
        
        # Preparar comercio_raw normalizado como fallback adicional
        comercio_raw = m.get("comercio_raw") or ""
        comercio_raw_norm = normalize_text(comercio_raw) if comercio_raw else ""
        
        if mt == "exact":
            # Match exacto: comercio_normalizado == match_value normalizado
            mask = df["comercio_normalizado"] == mv
            # Fallback con comercio_raw normalizado
            if not mask.any() and comercio_raw_norm and comercio_raw_norm != mv:
                mask = df["comercio_normalizado"] == comercio_raw_norm
        
        elif mt == "regex":
            # Regex: usar patrón original sin normalizar
            try:
                mask = df["comercio_normalizado"].str.contains(mv_original, regex=True, na=False)
            except Exception:
                mask = pd.Series([False] * len(df))
        
        elif mt.startswith("fuzzy"):
            # Fuzzy matching con threshold configurable
            if ":" in mt:
                try:
                    threshold = int(mt.split(":")[1])
                except ValueError:
                    threshold = 80
            else:
                threshold = 80
            
            # Fuzzy match con match_value normalizado
            mask = df["comercio_normalizado"].apply(
                lambda x: fuzzy_match_comercio(x, mv, threshold)
            )
            # Fallback con comercio_raw normalizado
            if not mask.any() and comercio_raw_norm and comercio_raw_norm != mv:
                mask = df["comercio_normalizado"].apply(
                    lambda x: fuzzy_match_comercio(x, comercio_raw_norm, threshold)
                )
        
        else:
            # Default: contains con match_value normalizado
            if mv:
                mask = df["comercio_normalizado"].str.contains(re.escape(mv), regex=True, na=False)
            else:
                mask = pd.Series([False] * len(df))
            
            # Fallback con comercio_raw normalizado
            if not mask.any() and comercio_raw_norm and comercio_raw_norm != mv:
                mask = df["comercio_normalizado"].str.contains(re.escape(comercio_raw_norm), regex=True, na=False)
        
        df.loc[mask, "comercio_depurado"] = m.get("comercio_depurado") or df.loc[mask, "comercio_depurado"]
        df.loc[mask, "categoria"] = m.get("categoria") or df.loc[mask, "categoria"]
        df.loc[mask, "subcategoria"] = m.get("subcategoria") or df.loc[mask, "subcategoria"]

    unmapped = df[df["categoria"] == "sin_clasificar"][["COMERCIO", "comercio_normalizado"]].drop_duplicates()
    return df, unmapped

def append_unmapped_queue(client, unmapped_df, flow_name):
    if unmapped_df.empty:
        return
    rows = []
    for _, r in unmapped_df.iterrows():
        rows.append(
            {
                "flow": flow_name,
                "comercio_raw": str(r.get("COMERCIO", "")),
                "comercio_normalizado": str(r.get("comercio_normalizado", "")),
                "sample_record": json.dumps({"comercio": str(r.get("COMERCIO", ""))}, ensure_ascii=False),
                "ins_dttm": datetime.utcnow(),
                "resolved": False,
            }
        )
    dfq = pd.DataFrame(rows)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{UNMAPPED_TABLE}"
    cfg = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    client.load_table_from_dataframe(dfq, table_id, job_config=cfg).result()

def should_skip_processing(payload):
    """
    Detecta si el payload corresponde a un email que NO debe procesarse.
    Esto evita loops infinitos cuando llegan emails de error/notificación.
    
    Returns:
        tuple: (should_skip: bool, reason: str)
    """
    if not isinstance(payload, dict):
        return False, ""
    
    # Lista de patrones que indican emails de sistema/error que deben ignorarse
    skip_patterns = {
        'sender': [
            'no-reply@sns.amazonaws.com',
            'notifications@amazonaws.com', 
            'noreply@',
            'mailer-daemon@',
            'postmaster@',
            'bounce@',
            'aws-notifications',
        ],
        'subject': [
            'Fallo en proceso ETL',
            'ETL Error',
            'Lambda Error',
            'Step Function Failed',
            'Delivery Status Notification',
            'Undeliverable:',
            'Mail delivery failed',
            'Exceeded rate limits',
        ],
        'etl_flow': [
            # Si por alguna razón llega un etl_flow vacío o inválido
        ]
    }
    
    # Verificar sender
    sender = payload.get('sender', '').lower()
    for pattern in skip_patterns['sender']:
        if pattern.lower() in sender:
            return True, f"Sender matches skip pattern: {pattern}"
    
    # Verificar subject
    subject = payload.get('subject', '')
    for pattern in skip_patterns['subject']:
        if pattern.lower() in subject.lower():
            return True, f"Subject matches skip pattern: {pattern}"
    
    # Verificar si el key contiene patrones de error
    key = payload.get('key', '')
    if 'error' in key.lower() or 'failed' in key.lower():
        return True, f"Key contains error pattern: {key}"
    
    return False, ""


def lambda_handler(event, context):
    try:
        print(f"📥 Event recibido: {json.dumps(event)}")
        
        # Normalizar input (Step Functions a veces envuelve el payload en {"statusCode", "body"})
        payload = event.get('body', event) if isinstance(event, dict) else event
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                # Si body viene como string no-JSON, lo dejamos tal cual y fallaremos con un error claro abajo
                pass
        
        # Algunos pasos pueden anidar nuevamente "body"
        if isinstance(payload, dict) and 'body' in payload and any(k in payload['body'] for k in ('etl_flow', 'bucket', 'key')):
            inner = payload.get('body')
            if isinstance(inner, str):
                try:
                    inner = json.loads(inner)
                except Exception:
                    inner = None
            if isinstance(inner, dict):
                payload = inner
        
        # FILTRO ANTI-LOOP: Detectar y saltar emails de error/notificación del sistema
        should_skip, skip_reason = should_skip_processing(payload)
        if should_skip:
            print(f"⏭️ SKIP: Email de sistema/error detectado. Razón: {skip_reason}")
            print(f"⏭️ Retornando éxito sin procesar para evitar loop infinito")
            return {
                'statusCode': 200,
                'message': f'Skipped: {skip_reason}',
                'skipped': True
            }
        
        # Inicializar clientes
        bq_client = get_bigquery_client()
        s3_client = boto3.client('s3')
        ensure_comercio_tables(bq_client)
        
        # Cargar mapeos desde archivos .txt SOLO si existen (carga inicial opcional)
        # Una vez que los mapeos están en BigQuery, los .txt se pueden eliminar
        # y el ETL seguirá funcionando usando solo la tabla dim_comercio_mapping
        base_dir = os.path.dirname(__file__)
        bank_mapping_file = os.path.join(base_dir, "mapeo_comercios_bank.txt")
        mp_mapping_file = os.path.join(base_dir, "mapeo_comercios_mp_report.txt")
        
        if os.path.exists(bank_mapping_file):
            bank_mappings = parse_mapping_file(bank_mapping_file, "bank_payments")
            if bank_mappings:
                upsert_mappings(bq_client, bank_mappings)
                print(f"📋 Cargados {len(bank_mappings)} mapeos desde mapeo_comercios_bank.txt")
        else:
            print("ℹ️ mapeo_comercios_bank.txt no existe, usando solo BigQuery")
        
        if os.path.exists(mp_mapping_file):
            mp_mappings = parse_mapping_file(mp_mapping_file, "mp_data")
            if mp_mappings:
                upsert_mappings(bq_client, mp_mappings)
                print(f"📋 Cargados {len(mp_mappings)} mapeos desde mapeo_comercios_mp_report.txt")
        else:
            print("ℹ️ mapeo_comercios_mp_report.txt no existe, usando solo BigQuery")
        
        # Extraer parámetros del event
        if not isinstance(payload, dict):
            raise ValueError(f"Input inválido: se esperaba dict o dict en body. type={type(payload)}")
        
        etl_flow = payload.get('etl_flow')
        bucket = payload.get('bucket')
        key = payload.get('key')
        
        if not isinstance(etl_flow, str) or not etl_flow:
            raise ValueError(f"`etl_flow` inválido. Esperado str no vacío, recibido: {etl_flow!r}")
        if not isinstance(bucket, str) or not bucket:
            raise ValueError(f"`bucket` inválido. Esperado str no vacío, recibido: {bucket!r}")
        if not isinstance(key, str) or not key:
            raise ValueError(
                f"`key` inválido. Esperado str no vacío (ej: 'raw/Ticket_31-12-25.pdf'), recibido: {key!r}. "
                f"Esto suele pasar cuando el Step previo pisa el key o devuelve false."
            )
        
        print(f"🔧 ETL Flow: {etl_flow}")
        print(f"📦 Bucket: {bucket}")
        print(f"📄 Key: {key}")
        
        # Descargar archivo desde S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Determinar tipo de archivo y cargar DataFrame
        if etl_flow == 'MP':
            dtype = {}
            table_name = 'mp_data'
        elif etl_flow == 'MP_TRANSFER':
            dtype = {'nro_cuenta': str}
            table_name = 'mp_transfer_data'
        elif etl_flow == 'BANK_TRANSFER':
            dtype = {'cbu_destino': str, 'nro_comprobante': str}
            table_name = 'bank_transfers'
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
        
        # Columnas de mapeo que NO se guardan en las tablas de hechos
        # (el mapeo se aplica en tiempo de consulta con JOIN a dim_comercio_mapping)
        MAPPING_COLUMNS = ['comercio_normalizado', 'comercio_depurado', 'categoria', 'subcategoria']
        
        # ========================================
        # FLUJO: MERCADO PAGO
        # ========================================
        if etl_flow == 'MP':
            report_id = payload['report_id']
            report_date = payload['report_date']
            
            df = column_name_mapping(df)
            df.columns = [clean_column_name(c) for c in df.columns]
            resolve_comercio_column(df)
            mp_rules = load_mapping_for_flow(bq_client, "mp_data")
            df, unmapped = apply_comercio_mapping(df, mp_rules, "mp_data")
            append_unmapped_queue(bq_client, unmapped, "mp_data")
            
            # Eliminar columnas de mapeo antes de guardar (no existen en tabla destino)
            df = df.drop(columns=[c for c in MAPPING_COLUMNS if c in df.columns], errors='ignore')
            
            df['REPORT_ID'] = report_id
            df['REPORT_DATE'] = report_date
            
            df = df.astype({col: "string" for col in df.columns})

            # Cargar a staging
            staging_table_id, _ = load_to_staging(bq_client, df, 'mp_data')
            
            # Hacer MERGE a prod (clave: REPORT_ID)
            merge_to_prod(bq_client, staging_table_id, 'mp_data', ['REPORT_ID'], list(df.columns))
            
            # Verificar
            verify_table_count(bq_client, BQ_DATASET_PROD, 'mp_data')
            
            tables_processed = ['mp_data']

        elif etl_flow == 'MP_TRANSFER':
            df = column_name_mapping(df)
            df.columns = [clean_column_name(c) for c in df.columns]
            original_columns = set(df.columns)
            resolve_comercio_column(df)
            mp_transfer_rules = load_mapping_for_flow(bq_client, "mp_transfer_data")
            df, unmapped = apply_comercio_mapping(df, mp_transfer_rules, "mp_transfer_data")
            append_unmapped_queue(bq_client, unmapped, "mp_transfer_data")

            # Eliminar columnas de mapeo y COMERCIO si fue creada por resolve_comercio_column
            drop_cols = [c for c in MAPPING_COLUMNS if c in df.columns]
            if 'COMERCIO' not in original_columns and 'COMERCIO' in df.columns:
                drop_cols.append('COMERCIO')
            df = df.drop(columns=drop_cols, errors='ignore')

            df = df.astype({col: "string" for col in df.columns})

            # Cargar a staging
            staging_table_id, _ = load_to_staging(bq_client, df, 'mp_transfer_data')

            # Hacer MERGE a prod (clave: REPORT_ID)
            merge_to_prod(bq_client, staging_table_id, 'mp_transfer_data', ['message_id'], list(df.columns))

            # Verificar
            verify_table_count(bq_client, BQ_DATASET_PROD, 'mp_transfer_data')

            tables_processed = ['mp_transfer_data']

        # ========================================
        # FLUJO: BANK TRANSFERS (Transferencias bancarias Santander)
        # ========================================
        elif etl_flow == 'BANK_TRANSFER':
            print(f"🏦 Procesando transferencia bancaria: {table_name}")
            df['importe'] = pd.to_numeric(df['importe'], errors='coerce')
            df.columns = [clean_column_name(c) for c in df.columns]
            original_columns = set(df.columns)
            resolve_comercio_column(df)
            bank_transfer_rules = load_mapping_for_flow(bq_client, "bank_transfers")
            df, unmapped = apply_comercio_mapping(df, bank_transfer_rules, "bank_transfers")
            append_unmapped_queue(bq_client, unmapped, "bank_transfers")

            # Eliminar columnas de mapeo y COMERCIO si fue creada por resolve_comercio_column
            drop_cols = [c for c in MAPPING_COLUMNS if c in df.columns]
            if 'COMERCIO' not in original_columns and 'COMERCIO' in df.columns:
                drop_cols.append('COMERCIO')
            df = df.drop(columns=drop_cols, errors='ignore')

            df = df.astype({col: "string" for col in df.columns if col != 'IMPORTE'})

            # Cargar a staging
            staging_table_id, _ = load_to_staging(bq_client, df, 'bank_transfers')

            # Hacer MERGE a prod (clave: nro_comprobante es único por transferencia)
            merge_to_prod(bq_client, staging_table_id, 'bank_transfers', ['NRO_COMPROBANTE'], list(df.columns))

            # Verificar
            verify_table_count(bq_client, BQ_DATASET_PROD, 'bank_transfers')

            tables_processed = ['bank_transfers']

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
                
                staging_table_id_1, _ = load_to_staging(bq_client, df_uploaded_files, 'archivos_ingestados')
                merge_to_prod(bq_client, staging_table_id_1, 'archivos_ingestados', ['id'], ['id', 'ins_dttm'])
                verify_table_count(bq_client, BQ_DATASET_PROD, 'archivos_ingestados')
                
                # 2) CARGAR carrefour_data (solo tickets nuevos)
                df_nuevos = df[df['nro_ticket'].isin(tickets_nuevos)].copy()
                
                staging_table_id_2, _ = load_to_staging(bq_client, df_nuevos, 'carrefour_data')
                merge_to_prod(
                    bq_client,
                    staging_table_id_2,
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
                
                staging_table_id_3, _ = load_to_staging(bq_client, df_dim, 'dim_producto')
                merge_to_prod(
                    bq_client,
                    staging_table_id_3,
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
            resolve_comercio_column(df_bank)
            bank_rules = load_mapping_for_flow(bq_client, "bank_payments")
            df_bank, unmapped = apply_comercio_mapping(df_bank, bank_rules, "bank_payments")
            append_unmapped_queue(bq_client, unmapped, "bank_payments")
            
            # Eliminar columnas de mapeo antes de guardar (no existen en tabla destino)
            df_bank = df_bank.drop(columns=[c for c in MAPPING_COLUMNS if c in df_bank.columns], errors='ignore')
            
            # Si existe columna MESSAGE_ID, usarla como clave
            key_cols = ['MESSAGE_ID'] if 'MESSAGE_ID' in df_bank.columns else []
            
            staging_table_id, _ = load_to_staging(bq_client, df_bank, table_name)
            merge_to_prod(bq_client, staging_table_id, table_name, key_cols, list(df_bank.columns))
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

def drop_bigquery_tables():
    print("🧨 Dropeando tablas STG y PRD...")
    bq_client = get_bigquery_client()
    tables = [
        f'{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.stg_mp_data',
        f'{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.mp_data'
    ]

    for t in tables:
        try:
            bq_client.delete_table(t)
            print(f"🔥 Tabla borrada: {t}")
        except Exception as e:
            print(f"⚠️ No se pudo borrar {t}: {e}")

def listar_archivos_s3():
    s3 = boto3.client("s3")
    archivos = []
    response = s3.list_objects_v2(Bucket=MP_REPORTS_BUCKET, Prefix='processed')
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv"):
            archivos.append(key)
    return archivos

def auth_mp():
    # Cliente AWS SSM para Parameter Store
    ssm_client = boto3.client("ssm", region_name="us-east-2")

    # Obtener el parámetro desde AWS Parameter Store
    try:
        response = ssm_client.get_parameter(Name=PARAMETER_NAME, WithDecryption=True)
        access_token = response["Parameter"]["Value"]
        return access_token
    except ssm_client.exceptions.ParameterNotFound:
        raise Exception(f"El parámetro {PARAMETER_NAME} no existe en AWS Parameter Store.")

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

def reproceso():
    archivos = listar_archivos_s3()
    access_token = auth_mp()
    print(f"📦 Archivos encontrados: {len(archivos)}")

    for key in archivos:
        print(f"\n🚀 Procesando archivo: {key}")

        s3_filename = key.split('/')[-1]
        report_id, file_type = get_report_id(s3_filename, access_token)
        s3_report_file_name, report_date_hour, report_date = format_report_file_name(s3_filename)

        event = {
            "etl_flow": 'MP',
            "bucket": 'mercadopago-reports',
            "key": key,
            "report_id": report_id,
            "report_date": report_date
        }

        lambda_handler(event, None)