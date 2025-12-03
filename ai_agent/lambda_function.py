import os
import json
import boto3
from telegram import Bot, Update
import requests
import openai
import time
from decimal import Decimal
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración inicial
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
bot = Bot(token=TELEGRAM_BOT_TOKEN)

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET_PROD = os.environ.get("BQ_DATASET_PROD", "PRD")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
REGION = os.environ.get("AWS_REGION", "us-east-2")
DDB_TABLE = os.environ.get("DDB_TABLE", "schema_cache")
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "604800"))  # 7 días

openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)

# =============================================================================
# METADATA ENRIQUECIDA DEL ESQUEMA - LA CLAVE PARA UN AGENTE INTELIGENTE
# =============================================================================
# Esta metadata le da al LLM todo el contexto que necesita para generar
# queries correctas sin que el usuario tenga que especificar detalles técnicos.

TABLE_METADATA = {
    "bank_payments": {
        "description": "Gastos y transacciones del banco Santander. TODOS los registros son del Banco Santander, NO filtrar por banco/santander.",
        "semantic_hints": [
            "Esta tabla contiene TODOS los gastos bancarios",
            "Si el usuario pregunta por 'gastos del banco' o 'banco santander', usar esta tabla SIN filtros adicionales de banco",
            "El total de gastos es la suma de MONTO"
        ],
        "columns": {
            "FECHA_PAGO": {
                "type": "STRING",
                "format": "dd/mm/yyyy",
                "description": "Fecha del pago en formato texto. IMPORTANTE: Para filtrar por fecha usar PARSE_DATE('%d/%m/%Y', FECHA_PAGO)",
                "example": "15/11/2024"
            },
            "MONTO": {
                "type": "STRING", 
                "format": "número con decimales",
                "description": "Monto del gasto. Castear a FLOAT64 para operaciones: CAST(MONTO AS FLOAT64)",
                "example": "1500.50"
            },
            "COMERCIO": {
                "type": "STRING",
                "description": "Nombre del comercio donde se realizó el gasto",
                "example": "SUPERMERCADO COTO"
            },
            "TARJETA": {
                "type": "STRING",
                "description": "Tipo o número de tarjeta utilizada",
                "example": "VISA DÉBITO"
            },
            "DIVISA": {
                "type": "STRING",
                "description": "Moneda de la transacción",
                "example": "ARS"
            }
        }
    },
    "mp_data": {
        "description": "Transacciones realizadas a través de Mercado Pago (transferencias, pagos QR, etc.)",
        "semantic_hints": [
            "Usar cuando pregunten por 'mercado pago', 'MP', 'transferencias', 'QR'",
            "Incluye tanto pagos enviados como recibidos"
        ],
        "columns": {
            "fecha": {
                "type": "DATE",
                "description": "Fecha de la transacción",
                "example": "2024-11-15"
            },
            "monto": {
                "type": "FLOAT64",
                "description": "Monto de la transacción",
                "example": "2500.00"
            },
            "descripcion": {
                "type": "STRING",
                "description": "Descripción o concepto del pago",
                "example": "Pago a comercio"
            }
        }
    },
    "carrefour_data": {
        "description": "Compras en supermercado Carrefour con detalle de productos",
        "semantic_hints": [
            "Usar cuando pregunten por 'carrefour', 'supermercado', 'compras de comida'",
            "Tiene detalle a nivel de producto individual"
        ],
        "columns": {
            "fecha_compra": {
                "type": "DATE",
                "description": "Fecha de la compra",
                "example": "2024-11-15"
            },
            "producto": {
                "type": "STRING",
                "description": "Nombre del producto comprado",
                "example": "LECHE ENTERA 1L"
            },
            "precio": {
                "type": "FLOAT64",
                "description": "Precio del producto",
                "example": "850.00"
            },
            "cantidad": {
                "type": "INT64",
                "description": "Cantidad comprada",
                "example": "2"
            }
        }
    },
    "dim_producto": {
        "description": "Dimensión de productos para categorización",
        "semantic_hints": [
            "Tabla auxiliar para JOINs con carrefour_data",
            "Contiene categorías y clasificaciones de productos"
        ],
        "columns": {
            "producto_id": {
                "type": "STRING",
                "description": "ID único del producto"
            },
            "categoria": {
                "type": "STRING",
                "description": "Categoría del producto",
                "example": "LÁCTEOS"
            }
        }
    }
}

# Ejemplos de queries correctas para few-shot learning
SQL_EXAMPLES = """
    EJEMPLOS DE QUERIES CORRECTAS:

    1. Pregunta: "¿Cuánto gasté en los últimos 3 meses?"
    SQL:
    SELECT SUM(CAST(MONTO AS FLOAT64)) AS total_gasto
    FROM `{project}.{dataset}.bank_payments`
    WHERE PARSE_DATE('%d/%m/%Y', FECHA_PAGO) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)

    2. Pregunta: "¿Cuáles fueron mis mayores gastos del mes?"
    SQL:
    SELECT COMERCIO, CAST(MONTO AS FLOAT64) AS monto, FECHA_PAGO
    FROM `{project}.{dataset}.bank_payments`
    WHERE PARSE_DATE('%d/%m/%Y', FECHA_PAGO) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
    ORDER BY CAST(MONTO AS FLOAT64) DESC
    LIMIT 10

    3. Pregunta: "Gastos por comercio este mes"
    SQL:
    SELECT COMERCIO, SUM(CAST(MONTO AS FLOAT64)) AS total, COUNT(*) AS cantidad_transacciones
    FROM `{project}.{dataset}.bank_payments`
    WHERE PARSE_DATE('%d/%m/%Y', FECHA_PAGO) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
    GROUP BY COMERCIO
    ORDER BY total DESC
    LIMIT 20
"""

# --- Clientes AWS/GCP ---
dynamo = boto3.resource("dynamodb", region_name=REGION)
ddb_table = dynamo.Table(DDB_TABLE)
glue_client = boto3.client("glue", region_name=REGION)
secrets_client = boto3.client("secretsmanager", region_name=REGION)

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

def get_bigquery_table_columns(client, table_name):
    """Obtiene las columnas de una tabla de BigQuery"""
    try:
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{table_name}"
        table = client.get_table(table_id)
        return [field.name for field in table.schema]
    except Exception as e:
        print(f"⚠️ No se pudo obtener esquema de {table_name}: {e}")
        return []

def get_table_columns_glue(database, table_prefix):
    try:
        paginator = glue_client.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=database):
            for t in page["TableList"]:
                if t["Name"].startswith(table_prefix):
                    return [c["Name"] for c in t["StorageDescriptor"]["Columns"]]
        return []
    except Exception as e:
        print(f"Error Glue {table_prefix}: {e}")
        return []

def get_cached_schema(table_name):
    try:
        resp = ddb_table.get_item(Key={"table_name": table_name})
        item = resp.get("Item")
        if not item:
            return None
        # FIX: DynamoDB devuelve Decimal, convertir a float para comparar
        timestamp = float(item["timestamp"]) if isinstance(item["timestamp"], Decimal) else item["timestamp"]
        if time.time() - timestamp > CACHE_TTL_SECONDS:
            return None
        return json.loads(item["columns_json"])
    except Exception as e:
        print(f"Cache miss por error: {e}")
        return None

def put_schema_cache(table_name, columns):
    try:
        ddb_table.put_item(
            Item={
                "table_name": table_name,
                "columns_json": json.dumps(columns),
                "timestamp": int(time.time()),
            }
        )
    except Exception as e:
        print(f"Error guardando cache: {e}")

def get_columns_for_table(table_name, bq_client, glue_db="default"):
    cols = get_cached_schema(table_name)
    if cols:
        return cols
    cols = get_bigquery_table_columns(bq_client, table_name)
    if not cols:
        cols = get_table_columns_glue(glue_db, table_name)
    if cols:
        put_schema_cache(table_name, cols)
    return cols

def get_table_columns_by_prefix(database: str, table_prefix: str) -> list:
    """Busca una tabla por prefijo en Glue y devuelve sus columnas (para compatibilidad)"""
    try:
        paginator = glue_client.get_paginator('get_tables')
        for page in paginator.paginate(DatabaseName=database):
            for table in page['TableList']:
                table_name = table['Name']
                if table_name.startswith(table_prefix):
                    return [col['Name'] for col in table['StorageDescriptor']['Columns']]
        print(f"⚠️ No se encontró ninguna tabla con prefijo '{table_prefix}' en el catálogo.")
        return []
    except Exception as e:
        print(f"❌ Error al buscar tablas con prefijo '{table_prefix}': {e}")
        return []

def build_enriched_schema_prompt():
    """Construye un prompt detallado con toda la metadata del esquema"""
    schema_parts = []
    
    for table_name, meta in TABLE_METADATA.items():
        table_desc = f"\n### Tabla: `{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{table_name}`"
        table_desc += f"\n**Descripción:** {meta['description']}"
        
        # Agregar hints semánticos
        if meta.get('semantic_hints'):
            table_desc += "\n**Notas importantes:**"
            for hint in meta['semantic_hints']:
                table_desc += f"\n  - {hint}"
        
        # Agregar columnas con detalle
        table_desc += "\n**Columnas:**"
        for col_name, col_info in meta.get('columns', {}).items():
            col_desc = f"\n  - `{col_name}` ({col_info['type']})"
            if col_info.get('format'):
                col_desc += f" - Formato: {col_info['format']}"
            if col_info.get('description'):
                col_desc += f" - {col_info['description']}"
            if col_info.get('example'):
                col_desc += f" - Ej: '{col_info['example']}'"
            table_desc += col_desc
        
        schema_parts.append(table_desc)
    
    return "\n".join(schema_parts)

def generate_sql_with_openai2(question, bq_client):
    """Genera SQL usando OpenAI con metadata enriquecida y few-shot learning"""
    try:
        # Construir schema enriquecido
        enriched_schema = build_enriched_schema_prompt()
        
        # Formatear ejemplos con el proyecto actual
        formatted_examples = SQL_EXAMPLES.format(
            project=GCP_PROJECT_ID, 
            dataset=BQ_DATASET_PROD
        )

        system_prompt = """Eres un experto en SQL para Google BigQuery (Standard SQL).
            Tu trabajo es convertir preguntas en lenguaje natural a consultas SQL precisas.

            REGLAS CRÍTICAS:
            1. NUNCA agregues filtros que el usuario no pidió explícitamente
            2. Si el usuario pregunta por "gastos del banco" o "banco santander", usa bank_payments SIN filtros de banco (toda la tabla ES del banco santander)
            3. Para campos de fecha tipo STRING con formato dd/mm/yyyy, SIEMPRE usar: PARSE_DATE('%d/%m/%Y', campo_fecha)
            4. Para campos MONTO tipo STRING, SIEMPRE usar: CAST(MONTO AS FLOAT64)
            5. Usa fechas relativas (CURRENT_DATE(), DATE_SUB, DATE_TRUNC)
            6. LIMIT 20 siempre
            7. Devuelve SOLO el SQL, sin explicaciones ni markdown
        """

        user_prompt = f"""
            ESQUEMA DETALLADO DE LAS TABLAS:
            {enriched_schema}

            {formatted_examples}

            PREGUNTA DEL USUARIO: "{question}"

            Genera la consulta SQL:
        """

        print(f"🤖 Generando SQL para: {question}")
        
        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=600,
            temperature=0.0,  # Más determinístico
        )
        
        sql = (res.choices[0].message.content or "").strip()
        
        # Limpiar markdown si existe
        if sql.startswith("```"):
            sql = sql.replace("```sql", "").replace("```", "").strip()
        
        print(f"📝 SQL generado:\n{sql}")
        return sql
        
    except Exception as e:
        print(f"❌ Error generando SQL: {e}")
        return ""

def generate_sql_with_openai(question: str, bq_client) -> str:
    """Genera SQL usando OpenAI GPT para BigQuery"""
    
    try:
        # Obtener esquemas actualizados de BigQuery
        bank_columns = get_bigquery_table_columns(bq_client, 'bank_payments')
        mp_columns = get_bigquery_table_columns(bq_client, 'mp_data')
        carrefour_columns = get_bigquery_table_columns(bq_client, 'carrefour_data')
        dim_producto_columns = get_bigquery_table_columns(bq_client, 'dim_producto')

        # Prompt para generar SQL de BigQuery
        prompt = f"""
            Eres un experto en SQL y análisis de datos. Necesito que generes una consulta SQL para responder a esta pregunta: "{question}"

            Esquema actual en BigQuery (dataset: {BQ_DATASET_PROD}):
            - bank_payments: {', '.join(bank_columns) if bank_columns else 'tabla no disponible'}
            - mp_data: {', '.join(mp_columns) if mp_columns else 'tabla no disponible'}
            - carrefour_data: {', '.join(carrefour_columns) if carrefour_columns else 'tabla no disponible'}
            - dim_producto: {', '.join(dim_producto_columns) if dim_producto_columns else 'tabla no disponible'}

            Reglas de oro:
            0. Utilizar valores para filtrar en las queries solo de valores existentes en las tablas.
            1. Usa solo estas columnas y las tablas mencionadas.
            2. Genera SQL válido para BigQuery (Standard SQL).
            3. Las tablas deben referenciarse como: `{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.nombre_tabla`
            4. Si la pregunta es sobre gastos del banco/santander, usa la tabla bank_payments.
            5. Si la pregunta es sobre transacciones/pagos a través de mercado pago, usa la tabla mp_data.
            6. Si la pregunta es sobre gastos del supermercado/carrefour, usa la tabla carrefour_data.
            7. Para información de productos, usa dim_producto y haz JOIN con carrefour_data si es necesario.
            8. Limita los resultados a máximo 20 filas con LIMIT 20.
            9. Para filtros de fecha, usa funciones de BigQuery como DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH).
            10. No uses fechas hardcodeadas, usa funciones relativas (CURRENT_DATE(), DATE_SUB, etc).
            11. Para formatear números usa FORMAT() o CAST().

            Genera solo el SQL, sin explicaciones adicionales:
        """
                
        # Llamar a OpenAI
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",  # Modelo económico y rápido
            messages=[
                {"role": "system", "content": "Eres un experto en SQL para BigQuery (Standard SQL)."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500,
            temperature=0.1
        )
        
        sql = response.choices[0].message.content
        if sql:
            sql = sql.strip()
        else:
            sql = ""
        
        # Limpiar SQL (remover markdown si existe)
        if sql.startswith('```sql'):
            sql = sql.replace('```sql', '').replace('```', '').strip()
        elif sql.startswith('```'):
            sql = sql.replace('```', '').strip()
        
        return sql
        
    except Exception as e:
        print(f"❌ Error generando SQL con OpenAI: {e}")
        return ""

def validate_sql_dry_run(client, sql: str) -> tuple:
    """
    Ejecuta un dry-run de la query para validar sintaxis sin ejecutar.
    Retorna (is_valid: bool, error_message: str or None)
    """
    try:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = client.query(sql, job_config=job_config)
        # Si llegamos aquí, la query es válida
        bytes_processed = query_job.total_bytes_processed
        print(f"✅ Dry-run exitoso. Bytes a procesar: {bytes_processed}")
        return True, None
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Dry-run falló: {error_msg}")
        return False, error_msg

def query_bigquery(client, sql: str) -> str:
    """Ejecuta query en BigQuery y retorna resultados formateados"""
    try:
        print(f"🔍 Ejecutando query en BigQuery:\n{sql}")
        
        # Primero validar con dry-run
        is_valid, validation_error = validate_sql_dry_run(client, sql)
        if not is_valid:
            return f"❌ Error de sintaxis SQL:\n{validation_error}\n\nQuery:\n{sql}"
        
        query_job = client.query(sql)
        results = query_job.result()  # Espera a que termine
        
        # Verificar si hay resultados
        if results.total_rows == 0:
            return "ℹ️ No se encontraron resultados para tu consulta."
        
        return format_bigquery_results(results)
        
    except Exception as e:
        error_msg = f"❌ Error en BigQuery:\n{str(e)}\n\nSQL ejecutado:\n{sql}"
        print(error_msg)  # Debug en CloudWatch
        return error_msg

def format_bigquery_results(results) -> str:
    """Formatea resultados de BigQuery para mostrar en Telegram"""
    formatted_lines = []
    
    # Obtener nombres de columnas
    columns = [field.name for field in results.schema]
    
    # Procesar cada fila
    for row in results:
        formatted_lines.append("---")
        for col_name in columns:
            value = row[col_name]
            
            # Formatear según el tipo de dato
            if value is None:
                formatted_value = "NULL"
            elif isinstance(value, (int, float)):
                if isinstance(value, float):
                    rounded = round(value, 2)
                    formatted_value = f"{rounded:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                else:
                    formatted_value = f"{value:,}".replace(",", ".")
            elif isinstance(value, bool):
                formatted_value = "Sí" if value else "No"
            else:
                formatted_value = str(value)
            
            formatted_lines.append(f"*{col_name}:* {formatted_value}")
    
    return "📊 *Resultados:*\n" + "\n".join(formatted_lines)

def handle_message(text: str, bq_client) -> tuple:
    """Maneja el mensaje del usuario y retorna SQL y respuesta"""
    question = text
    
    # Generar SQL con el nuevo sistema mejorado
    sql = generate_sql_with_openai2(question, bq_client)
    
    if not sql:
        return "", "❌ No se pudo generar la consulta SQL. Por favor, intenta con otra pregunta."
    
    # Validar primero con dry-run
    is_valid, validation_error = validate_sql_dry_run(bq_client, sql)
    
    if not is_valid:
        print(f"⚠️ Primera query inválida, intentando regenerar...")
        # Intentar regenerar con el error como contexto
        retry_sql = retry_sql_generation(question, sql, validation_error, bq_client)
        if retry_sql:
            sql = retry_sql
            is_valid, _ = validate_sql_dry_run(bq_client, sql)
    
    if not is_valid:
        return sql, f"❌ No pude generar una consulta válida. Error: {validation_error}"
    
    response = query_bigquery(bq_client, sql)
    
    return sql, response

def retry_sql_generation(question: str, failed_sql: str, error: str, bq_client) -> str:
    """Intenta regenerar la SQL corrigiendo el error"""
    try:
        enriched_schema = build_enriched_schema_prompt()
        
        prompt = f"""
            La siguiente consulta SQL falló con un error.

            PREGUNTA ORIGINAL: "{question}"

            SQL QUE FALLÓ:
            {failed_sql}

            ERROR:
            {error}

            ESQUEMA DE TABLAS:
            {enriched_schema}

            Por favor, genera una nueva consulta SQL corrigiendo el error. 
            Devuelve SOLO el SQL corregido, sin explicaciones.
        """

        res = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un experto en SQL para BigQuery. Corrige errores de SQL."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=600,
            temperature=0.0,
        )
        
        sql = (res.choices[0].message.content or "").strip()
        if sql.startswith("```"):
            sql = sql.replace("```sql", "").replace("```", "").strip()
        
        print(f"🔄 SQL regenerado:\n{sql}")
        return sql
        
    except Exception as e:
        print(f"❌ Error en retry: {e}")
        return ""

def send_telegram_message(chat_id, text, token):
    """Envía mensaje a Telegram"""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error enviando mensaje a Telegram: {e}")
        return None

def lambda_handler(event, context):
    try:
        print("📥 Evento recibido por Lambda")
        
        # Inicializar cliente de BigQuery
        bq_client = get_bigquery_client()
        
        data = json.loads(event["body"])
        text = data["message"]["text"]
        chat_id = data["message"]["chat"]["id"]

        print(f'💬 Mensaje input: {text}')
        print(f'👤 Chat_id: {chat_id}')

        # Manejar comando /start
        if text == "/start":
            welcome_message = """
                🤖 *Bot de Consultas de Datos con IA*

                ¡Hola! Soy tu asistente inteligente para consultar datos de transacciones y gastos.

                🎯 *Características:*
                • IA real con OpenAI GPT
                • Datos en BigQuery
                • Generación dinámica de SQL
                • Respuestas inteligentes y precisas

                💡 *Puedes preguntarme:*
                • "¿Cuánto gasté este mes?"
                • "Mostrame las transacciones de ayer"
                • "¿Cuál fue el gasto más alto?"
                • "Gastos por categoría"
                • "Transacciones pendientes"
                • "Resumen de gastos de la semana"
                • "¿Cuánto gasté en comida este año?"
                • "Productos más comprados en Carrefour"

                ¡Escribí tu pregunta y la IA generará la consulta SQL automáticamente!
            """
            send_telegram_message(chat_id, welcome_message, TELEGRAM_BOT_TOKEN)
            return {"statusCode": 200}

        # Procesar pregunta
        sql, response_text = handle_message(text, bq_client)
        
        # Enviar respuesta
        result = send_telegram_message(chat_id, response_text, TELEGRAM_BOT_TOKEN)

        if result is None:
           return {
               "statusCode": 200,
               "body": json.dumps({"message": "No se pudo enviar el mensaje a Telegram"})
           }
        else:
           return {
               "statusCode": 200,
               "body": json.dumps({"message": "Mensaje enviado correctamente"})
           }

    except Exception as e:
        print(f"[ERROR] Exception en Lambda: {str(e)}")
        import traceback
        traceback.print_exc()

        # Intentar enviar mensaje de error al usuario
        try:
            data = json.loads(event["body"])
            chat_id = data["message"]["chat"]["id"]
            send_telegram_message(
                chat_id, 
                "❌ Ocurrió un error al procesar tu mensaje. Por favor, intentá de nuevo.", 
                TELEGRAM_BOT_TOKEN
            )
        except Exception as nested_e:
            print(f"[ERROR] No se pudo enviar mensaje de error: {str(nested_e)}")

        return {"statusCode": 200}
