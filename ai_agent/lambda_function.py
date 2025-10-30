import os
import json
import boto3
from telegram import Bot, Update
import requests
import openai
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración inicial
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Configuración de BigQuery
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET_PROD = os.environ.get("BQ_DATASET_PROD", "PRD")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

# Cliente de Glue para obtener esquemas (mantener compatibilidad con S3)
glue_client = boto3.client('glue', region_name='us-east-2')

# Configuración de OpenAI
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is required")

openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)

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

def query_bigquery(client, sql: str) -> str:
    """Ejecuta query en BigQuery y retorna resultados formateados"""
    try:
        print(f"🔍 Ejecutando query en BigQuery:\n{sql}")
        
        query_job = client.query(sql)
        results = query_job.result()  # Espera a que termine
        
        # Verificar si hay resultados
        if query_job.total_rows == 0:
            return "ℹ️ No se encontraron resultados."
        
        return format_bigquery_results(results)
        
    except Exception as e:
        error_msg = f"❌ Error en BigQuery:\n```\n{str(e)}\n```\n\nSQL ejecutado:\n```sql\n{sql}\n```"
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
    sql = generate_sql_with_openai(question, bq_client)
    
    if not sql:
        return "", "❌ No se pudo generar la consulta SQL. Por favor, intenta con otra pregunta."
    
    response = query_bigquery(bq_client, sql)
    
    return sql, response

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
