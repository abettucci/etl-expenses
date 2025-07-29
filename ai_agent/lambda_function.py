import os
import json
import boto3
from telegram import Bot, Update
import requests
import openai

# Configuración inicial
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
bot = Bot(token=TELEGRAM_BOT_TOKEN)

redshift_data = boto3.client('redshift-data')
glue_client = boto3.client('glue')

# Configuración de OpenAI
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is required")

openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)

def get_table_columns(database: str, table: str) -> list:
    """Obtiene columnas de una tabla desde Glue Data Catalog"""
    try:
        response = glue_client.get_table(
            DatabaseName=database,
            Name=table
        )
        return [col['Name'] for col in response['Table']['StorageDescriptor']['Columns']]
    except Exception as e:
        print(f"❌ Error obteniendo esquema de {table}: {e}")
        return []

def generate_sql_with_openai(question: str) -> str:
    """Genera SQL usando OpenAI GPT"""
    
    try:
        # Obtener esquemas actualizados
        bank_columns = get_table_columns('etl_database', 'bank_payments')
        mp_columns = get_table_columns('etl_database', 'mp_data')
        # market_columns = get_table_columns('etl_database', 'carrefour_data')
        # 4. Si la pregunta es sobre gastos del supermercado/carrefour, usa carrefour_data.
        # - carrefour_data: {', '.join(market_columns)}

        print(f"bank_columns: {bank_columns}")
        print(f"mp_columns: {mp_columns}")

        # Prompt para generar SQL
        prompt = f"""
        Eres un experto en SQL y análisis de datos. Necesito que generes una consulta SQL para responder a esta pregunta: "{question}"
    
        Esquema actual:
        - bank_payments: {', '.join(bank_columns)}
        - mp_data: {', '.join(mp_columns)}

        Reglas de oro:
        1. Usa solo estas columnas y las tablas mencionadas.
        2. Genera SQL válido para Redshift.
        3. Si la pregunta es sobre gastos del banco/santander, usa bank_payments.
        4. Si la pregunta es sobre transacciones/pagos a traves de mercado pago, usa mp_data.
        5. Limita los resultados a máximo 20 filas.
        6. Incluye fechas relevantes cuando sea apropiado.

        Genera solo el SQL, sin explicaciones adicionales:
        """
        
        print(f"🤖 Enviando prompt a OpenAI GPT...")
        
        # Llamar a OpenAI
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",  # Modelo económico y rápido
            messages=[
                {"role": "system", "content": "Eres un experto en SQL para Redshift."},
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
        
        print(f"✅ SQL generado por OpenAI: {sql}")
        return sql
        
    except Exception as e:
        print(f"❌ Error generando SQL con OpenAI: {e}")
        return ""

def query_redshift(sql: str) -> str:
    try:
        print(f"🔍 Ejecutando SQL en Redshift:\n{sql}")  # Debug
        response = redshift_data.execute_statement(
            Database='dev',
            WorkgroupName='pdf-etl-workgroup',
            Sql=sql
        )
        query_id = response['Id']
        
        while True:
            status = redshift_data.describe_statement(Id=query_id)
            if status['Status'] == 'FINISHED':
                if status['HasResultSet']:
                    results = redshift_data.get_statement_result(Id=query_id)
                    return format_redshift_results(results)
                return "ℹ️ No se encontraron resultados."
            elif status['Status'] == 'FAILED':
                error_msg = f"❌ Error en Redshift:\n```\n{status['Error']}\n```\nSQL:\n```sql\n{sql}\n```"
                print(error_msg)  # Debug en CloudWatch
                return error_msg
    except Exception as e:
        error_msg = f"⚠️ Error inesperado:\n```\n{str(e)}\n```"
        print(error_msg)  # Debug
        return error_msg

def format_redshift_results(results: dict) -> str:
    columns = [col['name'] for col in results['ColumnMetadata']]
    formatted_rows = []
    
    for record in results['Records']:
        row = []
        for field in record:
            # Manejar todos los tipos de datos de Redshift Data API
            if 'stringValue' in field:
                row.append(str(field['stringValue']))
            elif 'longValue' in field:
                row.append(str(field['longValue']))
            elif 'doubleValue' in field:
                row.append(str(field['doubleValue']))
            elif 'booleanValue' in field:
                row.append("Sí" if field['booleanValue'] else "No")
            elif 'isNull' in field and field['isNull']:
                row.append("NULL")
            else:
                row.append("?")
        formatted_rows.append(" | ".join(row))
    
    return (
        "📊 *Resultados:*\n" +
        "| " + " | ".join(columns) + " |\n" +
        "|" + "|".join(["---"] * len(columns)) + "|\n" +
        "\n".join(["| " + row + " |" for row in formatted_rows])
    )

# Manejo de Telegram - versión con OpenAI
def handle_message(text: str) -> str:
    question = text
    sql = generate_sql_with_openai(question)
    
    if not sql:
        return "❌ No se pudo generar la consulta SQL. Por favor, intenta con otra pregunta."
    
    response = query_redshift(sql)

    return f"""
        🔍 *Consulta:* {question}

        ```sql
        {sql}
        ```

        {response}
    """

def send_telegram_message(chat_id, text, token):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
    }
    try:
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error enviando mensaje a Telegram: {e}")
        return None

def lambda_handler(event, context):
    try:
        print("== Evento recibido por Lambda ==")
        print(json.dumps(event))

        data = json.loads(event["body"])
        text = data["message"]["text"]
        chat_id = data["message"]["chat"]["id"]

        print('text: ', text)
        print('chat_id: ', chat_id)

        # Manejar comando /start
        if text == "/start":
            welcome_message = """
                🤖 *Bot de Consultas de Datos con OpenAI*

                ¡Hola! Soy tu asistente inteligente para consultar datos de transacciones y gastos.

                🎯 *Características:*
                • IA real con OpenAI GPT
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

                ¡Escribí tu pregunta y la IA generará la consulta SQL automáticamente!
            """
            send_telegram_message(chat_id, welcome_message, TELEGRAM_BOT_TOKEN)
            return {"statusCode": 200}

        response_text = handle_message(text)
        result = send_telegram_message(chat_id, response_text, TELEGRAM_BOT_TOKEN)

        if result is None:
           return {
               "statusCode": 200,
               "body": json.dumps({"message": "No se pudo enviar el mensaje a Telegram (chat_id inválido o error de red)"})
           }
        else:
           return {
               "statusCode": 200,
               "body": json.dumps({"message": "Mensaje enviado correctamente"})
           }

    except Exception as e:
        print("[ERROR] Exception en Lambda:", str(e))

        # Intentar enviar mensaje de error al usuario
        try:
            data = json.loads(event["body"])
            chat_id = data["message"]["chat"]["id"]
            send_telegram_message(chat_id, "❌ Ocurrió un error al procesar tu mensaje. Por favor, intentá de nuevo.", TELEGRAM_BOT_TOKEN)
        except Exception as nested_e:
            print("[ERROR] No se pudo enviar mensaje de error:", str(nested_e))

        return {"statusCode": 200}  # Cambiar a 200 para evitar reintentos 