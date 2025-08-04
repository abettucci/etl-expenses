import os
import json
import boto3
from telegram import Bot, Update
import requests
import openai

# Configuración inicial
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
bot = Bot(token=TELEGRAM_BOT_TOKEN)

redshift_data = boto3.client('redshift-data', region_name='us-east-2')
glue_client = boto3.client('glue', region_name = 'us-east-2')

# Configuración de OpenAI
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is required")

openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)

def get_table_columns_by_prefix(database: str, table_prefix: str) -> list:
    """Busca una tabla por prefijo y devuelve sus columnas"""
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

def generate_sql_with_openai(question: str) -> str:
    """Genera SQL usando OpenAI GPT"""
    
    try:
        # Obtener esquemas actualizados
        bank_columns = get_table_columns_by_prefix('etl_database', 'bank_payments_')
        mp_columns = get_table_columns_by_prefix('etl_database', 'mp_reports_')
        market_tickets_columns = get_table_columns_by_prefix('etl_database', 'market_tickets_')

        # Prompt para generar SQL
        prompt = f"""
        Eres un experto en SQL y análisis de datos. Necesito que generes una consulta SQL para responder a esta pregunta: "{question}"
    
        Esquema actual:
        - bank_payments: {', '.join(bank_columns)}
        - mp_data: {', '.join(mp_columns)}
        - carrefour_data: {', '.join(market_tickets_columns)}

        Reglas de oro:
        0. Utilizar valores para filtrar en las queries solo de valores existentes en las tablas.
        1. Usa solo estas columnas y las tablas mencionadas.
        2. Genera SQL válido para Redshift.
        3. Si la pregunta es sobre gastos del banco/santander, usa la tabla bank_payments y NO tenes que filtrar nada como comercio = 'banco santander' ni nada por el estilo.
        4. Si la pregunta es sobre transacciones/pagos a traves de mercado pago, usa la tabla mp_data y NO tenes que filtrar nada como comercio = 'mercado pago' ni nada por el estilo.
        5. Si la pregunta es sobre gastos del supermercado/carrefour, usa la tabla carrefour_data y NO tenes que filtrar nada como comercio = 'carrefour' ni nada por el estilo.
        5. Limita los resultados a máximo 20 filas.
        6. Si la consulta pide filtrar por fecha algun resultado, utiliza la columna que tenga tipo de dato fecha considerando la fecha pedida. Por ejemplo si se piden datos del ultimo mes, hacer el calculo del filtro de ultimo mes utilizando la columna de fecha que haya en la tabla correspondiente. No utilizar filtros de fechas en caso de no pedirse ningun filtro de fechas, tampoco utilizar fechas que no existen en los datos. 

        Genera solo el SQL, sin explicaciones adicionales:
        """
                
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
        
        return sql
        
    except Exception as e:
        print(f"❌ Error generando SQL con OpenAI: {e}")
        return ""

def query_redshift(sql: str) -> str:
    try:
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
        print(error_msg)  # Debug en CloudWatch
        return error_msg

def format_redshift_results(results: dict) -> str:
    columns = [col['name'] for col in results['ColumnMetadata']]
    formatted_lines = []

    for record in results['Records']:
        for col_name, field in zip(columns, record):
            if 'stringValue' in field:
                value = str(field['stringValue'])
            elif 'longValue' in field:
                value = f"{field['longValue']:,}".replace(",", ".")
            elif 'doubleValue' in field:
                rounded = round(field['doubleValue'])
                value = f"{rounded:,}".replace(",", ".")
            elif 'booleanValue' in field:
                value = "Sí" if field['booleanValue'] else "No"
            elif 'isNull' in field and field['isNull']:
                value = "NULL"
            else:
                value = "?"

            formatted_lines.append(f"*{col_name}:* {value}")

    return "📊 *Resultados:*\n" + "\n".join(formatted_lines)

# Manejo de Telegram - versión con OpenAI
def handle_message(text: str) -> str:
    question = text
    sql = generate_sql_with_openai(question)
    
    if not sql:
        return "❌ No se pudo generar la consulta SQL. Por favor, intenta con otra pregunta."
    
    response = query_redshift(sql)

    return sql, response

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
        print("Evento recibido por Lambda")
        data = json.loads(event["body"])
        text = data["message"]["text"]
        chat_id = data["message"]["chat"]["id"]

        print('Mensaje input: ', text)
        print('Chat_id: ', chat_id)

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