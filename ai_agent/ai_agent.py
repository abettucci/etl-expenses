import os
import json
import boto3
from telegram import Bot, Update
from langchain_community.llms import Bedrock
import requests

# Configuración inicial
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
# La URL del webhook se configurará manualmente en Telegram
# No necesitamos API_GATEWAY_URL como variable de entorno

bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Cambiar a un modelo más común y agregar manejo de errores
try:
    llm = Bedrock(
        model_id="anthropic.claude-v2",  # Modelo principal recomendado
        model_kwargs={
            "max_tokens": 512,
            "temperature": 0.1,
            "top_p": 0.9
        }
    )
    print("✅ Modelo Claude v2 cargado exitosamente")
except Exception as e:
    print(f"❌ Error cargando modelo Claude: {e}")
    try:
        # Fallback a Titan si Claude no está disponible
        llm = Bedrock(
            model_id="amazon.titan-text-express-v1",
            model_kwargs={"maxTokenCount": 512, "temperature": 0.1}
        )
        print("✅ Modelo Titan (fallback) cargado exitosamente")
    except Exception as e2:
        print(f"❌ Error cargando modelo fallback: {e2}")
        llm = None

redshift_data = boto3.client('redshift-data')

# Verificar que Bedrock esté disponible
try:
    bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-2')
    print("✅ Cliente Bedrock configurado correctamente")
except Exception as e:
    print(f"❌ Error configurando cliente Bedrock: {e}")

# Conexión a Redshift
def query_redshift(sql: str) -> str:
    """Ejecuta SQL y devuelve resultados formateados"""
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
                return "✅ Consulta ejecutada (sin resultados)"
            elif status['Status'] == 'FAILED':
                return f"❌ Error en Redshift: {status['Error']}"
    except Exception as e:
        return f"⚠️ Error: {str(e)}"

def format_redshift_results(results: dict) -> str:
    """Convierte resultados de Redshift Data API a Markdown"""
    columns = [col['name'] for col in results['ColumnMetadata']]
    rows = [
        " | ".join(str(field.get('stringValue', field.get('longValue', ''))) for field in record)
        for record in results['Records']
    ]
    return (
        "📊 Resultados:\n" +
        " | ".join(columns) + "\n" +
        "|-" * len(columns) + "\n" +
        "\n".join(rows)
    )

def generate_sql(question: str) -> str:
    # Versión simplificada sin FAISS
    if llm is None:
        return "SELECT 'Error: Modelo de IA no disponible' as error"
    
    try:
        prompt = f"""
            Genera SQL para responder a esta pregunta: {question}
            
            Las tablas disponibles son:
            - mp_data (transacciones)
            - bank_payments (gastos bancarios)
            
            SQL:
        """
        return llm(prompt)
    except Exception as e:
        print(f"❌ Error generando SQL: {e}")
        return "SELECT 'Error generando consulta SQL' as error"

# Manejo de Telegram - versión simplificada para Lambda
def handle_message(text: str) -> str:
    question = text
    sql = generate_sql(question)
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
        "parse_mode": "Markdown"
    }
    requests.post(url, json=payload)

def set_webhook(token, api_url):
    url = f"https://api.telegram.org/bot{token}/setWebhook"
    response = requests.post(url, params={"url": api_url})
    print("Webhook setup response:", response.json())

# El webhook se configurará manualmente después del deployment
# No se ejecuta automáticamente en Lambda

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
🤖 *Bot de Consultas de Datos*

¡Hola! Soy tu asistente para consultar datos de transacciones y gastos.

Puedes preguntarme cosas como:
• "¿Cuánto gasté este mes?"
• "Mostrame las transacciones de ayer"
• "¿Cuál fue el gasto más alto?"

¡Escribí tu pregunta!
            """
            send_telegram_message(chat_id, welcome_message, TELEGRAM_BOT_TOKEN)
            return {"statusCode": 200}

        response_text = handle_message(text)
        send_telegram_message(chat_id, response_text, TELEGRAM_BOT_TOKEN)

        return {"statusCode": 200}
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
    