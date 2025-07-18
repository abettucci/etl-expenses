import os
import json
import boto3
from telegram import Bot, Update
from langchain_community.llms import Bedrock
import requests

# Configuración inicial
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
API_GATEWAY_URL = os.environ.get("API_GATEWAY_URL")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
llm = Bedrock(model_id="amazon.nova-micro-v1:0")
redshift_data = boto3.client('redshift-data')

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
    prompt = f"""
        Genera SQL para responder a esta pregunta: {question}
        
        Las tablas disponibles son:
        - mp_data (transacciones)
        - bank_payments (gastos bancarios)
        
        SQL:
    """
    return llm(prompt)

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

# Dentro del lambda_handler o setup inicial
set_webhook(TELEGRAM_BOT_TOKEN, API_GATEWAY_URL)

def lambda_handler(event, context):
    try:
        print("== Evento recibido por Lambda ==")
        print(json.dumps(event))  # Agrega esto

        data = json.loads(event["body"])
        text = data["message"]["text"]
        chat_id = data["message"]["chat"]["id"]

        print('text: ', text)
        print('chat_id: ', chat_id)

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

        return {"statusCode": 500}
    