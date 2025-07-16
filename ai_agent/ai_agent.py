import os
import json
import boto3
from telegram import Bot, Update
from langchain_community.llms import Bedrock

# Configuración inicial
bot = Bot(token="REDACTED_TELEGRAM_BOT_TOKEN")
llm = Bedrock(model_id="anthropic.claude-3-sonnet-20240229-v1:0")
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
def handle_message(update: Update) -> str:
    question = update.message.text
    sql = generate_sql(question)
    response = query_redshift(sql)
    
    return f"""
        🔍 *Consulta:* {question}

        ```sql
        {sql}
        ```

        {response}
    """

def lambda_handler(event, context):
    try:
        # Procesar update
        update = Update.de_json(json.loads(event["body"]), bot)
        
        # Generar respuesta
        response_text = handle_message(update)
        
        # Enviar respuesta usando el bot directamente
        bot.send_message(
            chat_id=update.effective_chat.id,
            text=response_text,
            parse_mode="Markdown"
        )
            
        return {"statusCode": 200}
    except Exception as e:
        # En caso de error, intentar enviar mensaje de error
        try:
            update = Update.de_json(json.loads(event["body"]), bot)
            bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"❌ Error procesando tu consulta: {str(e)}"
            )
        except:
            pass
        return {"statusCode": 500}