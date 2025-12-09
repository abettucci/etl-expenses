import os
import json
import boto3
import base64
import requests
import pandas as pd
import openai
import time
import uuid
from datetime import datetime
from decimal import Decimal
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración inicial
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET_PROD = os.environ.get("BQ_DATASET_PROD", "PRD")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

# S3 Configuration para imágenes de tickets
S3_BUCKET_TICKETS = os.environ.get("S3_BUCKET_TICKETS", "etl-expenses-tickets")
S3_PREFIX_TICKETS = os.environ.get("S3_PREFIX_TICKETS", "receipts/")

# TabScanner API (fallback)
TABSCANNER_API_KEY = os.environ.get("TABSCANNER_API_KEY", "")

# BigQuery table para tickets de supermercado
BQ_TABLE_SUPERMARKET = os.environ.get("BQ_TABLE_SUPERMARKET", "supermarket_receipts")

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
REGION = os.environ.get("AWS_REGION", "us-east-2")
DDB_TABLE = os.environ.get("DDB_TABLE", "schema_cache")
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "604800"))  # 7 días

# Step Function para ETL de tickets (Express - síncrona)
RECEIPT_ETL_STATE_MACHINE = os.environ.get("RECEIPT_ETL_STATE_MACHINE", "")

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
            "HORA_PAGO": {
                "type": "STRING",
                "format": "HH/mm",
                "description": "Hora del pago en formato texto.",
                "example": "12:02"
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
            },
            "CUOTAS": {
                "type": "INTEGER",
                "description": "Cantidad de cuotas de la transacción",
                "example": "3"
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
            "TRANSACTION_DATE": {
                "type": "STRING",
                "description": "Fecha de la transacción",
                "example": "2025-02-12T17:55:15.000-03:00"
            },
            "PAYMENT_METHOD_TYPE" : {
                "type": "STRING",
                "description": "Metodo de pago",
                "example": "Tarjeta de crédito"
            },
            "PAYMENT_METHOD" : {
                "type" : "string",
                "descripcion" : "Emisor de tarjeta",
                "example" : "American Express"
            },
            "TRANSACTION_TYPE" : {
                "type" : "string",
                "descripcion" : "Tipo de transaccion realizada",
                "example" : "Devolución de dinero"
            },
            "TRANSACTION_AMOUNT": {
                "type": "STRING",
                "description": "Monto de la transacción",
                "example": "2500.00"
            },
            "STORE_NAME": {
                "type": "STRING",
                "description": "Nombre del destinatario de transferencia o comercio vendedor",
                "example": "DIA_TIENDA_478"
            },
            "REPORT_DATE" : {
                "type" : "string",
                "descripcion" : "Fecha en la que se emitio el reporte semanal de movimientos de mercado pago",
                "example" : "2024-10-14"
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
            "fecha": {
                "type": "STRING",
                "description": "Fecha de la compra",
                "example": "22/01/25"
            },
            "producto": {
                "type": "STRING",
                "description": "Nombre del producto comprado",
                "example": "LECHE ENTERA 1L"
            },
            "precio_unit": {
                "type": "STRING",
                "description": "Precio por unidad de producto",
                "example": "850.00"
            },
            "monto_total" : {
                "type" : "STRING",
                "descripcion" : "Monto total gastado en el producto. Resultado de multiplicar cantidad * precio_unit",
                "example" : "850.00"
            },
            "cantidad": {
                "type": "STRING",
                "description": "Cantidad comprada del producto",
                "example": "2.0"
            },
            "peso" : {
                "type" : "STRING",
                "descripcion" : "Cantidad comprada en peso (kg) del producto",
                "example" : "0.68"
            },
            "categoria" : {
                "type" : "STRING",
                "descripcion" : "Categoria de producto",
                "example" : "Frutas Y Verduras"
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
                "description": "ID único del producto",
                "example" : "11414"
            },
            "ean": {
                "type": "STRING",
                "description": "Codigo del producto",
                "example": "2505740010640"
            },
            "nombre_producto": {
                "type" : "STRING",
                "descripcion" : "Nombre del producto",
                "example" : "PICADA ESPECIAL NOVILLITO"
            },
            "grupo_producto" : {
                "type" : "STRING",
                "descripcion" : "Agrupador de productos",
                "example" : "picadaespecialnovillitoxkg"
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
s3_client = boto3.client("s3", region_name=REGION)
sfn_client = boto3.client("stepfunctions", region_name=REGION)

# =============================================================================
# PROCESAMIENTO DE IMÁGENES DE TICKETS
# =============================================================================

DDB_PROCESSED_MESSAGES_TABLE = os.environ.get("DDB_PROCESSED_MESSAGES_TABLE", "telegram_processed_messages")

def is_message_already_processed(message_id: int) -> bool:
    """Verifica si un mensaje de Telegram ya fue procesado (control de idempotencia)"""
    try:
        processed_table = dynamo.Table(DDB_PROCESSED_MESSAGES_TABLE)
        response = processed_table.get_item(Key={"message_id": str(message_id)})
        return "Item" in response
    except Exception as e:
        print(f"⚠️ Error verificando idempotencia: {e}")
        return False  # En caso de error, procesar de todos modos

def mark_message_as_processed(message_id: int) -> None:
    """Marca un mensaje de Telegram como procesado"""
    try:
        processed_table = dynamo.Table(DDB_PROCESSED_MESSAGES_TABLE)
        processed_table.put_item(
            Item={
                "message_id": str(message_id),
                "processed_at": int(time.time()),
                "ttl": int(time.time()) + 86400 * 7  # TTL de 7 días
            }
        )
    except Exception as e:
        print(f"⚠️ Error marcando mensaje como procesado: {e}")
        
def download_telegram_photo(file_id: str) -> bytes:
    """Descarga una foto de Telegram usando el file_id"""
    try:
        # Obtener información del archivo
        file_info_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile?file_id={file_id}"
        response = requests.get(file_info_url, timeout=10)
        file_info = response.json()
        
        if not file_info.get("ok"):
            raise Exception(f"Error obteniendo info del archivo: {file_info}")
        
        file_path = file_info["result"]["file_path"]
        
        # Descargar el archivo
        download_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"
        response = requests.get(download_url, timeout=30)
        response.raise_for_status()
        
        print(f"✅ Foto descargada de Telegram: {len(response.content)} bytes")
        return response.content
        
    except Exception as e:
        print(f"❌ Error descargando foto de Telegram: {e}")
        raise

def upload_image_to_s3(image_bytes: bytes, original_filename: str = None) -> str:
    """Sube una imagen a S3 y retorna la key"""
    try:
        # Generar nombre único
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        extension = ".jpg"  # Telegram envía JPG por defecto
        
        if original_filename and "." in original_filename:
            extension = "." + original_filename.split(".")[-1]
        
        s3_key = f"{S3_PREFIX_TICKETS}{timestamp}_{unique_id}{extension}"
        
        # Subir a S3
        s3_client.put_object(
            Bucket=S3_BUCKET_TICKETS,
            Key=s3_key,
            Body=image_bytes,
            ContentType="image/jpeg"
        )
        
        print(f"✅ Imagen subida a S3: s3://{S3_BUCKET_TICKETS}/{s3_key}")
        return s3_key
        
    except Exception as e:
        print(f"❌ Error subiendo imagen a S3: {e}")
        raise

def get_image_from_s3(s3_key: str) -> bytes:
    """Descarga una imagen de S3"""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_TICKETS, Key=s3_key)
        image_bytes = response["Body"].read()
        print(f"✅ Imagen leída de S3: {len(image_bytes)} bytes")
        return image_bytes
    except Exception as e:
        print(f"❌ Error leyendo imagen de S3: {e}")
        raise

def get_image_base64_from_s3(s3_key: str) -> str:
    """Obtiene una imagen de S3 y la convierte a base64"""
    image_bytes = get_image_from_s3(s3_key)
    return base64.b64encode(image_bytes).decode("utf-8")

def extract_receipt_with_openai(s3_key: str) -> dict:
    """
    Extrae datos de un ticket/recibo usando OpenAI Vision.
    Retorna un diccionario con los datos extraídos.
    """
    try:
        print(f"🤖 Extrayendo datos del ticket con OpenAI Vision...")
        
        # Obtener imagen en base64
        image_base64 = get_image_base64_from_s3(s3_key)
        
        # Schema de extracción
        extraction_prompt = """
            Analiza esta imagen de un ticket/recibo de supermercado y extrae la siguiente información en formato JSON:

            {
                "merchant_name": "nombre del comercio/supermercado",
                "transaction_date": "fecha de la compra en formato YYYY-MM-DD",
                "transaction_time": "hora de la compra en formato HH:MM",
                "total_amount": número con el total de la compra,
                "currency": "moneda (ARS, USD, etc.)",
                "payment_method": "método de pago si está visible",
                "line_items": [
                    {
                        "item_name": "nombre del producto",
                        "item_quantity": número de unidades,
                        "item_unit_price": precio unitario,
                        "item_total_price": precio total del item
                    }
                ]
            }

            IMPORTANTE:
            - Si algún campo no está visible o no se puede leer, usar null
            - Los precios deben ser números (sin símbolos de moneda)
            - La fecha debe estar en formato YYYY-MM-DD
            - Incluir TODOS los items que puedas leer del ticket
            - Responde SOLO con el JSON, sin explicaciones adicionales
        """

        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",  # gpt-4o-mini soporta vision
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": extraction_prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{image_base64}",
                                "detail": "high"
                            }
                        }
                    ]
                }
            ],
            max_tokens=2000,
            temperature=0.0
        )
        
        result_text = response.choices[0].message.content.strip()
        
        # Limpiar markdown si existe
        if result_text.startswith("```"):
            result_text = result_text.replace("```json", "").replace("```", "").strip()
        
        # Parsear JSON
        extracted_data = json.loads(result_text)
        extracted_data["extraction_method"] = "openai_vision"
        extracted_data["s3_key"] = s3_key
        
        print(f"✅ Datos extraídos con OpenAI: {len(extracted_data.get('line_items', []))} items")
        return extracted_data
        
    except json.JSONDecodeError as e:
        print(f"❌ Error parseando JSON de OpenAI: {e}")
        print(f"Respuesta raw: {result_text[:500]}")
        raise
    except Exception as e:
        print(f"❌ Error en extracción con OpenAI: {e}")
        raise

def extract_receipt_with_tabscanner(s3_key: str) -> dict:
    """
    Extrae datos de un ticket usando TabScanner API (fallback).
    """
    if not TABSCANNER_API_KEY:
        raise Exception("TABSCANNER_API_KEY no configurada")
    
    try:
        print(f"🔄 Extrayendo datos del ticket con TabScanner (fallback)...")
        
        # Obtener imagen de S3
        image_bytes = get_image_from_s3(s3_key)
        
        # Subir a TabScanner
        process_endpoint = "https://api.tabscanner.com/api/2/process"
        headers = {"apikey": TABSCANNER_API_KEY}
        files = {"file": ("receipt.jpg", image_bytes, "image/jpeg")}
        payload = {"documentType": "receipt"}
        
        response = requests.post(
            process_endpoint, 
            files=files, 
            data=payload, 
            headers=headers,
            timeout=30
        )
        result = response.json()
        
        if result.get("status") != "pending" and result.get("status") != "done":
            raise Exception(f"Error en TabScanner process: {result}")
        
        token = result.get("token")
        print(f"📝 TabScanner token: {token}")
        
        # Esperar y obtener resultado (con retry)
        result_endpoint = f"https://api.tabscanner.com/api/result/{token}"
        max_retries = 10
        
        for i in range(max_retries):
            time.sleep(2)  # Esperar 2 segundos entre intentos
            response = requests.get(result_endpoint, headers=headers, timeout=30)
            result = response.json()
            
            if result.get("status") == "done":
                break
            elif result.get("status") == "failed":
                raise Exception(f"TabScanner falló: {result}")
        
        if result.get("status") != "done":
            raise Exception("TabScanner timeout")
        
        # Convertir resultado de TabScanner a nuestro formato
        ts_result = result.get("result", {})
        
        extracted_data = {
            "merchant_name": ts_result.get("establishment"),
            "transaction_date": ts_result.get("date"),
            "transaction_time": ts_result.get("time"),
            "total_amount": ts_result.get("total"),
            "currency": ts_result.get("currency", "ARS"),
            "payment_method": ts_result.get("paymentMethod"),
            "line_items": [],
            "extraction_method": "tabscanner",
            "s3_key": s3_key
        }
        
        # Convertir line items
        for item in ts_result.get("lineItems", []):
            extracted_data["line_items"].append({
                "item_name": item.get("descClean") or item.get("desc"),
                "item_quantity": item.get("qty", 1),
                "item_unit_price": item.get("price"),
                "item_total_price": item.get("lineTotal")
            })
        
        print(f"✅ Datos extraídos con TabScanner: {len(extracted_data['line_items'])} items")
        return extracted_data
        
    except Exception as e:
        print(f"❌ Error en extracción con TabScanner: {e}")
        raise

def extract_receipt_data(s3_key: str, use_fallback: bool = True) -> dict:
    """
    Extrae datos de un ticket usando OpenAI Vision, con fallback a TabScanner.
    """
    try:
        # Intentar con OpenAI primero
        return extract_receipt_with_openai(s3_key)
    except Exception as openai_error:
        print(f"⚠️ OpenAI falló: {openai_error}")
        
        if use_fallback and TABSCANNER_API_KEY:
            print("🔄 Intentando con TabScanner...")
            return extract_receipt_with_tabscanner(s3_key)
        else:
            raise openai_error

def receipt_data_to_dataframe(extracted_data: dict) -> pd.DataFrame:
    """
    Convierte los datos extraídos del ticket a un DataFrame.
    """
    rows = []
    
    # Datos comunes del ticket
    common_data = {
        "merchant_name": extracted_data.get("merchant_name"),
        "transaction_date": extracted_data.get("transaction_date"),
        "transaction_time": extracted_data.get("transaction_time"),
        "total_amount": extracted_data.get("total_amount"),
        "currency": extracted_data.get("currency", "ARS"),
        "payment_method": extracted_data.get("payment_method"),
        "extraction_method": extracted_data.get("extraction_method"),
        "s3_key": extracted_data.get("s3_key"),
        "processed_at": datetime.now().isoformat()
    }
    
    # Crear una fila por cada item
    line_items = extracted_data.get("line_items", [])
    
    if line_items:
        for item in line_items:
            row = common_data.copy()
            row["item_name"] = item.get("item_name")
            row["item_quantity"] = item.get("item_quantity")
            row["item_unit_price"] = item.get("item_unit_price")
            row["item_total_price"] = item.get("item_total_price")
            rows.append(row)
    else:
        # Si no hay items, crear una fila con los datos generales
        rows.append(common_data)
    
    df = pd.DataFrame(rows)
    print(f"📊 DataFrame creado: {len(df)} filas, {len(df.columns)} columnas")
    return df

def load_receipt_to_bigquery(df: pd.DataFrame, bq_client) -> int:
    """
    Carga el DataFrame del ticket en BigQuery.
    Retorna el número de filas insertadas.
    """
    try:
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{BQ_TABLE_SUPERMARKET}"
        
        # Configurar el job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )
        
        # Cargar datos
        job = bq_client.load_table_from_dataframe(
            df, 
            table_id, 
            job_config=job_config
        )
        job.result()  # Esperar a que termine
        
        print(f"✅ Datos cargados en BigQuery: {table_id} ({len(df)} filas)")
        return len(df)
        
    except Exception as e:
        print(f"❌ Error cargando datos en BigQuery: {e}")
        raise

def format_receipt_response(extracted_data: dict, rows_inserted: int) -> str:
    """
    Formatea la respuesta para enviar al usuario por Telegram.
    """
    merchant = extracted_data.get("merchant_name", "Comercio desconocido")
    date = extracted_data.get("transaction_date", "Fecha desconocida")
    total = extracted_data.get("total_amount")
    items = extracted_data.get("line_items", [])
    
    # Formatear total
    if total:
        total_str = f"${total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    else:
        total_str = "No detectado"
    
    response = f"""
        ✅ *Ticket procesado exitosamente!*

        🏪 *Comercio:* {merchant}
        📅 *Fecha:* {date}
        💰 *Total:* {total_str}
        📦 *Items detectados:* {len(items)}

        *Productos extraídos:*
    """
    
    # Agregar primeros 10 items
    for i, item in enumerate(items[:10]):
        name = item.get("item_name", "?")
        qty = item.get("item_quantity", 1)
        price = item.get("item_total_price") or item.get("item_unit_price") or 0
        price_str = f"${price:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if price else "?"
        response += f"  • {name} x{qty} - {price_str}\n"
    
    if len(items) > 10:
        response += f"  _... y {len(items) - 10} productos más_\n"
    
    return response

def invoke_receipt_etl_step_function(sfn_client, s3_key: str, use_fallback: bool = True) -> dict:
    """
    Invoca la Step Function EXPRESS de forma síncrona para procesar el ticket.
    
    Args:
        s3_key: La key del archivo en S3
        use_fallback: Si usar TabScanner como fallback si OpenAI falla
    
    Returns:
        Diccionario con el resultado de la ejecución
    """
    try:
        if not RECEIPT_ETL_STATE_MACHINE:
            raise Exception("RECEIPT_ETL_STATE_MACHINE no está configurada")
        
        # Input para la Step Function
        sfn_input = {
            "s3_key": s3_key,
            "s3_bucket": S3_BUCKET_TICKETS,
            "use_fallback": use_fallback
        }
        
        # Generar nombre único para la ejecución
        execution_name = f"receipt-{datetime.now().strftime('%Y%m%d%H%M%S')}-{str(uuid.uuid4())[:8]}"
        
        print(f"🚀 Invocando Step Function: {RECEIPT_ETL_STATE_MACHINE}")
        print(f"📝 Input: {json.dumps(sfn_input)}")
        
        # Invocar Step Function EXPRESS de forma síncrona
        response = sfn_client.start_sync_execution(
            stateMachineArn=RECEIPT_ETL_STATE_MACHINE,
            name=execution_name,
            input=json.dumps(sfn_input)
        )
        
        # Procesar resultado
        status = response.get("status")
        
        if status == "SUCCEEDED":
            output = json.loads(response.get("output", "{}"))
            print(f"✅ Step Function completada exitosamente")
            return output
        else:
            error = response.get("error", "Unknown error")
            cause = response.get("cause", "No additional details")
            print(f"❌ Step Function falló: {status} - {error}: {cause}")
            raise Exception(f"ETL falló: {error} - {cause}")
            
    except Exception as e:
        print(f"❌ Error invocando Step Function: {e}")
        raise

def format_step_function_response(sfn_result: dict) -> str:
    """
    Formatea el resultado de la Step Function para enviar al usuario por Telegram.
    """
    if not sfn_result.get("success", False):
        error_msg = sfn_result.get("error_message", "Error desconocido")
        return f"❌ Error procesando el ticket:\n{error_msg}"
    
    extracted_data = sfn_result.get("extracted_data", {})
    rows_inserted = sfn_result.get("rows_inserted", 0)
    
    return format_receipt_response(extracted_data, rows_inserted)

def process_telegram_photo(sfn_client, message: dict, bq_client) -> str:
    """
    Procesa una foto enviada por Telegram:
    1. Descarga la foto
    2. Sube a S3
    3. Extrae datos con OpenAI/TabScanner
    4. Carga en BigQuery
    5. Retorna mensaje de respuesta
    """
    try:
        # Obtener el file_id de la foto (la de mayor resolución)
        photos = message.get("photo", [])
        if not photos:
            return "❌ No se encontró ninguna foto en el mensaje."
        
        # Telegram envía varias resoluciones, tomar la más grande
        photo = max(photos, key=lambda x: x.get("file_size", 0))
        file_id = photo.get("file_id")
        
        if not file_id:
            return "❌ No se pudo obtener el ID de la foto."
        
        print(f"📷 Procesando foto: {file_id}")
        
        # 1. Descargar foto de Telegram
        image_bytes = download_telegram_photo(file_id)

         # 2. Subir a S3
        s3_key = upload_image_to_s3(image_bytes)
        
        # 3. Invocar Step Function para ejecutar el ETL completo
        # La Step Function hace: OCR (OpenAI/TabScanner) + Carga a BigQuery
        sfn_result = invoke_receipt_etl_step_function(sfn_client, s3_key, use_fallback=True)
        
        # 4. Formatear respuesta basada en el resultado de la Step Function
        response = format_step_function_response(sfn_result)        
        
        return response
        
    except Exception as e:
        print(f"❌ Error procesando foto: {e}")
        import traceback
        traceback.print_exc()
        return f"❌ Error procesando el ticket: {str(e)}"

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

        system_prompt = """
            Eres un experto en SQL para Google BigQuery (Standard SQL).
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

        print(f"Generando SQL para: {question}")
        
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
        
        print(f"SQL generado:\n{sql}")
        return sql
        
    except Exception as e:
        print(f"Error generando SQL: {e}")
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
        print(f"Dry-run falló: {error_msg}")
        return False, error_msg

def query_bigquery(client, sql: str) -> str:
    """Ejecuta query en BigQuery y retorna resultados formateados"""
    try:
        print(f"🔍 Ejecutando query en BigQuery:\n{sql}")
        
        # Primero validar con dry-run
        is_valid, validation_error = validate_sql_dry_run(client, sql)
        if not is_valid:
            return f"Error de sintaxis SQL:\n{validation_error}\n\nQuery:\n{sql}"
        
        query_job = client.query(sql)
        results = query_job.result()  # Espera a que termine
        
        # Verificar si hay resultados
        if results.total_rows == 0:
            return "No se encontraron resultados para tu consulta."
        
        return format_bigquery_results(results)
        
    except Exception as e:
        error_msg = f"Error en BigQuery:\n{str(e)}\n\nSQL ejecutado:\n{sql}"
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
    
    return "Resultados:\n" + "\n".join(formatted_lines)

def handle_message(text: str, bq_client) -> tuple:
    """Maneja el mensaje del usuario y retorna SQL y respuesta"""
    question = text
    
    # Generar SQL con el nuevo sistema mejorado
    sql = generate_sql_with_openai2(question, bq_client)
    
    if not sql:
        return "", "No se pudo generar la consulta SQL. Por favor, intenta con otra pregunta."
    
    # Validar primero con dry-run
    is_valid, validation_error = validate_sql_dry_run(bq_client, sql)
    
    if not is_valid:
        print(f"Primera query inválida, intentando regenerar...")
        # Intentar regenerar con el error como contexto
        retry_sql = retry_sql_generation(question, sql, validation_error, bq_client)
        if retry_sql:
            sql = retry_sql
            is_valid, _ = validate_sql_dry_run(bq_client, sql)
    
    if not is_valid:
        return sql, f"No pude generar una consulta válida. Error: {validation_error}"
    
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
        
        print(f"SQL regenerado:\n{sql}")
        return sql
        
    except Exception as e:
        print(f"Error en retry: {e}")
        return ""

def send_telegram_message(chat_id, text, token):
    """Envía mensaje a Telegram"""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print("❌ Telegram API Error:")
        print(response.text)
        raise
    except Exception as e:
        print(f"Error enviando mensaje a Telegram: {e}")
        return None

def lambda_handler(event, context):
    try:
        print("📥 Evento recibido por Lambda")
        
        # Inicializar cliente de BigQuery
        bq_client = get_bigquery_client()
        
        data = json.loads(event["body"])
        message = data.get("message", {})
        chat_id = message.get("chat", {}).get("id")
        
        if not chat_id:
            print("⚠️ No se encontró chat_id en el mensaje")
            return {"statusCode": 200}

        print(f'👤 Chat_id: {chat_id}')
        
        # =========================================
        # DETECTAR SI ES UNA FOTO O TEXTO
        # =========================================
        
        # Verificar si el mensaje contiene una foto
        if "photo" in message:
            print("📷 Mensaje con foto detectado")

            # Control de idempotencia: verificar si ya procesamos este mensaje
            telegram_message_id = message.get("message_id")
            if telegram_message_id and is_message_already_processed(telegram_message_id):
                print(f"⚠️ Mensaje {telegram_message_id} ya fue procesado, ignorando duplicado")
                return {"statusCode": 200}
            
            # Marcar como procesado ANTES de procesar (para evitar race conditions)
            if telegram_message_id:
                mark_message_as_processed(telegram_message_id)
            
            # Enviar mensaje de "procesando" (solo una vez)            
            send_telegram_message(
                chat_id, 
                "📷 *Recibí tu ticket!*\n\n🔄 Procesando imagen...\nEsto puede tomar unos segundos.", 
                TELEGRAM_BOT_TOKEN
            )
            
            # Procesar la foto
            response_text = process_telegram_photo(sfn_client, message, bq_client)

            print('response_text: ', response_text)
            
            send_telegram_message(chat_id, response_text, TELEGRAM_BOT_TOKEN)
            return {"statusCode": 200}
        
        # Si no es foto, debe ser texto
        text = message.get("text", "")
        
        if not text:
            send_telegram_message(
                chat_id, 
                "❓ No entendí tu mensaje. Podés:\n• Enviarme una *pregunta* sobre tus gastos\n• Enviarme una *foto de un ticket* para procesarlo", 
                TELEGRAM_BOT_TOKEN
            )
            return {"statusCode": 200}

        print(f'💬 Mensaje input: {text}')

        # =========================================
        # MANEJAR COMANDOS
        # =========================================
        
        if text == "/start":
            welcome_message = """
            🤖 *Bot de Consultas de Datos con IA*

            ¡Hola! Soy tu asistente inteligente para gestionar tus gastos.

            🎯 *Características:*
            • Consultas de datos con lenguaje natural
            • Procesamiento de tickets de supermercado
            • Datos almacenados en BigQuery

            💬 *Puedes preguntarme:*
            • "¿Cuánto gasté este mes?"
            • "Mostrame los gastos por comercio"
            • "¿Cuál fue mi mayor gasto?"
            • "Gastos de los últimos 3 meses"

            📷 *También podés enviarme:*
            • Fotos de tickets de supermercado
            • Los proceso automáticamente con IA
            • Los datos se guardan en BigQuery

            ¡Escribí tu pregunta o enviame una foto de un ticket!
            """

            send_telegram_message(chat_id, welcome_message, TELEGRAM_BOT_TOKEN)
            return {"statusCode": 200}
        
        if text == "/help":
            help_message = """
                📚 *Ayuda del Bot*

                *Consultas de texto:*
                Escribí cualquier pregunta sobre tus gastos en lenguaje natural.

                *Ejemplos:*
                • ¿Cuánto gasté este mes?
                • Gastos por comercio
                • Mis mayores gastos
                • ¿Cuánto gasté en Carrefour?

                *Fotos de tickets:*
                Enviame una foto clara de un ticket de supermercado y lo proceso automáticamente.

                *Comandos:*
                • /start - Mensaje de bienvenida
                • /help - Esta ayuda
            """

            send_telegram_message(chat_id, help_message, TELEGRAM_BOT_TOKEN)
            return {"statusCode": 200}

        # =========================================
        # PROCESAR PREGUNTA DE TEXTO
        # =========================================
        
        sql, response_text = handle_message(text, bq_client)
        
        print('response_text: ', response_text)

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
            chat_id = data.get("message", {}).get("chat", {}).get("id")
            if chat_id:
                send_telegram_message(
                    chat_id, 
                    "❌ Ocurrió un error al procesar tu mensaje. Por favor, intentá de nuevo.", 
                    TELEGRAM_BOT_TOKEN
                )
        except Exception as nested_e:
            print(f"[ERROR] No se pudo enviar mensaje de error: {str(nested_e)}")

        return {"statusCode": 200}
