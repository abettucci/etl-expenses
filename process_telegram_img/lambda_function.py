import os
import json
import boto3
import base64
import requests
import openai
import pandas as pd
import time
import uuid
from datetime import datetime
from decimal import Decimal
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración inicial
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET_PROD = os.environ.get("BQ_DATASET_PROD", "PRD")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

# S3 Configuration para imágenes de tickets
S3_BUCKET_TICKETS = os.environ.get("S3_BUCKET_TICKETS", "etl-expenses-tickets")
S3_PREFIX_TICKETS = os.environ.get("S3_PREFIX_TICKETS", "receipts/")

# OpenAI API
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# TabScanner API (fallback)
TABSCANNER_API_KEY = os.environ.get("TABSCANNER_API_KEY", "")

# BigQuery table para tickets de supermercado
BQ_TABLE_SUPERMARKET = os.environ.get("BQ_TABLE_SUPERMARKET", "supermarket_receipts")

REGION = os.environ.get("AWS_REGION", "us-east-2")

# --- Clientes AWS/GCP ---
glue_client = boto3.client("glue", region_name=REGION)
secrets_client = boto3.client("secretsmanager", region_name=REGION)
s3_client = boto3.client("s3", region_name=REGION)

def get_bigquery_client(GCP_PROJECT_ID, BQ_LOCATION):
    """Inicializa cliente de BigQuery con credenciales de Secrets Manager"""
    secrets_client = boto3.client('secretsmanager')
    secret_response = secrets_client.get_secret_value(SecretId='gcp_api_credentials')
    credentials_json = json.loads(secret_response['SecretString'])
    
    credentials = service_account.Credentials.from_service_account_info(credentials_json)
    client = bigquery.Client(
        credentials=credentials,
        project=GCP_PROJECT_ID,
        location=BQ_LOCATION
    )
    return client

def upload_image_to_s3(s3_client, image_bytes, original_filename, S3_BUCKET_TICKETS, S3_PREFIX_TICKETS) -> str:
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

def extract_receipt_data(s3_key: str, OPENAI_API_KEY, TABSCANNER_API_KEY, s3_client, S3_BUCKET_TICKETS, use_fallback: bool = True) -> dict:
    """
    Extrae datos de un ticket usando OpenAI Vision, con fallback a TabScanner.
    """
    try:
        # Intentar con OpenAI primero
        return extract_receipt_with_openai(s3_key, OPENAI_API_KEY, s3_client, S3_BUCKET_TICKETS)
    except Exception as openai_error:
        print(f"⚠️ OpenAI falló: {openai_error}")
        
        if use_fallback and TABSCANNER_API_KEY:
            print("🔄 Intentando con TabScanner...")
            return extract_receipt_with_tabscanner(s3_key, s3_client, S3_BUCKET_TICKETS)
        else:
            raise openai_error

def extract_receipt_with_openai(s3_key: str, OPENAI_API_KEY, s3_client, S3_BUCKET_TICKETS) -> dict:
    """
    Extrae datos de un ticket/recibo usando OpenAI Vision.
    Retorna un diccionario con los datos extraídos.
    """
    try:
        print(f"🤖 Extrayendo datos del ticket con OpenAI Vision...")
        openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)
        
        # Obtener imagen en base64
        image_base64 = get_image_base64_from_s3(s3_key, s3_client, S3_BUCKET_TICKETS)
        
        # Schema de extracción
        extraction_prompt = """
            Analiza esta imagen de un ticket/recibo de supermercado y extrae la siguiente información en formato JSON:

            {
                "merchant_name": "nombre del comercio/supermercado",
                "merchant_address": "dirección del comercio (si está visible)",
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

            IMPORTANTE SOBRE LA FECHA:
            - Busca el campo "Fecha" o "Fecha:" en el ticket (generalmente cerca de "P.V. Nro", "Nro T." o "Hora")
            - En tickets argentinos, la fecha suele estar en formato DD/MM/YY o DD/MM/YYYY
            - Si el año tiene 2 dígitos (ej: 26), asume que es 20XX (ej: 2026)
            - Ejemplo: "Fecha 19/03/26" debe convertirse a "2026-03-19"
            - NO uses fechas de vencimiento (Vto:), CAE, o cualquier otra fecha que no sea la fecha de compra
            
            OTRAS REGLAS:
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

def extract_receipt_with_tabscanner(s3_key: str, TABSCANNER_API_KEY, s3_client, S3_BUCKET_TICKETS) -> dict:
    """
    Extrae datos de un ticket usando TabScanner API (fallback).
    """
    if not TABSCANNER_API_KEY:
        raise Exception("TABSCANNER_API_KEY no configurada")
    
    try:
        print(f"🔄 Extrayendo datos del ticket con TabScanner (fallback)...")
        
        # Obtener imagen de S3
        image_bytes = get_image_from_s3(s3_key, s3_client, S3_BUCKET_TICKETS)
        
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
            "merchant_address": ts_result.get("address"),
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

def get_image_from_s3(s3_key: str, s3_client, S3_BUCKET_TICKETS) -> bytes:
    """Descarga una imagen de S3"""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_TICKETS, Key=s3_key)
        image_bytes = response["Body"].read()
        print(f"✅ Imagen leída de S3: {len(image_bytes)} bytes")
        return image_bytes
    except Exception as e:
        print(f"❌ Error leyendo imagen de S3: {e}")
        raise

def get_image_base64_from_s3(s3_key: str, s3_client, S3_BUCKET_TICKETS) -> str:
    """Obtiene una imagen de S3 y la convierte a base64"""
    image_bytes = get_image_from_s3(s3_key, s3_client, S3_BUCKET_TICKETS)
    return base64.b64encode(image_bytes).decode("utf-8")

def receipt_data_to_dataframe(extracted_data: dict) -> pd.DataFrame:
    """
    Convierte los datos extraídos del ticket a un DataFrame.
    """
    rows = []
    
    # Datos comunes del ticket
    common_data = {
        "merchant_name": extracted_data.get("merchant_name"),
        "merchant_address": extracted_data.get("merchant_address"),
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

def load_receipt_to_bigquery(df: pd.DataFrame, bq_client, GCP_PROJECT_ID, BQ_DATASET_PROD, BQ_TABLE_SUPERMARKET) -> int:
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

def format_receipt_response(extracted_data, rows_inserted, BQ_TABLE_SUPERMARKET) -> str:
    """
    Formatea la respuesta para enviar al usuario por Telegram.
    """
    merchant = extracted_data.get("merchant_name", "Comercio desconocido")
    date = extracted_data.get("transaction_date", "Fecha desconocida")
    total = extracted_data.get("total_amount")
    items = extracted_data.get("line_items", [])
    method = extracted_data.get("extraction_method", "desconocido")
    
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
    
    response += f"""
        📊 *Datos cargados:* {rows_inserted} filas en BigQuery
        🗄️ *Tabla:* `{BQ_TABLE_SUPERMARKET}`
        🔍 *Método:* {method}
    """
    
    return response

def lambda_handler(event, context):
    """
    Handler para la Lambda de extracción OCR.
    
    Input (desde Step Function):
        {
            "s3_key": "receipts/20241130_123456_abc123.jpg",
            "s3_bucket": "telegram-receipts",
            "use_fallback": true
        }
    
    Output:
        {
            "merchant_name": "CARREFOUR",
            "transaction_date": "2024-11-30",
            "total_amount": 15420.50,
            "line_items": [...],
            "extraction_method": "openai_vision",
            "s3_key": "...",
            "s3_bucket": "..."
        }
    """

    try:
        print(f"📥 Evento recibido: {json.dumps(event)}")
        
        s3_key = event.get("s3_key")
        s3_bucket = event.get("s3_bucket", S3_BUCKET_TICKETS)
        use_fallback = event.get("use_fallback", True)
        
        if not s3_key:
            raise ValueError("s3_key es requerido")
        
        # Extraer datos del ticket
        extracted_data = extract_receipt_data(s3_key, OPENAI_API_KEY, TABSCANNER_API_KEY, s3_client, S3_BUCKET_TICKETS, use_fallback=True)

        # bq_client = get_bigquery_client(GCP_PROJECT_ID, BQ_LOCATION)
        # # Convertir a DataFrame
        # df = receipt_data_to_dataframe(extracted_data)
        # # Cargar en BigQuery
        # rows_inserted = load_recei    pt_to_bigquery(df, bq_client, GCP_PROJECT_ID, BQ_DATASET_PROD, BQ_TABLE_SUPERMARKET)
        # # Formatear respuesta
        # response = format_receipt_response(extracted_data, rows_inserted, BQ_TABLE_SUPERMARKET)
        
        return extracted_data
                
    except Exception as e:
        print(f"❌ Error en Lambda: {e}")
        import traceback
        traceback.print_exc()
        raise