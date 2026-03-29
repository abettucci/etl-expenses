import os
import io
import csv
import json
import html
import re
import boto3
import base64
import requests
import openai
import time
import uuid
from datetime import datetime, date, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.oauth2 import service_account

# Pandas - para conversión a DataFrame
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("⚠️ Pandas no disponible, usando conversión manual")

# Configuración inicial
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET_PROD = os.environ.get("BQ_DATASET_PROD", "PRD")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

# S3 Configuration para imágenes de tickets
S3_BUCKET_TICKETS = os.environ.get("S3_BUCKET_TICKETS", "telegram-receipts")
S3_PREFIX_TICKETS = os.environ.get("S3_PREFIX_TICKETS", "receipts/")
S3_PREFIX_EXPORTS = os.environ.get("S3_PREFIX_EXPORTS", "exports/")
EXPORT_MAX_ROWS = int(os.environ.get("EXPORT_MAX_ROWS", "3000"))
TELEGRAM_ALERT_CHAT_ID = os.environ.get("TELEGRAM_ALERT_CHAT_ID", "").strip()
ALERT_BUDGET_ARS = os.environ.get("ALERT_BUDGET_ARS", "").strip()
MAPPING_TABLE = os.environ.get("MAPPING_TABLE", "dim_comercio_mapping")
UNMAPPED_TABLE = os.environ.get("UNMAPPED_TABLE", "comercio_unmapped_queue")

# Step Function para ETL de tickets (Express - síncrona)
RECEIPT_ETL_STATE_MACHINE = os.environ.get("RECEIPT_ETL_STATE_MACHINE", "")

# TabScanner API (fallback) - usado por la Lambda de OCR
TABSCANNER_API_KEY = os.environ.get("TABSCANNER_API_KEY", "")

# BigQuery table para tickets de supermercado
BQ_TABLE_SUPERMARKET = os.environ.get("BQ_TABLE_SUPERMARKET", "supermarket_receipts")

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
REGION = os.environ.get("AWS_REGION", "us-east-2")
DDB_TABLE = os.environ.get("DDB_TABLE", "schema_cache")
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "604800"))  # 7 días

openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)

# Zona horaria para mostrar fechas de BigQuery en el chat
_TZ_AR = ZoneInfo("America/Argentina/Buenos_Aires")

# Etiquetas legibles (español) para columnas frecuentes en respuestas Telegram
_COLUMN_LABELS_ES = {
    "COMERCIO": "Comercio",
    "MONTO": "Monto",
    "FECHA_PAGO": "Fecha de pago",
    "HORA_PAGO": "Hora",
    "TARJETA": "Tarjeta",
    "DIVISA": "Divisa",
    "TRANSACTION_DATE": "Fecha y hora",
    "fecha_transaccion": "Fecha y hora",
    "SETTLEMENT_NET_AMOUNT": "Monto",
    "TRANSACTION_TYPE": "Tipo de operación",
    "PAYMENT_METHOD": "Medio de pago",
    "PAYMENT_METHOD_TYPE": "Tipo de medio",
    "SETTLEMENT_CURRENCY": "Moneda",
    "INSTALLMENTS": "Cuotas",
    "producto": "Producto",
    "categoria": "Categoría",
    "monto_total": "Monto",
    "nro_ticket": "Nº ticket",
    "fecha": "Fecha",
}

def _friendly_column_label(col_name: str) -> str:
    if col_name in _COLUMN_LABELS_ES:
        return _COLUMN_LABELS_ES[col_name]
    lower = col_name.lower()
    for k, v in _COLUMN_LABELS_ES.items():
        if k.lower() == lower:
            return v
    return col_name.replace("_", " ").strip().title()

def _looks_like_money_column(col_name: str) -> bool:
    u = col_name.upper()
    return any(
        x in u
        for x in ("MONTO", "AMOUNT", "PRECIO", "PRICE", "TOTAL", "NET", "SETTLEMENT")
    )

def _looks_like_date_column(col_name: str) -> bool:
    u = col_name.upper()
    return any(x in u for x in ("FECHA", "DATE", "TIME", "TIMESTAMP", "HORA"))

def _format_number_ar(value: float) -> str:
    s = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return s

def _format_money_ar(value: float) -> str:
    sign = "−" if value < 0 else ""
    body = _format_number_ar(abs(value))
    return f"{sign}$ {body}"

def _format_datetime_for_chat(value) -> str:
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(_TZ_AR)
        return dt.strftime("%d/%m/%Y %H:%M")
    if isinstance(value, date):
        return value.strftime("%d/%m/%Y")
    return str(value)

def _format_cell_for_chat(col_name: str, value) -> str:
    if value is None:
        return "—"
    if isinstance(value, str) and not value.strip():
        return "—"
    if isinstance(value, str) and value.strip().upper() in ("NULL", "NONE", "NAN"):
        return "—"

    if isinstance(value, Decimal):
        try:
            value = float(value)
        except Exception:
            return html.escape(str(value))

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if _looks_like_money_column(col_name):
            return _format_money_ar(float(value))
        if isinstance(value, float):
            return _format_number_ar(float(value))
        return str(value)

    if isinstance(value, datetime):
        return _format_datetime_for_chat(value)
    if isinstance(value, date):
        return _format_datetime_for_chat(value)

    s = str(value)
    if _looks_like_date_column(col_name) and not _looks_like_money_column(col_name):
        try:
            if re.match(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}", s):
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                return _format_datetime_for_chat(dt)
        except Exception:
            pass
    return html.escape(s)

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
            "Si el usuario pregunta por 'gastos del banco' o 'banco santander' o 'santander', usar esta tabla SIN filtros adicionales de banco",
            "El total de gastos es la suma de MONTO",
            "Cuando se pregunte por fechas, siempre convertir el campo FECHA_PAGO con la funcion PARSE_DATE(%d/%m/%Y', FECHA_PAGO)",
            "Cuando se pregunte por horarios, siempre convertir el campo HORA_PAGO con la funcion PARSE_TIME('%H:%S', HORA_PAGO)"
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
            "Incluye tanto pagos enviados como recibidos",
            "Cuando se pregunte por fechas, siempre convertir el campo TRANSACTION_DATE con la funcion TIMESTAMP()"
        ],
        "columns": {
            "TRANSACTION_DATE": {
                "type": "STRING",
                "description": "Fecha de la transacción",
                "example": "2025-02-12T17:55:15.000-03:00"
            },
            "SETTLEMENT_NET_AMOUNT": {
                "type": "STRING",
                "description": "Monto neto de la transacción",
                "example": "2500.00"
            },
            "TRANSACTION_TYPE": {
                "type": "STRING",
                "description": "Tipo de transaccion, si es una salida de dinero (PAYOUTS), si es una devolucion (CASHBACK), etc.",
                "example": "PAYOUTS"
            },
            "PAYMENT_METHOD": {
                "type": "STRING",
                "description": "Metodo de pago",
                "example": "American Express"
            },
            "PAYMENT_METHOD_TYPE": {
                "type": "STRING",
                "description": "Tipo de medio de pago",
                "example": "Tarjeta de credito"
            },
            "INSTALLMENTS": {
                "type": "STRING",
                "description": "Cuotas",
                "example": "1"
            },
            "SETTLEMENT_CURRENCY": {
                "type": "STRING",
                "description": "Divisa del pago",
                "example": "ARS"
            }
        }
    },
    "carrefour_data": {
        "description": "Compras en supermercado Carrefour con detalle de productos",
        "semantic_hints": [
            "Usar cuando pregunten por 'carrefour', 'supermercado', 'compras de comida'",
            "Tiene detalle a nivel de producto individual",
            "Cuando se pregunte por fechas, siempre convertir el campo FECHA con la funcion PARSE_DATE(%d/%m/%Y', FECHA)",
        ],
        "columns": {
            "fecha": {
                "type": "STRING",
                "description": "Fecha de la compra",
                "example": "2024-11-15"
            },
            "producto": {
                "type": "STRING",
                "description": "Nombre del producto comprado",
                "example": "LECHE ENTERA 1L"
            },
            "categoria": {
                "type": "STRING",
                "description": "Categoria del producto comprado",
                "example": "Frutas Y Verduras"
            },
            "monto_total": {
                "type": "FLOAT64",
                "description": "Monto total gasto en el producto. Puede ser el resultado de multilpicar precio_unit * cantidad o precio_unit * peso.",
                "example": "850.00"
            },
            "precio_unit": {
                "type": "FLOAT64",
                "description": "Precio por cada unidad del producto. En caso de ser un producto con peso <> 0 y cantidad = 0 entonces es precio por kilogramo del producto.",
                "example": "850.00"
            },
            "cantidad": {
                "type": "FLOAT64",
                "description": "Cantidad comprada",
                "example": "2.0"
            },
            "peso": {
                "type": "FLOAT64",
                "description": "Cantidad comprada en peso (kilogramos)",
                "example": "2.0"
            },
            "total_ticket_meli": {
                "type": "FLOAT64",
                "description": "Monto total del ticket considerando el descuento de Mercado Libre.",
                "example": "2.0"
            },
            "total_ticket_bruto": {
                "type": "FLOAT64",
                "description": "Monto total del ticket.",
                "example": "2.0"
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
            "nombre_producto": {
                "type": "STRING",
                "description": "Nombre del producto",
                "example" : "PICADA ESPECIAL NOVILLITO X KG"
            },
            "grupo_producto": {
                "type": "STRING",
                "description": "Agrupador de productos por nombres similares.",
                "example": "picadaespecialnovillitoxkg"
            }
        }
    },
    "supermarket_tickets": {
        "description": "Compras en supermercados que no son Carrefour, con detalle de productos",
        "semantic_hints": [
            "Usar cuando pregunten por 'supermercado', 'compras de comida'",
            "Tiene detalle a nivel de producto individual"
        ],
        "columns": {
            "merchant_name": {
                "type": "STRING",
                "description": "PONTEFRUT S.A."
            },
            "transaction_date": {
                "type": "STRING",
                "description": "Dia de la compra.",
                "example": "2022-12-03"
            },
            "transaction_time": {
                "type": "STRING",
                "description": "Horario de la compra.",
                "example": "18:40"
            },
            "total_amount": {
                "type": "STRING",
                "description": "Monto total del ticket",
                "example": "7750.0"
            },
            "currency": {
                "type": "STRING",
                "description": "Divisa del pago",
                "example": "ARS"
            },
            "payment_method" : {
                "type" : "STRING",
                "description" : "Metodo de pago",
                "example" : "Mercado Pago"
            },
            "item_name" : {
                "type" : "STRING",
                "description" : "Nombre del item",
                "example" : "Avena Arrollada Instantanea 1 Kg."
            },
            "item_quantity" : {
                "type" : "FLOAT64",
                "description" : "Cantidad comprada del item.",
                "example" : "1"
            },
            "item_unit_price" : {
                "type" : "FLOAT64",
                "description" : "Precio por unidad o kilogramo del item.",
                "example" : "3950.0"
            },
            "item_total_price" : {
                "type" : "FLOAT64",
                "description" : "Monto total comprado del item.",
                "example" : "3950.0"
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

# Tabla DynamoDB para control de idempotencia de mensajes de Telegram
DDB_PROCESSED_MESSAGES_TABLE = os.environ.get("DDB_PROCESSED_MESSAGES_TABLE", "telegram_processed_messages")
DDB_PENDING_TICKETS_TABLE = os.environ.get("DDB_PENDING_TICKETS_TABLE", "telegram_pending_tickets")
MULTI_PHOTO_TIMEOUT_SECONDS = 120  # 2 minutos para esperar más fotos

# Indicadores de que el ticket está completo (fin del ticket)
TICKET_END_INDICATORS = [
    "TOTAL",
    "TOTAL:",
    "TOT.",
    "SUBTOTAL",
    "IVA CONTENIDO",
    "GRACIAS POR SU COMPRA",
    "VUELTO",
    "SU VUELTO",
    "AHORRO",
    "TOT.AHORRO",
    "C.A.E",
    "CAE:",
    "CODIGO QR",
    "www.",
    "ATENCION TELEFONICA"
]

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

def try_acquire_message_lock(message_id: int) -> bool:
    """
    Intenta adquirir un lock atómico para procesar el mensaje.
    Usa ConditionExpression para garantizar que solo una invocación gane.
    Retorna True si este proceso debe procesar el mensaje, False si ya fue tomado por otro.
    """
    from botocore.exceptions import ClientError
    try:
        processed_table = dynamo.Table(DDB_PROCESSED_MESSAGES_TABLE)
        processed_table.put_item(
            Item={
                "message_id": str(message_id),
                "processed_at": int(time.time()),
                "ttl": int(time.time()) + 86400 * 7  # TTL de 7 días
            },
            ConditionExpression="attribute_not_exists(message_id)"
        )
        print(f"🔒 Lock adquirido para mensaje {message_id}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print(f"⚠️ Mensaje {message_id} ya está siendo procesado por otra invocación")
            return False
        print(f"⚠️ Error adquiriendo lock: {e}")
        return False
    except Exception as e:
        print(f"⚠️ Error inesperado adquiriendo lock: {e}")
        return False

# =============================================================================
# SISTEMA DE TICKETS MULTI-FOTO
# =============================================================================
# Permite procesar tickets largos que requieren múltiples fotos.
# Detecta si el ticket está completo buscando indicadores de "FINAL" o "TOTAL".

def get_pending_ticket(chat_id: int) -> dict:
    """Obtiene un ticket pendiente (incompleto) para un chat_id"""
    try:
        pending_table = dynamo.Table(DDB_PENDING_TICKETS_TABLE)
        response = pending_table.get_item(Key={"chat_id": str(chat_id)})
        item = response.get("Item")
        
        if not item:
            return None
        
        # Verificar si expiró (timeout)
        created_at = float(item.get("created_at", 0))
        if time.time() - created_at > MULTI_PHOTO_TIMEOUT_SECONDS:
            print(f"⏰ Ticket pendiente expirado para chat {chat_id}, eliminando...")
            delete_pending_ticket(chat_id)
            return None
        
        return item
    except Exception as e:
        print(f"⚠️ Error obteniendo ticket pendiente: {e}")
        return None

def save_pending_ticket(chat_id: int, partial_data: dict, s3_keys: list, photo_count: int) -> None:
    """Guarda un ticket pendiente (incompleto) para un chat_id"""
    try:
        pending_table = dynamo.Table(DDB_PENDING_TICKETS_TABLE)
        pending_table.put_item(
            Item={
                "chat_id": str(chat_id),
                "partial_data": json.dumps(partial_data),
                "s3_keys": s3_keys,
                "photo_count": photo_count,
                "created_at": Decimal(str(time.time())),
                "ttl": int(time.time()) + 300  # TTL de 5 minutos
            }
        )
        print(f"💾 Ticket pendiente guardado para chat {chat_id} ({photo_count} fotos)")
    except Exception as e:
        print(f"⚠️ Error guardando ticket pendiente: {e}")

def delete_pending_ticket(chat_id: int) -> None:
    """Elimina un ticket pendiente"""
    try:
        pending_table = dynamo.Table(DDB_PENDING_TICKETS_TABLE)
        pending_table.delete_item(Key={"chat_id": str(chat_id)})
        print(f"🗑️ Ticket pendiente eliminado para chat {chat_id}")
    except Exception as e:
        print(f"⚠️ Error eliminando ticket pendiente: {e}")

def check_ticket_is_complete(s3_key: str) -> tuple:
    """
    Analiza una imagen para verificar si contiene indicadores de fin de ticket.
    Retorna (is_complete: bool, extracted_data: dict)
    """
    try:
        print(f"🔍 Verificando si el ticket está completo...")
        
        image_base64 = get_image_base64_from_s3(s3_key)
        
        # Prompt para verificar si es el final del ticket
        check_prompt = """
            Analiza esta imagen de un ticket/recibo de supermercado.

            IMPORTANTE: Extrae ABSOLUTAMENTE TODOS los productos visibles, incluyendo:
            - El PRIMER producto que aparece en la parte superior de la imagen
            - El ÚLTIMO producto que aparece en la parte inferior de la imagen
            - NO omitas productos aunque estén parcialmente cortados

            Responde en formato JSON:

            {
                "is_complete": true/false,
                "has_total_with_amount": true/false,
                "has_cae": true/false,
                "has_qr_code": true/false,
                "detected_indicators": ["lista de indicadores encontrados"],
                "partial_data": {
                    "merchant_name": "nombre del comercio si es visible",
                    "transaction_date": "fecha en formato YYYY-MM-DD si es visible",
                    "transaction_time": "hora en formato HH:MM si es visible",
                    "total_amount": número del total FINAL si está visible (null si no hay TOTAL FINAL),
                    "currency": "ARS",
                    "line_items": [
                        {
                            "item_name": "nombre COMPLETO del producto",
                            "item_quantity": número,
                            "item_unit_price": precio unitario,
                            "item_total_price": precio total
                        }
                    ]
                }
            }

            CRITERIOS ESTRICTOS PARA is_complete=true (DEBEN cumplirse AL MENOS 2 de estos):
            1. Contiene la palabra "TOTAL" seguida de un monto final (ej: "TOTAL 53453,46")
            2. Contiene "C.A.E" o "CAE:" con número de autorización
            3. Contiene código QR fiscal visible
            4. Contiene "IVA CONTENIDO" con un monto
            5. Contiene "www." o número de atención telefónica al final

            is_complete=false SI:
            - Solo ves productos/items sin sección de totales
            - La imagen parece cortada y continúa más abajo
            - No hay información fiscal (CAE, IVA, QR)
            - Solo ves el encabezado del ticket con logo y primeros productos

            EXTRACCIÓN DE ITEMS:
            - Incluye TODOS los productos de arriba a abajo
            - El primer item visible es tan importante como el último
            - Si un producto está cortado pero se puede leer parcialmente, inclúyelo

            Responde SOLO con el JSON.
        """

        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": check_prompt},
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
        
        # Limpiar markdown
        if result_text.startswith("```"):
            result_text = result_text.replace("```json", "").replace("```", "").strip()
        
        result = json.loads(result_text)
        
        is_complete = result.get("is_complete", False)
        partial_data = result.get("partial_data", {})
        detected_indicators = result.get("detected_indicators", [])
        
        print(f"📋 Ticket completo: {is_complete}")
        print(f"📋 Indicadores detectados: {detected_indicators}")
        print(f"📋 Items extraídos: {len(partial_data.get('line_items', []))}")
        
        return is_complete, partial_data
        
    except Exception as e:
        print(f"❌ Error verificando completitud del ticket: {e}")
        # En caso de error, asumir que está completo para no bloquear
        return True, {}

def merge_ticket_data(data_parts: list) -> dict:
    """
    Fusiona datos de múltiples partes de un ticket en uno solo.
    Calcula el total sumando todos los items si no hay total explícito.
    """
    if not data_parts:
        return {}
    
    if len(data_parts) == 1:
        return data_parts[0]
    
    # Tomar datos generales de la primera parte (nombre comercio, fecha, etc.)
    merged = {
        "merchant_name": None,
        "transaction_date": None,
        "transaction_time": None,
        "total_amount": None,
        "currency": "ARS",
        "payment_method": None,
        "line_items": [],
        "extraction_method": "openai_vision_multi"
    }
    
    # Buscar merchant_name y fecha en las primeras partes
    for part in data_parts:
        if not merged["merchant_name"] and part.get("merchant_name"):
            merged["merchant_name"] = part["merchant_name"]
        if not merged["transaction_date"] and part.get("transaction_date"):
            merged["transaction_date"] = part["transaction_date"]
        if not merged["transaction_time"] and part.get("transaction_time"):
            merged["transaction_time"] = part["transaction_time"]
        if not merged["payment_method"] and part.get("payment_method"):
            merged["payment_method"] = part["payment_method"]
    
    # Concatenar todos los line_items (evitar duplicados por nombre similar)
    seen_items = set()
    for part in data_parts:
        items = part.get("line_items", [])
        for item in items:
            item_key = (item.get("item_name", ""), item.get("item_total_price", 0))
            if item_key not in seen_items:
                merged["line_items"].append(item)
                seen_items.add(item_key)
    
    # El total debería estar en la última parte (donde está el "TOTAL" del ticket)
    for part in reversed(data_parts):
        if part.get("total_amount"):
            merged["total_amount"] = part["total_amount"]
            break
    
    # Si no encontramos total explícito, calcular desde los items
    if not merged["total_amount"] and merged["line_items"]:
        calculated_total = 0
        for item in merged["line_items"]:
            price = item.get("item_total_price") or item.get("item_unit_price") or 0
            if isinstance(price, (int, float)):
                calculated_total += price
        if calculated_total > 0:
            merged["total_amount"] = calculated_total
            print(f"💰 Total calculado desde items: {calculated_total}")
    
    print(f"🔗 Ticket fusionado: {len(merged['line_items'])} items de {len(data_parts)} fotos")
    print(f"🔗 Total final: {merged['total_amount']}")
    
    return merged

# =============================================================================
# PROCESAMIENTO DE IMÁGENES DE TICKETS
# =============================================================================

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
            Analiza esta imagen de un ticket/recibo de supermercado.

            TAREA CRÍTICA: Debes extraer ABSOLUTAMENTE TODOS los productos visibles en la imagen.

            INSTRUCCIONES ESPECIALES:
            1. Empieza desde el PRIMER producto visible en la PARTE SUPERIOR de la imagen
            2. Continúa hasta el ÚLTIMO producto visible en la PARTE INFERIOR
            3. NO omitas ningún producto, aunque esté parcialmente cortado o borroso
            4. Si un producto está cortado pero puedes leer parte del nombre, inclúyelo
            5. Los productos suelen tener: descripción + código + precio

            Extrae en formato JSON:

            {
                "merchant_name": "nombre del comercio/supermercado",
                "transaction_date": "fecha de la compra en formato YYYY-MM-DD",
                "transaction_time": "hora de la compra en formato HH:MM",
                "total_amount": número con el total de la compra (solo si ves "TOTAL" con monto),
                "currency": "ARS",
                "payment_method": "método de pago si está visible",
                "line_items": [
                    {
                        "item_name": "nombre COMPLETO del producto",
                        "item_quantity": número de unidades (default 1),
                        "item_unit_price": precio unitario si está visible,
                        "item_total_price": precio total del item
                    }
                ]
            }

            REGLAS DE EXTRACCIÓN:
            - Incluir TODOS los items de arriba a abajo sin excepción
            - El primer item de la imagen es tan importante como el último
            - Si ves "0,164 x 17999,00" seguido de un nombre = ese es un item por peso
            - Los precios en Argentina usan coma para decimales: 2951,84 = 2951.84
            - Ignorar líneas de descuento (ej: "MERCADO PAGO 25% - V")
            - NO incluir líneas de subtotales parciales o descuentos como items
            - Si no hay TOTAL visible, dejar total_amount como null

            Responde SOLO con el JSON, sin explicaciones:
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
        
        # === LOGGING DETALLADO DEL DATAFRAME ===
        print("=" * 80)
        print("📊 ANÁLISIS DEL DATAFRAME ANTES DE CARGAR A BIGQUERY")
        print("=" * 80)
        
        # 1. Información general
        print(f"📋 Número de filas: {len(df)}")
        print(f"📋 Número de columnas: {len(df.columns)}")
        print(f"📋 Columnas: {list(df.columns)}")
        
        # 2. Tipos de datos del DataFrame
        print("\n📊 TIPOS DE DATOS DEL DATAFRAME:")
        for col in df.columns:
            dtype = df[col].dtype
            non_null = df[col].notna().sum()
            null_count = df[col].isna().sum()
            print(f"  • {col}: {dtype} (Non-null: {non_null}, Null: {null_count})")
            
            # Mostrar algunos valores de ejemplo
            sample_values = df[col].dropna().head(3).tolist()
            if sample_values:
                print(f"    Ejemplos: {sample_values}")
        
        # 3. Obtener el esquema actual de BigQuery
        print("\n📊 ESQUEMA DE LA TABLA EN BIGQUERY:")
        try:
            table = bq_client.get_table(table_id)
            print(f"  Tabla: {table_id}")
            for field in table.schema:
                print(f"  • {field.name}: {field.field_type} (mode: {field.mode})")
        except Exception as schema_error:
            print(f"  ⚠️ No se pudo obtener el esquema: {schema_error}")
        
        # 4. Muestra de los primeros 3 registros
        print("\n📋 MUESTRA DE DATOS (primeras 3 filas):")
        print(df.head(3).to_string())
        
        print("\n" + "=" * 80)
        
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
        print("\n" + "=" * 80)
        print("❌ ERROR AL CARGAR DATOS EN BIGQUERY")
        print("=" * 80)
        print(f"Error: {e}")
        print(f"Tipo de error: {type(e).__name__}")
        
        # Intentar identificar el campo problemático
        error_msg = str(e)
        if "truncated converting to int64" in error_msg.lower():
            print("\n🔍 ANÁLISIS DEL ERROR DE TIPO DE DATO:")
            print("  → El error indica que hay un valor float siendo convertido a int64")
            
            # Buscar campos que podrían ser el problema
            print("\n🔎 CAMPOS CON VALORES FLOAT EN EL DATAFRAME:")
            for col in df.columns:
                if df[col].dtype in ['float64', 'float32', 'float']:
                    has_decimals = (df[col].dropna() % 1 != 0).any()
                    if has_decimals:
                        print(f"  ⚠️ {col}: {df[col].dtype}")
                        print(f"     → Contiene valores con decimales")
                        print(f"     → Valores únicos: {df[col].dropna().unique()[:10]}")
            
            # Comparar con el esquema de BigQuery
            try:
                table = bq_client.get_table(table_id)
                print("\n🔎 CAMPOS INTEGER EN BIGQUERY:")
                for field in table.schema:
                    if field.field_type in ['INTEGER', 'INT64']:
                        print(f"  • {field.name}: {field.field_type}")
                        if field.name in df.columns:
                            df_type = df[field.name].dtype
                            print(f"    → En DataFrame: {df_type}")
                            sample_vals = df[field.name].dropna().head(5).tolist()
                            print(f"    → Valores: {sample_vals}")
            except Exception as schema_error:
                print(f"  ⚠️ No se pudo comparar esquemas: {schema_error}")
        
        print("=" * 80 + "\n")
        raise

def format_receipt_response(extracted_data: dict, rows_inserted: int) -> str:
    """
    Formatea la respuesta para enviar al usuario por Telegram.
    Usa formato Markdown compatible con Telegram.
    """
    merchant = extracted_data.get("merchant_name", "Comercio desconocido")
    date = extracted_data.get("transaction_date", "Fecha desconocida")
    total = extracted_data.get("total_amount")
    items = extracted_data.get("line_items", [])
    
    # Formatear total
    if total:
        if isinstance(total, (int, float)):
            total_str = f"${total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        else:
            total_str = f"${total}"
    else:
        # Calcular total desde items si no está disponible
        calculated = sum(
            (item.get("item_total_price") or item.get("item_unit_price") or 0) 
            for item in items 
            if isinstance(item.get("item_total_price") or item.get("item_unit_price"), (int, float))
        )
        if calculated > 0:
            total_str = f"${calculated:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        else:
            total_str = "No detectado"
    
    # Construir respuesta sin indentación extra (importante para Telegram)
    lines = [
        "✅ *Ticket procesado exitosamente!*",
        "",
        f"🏪 *Comercio:* {merchant}",
        f"📅 *Fecha:* {date}",
        f"💰 *Total:* {total_str}",
        f"📦 *Items detectados:* {len(items)}",
        "",
        "*Productos extraídos:*"
    ]
    
    # Mostrar hasta 15 items para dar más contexto
    max_items_to_show = 15
    for i, item in enumerate(items[:max_items_to_show]):
        name = item.get("item_name", "?")
        # Truncar nombres muy largos
        if len(name) > 35:
            name = name[:32] + "..."
        qty = item.get("item_quantity", 1)
        price = item.get("item_total_price") or item.get("item_unit_price") or 0
        if isinstance(price, (int, float)) and price > 0:
            price_str = f"${price:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        else:
            price_str = "?"
        lines.append(f"• {name} x{qty} - {price_str}")
    
    if len(items) > max_items_to_show:
        lines.append(f"_... y {len(items) - max_items_to_show} productos más_")
    
    return "\n".join(lines)

def invoke_receipt_etl_step_function(s3_key: str, use_fallback: bool = True) -> dict:
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

def process_telegram_photo(message: dict, bq_client, chat_id: int) -> tuple:
    """
    Procesa una foto enviada por Telegram con soporte para tickets multi-foto.
    
    Flujo:
    1. Descarga la foto de Telegram y sube a S3
    2. Verifica si hay un ticket pendiente para este chat
    3. Analiza si el ticket está completo (busca TOTAL, CAE, etc.)
    4. Si está incompleto, guarda en DynamoDB y espera más fotos
    5. Si está completo, procesa todo y carga a BigQuery
    
    Retorna: (response_text, should_send_message)
    - should_send_message: False si estamos esperando más fotos
    """
    try:
        # Obtener el file_id de la foto (la de mayor resolución)
        photos = message.get("photo", [])
        if not photos:
            return "❌ No se encontró ninguna foto en el mensaje.", True
        
        # Telegram envía varias resoluciones, tomar la más grande
        photo = max(photos, key=lambda x: x.get("file_size", 0))
        file_id = photo.get("file_id")
        
        if not file_id:
            return "❌ No se pudo obtener el ID de la foto.", True
        
        print(f"📷 Procesando foto: {file_id}")
        
        # 1. Descargar foto de Telegram y subir a S3
        image_bytes = download_telegram_photo(file_id)
        s3_key = upload_image_to_s3(image_bytes)
        
        # 2. Verificar si hay un ticket pendiente para este chat
        pending_ticket = get_pending_ticket(chat_id)
        
        # 3. Analizar si esta foto completa el ticket
        is_complete, partial_data = check_ticket_is_complete(s3_key)
        partial_data["s3_key"] = s3_key
        
        if pending_ticket:
            # Ya hay un ticket en progreso, agregar esta foto
            existing_data = json.loads(pending_ticket.get("partial_data", "{}"))
            existing_s3_keys = pending_ticket.get("s3_keys", [])
            photo_count = int(pending_ticket.get("photo_count", 1))
            
            # Agregar nueva foto
            existing_s3_keys.append(s3_key)
            photo_count += 1
            
            # Agregar los items de esta foto a los existentes
            existing_items = existing_data.get("line_items", [])
            new_items = partial_data.get("line_items", [])
            existing_data["line_items"] = existing_items + new_items
            
            # Si encontramos el total en esta foto, usarlo
            if partial_data.get("total_amount"):
                existing_data["total_amount"] = partial_data["total_amount"]
            
            print(f"📎 Agregando foto {photo_count} al ticket en progreso")
            
            if is_complete:
                # ¡Ticket completo! Procesar todo
                print(f"✅ Ticket completo después de {photo_count} fotos")
                delete_pending_ticket(chat_id)
                
                # Fusionar todos los datos
                existing_data["extraction_method"] = "openai_vision_multi"
                existing_data["s3_keys"] = existing_s3_keys
                
                # Procesar el ticket completo
                return process_complete_ticket(existing_data, bq_client, photo_count)
            else:
                # Aún incompleto, guardar y esperar más fotos
                save_pending_ticket(chat_id, existing_data, existing_s3_keys, photo_count)
                return f"📄 Foto {photo_count} recibida.\n\n⏳ Esperando más fotos del ticket...\n_Envía la siguiente parte del ticket._", True
        else:
            # No hay ticket pendiente, es una foto nueva
            if is_complete:
                # Ticket completo en una sola foto - flujo normal
                print("✅ Ticket completo en una sola foto")
                return process_single_photo_ticket(s3_key, bq_client)
            else:
                # Ticket incompleto, guardar y esperar más fotos
                print("📄 Ticket incompleto, esperando más fotos...")
                save_pending_ticket(chat_id, partial_data, [s3_key], 1)
                return "📄 Primera parte del ticket recibida.\n\n⏳ Parece que el ticket continúa...\n_Envía la siguiente foto para completarlo._", True
        
    except Exception as e:
        print(f"❌ Error procesando foto: {e}")
        import traceback
        traceback.print_exc()
        return f"❌ Error procesando el ticket: {str(e)}", True

def process_single_photo_ticket(s3_key: str, bq_client) -> tuple:
    """Procesa un ticket de una sola foto (flujo original)"""
    try:
        # Usar Step Function o procesamiento directo
        if RECEIPT_ETL_STATE_MACHINE:
            sfn_result = invoke_receipt_etl_step_function(s3_key, use_fallback=True)
            response = format_step_function_response(sfn_result)
        else:
            # Procesamiento directo sin Step Function
            extracted_data = extract_receipt_data(s3_key, use_fallback=True)
            df = receipt_data_to_dataframe(extracted_data)
            rows_inserted = load_receipt_to_bigquery(df, bq_client)
            response = format_receipt_response(extracted_data, rows_inserted)
        
        return response, True
    except Exception as e:
        print(f"❌ Error en process_single_photo_ticket: {e}")
        return f"❌ Error procesando el ticket: {str(e)}", True

def process_complete_ticket(merged_data: dict, bq_client, photo_count: int) -> tuple:
    """Procesa un ticket completo (puede ser de múltiples fotos)"""
    try:
        print(f"🎯 Procesando ticket completo de {photo_count} fotos")
        print(f"📊 Total de items: {len(merged_data.get('line_items', []))}")
        
        # Convertir a DataFrame y cargar a BigQuery
        df = receipt_data_to_dataframe(merged_data)
        rows_inserted = load_receipt_to_bigquery(df, bq_client)
        
        # Formatear respuesta
        response = format_receipt_response(merged_data, rows_inserted)
        
        # Agregar nota sobre múltiples fotos si aplica
        if photo_count > 1:
            response = f"🔗 _Ticket reconstruido de {photo_count} fotos_\n\n" + response
        
        return response, True
        
    except Exception as e:
        print(f"❌ Error procesando ticket completo: {e}")
        import traceback
        traceback.print_exc()
        return f"❌ Error procesando el ticket: {str(e)}", True

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
7. Devuelve SOLO el SQL, sin explicaciones ni markdown"""

        user_prompt = f"""
ESQUEMA DETALLADO DE LAS TABLAS:
{enriched_schema}

{formatted_examples}

PREGUNTA DEL USUARIO: "{question}"

Genera la consulta SQL:"""

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

def query_bigquery(client, sql: str) -> tuple:
    """Ejecuta query en BigQuery. Retorna (texto, parse_mode) con parse_mode \"HTML\" si hay tabla formateada."""
    try:
        print(f"🔍 Ejecutando query en BigQuery:\n{sql}")
        
        # Primero validar con dry-run
        is_valid, validation_error = validate_sql_dry_run(client, sql)
        if not is_valid:
            return (
                f"❌ Error de sintaxis SQL:\n{validation_error}\n\nQuery:\n{sql}",
                False,
            )
        
        query_job = client.query(sql)
        results = query_job.result()  # Espera a que termine
        
        # Verificar si hay resultados
        if results.total_rows == 0:
            return ("ℹ️ No se encontraron resultados para tu consulta.", False)
        
        return (format_bigquery_results(results), "HTML")
        
    except Exception as e:
        error_msg = f"❌ Error en BigQuery:\n{str(e)}\n\nSQL ejecutado:\n{sql}"
        print(error_msg)  # Debug en CloudWatch
        return (error_msg, False)

def format_bigquery_results(results) -> str:
    """Formatea resultados de BigQuery para Telegram (HTML, legible en español)."""
    columns = [field.name for field in results.schema]
    rows = list(results)
    n = len(rows)
    if n == 0:
        return "ℹ️ No se encontraron filas."

    def row_block(row, index=None) -> str:
        parts = []
        if index is not None:
            parts.append(f"<b>{index}.</b>")
        for col_name in columns:
            label = _friendly_column_label(col_name)
            raw = row[col_name]
            if isinstance(raw, bool):
                disp = "Sí" if raw else "No"
            else:
                disp = _format_cell_for_chat(col_name, raw)
            parts.append(f"• <b>{html.escape(label)}:</b> {disp}")
        return "\n".join(parts)

    if n == 1:
        body = row_block(rows[0], None)
        title = "📌 <b>Resultado</b>"
        footer = f"<i>1 fila · columnas: {len(columns)}</i>"
    else:
        blocks = []
        for i, row in enumerate(rows, start=1):
            blocks.append(row_block(row, i))
        body = "\n\n".join(blocks)
        title = f"📊 <b>Resultados</b> <i>({n} filas)</i>"
        footer = f"<i>Mostrando {n} filas</i>"

    out = f"{title}\n\n{body}\n\n{footer}"
    max_len = 4000
    if len(out) > max_len:
        out = out[: max_len - 40] + "\n\n<i>… (mensaje truncado; acotá la consulta o pedí menos filas)</i>"
    return out

def handle_message(text: str, bq_client) -> tuple:
    """Maneja el mensaje del usuario. Retorna (sql, respuesta, parse_mode) — parse_mode False = texto plano."""
    question = text
    
    # Generar SQL con el nuevo sistema mejorado
    sql = generate_sql_with_openai2(question, bq_client)
    
    if not sql:
        return "", "❌ No se pudo generar la consulta SQL. Por favor, intenta con otra pregunta.", False
    
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
        return sql, f"❌ No pude generar una consulta válida. Error: {validation_error}", False
    
    response, parse_mode = query_bigquery(bq_client, sql)
    
    return sql, response, parse_mode

def retry_sql_generation(question: str, failed_sql: str, error: str, bq_client) -> str:
    """Intenta regenerar la SQL corrigiendo el error"""
    try:
        enriched_schema = build_enriched_schema_prompt()
        
        prompt = f"""La siguiente consulta SQL falló con un error.

PREGUNTA ORIGINAL: "{question}"

SQL QUE FALLÓ:
{failed_sql}

ERROR:
{error}

ESQUEMA DE TABLAS:
{enriched_schema}

Por favor, genera una nueva consulta SQL corrigiendo el error. 
Devuelve SOLO el SQL corregido, sin explicaciones."""

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

def bq_fqn(table_name: str) -> str:
    return f"`{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{table_name}`"

def sql_quick_ultimo_gasto() -> str:
    return f"""
SELECT COMERCIO, MONTO, FECHA_PAGO, TARJETA, DIVISA
FROM {bq_fqn("bank_payments")}
WHERE PARSE_DATE('%d/%m/%Y', FECHA_PAGO) IS NOT NULL
ORDER BY PARSE_DATE('%d/%m/%Y', FECHA_PAGO) DESC
LIMIT 1
"""

def sql_quick_ultimo_mp() -> str:
    return f"""
SELECT
  TIMESTAMP(TRANSACTION_DATE) AS fecha_transaccion,
  CAST(SETTLEMENT_NET_AMOUNT AS FLOAT64) AS monto,
  TRANSACTION_TYPE,
  PAYMENT_METHOD
FROM {bq_fqn("mp_data")}
WHERE TRANSACTION_DATE IS NOT NULL AND CAST(TRANSACTION_DATE AS STRING) != ''
ORDER BY TIMESTAMP(TRANSACTION_DATE) DESC
LIMIT 1
"""

def sql_quick_mes() -> str:
    return f"""
SELECT 'Banco (tarjeta)' AS fuente,
  ROUND(SUM(CAST(MONTO AS FLOAT64)), 2) AS total_ars,
  COUNT(*) AS movimientos
FROM {bq_fqn("bank_payments")}
WHERE PARSE_DATE('%d/%m/%Y', FECHA_PAGO) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
UNION ALL
SELECT 'Carrefour (detalle productos)' AS fuente,
  ROUND(SUM(monto_total), 2) AS total_ars,
  COUNT(*) AS movimientos
FROM {bq_fqn("carrefour_data")}
WHERE SAFE_CAST(fecha AS DATE) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
"""

def sql_quick_ultimos5_banco() -> str:
    return f"""
SELECT COMERCIO, MONTO, FECHA_PAGO
FROM {bq_fqn("bank_payments")}
WHERE PARSE_DATE('%d/%m/%Y', FECHA_PAGO) IS NOT NULL
ORDER BY PARSE_DATE('%d/%m/%Y', FECHA_PAGO) DESC
LIMIT 5
"""

def sql_quick_mes_carrefour() -> str:
    return f"""
SELECT
  COUNT(DISTINCT nro_ticket) AS tickets_distintos,
  ROUND(SUM(monto_total), 2) AS total_mes_ars
FROM {bq_fqn("carrefour_data")}
WHERE SAFE_CAST(fecha AS DATE) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
"""

QUICK_SQL = {
    "ultimo_gasto": sql_quick_ultimo_gasto,
    "ultimo_mp": sql_quick_ultimo_mp,
    "mes": sql_quick_mes,
    "ultimos5_banco": sql_quick_ultimos5_banco,
    "mes_carrefour": sql_quick_mes_carrefour,
}

def quick_actions_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "Último gasto (banco)", "callback_data": "q:ultimo_gasto"},
                {"text": "Último MP", "callback_data": "q:ultimo_mp"},
            ],
            [
                {"text": "Mes actual (resumen)", "callback_data": "q:mes"},
                {"text": "Últimos 5 · banco", "callback_data": "q:ultimos5_banco"},
            ],
            [{"text": "Mes actual · Carrefour", "callback_data": "q:mes_carrefour"}],
        ]
    }

def run_quick_sql_key(key: str, bq_client) -> tuple:
    builder = QUICK_SQL.get(key)
    if not builder:
        return "", f"❌ Acción rápida desconocida: {key}", False
    sql = builder().strip()
    return (sql,) + query_bigquery(bq_client, sql)

def telegram_answer_callback(callback_query_id: str, text: str = None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text[:200]
    try:
        requests.post(url, json=payload, timeout=10).raise_for_status()
    except Exception as e:
        print(f"answerCallbackQuery: {e}")

def send_telegram_message(chat_id, text, token, parse_mode="Markdown", reply_markup=None):
    """Envía mensaje a Telegram. parse_mode=False omite formato (texto plano). HTML/Markdown según valor."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode is not False and parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error enviando mensaje a Telegram: {e}")
        if parse_mode is not False and parse_mode:
            print("🔄 Reintentando sin parse_mode...")
            payload.pop("parse_mode", None)
            try:
                response = requests.post(url, json=payload, timeout=10)
                response.raise_for_status()
                return response.json()
            except Exception as e2:
                print(f"Error en reintento: {e2}")
        return None

def send_telegram_document(chat_id, file_bytes: bytes, filename: str, caption: str, token: str):
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    try:
        files = {"document": (filename, file_bytes, "text/csv")}
        data = {"chat_id": str(chat_id)}
        if caption:
            data["caption"] = caption[:1024]
        r = requests.post(url, data=data, files=files, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error sendDocument: {e}")
        return None

def query_bigquery_to_csv_bytes(bq_client, sql: str) -> tuple:
    """Ejecuta SQL y devuelve (bytes_csv, num_filas) o (None, 0) si error."""
    try:
        is_valid, err = validate_sql_dry_run(bq_client, sql)
        if not is_valid:
            return None, 0
        job = bq_client.query(sql)
        it = job.result()
        rows = list(it)
        if not rows:
            return b"", 0
        cols = [k for k in rows[0].keys()]
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(cols)
        for row in rows:
            w.writerow([row[c] for c in cols])
        return buf.getvalue().encode("utf-8"), len(rows)
    except Exception as e:
        print(f"query_bigquery_to_csv_bytes: {e}")
        return None, 0

def export_table_csv(chat_id: str, table_key: str, bq_client, token: str) -> bool:
    """table_key: bank | carrefour | mp"""
    table_key = (table_key or "bank").lower().strip()
    tables = {"bank": "bank_payments", "carrefour": "carrefour_data", "mp": "mp_data"}
    tname = tables.get(table_key, "bank_payments")
    sql = f"SELECT * FROM {bq_fqn(tname)} LIMIT {EXPORT_MAX_ROWS}"
    data, n = query_bigquery_to_csv_bytes(bq_client, sql)
    if data is None:
        send_telegram_message(chat_id, "❌ No se pudo exportar (error en la consulta).", token, parse_mode=False)
        return False
    if n == 0:
        send_telegram_message(chat_id, "ℹ️ No hay filas para exportar.", token, parse_mode=False)
        return False
    fname = f"{tname}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    try:
        s3 = boto3.client("s3")
        key = f"{S3_PREFIX_EXPORTS.rstrip('/')}/{chat_id}/{fname}"
        s3.put_object(Bucket=S3_BUCKET_TICKETS, Key=key, Body=data, ContentType="text/csv")
    except Exception as e:
        print(f"S3 put export (opcional): {e}")
    cap = f"Export {tname}: {n} filas (máx. {EXPORT_MAX_ROWS})"
    send_telegram_document(chat_id, data, fname, cap, token)
    return True

def handle_telegram_callback(data: dict, bq_client) -> dict:
    cq = data.get("callback_query") or {}
    cq_id = cq.get("id")
    chat_id = cq.get("message", {}).get("chat", {}).get("id")
    raw = (cq.get("data") or "").strip()
    if not chat_id or not cq_id:
        return {"statusCode": 200}
    telegram_answer_callback(cq_id, "Listo")
    if raw.startswith("q:"):
        key = raw[2:]
        sql_txt, response_text, result_parse_mode = run_quick_sql_key(key, bq_client)
        print(f"Callback quick SQL {key}: {sql_txt[:120] if sql_txt else ''}...")
        pm = result_parse_mode if result_parse_mode in ("HTML", False) else "Markdown"
        send_telegram_message(
            chat_id,
            response_text,
            TELEGRAM_BOT_TOKEN,
            parse_mode=pm,
            reply_markup=quick_actions_keyboard(),
        )
    return {"statusCode": 200}

def run_monthly_budget_alert(event, context) -> dict:
    """Invocación programada (EventBridge) o manual: avisa si el gasto del mes en banco supera el tope."""
    if not TELEGRAM_ALERT_CHAT_ID or not ALERT_BUDGET_ARS:
        print("Alertas presupuesto: TELEGRAM_ALERT_CHAT_ID o ALERT_BUDGET_ARS no configurados.")
        return {"statusCode": 200}
    try:
        budget = float(ALERT_BUDGET_ARS.replace(",", "."))
    except ValueError:
        print("ALERT_BUDGET_ARS inválido")
        return {"statusCode": 200}
    try:
        bq_client = get_bigquery_client()
        sql = f"""
        SELECT ROUND(SUM(CAST(MONTO AS FLOAT64)), 2) AS total_ars
        FROM {bq_fqn("bank_payments")}
        WHERE PARSE_DATE('%d/%m/%Y', FECHA_PAGO) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
        """
        job = bq_client.query(sql)
        rows = list(job.result())
        total = float(rows[0]["total_ars"] or 0) if rows else 0.0
    except Exception as e:
        print(f"Alerta presupuesto query: {e}")
        return {"statusCode": 200}
    chat_id = int(TELEGRAM_ALERT_CHAT_ID)
    if total > budget:
        msg = (
            f"⚠️ *Alerta de presupuesto*\n\n"
            f"Gasto acumulado del mes (banco/tarjeta): *{total:.2f}* ARS\n"
            f"Tope configurado: *{budget:.2f}* ARS\n\n"
            f"Superaste el límite configurado."
        )
        send_telegram_message(chat_id, msg, TELEGRAM_BOT_TOKEN, parse_mode="Markdown")
    else:
        print(f"Presupuesto OK: gasto mes {total} <= {budget}")
    return {"statusCode": 200}


def run_daily_unmapped_alert(event, context) -> dict:
    """
    Notificación diaria de comercios no mapeados.
    Invocada por EventBridge con action="alert_unmapped" o source="aws.events".
    Envía al chat configurado los top N comercios pendientes de mapear.
    """
    if not TELEGRAM_ALERT_CHAT_ID:
        print("Alerta unmapped: TELEGRAM_ALERT_CHAT_ID no configurado.")
        return {"statusCode": 200}
    
    try:
        bq_client = get_bigquery_client()
        chat_id = int(TELEGRAM_ALERT_CHAT_ID)
        
        # Consultar comercios no mapeados agrupados por flow
        sql = f"""
        SELECT 
            flow,
            comercio_raw,
            COUNT(*) AS apariciones,
            MAX(ins_dttm) AS last_seen
        FROM {bq_fqn(UNMAPPED_TABLE)}
        WHERE IFNULL(resolved, FALSE) = FALSE
          AND IFNULL(comercio_normalizado, '') != ''
        GROUP BY flow, comercio_raw
        ORDER BY apariciones DESC, last_seen DESC
        LIMIT 10
        """
        
        job = bq_client.query(sql)
        rows = list(job.result())
        
        if not rows:
            print("No hay comercios pendientes de mapear.")
            return {"statusCode": 200}
        
        # Contar totales por flow
        count_sql = f"""
        SELECT 
            flow,
            COUNT(DISTINCT comercio_raw) AS total_pendientes
        FROM {bq_fqn(UNMAPPED_TABLE)}
        WHERE IFNULL(resolved, FALSE) = FALSE
          AND IFNULL(comercio_normalizado, '') != ''
        GROUP BY flow
        """
        count_job = bq_client.query(count_sql)
        count_rows = list(count_job.result())
        totals_by_flow = {r["flow"]: int(r["total_pendientes"]) for r in count_rows}
        total_pendientes = sum(totals_by_flow.values())
        
        # Construir mensaje
        lines = [
            "🧩 <b>Comercios pendientes de mapear</b>",
            f"<i>Total: {total_pendientes} comercios sin clasificar</i>",
            ""
        ]
        
        # Mostrar totales por flow
        for flow, count in totals_by_flow.items():
            flow_label = "Banco" if flow == "bank_payments" else "Mercado Pago" if flow == "mp_data" else flow
            lines.append(f"• <b>{flow_label}:</b> {count} pendientes")
        
        lines.append("")
        lines.append("<b>Top 10 más frecuentes:</b>")
        
        for i, r in enumerate(rows, start=1):
            flow_short = "🏦" if r["flow"] == "bank_payments" else "💳" if r["flow"] == "mp_data" else "❓"
            comercio = html.escape(str(r["comercio_raw"]))
            apariciones = int(r["apariciones"])
            lines.append(f"{i}. {flow_short} <code>{comercio}</code> (x{apariciones})")
        
        lines.append("")
        lines.append("<i>Usá /pendientes_comercio para ver más</i>")
        lines.append("<i>Usá /mapear_comercio flow|match|depurado|cat|subcat</i>")
        
        msg = "\n".join(lines)
        send_telegram_message(chat_id, msg, TELEGRAM_BOT_TOKEN, parse_mode="HTML")
        
        print(f"Alerta unmapped enviada: {total_pendientes} comercios pendientes")
        return {"statusCode": 200}
        
    except Exception as e:
        print(f"Error en alerta unmapped: {e}")
        import traceback
        traceback.print_exc()
        return {"statusCode": 200}

def list_unmapped_comercios(bq_client, flow: str = "all", limit: int = 20) -> str:
    where_flow = ""
    if flow and flow.lower() != "all":
        where_flow = "AND flow = @flow"
    q = f"""
    SELECT flow, comercio_raw, COUNT(*) AS apariciones, MAX(ins_dttm) AS last_seen
    FROM {bq_fqn(UNMAPPED_TABLE)}
    WHERE IFNULL(resolved, FALSE) = FALSE
      AND IFNULL(comercio_normalizado, '') != ''
      {where_flow}
    GROUP BY flow, comercio_raw
    ORDER BY apariciones DESC, last_seen DESC
    LIMIT @lim
    """
    params = [bigquery.ScalarQueryParameter("lim", "INT64", limit)]
    if flow and flow.lower() != "all":
        params.append(bigquery.ScalarQueryParameter("flow", "STRING", flow))
    cfg = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(bq_client.query(q, job_config=cfg).result())
    if not rows:
        return "✅ No hay comercios pendientes de mapear."
    lines = ["🧩 <b>Comercios pendientes</b>"]
    for i, r in enumerate(rows, start=1):
        lines.append(
            f"{i}. <b>{html.escape(r['flow'])}</b> · {html.escape(str(r['comercio_raw']))} "
            f"(x{int(r['apariciones'])})"
        )
    lines.append(
        "\n<i>Usá:</i> /mapear_comercio flow|match_value|comercio_depurado|categoria|subcategoria"
    )
    return "\n".join(lines)

def apply_mapping_backfill(bq_client, flow: str, match_value: str, comercio_depurado: str, categoria: str, subcategoria: str, match_type: str = "contains"):
    """
    Guarda un mapeo en dim_comercio_mapping y marca los pendientes como resueltos.
    
    NOTA: Las tablas de hechos (bank_payments, mp_data, etc.) NO tienen columnas
    de categoria/subcategoria. El mapeo se aplica:
    - Durante el ETL (load_data) para nuevos registros
    - En tiempo de consulta con JOIN a dim_comercio_mapping
    
    Args:
        flow: 'bank_payments', 'mp_data', 'mp_transfer_data', 'supermarket_receipts'
        match_value: texto a matchear (se normaliza automáticamente)
        comercio_depurado: nombre limpio del comercio
        categoria: categoría principal
        subcategoria: subcategoría
        match_type: 'contains' (default), 'exact', 'regex', 'fuzzy', 'fuzzy:85'
    """
    flow = flow.strip()
    match_norm = re.sub(r"[^A-Z0-9]+", "", match_value.upper())
    if not match_norm:
        raise ValueError("match_value vacío o inválido.")
    
    # Validar match_type
    valid_types = ["contains", "exact", "regex"]
    is_fuzzy = match_type.startswith("fuzzy")
    if not is_fuzzy and match_type not in valid_types:
        match_type = "contains"

    # 1. Guardar/actualizar regla en dim_comercio_mapping
    merge_q = f"""
    MERGE {bq_fqn(MAPPING_TABLE)} t
    USING (
      SELECT
        @flow AS flow,
        @match_type AS match_type,
        @match_norm AS match_value,
        @dep AS comercio_depurado,
        @cat AS categoria,
        @sub AS subcategoria,
        100 AS prioridad,
        TRUE AS activo
    ) s
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
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("flow", "STRING", flow),
            bigquery.ScalarQueryParameter("match_type", "STRING", match_type),
            bigquery.ScalarQueryParameter("match_norm", "STRING", match_norm),
            bigquery.ScalarQueryParameter("dep", "STRING", comercio_depurado),
            bigquery.ScalarQueryParameter("cat", "STRING", categoria),
            bigquery.ScalarQueryParameter("sub", "STRING", subcategoria),
        ]
    )
    bq_client.query(merge_q, job_config=cfg).result()

    # 2. Intentar marcar pendientes como resueltos en comercio_unmapped_queue
    # Nota: puede fallar si hay filas en streaming buffer (insertadas hace <30 min)
    # En ese caso, simplemente continuamos - el mapeo ya quedó guardado
    try:
        resolve_q = f"""
        UPDATE {bq_fqn(UNMAPPED_TABLE)}
        SET resolved = TRUE
        WHERE flow = @flow
          AND REGEXP_REPLACE(UPPER(IFNULL(comercio_normalizado, '')), r'[^A-Z0-9]+', '') LIKE CONCAT('%', @match_norm, '%')
          AND IFNULL(resolved, FALSE) = FALSE
        """
        bq_client.query(resolve_q, job_config=cfg).result()
    except Exception as e:
        # Si falla por streaming buffer, no es crítico - el mapeo ya está guardado
        print(f"⚠️ No se pudo marcar como resuelto en unmapped_queue (streaming buffer?): {e}")

def lambda_handler(event, context):
    try:
        print("📥 Evento recibido por Lambda")
        
        # Manejar invocaciones programadas de EventBridge
        if isinstance(event, dict):
            action = event.get("action", "")
            source = event.get("source", "")
            
            # Alerta de presupuesto mensual
            if action == "alert_budget" or (source == "aws.events" and event.get("detail-type") == "budget_alert"):
                return run_monthly_budget_alert(event, context)
            
            # Alerta diaria de comercios no mapeados
            if action == "alert_unmapped" or (source == "aws.events" and event.get("detail-type") == "unmapped_alert"):
                return run_daily_unmapped_alert(event, context)
            
            # Compatibilidad: si viene de EventBridge sin detail-type, ejecutar ambas alertas
            if source == "aws.events" and not event.get("detail-type"):
                run_monthly_budget_alert(event, context)
                return run_daily_unmapped_alert(event, context)
        
        body_raw = event.get("body") if isinstance(event, dict) else None
        if not body_raw:
            return {"statusCode": 200}
        data = json.loads(body_raw) if isinstance(body_raw, str) else body_raw
        
        bq_client = get_bigquery_client()
        
        if "callback_query" in data:
            return handle_telegram_callback(data, bq_client)
        
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
            
            # Control de idempotencia ATÓMICO: intentar adquirir lock
            telegram_message_id = message.get("message_id")
            if telegram_message_id and not try_acquire_message_lock(telegram_message_id):
                print(f"⚠️ Mensaje {telegram_message_id} ya fue procesado o está siendo procesado, ignorando")
                return {"statusCode": 200}
            
            # Verificar si hay un ticket pendiente (para no enviar "Recibí tu ticket" en cada foto)
            pending_ticket = get_pending_ticket(chat_id)
            
            if not pending_ticket:
                print("""
                *Recibí tu ticket!*
                🔄 Procesando imagen...
                Esto puede tomar unos segundos.
                """)
                # Primera foto - enviar mensaje de "procesando"
                send_telegram_message(
                    chat_id, 
                    "📷 *Recibí tu ticket!*\n\n🔄 Procesando imagen...\nEsto puede tomar unos segundos.", 
                    TELEGRAM_BOT_TOKEN
                )
            
            # Procesar la foto (ahora retorna tupla: response_text, should_send)
            response_text, should_send = process_telegram_photo(message, bq_client, chat_id)
            print('response_text: ' , response_text)
            
            if should_send:
                send_telegram_message(chat_id, response_text, TELEGRAM_BOT_TOKEN)
            
            return {"statusCode": 200}
        
        # Si no es foto, debe ser texto
        text = message.get("text", "")
        
        if not text:
            print("""No entendí tu mensaje. 
                Podés:
                • Enviarme una *pregunta* sobre tus gastos
                • Enviarme una *foto de un ticket* para procesarlo
            """)
            send_telegram_message(
                chat_id, 
                """No entendí tu mensaje. 
                Podés:
                • Enviarme una *pregunta* sobre tus gastos
                • Enviarme una *foto de un ticket* para procesarlo
                """, 
                TELEGRAM_BOT_TOKEN
            )
            return {"statusCode": 200}

        print(f'💬 Mensaje input: {text}')
        
        # Control de idempotencia ATÓMICO para mensajes de texto
        telegram_message_id = message.get("message_id")
        if telegram_message_id and not try_acquire_message_lock(telegram_message_id):
            print(f"⚠️ Mensaje de texto {telegram_message_id} ya fue procesado o está siendo procesado, ignorando")
            return {"statusCode": 200}

        # =========================================
        # MANEJAR COMANDOS
        # =========================================
        
        if text == "/start":
            welcome_message = """ 
                🤖*Bot de Consultas de Datos con IA*
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

                ⚡ *Atajos:* /ultimo_gasto · /ultimo_mp · /mes · /exportar banco

                ¡Escribí tu pregunta o enviame una foto de un ticket!
            """
            send_telegram_message(
                chat_id,
                welcome_message,
                TELEGRAM_BOT_TOKEN,
                reply_markup=quick_actions_keyboard(),
            )
            return {"statusCode": 200}
        
        if text == "/help":
            help_message = """📚 *Ayuda del Bot*

*Consultas de texto:*
Escribí cualquier pregunta sobre tus gastos en lenguaje natural.

*Ejemplos:*
• ¿Cuánto gasté este mes?
• Gastos por comercio
• Mis mayores gastos
• ¿Cuánto gasté en Carrefour?

*Fotos de tickets:*
Enviame una foto clara de un ticket de supermercado y lo proceso automáticamente.

*Comandos básicos:*
• /start - Mensaje de bienvenida
• /help - Esta ayuda
• /ultimo\_gasto - Último movimiento banco/tarjeta
• /ultimo\_mp - Última transacción Mercado Pago
• /mes - Resumen mes actual (banco + Carrefour)
• /exportar [banco|carrefour|mp] - Exportar CSV

*Mapeo de comercios:*
• /pendientes\_comercio [bank|mp|all] - Ver comercios sin clasificar
• /mapear\_comercio flow|match|depurado|cat|subcat|[tipo]

*Tipos de matching:*
• `contains` (default): busca substring
• `exact`: coincidencia exacta
• `fuzzy`: similitud >= 80%
• `fuzzy:85`: similitud personalizada

*Ejemplo fuzzy:*
`/mapear_comercio bank|SHELL|Shell|nafta|combustible|fuzzy:75`
Matchea "MERPAGO*SHELL PALERMO", "SHELL YPF", etc.
"""
            send_telegram_message(chat_id, help_message, TELEGRAM_BOT_TOKEN)
            return {"statusCode": 200}

        quick_cmd = {
            "/ultimo_gasto": "ultimo_gasto",
            "/ultimo_mp": "ultimo_mp",
            "/mes": "mes",
        }
        if text in quick_cmd:
            _, response_text, result_parse_mode = run_quick_sql_key(quick_cmd[text], bq_client)
            pm = result_parse_mode if result_parse_mode in ("HTML", False) else "Markdown"
            send_telegram_message(
                chat_id,
                response_text,
                TELEGRAM_BOT_TOKEN,
                parse_mode=pm,
                reply_markup=quick_actions_keyboard(),
            )
            return {"statusCode": 200}
        
        if text.startswith("/exportar"):
            parts = text.split(maxsplit=1)
            raw = parts[1].strip().lower() if len(parts) > 1 else "banco"
            alias = {
                "banco": "bank",
                "bank": "bank",
                "carrefour": "carrefour",
                "mp": "mp",
                "mercadopago": "mp",
            }
            which = alias.get(raw, "bank")
            export_table_csv(chat_id, which, bq_client, TELEGRAM_BOT_TOKEN)
            return {"statusCode": 200}

        if text.startswith("/pendientes_comercio"):
            parts = text.split(maxsplit=1)
            raw = parts[1].strip().lower() if len(parts) > 1 else "all"
            flow_map = {
                "all": "all",
                "bank": "bank_payments",
                "bank_payments": "bank_payments",
                "mp": "mp_data",
                "mp_data": "mp_data",
            }
            flow = flow_map.get(raw, "all")
            msg = list_unmapped_comercios(bq_client, flow=flow, limit=20)
            send_telegram_message(chat_id, msg, TELEGRAM_BOT_TOKEN, parse_mode="HTML")
            return {"statusCode": 200}

        if text.startswith("/mapear_comercio"):
            try:
                payload = text[len("/mapear_comercio"):].strip()
                parts = [p.strip() for p in payload.split("|")]
                if len(parts) < 5:
                    raise ValueError("Formato inválido (mínimo 5 campos)")
                
                flow_raw, match_value, depurado, categoria, subcategoria = parts[:5]
                # Sexto parámetro opcional: match_type (default: contains)
                match_type = parts[5].lower() if len(parts) > 5 else "contains"
                
                flow_alias = {
                    "bank": "bank_payments",
                    "bank_payments": "bank_payments",
                    "mp": "mp_data",
                    "mp_data": "mp_data",
                }
                flow = flow_alias.get(flow_raw.lower())
                if not flow:
                    raise ValueError("flow debe ser bank/bank_payments/mp/mp_data")
                
                apply_mapping_backfill(
                    bq_client,
                    flow=flow,
                    match_value=match_value,
                    comercio_depurado=depurado,
                    categoria=categoria,
                    subcategoria=subcategoria,
                    match_type=match_type,
                )
                
                # Mensaje de confirmación
                ok = (
                    f"✅ Mapeo guardado en dim_comercio_mapping.\n\n"
                    f"📋 Detalles:\n"
                    f"• flow: {flow}\n"
                    f"• match: {match_value}\n"
                    f"• match_type: {match_type}\n"
                    f"• depurado: {depurado}\n"
                    f"• categoria: {categoria}\n"
                    f"• subcategoria: {subcategoria}\n\n"
                    f"ℹ️ El mapeo se aplicará a nuevos registros en el próximo ETL.\n"
                    f"Para consultas históricas, usá JOIN con dim_comercio_mapping."
                )
                send_telegram_message(chat_id, ok, TELEGRAM_BOT_TOKEN, parse_mode=False)
            except Exception as e:
                err = (
                    "❌ No pude registrar el mapeo.\n\n"
                    "Formato:\n"
                    "/mapear_comercio flow|match|depurado|categoria|subcategoria|[tipo]\n\n"
                    "Tipos de matching:\n"
                    "• contains (default): busca substring\n"
                    "• exact: coincidencia exacta\n"
                    "• fuzzy: similitud >= 80%\n"
                    "• fuzzy:85: similitud >= 85%\n\n"
                    "Ejemplos:\n"
                    "/mapear_comercio bank|MERPAGO*SHELL|Shell|nafta|combustible\n"
                    "/mapear_comercio bank|SHELL|Shell|nafta|combustible|fuzzy:75\n\n"
                    f"Detalle: {str(e)}"
                )
                send_telegram_message(chat_id, err, TELEGRAM_BOT_TOKEN, parse_mode=False)
            return {"statusCode": 200}

        # =========================================
        # PROCESAR PREGUNTA DE TEXTO
        # =========================================
        
        sql, response_text, result_parse_mode = handle_message(text, bq_client)
        print('response_text: ', response_text)
        
        # Enviar respuesta (HTML para tablas BigQuery; texto plano para errores)
        pm = result_parse_mode if result_parse_mode in ("HTML", False) else "Markdown"
        markup = quick_actions_keyboard() if result_parse_mode == "HTML" else None
        result = send_telegram_message(
            chat_id,
            response_text,
            TELEGRAM_BOT_TOKEN,
            parse_mode=pm,
            reply_markup=markup,
        )

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
                print('Ocurrió un error al procesar tu mensaje. Por favor, intentá de nuevo.')
                send_telegram_message(
                    chat_id, 
                    "Ocurrió un error al procesar tu mensaje. Por favor, intentá de nuevo.", 
                    TELEGRAM_BOT_TOKEN
                )
        except Exception as nested_e:
            print(f"[ERROR] No se pudo enviar mensaje de error: {str(nested_e)}")

        return {"statusCode": 200}
