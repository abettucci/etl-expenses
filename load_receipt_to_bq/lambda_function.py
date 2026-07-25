"""
Lambda para cargar datos de tickets en BigQuery.
Recibe datos extraídos del OCR y los carga en la tabla de supermercado.
"""

import os
import json
import boto3
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET_PROD = os.environ.get("BQ_DATASET_PROD", "PRD")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
BQ_TABLE_SUPERMARKET = os.environ.get("BQ_TABLE_SUPERMARKET", "supermarket_receipts")
REGION = os.environ.get("AWS_REGION", "us-east-2")

def get_bigquery_client():
    """Inicializa cliente de BigQuery con credenciales de Secrets Manager"""
    secrets_client = boto3.client('secretsmanager', region_name=REGION)
    secret_response = secrets_client.get_secret_value(SecretId='gcp_sa_api_credentials')
    credentials_json = json.loads(secret_response['SecretString'])
    
    credentials = service_account.Credentials.from_service_account_info(credentials_json)
    client = bigquery.Client(
        credentials=credentials,
        project=GCP_PROJECT_ID,
        location=BQ_LOCATION
    )
    return client


def extracted_data_to_rows(extracted_data: dict, s3_key: str) -> list:
    """
    Convierte los datos extraídos del ticket a filas para BigQuery.
    Cada item del ticket es una fila.
    """
    rows = []
    
    # Datos comunes del ticket
    common_data = {
        "merchant_name": extracted_data.get("merchant_name"),
        "merchant_address": extracted_data.get("merchant_address"),
        "transaction_date": extracted_data.get("transaction_date"),
        "transaction_time": extracted_data.get("transaction_time"),
        "total_amount": float(extracted_data.get("total_amount") or 0),
        "currency": extracted_data.get("currency", "ARS"),
        "payment_method": extracted_data.get("payment_method"),
        "extraction_method": extracted_data.get("extraction_method"),
        "s3_key": s3_key,
        "processed_at": datetime.now().isoformat()
    }
    
    # Crear una fila por cada item
    line_items = extracted_data.get("line_items", [])
    
    if line_items:
        for item in line_items:
            row = common_data.copy()
            row["item_name"] = item.get("item_name")
            row["item_quantity"] = int(item.get("item_quantity") or 1)
            row["item_unit_price"] = float(item.get("item_unit_price") or 0)
            row["item_total_price"] = float(item.get("item_total_price") or 0)
            rows.append(row)
    else:
        # Si no hay items, crear una fila con los datos generales
        common_data["item_name"] = None
        common_data["item_quantity"] = None
        common_data["item_unit_price"] = None
        common_data["item_total_price"] = None
        rows.append(common_data)
    
    print(f"📊 Filas creadas: {len(rows)}")
    return rows


def ensure_table_exists(client, table_id: str):
    """
    Verifica que la tabla existe, si no la crea con el esquema básico.
    """
    try:
        client.get_table(table_id)
        print(f"✅ Tabla {table_id} existe")
    except Exception:
        print(f"📝 Creando tabla {table_id}...")
        
        schema = [
            bigquery.SchemaField("merchant_name", "STRING"),
            bigquery.SchemaField("merchant_address", "STRING"),
            bigquery.SchemaField("transaction_date", "STRING"),
            bigquery.SchemaField("transaction_time", "STRING"),
            bigquery.SchemaField("total_amount", "FLOAT64"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("payment_method", "STRING"),
            bigquery.SchemaField("item_name", "STRING"),
            bigquery.SchemaField("item_quantity", "INT64"),
            bigquery.SchemaField("item_unit_price", "FLOAT64"),
            bigquery.SchemaField("item_total_price", "FLOAT64"),
            bigquery.SchemaField("extraction_method", "STRING"),
            bigquery.SchemaField("s3_key", "STRING"),
            bigquery.SchemaField("processed_at", "STRING"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"✅ Tabla {table_id} creada")


def load_rows_to_bigquery(client, rows: list) -> int:
    """
    Carga las filas en BigQuery usando streaming insert.
    Retorna el número de filas insertadas.
    """
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{BQ_TABLE_SUPERMARKET}"
    
    # Asegurar que la tabla existe
    ensure_table_exists(client, table_id)
    
    # Insertar filas
    errors = client.insert_rows_json(table_id, rows)
    
    if errors:
        print(f"❌ Errores insertando filas: {errors}")
        raise Exception(f"Error en BigQuery: {errors}")
    
    print(f"✅ {len(rows)} filas insertadas en {table_id}")
    return len(rows)


def lambda_handler(event, context):
    """
    Handler para la Lambda de carga a BigQuery.
    
    Input (desde Step Function):
        {
            "extracted_data": {
                "merchant_name": "CARREFOUR",
                "transaction_date": "2024-11-30",
                "total_amount": 15420.50,
                "line_items": [...],
                ...
            },
            "s3_key": "receipts/20241130_123456_abc123.jpg"
        }
    
    Output:
        {
            "rows_inserted": 15,
            "table": "supermarket_receipts",
            "success": true
        }
    """
    try:
        print(f"📥 Evento recibido: {json.dumps(event)}")
        
        extracted_data = event.get("extracted_data")
        s3_key = event.get("s3_key")
        
        if not extracted_data:
            raise ValueError("extracted_data es requerido")
        
        if not s3_key:
            raise ValueError("s3_key es requerido")
        
        # Inicializar cliente de BigQuery
        bq_client = get_bigquery_client()
        
        # Convertir datos a filas
        rows = extracted_data_to_rows(extracted_data, s3_key)
        
        # Cargar en BigQuery
        rows_inserted = load_rows_to_bigquery(bq_client, rows)
        
        return {
            "rows_inserted": rows_inserted,
            "table": BQ_TABLE_SUPERMARKET,
            "success": True
        }
        
    except Exception as e:
        print(f"❌ Error en Lambda: {e}")
        import traceback
        traceback.print_exc()
        raise    