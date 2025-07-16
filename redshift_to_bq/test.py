import boto3
import json
import pandas as pd
import hashlib
from bs4 import BeautifulSoup
import io
from datetime import datetime, timedelta
import base64
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Funcion para obtener la API Key de Google Cloud y consumir la API de Gmail
def get_secret(SECRET_NAME, REGION_NAME):
    client = boto3.client('secretsmanager', region_name=REGION_NAME)
    response = client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(response['SecretString'])

def update_secret(updated_token_json, SECRET_NAME, REGION_NAME):
    client = boto3.client('secretsmanager', region_name=REGION_NAME)
    client.update_secret(
        SecretId=SECRET_NAME,
        SecretString=updated_token_json
    )

def auth_google(SECRET_NAME):
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    REGION_NAME = 'us-east-2'    
    token_info = get_secret(SECRET_NAME, REGION_NAME)
    creds = Credentials.from_authorized_user_info(token_info, SCOPES)
    
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        print("♻️ Token refrescado")

        # Guardar el token actualizado en Secrets Manager
        update_secret(creds.to_json(), SECRET_NAME, REGION_NAME)

    return creds

def find_html_part(payload):
    if payload.get("mimeType") == "text/html":
        return payload["body"].get("data", None)
    elif "parts" in payload:
        for part in payload["parts"]:
            result = find_html_part(part)
            if result:
                return result
    return None

# Funcion para extraer los PDFs especificos de Gmail
def extract_bank_payments_from_gmail(redshift_data):
    creds = auth_google('gcp_api_credentials_2')
    gmail_service = build('gmail', 'v1', credentials=creds)
    s3_client = boto3.client('s3')
    bucket_name = 'bank-payments'
    folder = 'raw/'

    # Query para crear la tabla de pagos del banco en Redshift
    crear_tabla_pagos_query = """
        CREATE TABLE bank_payments (
            id           VARCHAR(32) PRIMARY KEY,
            message_id   VARCHAR(255),
            fecha_pago   DATE,
            hora_pago    TIME,
            monto        DECIMAL(12,2),
            divisa       VARCHAR(5),
            tarjeta      VARCHAR(50),
            nro_tarjeta  VARCHAR(10),
            comercio     VARCHAR(100),
            cuotas       INT,
            extraido_en  TIMESTAMP
        );
    """

    # Ejecutar consulta
    response = redshift_data.execute_statement(
        Database='dev',
        WorkgroupName='pdf-etl-workgroup',
        Sql=crear_tabla_pagos_query
    )

    # Obtenemos la ultima fecha de la tabla de tickets ya ingestados de Redshift        
    date_query = """
        SELECT MAX(
            TO_DATE(
                CASE 
                    WHEN LENGTH(SPLIT_PART(fecha_pago, '/', 3)) = 2 THEN 
                        -- convertimos a formato DD/MM/20YY
                        SPLIT_PART(fecha_pago, '/', 1) || '/' || 
                        SPLIT_PART(fecha_pago, '/', 2) || '/' || 
                        '20' || SPLIT_PART(fecha_pago, '/', 3)
                    ELSE fecha_pago -- Asumir que ya está en formato DD/MM/YYYY
                END,
                'DD/MM/YYYY'
            )
        ) AS max_date 
        FROM bank_payments
    """
    
    # Ejecutar consulta
    response = redshift_data.execute_statement(
        Database='dev',
        WorkgroupName='pdf-etl-workgroup',
        Sql=date_query
    )

     # Esperar resultados (puede tomar algunos segundos)
    
    fecha_ultimo_payment_cargado = None
    while True:
        desc = redshift_data.describe_statement(Id=response['Id'])
        if desc['Status'] == 'FINISHED':
            if desc['HasResultSet']:
                result = redshift_data.get_statement_result(Id=response['Id'])
                try:
                    fecha_ultimo_payment_cargado = result['Records'][0][0]['stringValue']
                    if len(fecha_ultimo_payment_cargado.split('/')[-1]) == 2:
                        day, month, year = fecha_ultimo_payment_cargado.split('/')
                        fecha_ultimo_payment_cargado = f"{day}/{month}/20{year}"
                    fecha_ultimo_payment_cargado = datetime.strptime(fecha_ultimo_payment_cargado, '%Y-%m-%d')
                    fecha_ultimo_payment_cargado += timedelta(days=1)
                except Exception as e:
                    fecha_ultimo_payment_cargado = None
                    print(f"Error: {e}")
            break
        elif desc['Status'] == 'FAILED':
            print("Error al consultar Redshift:", desc['Error'])
            date_str = '2024/10/01'
            break
        
    if fecha_ultimo_payment_cargado is None:
        date_str = '2024/10/01' 
    else:
        date_str = fecha_ultimo_payment_cargado.strftime('%Y/%m/%d')

    # Obtenemos los ids existentes
    id_existentes_query = "SELECT DISTINCT id FROM bank_payments;"

    # Ejecutar consulta
    response = redshift_data.execute_statement(
        Database='dev',
        WorkgroupName='pdf-etl-workgroup',
        Sql=id_existentes_query
    )

    ids_existentes_en_redshift = set()
    while True:
        desc = redshift_data.describe_statement(Id=response['Id'])
        if desc['Status'] == 'FINISHED':
            if desc['HasResultSet']:
                try:
                    result = redshift_data.get_statement_result(Id=response['Id'])
                    ids_existentes_en_redshift = {
                        row[0]['stringValue'] for row in result['Records'] if 'stringValue' in row[0]
                    }
                except Exception as e:
                    ids_existentes_en_redshift = set()
                    print(f"❌ Error al obtener resultados de Redshift: {e}")
            break
        elif desc['Status'] == 'FAILED':
            print("❌ Error al consultar Redshift:", desc['Error'])
            break

    sender_email = "mensajesyavisos@mails.santander.com.ar"
    subject_contains = "Pagaste"
    # body_contains = "Te acercamos el detalle de tu consumo con la Tarjeta Santander"
    query = f'from:{sender_email} subject:"{subject_contains}" after:{date_str}'
    results = gmail_service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])

    print(f"Total de mails de Santander posterior a {date_str}: {len(messages)}")

    for msg in messages:
        message = gmail_service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        msg_id = msg['id']

        if msg_id not in ids_existentes_en_redshift:
            payload = message['payload']
            parts = payload.get('parts', [])
            html_encoded = find_html_part(payload)
            html_data = base64.urlsafe_b64decode(html_encoded).decode('utf-8', errors='replace') if html_encoded else None
            body_text = BeautifulSoup(html_data, 'html.parser').get_text() if html_data else ""

            mail_data = {
                "message_id": msg_id,
                "date": datetime.fromtimestamp(int(message['internalDate']) / 1000).isoformat(),
                "sender": sender_email,
                "subject": next(h['value'] for h in payload['headers'] if h['name'] == 'Subject'),
                "html_body": html_data,
                "raw_text": body_text,
            }

            s3_key = f"{folder}{mail_data['date'][:10]}-{msg_id}.json"
            s3_client.put_object(Body=json.dumps(mail_data), Bucket=bucket_name, Key=s3_key)
            print(f"✅ Archivo subido a S3: {s3_key}")
        else:
            print("⚠️ El archivo ya existe en S3, se omite la subida.")

def parse_monto(monto_raw):
    if not monto_raw:
        return None
    limpio = monto_raw.strip()
    for prefijo in ["U$S", "USD", "US$", "ARS$", "AR$", "$"]:
        limpio = limpio.replace(prefijo, "")
    limpio = limpio.replace(".", "").replace(",", ".")
    try:
        return float(limpio)
    except ValueError:
        print(f"❌ Error al convertir monto: '{monto_raw}' -> '{limpio}'")
        return None

def find_val(cuadro, label):
    try:
        idx = cuadro.index(label)
        return cuadro[idx + 1]
    except ValueError:
        return None
            
def parse_mail(json_obj):
    soup = BeautifulSoup(json_obj.get("html_body", ""), "html.parser")
    cuadro = list(soup.stripped_strings)
    
    date = json_obj.get("date", "")
    monto_raw = find_val(cuadro, "Monto")
    divisa = "USD" if monto_raw and "U$S" in monto_raw else "ARS" if monto_raw and "$" in monto_raw else None
    monto = parse_monto(monto_raw)
    fecha = find_val(cuadro, "Fecha")
    hora = find_val(cuadro, "Hora")
    comercio = find_val(cuadro, "Comercio")

    nro_tarjeta = None
    for i, text in enumerate(cuadro):
        if text.startswith("terminada en"):
            try:
                nro_tarjeta = cuadro[i + 1]
            except IndexError:
                nro_tarjeta = None

    if not (fecha and hora and comercio and monto and nro_tarjeta and divisa):
        print("❌ Faltan campos requeridos para crear el ID.")
        return None
    
    base_str = f"{fecha}_{hora}_{monto}_{comercio}_{nro_tarjeta or ''}_{divisa}"
    id_hash = hashlib.md5(base_str.encode('utf-8')).hexdigest()
    
    return {
        "id": id_hash,
        "message_id": json_obj["message_id"],
        "fecha_pago": fecha,
        "hora_pago": hora,
        "tarjeta": next((t for t in cuadro if "Tarjeta Santander" in t), None),
        "nro_tarjeta": nro_tarjeta,
        "comercio": comercio,
        "cuotas": int(find_val(cuadro, "Cuotas") or 1),
        "monto": monto,
        "divisa": divisa,
        "date" : date,
        "extraido_en": datetime.now().isoformat()
    }

def transform_bank_payments_data():
    bucket_name = 'bank-payments'
    prefix = 'raw/'
    destination_folder = 'processed/'
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    raw_json = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]

    for key in raw_json:
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = json.loads(obj['Body'].read().decode('utf-8'))
        records = parse_mail(content)
        df = pd.DataFrame([records])

        print(f"📄 Procesando: {key}")
        try:
            # Convertir el DataFrame a CSV en memoria (no guardar en disco)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            new_key = f"{destination_folder}{records["date"]}-{records["message_id"]}.csv"

            # Subir el CSV a S3
            s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=new_key)
            print(f"✅ Archivo subido como csv a S3/{new_key}")
        except Exception as e:
                print(f"Error al procesar {key}: {str(e)}")

def format_value(val):
    if val is None or pd.isna(val):
        return 'NULL'
    if isinstance(val, str):
        return f"'{val.replace("'", "''")}'"
    if isinstance(val, pd.Timestamp):
        return f"'{val.isoformat(sep=' ')}'"
    return str(val)  # para números

# Funcion para cargar los datos de los archivos transformados de los PDFs en la tabla de Redshift
def load_to_redshift_pdf_ticket(redshift_data, df, pdf_key):
    for _, row in df.iterrows():
        sql = f"""
        INSERT INTO carrefour_data VALUES (
            '{row['nro_ticket']}',
            '{row['fecha']}',
            '{row['categ'].replace("'", "''")}',
            '{row['prod'].replace("'", "''")}',
            '{row['cant']}',
            '{row['peso']}',
            '{row['p_unit']}',
            '{row['p_total']}',
            '{row['total_ticket_bruto']}',
            '{row['total_ticket_meli']}'
        )
        """
        redshift_data.execute_statement(
            Database='dev',
            WorkgroupName='pdf-etl-workgroup',
            Sql=sql
        )

# Funcion para cargar los datos de los archivos transformados de los reportes de MP en la tabla de Redshift
def load_to_redshift_mp_report(redshift_data, report_df, report_id, report_date):    
    # Obtenemos los ids de los reportes ya ingestados en Redshift 
    date_query = """
        SELECT DISTINCT REPORT_ID
        FROM mp_data
    """
    
    # Ejecutar consulta
    response = redshift_data.execute_statement(
        Database='dev',
        WorkgroupName='pdf-etl-workgroup',
        Sql=date_query
    )
    
    # Esperar resultados (puede tomar algunos segundos)
    set_redshift_reports_loaded = set()
    while True:
        desc = redshift_data.describe_statement(Id=response['Id'])
        if desc['Status'] == 'FINISHED':
            if desc['HasResultSet']:
                result = redshift_data.get_statement_result(Id=response['Id'])     
                if result['Records'] == []:
                    pass
                    print('Tabla vacia, se cargan todos los reportes')
                else:
                    set_redshift_reports_loaded = {
                        row[0]['stringValue'] for row in result['Records']
                    }                       
                print(f'La tabla en Redshift contiene datos de los siguientes reportes: {set_redshift_reports_loaded}')
            break
        elif desc['Status'] == 'FAILED':
            print("Error al consultar Redshift:", desc['Error'])
            break
           
    if report_id not in set_redshift_reports_loaded:
        inserted_rows = 0
        for _, row in report_df.iterrows():
            try: 
                sql = f"""
                    INSERT INTO mp_data (
                        SOURCE_ID,
                        REPORT_ID,
                        REPORT_DATE,
                        SETTLEMENT_DATE,
                        PAYMENT_METHOD_TYPE,
                        TRANSACTION_TYPE,
                        TRANSACTION_AMOUNT,
                        TRANSACTION_DATE,
                        REAL_AMOUNT,
                        POS_ID,
                        STORE_ID,
                        STORE_NAME,
                        PAYER_NAME,
                        BUSINESS_UNIT,
                        SUB_UNIT
                    ) VALUES (
                        {format_value(row['SOURCE_ID'])},
                        {format_value(report_id)},
                        {format_value(report_date)},
                        {format_value(row['SETTLEMENT_DATE'])},
                        {format_value(row['PAYMENT_METHOD_TYPE'])},
                        {format_value(row['TRANSACTION_TYPE'])},
                        {format_value(row['TRANSACTION_AMOUNT'])},
                        {format_value(row['TRANSACTION_DATE'])},
                        {format_value(row['REAL_AMOUNT'])},
                        {format_value(row['POS_ID'])},
                        {format_value(row['STORE_ID'])},
                        {format_value(row['STORE_NAME'])},
                        {format_value(row['PAYER_NAME'])},
                        {format_value(row['BUSINESS_UNIT'])},
                        {format_value(row['SUB_UNIT'])}
                )
                """
                redshift_data.execute_statement(
                    Database='dev',
                    WorkgroupName='pdf-etl-workgroup',
                    Sql=sql
                )
                inserted_rows += 1
            except:
                sql = f"""
                    INSERT INTO mp_data (
                        SOURCE_ID,
                        REPORT_ID,
                        REPORT_DATE,
                        SETTLEMENT_DATE,
                        PAYMENT_METHOD_TYPE,
                        TRANSACTION_TYPE,
                        TRANSACTION_AMOUNT,
                        TRANSACTION_DATE,
                        REAL_AMOUNT,
                        POS_ID,
                        STORE_ID,
                        STORE_NAME,
                        PAYER_NAME,
                        BUSINESS_UNIT,
                        SUB_UNIT
                    ) VALUES (
                        {format_value(row['ID DE OPERACIÓN EN MERCADO PAGO'])},
                        {format_value(report_id)},
                        {format_value(report_date)},
                        {format_value(row['FECHA DE APROBACIÓN'])},
                        {format_value(row['TIPO DE MEDIO DE PAGO'])},
                        {format_value(row['TIPO DE OPERACIÓN'])},
                        {format_value(row['VALOR DE LA COMPRA'])},
                        {format_value(row['FECHA DE ORIGEN'])},
                        {format_value(row['MONTO NETO DE OPERACIÓN'])},
                        {format_value(row['ID DE CAJA'])},
                        {format_value(row['ID DE LA SUCURSAL'])},
                        {format_value(row['NOMBRE DE LA SUCURSAL'])},
                        {format_value(row['PAGADOR'])},
                        {format_value(row['CANAL DE VENTA'])},
                        {format_value(row['PLATAFORMA DE COBRO'])}
                    ) 
                """
                redshift_data.execute_statement(
                    Database='dev',
                    WorkgroupName='pdf-etl-workgroup',
                    Sql=sql
                )
                inserted_rows += 1

        print(f"✅ Insertadas {inserted_rows} filas del reporte {report_id} con fecha {report_date}")

# Funcion para cargar en la tabla de Redshift los datos del csv que representa el gasto extraido del mail con el gasto reportado del banco
def load_to_redshift_bank_payment(redshift_data, df):
    id = df['id'].iloc[0]

    # Obtenemos los ids de los gastos ya ingestados en Redshift 
    ids_gastos_query = """
        SELECT DISTINCT id
        FROM bank_payments
    """
    
    # Ejecutar consulta
    response = redshift_data.execute_statement(
        Database='dev',
        WorkgroupName='pdf-etl-workgroup',
        Sql=ids_gastos_query
    )
    
    # Esperar resultados (puede tomar algunos segundos)
    set_redshift_gastos_cargados = set()
    while True:
        desc = redshift_data.describe_statement(Id=response['Id'])
        if desc['Status'] == 'FINISHED':
            if desc['HasResultSet']:
                result = redshift_data.get_statement_result(Id=response['Id'])     
                if result['Records'] == []:
                    pass
                    print('Tabla vacia, se cargan todos los gastos')
                else:
                    set_redshift_gastos_cargados = {
                        row[0]['stringValue'] for row in result['Records']
                    }                       
                print(f'La tabla bank_payments en Redshift contiene datos de los siguientes reportes: {set_redshift_gastos_cargados}')
            break
        elif desc['Status'] == 'FAILED':
            print("Error al consultar Redshift:", desc['Error'])
            break
    
    if id not in set_redshift_gastos_cargados:
        inserted_rows = 0
        for _, row in df.iterrows():
            fecha_pago = pd.to_datetime(row['fecha_pago'], dayfirst=True).strftime('%Y-%m-%d')
            hora_pago = row['hora_pago']
            if len(hora_pago) == 5:  # ejemplo: '19:44'
                hora_pago += ':00'
                
            id = format_value(row['id'])
            message_id = format_value(row['message_id'])
            fecha_pago = format_value(fecha_pago)
            hora_pago = format_value(hora_pago)
            tarjeta = format_value(row['tarjeta'])
            nro_tarjeta = format_value(row['nro_tarjeta'])
            comercio = format_value(row['comercio'])
            cuotas = format_value(row['cuotas'])
            monto = format_value(row['monto'])
            divisa = format_value(row['divisa'])
            extraido_en = format_value(row['extraido_en'])

            sql = f"""
                INSERT INTO bank_payments (
                    id,
                    message_id,
                    fecha_pago,
                    hora_pago,
                    tarjeta,
                    nro_tarjeta,
                    comercio,
                    cuotas,
                    monto,
                    divisa,
                    extraido_en
                ) VALUES (
                    {id},
                    {message_id},
                    {fecha_pago},
                    {hora_pago},
                    {tarjeta},
                    {nro_tarjeta},
                    {comercio},
                    {cuotas},
                    {monto},
                    {divisa},
                    {extraido_en}
            )
            """

            redshift_data.execute_statement(
                Database='dev',
                WorkgroupName='pdf-etl-workgroup',
                Sql=sql
            )
            inserted_rows += 1
        
        print(f"✅ Insertadas {inserted_rows} filas del gasto de {divisa} {monto} en {comercio} con fecha {fecha_pago}")

def load_data(event,context):
    try:
        # Conexion a Redshift
        redshift_data = boto3.client('redshift-data')

        # Obtenemos los datos de lo que necesitamos cargar, si es un pdf de tickets o un reporte de Mercado Pago
        etl_flow = event['etl_flow'] # MP o TICKET
        bucket = event['bucket']
        key = event['key']
        
        print(etl_flow)

        print(f"📥 Descargando archivo desde S3: s3://{bucket}/{key}")
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)

        if key.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(response['Body'].read()))
        elif key.endswith(".xlsx"):
            df = pd.read_excel(io.BytesIO(response['Body'].read()))
        else:
            raise Exception("Formato no soportado")

        if etl_flow == 'MP':
            report_id = event['report_id']
            report_date = event['report_date']
            print('Se lee el csv o xlsx de reporte de mp convertido en S3 y se mergea a la tabla de mp_data')
            load_to_redshift_mp_report(redshift_data, df, report_id, report_date)
        elif etl_flow == 'TICKET':
            report_id, report_date = '', ''
            print('Se lee el pdf convertido en csv en S3 y se mergea a la tabla de carrefour_data')
            load_to_redshift_pdf_ticket(redshift_data, df, key)
        else: # es un gasto del banco
            print('Se lee el mail convertido en csv en S3 y se mergea a la tabla de bank_payments')
            load_to_redshift_bank_payment(redshift_data, df) 

    except Exception as e:
        print("⚠️ Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

# redshift_data = boto3.client('redshift-data')
# print(extract_bank_payments_from_gmail(redshift_data))

# print(transform_bank_payments_data())
event = {
    "etl_flow" : 'BANK',
    "key" : "processed/2025-04-26T17:09:43-19673b85c05b020c.csv",
    "bucket" : "bank-payments"
}

print(load_data(event,''))

