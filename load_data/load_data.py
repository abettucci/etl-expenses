import pandas as pd
import boto3
import io
import json

def format_value(value):
    if pd.isna(value):
        return 'NULL'
    elif isinstance(value, str):
        return f"'{value.replace("'", "''")}'"
    elif isinstance(value, (int, float)):
        return "'" + str(value) + "'"
    else:
        return f"'{str(value)}'"

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

    # # Registrar archivo como procesado
    # register_query = f"""
    #     INSERT INTO archivos_ingestados (hash_archivo) 
    #     VALUES ('{hash_pdf}')
    # """
    # redshift_data.execute_statement(
    #     Database='dev',
    #     WorkgroupName='pdf-etl-workgroup',
    #     Sql=register_query
    # )
    # print(f"✅ PDF procesado y registrado: {pdf_key}")

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

def lambda_handler(event,context):
    try:
        # Conexion a Redshift
        redshift_data = boto3.client('redshift-data')

        # Obtenemos los datos de lo que necesitamos cargar, si es un pdf de tickets o un reporte de Mercado Pago
        etl_flow = event['etl_flow'] # MP o TICKET
        bucket = event['bucket']
        key = event['key']

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
            print('se lee el csv o xlsx de reporte de mp convertido en S3 y se mergea a la tabla de mp_data')
            load_to_redshift_mp_report(redshift_data, df, report_id, report_date)

        else: # es PDF MARKET TICKET
            report_id, report_date = '', ''
            print('se lee el pdf convertido en csv en S3 y se mergea a la tabla de carrefour_data')
            load_to_redshift_pdf_ticket(redshift_data, df, key)

    except Exception as e:
        print("⚠️ Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }