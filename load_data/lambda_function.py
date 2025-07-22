import pandas as pd
import boto3
import io
import json

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

def lambda_handler(event,context):
    try:
        # Conexion a Redshift
        redshift_data = boto3.client('redshift-data')

        print(event['body'])

        # Obtenemos los datos de lo que necesitamos cargar, si es un pdf de tickets o un reporte de Mercado Pago
        etl_flow = event['body']['etl_flow']
        bucket = event['body']['bucket']
        key = event['body']['key']
        
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