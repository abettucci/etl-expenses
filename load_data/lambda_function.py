import pandas as pd
import boto3
import io
import os
import json
import csv
import re
import unicodedata
import time
from datetime import datetime
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

iam_role = os.environ["IAM_ROLE_REDSHIFT"]

def format_value(val):
    if val is None or pd.isna(val):
        return 'NULL'
    if isinstance(val, str):
        # Escapar comillas simples en SQL ( ' -> '' )
        return "'" + val.replace("'", "''") + "'"
    if isinstance(val, pd.Timestamp):
        return f"'{val.isoformat(sep=' ')}'"
    return str(val)  # para números

def redshift_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "VARCHAR(500)"  # catch-all for strings, objects

def clean_column_name(col):
    # Eliminar tildes y convertir a ASCII
    col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode()
    # Reemplazar caracteres no alfanuméricos por guion bajo
    col = re.sub(r'[^A-Za-z0-9_]+', '_', col)
    return col.upper().strip('_')

def create_redshift_table_from_df(df, columnas_sql, table_name, redshift_data, database, workgroup, primary_key=None):
    pk_sql = f", PRIMARY KEY ({primary_key})" if primary_key else ""

    # 1️⃣ Verificar si existe antes
    check_stmt = f"""
        SELECT COUNT(*) AS count
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name = '{table_name}';
    """
    resp_check = redshift_data.execute_statement(
        Database=database,
        WorkgroupName=workgroup,
        Sql=check_stmt
    )
    stmt_id_check = resp_check["Id"]

    # Esperar resultado y mostrarlo para debug
    while True:
        desc = redshift_data.describe_statement(Id=stmt_id_check)
        if desc["Status"] == "FINISHED":
            result = redshift_data.get_statement_result(Id=stmt_id_check)
            existe_antes = int(result["Records"][0][0]["longValue"]) > 0
            break
        elif desc["Status"] == "FAILED":
            raise RuntimeError(desc.get("Error"))
        time.sleep(1)

    # 2️⃣ Ejecutar el create
    create_stmt = f"""
    CREATE TABLE IF NOT EXISTS public.{table_name} (
      {columnas_sql}{pk_sql}
    );
    """

    redshift_data.execute_statement(
        Database=database,
        WorkgroupName=workgroup,
        Sql=create_stmt
    )

    tiene_datos = False
    if existe_antes:
        count_stmt = f"SELECT COUNT(*) AS count FROM public.{table_name};"
        resp_count = redshift_data.execute_statement(
            Database=database,
            WorkgroupName=workgroup,
            Sql=count_stmt
        )
        stmt_id_count = resp_count["Id"]

        while True:
            desc = redshift_data.describe_statement(Id=stmt_id_count)
            if desc["Status"] == "FINISHED":
                result = redshift_data.get_statement_result(Id=stmt_id_count)
                tiene_datos = int(result["Records"][0][0]["longValue"]) > 0
                break
            elif desc["Status"] == "FAILED":
                raise RuntimeError(desc.get("Error"))
            time.sleep(1)

    # 4️⃣ Mensajes y retorno
    if existe_antes:
        print(f"ℹ️ La tabla {table_name} ya existía")
    else:
        print(f"✅ La tabla {table_name} fue creada ahora")

    if existe_antes:
        if tiene_datos:
            print(f"📊 La tabla {table_name} tiene datos")
        else:
            print(f"📭 La tabla {table_name} está vacía")

    return existe_antes, tiene_datos

def insert_df_into_redshift_copy_fixed(redshift_data, s3_client, df, table_name, bucket_name, database, workgroup, iam_role):
    """
    Versión corregida de la función COPY que maneja columnas faltantes
    """
    try:
        id_col = None
        if table_name == 'carrefour_data' and 'operation_id' in df.columns:
            pass #id_col = 'operation_id'
        elif table_name == 'mp_data' and 'report_id' in df.columns:
            id_col = 'report_id'
        elif table_name == 'archivos_ingestados' and 'id' in df.columns:
            id_col = 'id'
        elif table_name == 'dim_producto' and 'product_id' in df.columns:
            id_col = 'product_id'
        elif table_name == 'bank_payments' and 'id' in df.columns:
            id_col = 'id'

        if id_col:
            print(f"🔍 Verificando duplicados en columna clave '{id_col}' para {table_name}...")

            # Consultar los valores existentes en Redshift
            check_query = f"SELECT DISTINCT {id_col} FROM {table_name};"
            try:
                resp = redshift_data.execute_statement(
                    Database=database,
                    WorkgroupName=workgroup,
                    Sql=check_query
                )
                while True:
                    desc = redshift_data.describe_statement(Id=resp['Id'])
                    if desc["Status"] == "FINISHED":
                        result = redshift_data.get_statement_result(Id=resp['Id'])
                        existing_ids = {
                            list(row[0].values())[0]
                            for row in result.get("Records", [])
                            if row and list(row[0].values())[0] is not None
                        }
                        print(f"📦 {len(existing_ids)} registros ya existen en {table_name}")
                        break
                    elif desc["Status"] == "FAILED":
                        print(f"⚠️ Error al consultar duplicados: {desc['Error']}")
                        existing_ids = set()
                        break
                    time.sleep(1)
            except Exception as e:
                print(f"⚠️ No se pudo consultar duplicados: {str(e)}")
                existing_ids = set()

            # Filtrar el DataFrame para evitar duplicados
            before = len(df)
            df = df[~df[id_col].isin(existing_ids)]
            after = len(df)
            print(f"🧹 Filtradas {before - after} filas duplicadas ({after} filas nuevas)")


        # 1. Primero obtener el esquema actual de Redshift
        schema_query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        
        schema_resp = redshift_data.execute_statement(
            Database=database,
            WorkgroupName=workgroup,
            Sql=schema_query
        )
        
        schema_id = schema_resp['Id']
        while True:
            desc = redshift_data.describe_statement(Id=schema_id)
            if desc['Status'] == 'FINISHED':
                schema_result = redshift_data.get_statement_result(Id=schema_id)
                break
            time.sleep(1)
        
        redshift_columns = []
        for record in schema_result['Records']:
            redshift_columns.append(record[0]['stringValue'])
        
        print(f"🔍 Esquema Redshift: {redshift_columns}")
        
        # 2. Corregir el DataFrame para que coincida
        df_fixed = fix_dataframe_for_redshift_copy(df, redshift_columns)
        
        # 3. Mostrar preview del DataFrame corregido
        print(f"\n🔍 PREVIEW DEL DATAFRAME CORREGIDO:")
        print(f"Columnas: {list(df_fixed.columns)}")
        print(f"Primeras filas:")
        print(df_fixed.head(3).to_string())
        
        # 4. Verificar valores nulos en columnas críticas
        print(f"\n🔍 VALORES NULOS EN COLUMNAS CLAVE:")
        for col in ['operation_id', 'product_id', 'nro_ticket']:
            if col in df_fixed.columns:
                null_count = df_fixed[col].isnull().sum()
                print(f"   {col}: {null_count}/{len(df_fixed)} nulos")
        
        # 5. Exportar a CSV
        csv_buffer = io.StringIO()
        df_fixed.to_csv(csv_buffer, 
                       index=False,
                       sep=',',
                       quoting=csv.QUOTE_MINIMAL)
        
        # 6. Subir a S3
        s3_key = f"tmp/{table_name}_{int(time.time())}.csv"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue().encode('utf-8')
        )
        s3_path = f"s3://{bucket_name}/{s3_key}"
        print(f"📤 CSV subido a {s3_path}")
        
        # 7. Verificar contenido del CSV
        csv_obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        csv_content = csv_obj['Body'].read().decode('utf-8')
        lines = csv_content.split('\n')
        print(f"📄 CSV tiene {len(lines)} líneas, {len(df_fixed)} filas de datos")
        
        # Mostrar header del CSV
        if lines:
            print(f"📋 Header CSV: {lines[0]}")
        
        # 8. Ejecutar COPY
        copy_sql = f"""
            COPY {table_name}
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            CSV
            IGNOREHEADER 1
            DELIMITER ','
            EMPTYASNULL
            BLANKSASNULL
            TRUNCATECOLUMNS
            MAXERROR 100;
        """
        
        print(f"🔧 Ejecutando COPY...")
        print(f"SQL: {copy_sql}")
        
        resp = redshift_data.execute_statement(
            Database=database,
            WorkgroupName=workgroup,
            Sql=copy_sql
        )

        # 9. Polling
        start_time = time.time()
        while True:
            desc = redshift_data.describe_statement(Id=resp['Id'])
            status = desc["Status"]
            
            if status == "FAILED":
                error_msg = desc.get('Error', 'Error desconocido')
                print(f"❌ Error en COPY: {error_msg}")
                raise Exception(f"COPY failed: {error_msg}")
                
            elif status == "FINISHED":
                duration = time.time() - start_time
                print(f"✅ COPY completado en {duration:.2f}s")
                
                # Verificar que se insertaron datos
                count_query = f"SELECT COUNT(*) FROM {table_name};"
                count_resp = redshift_data.execute_statement(
                    Database=database,
                    WorkgroupName=workgroup,
                    Sql=count_query
                )
                
                count_id = count_resp['Id']
                while True:
                    count_desc = redshift_data.describe_statement(Id=count_id)
                    if count_desc['Status'] == 'FINISHED':
                        count_result = redshift_data.get_statement_result(Id=count_id)
                        row_count = count_result["Records"][0][0]["longValue"]
                        print(f"📊 VERIFICACIÓN: La tabla {table_name} ahora tiene {row_count} filas")
                        
                        if row_count > 0:
                            print(f"🎉 ¡COPY EXITOSO! Se insertaron {row_count} filas")
                        else:
                            print(f"⚠️ COPY completado pero la tabla sigue vacía")
                            
                        break
                    time.sleep(1)
                
                break
            else:
                print(f"⏳ Estado: {status}...")
                time.sleep(2)
                
    except Exception as e:
        print(f"❌ Error en COPY: {str(e)}")
        raise

def column_name_mapping(df):
    column_mapping = {
        "NUMERO_DE_IDENTIFICACION": "EXTERNAL_REFERENCE",
        "ID_DE_OPERACION_EN_MERCADO_PAGO": "SOURCE_ID",
        "CODIGO_DE_LA_CUENTA_DEL_VENDEDOR": "USER_ID",
        "TIPO_DE_MEDIO_DE_PAGO": "PAYMENT_METHOD_TYPE",
        "MEDIO_DE_PAGO": "PAYMENT_METHOD",
        "PAIS_DE_ORIGEN_DE_LA_CUENTA_DE_MERCADO_PAGO": "SITE",
        "TIPO_DE_OPERACION": "TRANSACTION_TYPE",
        "VALOR_DE_LA_COMPRA": "TRANSACTION_AMOUNT",
        "MONEDA": "TRANSACTION_CURRENCY",
        "MONTO_RECIBIDO_POR_COMPRAS_POR_SPLIT": "SELLER_AMOUNT",
        "FECHA_DE_ORIGEN": "TRANSACTION_DATE",
        "COMISION_MAS_IVA": "FEE_AMOUNT",
        "MONTO_NETO_DE_LA_OPERACION_QUE_IMPACTO_TU_DINERO": "SETTLEMENT_NET_AMOUNT",
        "MONEDA_DE_LA_LIQUIDACION": "SETTLEMENT_CURRENCY",
        "FECHA_DE_APROBACION": "SETTLEMENT_DATE",
        "MONTO_NETO_DE_OPERACION": "REAL_AMOUNT",
        "CUPON_DE_DESCUENTO": "COUPON_AMOUNT",
        "DATOS_EXTRA": "METADATA",
        "COMISION_DE_MERCADO_LIBRE_MAS_IVA": "MKP_FEE_AMOUNT",
        "COMISION_POR_OFRECER_CUOTAS_SIN_INTERES": "FINANCING_FEE_AMOUNT",
        "COSTO_DE_ENVIO": "SHIPPING_FEE_AMOUNT",
        "IMPUESTOS_COBRADOS_POR_RETENCIONES_IIBB": "TAXES_AMOUNT",
        "CUOTAS": "INSTALLMENTS",
        "DETALLE_DE_IMPUESTOS": "TAX_DETAIL",
        "ID_DE_CAJA": "POS_ID",
        "ID_DE_LA_SUCURSAL": "STORE_ID",
        "NOMBRE_DE_LA_SUCURSAL": "STORE_NAME",
        "ID_DE_CAJA_DEFINIDO_POR_EL_USUARIO": "EXTERNAL_POS_ID",
        "NOMBRE_DE_CAJA": "POS_NAME",
        "ID_DE_SUCURSAL_DEFINIDO_POR_EL_USUARIO": "EXTERNAL_STORE_ID",
        "ID_DE_LA_ORDEN": "ORDER_ID",
        "ID_DEL_ENVIO": "SHIPPING_ID",
        "MODO_DE_ENVIO": "SHIPMENT_MODE",
        "ID_DEL_PAQUETE": "PACK_ID",
        "IMPUESTOS_DESAGREGADOS": "TAXES_DISAGGREGATED",
        "NUMERO_DE_SERIE_DEL_LECTOR_(S/N)": "POI_ID",
        "BILLETERA_VIRTUAL": "POI_WALLET_NAME",
        "BANCO_DE_ORIGEN": "POI_BANK_NAME",
        "NUMERO_INICIAL_DE_TARJETA": "CARD_INITIAL_NUMBER",
        "OPERATION_TAGS": "OPERATION_TAGS",  # ya coincide
        "TIPO_DE_IDENTIFICACION_DEL_PAGADOR": "PAYER_ID_TYPE",
        "NUMERO_DE_IDENTIFICACION_DEL_PAGADOR": "PAYER_ID_NUMBER",
        "PAGADOR": "PAYER_NAME",
        "CANAL_DE_VENTA": "BUSINESS_UNIT",
        "PLATAFORMA_DE_COBRO": "SUB_UNIT",
        "FECHA_DE_LIBERACION_DEL_DINERO": "MONEY_RELEASE_DATE",
        "CODIGO_DE_PRODUCTO_SKU": "PRODUCT_SKU",
        "DETALLE_DE_LA_VENTA": "SALE_DETAIL"
    }

    df.rename(columns=column_mapping, inplace=True)

    return df

def fix_dataframe_for_redshift_copy(df, redshift_columns):
    """
    Corrige el DataFrame para que coincida con el esquema de Redshift
    Agrega columnas faltantes con valores NULL
    """
    df_fixed = df.copy()
    
    print("\n🔧 CORRIGIENDO DATAFRAME PARA REDSHIFT:")
    print(f"Columnas en Redshift: {redshift_columns}")
    print(f"Columnas en DataFrame: {list(df_fixed.columns)}")
    
    # 1. Agregar columnas faltantes con valores NULL
    columns_added = []
    for col in redshift_columns:
        if col not in df_fixed.columns:
            df_fixed[col] = None  # o pd.NA para pandas 1.0+
            columns_added.append(col)
            print(f"✅ Agregada columna faltante: {col} con valores NULL")
    
    if columns_added:
        print(f"📋 Columnas agregadas: {columns_added}")
    
    # 2. Reordenar columnas para que coincidan con Redshift
    df_fixed = df_fixed[redshift_columns]
    
    # 3. Corregir tipos de datos problemáticos
    type_corrections = {
        'product_id': 'Int64',
        'nro_ticket': 'Int64'
    }
    
    for col, target_type in type_corrections.items():
        if col in df_fixed.columns:
            if target_type == 'Int64':
                # Convertir a Int64 (permite NaN)
                df_fixed[col] = pd.to_numeric(df_fixed[col], errors='coerce').astype('Int64')
                print(f"✅ Corregido {col} a Int64")
    
    # 4. Limpiar strings y manejar valores nulos
    for col in df_fixed.columns:
        if df_fixed[col].dtype == 'object':
            df_fixed[col] = df_fixed[col].astype(str)
            df_fixed[col] = df_fixed[col].replace(['nan', 'NaN', 'None', '<NA>', 'NULL', 'null'], '')
            df_fixed[col] = df_fixed[col].str.strip()
    
    print(f"📊 DataFrame final: {df_fixed.shape[0]} filas, {df_fixed.shape[1]} columnas")
    
    return df_fixed

def delete_tmp_files_in_s3(s3, bucket_name):
    """
    Borra todos los archivos de la carpeta tmp/ en S3
    """    
    try:
        # Listar todos los objetos en tmp/
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix='tmp/')
        
        if 'Contents' in response:
            # Crear lista de objetos a borrar
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            
            # Borrar en lote
            s3.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects_to_delete}
            )
            
            print(f"✅ Borrados {len(objects_to_delete)} archivos temporales de s3://{bucket_name}/tmp/")
        else:
            print("✅ No hay archivos en la carpeta tmp/")
            
    except Exception as e:
        print(f"❌ Error borrando archivos temporales: {str(e)}")

def get_redshift_table_data(redshift_data, table_name):
    database = "dev"
    workgroup = "pdf-etl-workgroup"
    query = f"SELECT * FROM {table_name};"

    # Ejecutar consulta
    response = redshift_data.execute_statement(
        Database=database,
        WorkgroupName=workgroup,
        Sql=query
    )

    query_id = response["Id"]

    while True:
        desc = redshift_data.describe_statement(Id=query_id)
        status = desc["Status"]

        if status == "FAILED":
            print("❌ Error al consultar Redshift:", desc.get("Error", "Error desconocido"))
            break
        elif status == "FINISHED":
            print("✅ Query finalizada correctamente.\n")

            if desc.get("HasResultSet"):
                # Obtener los resultados
                result = redshift_data.get_statement_result(Id=query_id)
                
                # Nombres de columnas
                columns = [col["name"] for col in result["ColumnMetadata"]]
                
                # Parsear filas
                rows = []
                for record in result["Records"]:
                    row = [list(cell.values())[0] if cell else None for cell in record]
                    rows.append(row)
                
                # Crear DataFrame
                df = pd.DataFrame(rows, columns=columns)
                
                # Mostrar las primeras filas
                print("📊 Resultados de la tabla Redshift:")
                print(df.to_string(index=False))
                
            else:
                print("⚠️ La consulta no devolvió resultados.")
            break
        else:
            time.sleep(1)
    return df

def lambda_handler(event,context):
    try:
        redshift_data = boto3.client('redshift-data')
        print(event)

        etl_flow = event['etl_flow']
        bucket = event['bucket']
        key = event['key'] # ya tiene la carpeta en el path
        folder = 'processed/'

        print('etl_flow: ', etl_flow)
        print('bucket: ', bucket)
        print('key: ', key)
        
        # print(f"📥 Descargando archivo desde S3: s3://{bucket}/{key}")
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)

        if etl_flow == 'MP':
            table_name = 'mp_data'
            dtype = {}
        elif etl_flow == 'TICKET':
            dtype = {
                'ean': str,  # Leer como string desde el principio
                'grupo_producto': str,
                'product_id': 'Int64',  # Usar Int64 para enteros con NaN
                'nro_ticket': 'Int64',
                'categoria': str,
                'producto': str,
                'fecha': str  # Leer como string y luego convertir a datetime
            }
            table_name = 'carrefour_data'
        else:
            dtype = {}
            table_name = 'bank_payments'

        if key.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(response['Body'].read()),dtype=dtype)
        elif key.endswith(".xlsx"):
            df = pd.read_excel(io.BytesIO(response['Body'].read()))
        else:
            raise Exception("Formato no soportado")

        if etl_flow == 'MP':
            report_id = event['report_id']
            report_date = event['report_date']

            df = column_name_mapping(df)
            column_defs = [f"{clean_column_name(col)} {redshift_type(dtype)}" for col, dtype in zip(df.columns, df.dtypes)]
            column_defs += ["REPORT_ID VARCHAR(500)", "REPORT_DATE VARCHAR(500)"]
            columnas_sql = ",\n  ".join(column_defs)

            print(f'Se lee el csv {key} o xlsx de reporte de mp convertido en S3 y se mergea a la tabla de mp_data')
            flag_exists, tiene_datos = create_redshift_table_from_df(df, columnas_sql, table_name, redshift_data, 'dev', 'pdf-etl-workgroup','REPORT_ID')

            column_names_insert = [clean_column_name(col) for col in df.columns]
            column_names_insert += ["REPORT_ID", "REPORT_DATE"]
            columnas_sql_insert = ", ".join(column_names_insert)
            insert_df_into_redshift_copy_fixed(redshift_data, s3, df, table_name, bucket, 'dev', 'pdf-etl-workgroup', iam_role)

            tables = [table_name] 

        elif etl_flow == 'TICKET':
            report_id, report_date = '', ''
            column_defs = [f"{clean_column_name(col)} {redshift_type(dtype)}" for col, dtype in zip(df.columns, df.dtypes)]
            column_defs += ["operation_id VARCHAR(255)"]
            columnas_sql = ",\n  ".join(column_defs)

            print(f'Se lee el pdf {key} convertido en csv en S3 y se mergea a la tabla de {table_name}')
            flag_exists, tiene_datos = create_redshift_table_from_df(df, columnas_sql, table_name, redshift_data, 'dev', 'pdf-etl-workgroup', 'nro_ticket')

            print('df: ', df)

            # Obtener la tabla actual y la dimensión de productos desde Redshift
            df_actual = get_redshift_table_data(redshift_data, "carrefour_data")
            df_dim_producto = get_redshift_table_data(redshift_data, "dim_producto")
            df_dim_producto = df_dim_producto.rename(columns={
                'nombre_producto': 'producto',  # asegurar que los nombres coincidan
            })
            
            print('df_actual: ', df_actual)

            df_dim_producto = df_dim_producto.drop_duplicates(subset=['producto'], keep='first')

            # Enriquecer df con df_dim_producto (para obtener product_id y grupo_producto)
            df_enriquecido = df.merge(
                df_dim_producto[['producto', 'product_id', 'grupo_producto']],
                on='producto',
                how='left'
            )

            print('df_enriquecido: ', df_enriquecido)

            # dups = df_enriquecido[df_enriquecido.duplicated(subset=['producto', 'nro_ticket'], keep=False)]
            # if not dups.empty:
            #     print("⚠️ Aún hay duplicados después del merge:")
            #     print(dups[['producto', 'nro_ticket', 'product_id']])

            for col in ['product_id', 'grupo_producto']:
                col_x, col_y = f"{col}_x", f"{col}_y"
                if col_x in df_enriquecido.columns and col_y in df_enriquecido.columns:
                    # Si hay dos columnas, elegimos los valores nuevos de la derecha (_y)
                    df_enriquecido[col] = df_enriquecido[col_y].combine_first(df_enriquecido[col_x])
                    df_enriquecido.drop(columns=[col_x, col_y], inplace=True)
                elif col_y in df_enriquecido.columns:
                    df_enriquecido.rename(columns={col_y: col}, inplace=True)
                elif col_x in df_enriquecido.columns:
                    df_enriquecido.rename(columns={col_x: col}, inplace=True)

            # Generar operation_id directamente en pandas
            df_enriquecido['operation_id'] = (
                df_enriquecido['nro_ticket'].astype(str)
                + df_enriquecido['product_id'].astype(str)
                + df_enriquecido['monto_total'].astype(str)
                + '_'
                + df_enriquecido['fecha'].astype(str)
            )

            # Combinar con datos actuales y eliminar duplicados usando nro_ticket
            if not df_actual.empty:
                df_final = pd.concat([df_actual, df_enriquecido], ignore_index=True)
                df_final.drop_duplicates(subset=['nro_ticket'], inplace=True)
            else:
                df_final = df_enriquecido

            print(f"✅ Total filas únicas luego de merge: {len(df_final)}")

            existing_df = get_redshift_table_data(redshift_data, "carrefour_data")

            if not existing_df.empty:
                print(f"📦 Tabla actual en Redshift: {len(existing_df)} filas")
                # 2️⃣ Mantener solo las filas nuevas usando nro_ticket
                df_enriquecido = df_enriquecido[~df_enriquecido['nro_ticket'].isin(existing_df['nro_ticket'])]
                print(f"🧮 Filas nuevas detectadas: {len(df_enriquecido)}")
            else:
                print("📭 Tabla vacía en Redshift, se insertan todas las filas")

            # Si no hay filas nuevas, abortar el insert
            if df_enriquecido.empty:
                print("✅ No hay filas nuevas para insertar, omitiendo COPY.")
            else:
                insert_df_into_redshift_copy_fixed(redshift_data, s3, df_enriquecido, "carrefour_data", bucket, 'dev', 'pdf-etl-workgroup', iam_role)

            # Registrar nro_ticket en tabla de control
            data = df['nro_ticket'].unique().tolist()
            df_uploaded_files = pd.DataFrame(data, columns=['id'])
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df_uploaded_files['INS_DTTM'] = now
            column_defs = [f"{clean_column_name(col)} {redshift_type(dtype)}" for col, dtype in zip(df_uploaded_files.columns, df_uploaded_files.dtypes)]
            column_uploaded_files = ",\n  ".join(column_defs)
            flag_exists, tiene_datos = create_redshift_table_from_df(df_uploaded_files, column_uploaded_files, 'archivos_ingestados', redshift_data, 'dev', 'pdf-etl-workgroup', 'id')
            insert_df_into_redshift_copy_fixed(redshift_data, s3, df_uploaded_files, 'archivos_ingestados', bucket, 'dev', 'pdf-etl-workgroup', iam_role)

            # Limpieza de temporales
            delete_tmp_files_in_s3(s3, bucket)

            tables = ['archivos_ingestados', 'dim_producto', table_name]   
        else: # es un gasto del banco
            column_defs = [f"{clean_column_name(col)} {redshift_type(dtype)}" for col, dtype in zip(df.columns, df.dtypes)]
            columnas_sql = ",\n  ".join(column_defs)
            
            print(f'Se lee el mail {key} convertido en csv en S3 y se mergea a la tabla de {table_name}')
            create_redshift_table_from_df(df, columnas_sql, table_name, redshift_data, 'dev', 'pdf-etl-workgroup', 'id')

            column_names_insert = [clean_column_name(col) for col in df.columns]
            insert_df_into_redshift_copy_fixed(redshift_data, s3, df, table_name, bucket, 'dev', 'pdf-etl-workgroup', iam_role)

            tables = [table_name]  

        return {
            'table_name': tables
        }

    except Exception as e:
        print("⚠️ Error:", str(e))
        raise Exception(str(e))