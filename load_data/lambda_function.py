import pandas as pd
import boto3
import io
import os
import json
import csv
import re
import unicodedata
import time
from rapidfuzz import fuzz
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
    
    # Ejecutar CREATE TABLE y esperar a que termine
    create_resp = redshift_data.execute_statement(
        Database=database,
        WorkgroupName=workgroup,
        Sql=create_stmt
    )
    create_id = create_resp["Id"]
    while True:
        create_desc = redshift_data.describe_statement(Id=create_id)
        if create_desc["Status"] == "FINISHED":
            break
        elif create_desc["Status"] == "FAILED":
            raise RuntimeError(create_desc.get("Error", "CREATE TABLE failed"))
        time.sleep(1)

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

def generar_diccionario_normalizacion(productos, threshold=85):
    grupos = {}
    normalizacion = {}

    for producto in productos:
        encontrado = False
        for canonico in grupos:
            if fuzz.ratio(producto, canonico) >= threshold:
                normalizacion[producto] = normalizacion[canonico]
                encontrado = True
                break
        if not encontrado:
            clave = slugify(producto)
            grupos[producto] = clave
            normalizacion[producto] = clave

    return normalizacion

def slugify(text):
    return re.sub(r'[^a-z0-9]+', '', text.lower()).strip('')

def create_and_fill_product_dim_table_in_redshift(s3, bucket, folder, df, table_name, redshift_data, database, workgroup, pk, flag_check_ids_repetidos):
    # Primero vemos si ya existe la tabla 'dim_producto', si no existe la creamos con pk = 'product_id'   
    columnas_sql = f"""
        nombre_producto TEXT,
        product_id INTEGER,
        ean TEXT,
        grupo_producto TEXT
    """

    # 🔧 ARREGLO: Crear tabla sin PRIMARY KEY para evitar problemas de transacciones
    flag_exists, tiene_datos = create_redshift_table_from_df(
        df, columnas_sql, table_name, redshift_data, database, workgroup, None  # Sin PK
    )

    df["ean"] = (
        df["ean"]
        .astype(str)
        .replace(["nan", "None", "TRUE", "FALSE", "True", "False"], None)
        .where(df["ean"].notna(), None)
    )

    # ------------------------------------------------------------------------------------------
    # Si la tabla NO existe o está vacía → inicializamos la dimensión desde carrefour_data
    # ------------------------------------------------------------------------------------------
    if flag_exists == False or tiene_datos == False:
        query_distinct_products = f"""
            SELECT DISTINCT producto, ean
            FROM carrefour_data;
        """

        response = redshift_data.execute_statement(
            Database=database,
            WorkgroupName=workgroup,
            Sql=query_distinct_products
        )

        set_nombre_producto_ean = set()
        while True:
            desc = redshift_data.describe_statement(Id=response['Id'])
            if desc['Status'] == 'FINISHED':
                if desc['HasResultSet']:
                    result = redshift_data.get_statement_result(Id=response['Id'])
                    if not result['Records']:
                        print('Tabla vacía, se cargan todos los productos')
                    else:
                        for record in result['Records']:
                            nombre_producto = record[0].get('stringValue', None)
                            ean = (
                                record[1].get('stringValue')
                                or record[1].get('doubleValue')
                                or record[1].get('longValue')
                                or None
                            )
                            if ean is not None:
                                ean = str(ean).split('.')[0]
                            tupla = (nombre_producto, ean)
                            set_nombre_producto_ean.add(tupla)
                break
            elif desc['Status'] == 'FAILED':
                print("Error al consultar Redshift:", desc['Error'])
                break

        df_dim_producto = pd.DataFrame(list(set_nombre_producto_ean), columns=["nombre_producto", "ean"])

        # Normalización de nombre → grupo_producto
        normalizacion_productos = generar_diccionario_normalizacion(df_dim_producto["nombre_producto"].unique())
        df_dim_producto["grupo_producto"] = df_dim_producto["nombre_producto"].map(normalizacion_productos)

        # Asignación de IDs secuenciales incluso si EAN es None
        df_dim_producto["product_id"] = range(1, len(df_dim_producto) + 1)
        df_dim_producto.drop_duplicates(subset=["nombre_producto", "ean"], inplace=True)

        print('df_dim_producto a partir de la tabla de carerfour_data en redshift que se va a insertar: ', df_dim_producto)
        
        insert_df_into_redshift_copy_fixed(
            redshift_data, s3, df_dim_producto, table_name, bucket, "dev", "pdf-etl-workgroup", iam_role
        )
        print(f"✅ Cargamos los primeros datos en la tabla {table_name}")
        return df_dim_producto

    # ------------------------------------------------------------------------------------------
    # Si la tabla dim_producto ya existe y tiene datos → agregamos nuevos productos
    # ------------------------------------------------------------------------------------------
    else:
        query_distinct_product_id = f"""
            SELECT DISTINCT nombre_producto, ean, product_id
            FROM dim_producto;
        """

        response = redshift_data.execute_statement(
            Database=database,
            WorkgroupName=workgroup,
            Sql=query_distinct_product_id
        )

        set_nombre_producto_ean = set()
        max_product_id = 0
        while True:
            desc = redshift_data.describe_statement(Id=response['Id'])
            if desc['Status'] == 'FINISHED':
                if desc['HasResultSet']:
                    result = redshift_data.get_statement_result(Id=response['Id'])
                    if not result['Records']:
                        print('Tabla vacía, se cargan todos los productos')
                    else:
                        for record in result['Records']:
                            nombre_producto = record[0].get('stringValue', None)
                            ean = (
                                record[1].get("stringValue")
                                or record[1].get("doubleValue")
                                or record[1].get("longValue")
                                or None
                            )
                            if ean is not None:
                                ean = str(ean).split(".")[0]
                            set_nombre_producto_ean.add((nombre_producto, ean))
                break
            elif desc['Status'] == 'FAILED':
                print("Error al consultar Redshift:", desc['Error'])
                break
        
        df["ean"] = (
            df["ean"]
            .astype(str)
            .replace(["nan", "None", "TRUE", "FALSE", "True", "False"], None)
            .where(df["ean"].notna(), None)
        )

        # Reemplazar NaN en ean y detectar nuevos productos
        df["existe_en_set"] = [t in set_nombre_producto_ean for t in zip(df["producto"], df["ean"])]
        df_nuevos = df[df["existe_en_set"] == False].drop_duplicates(subset=["producto", "ean"])

        # Calcular el último product_id actual
        query_max_id = f"SELECT MAX(product_id) FROM {table_name};"
        resp_max = redshift_data.execute_statement(Database=database, WorkgroupName=workgroup, Sql=query_max_id)
        desc_max = redshift_data.describe_statement(Id=resp_max["Id"])
        while desc_max["Status"] != "FINISHED":
            time.sleep(1)
            desc_max = redshift_data.describe_statement(Id=resp_max["Id"])
        result_max = redshift_data.get_statement_result(Id=resp_max["Id"])
        current_max_id = int(result_max["Records"][0][0].get("longValue", 0)) if result_max["Records"][0][0] else 0

        new_rows = []
        for i, row in df_nuevos.iterrows():
            current_max_id += 1
            nombre_producto = row["producto"]
            ean = row["ean"]
            grupo_producto = generar_diccionario_normalizacion([nombre_producto])[nombre_producto]
            new_rows.append([nombre_producto, current_max_id, ean, grupo_producto])

        if new_rows:
            df_dim_producto = pd.DataFrame(new_rows, columns=["nombre_producto", "product_id", "ean", "grupo_producto"])
            insert_df_into_redshift_copy_fixed(
                redshift_data, s3, df_dim_producto, table_name, bucket, "dev", "pdf-etl-workgroup", iam_role
            )
            print(f"✅ Se cargaron {len(new_rows)} nuevos productos en {table_name}")
        else:
            print("ℹ️ No hay nuevos productos para agregar.")

        return df_nuevos
        
# El objetivo de esta funcion es agregar la columna "product_id" a la tabla carrefour_data para luego crear un "id" para cada fila
# que va a ser un concatenado de "column_id", "nro_ticket" y "fecha"
def aggregate_table_in_redshift(table_name, redshift_data, database, workgroup):
    if table_name == 'carrefour_data':
        agregado_dim_producto_sql = """
            UPDATE carrefour_data AS car
            SET 
                grupo_producto = prod.grupo_producto,
                product_id = prod.product_id
            FROM dim_producto AS prod
            WHERE car.producto = prod.nombre_producto
              AND (car.grupo_producto IS NULL OR car.product_id IS NULL);
        """

        try:
            response = redshift_data.execute_statement(
                Database=database,
                WorkgroupName=workgroup,
                Sql=agregado_dim_producto_sql
            )

            # Esperar finalización
            while True:
                desc = redshift_data.describe_statement(Id=response['Id'])
                if desc['Status'] == 'FINISHED':
                    print(f"✅ Tabla {table_name} actualizada con datos de dim_producto")
                    break
                elif desc['Status'] == 'FAILED':
                    print("❌ Error al consultar Redshift:", desc['Error'])
                    break
        except Exception as e:
            print(f"❌ Error ejecutando query: {str(e)}")

def add_concatenated_column(table_name, redshift_data, database, workgroup):
    #quizas tenga que sumar monto al concat
    update_sql = f"""
        UPDATE {table_name}
        SET operation_id = CONCAT(CONCAT(CONCAT(CAST(nro_ticket AS VARCHAR), ''),CONCAT(CONCAT(CAST(product_id AS VARCHAR), ''),CONCAT(CAST(TRUNC(monto_total,2) AS VARCHAR), '_'))),CAST(fecha AS VARCHAR));
    """

    resp = redshift_data.execute_statement(
        Database=database,
        WorkgroupName=workgroup,
        Sql=update_sql
    )

    while True:
        desc = redshift_data.describe_statement(Id=resp['Id'])
        if desc['Status'] == 'FINISHED':
            print(f"✅ Ejecutado: {update_sql.strip().split()[0]} en {table_name}")
            break
        elif desc['Status'] == 'FAILED':
            print("❌ Error:", desc['Error'])
            break

def column_has_data(column_name, table_name, redshift_data, database, workgroup):
    tiene_datos = False

    count_stmt = f"SELECT COUNT(DISTINCT {column_name}) AS count FROM {table_name};"
    resp_count = redshift_data.execute_statement(
        Database=database,
        WorkgroupName=workgroup,
        Sql=count_stmt
    )
    stmt_id_count = resp_count["Id"]

    while True:
        desc = redshift_data.describe_statement(Id=stmt_id_count)
        if desc['Status'] == 'FINISHED':
            if desc.get('HasResultSet', False):
                result = redshift_data.get_statement_result(Id=stmt_id_count)
                tiene_datos = int(result["Records"][0][0]["longValue"]) > 0
            else:
                pass
            break
        elif desc['Status'] == 'FAILED':
            print("Error al consultar Redshift:", desc['Error'])
            break
        time.sleep(1)
    
    return tiene_datos

def column_exists(table_name, column_name, redshift_data, database, workgroup):
    sql = f"""
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        AND column_name = '{column_name}';
    """
    resp = redshift_data.execute_statement(
        Database=database,
        WorkgroupName=workgroup,
        Sql=sql
    )
    statement_id = resp['Id']
    while True:
        desc = redshift_data.describe_statement(Id=statement_id)
        if desc['Status'] == 'FINISHED':
            if desc.get('HasResultSet', False):
                result = redshift_data.get_statement_result(Id=statement_id)
                return result.get('Records', []) != []
            else:
                return False
        elif desc['Status'] == 'FAILED':
            print("❌ Error:", desc['Error'])
            return False
        time.sleep(1)

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
            pass # 🔧 ARREGLO: Deshabilitar verificación de duplicados temporalmente
        elif table_name == 'dim_producto' and 'product_id' in df.columns:
            pass # 🔧 ARREGLO: Deshabilitar verificación de duplicados temporalmente
        elif table_name == 'bank_payments' and 'id' in df.columns:
            id_col = 'id'

        if id_col:
            print(f'Vemos en un df lo que hay actualmente en la tabla {table_name} en redshift')
            query = f"SELECT * FROM {table_name};"
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
                        result = redshift_data.get_statement_result(Id=query_id)
                        columns = [col["name"] for col in result["ColumnMetadata"]]
                        rows = []
                        for record in result["Records"]:
                            row = [list(cell.values())[0] if cell else None for cell in record]
                            rows.append(row)

                        df_existing = pd.DataFrame(rows, columns=columns)
                        print(f"📊 Resultados de la tabla {table_name} Redshift:")
                        print(df_existing.to_string(index=False))
                    else:
                        print("⚠️ La consulta no devolvió resultados.")
                    break
                else:
                    time.sleep(1)

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
            SELECT column_name, ordinal_position, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' AND table_schema = 'public'
            ORDER BY ordinal_position;
        """
        
        # Ejecutar consulta de esquema con reintentos por eventual consistencia
        max_retries = 5
        attempt = 0
        redshift_columns = []
        while attempt < max_retries and not redshift_columns:
            schema_resp = redshift_data.execute_statement(
                Database=database,
                WorkgroupName=workgroup,
                Sql=schema_query
            )
            schema_id = schema_resp['Id']
            while True:
                desc = redshift_data.describe_statement(Id=schema_id)
                if desc["Status"] == "FINISHED":
                    result = redshift_data.get_statement_result(Id=schema_id)
                    print(f"\n📋 Estructura de la tabla {table_name}:")
                    for r in result.get("Records", []):
                        col = r[0].get("stringValue")
                        if col:
                            redshift_columns.append(col)
                    if not redshift_columns:
                        print("⚠️ Esquema vacío, reintentando...")
                        time.sleep(2)
                    break
                elif desc["Status"] == "FAILED":
                    print(f"❌ Error consultando schema: {desc.get('Error')}")
                    break
                time.sleep(2)
            attempt += 1
                
        print(f"🔍 Esquema Redshift: {redshift_columns}")
        
        print('df antes de fix dataframe for redshift copy: ', df)

        # 2. Corregir el DataFrame para que coincida
        df_fixed = fix_dataframe_for_redshift_copy(df, redshift_columns)
        
        # 3. Mostrar preview del DataFrame corregido
        print(f"\n🔍 PREVIEW DEL DATAFRAME CORREGIDO:")
        print(f"Columnas: {list(df_fixed.columns)}")
        print(f"Data types: {list(df_fixed.dtypes)}")
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

        time.sleep(2)
        
        # 7. Verificar contenido del CSV
        csv_obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        csv_content = csv_obj['Body'].read().decode('utf-8')
        lines = csv_content.split('\n')
        print(f"📄 CSV tiene {len(lines)} líneas, {len(df_fixed)} filas de datos")
        
        # Mostrar header del CSV
        if lines:
            print(f"📋 Header CSV: {lines[0]}")
            
        # 🔧 ARREGLO: Mostrar contenido específico para archivos_ingestados
        if table_name == 'archivos_ingestados' and len(lines) > 1:
            print(f"🔍 Contenido CSV archivos_ingestados:")
            for i, line in enumerate(lines[:3]):  # Mostrar primeras 3 líneas
                print(f"   Línea {i}: {line}")
        
        # 🔍 DIAGNÓSTICO: Mostrar primeras filas del CSV para debugging
        print(f"🔍 PRIMERAS FILAS DEL CSV:")
        for i, line in enumerate(lines[:5]):  # Mostrar primeras 5 líneas
            print(f"   Línea {i}: {line}")
        
        print("🔎 Contexto ANTES del COPY:")
        ctx_before_sql = "SELECT current_database() AS db, current_schema() AS schema, current_user AS user;"
        ctx_before_stmt = redshift_data.execute_statement(
            Database=database,
            WorkgroupName=workgroup,
            Sql=ctx_before_sql
        )
        ctx_before_id = ctx_before_stmt["Id"]
        while True:
            ctx_before_desc = redshift_data.describe_statement(Id=ctx_before_id)
            if ctx_before_desc["Status"] == "FINISHED":
                ctx_before_result = redshift_data.get_statement_result(Id=ctx_before_id)
                print(ctx_before_result["Records"])
                break
            time.sleep(1)

        # 8. Ejecutar COPY
        columns_for_copy = list(df_fixed.columns)
        columns_clause = f"(" + ", ".join(columns_for_copy) + ")" if columns_for_copy else ""
        copy_sql = f"""
            COPY public.{table_name} {columns_clause}
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            CSV
            IGNOREHEADER 1
            DELIMITER ','
            EMPTYASNULL
            BLANKSASNULL
            TRUNCATECOLUMNS
            MAXERROR 0;
        """
        
        print(f"🔧 Ejecutando COPY...")
        print(f"SQL: {copy_sql}")
        
        resp = redshift_data.execute_statement(
            Database=database,
            WorkgroupName=workgroup,
            Sql=copy_sql
        )

        # 9. Polling del COPY
        start_time = time.time()
        while True:
            desc = redshift_data.describe_statement(Id=resp['Id'])
            status = desc["Status"]
            
            if status == "FAILED":
                error_msg = desc.get('Error', 'Error desconocido')
                print(f"❌ Error en COPY: {error_msg}")
                
                # 🔍 DIAGNÓSTICO: Consultar sys_load_error_detail para más detalles
                try:
                    error_detail_query = f"""
                        SELECT 
                            filename,
                            line_number,
                            colname,
                            type,
                            col_length,
                            raw_field_value,
                            err_reason
                        FROM sys_load_error_detail 
                        WHERE query = {resp['Id']}
                        ORDER BY line_number, colname;
                    """
                    
                    error_resp = redshift_data.execute_statement(
                        Database=database,
                        WorkgroupName=workgroup,
                        Sql=error_detail_query
                    )
                    
                    error_id = error_resp['Id']
                    while True:
                        error_desc = redshift_data.describe_statement(Id=error_id)
                        if error_desc["Status"] == "FINISHED":
                            if error_desc.get('HasResultSet'):
                                error_result = redshift_data.get_statement_result(Id=error_id)
                                print("🔍 DETALLES DEL ERROR DE CARGA:")
                                for record in error_result.get("Records", []):
                                    filename = record[0].get('stringValue', 'N/A')
                                    line_num = record[1].get('longValue', 'N/A')
                                    colname = record[2].get('stringValue', 'N/A')
                                    error_type = record[3].get('stringValue', 'N/A')
                                    raw_value = record[5].get('stringValue', 'N/A')
                                    reason = record[6].get('stringValue', 'N/A')
                                    print(f"   📄 Archivo: {filename}")
                                    print(f"   📍 Línea: {line_num}, Columna: {colname}")
                                    print(f"   🔍 Tipo: {error_type}")
                                    print(f"   💾 Valor: '{raw_value}'")
                                    print(f"   ❌ Razón: {reason}")
                                    print("   " + "-" * 50)
                            break
                        elif error_desc["Status"] == "FAILED":
                            print(f"⚠️ No se pudieron obtener detalles del error: {error_desc.get('Error')}")
                            break
                        time.sleep(1)
                except Exception as e:
                    print(f"⚠️ Error obteniendo detalles: {str(e)}")
                
                raise Exception(f"COPY failed: {error_msg}")
                
            elif status == "FINISHED":
                duration = time.time() - start_time
                print(f"✅ COPY completado en {duration:.2f}s")
                
                # 🔧 ARREGLO: Esperar a que termine el COMMIT
                commit_stmt = "COMMIT;"
                commit_resp = redshift_data.execute_statement(
                    Database=database,
                    WorkgroupName=workgroup,
                    Sql=commit_stmt
                )
                
                # Esperar a que termine el COMMIT
                commit_id = commit_resp["Id"]
                while True:
                    commit_desc = redshift_data.describe_statement(Id=commit_id)
                    if commit_desc["Status"] == "FINISHED":
                        print("🧾 Commit ejecutado correctamente")
                        break
                    elif commit_desc["Status"] == "FAILED":
                        print(f"❌ Error en COMMIT: {commit_desc.get('Error')}")
                        break
                    time.sleep(2)
                
                # Verificar que se insertaron datos DESPUÉS del COMMIT
                time.sleep(2)  # Pequeña pausa para asegurar consistencia
                
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
                        print(f"📊 VERIFICACIÓN FINAL: La tabla {table_name} tiene {row_count} filas")
                        # Muestra filas de ejemplo para validar persistencia
                        sample_sql = f"SELECT * FROM public.{table_name} LIMIT 3;"
                        sample_resp = redshift_data.execute_statement(Database=database, WorkgroupName=workgroup, Sql=sample_sql)
                        while True:
                            sample_desc = redshift_data.describe_statement(Id=sample_resp['Id'])
                            if sample_desc['Status'] == 'FINISHED':
                                if sample_desc.get('HasResultSet'):
                                    sample_res = redshift_data.get_statement_result(Id=sample_resp['Id'])
                                    print(f"🧪 MUESTRA {table_name}: {sample_res.get('Records', [])}")
                                break
                            elif sample_desc['Status'] == 'FAILED':
                                break
                            time.sleep(2)
                        if row_count > 0:
                            print(f"🎉 ¡COPY EXITOSO! Se insertaron {row_count} filas")
                        else:
                            print(f"⚠️ COPY completado pero la tabla sigue vacía")
                            
                        break
                    time.sleep(2)
                
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
        'nro_ticket': 'Int64',
        'id': 'Int64'  # Para archivos_ingestados
    }
    
    for col, target_type in type_corrections.items():
        if col in df_fixed.columns:
            if target_type == 'Int64':
                # Convertir a Int64 (permite NaN)
                try:
                    df_fixed[col] = pd.to_numeric(df_fixed[col], errors='coerce').astype('Int64')
                    print(f"✅ Corregido {col} a Int64")
                except Exception as e:
                    print(f"⚠️ Error convirtiendo {col} a Int64: {str(e)}")
                    # Mantener como string si falla la conversión
                    df_fixed[col] = df_fixed[col].astype(str)
                    print(f"✅ Mantenido {col} como string")
    
    print(df_fixed)
    print(list(df_fixed.columns))

    # 4. Limpiar strings y manejar valores nulos
    for col in df_fixed.columns:
        if df_fixed[col].dtype == 'object':
            print(df_fixed[col])
            df_fixed[col] = df_fixed[col].astype(str)
            df_fixed[col] = df_fixed[col].replace(['nan', 'NaN', 'None', '<NA>', 'NULL', 'null'], '')
            # df_fixed[col] = df_fixed[col].str.strip()
            
            # 🔧 ARREGLO: Manejar timestamps específicamente
            if col.lower() == 'ins_dttm':
                print(f"🔍 Procesando timestamp {col}: {df_fixed[col].iloc[0] if len(df_fixed) > 0 else 'N/A'}")
                print(f"🔍 Tipo de dato timestamp: {df_fixed[col].dtype}")
                # Asegurar que el formato sea correcto para Redshift
                if len(df_fixed) > 0:
                    sample_value = df_fixed[col].iloc[0]
                    print(f"🔍 Valor de muestra: '{sample_value}' (tipo: {type(sample_value)})")
                    # Validar formato de timestamp
                    if sample_value and sample_value != '':
                        try:
                            # Intentar parsear como datetime para validar formato
                            pd.to_datetime(sample_value)
                            print(f"✅ Timestamp válido: {sample_value}")
                        except Exception as e:
                            print(f"⚠️ Timestamp inválido: {sample_value} - {str(e)}")
                            # Generar timestamp válido si es inválido
                            df_fixed[col] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            print(f"🔧 Timestamp corregido: {df_fixed[col].iloc[0]}")
    
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

def persist_to_redshift_dedup_only(redshift_data, s3_client, df_all_data, table_name, bucket_name, database, workgroup, iam_role):
    """
    Versión que solo usa DELETE (funciona en Redshift Serverless)
    """
    print(f"📊 Total de filas antes de deduplicar: {len(df_all_data)}")
    
    # Eliminar duplicados
    df_dedup = df_all_data.drop_duplicates(subset=['operation_id'], keep='first')
    duplicados_eliminados = len(df_all_data) - len(df_dedup)
    
    if duplicados_eliminados == 0:
        print("✅ No hay duplicados. No se requiere reescritura.")
        return len(df_all_data)

    print(f"🧹 {duplicados_eliminados} duplicados eliminados — {len(df_dedup)} filas únicas")

    # Subir CSV a S3
    csv_buffer = io.StringIO()
    df_dedup.to_csv(csv_buffer, index=False)
    s3_key = f"tmp/{table_name}_{int(time.time())}.csv"
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    s3_path = f"s3://{bucket_name}/{s3_key}"
    print(f"📤 CSV subido a {s3_path}")

    print("🚀 Insertando filas nuevas sin borrar tabla...")

    # Subir CSV deduplicado a S3
    csv_buffer = io.StringIO()
    df_dedup.to_csv(csv_buffer, index=False)
    s3_key = f"tmp/{table_name}_{int(time.time())}.csv"
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    s3_path = f"s3://{bucket_name}/{s3_key}"

    # Cargar el CSV directo a una tabla staging
    staging_table = f"{table_name}_staging"
    create_staging_sql = f"""
        CREATE TEMP TABLE {staging_table} AS 
        SELECT * FROM {table_name} WHERE 1=0;
    """
    redshift_data.execute_statement(
        Database=database,
        WorkgroupName=workgroup,
        Sql=create_staging_sql
    )

    copy_sql = f"""
        COPY {staging_table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        CSV
        IGNOREHEADER 1
        DELIMITER ','
        EMPTYASNULL
        BLANKSASNULL
        TRUNCATECOLUMNS
        MAXERROR 0;
    """
    redshift_data.execute_statement(
        Database=database,
        WorkgroupName=workgroup,
        Sql=copy_sql
    )

    # Insertar solo filas nuevas basadas en operation_id
    merge_sql = f"""
        INSERT INTO {table_name}
        SELECT s.*
        FROM {staging_table} s
        LEFT JOIN {table_name} t
        ON s.operation_id = t.operation_id
        WHERE t.operation_id IS NULL;
    """
    merge_resp = redshift_data.execute_statement(
        Database=database,
        WorkgroupName=workgroup,
        Sql=merge_sql
    )

    merge_id = merge_resp["Id"]

    # Esperar hasta que termine el INSERT SELECT
    while True:
        desc = redshift_data.describe_statement(Id=merge_id)
        status = desc["Status"]
        if status == "FINISHED":
            print("✅ Nuevos registros insertados correctamente sin borrar tabla.")
            break
        elif status == "FAILED":
            error_msg = desc.get("Error", "Error desconocido")
            print(f"❌ Falló el merge_sql: {error_msg}")
            raise Exception(f"INSERT/SELECT failed: {error_msg}")
        else:
            print(f"⏳ Esperando que termine el merge_sql... (estado: {status})")
            time.sleep(2)

    print("✅ Nuevos registros insertados correctamente sin borrar tabla.")
    return len(df_dedup)
         
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
            
            # 🔍 VERIFICACIÓN SIMPLE: Solo verificar si el ticket ya existe
            tickets_del_archivo = df['nro_ticket'].unique().tolist()
            print(f"🔍 Tickets en el archivo: {tickets_del_archivo}")
            
            # Consultar tickets ya procesados (manejo de error si la tabla no existe)
            tickets_existentes = set()
            try:
                check_tickets_query = "SELECT DISTINCT id FROM archivos_ingestados;"
                check_resp = redshift_data.execute_statement(
                    Database='dev',
                    WorkgroupName='pdf-etl-workgroup',
                    Sql=check_tickets_query
                )
                
                while True:
                    desc = redshift_data.describe_statement(Id=check_resp['Id'])
                    if desc["Status"] == "FINISHED":
                        if desc.get('HasResultSet'):
                            result = redshift_data.get_statement_result(Id=check_resp['Id'])
                            tickets_existentes = {
                                int(record[0]['longValue']) 
                                for record in result.get("Records", [])
                                if record and record[0].get('longValue')
                            }
                        break
                    elif desc["Status"] == "FAILED":
                        print(f"⚠️ Tabla archivos_ingestados no existe aún, se creará")
                        break
                    time.sleep(1)
            except Exception as e:
                print(f"⚠️ No se pudo consultar tickets existentes: {str(e)}")
            
            print(f"📦 Tickets ya procesados: {tickets_existentes}")
            
            # Filtrar solo tickets nuevos
            tickets_nuevos = [t for t in tickets_del_archivo if t not in tickets_existentes]
            print(f"🆕 Tickets nuevos a procesar: {tickets_nuevos}")
            
            if not tickets_nuevos:
                print("✅ Todos los tickets ya fueron procesados. No se requiere carga.")
                tables = ['archivos_ingestados', 'dim_producto', 'carrefour_data']
            else:
                # 🔧 VOLVER A LA LÓGICA ORIGINAL QUE FUNCIONABA
                column_defs = [f"{clean_column_name(col)} {redshift_type(dtype)}" for col, dtype in zip(df.columns, df.dtypes)]
                columnas_sql = ",\n  ".join(column_defs)

                print(f'Se lee el pdf {key} convertido en csv en S3 y se mergea a la tabla de carrefour_data')
                flag_exists, tiene_datos = create_redshift_table_from_df(df, columnas_sql, "carrefour_data", redshift_data, 'dev', 'pdf-etl-workgroup', None)
                
                # Cargar datos básicos en carrefour_data (solo tickets nuevos)
                df_nuevos = df[df['nro_ticket'].isin(tickets_nuevos)].copy()
                insert_df_into_redshift_copy_fixed(redshift_data, s3, df_nuevos, "carrefour_data", bucket, 'dev', 'pdf-etl-workgroup', iam_role)
                print("✅ Datos básicos cargados en carrefour_data")
                
                # Crear dim_producto basado en los datos ya persistidos
                df_dim_producto = create_and_fill_product_dim_table_in_redshift(s3, bucket, 'dim_producto/', df_nuevos, 'dim_producto', redshift_data, 'dev', 'pdf-etl-workgroup','product_id', False)
                print("✅ dim_producto actualizado")
                
                # Registrar tickets procesados en archivos_ingestados
                df_uploaded_files = pd.DataFrame(tickets_nuevos, columns=['id'])
                now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                df_uploaded_files['ins_dttm'] = now_str
                print(f"🔍 Timestamp generado: {now_str}")

                column_defs = []
                for col, dtype in zip(df_uploaded_files.columns, df_uploaded_files.dtypes):
                    if col.lower() == 'ins_dttm':
                        column_defs.append(f"{clean_column_name(col)} TIMESTAMP")
                    else:
                        column_defs.append(f"{clean_column_name(col)} {redshift_type(dtype)}")
                column_uploaded_files = ",\n  ".join(column_defs)
                
                flag_exists, tiene_datos = create_redshift_table_from_df(df_uploaded_files, column_uploaded_files, 'archivos_ingestados', redshift_data, 'dev', 'pdf-etl-workgroup', None)
                insert_df_into_redshift_copy_fixed(redshift_data, s3, df_uploaded_files, 'archivos_ingestados', bucket, 'dev', 'pdf-etl-workgroup', iam_role)
                print(f"✅ Registrados {len(df_uploaded_files)} tickets nuevos en archivos_ingestados")

                tables = ['archivos_ingestados', 'dim_producto', 'carrefour_data']  
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

# s3_client = boto3.client('s3')
# bucket_name = 'market-tickets'
# folder = 'processed/'
# response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
# csvs = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

# dtype = {}
# for csv_key in csvs:
#     response = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
#     if csv_key.endswith(".csv"):
#         df = pd.read_csv(io.BytesIO(response['Body'].read()),dtype=dtype)

#     event = {
#         "body": json.dumps({
#             "etl_flow": 'TICKET',
#             "bucket": 'market-tickets',
#             "key": csv_key
#         })
#     }

#     lambda_handler(event,'')
#     exit()

def verify_table_schema_and_data(redshift_data, table_name, database, workgroup):
    """Verifica el esquema y datos de una tabla"""
    
    # Verificar esquema
    schema_query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' AND table_schema = 'public'
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
        if desc["Status"] == "FINISHED":
            result = redshift_data.get_statement_result(Id=schema_id)
            print(f"📋 Esquema de {table_name}:")
            for r in result["Records"]:
                col = r[0]["stringValue"]
                dtype = r[1]["stringValue"]
                nullable = r[2]["stringValue"]
                print(f"   {col}: {dtype} (nullable={nullable})")
            break
        time.sleep(1)
    
    # Verificar datos
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
            print(f"📊 Datos en {table_name}: {row_count} filas")
            break
        time.sleep(1)

def diagnose_redshift_tables():
    """Función de diagnóstico para verificar el estado de las tablas"""
    redshift_data = boto3.client('redshift-data')
    database = "dev"
    workgroup = "pdf-etl-workgroup"
    
    tables_to_check = ["dim_producto", "archivos_ingestados", "carrefour_data"]
    
    for table in tables_to_check:
        print(f"\n🔍 DIAGNÓSTICO DE TABLA: {table}")
        print("=" * 50)
        
        # Verificar si existe
        exists_query = f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = '{table}' AND table_schema = 'public';
        """
        
        exists_resp = redshift_data.execute_statement(
            Database=database,
            WorkgroupName=workgroup,
            Sql=exists_query
        )
        
        exists_id = exists_resp['Id']
        while True:
            desc = redshift_data.describe_statement(Id=exists_id)
            if desc["Status"] == "FINISHED":
                result = redshift_data.get_statement_result(Id=exists_id)
                exists = int(result["Records"][0][0]["longValue"]) > 0
                print(f"✅ Tabla existe: {exists}")
                break
            time.sleep(1)
        
        if exists:
            verify_table_schema_and_data(redshift_data, table, database, workgroup)
        else:
            print(f"❌ La tabla {table} no existe")
            
# s3_client = boto3.client('s3')
# bucket_name = 'market-tickets'
# folder = 'processed/'
# response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
# csvs = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

# dtype = {}
# for csv_key in csvs:
#     response = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
#     if csv_key.endswith(".csv"):
#         df = pd.read_csv(io.BytesIO(response['Body'].read()),dtype=dtype)

#     event = {
#         "body": json.dumps({
#             "etl_flow": 'TICKET',
#             "bucket": 'market-tickets',
#             "key": csv_key
#         })
#     }

#     lambda_handler(event,'')
#     exit()