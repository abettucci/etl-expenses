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
    return re.sub(r'[^a-z0-9]+', '_', text.lower()).strip('_')

def create_and_fill_product_dim_table_in_redshift(s3, bucket, folder, df, table_name, redshift_data, database, workgroup, pk, flag_check_ids_repetidos):
    # Primero vemos si ya existe la tabla 'dim_producto', si no existe la creamos con pk = 'product_id'   
    columnas_sql = f"""
        nombre_producto TEXT,
        product_id INTEGER,
        ean TEXT,
        grupo_producto TEXT
    """

    if flag_check_ids_repetidos == False:
        flag_exists, tiene_datos = create_redshift_table_from_df(df, columnas_sql, table_name, redshift_data, database, workgroup, pk)
    else:
        flag_exists, tiene_datos = False, False

    # Luego cargamos los datos en la tabla dim_producto
    # Si la tabla no existia, leemos todas las filas de las columnas de nombre_producto y EAN de la tabla de redshift de carrefour_data
    # y creamos el diccionario inicial que va a insertarse en la tabla dim_producto
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
                    if result['Records'] == []:
                        pass
                        print('Tabla vacia, se cargan todos los gastos')
                    else:                    
                        for record in result['Records']:
                            nombre_producto = record[0].get('stringValue', None)
                            ean = (
                                record[1].get('stringValue') or
                                record[1].get('doubleValue') or
                                record[1].get('longValue') or
                                None
                            )
                            if ean is not None:
                                ean = str(ean).split('.')[0]

                            tupla = (nombre_producto, ean)
                            set_nombre_producto_ean.add(tupla)
                break
            elif desc['Status'] == 'FAILED':
                print("Error al consultar Redshift:", desc['Error'])
                break
        
        normalizacion_productos = {}
        df_dim_producto = pd.DataFrame(list(set_nombre_producto_ean), columns=["nombre_producto", "ean"])
        nombres_unicos = df_dim_producto['nombre_producto'].unique().tolist()
        normalizacion_productos.update(generar_diccionario_normalizacion(nombres_unicos))
        df_dim_producto['grupo_producto'] = df_dim_producto['nombre_producto'].map(normalizacion_productos)

        # ver si no lo modifico por una unica funcion que se usa en ambas condiciones del if
        df_dim_producto["product_id"] = pd.factorize(df_dim_producto["nombre_producto"].astype(str) + "_" + df_dim_producto["ean"].astype(str))[0] + 1
        df_dim_producto = df_dim_producto.drop_duplicates(subset=['nombre_producto','ean'])

        # insert_df_into_redshift_copy(df_dim_producto, columnas_sql, table_name, redshift_data, database, workgroup, '', '')
        insert_df_into_redshift_copy_fixed(redshift_data, s3, df_dim_producto, table_name, bucket, 'dev', 'pdf-etl-workgroup', iam_role)
        print(f"✅ Cargamos los primeros datos en la tabla {table_name}")

        return df_dim_producto

    # Si la tabla dim_producto ya existe, vemos si el nombre_producto de input del nuevo archivo a ingestar ya existe en la tabla y en caso de no
    # estar, lo agregamos.
    else: # flag_exists == True y no es vacia
        query_distinct_product_id = f"""
            SELECT DISTINCT nombre_producto, ean
            FROM dim_producto;
        """

        response = redshift_data.execute_statement(
            Database=database,
            WorkgroupName=workgroup,
            Sql=query_distinct_product_id
        )

        set_nombre_producto_ean = set()
        while True:
            desc = redshift_data.describe_statement(Id=response['Id'])
            if desc['Status'] == 'FINISHED':
                if desc['HasResultSet']:
                    result = redshift_data.get_statement_result(Id=response['Id'])     
                    if result['Records'] == []:
                        pass
                        print('Tabla vacia, se cargan todos los gastos')
                    else:                
                        for record in result['Records']:
                            nombre_producto = record[0].get('stringValue', None)
                            ean = (
                                record[1].get('stringValue') or
                                record[1].get('doubleValue') or
                                record[1].get('longValue') or
                                None # o usar np.NaN pero implica importar Numpy solo para esto
                            )
                            if ean is not None:
                                ean = str(ean).split('.')[0]

                            tupla = (nombre_producto, ean)
                            set_nombre_producto_ean.add(tupla)
                break
            elif desc['Status'] == 'FAILED':
                print("Error al consultar Redshift:", desc['Error'])
                break
        
        df['ean'] = df['ean'].where(pd.notna(df['ean']), None)
        df['existe_en_set'] = [t in set_nombre_producto_ean for t in zip(df['producto'], df['ean'])]
        df = df.loc[df['existe_en_set']==False]
        df = df.drop_duplicates(subset=['producto','ean'])

        cont = 0
        for producto in list(df['producto'].unique()):
            if cont == 0:
                product_id = len(set_nombre_producto_ean) + 1
                cont += 1
            else:
                product_id += 1

            normalizacion_productos = {}
            ean = df.loc[df['producto']==producto]['ean'].unique()[0]
            data = [producto, product_id, ean]
            df_dim_producto = pd.DataFrame([data], columns=["nombre_producto", "product_id", "ean"])
            nombres_unicos = df_dim_producto['nombre_producto'].unique().tolist()
            normalizacion_productos.update(generar_diccionario_normalizacion(nombres_unicos))
            df_dim_producto['grupo_producto'] = df_dim_producto['nombre_producto'].map(normalizacion_productos)

            # insert_df_into_redshift_copy(df_dim_producto, columnas_sql, table_name, redshift_data, database, workgroup, '', '')
            insert_df_into_redshift_copy_fixed(redshift_data, s3, df_dim_producto, table_name, bucket, 'dev', 'pdf-etl-workgroup', iam_role)
            print(f"✅ Cargamos un nuevo registro de producto {df_dim_producto[['nombre_producto','product_id','ean']]} en la tabla {table_name}")
            
            return df_dim_producto

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
        SET operation_id = CONCAT(CONCAT(CONCAT(CAST(nro_ticket AS VARCHAR), '_'),CONCAT(CONCAT(CAST(product_id AS VARCHAR), '_'),CONCAT(CAST(TRUNC(monto_total,2) AS VARCHAR), '_'))),CAST(fecha AS VARCHAR));
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

# def insert_df_into_redshift(df, columnas_sql, table_name, redshift_data, database, workgroup, report_id=None, report_date=None):
#     if isinstance(columnas_sql, list):
#         columnas_sql = ", ".join(columnas_sql)

#     if table_name == 'mp_data':
#         id_col = 'report_id'
#         df[id_col] = df[id_col].iloc[0] if 'report_id' in df.columns else report_id
#         df['report_date'] = report_date

#         if column_exists(table_name, id_col, redshift_data, database, workgroup) is True and\
#             column_has_data(id_col, table_name, redshift_data, database, workgroup) is True:
            
#             # Check de columna operation_id aca, si no existe hacemos un pass, si existe hacemos return 
#             query_distinct_col_ids = f"""
#                 SELECT DISTINCT {id_col}
#                 FROM {table_name}
#             """

#             response = redshift_data.execute_statement(
#                 Database=database,
#                 WorkgroupName=workgroup,
#                 Sql=query_distinct_col_ids
#             )

#             set_operaciones_cargadas = set()
#             while True:
#                 desc = redshift_data.describe_statement(Id=response['Id'])
#                 if desc['Status'] == 'FINISHED':
#                     if desc.get('HasResultSet', False):
#                         result = redshift_data.get_statement_result(Id=response['Id'])     
#                         if result['Records'] == []:
#                             print('Tabla vacía, se carga la fila')
#                         else:                    
#                             set_ids_cargados = {
#                                 list(row[0].values())[0] for row in result['Records']
#                             }

#                             # Filtramos los id que ya fueron cargados en la tabla
#                             df = df[~df[id_col].isin(set_ids_cargados)]
#                     else:
#                         # No hay resultados, la tabla está vacía
#                         set_ids_cargados = set()
#                         print('Tabla vacía, se carga la fila')
#                     break
#                 elif desc['Status'] == 'FAILED':
#                     print("Error al consultar Redshift:", desc['Error'])
#                     break
#     elif table_name == 'carrefour_data':
#         df['operation_id'] = None
#         id_col = 'operation_id'
#         if column_exists(table_name, id_col, redshift_data, database, workgroup) is True and\
#             column_has_data(id_col, table_name, redshift_data, database, workgroup) is True:
            
#             # Check de columna operation_id aca, si no existe hacemos un pass, si existe hacemos return 
#             query_distinct_operation_ids = """
#                 SELECT DISTINCT operation_id
#                 FROM carrefour_data
#             """

#             response = redshift_data.execute_statement(
#                 Database=database,
#                 WorkgroupName=workgroup,
#                 Sql=query_distinct_operation_ids
#             )

#             set_operaciones_cargadas = set()
#             while True:
#                 desc = redshift_data.describe_statement(Id=response['Id'])
#                 if desc['Status'] == 'FINISHED':
#                     if desc.get('HasResultSet', False):
#                         result = redshift_data.get_statement_result(Id=response['Id'])     
#                         if result['Records'] == []:
#                             print('Tabla vacía, se carga la fila')
#                         else:                    
#                             set_operaciones_cargadas = {
#                                 list(row[0].values())[0] for row in result['Records']
#                             }

#                             print('\n')
#                             print(set_operaciones_cargadas)

#                             # Completamos el valor de operation_id e Insertamos las filas del df que solo sean operaciones nuevas
#                             # Para obtener el product_id en el dataframe de input, lo buscamos en el df de df_dim_producto que viene en el return
#                             # al crear la tabla y rellenarla en Redshift
#                             df_dim_producto = create_and_fill_product_dim_table_in_redshift(df, 'dim_producto', redshift_data, database, workgroup, 'product_id', True)

#                             df = df.merge(
#                                 df_dim_producto[['nombre_producto', 'product_id']],
#                                 left_on='producto',
#                                 right_on='nombre_producto',
#                                 how='left'
#                             )

#                             df.drop('product_id_x', axis=1, inplace=True)   
#                             df.rename(columns={'product_id_y': 'product_id'}, inplace=True)

#                             df['operation_id'] = df['nro_ticket'].astype(str) + '_' + df['product_id'].astype(str) + '_' \
#                                 + df['monto_total'].astype(str) + '_' + df['fecha'].astype(str)

#                             # Filtramos los operation_id que ya fueron cargados en la tabla
#                             df = df[~df['operation_id'].isin(set_operaciones_cargadas)]
#                     else:
#                         # No hay resultados, la tabla está vacía
#                         set_operaciones_cargadas = set()
#                         print('Tabla vacía, se carga la fila')
#                     break
#                 elif desc['Status'] == 'FAILED':
#                     print("Error al consultar Redshift:", desc['Error'])
#                     break
#     elif table_name == 'archivos_ingestados':
#         id_col = 'id'        
#         if column_exists(table_name, 'id', redshift_data, database, workgroup) is True and\
#             column_has_data('id', table_name, redshift_data, database, workgroup) is True:

#             # Aca hacemos el check de si ya fue ingestado el archivo, en caso de que no, hacemos pass, en caso de que si hacemos return
#             # Check de columna operation_id aca, si no existe hacemos un pass, si existe hacemos return 
#             query_distinct_nro_ticket = """
#                 SELECT DISTINCT id
#                 FROM archivos_ingestados
#             """

#             response = redshift_data.execute_statement(
#                 Database=database,
#                 WorkgroupName=workgroup,
#                 Sql=query_distinct_nro_ticket
#             )

#             set_tickets_cargados = set()
#             while True:
#                 desc = redshift_data.describe_statement(Id=response['Id'])
#                 if desc['Status'] == 'FINISHED':
#                     if desc['HasResultSet']:
#                         result = redshift_data.get_statement_result(Id=response['Id'])     
#                         if result['Records'] == []:
#                             pass
#                             print('Tabla vacia, se carga la fila')
#                         else:                    
#                             set_tickets_cargados = {list(row[0].values())[0] for row in result['Records']}

#                             # Insertamos las filas del df que solo sean nro_ticket nuevos
#                             df = df[~df['id'].isin(set_tickets_cargados)]
#                     break
#                 elif desc['Status'] == 'FAILED':
#                     print("Error al consultar Redshift:", desc['Error'])
#                     break

#     elif table_name == 'bank_payments':
#         id_col = 'id'
#         if column_exists(table_name, id_col, redshift_data, database, workgroup) is True and\
#             column_has_data(id_col, table_name, redshift_data, database, workgroup) is True:
            
#             # Check de columna operation_id aca, si no existe hacemos un pass, si existe hacemos return 
#             query_distinct_col_ids = f"""
#                 SELECT DISTINCT {id_col}
#                 FROM {table_name}
#             """

#             response = redshift_data.execute_statement(
#                 Database=database,
#                 WorkgroupName=workgroup,
#                 Sql=query_distinct_col_ids
#             )

#             set_operaciones_cargadas = set()
#             while True:
#                 desc = redshift_data.describe_statement(Id=response['Id'])
#                 if desc['Status'] == 'FINISHED':
#                     if desc.get('HasResultSet', False):
#                         result = redshift_data.get_statement_result(Id=response['Id'])     
#                         if result['Records'] == []:
#                             print('Tabla vacía, se carga la fila')
#                         else:                    
#                             set_ids_cargados = {
#                                 list(row[0].values())[0] for row in result['Records']
#                             }

#                             # Filtramos los id que ya fueron cargados en la tabla
#                             df = df[~df[id_col].isin(set_ids_cargados)]
#                     else:
#                         # No hay resultados, la tabla está vacía
#                         set_ids_cargados = set()
#                         print('Tabla vacía, se carga la fila')
#                     break
#                 elif desc['Status'] == 'FAILED':
#                     print("Error al consultar Redshift:", desc['Error'])
#                     break
#     else: # dim_producto
#         pass  
    
#     print('df_empty? ', df.empty)

#     print(df)

#     if df.empty == True:
#         print(f"⚠️ No se insertaron filas en {table_name}, todos los registros ya existen")
#         return None
#     else:
#         # Nombre de columnas (incluso si hay extra como report_id, report_date)
#         df_columns = df.columns.tolist()
#         columns_formatted = ", ".join(df_columns)

#         # Generar VALUES en batch
#         values_sql = ",\n".join([
#             f"({', '.join(format_value(v) for v in row)})"
#             for row in df.itertuples(index=False, name=None)
#         ])

#         insert_stmt = f"""
#             INSERT INTO {table_name} ({columns_formatted})
#             VALUES {values_sql};
#         """ 

#         try:
#             resp = redshift_data.execute_statement(
#                 Database=database,
#                 WorkgroupName=workgroup,
#                 Sql=insert_stmt
#             )

#             while True:
#                 desc = redshift_data.describe_statement(Id=resp['Id'])
#                 if desc['Status'] == 'FAILED':
#                     print(f"❌ Error insertando en {table_name}: {desc['Error']}")
#                     break
#                 elif desc['Status'] == 'FINISHED':
#                     print(f"✅ Insert completado en {table_name}")
#                     break
#                 else:
#                     time.sleep(1)
            
#         except Exception as e:
#             print(f"❌ Error insertando fila en tabla: {str(e)}")

def insert_df_into_redshift_copy_fixed(redshift_data, s3_client, df, table_name, bucket_name, database, workgroup, iam_role):
    """
    Versión corregida de la función COPY que maneja columnas faltantes
    """
    try:
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
            # insert_df_into_redshift_copy(df, columnas_sql_insert, table_name, 'dev', 'pdf-etl-workgroup', report_id, report_date)
            insert_df_into_redshift_copy_fixed(redshift_data, s3, df, table_name, bucket, 'dev', 'pdf-etl-workgroup', iam_role)

        elif etl_flow == 'TICKET':
            report_id, report_date = '', ''
            column_defs = [f"{clean_column_name(col)} {redshift_type(dtype)}" for col, dtype in zip(df.columns, df.dtypes)]
            column_defs += ["operation_id VARCHAR(255)"]
            columnas_sql = ",\n  ".join(column_defs)

            print(f'Se lee el pdf {key} convertido en csv en S3 y se mergea a la tabla de {table_name}')
            flag_exists, tiene_datos = create_redshift_table_from_df(df, columnas_sql, table_name, redshift_data, 'dev', 'pdf-etl-workgroup', 'nro_ticket')

            # column_names_insert = [clean_column_name(col) for col in df.columns]
            # insert_df_into_redshift(df, '', table_name, redshift_data, 'dev', 'pdf-etl-workgroup', '', '')

            # Mostrar diagnóstico inicial
            print(f"🔍 DIAGNÓSTICO INICIAL:")
            print(f"DataFrame original: {df.shape[0]} filas, {df.shape[1]} columnas")
            print(f"Columnas: {list(df.columns)}")

            # 🔥 USAR LA NUEVA FUNCIÓN CORREGIDA
            insert_df_into_redshift_copy_fixed(redshift_data, s3, df, table_name, bucket, 'dev', 'pdf-etl-workgroup', iam_role)

            # Cargamos el valor de nro_ticket a la tabla de archivos ingestados para no duplicar datos en una proxima carga
            # estandarizar nombre columna "id", "fecha_insert", "fecha_update" donde id para carrefour va a ser nro_ticket, para mp va a ser report_id
            # y para bank_payments va a ser id.
            data = df['nro_ticket'].unique().tolist()
            df_uploaded_files = pd.DataFrame(data, columns=['id'])
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df_uploaded_files['INS_DTTM'] = now
            column_defs = [f"{clean_column_name(col)} {redshift_type(dtype)}" for col, dtype in zip(df_uploaded_files.columns, df_uploaded_files.dtypes)]
            column_uploaded_files = ",\n  ".join(column_defs)
            flag_exists, tiene_datos = create_redshift_table_from_df(df_uploaded_files, column_uploaded_files, 'archivos_ingestados', redshift_data, 'dev', 'pdf-etl-workgroup', 'id')
            
            # insert_df_into_redshift_copy(df_uploaded_files, column_uploaded_files, 'archivos_ingestados', redshift_data, 'dev', 'pdf-etl-workgroup', '', '') 
            insert_df_into_redshift_copy_fixed(redshift_data, s3, df, table_name, bucket, 'dev', 'pdf-etl-workgroup', iam_role)

            # Crear tabla de dimensiones de producto o utilizarla si ya existe
            df_dim_producto = create_and_fill_product_dim_table_in_redshift(s3, bucket, 'dim_producto/', df, 'dim_producto', redshift_data, 'dev', 'pdf-etl-workgroup','product_id', False)

            # Una vez cargados los nuevos datos, ahi ejecutamos el join con la tabla de dim_producto para completar el valor de product_id,
            # grupo_producto y otros.
            # Agregamos la columna nueva que creamos "operation_id" a la tabla de carrefour_data 
            # para evitar insertar registros repetidos de cada archivo => deberiamos hacer un check de esta
            # columna dentro del insert_df_into_redshift que se hace en carrefour_data
            aggregate_table_in_redshift(table_name, redshift_data, 'dev', 'pdf-etl-workgroup')
            add_concatenated_column(table_name, redshift_data, 'dev', 'pdf-etl-workgroup')           
        else: # es un gasto del banco
            column_defs = [f"{clean_column_name(col)} {redshift_type(dtype)}" for col, dtype in zip(df.columns, df.dtypes)]
            columnas_sql = ",\n  ".join(column_defs)
            
            print(f'Se lee el mail {key} convertido en csv en S3 y se mergea a la tabla de {table_name}')
            create_redshift_table_from_df(df, columnas_sql, table_name, redshift_data, 'dev', 'pdf-etl-workgroup', 'id')

            column_names_insert = [clean_column_name(col) for col in df.columns]
            # insert_df_into_redshift_copy(df, column_names_insert, table_name, redshift_data, 'dev', 'pdf-etl-workgroup', '', '')
            insert_df_into_redshift_copy_fixed(redshift_data, s3, df, table_name, bucket, 'dev', 'pdf-etl-workgroup', iam_role)

        return {
            'table_name': table_name
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