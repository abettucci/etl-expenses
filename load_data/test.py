import boto3
import time

redshift_data = boto3.client('redshift-data')

# def check_primary_key(redshift_data, database, workgroup, schema, table_name):
#     try:
#         # Consulta para verificar PRIMARY KEY
#         query = """
#         SELECT 
#             kcu.column_name,
#             tc.constraint_name
#         FROM 
#             information_schema.table_constraints tc
#         JOIN 
#             information_schema.key_column_usage kcu
#             ON tc.constraint_name = kcu.constraint_name
#             AND tc.table_schema = kcu.table_schema
#         WHERE 
#             tc.constraint_type = 'PRIMARY KEY'
#             AND tc.table_schema = '{}'
#             AND tc.table_name = '{}';
#         """.format(schema, table_name)
        
#         # Ejecutar consulta
#         response = redshift_data.execute_statement(
#             Database=database,
#             WorkgroupName=workgroup,
#             Sql=query
#         )
        
#         # Esperar y obtener resultados
#         while True:
#             desc = redshift_data.describe_statement(Id=response['Id'])
#             if desc['Status'] == 'FINISHED':
#                 if desc['HasResultSet']:
#                     result = redshift_data.get_statement_result(Id=response['Id'])
#                     if result['Records']:
#                         print(f"PRIMARY KEY encontrado en {schema}.{table_name}:")
#                         for record in result['Records']:
#                             column = record[0]['stringValue']
#                             constraint = record[1]['stringValue']
#                             print(f"- Columna: {column}, Constraint: {constraint}")
#                     else:
#                         print(f"No se encontró PRIMARY KEY en {schema}.{table_name}")
#                 break
#             elif desc['Status'] == 'FAILED':
#                 print("Error al consultar Redshift:", desc['Error'])
#                 break
#             time.sleep(1)  # Esperar 1 segundo antes de verificar nuevamente
            
#     except Exception as e:
#         print(f"Error: {e}")

# # Configuración inicial
# redshift_data = boto3.client('redshift-data')
# DATABASE = 'dev'
# WORKGROUP = 'pdf-etl-workgroup'
# SCHEMA = 'public'
# TABLE_NAME = 'carrefour_data'

# # Ejecutar la función
# check_primary_key(redshift_data, DATABASE, WORKGROUP, SCHEMA, TABLE_NAME)

# redshift_data.execute_statement(
#     Database='dev',
#     WorkgroupName='pdf-etl-workgroup',
#     Sql="DROP TABLE bank_payments"
# )     

# redshift_data.execute_statement(
#     Database='dev',
#     WorkgroupName='pdf-etl-workgroup',
#     Sql="TRUNCATE TABLE carrefour_data"
# )     

# query = """
# SELECT column_name, data_type
# FROM information_schema.columns
# WHERE table_name = 'mp_data';
# """     

# query = """
# SELECT *
# FROM archivos_ingestados
# """

# # Ejecutar consulta
# response = redshift_data.execute_statement(
#     Database='dev',
#     WorkgroupName='pdf-etl-workgroup',
#     Sql=query
# )

# while True:
#     desc = redshift_data.describe_statement(Id=response['Id'])
#     if desc['Status'] == 'FINISHED':
#         if desc['HasResultSet']:
#             result = redshift_data.get_statement_result(Id=response['Id'])     
#             print(result['Records'])
#         break
#     elif desc['Status'] == 'FAILED':
#         print("Error al consultar Redshift:", desc['Error'])
#         break

# df.to_csv('/tmp/data.csv', index=False)
# s3.upload_file('/tmp/data.csv', 'my-bucket', 'temp/data.csv')

# sql = """
# COPY carrefour_data
# FROM 's3://my-bucket/temp/data.csv'
# IAM_ROLE 'arn:aws:iam::123456789:role/redshift-access-role'
# FORMAT AS CSV
# IGNOREHEADER 1;
# """
# redshift_data.execute_statement(Database='dev', WorkgroupName='...', Sql=sql)


# values_sql = ",\n".join([
#     f"({', '.join(format_value(v) for v in row)})"
#     for row in df.itertuples(index=False, name=None)
# ])

# merge_sql = f"""
# MERGE INTO carrefour_data AS target
# USING (
#     VALUES 
#         {values_sql}
# ) AS source(nro_ticket, fecha, categoria, producto, cantidad, peso, precio_unit, monto_total, total_ticket_bruto, total_ticket_meli)
# ON target.nro_ticket = source.nro_ticket
# WHEN NOT MATCHED THEN
#   INSERT VALUES (
#     source.nro_ticket, source.fecha, source.categoria, source.producto,
#     source.cantidad, source.peso, source.precio_unit, source.monto_total,
#     source.total_ticket_bruto, source.total_ticket_meli
#   );
# """