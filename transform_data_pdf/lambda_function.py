import boto3
import io
import pdfplumber
import json
import pandas as pd
import hashlib
from PyPDF2 import PdfReader

def calcular_hash_pdf(content_bytes):
    return hashlib.sha256(content_bytes).hexdigest()

def hash_ya_procesado(hash_pdf, redshift_data):
    check_query = f"""
    SELECT 1 FROM archivos_ingestados 
    WHERE hash_archivo = '{hash_pdf}'
    LIMIT 1
    """
    check_response = redshift_data.execute_statement(
        Database='dev',
        WorkgroupName='pdf-etl-workgroup',
        Sql=check_query
    )

    return check_response

def parsear_pdf_a_rows(pdfs, s3, bucket, redshift_data):
    for pdf_key in pdfs:
        print('Nombre archivo leido: ', pdf_key)
        print(f"📄 Procesando: {pdf_key}")
        pdf_obj = s3.get_object(Bucket=bucket, Key=pdf_key)
        pdf_content = pdf_obj['Body'].read()

        hash_pdf = calcular_hash_pdf(pdf_content)
        # check_response = hash_ya_procesado(hash_pdf, redshift_data)
        # describe_response = redshift_data.describe_statement(Id=check_response['Id'])

        # while describe_response['Status'] not in ['FINISHED', 'FAILED']:
        #     time.sleep(1)
        #     describe_response = redshift_data.describe_statement(Id=check_response['Id'])
        
        # if describe_response['Status'] == 'FINISHED' and describe_response['HasResultSet']:
        #     # El archivo ya existe, salir
        #     print(f"Archivo {pdf_key} ya fue procesado (hash: {hash_pdf})")
        #     return
    
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            pdf_reader = PdfReader(io.BytesIO(pdf_content))
            texto_completo = "".join([p.extract_text() for p in pdf_reader.pages])
            for pagina in pdf_reader.pages:
                texto_pagina = pagina.extract_text()
                texto_completo += texto_pagina

            # Dividir el texto completo en líneas
            lineas = [linea.replace('\xa0', ' ').replace('\xad', '').strip() for linea in texto_completo.split('\n')]

            indice_fecha, indice_inicial, indice_final, indice_nro_ticket, indice_descuentos, suma_total_descuentos  = 0, 0, 0, 0, 0, 0

            for i, linea in enumerate(lineas):
                if "Fecha" in linea and indice_fecha == 0:
                    indice_fecha = i
                if "Caja" in linea and indice_inicial == 0:
                    indice_inicial = i
                if "TOTAL" in linea and indice_final == 0:
                    indice_final = i
                if "P.V." in linea and indice_nro_ticket == 0:
                    indice_nro_ticket = i    
                if "AHORRO" in linea and indice_descuentos == 0:
                    indice_descuentos = i

            if indice_fecha > 0:
                fecha_compra = lineas[indice_fecha][len('Fecha '):lineas[indice_fecha].find('Hora')].strip()
            
            if indice_nro_ticket > 0:
                nro_ticket = lineas[indice_nro_ticket][lineas[indice_nro_ticket].find('Nro T.') + len('Nro T.'):].strip()
                
            if indice_descuentos > 0:
                suma_total_descuentos = lineas[indice_descuentos]
                suma_total_descuentos = suma_total_descuentos[suma_total_descuentos.find('$')+1:].strip()
                suma_total_descuentos = float(suma_total_descuentos.replace(',', '.'))

            # Quedarse solo con los elementos que vienen después de ese índice
            if indice_inicial is not None and indice_final is not None:
                texto_final = lineas[indice_inicial + 2:indice_final+1]
            else:
                texto_final = []

            texto_final = [valor for valor in texto_final if valor != '']
            # texto_final.remove("================================================")

            lista_items = []
            categorias = ['Bebidas','Carniceria','Almacen','Frutas Y Verduras','Limpieza','Perfumeria','Hogar Bazar','Perfumeria'] # ir agregando categorias a medida que compre

            for linea in texto_final:

                if linea in categorias:
                    categoria = linea
                
                elif linea[0].isnumeric() and linea[2:3].lower() == 'x' and linea[4:5].isnumeric():
                    peso_item = 0
                    cantidad_item = 0

                    indice_fila_de_precio = texto_final.index(linea)
                    cant_precio_item = linea[:linea.find("(")].strip() # hasta el IVA que es (21.00%)

                    if cant_precio_item.count('x') == 1:
                        cantidad_item = cant_precio_item[:cant_precio_item.find("x")].strip()
                        precio_item = cant_precio_item[cant_precio_item.find("x")+2:].strip()

                    if cant_precio_item.count('x') > 1: # es carne o algo por peso
                        cant_precio_item_split = cant_precio_item.split('x')[1:]  #hasta la primera X siempre es "1x", entonces tomamos desde la segunda x    
                        peso_item = cant_precio_item_split[0].strip() 
                        precio_item = cant_precio_item_split[1].strip()

                    monto_total = linea[linea.find("(")+8:].strip()
                    monto_total = monto_total[monto_total.find(']')+1:].strip()

                    item_compra = [categoria, nombre_item, cantidad_item, peso_item, precio_item, monto_total]
                    lista_items.append(item_compra)

                else: # el segundo elemento es el producto generalmente, el primero la categoria, el tercero el precio y el cuarto el descuento si hay
                    nombre_item = linea

            # Definimos dataframe de producto, peso, cantidad, precio_unit y monto total item
            data = []
            for item in lista_items:
                fila = {
                    "nro_ticket" : nro_ticket,
                    "fecha" : fecha_compra,
                    "categ" : item[0],
                    "prod": item[1],
                    "cant": item[2],
                    "peso": float(item[3]),  # Convertir el precio a float
                    "p_unit": float(item[4].replace(',', '.')),   # Convertir la cantidad a entero
                    "p_total" : float(item[5])
                }   
                data.append(fila)

            # Crear el DataFrame a partir de la lista de diccionarios
            df = pd.DataFrame(data)

            df['total_ticket_bruto'] = round(df['p_total'].sum() - suma_total_descuentos, 0)
            df['total_ticket_meli'] = round((df['p_total'].sum() - suma_total_descuentos)*0.3, 0)
            
            # Nombre y ruta del archivo Excel
            monto_total_ticket = str(df['total_ticket_meli'].iloc[0])
            
            if '.' in monto_total_ticket:
                monto_total_ticket = monto_total_ticket[:monto_total_ticket.find('.')]

        try:
            print('Se convierte el pdf a df y se carga como csv a S3 en la carpeta de processed')
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            s3 = boto3.client("s3")
            destination_folder = 'processed/' 
            filename = pdf_key.split('/')[-1]
            new_key = destination_folder + filename

            s3.put_object(
                Bucket=bucket,
                Key=new_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )

            # Eliminamos el archivo SOLO si ya se subio a la otra carpeta como csv
            s3.delete_object(Bucket=bucket, Key=pdf_key)
            
            print(f"PDF movido con exito: {pdf_key} -> {new_key}")
        except Exception as e:
                print(f"Error al mover {pdf_key}: {str(e)}")

    return new_key
    
def transform_pdfs_data():
    # Conexion a  S3 y PostgreSQL config
    bucket = 'market-tickets'
    prefix = 'raw/'
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    pdfs = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.pdf')]

    # Conexion a Redshift
    redshift_data = boto3.client('redshift-data')

    sql = f"""
        CREATE TABLE IF NOT EXISTS archivos_ingestados (
        hash_archivo TEXT PRIMARY KEY,
        fecha_ingesta TIMESTAMP DEFAULT GETDATE()
        );
    """

    redshift_data.execute_statement(
        Database='dev',
        WorkgroupName='pdf-etl-workgroup',
        Sql=sql
    )

    new_key = parsear_pdf_a_rows(pdfs, s3, bucket, redshift_data)

    return new_key

def lambda_handler(event,context):
    try:
        key = transform_pdfs_data()
        return {
            "statusCode": 200,
            "body": {
                "etl_flow": 'TICKET',
                "bucket": 'market-tickets',
                "key": key
            }
        }
    except Exception as e:
        print("⚠️ Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }