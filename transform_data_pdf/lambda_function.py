import boto3
import io
import pdfplumber
import json
import pandas as pd
import hashlib
from PyPDF2 import PdfReader

def calcular_hash_pdf(content_bytes):
    return hashlib.sha256(content_bytes).hexdigest()

def transform_pdf_to_dataframe(pdf_content, pdf_key):
    try:
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            texto_completo = ""
            pdf_reader = PdfReader(io.BytesIO(pdf_content))
            
            for pagina in pdf_reader.pages:
                print(pagina)

                try:
                    texto_pagina = pagina.extract_text()
                    if texto_pagina:
                        texto_completo += texto_pagina + "\n"
                except:
                    continue
            
            if not texto_completo:
                print(f"⚠️ No se pudo extraer texto del PDF: {pdf_key}")
                return pd.DataFrame()

            lineas = [linea.replace('\xa0', ' ').replace('\xad', '').strip() 
                     for linea in texto_completo.split('\n') if linea.strip()]

            # Inicializar variables
            fecha_compra = nro_ticket = suma_total_descuentos = ""
            indice_fecha = indice_inicial = indice_final = indice_nro_ticket = indice_descuentos = None

            for i, linea in enumerate(lineas):
                if "Fecha" in linea and indice_fecha is None:
                    indice_fecha = i
                if "Caja" in linea and indice_inicial is None:
                    indice_inicial = i
                if "TOTAL" in linea and indice_final is None:
                    indice_final = i
                if "P.V." in linea and indice_nro_ticket is None:
                    indice_nro_ticket = i    
                if "AHORRO" in linea and indice_descuentos is None:
                    indice_descuentos = i

            # Extraer datos
            if indice_fecha is not None:
                fecha_linea = lineas[indice_fecha]
                fecha_compra = fecha_linea[len('Fecha '):fecha_linea.find('Hora')].strip() if 'Hora' in fecha_linea else ""
            
            if indice_nro_ticket is not None:
                nro_linea = lineas[indice_nro_ticket]
                nro_ticket = nro_linea[nro_linea.find('Nro T.') + len('Nro T.'):].strip() if 'Nro T.' in nro_linea else ""
                
            if indice_descuentos is not None:
                descuento_linea = lineas[indice_descuentos]
                if '$' in descuento_linea:
                    suma_total_descuentos = descuento_linea[descuento_linea.find('$')+1:].strip()
                    try:
                        suma_total_descuentos = float(suma_total_descuentos.replace(',', '.'))
                    except:
                        suma_total_descuentos = 0

            # Procesar items
            lista_items = []
            categorias = ['Bebidas','Carniceria','Almacen','Frutas Y Verduras','Limpieza','Perfumeria','Hogar Bazar']
            categoria_actual = ""
            nombre_item = ""

            if indice_inicial is not None and indice_final is not None:
                lineas_items = lineas[indice_inicial+1:indice_final]
            else:
                lineas_items = []

            for linea in lineas_items:
                if linea in categorias:
                    categoria_actual = linea
                elif any(c in linea for c in ['x', '$']) and any(c.isdigit() for c in linea):
                    # Procesar línea de item
                    try:
                        partes = linea.split()
                        cantidad = peso = precio = monto_total = 0
                        
                        if 'x' in linea:
                            if linea.count('x') == 1:
                                cantidad, precio = linea.split('x')
                                cantidad = float(cantidad.strip())
                                precio = float(precio.split()[0].replace(',', '.'))
                            else:
                                partes = linea.split('x')
                                peso = float(partes[1].strip())
                                precio = float(partes[2].split()[0].replace(',', '.'))
                        
                        if '(' in linea and ')' in linea:
                            monto_total = linea[linea.rfind(')')+1:].strip()
                            monto_total = float(monto_total.replace(',', '.'))
                        
                        item = {
                            "categoria": categoria_actual,
                            "producto": nombre_item,
                            "cantidad": cantidad,
                            "peso": peso,
                            "precio_unit": precio,
                            "monto_total": monto_total
                        }
                        lista_items.append(item)
                    except Exception as e:
                        print(f"Error procesando línea: {linea} - {str(e)}")
                else:
                    nombre_item = linea

            # Crear DataFrame
            if lista_items:
                df = pd.DataFrame(lista_items)
                df['nro_ticket'] = nro_ticket
                df['fecha'] = fecha_compra
                
                if not df.empty and 'monto_total' in df.columns:
                    total_bruto = df['monto_total'].sum() - suma_total_descuentos
                    df['total_ticket_bruto'] = round(total_bruto, 2)
                    df['total_ticket_meli'] = round(total_bruto * 0.3, 2)
                
                return df
            return pd.DataFrame()

    except Exception as e:
        print(f"❌ Error procesando PDF {pdf_key}: {str(e)}")
        return pd.DataFrame()

def process_pdf_file(s3, bucket, pdf_key):
    try:
        print(f"📄 Procesando: {pdf_key}")
        pdf_obj = s3.get_object(Bucket=bucket, Key=pdf_key)
        pdf_content = pdf_obj['Body'].read()

        if not pdf_content.startswith(b'%PDF'):
            print(f"⚠️ El archivo {pdf_key} no es un PDF válido")
            return False

        df = transform_pdf_to_dataframe(pdf_content, pdf_key)
        
        if df.empty:
            print(f"⚠️ No se pudo extraer datos del PDF: {pdf_key}")
            return False

        # Guardar CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, sep=',', encoding='utf-8')
        
        csv_key = pdf_key.replace('raw/', 'processed/').replace('.pdf', '.csv')
        s3.put_object(
            Bucket=bucket,
            Key=csv_key,
            Body=csv_buffer.getvalue()
        )
        
        print(f"✅ CSV generado: {csv_key}")
        return True

    except Exception as e:
        print(f"❌ Error procesando {pdf_key}: {str(e)}")
        return False

def transform_mp_report_data():    
    s3 = boto3.client('s3')
    bucket = 'market-tickets'
    
    # Listar solo archivos PDF (excluyendo directorios)
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix='raw/',
        Delimiter='/'
    )
    
    pdfs = [obj['Key'] for obj in response.get('Contents', []) 
            if obj['Key'].lower().endswith('.pdf') and obj['Size'] > 0]
    
    for pdf_key in pdfs:
        process_pdf_file(s3, bucket, pdf_key)

def lambda_handler(event, context):
    try:
        transform_mp_report_data()
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Proceso completado",
                "success": True
            })
        }
    except Exception as e:
        print("⚠️ Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e),
                "success": False
            })
        }