import sys
import os
import re
from PyPDF2 import PdfReader
from tabulate import tabulate 
import tabula 
from tabula import read_pdf

diccionario_pago_tarjetas = dict()
pagos_de_tarjeta = []

ruta = os.environ.get("AMEX_STATEMENTS_PATH", "./statements/Amex")

regex_pattern = r"^Resumen de tarjeta de crédito.*"

for filename in os.listdir(ruta):

    print(filename)

    if re.match(regex_pattern, filename):
        reader = PdfReader(ruta + "/" + filename)

        # getting a specific page from the pdf file
        page = reader.pages[0]

        # extracting text from page
        text = page.extract_text()

        # print(text)

        # quit()

        # # Define a regular expression pattern to extract the text
        # pattern = r"LIMITES: COMPRA"

        # Use re.findall() to find all occurrences of the pattern in the string
        # extracted_text = re.findall(pattern, text)
        
        lines = text.split('\n')
        print(lines)

        quit()
        # text2 = text[text.find('LIMITES: COMPRA'):]
        
        # print(text2)

        # quit()

        # # concepto = 
        # monto_concepto = text[text.find("Sueldo Basico")+23 : text.find("Sueldo Basico")+24+text[text.find("Sueldo Basico")+23:].find(",")-1]