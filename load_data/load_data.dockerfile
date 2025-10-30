# Imagen base de AWS Lambda con Python 3.9
FROM public.ecr.aws/lambda/python:3.9

# Actualizar pip primero
RUN pip install --upgrade pip

# Copiar requirements
COPY requirements.txt .

# Actualizar pip y setuptools
RUN pip install --upgrade pip setuptools wheel

# ⭐ CRÍTICO: Instalar numpy PRIMERO
RUN pip install --no-cache-dir numpy==1.24.3

# Luego instalar el resto
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código de la lambda
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Limpiar archivos temporales para reducir tamaño
RUN rm -rf /var/cache/pip/* /tmp/* /var/tmp/* && \
    find /var/lang -name "*.pyc" -delete 2>/dev/null || true

# Comando de ejecución
CMD ["lambda_function.lambda_handler"]