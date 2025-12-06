# Dockerfile para load_receipt_to_bigquery Lambda
# Carga datos de tickets en BigQuery

FROM public.ecr.aws/lambda/python:3.9

# Copiar e instalar dependencias
COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir

# Copiar código de la función
COPY lambda_function.py ${LAMBDA_TASK_ROOT}/

# Limpiar cache para reducir tamaño
RUN rm -rf /var/cache/pip/* /tmp/* /var/tmp/*
RUN find /var/lang -name "*.pyc" -delete 2>/dev/null || true

CMD ["lambda_function.lambda_handler"]