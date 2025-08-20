# Imagen base de Lambda con Python 3.9
FROM public.ecr.aws/lambda/python:3.9

# Instalar dependencias
COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir

# Copiar el código de la lambda
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Limpieza opcional
RUN rm -rf /var/cache/pip/* /tmp/* /var/tmp/*
RUN find /var/lang -name "*.pyc" -delete 2>/dev/null || true

# Handler
CMD ["lambda_function.lambda_handler"]
