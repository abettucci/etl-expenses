# Dockerfile.gmail_extractor

FROM public.ecr.aws/lambda/python:3.9

# Agregar dependencias específicas para esta función
COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir --no-deps

# Copia el código específico de esta función
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Limpiar cache y archivos temporales para reducir tamaño
RUN rm -rf /var/cache/pip/* /tmp/* /var/tmp/*
RUN find /var/lang -name "*.pyc" -delete 2>/dev/null || true

CMD ["lambda_function.lambda_handler"]