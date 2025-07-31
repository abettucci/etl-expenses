# Dockerfile optimizado para ai_agent
# Usa imagen base con dependencias compartidas

# Solo los ARG que se usan en FROM pueden ir antes

# Imagen base optimizada
FROM public.ecr.aws/lambda/python:3.9

# Ahora sí, el resto de los ARG y ENV
ARG TELEGRAM_BOT_TOKEN
ENV TELEGRAM_BOT_TOKEN=$TELEGRAM_BOT_TOKEN

# Agregamos dependencias específicas de esta función (solo las que no están en base)
COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir

# Copiar código de la función
COPY lambda_function.py ${LAMBDA_TASK_ROOT}/

# Limpiar cache y archivos temporales para reducir tamaño
RUN rm -rf /var/cache/pip/* /tmp/* /var/tmp/*
RUN find /var/lang -name "*.pyc" -delete 2>/dev/null || true

CMD ["lambda_function.lambda_handler"]