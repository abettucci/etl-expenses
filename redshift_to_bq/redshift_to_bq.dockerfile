# Referenciamos a la imagen de la lambda base con librerias comun entre todas las lambda
FROM public.ecr.aws/lambda/python:3.9
# El resto de tu Dockerfile...
COPY redshift_to_bq/requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir --no-deps

COPY redshift_to_bq/lambda_function.py ${LAMBDA_TASK_ROOT}

RUN rm -rf /var/cache/pip/* /tmp/* /var/tmp/*
RUN find /var/lang -name "*.pyc" -delete 2>/dev/null || true

CMD ["lambda_function.lambda_handler"]