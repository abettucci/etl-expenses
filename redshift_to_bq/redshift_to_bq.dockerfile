# Importamos la variable de github secrets
ARG aws_account_id

# Referenciamos a la imagen de la lambda base con librerias comun entre todas las lambda
FROM ${aws_account_id}.dkr.ecr.us-east-2.amazonaws.com/etl-expenses:lambda-base

# Agregamos dependencias específicas de esta función
COPY redshift_to_bq/requirements.txt .
RUN pip install -r requirements.txt

COPY redshift_to_bq/redshift_to_bq.py ${LAMBDA_TASK_ROOT}
CMD ["redshift_to_bq.lambda_handler"]