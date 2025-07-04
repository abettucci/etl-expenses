# Importamos la variable de github secrets
ARG aws_account_id

# Referenciamos a la imagen de la lambda base con librerias comun entre todas las lambda
FROM ${aws_account_id}.dkr.ecr.us-east-2.amazonaws.com/etl-expenses:lambda-base

# Agregar dependencias específicas para esta función
COPY webhook_mp_report/requirements.txt .
RUN pip install -r requirements.txt

COPY webhook_mp_report/lambda_function.py ${LAMBDA_TASK_ROOT}
CMD ["lambda_function.lambda_handler"]