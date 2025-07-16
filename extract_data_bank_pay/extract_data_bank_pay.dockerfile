# Importamos la variable de github secrets
ARG aws_account_id

# Referenciamos a la imagen de la lambda base con librerias comun entre todas las lambda
FROM ${aws_account_id}.dkr.ecr.us-east-2.amazonaws.com/etl-expenses:lambda-base

# Agregamos dependencias específicas de esta función
COPY extract_data_bank_pay/requirements.txt .
RUN pip install -r requirements.txt

COPY extract_data_bank_pay/extract_data_bank_pay.py ${LAMBDA_TASK_ROOT}
CMD ["extract_data_bank_pay.lambda_handler"]
