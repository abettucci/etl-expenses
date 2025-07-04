# Dockerfile.gmail_extractor
ARG aws_account_id

FROM ${aws_account_id}.dkr.ecr.us-east-2.amazonaws.com/etl-expenses:lambda-base

# Agregar dependencias específicas para esta función
COPY compensation_flow/requirements.txt .
RUN pip install -r requirements.txt

# Copia el código específico de esta función
COPY compensation_flow/lambda_function.py ${LAMBDA_TASK_ROOT}
CMD ["lambda_function.lambda_handler"]