# Importamos la variable de github secrets
ARG aws_account_id
ARG TELEGRAM_BOT_TOKEN
ENV TELEGRAM_BOT_TOKEN=$TELEGRAM_BOT_TOKEN

# Referenciamos a la imagen de la lambda base con librerias comun entre todas las lambda
FROM ${aws_account_id}.dkr.ecr.us-east-2.amazonaws.com/etl-expenses:lambda-base

# Agregamos dependencias específicas de esta función
COPY ai_agent/requirements.txt .
RUN pip install -r requirements.txt

COPY ai_agent/ai_agent.py ${LAMBDA_TASK_ROOT}
CMD ["ai_agent.lambda_handler"]