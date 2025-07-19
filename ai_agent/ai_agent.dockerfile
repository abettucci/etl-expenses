# Solo los ARG que se usan en FROM pueden ir antes
ARG aws_account_id

# Imagen base
FROM ${aws_account_id}.dkr.ecr.us-east-2.amazonaws.com/etl-expenses:lambda-base

# Ahora sí, el resto de los ARG y ENV
ARG TELEGRAM_BOT_TOKEN
ENV TELEGRAM_BOT_TOKEN=$TELEGRAM_BOT_TOKEN

# Agregamos dependencias específicas de esta función
COPY ai_agent/requirements.txt .
RUN pip install -r requirements.txt

COPY ai_agent/ai_agent.py ${LAMBDA_TASK_ROOT}/ai_agent.py
CMD ["ai_agent.lambda_handler"]