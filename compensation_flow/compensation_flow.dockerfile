# Dockerfile.gmail_extractor

FROM public.ecr.aws/lambda/python:3.9

# Agregar dependencias específicas para esta función
COPY compensation_flow/requirements.txt .
RUN pip install -r requirements.txt

# Copia el código específico de esta función
COPY compensation_flow/lambda_function.py ${LAMBDA_TASK_ROOT}
CMD ["lambda_function.lambda_handler"]