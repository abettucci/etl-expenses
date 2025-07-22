# Referenciamos a la imagen de la lambda base con librerias comun entre todas las lambda
FROM public.ecr.aws/lambda/python:3.9

# Agregamos dependencias específicas de esta función
COPY extract_data_bank_pay/requirements.txt .
RUN pip install -r requirements.txt

COPY extract_data_bank_pay/lambda_function.py ${LAMBDA_TASK_ROOT}
CMD ["lambda_function.lambda_handler"]
