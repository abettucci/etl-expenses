# Referenciamos a la imagen de la lambda base con librerias comun entre todas las lambda
FROM public.ecr.aws/lambda/python:3.9

# Agregar dependencias específicas para esta función
COPY transform_data_bank_pay/requirements.txt .
RUN pip install -r requirements.txt

COPY transform_data_bank_pay/transform_data_bank_pay.py ${LAMBDA_TASK_ROOT}
CMD ["transform_data_bank_pay.lambda_handler"]