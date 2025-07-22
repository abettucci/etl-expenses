# Referenciamos a la imagen de la lambda base con librerias comun entre todas las lambda
FROM public.ecr.aws/lambda/python:3.9

# Agregar dependencias específicas para esta función
COPY transform_data_pdf/requirements.txt .
RUN pip install -r requirements.txt

COPY transform_data_pdf/lambda_function.py ${LAMBDA_TASK_ROOT}
CMD ["lambda_function.lambda_handler"]