# Referenciamos a la imagen de la lambda base con librerias comun entre todas las lambda
FROM public.ecr.aws/lambda/python:3.9

# Agregamos dependencias específicas de esta función
COPY extract_data_pdf/requirements.txt .
RUN pip install -r requirements.txt

COPY extract_data_pdf/extract_data_pdf.py ${LAMBDA_TASK_ROOT}
CMD ["extract_data_pdf.lambda_handler"]
