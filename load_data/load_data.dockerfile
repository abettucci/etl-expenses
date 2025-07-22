# Referenciamos a la imagen de la lambda base con librerias comun entre todas las lambda
FROM public.ecr.aws/lambda/python:3.9

COPY load_data/load_data.py ${LAMBDA_TASK_ROOT}
CMD ["load_data.lambda_handler"]