# Referenciamos a la imagen de la lambda base con librerias comun entre todas las lambda
FROM public.ecr.aws/lambda/python:3.9

COPY extract_data_mp/extract_data_mp.py ${LAMBDA_TASK_ROOT}
CMD ["extract_data_mp.lambda_handler"]