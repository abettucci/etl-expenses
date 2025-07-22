# Referenciamos a la imagen de la lambda base con librerias comun entre todas las lambda
FROM public.ecr.aws/lambda/python:3.9

COPY transform_data_mp/transform_data_mp.py ${LAMBDA_TASK_ROOT}
CMD ["transform_data_mp.lambda_handler"]