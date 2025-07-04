# Dockerfile.base
FROM public.ecr.aws/lambda/python:3.10

# Instala dependencias comunes
COPY lambda-base/requirements_common.txt .
RUN pip install -r requirements_common.txt

# Esta imagen base se puede reutilizar en varias funciones