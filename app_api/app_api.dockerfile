FROM public.ecr.aws/lambda/python:3.11
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt --target ${LAMBDA_TASK_ROOT}
COPY domain.py forecast.py finance_balance.py personal_workspace.py integration_parsers.py user_integrations.py merchant_mappings.py insights.py lambda_function.py ${LAMBDA_TASK_ROOT}/
CMD ["lambda_function.lambda_handler"]
