FROM public.ecr.aws/lambda/python:3.12

RUN pip install --no-cache-dir pandas pyarrow awswrangler requests

COPY . ${LAMBDA_TASK_ROOT}

CMD ["app.handler"]


