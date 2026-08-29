# Container-image Lambda for the producer handler. pyarrow + pyiceberg
# are far too large for a zipped layer, so a container image is the reliable path.
# Build with the experiments/ dir as context so config.py + schemas.py are in scope:
#   docker build -f lambda/Dockerfile -t iceberg-maint-eval .
FROM public.ecr.aws/lambda/python:3.12

COPY lambda/requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Single source of truth: reuse the same config + schemas the setup scripts use.
COPY config.py schemas.py ${LAMBDA_TASK_ROOT}/
COPY lambda/handler.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.handler"]
