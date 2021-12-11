FROM namely/protoc-all AS protos

COPY protos /defs

RUN entrypoint.sh -f task.proto -l python -o .

###############################################

FROM python:3.9.9-alpine3.14 AS builder

ENV SERVICE_NAME=consumer
WORKDIR /app

COPY --from=protos /defs protos

COPY ${SERVICE_NAME}/requirements.txt ${SERVICE_NAME}/
RUN pip install --no-cache-dir -r ${SERVICE_NAME}/requirements.txt

COPY ${SERVICE_NAME}/* ${SERVICE_NAME}/

ENV PYTHONPATH=.
CMD ["python3", "consumer/main.py"]
