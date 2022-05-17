FROM namely/protoc-all AS protos

COPY protos /defs

RUN entrypoint.sh -d . -l python -o . && \
    sed -i -r 's/^import (.+_pb2.*)/from . import \1/g' *_pb2*.py

###############################################

FROM python:3.9.9-alpine3.14

ENV SERVICE_NAME=consumer
WORKDIR /app

RUN apk add --no-cache build-base cmake gcc libc-dev libffi-dev linux-headers perl

COPY ${SERVICE_NAME}/requirements.txt ${SERVICE_NAME}/
RUN pip install --no-cache-dir -r ${SERVICE_NAME}/requirements.txt

COPY --from=protos /defs protos
COPY ${SERVICE_NAME} ${SERVICE_NAME}/

ENV PYTHONPATH=.
CMD ["python3", "consumer/main.py"]