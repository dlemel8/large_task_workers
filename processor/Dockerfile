FROM alpine:3.14 AS grpc_probe

ENV PROBE_URL=https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.6/grpc_health_probe-linux-amd64
ENV PROBE_NAME=grpc_health_probe

RUN apk add --no-cache curl && \
    curl -fSL ${PROBE_URL} -o /tmp/${PROBE_NAME} && \
    chmod +x /tmp/${PROBE_NAME}

###############################################

FROM namely/protoc-all AS protos

COPY protos /defs

RUN entrypoint.sh -d . -l go -o .

###############################################

FROM golang:1.16.10-alpine3.14 AS builder

ENV CGO_ENABLED=0
ENV SERVICE_NAME=processor

COPY --from=protos /defs protos

WORKDIR ${SERVICE_NAME}
COPY ${SERVICE_NAME}/go.mod ${SERVICE_NAME}/go.sum ./
RUN go mod download -x

COPY ${SERVICE_NAME}/ .
RUN go build -v -o /tmp/${SERVICE_NAME}

###############################################

FROM scratch

ENV GRPC_PORT=7777
COPY --from=grpc_probe /tmp/${PROBE_NAME} /
EXPOSE ${GRPC_PORT}
HEALTHCHECK CMD [ "/grpc_health_probe", "-addr=:7777"]

COPY --from=builder /tmp/${SERVICE_NAME} /
CMD [ "/processor" ]
