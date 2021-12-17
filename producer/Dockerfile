FROM namely/protoc-all AS protos

COPY protos /defs

RUN entrypoint.sh -f task.proto -l go -o .

###############################################

FROM golang:1.16.10-alpine3.14 AS builder

ENV CGO_ENABLED=0
ENV SERVICE_NAME=producer

WORKDIR ${SERVICE_NAME}
COPY protos/go.mod ../protos/
COPY ${SERVICE_NAME}/go.mod ${SERVICE_NAME}/go.sum ./
RUN go mod download -x

COPY --from=protos /defs/*.go ../protos/
COPY ${SERVICE_NAME}/ .
RUN go build -v -o /tmp/${SERVICE_NAME}

###############################################

FROM scratch

COPY --from=builder /tmp/${SERVICE_NAME} /

CMD [ "/producer" ]
