import grpc

from consumer.application.processor import ExternalProcessorClient, Task
from protos.processor_pb2 import ProcessQuery
from protos.processor_pb2_grpc import ProcessorStub


class ExternalProcessorGrpcClient(ExternalProcessorClient):
    def __init__(self, channel: grpc.Channel):
        self._channel = channel

    def process(self, task: Task) -> bool:
        client = ProcessorStub(self._channel)
        query = ProcessQuery(labels=task.labels, data=task.data.tobytes())
        result = client.Process(query)
        return result.success
