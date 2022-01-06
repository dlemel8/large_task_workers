import grpc

from consumer.application.processor import ExternalProcessorClient, Task
from protos.processor_pb2 import InternalDataQuery, ExternalDataQuery
from protos.processor_pb2_grpc import ProcessorStub


class ExternalProcessorGrpcClient(ExternalProcessorClient):
    def __init__(self, channel: grpc.Channel):
        self._channel = channel

    def process(self, task: Task) -> bool:
        client = ProcessorStub(self._channel)

        if task.metadata.data_key:
            query = ExternalDataQuery(labels=task.metadata.labels, dataKey=task.metadata.data_key)
            result = client.ProcessExternalDataTask(query)
        else:
            query = InternalDataQuery(labels=task.metadata.labels, data=task.data.tobytes())
            result = client.ProcessInternalDataTask(query)

        return result.success
