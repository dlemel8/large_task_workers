from struct import unpack

from consumer.application.handler import TaskHandler
from consumer.application.processor import Task, time_ms, TaskMetadata
from consumer.interfaces.prometheus import TASK_CONSUMER_DURATIONS
from protos.task_pb2 import Metadata

SIZE_HEADER_SIZE_IN_BYTES = 4


class TaskConsumer:
    def __init__(self, handler: TaskHandler):
        self._handler = handler

    def consume_internal_data_task(self, task_bytes: memoryview) -> None:
        start_time = time_ms()
        data_size, *_ = unpack('>L', task_bytes[:SIZE_HEADER_SIZE_IN_BYTES])
        metadata_offset = SIZE_HEADER_SIZE_IN_BYTES + data_size
        metadata = self._deserialize_metadata(task_bytes[metadata_offset:])
        task = Task(metadata, task_bytes[data_size:metadata_offset])
        self._handler.handle_internal_data_task(task)
        TASK_CONSUMER_DURATIONS.observe(time_ms() - start_time)

    def consume_external_data_task(self, task_bytes: memoryview) -> None:
        start_time = time_ms()
        metadata = self._deserialize_metadata(task_bytes)
        self._handler.handle_external_data_task(metadata)
        TASK_CONSUMER_DURATIONS.observe(time_ms() - start_time)

    @staticmethod
    def _deserialize_metadata(metadata_bytes: memoryview) -> TaskMetadata:
        metadata = Metadata()
        metadata.ParseFromString(metadata_bytes)
        return TaskMetadata(task_id=metadata.taskId, labels=metadata.labels, data_key=metadata.dataKey)
