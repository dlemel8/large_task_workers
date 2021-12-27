from abc import ABC, abstractmethod
from struct import unpack
from threading import Event

from consumer.application.processor import Task, TaskHandler, time_ms
from consumer.interfaces.prometheus import TASK_CONSUMER_DURATIONS
from protos.task_pb2 import Metadata

SIZE_HEADER_SIZE_IN_BYTES = 4


class TaskConsumer(ABC):
    def __init__(self, handler: TaskHandler):
        self._handler = handler

    @abstractmethod
    def consume_tasks(self, done: Event) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    def _consume_task(self, task_bytes: memoryview):
        start_time = time_ms()
        task = self._deserialize_task(task_bytes)
        self._handler.handle(task)
        TASK_CONSUMER_DURATIONS.observe(time_ms() - start_time)

    @staticmethod
    def _deserialize_task(task_bytes: memoryview) -> Task:
        data_size, *_ = unpack('>L', task_bytes[:SIZE_HEADER_SIZE_IN_BYTES])
        metadata_offset = SIZE_HEADER_SIZE_IN_BYTES + data_size
        metadata = Metadata()
        metadata.ParseFromString(task_bytes[metadata_offset:])
        return Task(metadata.labels, task_bytes[data_size:metadata_offset])
