import logging
from abc import ABC, abstractmethod

from consumer.application.processor import ProcessorSelector, Task, TaskMetadata

LOGGER = logging.getLogger(__file__)


class BytesStore(ABC):
    @abstractmethod
    def load(self, key):
        raise NotImplementedError

    @abstractmethod
    def delete(self, key):
        raise NotImplementedError


class TaskHandler:
    def __init__(self, selector: ProcessorSelector, bytes_store: BytesStore):
        self._selector = selector
        self._bytes_store = bytes_store

    def handle_internal_data_task(self, task: Task) -> None:
        self._handle(task)

    def handle_external_data_task(self, metadata: TaskMetadata) -> None:
        if not metadata.data_key:
            LOGGER.error('missing data key in task %d', metadata.task_id)
            return

        try:
            with self._bytes_store.load(metadata.data_key) as task_bytes:
                task = Task(metadata, task_bytes)
                self._handle(task)
        finally:
            self._bytes_store.delete(metadata.data_key)

    def _handle(self, task: Task) -> None:
        processor = self._selector.select(task.data)
        processor.process(task)
