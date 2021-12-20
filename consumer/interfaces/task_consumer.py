import logging
from struct import unpack
from threading import Event

from redis import from_url

from consumer.application.processor import Task, TaskHandler, time_ms
from consumer.interfaces.prometheus import TASK_CONSUMER_DURATIONS
from protos.task_pb2 import Metadata

SIZE_HEADER_SIZE_IN_BYTES = 4
GET_NEW_TASK_TIMEOUT_IN_SECONDS = 3


class TaskConsumer:
    def __init__(self, handler: TaskHandler):
        self._handler = handler

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


class RedisConsumer(TaskConsumer):
    LOGGER = logging.getLogger('REDIS_CONSUMER')

    def __init__(self,
                 redis_url: str,
                 processing_tasks_queue_name: str,
                 published_tasks_queue_name: str,
                 handler: TaskHandler):
        super().__init__(handler)
        self._client = from_url(redis_url)
        self._processing_tasks_queue_name = processing_tasks_queue_name
        self._published_tasks_queue_name = published_tasks_queue_name

    def consume_tasks(self, done: Event) -> None:
        self.LOGGER.info('start to consume tasks')
        while not done.is_set():
            for task_bytes in self._client.lrange(self._processing_tasks_queue_name, 0, -1):
                self._consume_task(memoryview(task_bytes))

            self._client.delete(self._processing_tasks_queue_name)
            self._client.blmove(self._published_tasks_queue_name,
                                self._processing_tasks_queue_name,
                                GET_NEW_TASK_TIMEOUT_IN_SECONDS)
