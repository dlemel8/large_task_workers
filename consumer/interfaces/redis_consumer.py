import logging
from threading import Event
from typing import Callable

from redis import from_url

LOGGER = logging.getLogger(__file__)

GET_NEW_TASK_TIMEOUT_IN_SECONDS = 3


class RedisConsumer:
    def __init__(self,
                 redis_url: str,
                 processing_tasks_queue_name: str,
                 published_tasks_queue_name: str):
        self._client = from_url(redis_url)
        self._processing_tasks_queue_name = processing_tasks_queue_name
        self._published_tasks_queue_name = published_tasks_queue_name

    def consume_tasks(self, done: Event, callback: Callable[[memoryview], None]) -> None:
        LOGGER.info('start to consume tasks')
        while not done.is_set():
            for task_bytes in self._client.lrange(self._processing_tasks_queue_name, 0, -1):
                callback(memoryview(task_bytes))

            self._client.delete(self._processing_tasks_queue_name)
            self._client.blmove(self._published_tasks_queue_name,
                                self._processing_tasks_queue_name,
                                GET_NEW_TASK_TIMEOUT_IN_SECONDS)

        LOGGER.info('consume tasks was cancelled')

    def close(self) -> None:
        self._client.close()
