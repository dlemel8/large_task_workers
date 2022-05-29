import logging
from threading import Event
from typing import Callable

from redis import from_url

LOGGER = logging.getLogger(__file__)


class RedisConsumer:
    def __init__(self,
                 redis_url: str,
                 published_tasks_queue_name: str):
        self._client = from_url(redis_url)
        self._published_tasks_queue_name = published_tasks_queue_name

    def consume_tasks(self, done: Event, callback: Callable[[memoryview], None]) -> None:
        LOGGER.info('start to consume tasks')
        while not done.is_set():
            _, task_bytes = self._client.blpop(self._published_tasks_queue_name)
            callback(memoryview(task_bytes))

        LOGGER.info('consume tasks was cancelled')

    def close(self) -> None:
        self._client.close()
