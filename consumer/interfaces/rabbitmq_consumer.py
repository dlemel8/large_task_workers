import logging
from threading import Event
from typing import Callable

from pika import BlockingConnection, URLParameters

LOGGER = logging.getLogger(__file__)

QUEUE_MAX_SIZE_KEY_NAME = 'x-max-length'


class RabbitMqConsumer:
    def __init__(self,
                 rabbitmq_url: str,
                 published_tasks_queue_name: str,
                 published_tasks_queue_max_size: int):
        self._published_tasks_queue_name = published_tasks_queue_name
        self._connection = None
        self._channel = None

        try:
            self._connection = BlockingConnection(URLParameters(rabbitmq_url))
            self._channel = self._connection.channel()
            self._queue = self._channel.queue_declare(queue=published_tasks_queue_name,
                                                      arguments={
                                                          QUEUE_MAX_SIZE_KEY_NAME: published_tasks_queue_max_size
                                                      })
        except Exception:
            LOGGER.exception('failed to setup RabbitMQ resources')
            self.close()

    def consume_tasks(self, done: Event, callback: Callable[[memoryview], None]) -> None:
        LOGGER.info('start to consume tasks')
        for *_, body in self._channel.consume(self._published_tasks_queue_name, auto_ack=True):
            callback(memoryview(body))
            if done.is_set():
                break

        LOGGER.info('consume tasks was cancelled')
        self._channel.cancel()

    def close(self) -> None:
        if self._channel is not None:
            self._channel.close()

        if self._connection is not None:
            self._connection.close()
