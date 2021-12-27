import logging
from threading import Event

from pika import BlockingConnection, URLParameters

from consumer.application.processor import TaskHandler
from consumer.interfaces.consumer import TaskConsumer

QUEUE_MAX_SIZE_KEY_NAME = 'x-max-length'


class RabbitMqConsumer(TaskConsumer):
    LOGGER = logging.getLogger('RABBITMQ_CONSUMER')

    def __init__(self,
                 rabbitmq_url: str,
                 published_tasks_queue_name: str,
                 published_tasks_queue_max_size: int,
                 handler: TaskHandler):
        super().__init__(handler)
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
            self.LOGGER.exception('failed to setup RabbitMQ resources')
            self.close()

    def consume_tasks(self, done: Event) -> None:
        self.LOGGER.info('start to consume tasks')
        for method_frame, _, body in self._channel.consume(self._published_tasks_queue_name):
            self._consume_task(memoryview(body))
            self._channel.basic_ack(method_frame.delivery_tag)
            if done.is_set():
                break

        self._channel.cancel()

    def close(self) -> None:
        if self._channel is not None:
            self._channel.close()

        if self._connection is not None:
            self._connection.close()
