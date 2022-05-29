import logging
import random
import signal
import socket
from enum import Enum
from pathlib import Path
from threading import Event
from typing import Sequence, Callable

import grpc
from vyper import v as config

from consumer.application.handler import TaskHandler
from consumer.application.processor import ProcessorSelector, InternalProcessor, ExternalProcessor, Processor, \
    ProcessorReporter
from consumer.infrastructure.external_processor import ExternalProcessorGrpcClient
from consumer.infrastructure.file_store import FileStore, FileLoadStrategy
from consumer.interfaces.nng_consumer import NanoMsgNgConsumer
from consumer.interfaces.prometheus import serve_prometheus_metrics, PrometheusReporter
from consumer.interfaces.rabbitmq_consumer import RabbitMqConsumer
from consumer.interfaces.redis_consumer import RedisConsumer
from consumer.interfaces.task_consumer import TaskConsumer

LOGGER = logging.getLogger(__file__)


class MessagingStrategy(Enum):
    METADATA_AND_DATA_IN_REDIS = 'MetadataAndDataInRedis'
    METADATA_AND_DATA_IN_RABBITMQ = 'MetadataAndDataInRabbitMq'
    METADATA_AND_DATA_IN_NNG = 'MetadataAndDataInNng'
    METADATA_IN_RABBIT_MQ_AND_DATA_IN_FILE = 'MetadataInRabbitMqAndDataInFile'


def main() -> None:
    logging.basicConfig(format='%(asctime)-15s %(message)s', level=logging.INFO)
    LOGGER.info('start to prepare workers')
    random.seed()
    config.automatic_env()

    serve_prometheus_metrics(config.get_int('metrics_port'))

    done = Event()
    for signal_ in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signal_, lambda _signum, _frame: done.set())

    reporter = PrometheusReporter()
    processors = prepare_processors(reporter)
    consumer = TaskConsumer(
        TaskHandler(
            ProcessorSelector(
                processors,
                config.get_float('selector_min_duration_ms'),
                config.get_float('selector_max_duration_ms'),
                reporter
            ),
            FileStore(
                Path(config.get_string('file_store_path')),
                FileLoadStrategy(config.get_string('file_load_strategy')),
            ),
        )
    )

    messaging_strategy = MessagingStrategy(config.get_string('messaging_strategy'))
    consumer_callback = select_consumer_callback(messaging_strategy, consumer)
    run_consumer(messaging_strategy, done, consumer_callback)

    LOGGER.info('goodbye')


def prepare_processors(reporter: ProcessorReporter) -> Sequence[Processor]:
    internal_processor_min_duration_ms = config.get_float('internal_processor_min_duration_ms')
    internal_processor_max_duration_ms = config.get_float('internal_processor_max_duration_ms')
    processor_grpc_channel = grpc.insecure_channel(config.get_string('external_processor_grpc_url'))
    processor_grpc_client = ExternalProcessorGrpcClient(processor_grpc_channel)
    res = []
    for i in range(config.get_int('number_of_processors')):
        res.append(
            InternalProcessor(internal_processor_min_duration_ms, internal_processor_max_duration_ms, reporter)
            if i % 2 == 0 else
            ExternalProcessor(processor_grpc_client, reporter)
        )
    return res


def select_consumer_callback(strategy: MessagingStrategy, consumer: TaskConsumer) -> Callable[[memoryview], None]:
    if strategy in (MessagingStrategy.METADATA_AND_DATA_IN_REDIS,
                    MessagingStrategy.METADATA_AND_DATA_IN_RABBITMQ,
                    MessagingStrategy.METADATA_AND_DATA_IN_NNG):
        return consumer.consume_internal_data_task

    if strategy == MessagingStrategy.METADATA_IN_RABBIT_MQ_AND_DATA_IN_FILE:
        return consumer.consume_external_data_task

    raise ValueError(f'unsupported {strategy=}')


def run_consumer(strategy: MessagingStrategy, done: Event, callback: Callable[[memoryview], None]) -> None:
    published_tasks_queue_name = config.get_string('published_tasks_queue_name')
    if strategy == MessagingStrategy.METADATA_AND_DATA_IN_REDIS:
        consumer = RedisConsumer(
            config.get_string('redis_url'),
            '_'.join([config.get_string('processing_tasks_queue_name'), socket.gethostname()]),
            published_tasks_queue_name,
        )
    elif strategy in (MessagingStrategy.METADATA_AND_DATA_IN_RABBITMQ,
                      MessagingStrategy.METADATA_IN_RABBIT_MQ_AND_DATA_IN_FILE):
        consumer = RabbitMqConsumer(
            config.get_string('rabbitmq_url'),
            published_tasks_queue_name,
            config.get_int('published_tasks_queue_max_size'),
        )
    elif strategy == MessagingStrategy.METADATA_AND_DATA_IN_NNG:
        consumer = NanoMsgNgConsumer(config.get_string('nng_tcp_port'))
    else:
        raise ValueError(f'unsupported {strategy=}')

    try:
        consumer.consume_tasks(done, callback)
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
