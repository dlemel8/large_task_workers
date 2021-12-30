import logging
import random
import signal
import socket
from enum import Enum
from threading import Event
from typing import Sequence

import grpc
from vyper import v as config

from consumer.application.processor import ProcessorSelector, InternalProcessor, ExternalProcessor, Processor, \
    TaskHandler
from consumer.infrastructure.external_processor import ExternalProcessorGrpcClient
from consumer.interfaces.consumer import TaskConsumer
from consumer.interfaces.prometheus import serve_prometheus_metrics, PrometheusSelectorReporter, \
    PrometheusProcessorReporter
from consumer.interfaces.rabbitmq_consumer import RabbitMqConsumer
from consumer.interfaces.redis_consumer import RedisConsumer

LOGGER = logging.getLogger(__file__)


class Strategy(Enum):
    METADATA_AND_DATA_IN_REDIS = 'MetadataAndDataInRedis'
    METADATA_AND_DATA_IN_RABBITMQ = 'MetadataAndDataInRabbitMq'


def main() -> None:
    logging.basicConfig(format='%(asctime)-15s %(message)s', level=logging.INFO)
    LOGGER.info('start to prepare workers')
    random.seed()
    config.automatic_env()

    serve_prometheus_metrics(config.get_int('metrics_port'))

    done = Event()
    for signal_ in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signal_, lambda _signum, _frame: done.set())

    processors = prepare_processors()
    selector = ProcessorSelector(
        processors,
        config.get_float('selector_min_duration_ms'),
        config.get_float('selector_max_duration_ms'),
        PrometheusSelectorReporter()
    )
    handler = TaskHandler(selector)

    consumer = prepare_consumer(handler)
    consumer.consume_tasks(done)
    consumer.close()

    LOGGER.info('goodbye')


def prepare_processors() -> Sequence[Processor]:
    internal_processor_min_duration_ms = config.get_float('internal_processor_min_duration_ms')
    internal_processor_max_duration_ms = config.get_float('internal_processor_max_duration_ms')
    processor_grpc_channel = grpc.insecure_channel(config.get_string('external_processor_grpc_url'))
    processor_grpc_client = ExternalProcessorGrpcClient(processor_grpc_channel)
    reporter = PrometheusProcessorReporter()
    res = []
    for i in range(config.get_int('number_of_processors')):
        res.append(
            InternalProcessor(internal_processor_min_duration_ms, internal_processor_max_duration_ms, reporter)
            if i % 2 == 0 else
            ExternalProcessor(processor_grpc_client, reporter)
        )
    return res


def prepare_consumer(handler: TaskHandler) -> TaskConsumer:
    published_tasks_queue_name = config.get_string('published_tasks_queue_name')
    strategy = config.get_string('strategy')
    if strategy == Strategy.METADATA_AND_DATA_IN_REDIS.value:
        return RedisConsumer(
            config.get_string('redis_url'),
            '_'.join([config.get_string('processing_tasks_queue_name'), socket.gethostname()]),
            published_tasks_queue_name,
            handler,
        )

    if strategy == Strategy.METADATA_AND_DATA_IN_RABBITMQ.value:
        return RabbitMqConsumer(
            config.get_string('rabbitmq_url'),
            published_tasks_queue_name,
            config.get_int('published_tasks_queue_max_size'),
            handler,
        )

    raise Exception(f'unsupported {strategy=}')


if __name__ == '__main__':
    main()
