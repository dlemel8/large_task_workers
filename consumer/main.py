import logging
import random
import signal
import socket
from threading import Event
from typing import Sequence

import grpc
from vyper import v as config

from consumer.application.processor import ProcessorSelector, InternalProcessor, ExternalProcessor, Processor, \
    TaskHandler
from consumer.infrastructure.external_processor import ExternalProcessorGrpcClient
from consumer.interfaces.prometheus import serve_prometheus_metrics, PrometheusSelectorReporter, \
    PrometheusProcessorReporter
from consumer.interfaces.task_consumer import RedisConsumer

LOGGER = logging.getLogger(__file__)


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

    consumer = RedisConsumer(
        config.get_string('redis_url'),
        '_'.join([config.get_string('processing_tasks_queue_name'), socket.gethostname()]),
        config.get_string('published_tasks_queue_name'),
        handler
    )
    consumer.consume_tasks(done)

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


if __name__ == '__main__':
    main()
