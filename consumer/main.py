import logging
import random
import signal
from struct import unpack
from threading import Event

from redis import from_url
from vyper import v as config

from consumer.processor import Task, ProcessorSelector, Processor
from protos.task_pb2 import Metadata

SIZE_HEADER_SIZE_IN_BYTES = 4
GET_NEW_TASK_TIMEOUT_IN_SECONDS = 3
LOGGER = logging.getLogger(__file__)


def main() -> None:
    logging.basicConfig(format='%(asctime)-15s %(message)s', level=logging.INFO)
    LOGGER.info('start to prepare workers')
    random.seed()
    config.automatic_env()

    done = Event()
    for signal_ in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signal_, lambda _signum, _frame: done.set())

    processors = [
        Processor(config.get_int('processor_min_duration_ms'), config.get_int('processor_max_duration_ms'))
        for _ in range(config.get_int('number_of_processors'))
    ]
    selector = ProcessorSelector(
        processors,
        config.get_int('selector_min_duration_ms'),
        config.get_int('selector_max_duration_ms'),
    )
    consume_tasks(done, selector)
    LOGGER.info('goodbye')


def consume_tasks(done: Event, selector: ProcessorSelector) -> None:
    redis_url = config.get_string('redis_url')
    client = from_url(redis_url)

    processing_tasks_queue_name = config.get_string('processing_tasks_queue_name')
    published_tasks_queue_name = config.get_string('published_tasks_queue_name')
    LOGGER.info('start to consume tasks')
    while not done.is_set():
        for task_bytes in client.lrange(processing_tasks_queue_name, 0, -1):
            task = deserialize_task(memoryview(task_bytes))
            process_task(task, selector)

        client.delete(processing_tasks_queue_name)
        client.blmove(published_tasks_queue_name, processing_tasks_queue_name, GET_NEW_TASK_TIMEOUT_IN_SECONDS)


def deserialize_task(task_bytes: memoryview) -> Task:
    data_size, *_ = unpack('>L', task_bytes[:SIZE_HEADER_SIZE_IN_BYTES])
    metadata_offset = SIZE_HEADER_SIZE_IN_BYTES + data_size
    metadata = Metadata()
    metadata.ParseFromString(task_bytes[metadata_offset:])
    return Task(metadata, task_bytes[data_size:metadata_offset])


def process_task(task: Task, selector: ProcessorSelector) -> None:
    processor = selector.select(task.data)
    processor.run(task)


if __name__ == '__main__':
    main()
