import logging
from dataclasses import dataclass
from struct import unpack

from redis import from_url
from vyper import v as config

from protos.task_pb2 import Metadata

SIZE_HEADER_SIZE_IN_BYTES = 4
GET_NEW_TASK_TIMEOUT_IN_SECONDS = 5
LOGGER = logging.getLogger()


@dataclass
class Task:
    metadata: Metadata
    data: memoryview


def main() -> None:
    logging.basicConfig(format='%(asctime)-15s %(message)s')
    config.automatic_env()
    LOGGER.info('everything is set')
    consume_tasks()
    LOGGER.info('goodbye')


def consume_tasks() -> None:
    redis_url = config.get_string('redis_url')
    client = from_url(redis_url)

    processing_tasks_queue_name = config.get_string('processing_tasks_queue_name')
    published_tasks_queue_name = config.get_string('published_tasks_queue_name')
    LOGGER.info('start to consume tasks')
    while True:
        for task_bytes in client.lrange(processing_tasks_queue_name, 0, -1):
            task = deserialize_task(memoryview(task_bytes))
            consume_task(task)

        client.delete(processing_tasks_queue_name)
        client.blmove(published_tasks_queue_name, processing_tasks_queue_name, GET_NEW_TASK_TIMEOUT_IN_SECONDS)


def deserialize_task(task_bytes: memoryview) -> Task:
    data_size, *_ = unpack('>L', task_bytes[:SIZE_HEADER_SIZE_IN_BYTES])
    metadata_offset = SIZE_HEADER_SIZE_IN_BYTES + data_size
    metadata = Metadata()
    metadata.ParseFromString(task_bytes[metadata_offset:])
    return Task(metadata, task_bytes[data_size:metadata_offset])


def consume_task(task: Task) -> None:
    print(task)


if __name__ == '__main__':
    main()
