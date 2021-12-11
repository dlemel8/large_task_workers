import logging
import random
from dataclasses import dataclass
from functools import reduce
from typing import Sequence

from time import time

from protos.task_pb2 import Metadata

LOGGER = logging.getLogger(__file__)


@dataclass
class Task:
    metadata: Metadata
    data: memoryview


class Processor:
    def __init__(self, min_duration_ms: int, max_duration_ms: int):
        self._min_duration_ms = min_duration_ms
        self._max_duration_ms = max_duration_ms

    def run(self, task: Task) -> None:
        start_time = time_ms()
        value = reduce(lambda x, y: (x * y) % 7, task.data)
        LOGGER.debug('task value is %d', value)

        time_so_far = time_ms() - start_time
        simulate_cpu_bound_work(self._min_duration_ms - time_so_far, self._max_duration_ms - time_so_far)


class ProcessorSelector:
    def __init__(self, processors: Sequence[Processor], min_duration_ms: int, max_duration_ms: int):
        self._processors = processors
        self._min_duration_ms = min_duration_ms
        self._max_duration_ms = max_duration_ms

    def select(self, data: memoryview) -> Processor:
        start_time = time_ms()
        number_of_processors = len(self._processors)
        processor_id = reduce(lambda x, y: (x + y) % number_of_processors, data)
        LOGGER.debug('processor id is %d', processor_id)

        time_so_far = time_ms() - start_time
        simulate_cpu_bound_work(self._min_duration_ms - time_so_far, self._max_duration_ms - time_so_far)
        return self._processors[processor_id]


def simulate_cpu_bound_work(min_duration_ms: float, max_duration_ms: float) -> None:
    duration = random.uniform(min_duration_ms, max_duration_ms)
    start = time_ms()
    number = max_duration_ms
    while time_ms() - start < duration:
        number *= number


def time_ms():
    return time() * 1000
