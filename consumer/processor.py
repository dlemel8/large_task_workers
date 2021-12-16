import logging
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import reduce
from typing import Sequence

import grpc
from prometheus_client import Histogram
from prometheus_client.utils import INF
from time import time

from protos.processor_pb2 import ProcessQuery
from protos.processor_pb2_grpc import ProcessorStub
from protos.task_pb2 import Metadata

LOGGER = logging.getLogger(__file__)

HISTOGRAM_BUCKETS = [10 ** x for x in range(5)] + [INF]
PROCESSOR_DURATIONS = Histogram('processor_duration_ms', '', labelnames=('type', 'success'), buckets=HISTOGRAM_BUCKETS)
SELECTOR_DURATIONS = Histogram('selector_duration_ms', '', buckets=HISTOGRAM_BUCKETS)


@dataclass
class Task:
    metadata: Metadata
    data: memoryview


class Processor(ABC):
    def process(self, task: Task) -> None:
        start_time = time_ms()
        success = str(self._process(task, start_time))
        PROCESSOR_DURATIONS.labels(type=type(self).__name__, success=success).observe(time_ms() - start_time)

    @abstractmethod
    def _process(self, task: Task, start_time: float) -> bool:
        raise NotImplementedError


class InternalProcessor(Processor):
    def __init__(self, min_duration_ms: int, max_duration_ms: int):
        self._min_duration_ms = min_duration_ms
        self._max_duration_ms = max_duration_ms

    def _process(self, task: Task, start_time: float) -> bool:
        value = reduce(lambda x, y: (x * y) % 7, task.data)
        LOGGER.debug('task value is %d', value)

        time_so_far = time_ms() - start_time
        simulate_cpu_bound_work(self._min_duration_ms - time_so_far, self._max_duration_ms - time_so_far)
        return True


class ExternalProcessor(Processor):
    def __init__(self, channel: grpc.Channel):
        self._channel = channel

    def _process(self, task: Task, start_time: float) -> bool:
        client = ProcessorStub(self._channel)
        query = ProcessQuery(metadata=task.metadata, data=task.data.tobytes())
        result = client.Process(query)
        return result.success


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

        SELECTOR_DURATIONS.observe(time_ms() - start_time)
        return self._processors[processor_id]


def simulate_cpu_bound_work(min_duration_ms: float, max_duration_ms: float) -> None:
    duration = random.uniform(min_duration_ms, max_duration_ms)
    start = time_ms()
    number = max_duration_ms
    while time_ms() - start < duration:
        number *= number


def time_ms() -> float:
    return time() * 1000
