import logging
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import reduce
from typing import Sequence, Optional

from time import time

LOGGER = logging.getLogger(__file__)

Milliseconds = float


@dataclass
class TaskMetadata:
    task_id: int
    labels: Sequence[str]
    data_key: Optional[str]


@dataclass
class Task:
    metadata: TaskMetadata
    data: memoryview


class ProcessorReporter(ABC):
    @abstractmethod
    def processed_task(self, took: Milliseconds, type_: str, success: bool) -> None:
        raise NotImplementedError


class Processor(ABC):
    def __init__(self, reporter: ProcessorReporter):
        self._reporter = reporter

    def process(self, task: Task) -> None:
        start_time = time_ms()
        success = self._process(task, start_time)
        self._reporter.processed_task(time_ms() - start_time, type(self).__name__, success)

    @abstractmethod
    def _process(self, task: Task, start_time: Milliseconds) -> bool:
        raise NotImplementedError


class InternalProcessor(Processor):
    def __init__(self, min_duration: Milliseconds, max_duration: Milliseconds, reporter: ProcessorReporter):
        super().__init__(reporter)
        self._min_duration = min_duration
        self._max_duration = max_duration

    def _process(self, task: Task, start_time: Milliseconds) -> bool:
        modulo_divisor = len(task.metadata.labels)
        value = reduce(lambda x, y: (x * y) % modulo_divisor, task.data)
        LOGGER.debug('task value is %d', value)

        time_so_far = time_ms() - start_time
        simulate_cpu_bound_work(self._min_duration - time_so_far, self._max_duration - time_so_far)
        return True


class ExternalProcessorClient(ABC):
    @abstractmethod
    def process(self, task: Task) -> bool:
        raise NotImplementedError


class ExternalProcessor(Processor):
    def __init__(self, client: ExternalProcessorClient, reporter: ProcessorReporter):
        super().__init__(reporter)
        self._client = client

    def _process(self, task: Task, start_time: Milliseconds) -> bool:
        return self._client.process(task)


class SelectorReporter(ABC):
    @abstractmethod
    def selected_processor(self, took: Milliseconds) -> None:
        raise NotImplementedError


class ProcessorSelector:
    def __init__(self, processors: Sequence[Processor],
                 min_duration: Milliseconds,
                 max_duration: Milliseconds,
                 reporter: SelectorReporter):
        self._processors = processors
        self._min_duration = min_duration
        self._max_duration = max_duration
        self._reporter = reporter

    def select(self, data: memoryview) -> Processor:
        start_time = time_ms()
        number_of_processors = len(self._processors)
        processor_id = reduce(lambda x, y: (x + y) % number_of_processors, data)
        LOGGER.debug('processor id is %d', processor_id)

        time_so_far = time_ms() - start_time
        simulate_cpu_bound_work(self._min_duration - time_so_far, self._max_duration - time_so_far)

        self._reporter.selected_processor(time_ms() - start_time)
        return self._processors[processor_id]


def simulate_cpu_bound_work(min_duration: Milliseconds, max_duration: Milliseconds) -> None:
    if max_duration <= 0 or max_duration - min_duration <= 0:
        return

    duration = random.uniform(min_duration, max_duration)
    start = time_ms()
    number = max_duration
    while time_ms() - start < duration:
        number *= number


def time_ms() -> Milliseconds:
    return time() * 1000
