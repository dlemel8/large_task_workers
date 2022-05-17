import logging
from threading import Event
from typing import Callable

import pynng

LOGGER = logging.getLogger(__file__)


class NanoMsgNgConsumer:
    def __init__(self, nng_url: str):
        self._pull = pynng.Pull0()
        self._pull.dial(nng_url, block=True)

    def consume_tasks(self, done: Event, callback: Callable[[memoryview], None]) -> None:
        LOGGER.info('start to consume tasks')

        while not done.is_set():
            task_bytes = self._pull.recv()
            callback(memoryview(task_bytes))

        LOGGER.info('consume tasks was cancelled')

    def close(self) -> None:
        self._pull.close()
