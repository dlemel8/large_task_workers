import logging
import socket
from dataclasses import dataclass
from threading import Event
from typing import Callable

import pynng

LOGGER = logging.getLogger(__file__)


@dataclass
class TcpEndpoint:
    name: str
    port: int

    def __str__(self):
        return f'tcp://{self.name}:{self.port}'


class NanoMsgNgConsumer:
    def __init__(self, tcp_port: int):
        self._pull = pynng.Pull0()
        endpoint = TcpEndpoint(name=socket.gethostbyname(socket.gethostname()), port=tcp_port)
        self._pull.listen(str(endpoint))

    def consume_tasks(self, done: Event, callback: Callable[[memoryview], None]) -> None:
        LOGGER.info('start to consume tasks')

        while not done.is_set():
            task_bytes = self._pull.recv()
            callback(memoryview(task_bytes))

        LOGGER.info('consume tasks was cancelled')

    def close(self) -> None:
        self._pull.close()
