import os
from contextlib import contextmanager
from enum import Enum
from pathlib import Path

from mmap import mmap, PROT_READ, MAP_SHARED

from consumer.application.handler import BytesStore


class FileLoadStrategy(Enum):
    SYSCALL = 'Syscall'
    MMAP = 'Mmap'


class FileStore(BytesStore):
    def __init__(self, directory: Path, strategy: FileLoadStrategy):
        if not directory.is_dir():
            raise ValueError(f'{directory} is not a directory')

        if not os.access(directory, os.W_OK):
            raise ValueError(f'directory {directory} is not writable')

        self._directory = directory
        self._strategy = strategy

    @contextmanager
    def load(self, key: str) -> memoryview:
        file_path = self._file_path(key)
        with open(file_path, mode='rb') as file_obj:
            if self._strategy == FileLoadStrategy.SYSCALL:
                yield memoryview(file_obj.read()).toreadonly()

            elif self._strategy == FileLoadStrategy.MMAP:
                with mmap(file_obj.fileno(), length=0, flags=MAP_SHARED, prot=PROT_READ) as mmap_obj:
                    res = memoryview(mmap_obj).toreadonly()
                    try:
                        yield res
                    finally:
                        res.release()

            else:
                raise Exception(f'unsupported {self._strategy=}')

    def delete(self, key: str):
        file_path = self._file_path(key)
        os.remove(file_path)

    def _file_path(self, key):
        return self._directory.joinpath(key)
