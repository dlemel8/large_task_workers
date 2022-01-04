import os
from contextlib import contextmanager
from pathlib import Path

from mmap import mmap, ACCESS_READ

from consumer.application.handler import BytesStore


class FileStore(BytesStore):
    def __init__(self, directory: Path):
        if not directory.is_dir():
            raise ValueError(f'{directory} is not a directory')

        if not os.access(directory, os.W_OK):
            raise ValueError(f'directory {directory} is not writable')

        self._directory = directory

    @contextmanager
    def load(self, key: str) -> memoryview:
        file_path = self._file_path(key)
        with open(file_path, mode='r', encoding='utf-8') as file_obj:
            with mmap(file_obj.fileno(), length=0, access=ACCESS_READ) as mmap_obj:
                res = memoryview(mmap_obj).toreadonly()
                try:
                    yield res
                finally:
                    res.release()

    def delete(self, key: str):
        file_path = self._file_path(key)
        os.remove(file_path)

    def _file_path(self, key):
        return self._directory.joinpath(key)
