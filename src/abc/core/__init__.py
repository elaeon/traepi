from abc import ABC, abstractmethod
from pathlib import Path


class MetadataABC(ABC):
    file_extension = ''
    filename = ''

    def __init__(self, basepath: Path):
        self.basepath = basepath.resolve()

    def filepath(self) -> Path:
        return self.basepath.joinpath(f'{self.filename}.{self.file_extension}')

    @abstractmethod
    def write(self, output):
        raise NotImplementedError

    @abstractmethod
    def clean(self):
        raise NotImplemented

    @abstractmethod
    def error(self, name, url):
        raise NotImplemented


class StreamABC(ABC):
    response_wait_key = None
    url = None
    url_params = None
    key = None

    @abstractmethod
    def __next__(self):
        raise NotImplemented

    def __iter__(self):
        return self

    @abstractmethod
    def set_next(self, value):
        raise NotImplemented

