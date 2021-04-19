from abc import ABC, abstractmethod
from pathlib import Path
import uuid
import urllib


class OutputABC(ABC):
    file_extension = None
    basepath = ''

    @abstractmethod
    def write(self):
        raise NotImplementedError

    @abstractmethod
    def clean(self):
        raise NotImplementedError