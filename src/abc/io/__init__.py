from abc import ABC, abstractmethod


class OutputABC(ABC):
    file_extension = None

    @abstractmethod
    def write(self):
        raise NotImplementedError

    @abstractmethod
    def clean(self):
        raise NotImplementedError
