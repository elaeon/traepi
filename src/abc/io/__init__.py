from abc import ABC, abstractmethod


class OutputABC(ABC):
    file_extension = None

    @abstractmethod
    def write_buff(self):
        raise NotImplemented

    @abstractmethod
    def write_disk(self):
        raise NotImplemented

    @abstractmethod
    def clean(self):
        raise NotImplementedError
