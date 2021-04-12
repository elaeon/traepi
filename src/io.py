import uuid
import urllib
from abc import ABC, abstractmethod
from .core import Metadata, CSVMetadata
import io
import csv

try:
    import pyorc
except ImportError:
    pass


class Output(ABC):
    def __init__(self, content: list, url: str, headers: dict = None):
        self.content = content
        self.url = url
        self.id = str(uuid.uuid5(uuid.NAMESPACE_DNS, url))
        self.domain_name = urllib.parse.urlparse(self.url).netloc
        self.headers = headers

    @abstractmethod
    def write(self):
        raise NotImplementedError

    def get_header(self) -> dict.keys:
        if isinstance(self.content, list):
            if isinstance(self.content[0], dict):
                return self.content[0].keys()
            else:
                fake_header = {f'col{i}': False for i in range(len(self.content))}
                return fake_header.keys()
        elif isinstance(self.content, dict):
            return self.content.keys()
        else:
            return None


class CsvOutput(Output):
    def write(self, header=True, compress=None, sep='|', metadata: Metadata = CSVMetadata):
        if not self.is_valid():
            raise Exception("not csv compatible")
        names = self.get_header()
        output_io = io.StringIO()
        dict_writer = csv.DictWriter(output_io, fieldnames=names, delimiter=sep)
        if header is True:
            dict_writer.writeheader()
        dict_writer.writerows(self.content)
        if compress is None:
            with open(f'{self.id}.csv', 'w', encoding='utf-8', newline='') as f:
                f.write(output_io.getvalue())
        else:
            compress(self.id, output_io)
        if metadata is not None:
            metadata.write(self.domain_name, self.content, self.url)

    def is_valid(self) -> bool:
        return self.get_header() is not None


class Stdout(Output):
    def write(self):
        print(self.content)


class OrcOuput(Output):
    def write(self, srt_type: dict = None, metadata: Metadata = CSVMetadata):
        headers = self.get_header()
        cols = []
        for key in headers:
            ctype = srt_type[key]
            cols.append(f'{key}:{ctype}')
        str_cols = ",".join(cols)
        struct_col = f"struct<{str_cols}>"
        with open(f'{self.id}.orc', "wb") as f:
            with pyorc.Writer(f, struct_col) as writer:
                for r in self.content:
                    writer.write(tuple(r.values()))
