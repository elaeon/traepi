import uuid
import urllib
from abc import ABC, abstractmethod
import io
import csv
import logging
from pathlib import Path


log = logging.getLogger(__file__)
log_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)
log.addHandler(log_handler)
log.setLevel(logging.INFO)


try:
    import pyorc
except ImportError:
    log.debug("pyrorc is not installed")


class Output(ABC):
    file_extension = None
    basepath = ''

    def __init__(self, content: list, url: str, headers: dict = None, basepath: str = ''):
        self.content = content
        self.url = url
        self.id = str(uuid.uuid5(uuid.NAMESPACE_DNS, url))
        self.domain_name = urllib.parse.urlparse(self.url).netloc
        self.headers = headers
        self.basepath = basepath

    def filepath(self) -> Path:
        filename = f'{self.id}.{self.file_extension}'
        path = Path(self.basepath)
        path.mkdir(parents=False, exist_ok=True)
        return path.joinpath(filename)

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

    @abstractmethod
    def clean(self):
        raise NotImplementedError


class CsvOutput(Output):
    file_extension = 'csv'

    def write(self, header=True, compress=None, sep='|'):
        if not self.is_valid():
            raise Exception("not csv compatible")
        names = self.get_header()
        output_io = io.StringIO()
        dict_writer = csv.DictWriter(output_io, fieldnames=names, delimiter=sep)
        if header is True:
            dict_writer.writeheader()
        dict_writer.writerows(self.content)
        if compress is None:
            with self.filepath().open('w', encoding='utf-8', newline='') as f:
                f.write(output_io.getvalue())
        else:
            compress(self.id, output_io)

    def is_valid(self) -> bool:
        return self.get_header() is not None

    def clean(self):
        self.filepath().unlink(True)


class Stdout(Output):
    file_extension = 'txt'

    def write(self):
        log.info(len(self.content))
        if len(self.content) > 0:
            with self.filepath().open('w', encoding='utf-8') as f:
                f.write(str(self.content))

    def clean(self):
        self.filepath().unlink(True)


class OrcOuput(Output):
    file_extension = 'orc'

    def write(self, srt_type: dict = None):
        headers = self.get_header()
        cols = []
        for key in headers:
            ctype = srt_type[key]
            cols.append(f'{key}:{ctype}')
        str_cols = ",".join(cols)
        struct_col = f"struct<{str_cols}>"
        with self.filepath().open("wb") as f:
            with pyorc.Writer(f, struct_col) as writer:
                for r in self.content:
                    writer.write(tuple(r.values()))

    def clean(self):
        self.filepath().unlink()
