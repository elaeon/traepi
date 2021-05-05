import urllib
import io
import csv
import logging
from pathlib import Path
import uuid
from .abc.io import OutputABC
from abc import abstractmethod
import gzip


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


class Output(OutputABC):

    def __init__(self, content, url: str, headers: dict = None, basepath: Path = None):
        self.content = content
        self.url = url
        self.id = str(uuid.uuid5(uuid.NAMESPACE_DNS, url))
        self.domain_name = urllib.parse.urlparse(self.url).netloc
        self.headers = headers
        self.basepath = basepath.resolve()
        self.buffer = None

    def filepath(self) -> Path:
        filename = f'{self.id}.{self.file_extension}'
        self.basepath.mkdir(parents=False, exist_ok=True)
        return self.basepath.joinpath(filename)

    def get_header(self) -> dict.keys:
        if isinstance(self.content, list):
            if len(self.content) > 0:
                if isinstance(self.content[0], dict):
                    return self.content[0].keys()
                else:
                    fake_header = {f'col{i}': False for i in range(len(self.content))}
                    return fake_header.keys()
            else:
                return ''
        elif isinstance(self.content, dict):
            return self.content.keys()
        else:
            return None

    @abstractmethod
    def write_buff(self):
        raise NotImplementedError

    @abstractmethod
    def write_disk(self):
        raise NotImplementedError

    def write(self):
        self.write_buff()
        self.write_disk()

    @abstractmethod
    def clean(self):
        raise NotImplementedError


class CsvOutput(Output):
    file_extension = 'csv'

    def write_buff(self, header=True, sep='|'):
        if not self.is_valid():
            raise Exception("not csv compatible")
        names = self.get_header()
        self.buffer = io.StringIO()
        dict_writer = csv.DictWriter(self.buffer, fieldnames=names, delimiter=sep)
        if header is True:
            dict_writer.writeheader()
        dict_writer.writerows(self.content)

    def write_disk(self):
        if self.buffer is not None:
            with self.filepath().open('w', encoding='utf-8', newline='') as f:
                f.write(self.buffer.getvalue())

    def is_valid(self) -> bool:
        return self.get_header() is not None

    def clean(self):
        self.filepath().unlink(True)


class TextOutput(Output):
    file_extension = 'txt'

    def write_buff(self):
        log.info(len(self.content))
        if len(self.content) > 0:
            self.buffer = io.StringIO()
            self.buffer.write(str(self.content))

    def write_disk(self):
        if self.buffer is not None:
            with self.filepath().open('w', encoding='utf-8') as f:
                f.write(self.buffer.getvalue())

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


class GzipOutput(Output):
    file_extension = 'gz'

    def __init__(self, output: Output):
        super(GzipOutput, self).__init__(output.content, output.url, basepath=output.basepath)
        self.output = output

    def write_buff(self):
        self.buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=self.buffer, mode='wb') as f:
            f.write(self.output.buffer.getvalue().encode())

    def write_disk(self):
        with self.filepath().open(mode='wb') as f:
            f.write(self.buffer.getvalue())

    def clean(self):
        self.filepath().unlink()
