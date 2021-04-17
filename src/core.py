import logging
import aiohttp
import asyncio
import io
import gzip
import urllib
from abc import ABC, abstractmethod
import csv
from pathlib import Path


log = logging.getLogger(__file__)
log_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)
log.addHandler(log_handler)
log.setLevel(logging.INFO)


class Metadata(ABC):
    file_extension = ''
    filename = ''

    def __init__(self, basepath):
        self.basepath = basepath
        self.clean()

    def filepath(self) -> Path:
        return Path(self.basepath).joinpath(f'{self.filename}.{self.file_extension}')

    @abstractmethod
    def write(self, output):
        raise NotImplementedError

    @abstractmethod
    def clean(self):
        raise NotImplemented

    @abstractmethod
    def error(self, name, url):
        raise NotImplemented


class DummyMetadata(Metadata):
    def write(self, output):
        pass

    def clean(self):
        pass

    def error(self, name, url):
        log.info(f'{name}, {url}')


class CSVMetadata(Metadata):
    file_extension = 'csv'
    filename = 'metadata'

    def write(self, output):
        if len(output.content) > 0:
            with self.filepath().open('a', encoding='utf-8', newline='') as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow([str(len(output.content)), output.filepath().name, output.url])

    def read(self):
        with self.filepath().open('r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for line in csv_reader:
                yield line

    def clean(self):
        try:
            self.filepath().unlink()
        except FileNotFoundError:
            log.exception("File not Found")

    def error(self, name, url):
        with self.filepath().open('a', encoding='utf-8', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([name, '', url])


class Stream(ABC):
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

    @staticmethod
    def build_url(url, url_params):
        return urllib.parse.urlunparse(
            urllib.parse.urlparse(url)._replace(query=urllib.parse.urlencode(url_params)))


# fixme: for reliability would be good change BaseStream name to Stream
class BaseStream(Stream):
    def __init__(self, seq):
        if isinstance(seq, list) or isinstance(seq, tuple):
            self.stream = iter(seq)
        elif isinstance(seq, iter):
            self.stream = seq
        else:
            self.stream = []

    def set_next(self, value):
        return next(self.stream)

    def __next__(self):
        return self.set_next(None)


class StreamPage(Stream):
    def __init__(self, resource, key, response_wait_key=None, **params):
        # {'pageIndex': 1, 'pageSize': 100}
        self.url_params = params if len(params) > 0 else \
            dict(urllib.parse.parse_qsl(urllib.parse.urlparse(resource).query))
        self.url = self.build_url(resource, self.url_params)
        self.key = key
        self.response_wait_key = response_wait_key

    def set_next(self, value):
        self.url_params[self.key] = value

    def __next__(self):
        try:
            self.set_next(int(self.url_params[self.key]) + 1)
        except KeyError:
            url = self.build_url(self.url, self.url_params)
            self.url_params[self.key] = 0
            return url
        else:
            return self.build_url(self.url, self.url_params)


class StreamPageWait(StreamPage):
    def __next__(self):
        return self.build_url(self.url, self.url_params)


class BuffStream:
    def __init__(self, stream: Stream = None, buff_size: int = 5):
        if isinstance(stream, StreamPageWait):
            self.buff_size = 1
        else:
            self.buff_size = buff_size
        self.stream = stream

    def __repr__(self):
        return f'{self.__class__.__name__}'

    def __iter__(self):
        return self

    def __next__(self) -> list:
        buff = []
        for e in self.stream:
            buff.append(e)
            if len(buff) >= self.buff_size:
                break
        if len(buff) > 0:
            return buff
        else:
            raise StopIteration


class Request:
    def __init__(self, buff: BuffStream, metadata: Metadata):
        self.buff = buff
        self.is_buff_depleted = False
        self.metadata = metadata

    async def except_fn(self, callback, session, url, headers):
        try:
            return await callback(session, url, headers)
        except aiohttp.ServerDisconnectedError:
            self.metadata.error("timeout", url)
            log.info("Server disconnected")

    async def get(self, headers=None, callback=None):
        for batch in self.buff:
            async with aiohttp.ClientSession() as session:
                tasks = [asyncio.create_task(self.except_fn(callback, session, url, headers)) for url in batch]
                yield tasks
                if self.is_buff_depleted is True:
                    break

    async def output(self, coro, clean: bool = False, timeout=5, **kwargs):
        tasks = coro
        async for task_b in tasks:
            for task in asyncio.as_completed(task_b, timeout=timeout):
                try:
                    output = await task
                    if self.buff.stream.response_wait_key is not None:
                        self.buff.stream.set_next(output.headers.get(self.buff.stream.response_wait_key))
                    if output.content is None or len(output.content) == 0:
                        self.is_buff_depleted = True
                    output.write(**kwargs)
                    self.metadata.write(output)
                    if clean:
                        output.clean()
                except asyncio.exceptions.TimeoutError:
                    log.info(f"Request timeout")

    @staticmethod
    def run(output):
        r = asyncio.run(output)
        return r


def gzipf(filename: str, buffer: io.StringIO):
    with gzip.GzipFile(f'{filename}.csv.gz', mode='wb') as f:
        f.write(buffer.getvalue().encode())
